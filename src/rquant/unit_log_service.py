"""Private, bounded Unix transport for the signed service journal reader."""

from __future__ import annotations

import os
import re
import socket
import stat
import struct
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from datetime import datetime
from pathlib import Path
from typing import Protocol

from pydantic import ValidationError

from rquant.strict_json import canonical_json_bytes, strict_json_loads
from rquant.unit_log_reader import (
    JournalCursorError,
    JournalPage,
    JournalRequestError,
    JournalUnavailableError,
)

_MAX_REQUEST_BYTES = 8 * 1024
_MAX_RESPONSE_BYTES = 256 * 1024
_MAX_SOCKET_PATH_BYTES = 100
_MAX_PAGE_SIZE = 498
_READ_TIMEOUT_SECONDS = 1.0
_ERROR_MESSAGES = {
    "forbidden": "无权查看运行日志",
    "invalid_request": "日志请求无效",
    "busy": "请求较多，请稍后重试",
    "cursor_changed": "日志已更新，请重新查看",
    "unavailable": "运行日志暂不可用",
}
_REQUEST_FIELDS = frozenset({"version", "op", "unit", "since", "level", "page_size", "cursor"})
_SERVICE_UNIT = re.compile(r"rquant-[a-z0-9-]{1,100}\.service\Z")
_SINCE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})\Z"
)
_LEVELS = frozenset({"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"})


class JournalReader(Protocol):
    def read(
        self,
        *,
        unit: str,
        since: datetime,
        level: str | None = None,
        page_size: int = 100,
        cursor: str | None = None,
    ) -> JournalPage: ...


class UnitLogServiceError(RuntimeError):
    """A closed transport error whose message never includes source data."""

    def __init__(self, code: str) -> None:
        if code not in _ERROR_MESSAGES:
            code = "unavailable"
        self.code = code
        super().__init__(_ERROR_MESSAGES[code])


def _kernel_peer_uid(connection: socket.socket) -> int:
    if hasattr(socket, "SO_PEERCRED"):
        raw = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        _pid, uid, _gid = struct.unpack("3i", raw)
        return uid
    if hasattr(connection, "getpeereid"):
        uid, _gid = connection.getpeereid()  # type: ignore[attr-defined]
        return uid
    if hasattr(socket, "LOCAL_PEERCRED"):
        raw = connection.getsockopt(0, socket.LOCAL_PEERCRED, 76)
        version, uid, count = struct.unpack_from("=IIh", raw)
        if version != 0 or count < 1:
            raise OSError
        return uid
    raise OSError


def _private_directory(path: Path, *, owner_uid: int, web_group_gid: int) -> None:
    if (
        not path.is_absolute()
        or ".." in path.parts
        or len(os.fsencode(path)) >= _MAX_SOCKET_PATH_BYTES
    ):
        raise ValueError("private unit log socket path is unsafe")
    directory = path.parent.lstat()
    if (
        not stat.S_ISDIR(directory.st_mode)
        or directory.st_uid != owner_uid
        or directory.st_gid != web_group_gid
        or stat.S_IMODE(directory.st_mode) != 0o710
    ):
        raise ValueError("private unit log directory must be owned and mode 0710")


def _socket_identity(path: Path, *, owner_uid: int, web_group_gid: int) -> None:
    state = path.lstat()
    if (
        not stat.S_ISSOCK(state.st_mode)
        or state.st_uid != owner_uid
        or state.st_gid != web_group_gid
        or stat.S_IMODE(state.st_mode) != 0o660
    ):
        raise UnitLogServiceError("unavailable")


@contextmanager
def _private_listener(path: Path, *, web_group_gid: int) -> Iterator[socket.socket]:
    owner_uid = os.geteuid()
    _private_directory(path, owner_uid=owner_uid, web_group_gid=web_group_gid)
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("private unit log socket path already exists")

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    identity: tuple[int, int] | None = None
    try:
        old_umask = os.umask(0o177)
        try:
            listener.bind(str(path))
        finally:
            os.umask(old_umask)
        state = path.lstat()
        identity = state.st_dev, state.st_ino
        if not stat.S_ISSOCK(state.st_mode) or state.st_uid != owner_uid:
            raise UnitLogServiceError("unavailable")
        os.chown(path, -1, web_group_gid, follow_symlinks=False)
        os.chmod(path, 0o660, follow_symlinks=False)
        _socket_identity(path, owner_uid=owner_uid, web_group_gid=web_group_gid)
        ready = path.lstat()
        if (ready.st_dev, ready.st_ino) != identity:
            raise UnitLogServiceError("unavailable")
        listener.listen(4)
        listener.settimeout(0.1)
        yield listener
    finally:
        listener.close()
        if identity is not None:
            try:
                current = path.lstat()
            except FileNotFoundError:
                pass
            else:
                if stat.S_ISSOCK(current.st_mode) and (current.st_dev, current.st_ino) == identity:
                    path.unlink()


def _receive_exact(connection: socket.socket, size: int, *, deadline: float) -> bytes:
    received = bytearray()
    while len(received) < size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        connection.settimeout(remaining)
        chunk = connection.recv(size - len(received))
        if not chunk:
            raise ValueError
        received.extend(chunk)
    return bytes(received)


def _read_frame(connection: socket.socket, *, limit: int, deadline: float) -> bytes:
    length = struct.unpack("!I", _receive_exact(connection, 4, deadline=deadline))[0]
    if not 0 < length <= limit:
        raise ValueError
    return _receive_exact(connection, length, deadline=deadline)


def _send_frame(connection: socket.socket, payload: bytes) -> None:
    if not 0 < len(payload) <= _MAX_RESPONSE_BYTES:
        raise ValueError
    connection.sendall(struct.pack("!I", len(payload)) + payload)


def _request_values(payload: bytes) -> dict[str, object]:
    try:
        request = strict_json_loads(payload)
        if type(request) is not dict or set(request) != _REQUEST_FIELDS:
            raise ValueError
        unit = request["unit"]
        since = request["since"]
        level = request["level"]
        size = request["page_size"]
        cursor = request["cursor"]
        if (
            type(request["version"]) is not int
            or request["version"] != 1
            or request["op"] != "read"
            or type(unit) is not str
            or _SERVICE_UNIT.fullmatch(unit) is None
            or type(since) is not str
            or _SINCE.fullmatch(since) is None
            or (level is not None and (type(level) is not str or level not in _LEVELS))
            or type(size) is not int
            or not 1 <= size <= _MAX_PAGE_SIZE
            or (cursor is not None and (type(cursor) is not str or len(cursor) > 4096))
        ):
            raise ValueError
        parsed_since = datetime.fromisoformat(since)
        if parsed_since.utcoffset() is None:
            raise ValueError
        return {
            "unit": unit,
            "since": parsed_since,
            "level": level,
            "page_size": size,
            "cursor": cursor,
        }
    except (TypeError, ValueError, UnicodeError):
        raise UnitLogServiceError("invalid_request") from None


def _error_response(code: str) -> bytes:
    return canonical_json_bytes({"status": "error", "code": code, "message": _ERROR_MESSAGES[code]})


class UnitLogService:
    """Serve exactly one journal read at a time, refusing excess work quickly."""

    def __init__(
        self,
        *,
        socket_path: Path,
        web_group_gid: int,
        web_uid: int,
        reader: JournalReader,
        peer_uid: Callable[[socket.socket], int] | None = None,
        min_interval_seconds: float = 1.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if (
            type(web_uid) is not int
            or web_uid < 0
            or type(web_group_gid) is not int
            or web_group_gid < 0
            or type(min_interval_seconds) not in (int, float)
            or not 1 <= min_interval_seconds <= 60
        ):
            raise ValueError("unit log service configuration is invalid")
        self.socket_path = Path(socket_path)
        self.web_group_gid = web_group_gid
        self.web_uid = web_uid
        self.reader = reader
        self.peer_uid = peer_uid or _kernel_peer_uid
        self.min_interval_seconds = min_interval_seconds
        self.monotonic = monotonic

    def _serve_one(self, connection: socket.socket, gate: threading.Lock) -> None:
        try:
            with connection:
                try:
                    connection.settimeout(_READ_TIMEOUT_SECONDS)
                    try:
                        values = _request_values(
                            _read_frame(
                                connection,
                                limit=_MAX_REQUEST_BYTES,
                                deadline=time.monotonic() + _READ_TIMEOUT_SECONDS,
                            )
                        )
                    except (OSError, ValueError):
                        raise UnitLogServiceError("invalid_request") from None
                    page = self.reader.read(**values)
                    if type(page) is not JournalPage:
                        raise ValueError
                    response = canonical_json_bytes(
                        {"status": "ok", "page": page.model_dump(mode="json")}
                    )
                    if len(response) > _MAX_RESPONSE_BYTES:
                        raise ValueError
                except UnitLogServiceError as exc:
                    response = _error_response(exc.code)
                except JournalRequestError:
                    response = _error_response("invalid_request")
                except JournalCursorError:
                    response = _error_response("cursor_changed")
                except JournalUnavailableError:
                    response = _error_response("unavailable")
                except Exception:
                    response = _error_response("unavailable")
                with suppress(OSError, ValueError):
                    _send_frame(connection, response)
        finally:
            gate.release()

    def serve(self, *, stop: threading.Event, ready: threading.Event | None = None) -> None:
        gate = threading.Lock()
        last_admitted: float | None = None
        worker: threading.Thread | None = None
        with _private_listener(self.socket_path, web_group_gid=self.web_group_gid) as listener:
            if ready is not None:
                ready.set()
            while not stop.is_set():
                try:
                    connection, _ = listener.accept()
                except TimeoutError:
                    continue
                with connection:
                    try:
                        uid = self.peer_uid(connection)
                    except Exception:
                        uid = None
                    if uid != self.web_uid:
                        response = _error_response("forbidden")
                    else:
                        now = self.monotonic()
                        if not gate.acquire(blocking=False):
                            response = _error_response("busy")
                        elif (
                            last_admitted is not None
                            and now - last_admitted < self.min_interval_seconds
                        ):
                            gate.release()
                            response = _error_response("busy")
                        else:
                            last_admitted = now
                            worker_connection = connection.detach()
                            worker_socket = socket.socket(fileno=worker_connection)
                            worker = threading.Thread(
                                target=self._serve_one,
                                args=(worker_socket, gate),
                                daemon=True,
                            )
                            worker.start()
                            continue
                    try:
                        connection.settimeout(0.2)
                        _send_frame(connection, response)
                    except (OSError, ValueError):
                        pass
            if worker is not None:
                worker.join(6.5)


class UnitLogClient:
    """Validate the private endpoint and decode only closed journal projections."""

    def __init__(
        self,
        *,
        socket_path: Path,
        service_uid: int,
        web_group_gid: int,
        peer_uid: Callable[[socket.socket], int] | None = None,
    ) -> None:
        self.socket_path = Path(socket_path)
        self.service_uid = service_uid
        self.web_group_gid = web_group_gid
        self.peer_uid = peer_uid or _kernel_peer_uid

    def read(
        self,
        *,
        unit: str,
        since: datetime,
        level: str | None = None,
        page_size: int = 100,
        cursor: str | None = None,
    ) -> JournalPage:
        try:
            request = {
                "version": 1,
                "op": "read",
                "unit": unit,
                "since": since.isoformat(),
                "level": level,
                "page_size": page_size,
                "cursor": cursor,
            }
            _request_values(canonical_json_bytes(request))
            _private_directory(
                self.socket_path,
                owner_uid=self.service_uid,
                web_group_gid=self.web_group_gid,
            )
            _socket_identity(
                self.socket_path,
                owner_uid=self.service_uid,
                web_group_gid=self.web_group_gid,
            )
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(7)
                connection.connect(str(self.socket_path))
                if self.peer_uid(connection) != self.service_uid:
                    raise UnitLogServiceError("unavailable")
                _send_frame(connection, canonical_json_bytes(request))
                payload = _read_frame(
                    connection,
                    limit=_MAX_RESPONSE_BYTES,
                    deadline=time.monotonic() + 7,
                )
            response = strict_json_loads(payload)
            if type(response) is not dict or response.get("status") not in {"ok", "error"}:
                raise ValueError
            if response["status"] == "ok":
                if set(response) != {"status", "page"}:
                    raise ValueError
                return JournalPage.model_validate(response["page"])
            if set(response) != {"status", "code", "message"}:
                raise ValueError
            code = response["code"]
            if type(code) is not str or code not in _ERROR_MESSAGES:
                raise ValueError
            if response["message"] != _ERROR_MESSAGES[code]:
                raise ValueError
            raise UnitLogServiceError(code)
        except UnitLogServiceError:
            raise
        except (OSError, ValueError, TypeError, ValidationError, AttributeError):
            raise UnitLogServiceError("unavailable") from None


__all__ = ["UnitLogClient", "UnitLogService", "UnitLogServiceError"]
