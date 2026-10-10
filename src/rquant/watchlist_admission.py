"""Opt-in local admission for owner-bound PageControl watchlist commands."""

from __future__ import annotations

import http.client
import json
import os
import socket
import socketserver
import sqlite3
import stat
import struct
import sys
import threading
from collections.abc import Callable, Mapping
from contextlib import closing
from ctypes import CDLL, byref, c_uint, get_errno
from http.server import BaseHTTPRequestHandler
from pathlib import Path

from rquant.manual_watchlist import ManualWatchlistKey
from rquant.page_control import (
    AddWatchlistItem,
    PageControlCommandConflictError,
    PageControlOutbox,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
    RemoveWatchlistItem,
    parse_page_control_command,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import strict_json_loads

_MAX_COMMAND_BYTES = 8 * 1024
_MAX_RESPONSE_BYTES = 16 * 1024
_ROUTE = "/v1/watchlist-admission"
_LOOKUP_ROUTE = f"{_ROUTE}/lookup"
_RESUME_ROUTE = f"{_ROUTE}/resume"
WatchlistCommand = AddWatchlistItem | RemoveWatchlistItem


class WatchlistAdmissionOwnerMismatchError(ValueError):
    """Authenticated owner and the proposed command disagree."""


class WatchlistAdmissionMissingError(ValueError):
    """Resume cannot create a command that has no persisted identity."""


class WatchlistAdmissionUnavailableError(RuntimeError):
    """The private listener did not return a usable command receipt."""


class WatchlistAdmissionRejectedError(ValueError):
    """The private listener rejected a known command without changing state."""


class WatchlistAdmission:
    """Recheck the trusted owner and use PageControl's durable atomic command path."""

    def __init__(self, service: PageControlService) -> None:
        self.service = service
        self._lock = threading.Lock()

    def admit(
        self, command: WatchlistCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        owner = self._validated_owner(command, authenticated_owner_id)
        with self._lock:
            return self.service._submit_trusted_watchlist(command, authenticated_owner_id=owner)

    @staticmethod
    def _validated_owner(command: WatchlistCommand, authenticated_owner_id: str) -> str:
        if not isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise TypeError("watchlist admission requires a watchlist command")
        try:
            owner = ManualWatchlistKey(
                owner_id=authenticated_owner_id, ts_code=command.item.ts_code
            ).owner_id
        except ValueError as exc:
            raise WatchlistAdmissionOwnerMismatchError("invalid authenticated owner") from exc
        if owner != command.item.owner_id:
            raise WatchlistAdmissionOwnerMismatchError("authenticated owner differs from command")
        return owner

    def lookup(
        self, command: WatchlistCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        self._validated_owner(command, authenticated_owner_id)
        with self._lock:
            return self._lookup_exact(command)

    def resume(
        self, command: WatchlistCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        self._validated_owner(command, authenticated_owner_id)
        with self._lock:
            original = self._lookup_exact(command)
            if original is None:
                raise WatchlistAdmissionMissingError("watchlist command does not exist")
            return self.service._settle(command, original)

    def _lookup_exact(self, command: WatchlistCommand) -> PageControlReceipt | None:
        path = Path(os.path.abspath(self.service.outbox.path))
        uri = f"{path.as_uri()}?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=1.0)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            row = connection.execute(
                "SELECT * FROM page_control_command WHERE command_id = ?",
                (command.command_id,),
            ).fetchone()
        if row is None:
            return None
        command_hash = canonical_sha256(command.model_dump(mode="json"))
        if row["command_kind"] != command.kind or row["command_hash"] != command_hash:
            raise PageControlCommandConflictError(
                "command_id already exists with different payload"
            )
        try:
            stored = parse_page_control_command(strict_json_loads(row["payload_json"]))
        except (TypeError, ValueError) as exc:
            raise PageControlCommandConflictError(
                "stored watchlist command payload is invalid"
            ) from exc
        if (
            type(stored) is not type(command)
            or canonical_sha256(stored.model_dump(mode="json")) != command_hash
        ):
            raise PageControlCommandConflictError(
                "stored watchlist command conflicts with its hash"
            )
        receipt = PageControlOutbox._receipt(row)
        if receipt.enqueued_at != command.requested_at:
            raise PageControlCommandConflictError(
                "stored watchlist command receipt conflicts with its payload"
            )
        return receipt


def _peer_uid(connection: socket.socket) -> int:
    getpeereid = getattr(connection, "getpeereid", None)
    if callable(getpeereid):
        uid, _gid = getpeereid()
        return int(uid)
    if sys.platform == "darwin":
        uid = c_uint()
        gid = c_uint()
        libc = CDLL(None, use_errno=True)
        if libc.getpeereid(connection.fileno(), byref(uid), byref(gid)) != 0:
            raise OSError(get_errno(), "getpeereid failed")
        return int(uid.value)
    if hasattr(socket, "SO_PEERCRED"):
        credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        _pid, uid, _gid = struct.unpack("3i", credentials)
        return int(uid)
    raise OSError("Unix peer credentials are unavailable")


def _socket_identity(path: Path) -> tuple[int, int] | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISSOCK(info.st_mode):
        return None
    return info.st_dev, info.st_ino


def _unlink_matching_socket(path: Path, identity: tuple[int, int] | None) -> None:
    if identity is not None and _socket_identity(path) == identity:
        path.unlink()


def _assert_private_endpoint(path: Path, expected_uid: int) -> tuple[int, int]:
    parent = path.parent.lstat()
    socket_info = path.lstat()
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != expected_uid
        or stat.S_IMODE(parent.st_mode) != 0o700
        or not stat.S_ISSOCK(socket_info.st_mode)
        or socket_info.st_uid != expected_uid
        or stat.S_IMODE(socket_info.st_mode) != 0o600
    ):
        raise ValueError("watchlist admission endpoint identity or permissions are unsafe")
    return socket_info.st_dev, socket_info.st_ino


class WatchlistAdmissionServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(
        self,
        socket_path: Path,
        admission: WatchlistAdmission,
        *,
        trusted_uid: int,
        peer_uid: Callable[[socket.socket], int],
    ) -> None:
        self.socket_path = socket_path
        self.admission = admission
        self.trusted_uid = trusted_uid
        self.peer_uid = peer_uid
        self._bound_identity: tuple[int, int] | None = None
        super().__init__(str(socket_path), _handler_for_admission())
        try:
            info = socket_path.lstat()
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid():
                raise ValueError("watchlist admission socket is unsafe")
            self._bound_identity = (info.st_dev, info.st_ino)
            os.chmod(socket_path, 0o600, follow_symlinks=False)
            info = socket_path.lstat()
            if (
                not stat.S_ISSOCK(info.st_mode)
                or info.st_uid != os.geteuid()
                or (info.st_dev, info.st_ino) != self._bound_identity
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise ValueError("watchlist admission socket permissions are unsafe")
        except Exception:
            super().server_close()
            _unlink_matching_socket(socket_path, self._bound_identity)
            raise

    def verify_request(self, request: socket.socket, client_address: object) -> bool:
        try:
            return self.peer_uid(request) == self.trusted_uid
        except Exception:
            return False

    def server_close(self) -> None:
        super().server_close()
        _unlink_matching_socket(self.socket_path, self._bound_identity)


def _decode_request(body: bytes) -> tuple[WatchlistCommand, str]:
    payload = strict_json_loads(body)
    if not isinstance(payload, dict) or set(payload) != {"authenticated_owner_id", "command"}:
        raise ValueError("invalid watchlist admission envelope")
    raw = payload["command"]
    if not isinstance(raw, dict):
        raise ValueError("invalid watchlist admission command")
    if raw.get("kind") == "add_watchlist_item":
        command = AddWatchlistItem.model_validate(raw)
    elif raw.get("kind") == "remove_watchlist_item":
        command = RemoveWatchlistItem.model_validate(raw)
    else:
        raise ValueError("unsupported watchlist admission command")
    owner = payload["authenticated_owner_id"]
    if not isinstance(owner, str):
        raise ValueError("authenticated owner must be a string")
    return command, owner


def _handler_for_admission() -> type[BaseHTTPRequestHandler]:
    class WatchlistAdmissionHandler(BaseHTTPRequestHandler):
        server: WatchlistAdmissionServer

        def do_POST(self) -> None:  # noqa: N802
            if self.path not in {_ROUTE, _LOOKUP_ROUTE, _RESUME_ROUTE}:
                self.send_error(404)
                return
            try:
                lengths = self.headers.get_all("Content-Length", [])
                if (
                    len(lengths) != 1
                    or self.headers.get("Transfer-Encoding") is not None
                    or self.headers.get("Content-Type") != "application/json"
                ):
                    raise ValueError("invalid watchlist request framing")
                size = int(lengths[0])
                if not 1 <= size <= _MAX_COMMAND_BYTES:
                    raise ValueError("invalid watchlist request size")
                command, owner = _decode_request(self.rfile.read(size))
            except (OSError, TypeError, ValueError):
                self._json(400, {"error": "invalid_command"})
                return
            try:
                if self.path == _LOOKUP_ROUTE:
                    receipt = self.server.admission.lookup(command, authenticated_owner_id=owner)
                    self._json(
                        200,
                        {"found": False}
                        if receipt is None
                        else {"found": True, "receipt": receipt.model_dump(mode="json")},
                    )
                    return
                receipt = (
                    self.server.admission.resume(command, authenticated_owner_id=owner)
                    if self.path == _RESUME_ROUTE
                    else self.server.admission.admit(command, authenticated_owner_id=owner)
                )
            except WatchlistAdmissionOwnerMismatchError:
                self._json(403, {"error": "owner_mismatch"})
                return
            except WatchlistAdmissionMissingError:
                self._json(404, {"error": "not_found"})
                return
            except PageControlCommandConflictError:
                self._json(409, {"error": "command_conflict"})
                return
            except ValueError:
                self._json(409, {"error": "rejected"})
                return
            except Exception:
                self._json(503, {"error": "unavailable"})
                return
            self._json(200, receipt.model_dump(mode="json"))

        def log_message(self, format: str, *args: object) -> None:
            return

        def _json(self, status: int, payload: object) -> None:
            body = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
            if len(body) > _MAX_RESPONSE_BYTES:
                status = 503
                body = b'{"error":"unavailable"}'
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return WatchlistAdmissionHandler


def build_watchlist_admission_server(
    admission: WatchlistAdmission,
    *,
    socket_path: Path | None,
    trusted_uid: int | None = None,
    peer_uid: Callable[[socket.socket], int] = _peer_uid,
) -> WatchlistAdmissionServer | None:
    """Bind only an explicit, private and previously unused local socket path."""
    if socket_path is None:
        return None
    path = Path(socket_path)
    if not path.is_absolute() or path.name in {"", ".", ".."} or len(os.fsencode(path)) >= 100:
        raise ValueError("watchlist admission socket path must be short and absolute")
    parent = path.parent
    if not parent.exists():
        parent.mkdir(mode=0o700)
    info = parent.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("watchlist admission directory must be owned and mode 0700")
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("watchlist admission socket path already exists")
    uid = os.geteuid() if trusted_uid is None else trusted_uid
    if uid != os.geteuid():
        raise ValueError("watchlist admission trusted UID must be the service UID")
    return WatchlistAdmissionServer(path, admission, trusted_uid=uid, peer_uid=peer_uid)


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(
        self, socket_path: Path, *, timeout_seconds: float, expected_service_uid: int
    ) -> None:
        super().__init__("localhost", timeout=timeout_seconds)
        self.socket_path = socket_path
        self.expected_service_uid = expected_service_uid

    def connect(self) -> None:
        before = _assert_private_endpoint(self.socket_path, self.expected_service_uid)
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            connection.settimeout(self.timeout)
            connection.connect(str(self.socket_path))
            if _peer_uid(connection) != self.expected_service_uid:
                raise OSError("watchlist admission peer UID is untrusted")
            if _assert_private_endpoint(self.socket_path, self.expected_service_uid) != before:
                raise OSError("watchlist admission socket changed during connection")
        except BaseException:
            connection.close()
            raise
        self.sock = connection


class WatchlistAdmissionClient:
    """Bounded private client; uncertain responses are retried with the same command."""

    def __init__(
        self,
        socket_path: Path,
        *,
        timeout_seconds: float = 1.0,
        expected_service_uid: int | None = None,
    ) -> None:
        self.socket_path = Path(socket_path)
        if not self.socket_path.is_absolute() or timeout_seconds <= 0:
            raise ValueError("watchlist admission client requires an absolute path and timeout")
        self.timeout_seconds = timeout_seconds
        self.expected_service_uid = (
            os.geteuid() if expected_service_uid is None else expected_service_uid
        )
        if type(self.expected_service_uid) is not int or self.expected_service_uid < 0:
            raise ValueError("watchlist admission expected service UID is invalid")

    def submit(
        self, command: WatchlistCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        receipt = self._request(_ROUTE, command, authenticated_owner_id=authenticated_owner_id)
        assert receipt is not None
        return receipt

    def lookup(
        self, command: WatchlistCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        return self._request(_LOOKUP_ROUTE, command, authenticated_owner_id=authenticated_owner_id)

    def resume(
        self, command: WatchlistCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        receipt = self._request(
            _RESUME_ROUTE, command, authenticated_owner_id=authenticated_owner_id
        )
        assert receipt is not None
        return receipt

    def _request(
        self, route: str, command: WatchlistCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        if not isinstance(command, (AddWatchlistItem, RemoveWatchlistItem)):
            raise TypeError("watchlist admission client requires a watchlist command")
        body = json.dumps(
            {
                "authenticated_owner_id": authenticated_owner_id,
                "command": command.model_dump(mode="json"),
            },
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(body) > _MAX_COMMAND_BYTES:
            raise ValueError("watchlist admission command exceeds bound")
        connection = _UnixHTTPConnection(
            self.socket_path,
            timeout_seconds=self.timeout_seconds,
            expected_service_uid=self.expected_service_uid,
        )
        try:
            connection.request(
                "POST", route, body=body, headers={"Content-Type": "application/json"}
            )
            response = connection.getresponse()
            received = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(received) > _MAX_RESPONSE_BYTES:
                raise WatchlistAdmissionUnavailableError(
                    "watchlist admission response exceeds bound"
                )
            if response.status in (403, 404, 409):
                rejection = strict_json_loads(received)
                code = rejection.get("error") if isinstance(rejection, dict) else None
                if code in {"owner_mismatch", "command_conflict", "rejected", "not_found"}:
                    raise WatchlistAdmissionRejectedError(code)
            if response.status != 200:
                raise WatchlistAdmissionUnavailableError(
                    f"watchlist admission returned HTTP {response.status}"
                )
            decoded = strict_json_loads(received)
            if route == _LOOKUP_ROUTE:
                if decoded == {"found": False}:
                    return None
                if (
                    not isinstance(decoded, dict)
                    or set(decoded) != {"found", "receipt"}
                    or decoded["found"] is not True
                ):
                    raise ValueError("watchlist lookup response is invalid")
                decoded = decoded["receipt"]
            receipt = PageControlReceipt.model_validate(decoded)
            if (
                receipt.command_id != command.command_id
                or receipt.enqueued_at != command.requested_at
            ):
                raise WatchlistAdmissionUnavailableError(
                    "watchlist admission returned another command"
                )
            if receipt.status is PageControlStatus.SUCCEEDED:
                expected_action = "add" if isinstance(command, AddWatchlistItem) else "remove"
                if (
                    not isinstance(receipt.result, Mapping)
                    or receipt.result.get("ts_code") != command.item.ts_code
                    or receipt.result.get("action") != expected_action
                ):
                    raise WatchlistAdmissionUnavailableError(
                        "watchlist admission returned a mismatched result"
                    )
            return receipt
        except WatchlistAdmissionRejectedError:
            raise
        except (OSError, TimeoutError, http.client.HTTPException, ValueError) as exc:
            raise WatchlistAdmissionUnavailableError(
                "watchlist admission unavailable or invalid"
            ) from exc
        finally:
            connection.close()
