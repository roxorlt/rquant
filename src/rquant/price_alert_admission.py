"""Opt-in, distinct-UID Unix admission for owner-bound price alert rules."""

from __future__ import annotations

import http.client
import json
import os
import socket
import socketserver
import stat
import struct
import sys
import threading
from collections.abc import Callable, Mapping
from ctypes import CDLL, byref, c_uint, get_errno
from http.server import BaseHTTPRequestHandler
from pathlib import Path

from pydantic import TypeAdapter

from rquant.manual_watchlist import OwnerId
from rquant.page_control import (
    DeletePriceAlertRule,
    PageControlCommandConflictError,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
    SavePriceAlertRule,
    SetPriceAlertRuleEnabled,
)
from rquant.strict_json import strict_json_loads

_MAX_COMMAND_BYTES = 8 * 1024
_MAX_RESPONSE_BYTES = 16 * 1024
_ROUTE = "/v1/price-alert-admission"
_LOOKUP_ROUTE = f"{_ROUTE}/lookup"
_RESUME_ROUTE = f"{_ROUTE}/resume"
_OWNER_ADAPTER = TypeAdapter(OwnerId)
PriceRuleCommand = SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule


class PriceAlertAdmissionUnavailableError(RuntimeError):
    """The private listener did not return a trustworthy final response."""


class PriceAlertAdmissionRejectedError(ValueError):
    """The listener proved that this request was rejected without a new effect."""


class PriceAlertAdmission:
    """Use only the PageControl trusted, owner-injecting price rule paths."""

    def __init__(self, service: PageControlService) -> None:
        self.service = service
        self._lock = threading.Lock()

    def submit(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        with self._lock:
            return self.service._submit_trusted_price_rule(
                command, authenticated_owner_id=authenticated_owner_id
            )

    def lookup(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        with self._lock:
            return self.service._lookup_trusted_price_rule(
                command, authenticated_owner_id=authenticated_owner_id
            )

    def resume(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        with self._lock:
            return self.service._resume_trusted_price_rule(
                command, authenticated_owner_id=authenticated_owner_id
            )


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


def _validate_socket_path(path: Path) -> None:
    if (
        not path.is_absolute()
        or ".." in path.parts
        or path.name in {"", ".", ".."}
        or len(os.fsencode(path)) >= 100
    ):
        raise ValueError("price admission socket path must be short and absolute")
    current = Path(path.anchor)
    for component in path.parts[1:-1]:
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise ValueError("price admission path contains a symlink")


def _socket_identity(path: Path) -> tuple[int, int] | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    return (info.st_dev, info.st_ino) if stat.S_ISSOCK(info.st_mode) else None


def _unlink_matching_socket(path: Path, identity: tuple[int, int] | None) -> None:
    if identity is not None and _socket_identity(path) == identity:
        path.unlink()


def _valid_uid_or_gid(value: object) -> bool:
    return type(value) is int and value >= 0


def _assert_endpoint(path: Path, service_uid: int, shared_gid: int) -> tuple[int, int, int, int]:
    _validate_socket_path(path)
    parent = path.parent.lstat()
    endpoint = path.lstat()
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != service_uid
        or parent.st_gid != shared_gid
        or stat.S_IMODE(parent.st_mode) != 0o710
        or not stat.S_ISSOCK(endpoint.st_mode)
        or endpoint.st_uid != service_uid
        or endpoint.st_gid != shared_gid
        or stat.S_IMODE(endpoint.st_mode) != 0o660
    ):
        raise ValueError("price admission endpoint identity or permissions are unsafe")
    return parent.st_dev, parent.st_ino, endpoint.st_dev, endpoint.st_ino


class PriceAlertAdmissionServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(
        self,
        socket_path: Path,
        admission: PriceAlertAdmission,
        *,
        trusted_web_uid: int,
        shared_gid: int,
        peer_uid: Callable[[socket.socket], int],
    ) -> None:
        self.socket_path = socket_path
        self.admission = admission
        self.trusted_web_uid = trusted_web_uid
        self.peer_uid = peer_uid
        self._bound_identity: tuple[int, int] | None = None
        parent_info = socket_path.parent.lstat()
        parent_identity = (parent_info.st_dev, parent_info.st_ino)
        super().__init__(str(socket_path), _handler_for_admission(), bind_and_activate=False)
        try:
            self.server_bind()
            self._bound_identity = _socket_identity(socket_path)
            if self._bound_identity is None:
                raise ValueError("price admission socket was replaced during bind")
            os.chown(socket_path, os.geteuid(), shared_gid, follow_symlinks=False)
            os.chmod(socket_path, 0o660, follow_symlinks=False)
            current_parent = socket_path.parent.lstat()
            if (
                _assert_endpoint(socket_path, os.geteuid(), shared_gid)[2:] != self._bound_identity
                or (current_parent.st_dev, current_parent.st_ino) != parent_identity
            ):
                raise ValueError("price admission endpoint changed during bind")
            self.server_activate()
        except Exception:
            self.server_close()
            raise

    def verify_request(self, request: socket.socket, client_address: object) -> bool:
        # socketserver calls this before a handler exists or any request byte is read.
        try:
            return self.peer_uid(request) == self.trusted_web_uid
        except Exception:
            return False

    def server_close(self) -> None:
        super().server_close()
        _unlink_matching_socket(self.socket_path, self._bound_identity)


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"invalid JSON constant {value}")


def _decode_request(body: bytes) -> tuple[PriceRuleCommand, str]:
    payload = strict_json_loads(body, parse_constant=_reject_json_constant)
    if not isinstance(payload, dict) or set(payload) != {"authenticated_owner_id", "command"}:
        raise ValueError("invalid price admission envelope")
    owner = payload["authenticated_owner_id"]
    if not isinstance(owner, str):
        raise ValueError("authenticated owner must be a string")
    owner = _OWNER_ADAPTER.validate_python(owner)
    raw = payload["command"]
    if not isinstance(raw, dict):
        raise ValueError("invalid price rule command")
    kind = raw.get("kind")
    if kind == "save_price_alert_rule":
        command = SavePriceAlertRule.model_validate(raw)
    elif kind == "set_price_alert_rule_enabled":
        command = SetPriceAlertRuleEnabled.model_validate(raw)
    elif kind == "delete_price_alert_rule":
        command = DeletePriceAlertRule.model_validate(raw)
    else:
        raise ValueError("unsupported price rule command")
    return command, owner


def _handler_for_admission() -> type[BaseHTTPRequestHandler]:
    class PriceAlertAdmissionHandler(BaseHTTPRequestHandler):
        server: PriceAlertAdmissionServer

        def do_POST(self) -> None:  # noqa: N802
            if self.path not in {_ROUTE, _LOOKUP_ROUTE, _RESUME_ROUTE}:
                self.send_error(404)
                return
            self.connection.settimeout(2.0)
            try:
                lengths = self.headers.get_all("Content-Length", [])
                content_types = self.headers.get_all("Content-Type", [])
                if (
                    len(lengths) != 1
                    or len(content_types) != 1
                    or content_types[0] != "application/json"
                    or self.headers.get("Transfer-Encoding") is not None
                    or self.headers.get("Content-Encoding") is not None
                    or not lengths[0].isdecimal()
                ):
                    raise ValueError("invalid price admission framing")
                size = int(lengths[0])
                if not 1 <= size <= _MAX_COMMAND_BYTES:
                    raise ValueError("invalid price admission body size")
                body = self.rfile.read(size)
                if len(body) != size:
                    raise ValueError("truncated price admission body")
                command, owner = _decode_request(body)
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
                    else self.server.admission.submit(command, authenticated_owner_id=owner)
                )
            except KeyError:
                self._json(404, {"error": "not_found"})
                return
            except PageControlCommandConflictError:
                self._json(409, {"error": "command_conflict"})
                return
            except ValueError:
                if self.path == _ROUTE:
                    try:
                        original = self.server.admission.lookup(
                            command, authenticated_owner_id=owner
                        )
                    except Exception:
                        original = True
                    if original is None:
                        self._json(409, {"error": "rejected"})
                        return
                self._json(503, {"error": "unavailable"})
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

    return PriceAlertAdmissionHandler


def build_price_alert_admission_server(
    admission: PriceAlertAdmission,
    *,
    socket_path: Path | None,
    trusted_web_uid: int | None = None,
    shared_gid: int | None = None,
    peer_uid: Callable[[socket.socket], int] = _peer_uid,
) -> PriceAlertAdmissionServer | None:
    if socket_path is None:
        if trusted_web_uid is not None or shared_gid is not None:
            raise ValueError("price admission requires socket, Web UID and shared GID together")
        return None
    path = Path(socket_path)
    _validate_socket_path(path)
    if (
        not _valid_uid_or_gid(trusted_web_uid)
        or not _valid_uid_or_gid(shared_gid)
        or trusted_web_uid == os.geteuid()
    ):
        raise ValueError("price admission requires a distinct trusted Web UID and shared GID")
    assert isinstance(trusted_web_uid, int) and isinstance(shared_gid, int)
    parent = path.parent
    try:
        parent.lstat()
    except FileNotFoundError:
        parent.mkdir(mode=0o710)
        os.chown(parent, os.geteuid(), shared_gid, follow_symlinks=False)
        os.chmod(parent, 0o710, follow_symlinks=False)
    info = parent.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_gid != shared_gid
        or stat.S_IMODE(info.st_mode) != 0o710
    ):
        raise ValueError("price admission directory must be owner-private and mode 0710")
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("price admission socket path already exists")
    return PriceAlertAdmissionServer(
        path,
        admission,
        trusted_web_uid=trusted_web_uid,
        shared_gid=shared_gid,
        peer_uid=peer_uid,
    )


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(
        self,
        socket_path: Path,
        *,
        timeout_seconds: float,
        expected_service_uid: int,
        shared_gid: int,
        client_uid: Callable[[], int],
    ) -> None:
        super().__init__("localhost", timeout=timeout_seconds)
        self.socket_path = socket_path
        self.expected_service_uid = expected_service_uid
        self.shared_gid = shared_gid
        self.client_uid = client_uid

    def connect(self) -> None:
        if self.client_uid() == self.expected_service_uid:
            raise OSError("price admission service UID must differ from client UID")
        before = _assert_endpoint(self.socket_path, self.expected_service_uid, self.shared_gid)
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            connection.settimeout(self.timeout)
            connection.connect(str(self.socket_path))
            if _peer_uid(connection) != self.expected_service_uid:
                raise OSError("price admission peer UID is untrusted")
            if (
                _assert_endpoint(self.socket_path, self.expected_service_uid, self.shared_gid)
                != before
            ):
                raise OSError("price admission socket changed during connection")
        except BaseException:
            connection.close()
            raise
        self.sock = connection


class PriceAlertAdmissionClient:
    """Bounded client that preserves the original command on uncertain outcomes."""

    def __init__(
        self,
        socket_path: Path,
        *,
        expected_service_uid: int,
        shared_gid: int,
        timeout_seconds: float = 1.0,
        client_uid: Callable[[], int] = os.geteuid,
    ) -> None:
        self.socket_path = Path(socket_path)
        _validate_socket_path(self.socket_path)
        if (
            timeout_seconds <= 0
            or not _valid_uid_or_gid(expected_service_uid)
            or not _valid_uid_or_gid(shared_gid)
            or expected_service_uid == client_uid()
        ):
            raise ValueError("price admission requires a distinct service UID and shared GID")
        self.expected_service_uid = expected_service_uid
        self.shared_gid = shared_gid
        self.timeout_seconds = timeout_seconds
        self.client_uid = client_uid

    def submit(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        receipt = self._request(_ROUTE, command, authenticated_owner_id=authenticated_owner_id)
        assert receipt is not None
        return receipt

    def lookup(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        return self._request(_LOOKUP_ROUTE, command, authenticated_owner_id=authenticated_owner_id)

    def resume(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        receipt = self._request(
            _RESUME_ROUTE, command, authenticated_owner_id=authenticated_owner_id
        )
        assert receipt is not None
        return receipt

    def _request(
        self, route: str, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        if type(command) not in (
            SavePriceAlertRule,
            SetPriceAlertRuleEnabled,
            DeletePriceAlertRule,
        ):
            raise TypeError("price admission requires an ownerless price rule command")
        if not isinstance(authenticated_owner_id, str):
            raise ValueError("authenticated owner must be a string")
        owner = _OWNER_ADAPTER.validate_python(authenticated_owner_id)
        body = json.dumps(
            {"authenticated_owner_id": owner, "command": command.model_dump(mode="json")},
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(body) > _MAX_COMMAND_BYTES:
            raise ValueError("price admission request exceeds bound")
        connection = _UnixHTTPConnection(
            self.socket_path,
            timeout_seconds=self.timeout_seconds,
            expected_service_uid=self.expected_service_uid,
            shared_gid=self.shared_gid,
            client_uid=self.client_uid,
        )
        try:
            connection.request(
                "POST", route, body=body, headers={"Content-Type": "application/json"}
            )
            response = connection.getresponse()
            lengths = [
                value for key, value in response.getheaders() if key.lower() == "content-length"
            ]
            types = [value for key, value in response.getheaders() if key.lower() == "content-type"]
            if (
                len(lengths) != 1
                or not lengths[0].isdecimal()
                or not 1 <= int(lengths[0]) <= _MAX_RESPONSE_BYTES
                or types != ["application/json"]
                or response.getheader("Transfer-Encoding") is not None
            ):
                raise ValueError("price admission response framing is invalid")
            received = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(received) != int(lengths[0]):
                raise ValueError("price admission response length is invalid")
            decoded = strict_json_loads(received, parse_constant=_reject_json_constant)
            if (
                response.status in (404, 409)
                and isinstance(decoded, dict)
                and set(decoded) == {"error"}
                and decoded["error"] in {"not_found", "command_conflict", "rejected"}
            ):
                raise PriceAlertAdmissionRejectedError(decoded["error"])
            if response.status != 200:
                raise PriceAlertAdmissionUnavailableError(
                    f"price admission returned HTTP {response.status}"
                )
            if route == _LOOKUP_ROUTE:
                if decoded == {"found": False}:
                    return None
                if (
                    not isinstance(decoded, dict)
                    or set(decoded) != {"found", "receipt"}
                    or decoded["found"] is not True
                ):
                    raise ValueError("price admission lookup response is invalid")
                decoded = decoded["receipt"]
            receipt = PageControlReceipt.model_validate(decoded)
            if (
                receipt.command_id != command.command_id
                or receipt.enqueued_at != command.requested_at
            ):
                raise ValueError("price admission returned another command")
            if receipt.status is PageControlStatus.SUCCEEDED:
                expected_action = (
                    "save"
                    if type(command) is SavePriceAlertRule
                    else "set_enabled"
                    if type(command) is SetPriceAlertRuleEnabled
                    else "delete"
                )
                expected_rule_id = (
                    command.rule.rule_id if type(command) is SavePriceAlertRule else command.rule_id
                )
                if (
                    not isinstance(receipt.result, Mapping)
                    or receipt.result.get("rule_id") != expected_rule_id
                    or receipt.result.get("action") != expected_action
                ):
                    raise ValueError("price admission returned mismatched effect")
            return receipt
        except PriceAlertAdmissionRejectedError:
            raise
        except (OSError, TimeoutError, http.client.HTTPException, ValueError) as exc:
            raise PriceAlertAdmissionUnavailableError(
                "price admission unavailable or invalid"
            ) from exc
        finally:
            connection.close()
