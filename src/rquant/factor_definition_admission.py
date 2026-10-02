"""Opt-in, distinct-UID Unix admission for verified factor archives."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import socket
import socketserver
import stat
import struct
import sys
import threading
from collections.abc import Callable
from ctypes import CDLL, byref, c_uint, get_errno
from http.server import BaseHTTPRequestHandler
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from rquant.factor.capability import DailyFactorCapabilities
from rquant.factor.draft import FactorSaveDraft, build_draft_definition, draft_factor_id
from rquant.factor.registry import FactorDefinitionReceipt
from rquant.manual_watchlist import OwnerId
from rquant.page_control import (
    ArchiveFactor,
    PageControlCommandConflictError,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
)
from rquant.strict_json import canonical_json_bytes, strict_json_loads

_MAX_COMMAND_BYTES = 8 * 1024
_MAX_RESPONSE_BYTES = 16 * 1024
_ROUTE = "/v1/factor-definition-admission"
_LOOKUP_ROUTE = f"{_ROUTE}/lookup"
_RESUME_ROUTE = f"{_ROUTE}/resume"
_SAVE_ROUTE = f"{_ROUTE}/save"
_SAVE_LOOKUP_ROUTE = f"{_SAVE_ROUTE}/lookup"
_SAVE_RESUME_ROUTE = f"{_SAVE_ROUTE}/resume"
_CAPABILITIES_ROUTE = f"{_ROUTE}/capabilities"
_ACTOR_ADAPTER = TypeAdapter(OwnerId)
_INSTANCE_PATTERN = re.compile(r"^[0-9a-f]{32}$")


class FactorDefinitionAdmissionUnavailableError(RuntimeError):
    """The private listener did not return a trustworthy final response."""


class FactorDefinitionAdmissionRejectedError(ValueError):
    """The listener proved that this request was rejected without a new effect."""


class FactorArchiveAdmissionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    registry_instance_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    receipt: PageControlReceipt
    definition_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class FactorDefinitionAdmission:
    """Authorize one actor and call only the owned, targeted archive paths."""

    def __init__(
        self,
        service: PageControlService,
        *,
        editor_users: frozenset[str],
        save_enabled: bool = False,
    ) -> None:
        if len(editor_users) > 16:
            raise ValueError("factor editors exceed bounded allowlist")
        for actor in editor_users:
            _ACTOR_ADAPTER.validate_python(actor)
        self.service = service
        self.editor_users = editor_users
        self.save_enabled = save_enabled
        self._lock = threading.Lock()

    def submit(
        self,
        command: ArchiveFactor,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> FactorArchiveAdmissionResult:
        self._authorize(authenticated_actor_id)
        with self._lock:
            receipt = self.service._submit_trusted_factor_archive(
                command,
                authenticated_actor_id=authenticated_actor_id,
                verified_registry_instance_id=verified_registry_instance_id,
            )
            return self._bound_result(command, authenticated_actor_id, receipt)

    def lookup(
        self, command: ArchiveFactor, *, authenticated_actor_id: str
    ) -> FactorArchiveAdmissionResult | None:
        self._authorize(authenticated_actor_id)
        with self._lock:
            receipt = self.service._lookup_trusted_factor_archive(
                command, authenticated_actor_id=authenticated_actor_id
            )
            return (
                None
                if receipt is None
                else self._bound_result(command, authenticated_actor_id, receipt)
            )

    def resume(
        self, command: ArchiveFactor, *, authenticated_actor_id: str
    ) -> FactorArchiveAdmissionResult:
        self._authorize(authenticated_actor_id)
        with self._lock:
            receipt = self.service._resume_trusted_factor_archive(
                command, authenticated_actor_id=authenticated_actor_id
            )
            return self._bound_result(command, authenticated_actor_id, receipt)

    def _bound_result(
        self, command: ArchiveFactor, actor_id: str, receipt: PageControlReceipt
    ) -> FactorArchiveAdmissionResult:
        matched = self.service.outbox.lookup_factor_archive_command(
            command, authenticated_actor_id=actor_id
        )
        if matched is None or matched[1] != receipt:
            raise RuntimeError("factor archive receipt changed during admission")
        return FactorArchiveAdmissionResult(
            registry_instance_id=matched[0].registry_identity.instance_id,
            receipt=receipt,
        )

    def _authorize(self, actor_id: str) -> None:
        if not self.editor_users or actor_id not in self.editor_users:
            raise FactorDefinitionAdmissionRejectedError("actor_forbidden")

    def submit_save(
        self,
        draft: FactorSaveDraft,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> FactorArchiveAdmissionResult:
        self._authorize(authenticated_actor_id)
        if not self.save_enabled:
            raise FactorDefinitionAdmissionRejectedError("save_disabled")
        with self._lock:
            try:
                build_draft_definition(
                    draft,
                    authenticated_actor_id=authenticated_actor_id,
                    capabilities=self.service.factor_definition_capabilities(),
                )
            except ValueError as exc:
                raise FactorDefinitionAdmissionRejectedError("invalid_draft") from exc
            try:
                receipt = self.service._submit_trusted_factor_save(
                    draft,
                    authenticated_actor_id=authenticated_actor_id,
                    verified_registry_instance_id=verified_registry_instance_id,
                )
            except PageControlCommandConflictError as exc:
                raise FactorDefinitionAdmissionRejectedError("command_conflict") from exc
            return self._bound_save_result(draft, authenticated_actor_id, receipt)

    def capabilities(self, *, authenticated_actor_id: str) -> DailyFactorCapabilities:
        self._authorize(authenticated_actor_id)
        if not self.save_enabled:
            raise FactorDefinitionAdmissionRejectedError("save_disabled")
        return self.service.factor_definition_capabilities()

    def lookup_save(
        self, draft: FactorSaveDraft, *, authenticated_actor_id: str
    ) -> FactorArchiveAdmissionResult | None:
        self._authorize(authenticated_actor_id)
        if not self.save_enabled:
            raise FactorDefinitionAdmissionRejectedError("save_disabled")
        with self._lock:
            try:
                receipt = self.service._lookup_trusted_factor_save(
                    draft, authenticated_actor_id=authenticated_actor_id
                )
            except PageControlCommandConflictError as exc:
                raise FactorDefinitionAdmissionRejectedError("command_conflict") from exc
            return (
                None
                if receipt is None
                else self._bound_save_result(draft, authenticated_actor_id, receipt)
            )

    def resume_save(
        self, draft: FactorSaveDraft, *, authenticated_actor_id: str
    ) -> FactorArchiveAdmissionResult:
        self._authorize(authenticated_actor_id)
        if not self.save_enabled:
            raise FactorDefinitionAdmissionRejectedError("save_disabled")
        with self._lock:
            try:
                receipt = self.service._resume_trusted_factor_save(
                    draft, authenticated_actor_id=authenticated_actor_id
                )
            except PageControlCommandConflictError as exc:
                raise FactorDefinitionAdmissionRejectedError("command_conflict") from exc
            return self._bound_save_result(draft, authenticated_actor_id, receipt)

    def _bound_save_result(
        self, draft: FactorSaveDraft, actor_id: str, receipt: PageControlReceipt
    ) -> FactorArchiveAdmissionResult:
        matched = self.service.outbox.lookup_factor_save_command(
            draft, authenticated_actor_id=actor_id
        )
        if matched is None or matched[1] != receipt:
            raise RuntimeError("factor save receipt changed during admission")
        owned = matched[0]
        digest = hashlib.sha256(
            canonical_json_bytes(owned.definition.model_dump(mode="json", round_trip=True))
        ).hexdigest()
        if receipt.status is PageControlStatus.SUCCEEDED:
            effect = FactorDefinitionReceipt.model_validate(receipt.result)
            if (
                effect.command_id != owned.command_id
                or effect.action != "save"
                or effect.factor_id != owned.definition.factor_id
                or effect.version != owned.definition.version
                or effect.content_sha256 != digest
                or effect.archived
                or receipt.completed_at is None
            ):
                raise RuntimeError("factor save effect differs from original definition")
        assert owned.registry_identity is not None
        return FactorArchiveAdmissionResult(
            registry_instance_id=owned.registry_identity.instance_id,
            receipt=receipt,
            definition_sha256=digest,
        )


def _public_result(
    result: FactorArchiveAdmissionResult, *, action: str = "archive"
) -> dict[str, object]:
    receipt = result.receipt
    if receipt.status in {PageControlStatus.FAILED, PageControlStatus.AMBIGUOUS}:
        receipt = receipt.model_copy(
            update={"result": None, "error": "保存未完成" if action == "save" else "归档未完成"}
        )
    return {
        "registry_instance_id": result.registry_instance_id,
        "receipt": receipt.model_dump(mode="json"),
        "definition_sha256": result.definition_sha256,
    }


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
        raise ValueError("factor archive admission socket path must be short and absolute")
    current = Path(path.anchor)
    for component in path.parts[1:-1]:
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise ValueError("factor archive admission path contains a symlink")


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
        raise ValueError("factor archive admission endpoint identity or permissions are unsafe")
    return parent.st_dev, parent.st_ino, endpoint.st_dev, endpoint.st_ino


class FactorDefinitionAdmissionServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(
        self,
        socket_path: Path,
        admission: FactorDefinitionAdmission,
        *,
        trusted_web_uid: int,
        shared_gid: int,
        peer_uid: Callable[[socket.socket], int],
        handler_type: type[BaseHTTPRequestHandler] | None = None,
    ) -> None:
        self.socket_path = socket_path
        self.admission = admission
        self.trusted_web_uid = trusted_web_uid
        self.peer_uid = peer_uid
        self._bound_identity: tuple[int, int] | None = None
        parent_info = socket_path.parent.lstat()
        parent_identity = (parent_info.st_dev, parent_info.st_ino)
        super().__init__(
            str(socket_path), handler_type or _handler_for_admission(), bind_and_activate=False
        )
        try:
            self.server_bind()
            self._bound_identity = _socket_identity(socket_path)
            if self._bound_identity is None:
                raise ValueError("factor archive admission socket was replaced during bind")
            os.chown(socket_path, os.geteuid(), shared_gid, follow_symlinks=False)
            os.chmod(socket_path, 0o660, follow_symlinks=False)
            current_parent = socket_path.parent.lstat()
            if (
                _assert_endpoint(socket_path, os.geteuid(), shared_gid)[2:] != self._bound_identity
                or (current_parent.st_dev, current_parent.st_ino) != parent_identity
            ):
                raise ValueError("factor archive admission endpoint changed during bind")
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


def _decode_request(body: bytes, *, submit: bool) -> tuple[ArchiveFactor, str, str | None]:
    payload = strict_json_loads(body, parse_constant=_reject_json_constant)
    fields = {"authenticated_actor_id", "command"}
    if submit:
        fields.add("verified_registry_instance_id")
    if not isinstance(payload, dict) or set(payload) != fields:
        raise ValueError("invalid factor archive admission envelope")
    actor = payload["authenticated_actor_id"]
    if not isinstance(actor, str):
        raise ValueError("authenticated actor must be a string")
    actor = _ACTOR_ADAPTER.validate_python(actor)
    raw = payload["command"]
    if not isinstance(raw, dict):
        raise ValueError("invalid factor archive command")
    if raw.get("kind") != "archive_factor" or "actor_id" in raw:
        raise ValueError("unsupported factor archive command")
    command = ArchiveFactor.model_validate(raw)
    instance_id = payload.get("verified_registry_instance_id")
    if submit and (
        not isinstance(instance_id, str) or _INSTANCE_PATTERN.fullmatch(instance_id) is None
    ):
        raise ValueError("invalid verified factor registry instance")
    return command, actor, instance_id


def _decode_save_request(body: bytes, *, submit: bool) -> tuple[FactorSaveDraft, str, str | None]:
    payload = strict_json_loads(body, parse_constant=_reject_json_constant)
    fields = {"authenticated_actor_id", "draft"}
    if submit:
        fields.add("verified_registry_instance_id")
    if not isinstance(payload, dict) or set(payload) != fields:
        raise ValueError("invalid factor save admission envelope")
    actor = _ACTOR_ADAPTER.validate_python(payload["authenticated_actor_id"])
    if not isinstance(payload["draft"], dict):
        raise ValueError("factor save draft must be an object")
    draft = FactorSaveDraft.model_validate_json(
        json.dumps(payload["draft"], ensure_ascii=True, allow_nan=False)
    )
    instance_id = payload.get("verified_registry_instance_id")
    if submit and (
        not isinstance(instance_id, str) or _INSTANCE_PATTERN.fullmatch(instance_id) is None
    ):
        raise ValueError("invalid verified factor registry instance")
    return draft, actor, instance_id


def _handler_for_admission() -> type[BaseHTTPRequestHandler]:
    class FactorDefinitionAdmissionHandler(BaseHTTPRequestHandler):
        server: FactorDefinitionAdmissionServer

        def do_POST(self) -> None:  # noqa: N802
            save_route = self.path in {_SAVE_ROUTE, _SAVE_LOOKUP_ROUTE, _SAVE_RESUME_ROUTE}
            capability_route = self.path == _CAPABILITIES_ROUTE
            if self.path not in {
                _ROUTE,
                _LOOKUP_ROUTE,
                _RESUME_ROUTE,
                _SAVE_ROUTE,
                _SAVE_LOOKUP_ROUTE,
                _SAVE_RESUME_ROUTE,
                _CAPABILITIES_ROUTE,
            }:
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
                    raise ValueError("invalid factor archive admission framing")
                size = int(lengths[0])
                if not 1 <= size <= _MAX_COMMAND_BYTES:
                    raise ValueError("invalid factor archive admission body size")
                body = self.rfile.read(size)
                if len(body) != size:
                    raise ValueError("truncated factor archive admission body")
                if capability_route:
                    envelope = strict_json_loads(body, parse_constant=_reject_json_constant)
                    if not isinstance(envelope, dict) or set(envelope) != {
                        "authenticated_actor_id"
                    }:
                        raise ValueError("invalid factor capability envelope")
                    actor = _ACTOR_ADAPTER.validate_python(envelope["authenticated_actor_id"])
                elif save_route:
                    command, actor, instance_id = _decode_save_request(
                        body, submit=self.path == _SAVE_ROUTE
                    )
                else:
                    command, actor, instance_id = _decode_request(body, submit=self.path == _ROUTE)
            except (OSError, TypeError, ValueError):
                self._json(400, {"error": "invalid_command"})
                return
            try:
                if capability_route:
                    capabilities = self.server.admission.capabilities(authenticated_actor_id=actor)
                    self._json(200, capabilities.model_dump(mode="json"))
                    return
                if save_route:
                    assert isinstance(command, FactorSaveDraft)
                    if self.path == _SAVE_LOOKUP_ROUTE:
                        saved = self.server.admission.lookup_save(
                            command, authenticated_actor_id=actor
                        )
                        self._json(
                            200,
                            {"found": False}
                            if saved is None
                            else {"found": True, "result": _public_result(saved, action="save")},
                        )
                        return
                    saved = (
                        self.server.admission.resume_save(command, authenticated_actor_id=actor)
                        if self.path == _SAVE_RESUME_ROUTE
                        else self.server.admission.submit_save(
                            command,
                            authenticated_actor_id=actor,
                            verified_registry_instance_id=instance_id,
                        )
                    )
                    self._json(200, _public_result(saved, action="save"))
                    return
                if self.path == _LOOKUP_ROUTE:
                    receipt = self.server.admission.lookup(command, authenticated_actor_id=actor)
                    self._json(
                        200,
                        {"found": False}
                        if receipt is None
                        else {"found": True, "result": _public_result(receipt)},
                    )
                    return
                receipt = (
                    self.server.admission.resume(command, authenticated_actor_id=actor)
                    if self.path == _RESUME_ROUTE
                    else self.server.admission.submit(
                        command,
                        authenticated_actor_id=actor,
                        verified_registry_instance_id=instance_id,
                    )
                )
            except KeyError:
                self._json(404, {"error": "not_found"})
                return
            except PageControlCommandConflictError:
                self._json(409, {"error": "command_conflict"})
                return
            except FactorDefinitionAdmissionRejectedError as exc:
                reason = str(exc)
                if reason in {"actor_forbidden", "save_disabled"}:
                    self._json(403, {"error": "actor_forbidden"})
                else:
                    self._json(409, {"error": "invalid_draft"})
                return
            except ValueError:
                if self.path == _ROUTE:
                    try:
                        original = self.server.admission.lookup(
                            command, authenticated_actor_id=actor
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
            self._json(200, _public_result(receipt))

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

    return FactorDefinitionAdmissionHandler


def build_factor_definition_admission_server(
    admission: FactorDefinitionAdmission,
    *,
    socket_path: Path | None,
    trusted_web_uid: int | None = None,
    shared_gid: int | None = None,
    peer_uid: Callable[[socket.socket], int] = _peer_uid,
    _handler_type: type[BaseHTTPRequestHandler] | None = None,
) -> FactorDefinitionAdmissionServer | None:
    if not admission.editor_users:
        return None
    if socket_path is None:
        if trusted_web_uid is not None or shared_gid is not None:
            raise ValueError(
                "factor archive admission requires socket, Web UID and shared GID together"
            )
        return None
    path = Path(socket_path)
    _validate_socket_path(path)
    if (
        not _valid_uid_or_gid(trusted_web_uid)
        or not _valid_uid_or_gid(shared_gid)
        or trusted_web_uid == os.geteuid()
    ):
        raise ValueError(
            "factor archive admission requires a distinct trusted Web UID and shared GID"
        )
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
        raise ValueError("factor archive admission directory must be owner-private and mode 0710")
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("factor archive admission socket path already exists")
    return FactorDefinitionAdmissionServer(
        path,
        admission,
        trusted_web_uid=trusted_web_uid,
        shared_gid=shared_gid,
        peer_uid=peer_uid,
        handler_type=_handler_type,
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
            raise OSError("factor archive admission service UID must differ from client UID")
        before = _assert_endpoint(self.socket_path, self.expected_service_uid, self.shared_gid)
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            connection.settimeout(self.timeout)
            connection.connect(str(self.socket_path))
            if _peer_uid(connection) != self.expected_service_uid:
                raise OSError("factor archive admission peer UID is untrusted")
            if (
                _assert_endpoint(self.socket_path, self.expected_service_uid, self.shared_gid)
                != before
            ):
                raise OSError("factor archive admission socket changed during connection")
        except BaseException:
            connection.close()
            raise
        self.sock = connection


class FactorDefinitionAdmissionClient:
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
            raise ValueError(
                "factor archive admission requires a distinct service UID and shared GID"
            )
        self.expected_service_uid = expected_service_uid
        self.shared_gid = shared_gid
        self.timeout_seconds = timeout_seconds
        self.client_uid = client_uid

    def submit(
        self,
        command: ArchiveFactor,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> FactorArchiveAdmissionResult:
        receipt = self._request(
            _ROUTE,
            command,
            authenticated_actor_id=authenticated_actor_id,
            verified_registry_instance_id=verified_registry_instance_id,
        )
        assert receipt is not None
        return receipt

    def capabilities(self, *, authenticated_actor_id: str) -> DailyFactorCapabilities:
        result = self._request(
            _CAPABILITIES_ROUTE, None, authenticated_actor_id=authenticated_actor_id
        )
        assert isinstance(result, DailyFactorCapabilities)
        return result

    def submit_save(
        self,
        draft: FactorSaveDraft,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str,
    ) -> FactorArchiveAdmissionResult:
        result = self._request(
            _SAVE_ROUTE,
            draft,
            authenticated_actor_id=authenticated_actor_id,
            verified_registry_instance_id=verified_registry_instance_id,
        )
        assert result is not None
        return result

    def lookup_save(
        self, draft: FactorSaveDraft, *, authenticated_actor_id: str
    ) -> FactorArchiveAdmissionResult | None:
        return self._request(
            _SAVE_LOOKUP_ROUTE, draft, authenticated_actor_id=authenticated_actor_id
        )

    def resume_save(
        self, draft: FactorSaveDraft, *, authenticated_actor_id: str
    ) -> FactorArchiveAdmissionResult:
        result = self._request(
            _SAVE_RESUME_ROUTE, draft, authenticated_actor_id=authenticated_actor_id
        )
        assert result is not None
        return result

    def lookup(
        self, command: ArchiveFactor, *, authenticated_actor_id: str
    ) -> FactorArchiveAdmissionResult | None:
        return self._request(_LOOKUP_ROUTE, command, authenticated_actor_id=authenticated_actor_id)

    def resume(
        self, command: ArchiveFactor, *, authenticated_actor_id: str
    ) -> FactorArchiveAdmissionResult:
        receipt = self._request(
            _RESUME_ROUTE, command, authenticated_actor_id=authenticated_actor_id
        )
        assert receipt is not None
        return receipt

    def _request(
        self,
        route: str,
        command: ArchiveFactor | FactorSaveDraft | None,
        *,
        authenticated_actor_id: str,
        verified_registry_instance_id: str | None = None,
    ) -> FactorArchiveAdmissionResult | DailyFactorCapabilities | None:
        save_route = route in {_SAVE_ROUTE, _SAVE_LOOKUP_ROUTE, _SAVE_RESUME_ROUTE}
        capability_route = route == _CAPABILITIES_ROUTE
        if save_route and type(command) is not FactorSaveDraft:
            raise TypeError("factor save admission requires a typed original draft")
        if not save_route and not capability_route and type(command) is not ArchiveFactor:
            raise TypeError("factor archive admission requires an ownerless archive command")
        if not isinstance(authenticated_actor_id, str):
            raise ValueError("authenticated actor must be a string")
        actor = _ACTOR_ADAPTER.validate_python(authenticated_actor_id)
        envelope: dict[str, object] = {"authenticated_actor_id": actor}
        if not capability_route:
            envelope["draft" if save_route else "command"] = command.model_dump(mode="json")
        if route in {_ROUTE, _SAVE_ROUTE}:
            if (
                not isinstance(verified_registry_instance_id, str)
                or _INSTANCE_PATTERN.fullmatch(verified_registry_instance_id) is None
            ):
                raise ValueError("verified factor registry instance is required")
            envelope["verified_registry_instance_id"] = verified_registry_instance_id
        body = json.dumps(
            envelope,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(body) > _MAX_COMMAND_BYTES:
            raise ValueError("factor archive admission request exceeds bound")
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
                raise ValueError("factor archive admission response framing is invalid")
            received = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(received) != int(lengths[0]):
                raise ValueError("factor archive admission response length is invalid")
            decoded = strict_json_loads(received, parse_constant=_reject_json_constant)
            if (
                response.status in (404, 409)
                and isinstance(decoded, dict)
                and set(decoded) == {"error"}
                and decoded["error"]
                in {"not_found", "command_conflict", "rejected", "invalid_draft"}
            ):
                raise FactorDefinitionAdmissionRejectedError(decoded["error"])
            if response.status == 403 and decoded == {"error": "actor_forbidden"}:
                raise FactorDefinitionAdmissionRejectedError("actor_forbidden")
            if response.status != 200:
                raise FactorDefinitionAdmissionUnavailableError(
                    f"factor archive admission returned HTTP {response.status}"
                )
            if capability_route:
                return DailyFactorCapabilities.model_validate_json(received)
            if route in {_LOOKUP_ROUTE, _SAVE_LOOKUP_ROUTE}:
                if decoded == {"found": False}:
                    return None
                if (
                    not isinstance(decoded, dict)
                    or set(decoded) != {"found", "result"}
                    or decoded["found"] is not True
                ):
                    raise ValueError("factor archive admission lookup response is invalid")
                decoded = decoded["result"]
            result = FactorArchiveAdmissionResult.model_validate(decoded)
            receipt = result.receipt
            if (
                receipt.command_id != command.command_id
                or receipt.enqueued_at != command.requested_at
                or (
                    route in {_ROUTE, _SAVE_ROUTE}
                    and result.registry_instance_id != verified_registry_instance_id
                )
            ):
                raise ValueError("factor archive admission returned another command")
            if receipt.status is PageControlStatus.SUCCEEDED:
                effect = FactorDefinitionReceipt.model_validate(receipt.result)
                if save_route:
                    assert isinstance(command, FactorSaveDraft)
                    expected_id = draft_factor_id(command, authenticated_actor_id=actor)
                    expected_version = (
                        1 if command.expected_head is None else command.expected_head.version + 1
                    )
                    mismatched = (
                        effect.action != "save"
                        or effect.factor_id != expected_id
                        or effect.version != expected_version
                        or effect.content_sha256 != result.definition_sha256
                        or effect.archived
                    )
                else:
                    assert isinstance(command, ArchiveFactor)
                    mismatched = (
                        effect.action != "archive"
                        or effect.factor_id != command.factor_id
                        or effect.version != command.expected_head.version
                        or effect.content_sha256 != command.expected_head.content_sha256
                        or effect.archived is not True
                    )
                if (
                    effect.command_id != command.command_id
                    or mismatched
                    or receipt.completed_at is None
                ):
                    raise ValueError("factor admission returned mismatched effect")
            elif receipt.status in {PageControlStatus.FAILED, PageControlStatus.AMBIGUOUS}:
                if receipt.completed_at is None:
                    raise ValueError("factor archive terminal receipt lacks completion time")
            return result
        except FactorDefinitionAdmissionRejectedError:
            raise
        except (OSError, TimeoutError, http.client.HTTPException, ValueError) as exc:
            raise FactorDefinitionAdmissionUnavailableError(
                "factor archive admission unavailable or invalid"
            ) from exc
        finally:
            connection.close()
