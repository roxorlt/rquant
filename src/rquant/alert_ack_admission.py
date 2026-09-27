"""Private Unix socket admission for new PageControl alert acknowledgments."""

from __future__ import annotations

import http.client
import json
import os
import socket
import socketserver
import stat
import struct
import sys
from collections.abc import Callable
from ctypes import CDLL, byref, c_uint, get_errno
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

from rquant.alert_ack_read import read_alert_ack
from rquant.page_control import AckAlert, PageControlReceipt, PageControlService
from rquant.serving_publisher import ServingReader

_MAX_BODY_BYTES = 1024 * 1024
_DEFAULT_STALE_AFTER = timedelta(minutes=10)


class AckAdmission:
    """Recheck a new command against the current immutable Serving generation."""

    def __init__(
        self,
        service: PageControlService,
        serving_root: Path,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        stale_after: timedelta = _DEFAULT_STALE_AFTER,
        reader_factory: Callable[[Path], ServingReader] = ServingReader,
        before_final_pointer_check: Callable[[], None] | None = None,
        after_final_pointer_check: Callable[[], None] | None = None,
    ) -> None:
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        self.service = service
        self.serving_root = Path(serving_root)
        self.clock = clock
        self.stale_after = stale_after
        self.reader_factory = reader_factory
        self.before_final_pointer_check = before_final_pointer_check
        self.after_final_pointer_check = after_final_pointer_check

    def admit(self, command: AckAlert) -> PageControlReceipt:
        original = self.service.lookup_ack_command(command)
        if original is not None:
            return original
        reader = self.reader_factory(self.serving_root)
        with reader.acquire_generation() as lease:
            if lease.pointer is None or lease.manifest.generation_id != command.generation_id:
                raise ValueError("Serving generation changed")
            now = self.clock()
            if now.tzinfo is None or now.utcoffset() is None:
                raise ValueError("admission clock must be timezone-aware")
            now = now.astimezone(UTC)
            built_at = lease.manifest.built_at.astimezone(UTC)
            serving_ready = built_at <= now and now - built_at <= self.stale_after
            cursor = lease.connection.cursor()
            try:
                generation = _AdmissionGeneration(lease.manifest, cursor)
                alerts = read_alert_ack(
                    generation,
                    serving_ready=serving_ready,
                    now=now,
                    stale_after=self.stale_after,
                )
            finally:
                cursor.close()
            if (
                alerts.summary.state != "ready"
                or alerts.activated_at is None
                or not any(
                    event.alert_id == command.alert_id
                    and alerts.is_eligible(event.source, event.alert_id)
                    for event in alerts.events.values()
                )
            ):
                raise ValueError("alert is not eligible in complete Serving sources")
            if self.service.outbox.alert_ack_activated_at() != alerts.activated_at:
                raise ValueError("alert activation differs from the Serving projection")
            if self.before_final_pointer_check is not None:
                self.before_final_pointer_check()
            # This is the final Serving observation and the admission decision point.
            current = reader.current_pointer()
            if (
                current.generation_id != command.generation_id
                or current.manifest_sha256 != lease.pointer.manifest_sha256
            ):
                raise ValueError("Serving generation changed before admission")
            if self.after_final_pointer_check is not None:
                self.after_final_pointer_check()
            return self.service._submit_verified_ack(command)


class _AdmissionGeneration:
    def __init__(self, manifest: Any, cursor: Any) -> None:
        self.manifest = manifest
        self.cursor = cursor


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


class AckAdmissionServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(
        self,
        socket_path: Path,
        admission: AckAdmission,
        *,
        trusted_uid: int,
        peer_uid: Callable[[socket.socket], int],
    ) -> None:
        self.socket_path = socket_path
        self.admission = admission
        self.trusted_uid = trusted_uid
        self.peer_uid = peer_uid
        super().__init__(str(socket_path), _handler_for_admission())
        try:
            os.chmod(socket_path, 0o600, follow_symlinks=False)
            info = socket_path.lstat()
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid():
                raise ValueError("ack admission socket is unsafe")
            if stat.S_IMODE(info.st_mode) != 0o600:
                raise ValueError("ack admission socket permissions are unsafe")
            self._socket_identity = (info.st_dev, info.st_ino)
        except Exception:
            super().server_close()
            socket_path.unlink(missing_ok=True)
            raise

    def verify_request(self, request: socket.socket, client_address: object) -> bool:
        try:
            return self.peer_uid(request) == self.trusted_uid
        except Exception:
            return False

    def server_close(self) -> None:
        super().server_close()
        try:
            info = self.socket_path.lstat()
        except FileNotFoundError:
            return
        if (info.st_dev, info.st_ino) == self._socket_identity and stat.S_ISSOCK(info.st_mode):
            self.socket_path.unlink()


def _handler_for_admission() -> type[BaseHTTPRequestHandler]:
    class AckAdmissionHandler(BaseHTTPRequestHandler):
        server: AckAdmissionServer

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/v1/ack-admission":
                self.send_error(404)
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 1 <= size <= _MAX_BODY_BYTES:
                    raise ValueError("invalid body length")
                payload = json.loads(self.rfile.read(size))
                command = AckAlert.model_validate(payload)
            except Exception:
                self._json(400, {"error": "invalid acknowledgment command"})
                return
            try:
                receipt = self.server.admission.admit(command)
            except ValueError:
                self._json(409, {"error": "acknowledgment is not eligible or conflicts"})
                return
            except Exception:
                self._json(503, {"error": "acknowledgment admission is unavailable"})
                return
            self._json(200, receipt.model_dump(mode="json"))

        def log_message(self, format: str, *args: object) -> None:
            return

        def _json(self, status: int, payload: object) -> None:
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return AckAdmissionHandler


def build_ack_admission_server(
    admission: AckAdmission,
    *,
    socket_path: Path | None,
    trusted_uid: int | None = None,
    peer_uid: Callable[[socket.socket], int] = _peer_uid,
) -> AckAdmissionServer | None:
    """Return no listener until an explicit private socket path is configured."""
    if socket_path is None:
        return None
    path = Path(socket_path)
    if not path.is_absolute():
        raise ValueError("ack admission socket path must be absolute")
    if path.name in {"", ".", ".."} or len(os.fsencode(path)) >= 100:
        raise ValueError("ack admission socket path is invalid")
    parent = path.parent
    if not parent.exists():
        parent.mkdir(mode=0o700)
    info = parent.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("ack admission directory must be owned and mode 0700")
    if path.exists() or path.is_symlink():
        raise ValueError("ack admission socket path already exists")
    uid = os.geteuid() if trusted_uid is None else trusted_uid
    if uid != os.geteuid():
        raise ValueError("ack admission trusted UID must be the service UID")
    return AckAdmissionServer(path, admission, trusted_uid=uid, peer_uid=peer_uid)


class AckAdmissionUnavailableError(RuntimeError):
    """The private PageControl admission listener did not return a usable receipt."""


class AckAdmissionRejectedError(ValueError):
    """The current published generation does not admit this new command."""


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: Path, *, timeout_seconds: float) -> None:
        super().__init__("localhost", timeout=timeout_seconds)
        self.socket_path = socket_path

    def connect(self) -> None:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self.timeout)
        connection.connect(str(self.socket_path))
        self.sock = connection


class AckAdmissionClient:
    """Typed client for the next Web integration slice; no fallback to TCP writes."""

    def __init__(self, socket_path: Path, *, timeout_seconds: float = 1.0) -> None:
        self.socket_path = Path(socket_path)
        self.timeout_seconds = timeout_seconds

    def submit(self, command: AckAlert) -> PageControlReceipt:
        connection = _UnixHTTPConnection(self.socket_path, timeout_seconds=self.timeout_seconds)
        try:
            connection.request(
                "POST",
                "/v1/ack-admission",
                body=command.model_dump_json(),
                headers={"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            body = response.read(_MAX_BODY_BYTES + 1)
            if response.status == 409:
                raise AckAdmissionRejectedError("acknowledgment is not eligible or conflicts")
            if response.status != 200:
                raise AckAdmissionUnavailableError(f"ack admission returned HTTP {response.status}")
            if len(body) > _MAX_BODY_BYTES:
                raise AckAdmissionUnavailableError("ack admission response exceeds bound")
            receipt = PageControlReceipt.model_validate_json(body)
            if receipt.command_id != command.command_id:
                raise AckAdmissionUnavailableError("ack admission returned another command")
            return receipt
        except AckAdmissionRejectedError:
            raise
        except (OSError, TimeoutError, http.client.HTTPException, ValueError) as exc:
            raise AckAdmissionUnavailableError("ack admission unavailable or invalid") from exc
        finally:
            connection.close()
