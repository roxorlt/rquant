"""Self-contained root runtime for the Daily receipt socket authority.

This module is bundled into a root-owned zipapp by
``install-runtime-credential-infra.sh``.  It deliberately imports only the
standard library so the root signer never imports a checkout, virtualenv, or
site package owned by the runtime user.
"""

from __future__ import annotations

import base64
import grp
import hashlib
import json
import os
import pwd
import re
import select
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
from contextlib import suppress
from pathlib import Path

SOCKET_ENDPOINT = Path("/run/rquant/daily-receipt-signer.sock")
KEYS_FILE = Path("/etc/rquant/daily-receipt-keys.json")
NONCE_ROOT = Path("/var/lib/rquant/daily-receipt-signer")
ALLOWED_USER = "lighthouse"
ALLOWED_GROUP = "lighthouse"
STAGE_NAMESPACE = "rquant-daily-shadow-stage-completion-receipt"
RUN_NAMESPACE = "rquant-daily-shadow-run-completion-receipt"
NAMESPACES = frozenset((STAGE_NAMESPACE, RUN_NAMESPACE))
IDENTITY_PROTOCOL = "rquant-daily-receipt-authority.identity"
IDENTITY_OPERATION = "identity"
IDENTITY_VERSION = 1
GENESIS_HASH = "0" * 64
MAX_FRAME_BYTES = 2 * 1024 * 1024
MAX_PAYLOAD_BYTES = 1024 * 1024
MAX_KEY_FILE_BYTES = 64 * 1024
MAX_NONCES = 16_384
NONCE_RETENTION_SECONDS = 7 * 24 * 60 * 60
CONNECTION_TIMEOUT_SECONDS = 10.0
MAX_CONNECTIONS = 16
KEY_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class AuthorityError(ValueError):
    """Reject unsafe signer state or an invalid socket request."""


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AuthorityError("duplicate JSON key")
        result[key] = value
    return result


def strict_canonical_json_loads(payload: bytes) -> object:
    try:
        decoded = json.loads(payload, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, AuthorityError) as exc:
        raise AuthorityError("invalid canonical JSON") from exc
    if payload != canonical_json_bytes(decoded):
        raise AuthorityError("persistent JSON is not canonical")
    return decoded


def _verify_parent_chain(path: Path, *, label: str) -> None:
    expected_owner = os.geteuid()
    for parent in reversed(path.parents):
        try:
            observed = os.stat(parent, follow_symlinks=False)
        except OSError as exc:
            raise AuthorityError(f"{label} parent is unavailable") from exc
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid not in {0, expected_owner}
            or (observed.st_mode & 0o022 and not observed.st_mode & stat.S_ISVTX)
        ):
            raise AuthorityError(f"{label} parent is unsafe")


def _read_secure_file(path: Path, *, mode: int, label: str) -> bytes:
    candidate = Path(os.path.abspath(path))
    if candidate != path:
        raise AuthorityError(f"{label} path is invalid")
    _verify_parent_chain(candidate, label=label)
    descriptor = -1
    try:
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        named = os.stat(candidate, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or stat.S_IMODE(opened.st_mode) != mode
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or not 0 < opened.st_size <= MAX_KEY_FILE_BYTES
        ):
            raise AuthorityError(f"{label} is unsafe")
        payload = os.read(descriptor, MAX_KEY_FILE_BYTES + 1)
        after = os.fstat(descriptor)
        if len(payload) != opened.st_size or (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise AuthorityError(f"{label} changed while reading")
        return payload
    except OSError as exc:
        raise AuthorityError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _ensure_private_directory(path: Path) -> None:
    candidate = Path(os.path.abspath(path))
    if candidate != path:
        raise AuthorityError("nonce state path is invalid")
    candidate.mkdir(mode=0o700, parents=True, exist_ok=True)
    _verify_parent_chain(candidate, label="nonce state")
    observed = os.stat(candidate, follow_symlinks=False)
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise AuthorityError("nonce state is unsafe")


class NonceStore:
    def __init__(self, root: Path) -> None:
        self._root = Path(os.path.abspath(root))
        self._lock = threading.Lock()
        _ensure_private_directory(self._root)

    def claim(self, nonce: str, *, envelope_hash: str) -> None:
        if SHA256.fullmatch(nonce) is None or SHA256.fullmatch(envelope_hash) is None:
            raise AuthorityError("nonce state is invalid")
        with self._lock:
            self._expire()
            if self._count() >= MAX_NONCES:
                raise AuthorityError("nonce state is full")
            path = self._root / nonce
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(path, flags, 0o600)
            except FileExistsError as exc:
                raise AuthorityError("nonce replay") from exc
            except OSError as exc:
                raise AuthorityError("nonce state failed") from exc
            try:
                os.write(
                    descriptor,
                    canonical_json_bytes(
                        {
                            "schema_version": 1,
                            "nonce": nonce,
                            "envelope_hash": envelope_hash,
                            "claimed_at_unix": int(time.time()),
                        }
                    ),
                )
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _validate_record(self, path: Path) -> os.stat_result:
        observed = os.stat(path, follow_symlinks=False)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_nlink != 1
            or SHA256.fullmatch(path.name) is None
        ):
            raise AuthorityError("nonce state is unsafe")
        return observed

    def _expire(self) -> None:
        cutoff = time.time() - NONCE_RETENTION_SECONDS
        for path in self._root.iterdir():
            observed = self._validate_record(path)
            if observed.st_mtime < cutoff:
                path.unlink()

    def _count(self) -> int:
        count = 0
        for path in self._root.iterdir():
            self._validate_record(path)
            count += 1
        return count


def _load_active_key() -> tuple[str, Path]:
    document = strict_canonical_json_loads(
        _read_secure_file(KEYS_FILE, mode=0o600, label="Daily key manifest")
    )
    expected = {
        "schema_version",
        "generation",
        "previous_manifest_hash",
        "active_key_id",
        "active_private_key_path",
        "previous_public_keys",
    }
    if (
        not isinstance(document, dict)
        or set(document) != expected
        or document.get("schema_version") != 2
        or type(document.get("generation")) is not int
        or int(document["generation"]) < 1
        or not isinstance(document.get("previous_manifest_hash"), str)
        or SHA256.fullmatch(str(document["previous_manifest_hash"])) is None
        or not isinstance(document.get("active_key_id"), str)
        or KEY_ID.fullmatch(str(document["active_key_id"])) is None
        or not isinstance(document.get("active_private_key_path"), str)
        or not isinstance(document.get("previous_public_keys"), dict)
    ):
        raise AuthorityError("Daily key manifest shape is invalid")
    generation = int(document["generation"])
    previous_hash = str(document["previous_manifest_hash"])
    previous = document["previous_public_keys"]
    if generation == 1 and (previous_hash != GENESIS_HASH or previous):
        raise AuthorityError("Daily key manifest genesis binding is invalid")
    if generation > 1 and (previous_hash == GENESIS_HASH or not previous):
        raise AuthorityError("Daily key manifest rotation binding is invalid")
    key_id = str(document["active_key_id"])
    for prior_id, public_key in previous.items():
        if (
            not isinstance(prior_id, str)
            or KEY_ID.fullmatch(prior_id) is None
            or prior_id == key_id
            or not isinstance(public_key, str)
            or not public_key
        ):
            raise AuthorityError("Daily previous key manifest is invalid")
    private_key = Path(str(document["active_private_key_path"]))
    if not private_key.is_absolute() or private_key != Path(os.path.abspath(private_key)):
        raise AuthorityError("Daily private key path is invalid")
    _read_secure_file(private_key, mode=0o600, label="Daily private key")
    return key_id, private_key


def _zipapp_path(value: str | Path) -> Path | None:
    """Resolve the outer zipapp when ``__file__`` points inside it."""

    candidate = Path(os.path.abspath(value))
    if candidate.is_file() and zipfile.is_zipfile(candidate):
        return candidate
    text = str(candidate)
    marker = ".pyz/"
    index = text.find(marker)
    if index >= 0:
        outer = Path(text[: index + len(".pyz")])
        if outer.is_file() and zipfile.is_zipfile(outer):
            return outer
    return None


def _loaded_source_sha256() -> str:
    """Hash the exact ``__main__.py`` bytes loaded by this process.

    The production artifact is a one-file zipapp.  Keeping this calculation in
    the authority process makes a stale process observable even if the
    ``current`` symlink has already moved to another release.
    """

    candidates = [Path(__file__), Path(sys.argv[0])]
    for candidate in candidates:
        archive = _zipapp_path(candidate)
        if archive is not None:
            try:
                with zipfile.ZipFile(archive) as bundle:
                    if bundle.namelist() != ["__main__.py"]:
                        raise AuthorityError("Daily authority zipapp contents are invalid")
                    source = bundle.read("__main__.py")
            except (OSError, zipfile.BadZipFile, KeyError) as exc:
                raise AuthorityError("Daily authority zipapp cannot be inspected") from exc
            return hashlib.sha256(source).hexdigest()
        if candidate.name == "__main__.py" and candidate.is_file():
            try:
                return hashlib.sha256(candidate.read_bytes()).hexdigest()
            except OSError as exc:
                raise AuthorityError("Daily authority source cannot be inspected") from exc
    raise AuthorityError("Daily authority source identity is unavailable")


def _openssl() -> str:
    candidate = Path("/usr/bin/openssl")
    try:
        observed = candidate.stat()
    except OSError as exc:
        raise AuthorityError("openssl is unavailable") from exc
    if (
        stat.S_ISREG(observed.st_mode)
        and observed.st_uid == 0
        and observed.st_nlink == 1
        and not observed.st_mode & 0o022
    ):
        return str(candidate)
    raise AuthorityError("openssl is unavailable")


def _sign(private_key: Path, payload: bytes) -> str:
    with tempfile.TemporaryDirectory(prefix="rquant-daily-receipt-sign-") as raw_root:
        root = Path(raw_root)
        root.chmod(0o700)
        payload_path = root / "payload.bin"
        payload_path.write_bytes(payload)
        payload_path.chmod(0o600)
        try:
            result = subprocess.run(
                (
                    _openssl(),
                    "pkeyutl",
                    "-sign",
                    "-rawin",
                    "-inkey",
                    str(private_key),
                    "-in",
                    str(payload_path),
                ),
                stdin=subprocess.DEVNULL,
                capture_output=True,
                check=False,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise AuthorityError("Daily Ed25519 signing failed") from exc
    if result.returncode != 0 or len(result.stdout) != 64:
        raise AuthorityError("Daily Ed25519 signing failed")
    return base64.b64encode(result.stdout).decode("ascii")


def _decode_request(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > MAX_FRAME_BYTES:
        raise AuthorityError("Daily signing request size is invalid")
    request = strict_canonical_json_loads(payload)
    expected = {
        "version",
        "operation",
        "namespace",
        "nonce",
        "payload_sha256",
        "canonical_payload",
    }
    if (
        not isinstance(request, dict)
        or set(request) != expected
        or request.get("version") != 1
        or request.get("operation") != "sign"
        or request.get("namespace") not in NAMESPACES
        or not isinstance(request.get("nonce"), str)
        or SHA256.fullmatch(str(request["nonce"])) is None
        or not isinstance(request.get("payload_sha256"), str)
        or SHA256.fullmatch(str(request["payload_sha256"])) is None
        or not isinstance(request.get("canonical_payload"), str)
    ):
        raise AuthorityError("Daily signing request shape is invalid")
    try:
        decoded = base64.b64decode(str(request["canonical_payload"]), validate=True)
    except (TypeError, ValueError) as exc:
        raise AuthorityError("Daily signing payload is invalid") from exc
    if not decoded or len(decoded) > MAX_PAYLOAD_BYTES:
        raise AuthorityError("Daily signing payload size is invalid")
    if hashlib.sha256(decoded).hexdigest() != request["payload_sha256"]:
        raise AuthorityError("Daily signing payload hash is invalid")
    return request


def _decode_identity_request(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > MAX_FRAME_BYTES:
        raise AuthorityError("Daily identity request size is invalid")
    request = strict_canonical_json_loads(payload)
    expected = {"version", "operation", "protocol", "nonce"}
    if (
        not isinstance(request, dict)
        or set(request) != expected
        or request.get("version") != IDENTITY_VERSION
        or request.get("operation") != IDENTITY_OPERATION
        or request.get("protocol") != IDENTITY_PROTOCOL
        or not isinstance(request.get("nonce"), str)
        or SHA256.fullmatch(str(request["nonce"])) is None
    ):
        raise AuthorityError("Daily identity request shape is invalid")
    return request


def _signature_envelope(
    *, namespace: str, nonce: str, payload_sha256: str, key_id: str
) -> dict[str, object]:
    if (
        namespace not in NAMESPACES
        or SHA256.fullmatch(nonce) is None
        or SHA256.fullmatch(payload_sha256) is None
        or KEY_ID.fullmatch(key_id) is None
    ):
        raise AuthorityError("Daily signing envelope is invalid")
    return {
        "version": 1,
        "operation": "sign",
        "namespace": namespace,
        "nonce": nonce,
        "payload_sha256": payload_sha256,
        "key_id": key_id,
    }


def _identity_envelope(
    *, nonce: str, source_sha256: str, key_id: str
) -> dict[str, object]:
    if (
        SHA256.fullmatch(nonce) is None
        or SHA256.fullmatch(source_sha256) is None
        or KEY_ID.fullmatch(key_id) is None
    ):
        raise AuthorityError("Daily identity envelope is invalid")
    return {
        "version": IDENTITY_VERSION,
        "operation": IDENTITY_OPERATION,
        "protocol": IDENTITY_PROTOCOL,
        "nonce": nonce,
        "source_sha256": source_sha256,
        "key_id": key_id,
    }


class Authority:
    def __init__(self) -> None:
        self._source_sha256 = _loaded_source_sha256()
        self._nonces = NonceStore(NONCE_ROOT)

    def handle(self, payload: bytes) -> bytes:
        decoded = strict_canonical_json_loads(payload)
        if isinstance(decoded, dict) and decoded.get("operation") == IDENTITY_OPERATION:
            request = _decode_identity_request(payload)
            key_id, private_key = _load_active_key()
            envelope = _identity_envelope(
                nonce=str(request["nonce"]),
                source_sha256=self._source_sha256,
                key_id=key_id,
            )
            return canonical_json_bytes(
                {
                    **envelope,
                    "signature": _sign(private_key, canonical_json_bytes(envelope)),
                }
            )
        request = _decode_request(payload)
        key_id, private_key = _load_active_key()
        canonical_payload = base64.b64decode(
            str(request["canonical_payload"]), validate=True
        )
        envelope = _signature_envelope(
            namespace=str(request["namespace"]),
            nonce=str(request["nonce"]),
            payload_sha256=str(request["payload_sha256"]),
            key_id=key_id,
        )
        self._nonces.claim(
            str(request["nonce"]),
            envelope_hash=hashlib.sha256(canonical_json_bytes(envelope)).hexdigest(),
        )
        return canonical_json_bytes(
            {
                **envelope,
                "signature": _sign(private_key, canonical_payload),
            }
        )


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    parts: list[bytes] = []
    remaining = size
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise AuthorityError("Daily socket frame is truncated")
        parts.append(chunk)
        remaining -= len(chunk)
    return b"".join(parts)


def _read_frame(connection: socket.socket) -> bytes:
    size = int.from_bytes(_recv_exact(connection, 4), "big")
    if not 0 < size <= MAX_FRAME_BYTES:
        raise AuthorityError("Daily socket frame size is invalid")
    return _recv_exact(connection, size)


def _write_frame(connection: socket.socket, payload: bytes) -> None:
    if not payload or len(payload) > MAX_FRAME_BYTES:
        raise AuthorityError("Daily socket response size is invalid")
    connection.sendall(len(payload).to_bytes(4, "big") + payload)


def _peer_credentials(connection: socket.socket) -> tuple[int, int]:
    if not sys.platform.startswith("linux") or not hasattr(socket, "SO_PEERCRED"):
        raise AuthorityError("Linux SO_PEERCRED is required")
    raw = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    _pid, uid, gid = struct.unpack("3i", raw)
    return uid, gid


def _safe_error(exc: BaseException) -> str:
    return (str(exc) or exc.__class__.__name__)[:512]


def _serve_one(
    connection: socket.socket,
    authority: Authority,
    allowed_uid: int,
    allowed_gid: int,
    slots: threading.Semaphore,
) -> None:
    try:
        with connection:
            connection.settimeout(CONNECTION_TIMEOUT_SECONDS)
            try:
                uid, gid = _peer_credentials(connection)
                if uid != allowed_uid or gid != allowed_gid:
                    raise AuthorityError("Daily receipt signer peer credentials are not allowed")
                _write_frame(connection, authority.handle(_read_frame(connection)))
            except Exception as exc:  # noqa: BLE001
                with suppress(OSError, TimeoutError, AuthorityError):
                    _write_frame(
                        connection,
                        canonical_json_bytes(
                            {"ok": False, "error": _safe_error(exc)}
                        ),
                    )
    finally:
        slots.release()


def _inherited_listener() -> socket.socket:
    try:
        listen_pid = int(os.environ.get("LISTEN_PID", "0"))
        listen_fds = int(os.environ.get("LISTEN_FDS", "0"))
    except ValueError as exc:
        raise AuthorityError("Daily socket activation is invalid") from exc
    if listen_pid != os.getpid() or listen_fds != 1:
        raise AuthorityError("Daily socket activation must provide exactly one fd")
    listener: socket.socket | None = None
    try:
        listener = socket.fromfd(3, socket.AF_UNIX, socket.SOCK_STREAM)
        if listener.family != socket.AF_UNIX:
            raise AuthorityError("Daily inherited fd is not an AF_UNIX socket")
        if listener.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM:
            raise AuthorityError("Daily inherited fd is not a stream socket")
        if listener.getsockname() != str(SOCKET_ENDPOINT):
            raise AuthorityError("Daily inherited socket endpoint mismatch")
        poller = select.poll()
        poller.register(listener.fileno(), select.POLLIN)
        return listener
    except Exception:
        if listener is not None:
            with suppress(OSError):
                listener.close()
        raise


def run() -> int:
    if len(sys.argv) != 1:
        raise AuthorityError("Daily socket authority arguments are not allowed")
    listener = _inherited_listener()
    authority = Authority()
    try:
        allowed_uid = pwd.getpwnam(ALLOWED_USER).pw_uid
        allowed_gid = grp.getgrnam(ALLOWED_GROUP).gr_gid
    except KeyError as exc:
        raise AuthorityError("Daily signer service account is unavailable") from exc
    slots = threading.Semaphore(MAX_CONNECTIONS)
    workers: list[threading.Thread] = []
    with listener:
        while True:
            connection, _address = listener.accept()
            slots.acquire()
            worker = threading.Thread(
                target=_serve_one,
                args=(connection, authority, allowed_uid, allowed_gid, slots),
                daemon=True,
            )
            worker.start()
            workers = [item for item in workers if item.is_alive()]
            workers.append(worker)


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except AuthorityError as exc:
        print(f"Daily receipt socket authority rejected request: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
