"""Root-owned Unix-socket authority for Daily Ed25519 receipt signing."""

from __future__ import annotations

import base64
import grp
import hashlib
import os
import pwd
import re
import select
import shutil
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import MappingProxyType
from typing import Literal

from pydantic import Field

from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_shadow_validation import (
    _valid_ed25519_signature,
    _verify_ed25519_signature,
)
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    strict_canonical_json_loads,
)

DAILY_RECEIPT_SOCKET_ENDPOINT = Path("/run/rquant/daily-receipt-signer.sock")
DAILY_RECEIPT_KEYS_FILE = Path("/etc/rquant/daily-receipt-keys.json")
DAILY_RECEIPT_TRUSTED_KEYRING_PATH = Path("/etc/rquant/daily-receipt-trusted-keys.json")
DAILY_RECEIPT_SIGNER_STATE_ROOT = Path("/var/lib/rquant/daily-receipt-signer")
DAILY_RECEIPT_ALLOWED_USER = "lighthouse"
DAILY_RECEIPT_ALLOWED_GROUP = "lighthouse"
DAILY_RECEIPT_NAMESPACE_STAGE = "rquant-daily-shadow-stage-completion-receipt"
DAILY_RECEIPT_NAMESPACE_RUN = "rquant-daily-shadow-run-completion-receipt"
DAILY_RECEIPT_NAMESPACES = frozenset(
    {DAILY_RECEIPT_NAMESPACE_STAGE, DAILY_RECEIPT_NAMESPACE_RUN}
)
DAILY_RECEIPT_IDENTITY_PROTOCOL = "rquant-daily-receipt-authority.identity"
DAILY_RECEIPT_IDENTITY_OPERATION = "identity"
DAILY_RECEIPT_IDENTITY_VERSION = 1

_GENESIS_MANIFEST_HASH = "0" * 64
_KEY_ID = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_FRAME_BYTES = 2 * 1024 * 1024
_MAX_PAYLOAD_BYTES = 1024 * 1024
_MAX_KEY_FILE_BYTES = 64 * 1024
_MAX_NONCES = 16_384
_NONCE_RETENTION_SECONDS = 7 * 24 * 60 * 60
_FRAME_HEADER_BYTES = 4
_ED25519_SIGNATURE_BYTES = 64
_CONNECTION_TIMEOUT_SECONDS = 10.0
_MAX_CONCURRENT_CONNECTIONS = 16


class DailyReceiptSocketAuthorityError(ValueError):
    """Malformed Daily signer socket request or unsafe authority state."""


class DailyReceiptSocketSigningResponse(RuntimeContractModel):
    version: Literal[1] = 1
    operation: Literal["sign"] = "sign"
    namespace: str = Field(min_length=1, max_length=256)
    nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    signature: str = Field(min_length=1, max_length=16_384)


class DailyReceiptSocketIdentityResponse(RuntimeContractModel):
    version: Literal[1] = 1
    operation: Literal["identity"] = "identity"
    protocol: Literal["rquant-daily-receipt-authority.identity"] = (
        DAILY_RECEIPT_IDENTITY_PROTOCOL
    )
    nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    signature: str = Field(min_length=1, max_length=16_384)


class DailyReceiptTrustedKeyring(RuntimeContractModel):
    active_key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    active_public_key_pem: str = Field(min_length=1, max_length=16_384)
    previous_public_key_pems: dict[str, str] = Field(default_factory=dict)


class DailyReceiptSocketSigningClient:
    """Fixed Daily socket client; production callers cannot choose helper argv or env."""

    def __init__(
        self,
        *,
        socket_path: Path,
        active_key_id: str,
        active_public_key_pem: str,
        previous_public_key_pems: dict[str, str] | None = None,
        timeout_seconds: float,
        max_attempts: int = 3,
        _test_endpoint: bool = False,
    ) -> None:
        normalized_socket = Path(os.path.abspath(socket_path))
        if (
            not _test_endpoint
            and normalized_socket != DAILY_RECEIPT_SOCKET_ENDPOINT
        ):
            raise ValueError("Daily receipt socket endpoint must be fixed")
        if not active_key_id or _KEY_ID.fullmatch(active_key_id) is None:
            raise ValueError("Daily receipt active key id is invalid")
        if not active_public_key_pem:
            raise ValueError("Daily receipt active public key is invalid")
        previous = dict(sorted((previous_public_key_pems or {}).items()))
        if active_key_id in previous:
            raise ValueError("Daily receipt active key cannot also be previous")
        if any(
            _KEY_ID.fullmatch(key_id) is None or not public
            for key_id, public in previous.items()
        ):
            raise ValueError("Daily receipt previous public keyring is invalid")
        if not 0.1 <= timeout_seconds <= 30:
            raise ValueError("Daily receipt socket timeout is invalid")
        if not 1 <= max_attempts <= 10:
            raise ValueError("Daily receipt socket retry budget is invalid")
        self._socket_path = normalized_socket
        self.key_id = active_key_id
        self._active_public_key = active_public_key_pem.encode("utf-8")
        self._previous_public_keys = MappingProxyType(
            {key_id: public_key.encode("utf-8") for key_id, public_key in previous.items()}
        )
        self.timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts

    @classmethod
    def production(
        cls,
        *,
        active_key_id: str,
        active_public_key_pem: str,
        previous_public_key_pems: dict[str, str] | None = None,
        timeout_seconds: float,
        max_attempts: int = 3,
    ) -> DailyReceiptSocketSigningClient:
        return cls(
            socket_path=DAILY_RECEIPT_SOCKET_ENDPOINT,
            active_key_id=active_key_id,
            active_public_key_pem=active_public_key_pem,
            previous_public_key_pems=previous_public_key_pems,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )

    @classmethod
    def for_test_endpoint(
        cls,
        *,
        socket_path: Path,
        active_key_id: str,
        active_public_key_pem: str,
        previous_public_key_pems: dict[str, str] | None = None,
        timeout_seconds: float,
        max_attempts: int = 3,
    ) -> DailyReceiptSocketSigningClient:
        return cls(
            socket_path=socket_path,
            active_key_id=active_key_id,
            active_public_key_pem=active_public_key_pem,
            previous_public_key_pems=previous_public_key_pems,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            _test_endpoint=True,
        )

    @property
    def socket_path(self) -> Path:
        return self._socket_path

    def sign(self, *, namespace: str, payload: bytes) -> str:
        if namespace not in DAILY_RECEIPT_NAMESPACES:
            raise ValueError("Daily receipt signer namespace is not allowed")
        if not payload or len(payload) > _MAX_PAYLOAD_BYTES:
            raise ValueError("Daily receipt signer payload is invalid")
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        request = {
            "version": 1,
            "operation": "sign",
            "namespace": namespace,
            "nonce": os.urandom(32).hex(),
            "payload_sha256": payload_sha256,
            "canonical_payload": base64.b64encode(payload).decode("ascii"),
        }
        response = self._request(canonical_json_bytes(request))
        verified = DailyReceiptSocketSigningResponse.model_validate(response)
        if (
            verified.namespace != namespace
            or verified.nonce != request["nonce"]
            or verified.payload_sha256 != payload_sha256
            or verified.key_id != self.key_id
            or not _valid_ed25519_signature(verified.signature)
            or not _verify_ed25519_signature(
                public_key=self._active_public_key,
                payload=payload,
                signature=verified.signature,
            )
        ):
            raise RuntimeError("Daily receipt socket authority response does not match")
        return verified.signature

    def identity(self) -> DailyReceiptSocketIdentityResponse:
        """Read and authenticate the SHA of the authority process actually serving."""

        nonce = os.urandom(32).hex()
        response = self._request(
            canonical_json_bytes(
                {
                    "version": DAILY_RECEIPT_IDENTITY_VERSION,
                    "operation": DAILY_RECEIPT_IDENTITY_OPERATION,
                    "protocol": DAILY_RECEIPT_IDENTITY_PROTOCOL,
                    "nonce": nonce,
                }
            )
        )
        verified = DailyReceiptSocketIdentityResponse.model_validate(response)
        envelope = {
            "version": verified.version,
            "operation": verified.operation,
            "protocol": verified.protocol,
            "nonce": verified.nonce,
            "source_sha256": verified.source_sha256,
            "key_id": verified.key_id,
        }
        if (
            verified.nonce != nonce
            or verified.protocol != DAILY_RECEIPT_IDENTITY_PROTOCOL
            or verified.key_id != self.key_id
            or not _valid_ed25519_signature(verified.signature)
            or not _verify_ed25519_signature(
                public_key=self._active_public_key,
                payload=canonical_json_bytes(envelope),
                signature=verified.signature,
            )
        ):
            raise RuntimeError("Daily receipt socket identity response does not match")
        return verified

    def _request(self, payload: bytes) -> object:
        delay = 0.05
        last_error: BaseException | None = None
        for attempt in range(self._max_attempts):
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(self.timeout_seconds)
                    client.connect(str(self._socket_path))
                    _write_frame(client, payload)
                    response = _read_frame(client)
                decoded = strict_canonical_json_loads(response)
                if isinstance(decoded, dict) and decoded.get("ok") is False:
                    raise RuntimeError(str(decoded.get("error") or "Daily receipt signer failed"))
                return decoded
            except (OSError, TimeoutError, StrictJsonError, ValueError, RuntimeError) as exc:
                last_error = exc
                if attempt + 1 >= self._max_attempts:
                    break
                time.sleep(delay)
                delay = min(delay * 2, 0.5)
        raise RuntimeError("Daily receipt socket authority failed") from last_error


def daily_receipt_peer_credentials_supported() -> bool:
    """Real Linux ``SO_PEERCRED`` is the only accepted Daily peer identity source."""

    return sys.platform.startswith("linux") and hasattr(socket, "SO_PEERCRED")


def serve_daily_receipt_socket_authority(
    *,
    socket_path: Path | None = None,
    inherited_socket_fd: int | None = None,
    keys_path: Path = DAILY_RECEIPT_KEYS_FILE,
    nonce_state_root: Path = DAILY_RECEIPT_SIGNER_STATE_ROOT,
    allowed_uid: int | None = None,
    allowed_gid: int | None = None,
    max_connections: int | None = None,
) -> None:
    # There is deliberately no peer-credential seam: an injectable provider would let a
    # test double (or a patched process) hand the root authority any identity it likes.
    if not daily_receipt_peer_credentials_supported():
        raise DailyReceiptSocketAuthorityError("Linux SO_PEERCRED is required")
    authority = _DailyReceiptSocketAuthority(
        keys_path=keys_path,
        nonce_store=_DailyReceiptNonceStore(root=nonce_state_root),
        source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    )
    uid = _uid_for_user(DAILY_RECEIPT_ALLOWED_USER) if allowed_uid is None else allowed_uid
    gid = _gid_for_group(DAILY_RECEIPT_ALLOWED_GROUP) if allowed_gid is None else allowed_gid
    listener_owned = False
    if inherited_socket_fd is not None:
        expected_socket = DAILY_RECEIPT_SOCKET_ENDPOINT if socket_path is None else socket_path
        listener = _validate_inherited_listener(
            inherited_socket_fd,
            expected_path=expected_socket,
        )
    else:
        if socket_path is None:
            raise DailyReceiptSocketAuthorityError("Daily receipt socket path is required")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener_owned = True
        candidate = Path(os.path.abspath(socket_path))
        candidate.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        candidate.unlink(missing_ok=True)
        listener.bind(str(candidate))
        listener.listen(128)
    processed = 0
    slots = threading.Semaphore(_MAX_CONCURRENT_CONNECTIONS)
    workers: list[threading.Thread] = []
    try:
        with listener:
            while max_connections is None or processed < max_connections:
                connection, _address = listener.accept()
                processed += 1
                slots.acquire()
                worker = threading.Thread(
                    target=_serve_one_connection,
                    args=(connection, authority, uid, gid, slots),
                    daemon=True,
                )
                worker.start()
                workers = [thread for thread in workers if thread.is_alive()]
                workers.append(worker)
    finally:
        for thread in workers:
            thread.join(timeout=_CONNECTION_TIMEOUT_SECONDS + 5)
        if listener_owned and socket_path is not None:
            Path(socket_path).unlink(missing_ok=True)


def _serve_one_connection(
    connection: socket.socket,
    authority: _DailyReceiptSocketAuthority,
    allowed_uid: int,
    allowed_gid: int,
    slots: threading.Semaphore,
) -> None:
    try:
        with connection:
            # A client that connects and then stalls must never wedge the root authority.
            connection.settimeout(_CONNECTION_TIMEOUT_SECONDS)
            try:
                peer_uid, peer_gid = _peer_credentials(connection)
                if peer_uid != allowed_uid or peer_gid != allowed_gid:
                    raise DailyReceiptSocketAuthorityError(
                        "Daily receipt signer peer credentials are not allowed"
                    )
                request_payload = _read_frame(connection)
                response_payload = authority.handle(request_payload)
                _write_frame(connection, response_payload)
            except Exception as exc:  # noqa: BLE001
                _fail_closed(connection, exc)
    finally:
        slots.release()


def _fail_closed(connection: socket.socket, exc: BaseException) -> None:
    try:
        _write_frame(
            connection,
            canonical_json_bytes({"ok": False, "error": _safe_error(exc)}),
        )
    except (OSError, TimeoutError, DailyReceiptSocketAuthorityError):
        return


class _DailyReceiptSocketAuthority:
    def __init__(
        self,
        *,
        keys_path: Path,
        nonce_store: _DailyReceiptNonceStore,
        source_sha256: str,
    ) -> None:
        self._keys_path = keys_path
        self._nonce_store = nonce_store
        self._source_sha256 = source_sha256

    def handle(self, payload: bytes) -> bytes:
        decoded = strict_canonical_json_loads(payload)
        if (
            isinstance(decoded, dict)
            and decoded.get("operation") == DAILY_RECEIPT_IDENTITY_OPERATION
        ):
            request = _decode_identity_request(payload)
            _generation, _previous_hash, key_id, private_key, _public_key, _previous = _load_keys(
                self._keys_path
            )
            envelope = daily_receipt_socket_identity_envelope(
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
        request = _decode_socket_request(payload)
        nonce = str(request["nonce"])
        _generation, _previous_hash, key_id, private_key, _public_key, _previous = _load_keys(
            self._keys_path
        )
        payload_bytes = base64.b64decode(str(request["canonical_payload"]), validate=True)
        envelope = daily_receipt_socket_signature_envelope(
            namespace=str(request["namespace"]),
            nonce=nonce,
            payload_sha256=str(request["payload_sha256"]),
            key_id=key_id,
        )
        self._nonce_store.claim(
            nonce,
            envelope_hash=hashlib.sha256(canonical_json_bytes(envelope)).hexdigest(),
        )
        response = {
            "version": 1,
            "operation": "sign",
            "namespace": request["namespace"],
            "nonce": nonce,
            "payload_sha256": request["payload_sha256"],
            "key_id": key_id,
            "signature": _sign(private_key, payload_bytes),
        }
        if not _valid_ed25519_signature(str(response["signature"])):
            raise DailyReceiptSocketAuthorityError("Daily signer produced an invalid signature")
        return canonical_json_bytes(response)


def daily_receipt_socket_signature_envelope(
    *,
    namespace: str,
    nonce: str,
    payload_sha256: str,
    key_id: str,
) -> dict[str, object]:
    if namespace not in DAILY_RECEIPT_NAMESPACES:
        raise DailyReceiptSocketAuthorityError("Daily signing envelope namespace is invalid")
    if _SHA256.fullmatch(nonce) is None:
        raise DailyReceiptSocketAuthorityError("Daily signing envelope nonce is invalid")
    if _SHA256.fullmatch(payload_sha256) is None:
        raise DailyReceiptSocketAuthorityError("Daily signing envelope payload hash is invalid")
    if _KEY_ID.fullmatch(key_id) is None:
        raise DailyReceiptSocketAuthorityError("Daily signing envelope key id is invalid")
    return {
        "version": 1,
        "operation": "sign",
        "namespace": namespace,
        "nonce": nonce,
        "payload_sha256": payload_sha256,
        "key_id": key_id,
    }


def daily_receipt_socket_identity_envelope(
    *, nonce: str, source_sha256: str, key_id: str
) -> dict[str, object]:
    if (
        _SHA256.fullmatch(nonce) is None
        or _SHA256.fullmatch(source_sha256) is None
        or _KEY_ID.fullmatch(key_id) is None
    ):
        raise DailyReceiptSocketAuthorityError("Daily identity envelope is invalid")
    return {
        "version": DAILY_RECEIPT_IDENTITY_VERSION,
        "operation": DAILY_RECEIPT_IDENTITY_OPERATION,
        "protocol": DAILY_RECEIPT_IDENTITY_PROTOCOL,
        "nonce": nonce,
        "source_sha256": source_sha256,
        "key_id": key_id,
    }


class _DailyReceiptNonceStore:
    def __init__(
        self,
        *,
        root: Path,
        retention_seconds: int = _NONCE_RETENTION_SECONDS,
        max_records: int = _MAX_NONCES,
    ) -> None:
        self._root = Path(os.path.abspath(root))
        self._retention_seconds = retention_seconds
        self._max_records = max_records
        self._lock = threading.Lock()
        _ensure_private_directory(self._root, label="Daily nonce state directory")

    def claim(self, nonce: str, *, envelope_hash: str) -> None:
        if _SHA256.fullmatch(nonce) is None or _SHA256.fullmatch(envelope_hash) is None:
            raise DailyReceiptSocketAuthorityError("Daily signing nonce state is invalid")
        with self._lock:
            self._expire_old_records()
            count = self._record_count()
            if count >= self._max_records:
                raise DailyReceiptSocketAuthorityError("Daily signing nonce state is full")
            path = self._root / nonce
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(path, flags, 0o600)
            except FileExistsError as exc:
                raise DailyReceiptSocketAuthorityError("Daily signing nonce replay") from exc
            except OSError as exc:
                raise DailyReceiptSocketAuthorityError("Daily signing nonce state failed") from exc
            try:
                payload = canonical_json_bytes(
                    {
                        "schema_version": 1,
                        "nonce": nonce,
                        "envelope_hash": envelope_hash,
                        "claimed_at_unix": int(time.time()),
                    }
                )
                os.write(descriptor, payload)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _expire_old_records(self) -> None:
        cutoff = time.time() - self._retention_seconds
        for path in self._root.iterdir():
            try:
                observed = self._validate_nonce_record(path)
            except OSError:
                continue
            if observed.st_mtime < cutoff:
                path.unlink()

    def _record_count(self) -> int:
        count = 0
        for path in self._root.iterdir():
            self._validate_nonce_record(path)
            count += 1
        return count

    @staticmethod
    def _validate_nonce_record(path: Path) -> os.stat_result:
        observed = path.stat(follow_symlinks=False)
        expected_owner = 0 if os.geteuid() == 0 else os.geteuid()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != expected_owner
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_nlink != 1
        ):
            raise DailyReceiptSocketAuthorityError("Daily signing nonce state is unsafe")
        return observed


def _decode_socket_request(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > _MAX_FRAME_BYTES:
        raise DailyReceiptSocketAuthorityError("Daily signing request size is invalid")
    try:
        request = strict_canonical_json_loads(payload)
    except (StrictJsonError, UnicodeDecodeError) as exc:
        raise DailyReceiptSocketAuthorityError("Daily signing request is invalid JSON") from exc
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
        or request.get("namespace") not in DAILY_RECEIPT_NAMESPACES
        or not isinstance(request.get("nonce"), str)
        or _SHA256.fullmatch(request["nonce"]) is None
        or not isinstance(request.get("payload_sha256"), str)
        or _SHA256.fullmatch(request["payload_sha256"]) is None
        or not isinstance(request.get("canonical_payload"), str)
    ):
        raise DailyReceiptSocketAuthorityError("Daily signing request shape is invalid")
    try:
        payload_bytes = base64.b64decode(request["canonical_payload"], validate=True)
    except (TypeError, ValueError) as exc:
        raise DailyReceiptSocketAuthorityError("Daily signing payload is invalid") from exc
    if not payload_bytes or len(payload_bytes) > _MAX_PAYLOAD_BYTES:
        raise DailyReceiptSocketAuthorityError("Daily signing payload size is invalid")
    if hashlib.sha256(payload_bytes).hexdigest() != request["payload_sha256"]:
        raise DailyReceiptSocketAuthorityError("Daily signing payload hash is invalid")
    return request


def _decode_identity_request(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > _MAX_FRAME_BYTES:
        raise DailyReceiptSocketAuthorityError("Daily identity request size is invalid")
    try:
        request = strict_canonical_json_loads(payload)
    except (StrictJsonError, UnicodeDecodeError) as exc:
        raise DailyReceiptSocketAuthorityError("Daily identity request is invalid JSON") from exc
    expected = {"version", "operation", "protocol", "nonce"}
    if (
        not isinstance(request, dict)
        or set(request) != expected
        or request.get("version") != DAILY_RECEIPT_IDENTITY_VERSION
        or request.get("operation") != DAILY_RECEIPT_IDENTITY_OPERATION
        or request.get("protocol") != DAILY_RECEIPT_IDENTITY_PROTOCOL
        or not isinstance(request.get("nonce"), str)
        or _SHA256.fullmatch(request["nonce"]) is None
    ):
        raise DailyReceiptSocketAuthorityError("Daily identity request shape is invalid")
    return request


def _load_keys(path: Path) -> tuple[int, str, str, Path, str, dict[str, str]]:
    payload = _secure_file(path, mode=0o600, label="Daily key manifest")
    try:
        document = strict_canonical_json_loads(payload)
    except (StrictJsonError, UnicodeDecodeError) as exc:
        raise DailyReceiptSocketAuthorityError("Daily key manifest is invalid") from exc
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
        or document["generation"] < 1
        or not isinstance(document.get("previous_manifest_hash"), str)
        or _SHA256.fullmatch(document["previous_manifest_hash"]) is None
        or not isinstance(document.get("active_key_id"), str)
        or _KEY_ID.fullmatch(document["active_key_id"]) is None
        or not isinstance(document.get("active_private_key_path"), str)
        or not isinstance(document.get("previous_public_keys"), dict)
    ):
        raise DailyReceiptSocketAuthorityError("Daily key manifest shape is invalid")
    generation = int(document["generation"])
    previous_manifest_hash = str(document["previous_manifest_hash"])
    active_key_id = str(document["active_key_id"])
    private_key = Path(str(document["active_private_key_path"]))
    if not private_key.is_absolute() or private_key != Path(os.path.abspath(private_key)):
        raise DailyReceiptSocketAuthorityError("Daily private key path is invalid")
    raw_previous = document["previous_public_keys"]
    if generation == 1 and (
        previous_manifest_hash != _GENESIS_MANIFEST_HASH or raw_previous
    ):
        raise DailyReceiptSocketAuthorityError("Daily key manifest genesis binding is invalid")
    if generation > 1 and (
        previous_manifest_hash == _GENESIS_MANIFEST_HASH or not raw_previous
    ):
        raise DailyReceiptSocketAuthorityError("Daily key manifest rotation binding is invalid")
    previous: dict[str, str] = {}
    for key_id, public_key in raw_previous.items():
        if (
            not isinstance(key_id, str)
            or _KEY_ID.fullmatch(key_id) is None
            or key_id == active_key_id
            or not isinstance(public_key, str)
        ):
            raise DailyReceiptSocketAuthorityError("Daily previous key id is invalid")
        _validate_public_key(public_key, label="Daily previous public key")
        previous[key_id] = public_key
    active_public_key = _public_key_from_private(private_key)
    return (
        generation,
        previous_manifest_hash,
        active_key_id,
        private_key,
        active_public_key,
        dict(sorted(previous.items())),
    )


def load_daily_receipt_trusted_keyring(path: Path) -> DailyReceiptTrustedKeyring:
    payload = _secure_public_keyring_file(path)
    try:
        document = strict_canonical_json_loads(payload)
    except (StrictJsonError, UnicodeDecodeError) as exc:
        raise DailyReceiptSocketAuthorityError("Daily public keyring is invalid JSON") from exc
    expected = {
        "schema_version",
        "generation",
        "previous_manifest_hash",
        "active_key_id",
        "active_public_key",
        "previous_public_keys",
        "manifest_hash",
        "signature",
    }
    if (
        not isinstance(document, dict)
        or set(document) != expected
        or document.get("schema_version") != 2
        or type(document.get("generation")) is not int
        or document["generation"] < 1
        or not isinstance(document.get("previous_manifest_hash"), str)
        or _SHA256.fullmatch(document["previous_manifest_hash"]) is None
        or not isinstance(document.get("active_key_id"), str)
        or _KEY_ID.fullmatch(document["active_key_id"]) is None
        or not isinstance(document.get("active_public_key"), str)
        or not isinstance(document.get("previous_public_keys"), dict)
        or not isinstance(document.get("manifest_hash"), str)
        or _SHA256.fullmatch(document["manifest_hash"]) is None
        or not isinstance(document.get("signature"), str)
        or not document["signature"]
    ):
        raise DailyReceiptSocketAuthorityError("Daily public keyring shape is invalid")
    active_key_id = str(document["active_key_id"])
    active_public_key = _validate_public_key(
        document["active_public_key"],
        label="Daily active public key",
    )
    previous: dict[str, str] = {}
    for key_id, public_key in document["previous_public_keys"].items():
        if (
            not isinstance(key_id, str)
            or _KEY_ID.fullmatch(key_id) is None
            or key_id == active_key_id
            or not isinstance(public_key, str)
        ):
            raise DailyReceiptSocketAuthorityError("Daily public keyring previous key is invalid")
        previous[key_id] = _validate_public_key(public_key, label="Daily previous public key")
    generation = int(document["generation"])
    previous_manifest_hash = str(document["previous_manifest_hash"])
    if generation == 1 and (previous_manifest_hash != _GENESIS_MANIFEST_HASH or previous):
        raise DailyReceiptSocketAuthorityError("Daily public keyring genesis binding is invalid")
    if generation > 1 and (previous_manifest_hash == _GENESIS_MANIFEST_HASH or not previous):
        raise DailyReceiptSocketAuthorityError("Daily public keyring rotation binding is invalid")
    body = {
        key: document[key]
        for key in expected
        if key not in {"manifest_hash", "signature"}
    }
    expected_hash = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    if document["manifest_hash"] != expected_hash:
        raise DailyReceiptSocketAuthorityError("Daily public keyring manifest hash is invalid")
    if not _verify_ed25519_signature(
        public_key=active_public_key.encode("utf-8"),
        payload=expected_hash.encode("ascii"),
        signature=str(document["signature"]),
    ):
        raise DailyReceiptSocketAuthorityError("Daily public keyring signature is invalid")
    return DailyReceiptTrustedKeyring(
        active_key_id=active_key_id,
        active_public_key_pem=active_public_key,
        previous_public_key_pems=dict(sorted(previous.items())),
    )


def probe_daily_receipt_authority_identity(
    *,
    keyring_path: Path = DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
    socket_path: Path = DAILY_RECEIPT_SOCKET_ENDPOINT,
    timeout_seconds: float = 2.0,
    max_attempts: int = 1,
    test_endpoint: bool = False,
) -> DailyReceiptSocketIdentityResponse:
    """Probe the running root authority and authenticate its loaded source SHA.

    Production callers are deliberately constrained to the fixed keyring and
    socket.  ``test_endpoint`` exists only for focused tests that use an
    isolated root-owned fixture; it does not change the request or response
    protocol.
    """

    keyring = load_daily_receipt_trusted_keyring(keyring_path)
    if test_endpoint:
        client = DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=socket_path,
            active_key_id=keyring.active_key_id,
            active_public_key_pem=keyring.active_public_key_pem,
            previous_public_key_pems=keyring.previous_public_key_pems,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )
    else:
        if socket_path != DAILY_RECEIPT_SOCKET_ENDPOINT:
            raise ValueError("Daily receipt identity probe socket endpoint must be fixed")
        if keyring_path != DAILY_RECEIPT_TRUSTED_KEYRING_PATH:
            raise ValueError("Daily receipt identity probe keyring path must be fixed")
        client = DailyReceiptSocketSigningClient.production(
            active_key_id=keyring.active_key_id,
            active_public_key_pem=keyring.active_public_key_pem,
            previous_public_key_pems=keyring.previous_public_key_pems,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )
    return client.identity()


def _secure_file(path: Path, *, mode: int, label: str) -> bytes:
    candidate = Path(os.path.abspath(path))
    if not candidate.is_absolute() or candidate != path:
        raise DailyReceiptSocketAuthorityError(f"{label} path is invalid")
    _verify_secure_parent_chain(candidate, label=label)
    descriptor = -1
    try:
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        expected_owner = 0 if os.geteuid() == 0 else os.geteuid()
        named = os.stat(candidate, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_uid != expected_owner
            or stat.S_IMODE(opened.st_mode) != mode
            or not 0 < opened.st_size <= _MAX_KEY_FILE_BYTES
        ):
            raise DailyReceiptSocketAuthorityError(f"{label} is unsafe")
        payload = os.read(descriptor, _MAX_KEY_FILE_BYTES + 1)
        after = os.fstat(descriptor)
        if len(payload) != opened.st_size or (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise DailyReceiptSocketAuthorityError(f"{label} changed while reading")
        return payload
    except OSError as exc:
        raise DailyReceiptSocketAuthorityError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _ensure_private_directory(path: Path, *, label: str) -> None:
    candidate = Path(os.path.abspath(path))
    if not candidate.is_absolute() or candidate != path:
        raise DailyReceiptSocketAuthorityError(f"{label} path is invalid")
    candidate.mkdir(mode=0o700, parents=True, exist_ok=True)
    _verify_secure_parent_chain(candidate, label=label)
    observed = os.stat(candidate, follow_symlinks=False)
    expected_owner = 0 if os.geteuid() == 0 else os.geteuid()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != expected_owner
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise DailyReceiptSocketAuthorityError(f"{label} is unsafe")


def _verify_secure_parent_chain(path: Path, *, label: str) -> None:
    expected_owner = 0 if os.geteuid() == 0 else os.geteuid()
    for parent in reversed(path.parents):
        try:
            observed = os.stat(parent, follow_symlinks=False)
        except OSError as exc:
            raise DailyReceiptSocketAuthorityError(f"{label} parent is unavailable") from exc
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid not in {0, expected_owner}
            # Sticky world-writable ancestors (/tmp on Linux CI runners) cannot
            # displace foreign entries; child ownership/mode checks still apply.
            or (observed.st_mode & 0o022 and not observed.st_mode & stat.S_ISVTX)
        ):
            raise DailyReceiptSocketAuthorityError(f"{label} parent is unsafe")


def _secure_public_keyring_file(path: Path) -> bytes:
    candidate = Path(os.path.abspath(path))
    if not candidate.is_absolute() or candidate != path:
        raise DailyReceiptSocketAuthorityError("Daily public keyring path is invalid")
    descriptor = -1
    try:
        for parent in candidate.parents:
            observed_parent = os.stat(parent, follow_symlinks=False)
            if (
                not stat.S_ISDIR(observed_parent.st_mode)
                or observed_parent.st_uid != 0
                or observed_parent.st_mode & 0o022
            ):
                raise DailyReceiptSocketAuthorityError(
                    "Daily public keyring parent must be root-owned and not writable"
                )
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        named = os.stat(candidate, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_uid != 0
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o444
            or not 0 < opened.st_size <= _MAX_KEY_FILE_BYTES
        ):
            raise DailyReceiptSocketAuthorityError("Daily public keyring file is unsafe")
        payload = os.read(descriptor, _MAX_KEY_FILE_BYTES + 1)
        after = os.fstat(descriptor)
        if len(payload) != opened.st_size or (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise DailyReceiptSocketAuthorityError("Daily public keyring changed while reading")
        return payload
    except OSError as exc:
        raise DailyReceiptSocketAuthorityError("Daily public keyring is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _openssl() -> str:
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/bin/openssl", shutil.which("openssl")):
        if candidate and Path(candidate).is_file():
            return candidate
    raise DailyReceiptSocketAuthorityError("openssl is unavailable")


def _run_openssl(
    arguments: list[str],
    *,
    input_data: bytes = b"",
) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            [_openssl(), *arguments],
            input=input_data,
            capture_output=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DailyReceiptSocketAuthorityError("openssl operation failed") from exc


def _validate_public_key(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 8192:
        raise DailyReceiptSocketAuthorityError(f"{label} is invalid")
    result = _run_openssl(
        ["pkey", "-pubin", "-pubcheck", "-text_pub", "-noout"],
        input_data=value.encode("utf-8"),
    )
    if result.returncode != 0 or b"ED25519" not in result.stdout.upper():
        raise DailyReceiptSocketAuthorityError(f"{label} is not Ed25519")
    return value


def _public_key_from_private(path: Path) -> str:
    _secure_file(path, mode=0o600, label="Daily private key")
    result = _run_openssl(["pkey", "-in", str(path), "-pubout"])
    if result.returncode != 0 or not result.stdout:
        raise DailyReceiptSocketAuthorityError("Daily private key is unusable")
    return _validate_public_key(
        result.stdout.decode("utf-8"),
        label="Daily active public key",
    )


def _sign(private_key: Path, payload: bytes) -> str:
    with tempfile.TemporaryDirectory(prefix="rquant-daily-receipt-socket-sign-") as raw_root:
        payload_path = Path(raw_root) / "payload.bin"
        payload_path.write_bytes(payload)
        payload_path.chmod(0o600)
        result = _run_openssl(
            ["pkeyutl", "-sign", "-rawin", "-inkey", str(private_key), "-in", str(payload_path)]
        )
    if result.returncode != 0 or len(result.stdout) != _ED25519_SIGNATURE_BYTES:
        raise DailyReceiptSocketAuthorityError("Daily Ed25519 signing failed")
    return base64.b64encode(result.stdout).decode("ascii")


def _write_frame(connection: socket.socket, payload: bytes) -> None:
    if not payload or len(payload) > _MAX_FRAME_BYTES:
        raise DailyReceiptSocketAuthorityError("Daily socket response size is invalid")
    connection.sendall(len(payload).to_bytes(_FRAME_HEADER_BYTES, "big") + payload)


def _read_frame(connection: socket.socket) -> bytes:
    header = _recv_exact(connection, _FRAME_HEADER_BYTES)
    size = int.from_bytes(header, "big")
    if not 0 < size <= _MAX_FRAME_BYTES:
        raise DailyReceiptSocketAuthorityError("Daily socket frame size is invalid")
    return _recv_exact(connection, size)


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise DailyReceiptSocketAuthorityError("Daily socket frame is truncated")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _peer_credentials(connection: socket.socket) -> tuple[int, int]:
    raw = connection.getsockopt(
        socket.SOL_SOCKET,
        socket.SO_PEERCRED,
        struct.calcsize("3i"),
    )
    _pid, uid, gid = struct.unpack("3i", raw)
    return uid, gid


def _validate_inherited_listener(fd: int, *, expected_path: Path) -> socket.socket:
    expected = Path(os.path.abspath(expected_path))
    if expected != expected_path:
        raise DailyReceiptSocketAuthorityError("Daily receipt socket endpoint is invalid")
    try:
        listener = socket.fromfd(fd, socket.AF_UNIX, socket.SOCK_STREAM)
    except OSError as exc:
        raise DailyReceiptSocketAuthorityError("Daily inherited fd is not a socket") from exc
    try:
        if listener.family != socket.AF_UNIX:
            raise DailyReceiptSocketAuthorityError("Daily inherited fd is not an AF_UNIX socket")
        socket_type = listener.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
        if socket_type != socket.SOCK_STREAM:
            raise DailyReceiptSocketAuthorityError(
                "Daily inherited fd is not a SOCK_STREAM socket"
            )
        if hasattr(socket, "SO_ACCEPTCONN"):
            try:
                accepting = listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN)
            except OSError:
                accepting = 0
            if accepting != 1:
                raise DailyReceiptSocketAuthorityError(
                    "Daily inherited socket is not listening"
                )
        bound = listener.getsockname()
        if isinstance(bound, bytes):
            bound = bound.decode("utf-8", errors="strict")
        if not isinstance(bound, str) or not bound:
            raise DailyReceiptSocketAuthorityError("Daily inherited socket path is unavailable")
        if Path(os.path.abspath(bound)) != expected:
            raise DailyReceiptSocketAuthorityError("Daily inherited socket endpoint mismatch")
        return listener
    except Exception:
        listener.close()
        raise


def _uid_for_user(user: str) -> int:
    try:
        return pwd.getpwnam(user).pw_uid
    except KeyError as exc:
        raise DailyReceiptSocketAuthorityError("Daily signer service user is unavailable") from exc


def _gid_for_group(group: str) -> int:
    try:
        return grp.getgrnam(group).gr_gid
    except KeyError as exc:
        raise DailyReceiptSocketAuthorityError("Daily signer service group is unavailable") from exc


def _safe_error(exc: BaseException) -> str:
    text = str(exc) or exc.__class__.__name__
    return text[:512]


def _systemd_inherited_socket_fd() -> int | None:
    if "LISTEN_FDS" not in os.environ and "LISTEN_PID" not in os.environ:
        return None
    try:
        listen_pid = int(os.environ.get("LISTEN_PID", "0"))
        listen_fds = int(os.environ.get("LISTEN_FDS", "0"))
    except ValueError as exc:
        raise DailyReceiptSocketAuthorityError(
            "Daily systemd socket activation is invalid"
        ) from exc
    if listen_pid != os.getpid() or listen_fds != 1:
        raise DailyReceiptSocketAuthorityError(
            "Daily systemd socket activation must provide exactly one fd"
        )
    descriptor = 3
    poller = select.poll()
    poller.register(descriptor, select.POLLIN)
    return descriptor


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments:
        raise DailyReceiptSocketAuthorityError("Daily socket authority arguments are not allowed")
    inherited_fd = _systemd_inherited_socket_fd()
    serve_daily_receipt_socket_authority(
        socket_path=None if inherited_fd is not None else DAILY_RECEIPT_SOCKET_ENDPOINT,
        inherited_socket_fd=inherited_fd,
        keys_path=DAILY_RECEIPT_KEYS_FILE,
    )
    return 0


__all__ = [
    "DAILY_RECEIPT_SOCKET_ENDPOINT",
    "DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
    "DailyReceiptSocketAuthorityError",
    "DailyReceiptSocketSigningClient",
    "DailyReceiptTrustedKeyring",
    "daily_receipt_peer_credentials_supported",
    "daily_receipt_socket_signature_envelope",
    "load_daily_receipt_trusted_keyring",
    "probe_daily_receipt_authority_identity",
    "serve_daily_receipt_socket_authority",
]


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DailyReceiptSocketAuthorityError as exc:
        print(f"Daily receipt socket authority rejected request: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
