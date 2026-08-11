"""Persistent Unix daemon runtime for the shared external monotonic root."""

from __future__ import annotations

import base64
import hashlib
import os
import shutil
import socket
import sqlite3
import struct
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Final, Literal, Protocol, Self, final

from pydantic import Field, ValidationError, model_validator

from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    read_secure_regular_file,
    secure_create_regular_file,
    secure_path_metadata,
)
from rquant.external_monotonic_root import (
    EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
    ExternalMonotonicRootRequest,
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strict_json import (
    canonical_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE: Final = "rquant-external-monotonic-root-service-probe/v1"
_APPLICATION_ID: Final = 0x52514552
_SCHEMA_VERSION: Final = 1
_MAX_FRAME_BYTES: Final = 8 * 1024 * 1024
_ZERO_HASH: Final = "0" * 64


class ExternalMonotonicRootServiceError(RuntimeError):
    """The external root service configuration or durable state is untrusted."""


def _operation_effect_hash(request: ExternalMonotonicRootRequest) -> str:
    return canonical_sha256(
        request.model_dump(
            mode="python",
            exclude={"challenge_nonce", "request_hash"},
        )
    )


class ExternalRootStoredState(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-external-monotonic-root-stored-state/v1"] = (
        "rquant-external-monotonic-root-stored-state/v1"
    )
    role: str = Field(min_length=1, max_length=200)
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    subject_authority_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint_contract: str = Field(min_length=1, max_length=300)
    checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint_json: str

    @model_validator(mode="after")
    def validate_checkpoint(self) -> Self:
        try:
            checkpoint = strict_canonical_json_loads(self.checkpoint_json)
        except (TypeError, ValueError) as exc:
            raise ValueError("stored external root checkpoint is not canonical") from exc
        if (
            not isinstance(checkpoint, dict)
            or checkpoint.get("contract") != self.checkpoint_contract
            or canonical_sha256(checkpoint) != self.checkpoint_hash
        ):
            raise ValueError("stored external root checkpoint identity is invalid")
        return self


class ExternalRootServiceConfiguration(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-external-monotonic-root-service-config/v1"] = (
        "rquant-external-monotonic-root-service-config/v1"
    )
    socket_path: Path
    socket_uid: int = Field(strict=True, ge=0)
    socket_gid: int = Field(strict=True, ge=0)
    service_uid: int = Field(strict=True, ge=0)
    service_gid: int = Field(strict=True, ge=0)
    allowed_peer_uid: int = Field(strict=True, ge=0)
    allowed_peer_gid: int = Field(strict=True, ge=0)
    socket_mode: Literal[0o600, 0o660] = 0o600
    socket_directory_mode: Literal[0o700, 0o750] = 0o700
    role: str = Field(min_length=1, max_length=200)
    authority_id: str = Field(min_length=1, max_length=200)
    store_id: str = Field(min_length=1, max_length=200)
    rollback_domain_id: str = Field(min_length=1, max_length=200)
    transport_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_timeout_ms: int = Field(default=2_000, strict=True, ge=1, le=30_000)

    @model_validator(mode="after")
    def validate_socket(self) -> Self:
        if not self.socket_path.is_absolute():
            raise ValueError("external root service socket path must be absolute")
        return self


class ExternalRootServiceProbeRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-external-monotonic-root-service-probe-request/v1"] = (
        "rquant-external-monotonic-root-service-probe-request/v1"
    )
    challenge_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")


class ExternalRootServiceProbeReceipt(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-external-monotonic-root-service-probe-receipt/v1"] = (
        "rquant-external-monotonic-root-service-probe-receipt/v1"
    )
    challenge_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    role: str = Field(min_length=1, max_length=200)
    authority_id: str = Field(min_length=1, max_length=200)
    store_id: str = Field(min_length=1, max_length=200)
    rollback_domain_id: str = Field(min_length=1, max_length=200)
    transport_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    capabilities: tuple[Literal["current", "pin", "advance"], ...]
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_capabilities(self) -> Self:
        if self.capabilities != ("current", "pin", "advance"):
            raise ValueError("external root service capabilities are not closed")
        return self

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class ExternalMonotonicRootRoleHandler(Protocol):
    def response_json(
        self,
        request: ExternalMonotonicRootRequest,
        state: ExternalRootStoredState | None,
    ) -> str | None: ...


class ExternalMonotonicRootSigningCapability(Protocol):
    issuer: str
    key_id: str
    key_purpose: str
    signature_algorithm: Literal["ed25519"]
    public_key_fingerprint: str

    def sign(self, *, namespace: str, payload: bytes) -> str: ...


def _openssl_binary() -> str:
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/bin/openssl", shutil.which("openssl")):
        if candidate and Path(candidate).is_file():
            return candidate
    raise ExternalMonotonicRootServiceError("openssl is required for external root signing")


def _secure_key_bytes(path: Path, *, private: bool) -> bytes:
    try:
        current_uid = os.geteuid()
        current_gids = frozenset({os.getegid(), *os.getgroups()})
        return read_secure_regular_file(
            Path(path),
            trusted_root=Path("/"),
            allowed_ancestor_uids=frozenset({0, current_uid}),
            expected_uid=current_uid if private else 0,
            expected_gid=os.getegid(),
            allowed_final_uids=(
                frozenset({current_uid}) if private else frozenset({0, current_uid})
            ),
            allowed_final_gids=current_gids,
            allowed_modes=(
                frozenset({0o400, 0o600})
                if private
                else frozenset({0o400, 0o440, 0o444, 0o600, 0o640, 0o644})
            ),
            max_bytes=64 * 1024,
        )
    except AuthorityPathSecurityError as exc:
        raise ExternalMonotonicRootServiceError("external root key is unavailable") from exc


def _domain_payload(*, namespace: str, payload: bytes) -> bytes:
    return canonical_json_bytes(
        {
            "contract": "rquant-external-monotonic-root-signature-domain/v1",
            "namespace": namespace,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
        }
    )


def _run_openssl(arguments: tuple[str, ...]) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            (_openssl_binary(), *arguments),
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ExternalMonotonicRootServiceError("external root crypto operation failed") from exc


@final
class ClosedExternalMonotonicRootVerifier:
    """Exact public-only verifier accepted by production resource composition."""

    signature_algorithm: Literal["ed25519"] = "ed25519"

    def __init__(
        self,
        *,
        public_key_path: Path,
        issuer: str,
        key_id: str,
        key_purpose: str,
    ) -> None:
        self.issuer = issuer.strip()
        self.key_id = key_id.strip()
        self.key_purpose = key_purpose.strip()
        if not self.issuer or not self.key_id or not self.key_purpose:
            raise ExternalMonotonicRootServiceError("external root verifier identity is empty")
        self._public_key = _secure_key_bytes(public_key_path, private=False)
        self.public_key_fingerprint = hashlib.sha256(self._public_key).hexdigest()
        with tempfile.TemporaryDirectory(prefix="rquant-root-pubcheck-") as temporary:
            public = Path(temporary) / "public.pem"
            public.write_bytes(self._public_key)
            public.chmod(0o600)
            completed = _run_openssl(("pkey", "-pubin", "-pubcheck", "-in", str(public), "-noout"))
        if completed.returncode != 0:
            raise ExternalMonotonicRootServiceError("external root public key is invalid")

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool:
        try:
            signature_bytes = base64.b64decode(signature, validate=True)
            if len(signature_bytes) != 64:
                return False
            with tempfile.TemporaryDirectory(prefix="rquant-root-verify-") as temporary:
                root = Path(temporary)
                public = root / "public.pem"
                message = root / "message.bin"
                signed = root / "signature.bin"
                public.write_bytes(self._public_key)
                message.write_bytes(_domain_payload(namespace=namespace, payload=payload))
                signed.write_bytes(signature_bytes)
                for path in (public, message, signed):
                    path.chmod(0o600)
                completed = _run_openssl(
                    (
                        "pkeyutl",
                        "-verify",
                        "-pubin",
                        "-inkey",
                        str(public),
                        "-sigfile",
                        str(signed),
                        "-rawin",
                        "-in",
                        str(message),
                    )
                )
            return completed.returncode == 0
        except (OSError, ValueError, ExternalMonotonicRootServiceError):
            return False


@final
class OpenSslExternalMonotonicRootSigner:
    signature_algorithm: Literal["ed25519"] = "ed25519"

    def __init__(
        self,
        *,
        private_key_path: Path,
        public_key_path: Path,
        issuer: str,
        key_id: str,
        key_purpose: str,
        allowed_namespaces: frozenset[str],
    ) -> None:
        self.issuer = issuer.strip()
        self.key_id = key_id.strip()
        self.key_purpose = key_purpose.strip()
        self.allowed_namespaces = frozenset(allowed_namespaces)
        if (
            not self.issuer
            or not self.key_id
            or not self.key_purpose
            or not self.allowed_namespaces
        ):
            raise ExternalMonotonicRootServiceError("external root signer identity is invalid")
        self._private_key_path = Path(private_key_path)
        _secure_key_bytes(self._private_key_path, private=True)
        self._public_key = _secure_key_bytes(public_key_path, private=False)
        self.public_key_fingerprint = hashlib.sha256(self._public_key).hexdigest()

    def sign(self, *, namespace: str, payload: bytes) -> str:
        if namespace not in self.allowed_namespaces:
            raise ExternalMonotonicRootServiceError("external root signing namespace is forbidden")
        with tempfile.TemporaryDirectory(prefix="rquant-root-sign-") as temporary:
            root = Path(temporary)
            message = root / "message.bin"
            signature = root / "signature.bin"
            message.write_bytes(_domain_payload(namespace=namespace, payload=payload))
            message.chmod(0o600)
            completed = _run_openssl(
                (
                    "pkeyutl",
                    "-sign",
                    "-rawin",
                    "-inkey",
                    str(self._private_key_path),
                    "-in",
                    str(message),
                    "-out",
                    str(signature),
                )
            )
            if completed.returncode != 0:
                raise ExternalMonotonicRootServiceError("external root signing failed")
            raw = signature.read_bytes()
        if len(raw) != 64:
            raise ExternalMonotonicRootServiceError("external root signature is invalid")
        return base64.b64encode(raw).decode("ascii")


class PersistentExternalMonotonicRootBackend:
    """Role-neutral, append-only SQLite compare-and-swap state."""

    def __init__(self, path: Path, *, role: str, authority_id: str, store_id: str) -> None:
        self.path = Path(path).expanduser()
        self.role = role.strip()
        self.authority_id = authority_id.strip()
        self.store_id = store_id.strip()
        if (
            not self.path.is_absolute()
            or ".." in self.path.parts
            or self.path != Path(os.path.abspath(self.path))
            or self.path.resolve(strict=False) != self.path
            or not self.role
            or not self.authority_id
            or not self.store_id
        ):
            raise ExternalMonotonicRootServiceError("external root backend identity is empty")
        try:
            state = secure_create_regular_file(
                self.path,
                allowed_ancestor_uids=frozenset({0, os.geteuid()}),
                expected_uid=os.geteuid(),
                expected_gid=os.getegid(),
                expected_mode=0o600,
            )
        except AuthorityPathSecurityError as exc:
            raise ExternalMonotonicRootServiceError(
                "external root backend directory is untrusted"
            ) from exc
        self._initialize(create_schema=state.created)
        try:
            secure_path_metadata(
                self.path,
                allowed_ancestor_uids=frozenset({0, os.geteuid()}),
                kind="file",
                expected_uid=os.geteuid(),
                expected_gid=os.getegid(),
                expected_mode=0o600,
            )
        except AuthorityPathSecurityError as exc:
            raise ExternalMonotonicRootServiceError(
                "external root backend file is untrusted"
            ) from exc

    def apply(self, request: ExternalMonotonicRootRequest) -> ExternalRootStoredState | None:
        try:
            validated = ExternalMonotonicRootRequest.model_validate(request, strict=True)
        except ValidationError as exc:
            raise ExternalMonotonicRootServiceError("external root request is invalid") from exc
        if (
            validated.role != self.role
            or validated.root_authority_id != self.authority_id
            or validated.root_store_id != self.store_id
        ):
            raise ExternalMonotonicRootServiceError("external root request identity conflicts")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                states, operation_count, journal_head = self._audit(connection)
                current = states.get(validated.subject_authority_id)
                if validated.kind == "current":
                    connection.commit()
                    return current
                assert validated.operation_id is not None
                existing = connection.execute(
                    "SELECT request_json, state_json FROM root_operation WHERE operation_id = ?",
                    (validated.operation_id,),
                ).fetchone()
                request_json = canonical_json_bytes(validated.model_dump(mode="json")).decode()
                if existing is not None:
                    prior_request = strict_model_validate_canonical_json(
                        ExternalMonotonicRootRequest,
                        str(existing[0]),
                    )
                    if _operation_effect_hash(prior_request) != _operation_effect_hash(validated):
                        raise ExternalMonotonicRootServiceError(
                            "external root operation_id was rebound"
                        )
                    result = strict_model_validate_canonical_json(
                        ExternalRootStoredState,
                        str(existing[1]),
                    )
                    connection.commit()
                    return result
                if validated.kind == "pin":
                    if current is not None:
                        raise ExternalMonotonicRootServiceError(
                            "external root subject is already pinned"
                        )
                    if validated.previous_checkpoint_hash != EXTERNAL_MONOTONIC_ROOT_ZERO_HASH:
                        raise ExternalMonotonicRootServiceError(
                            "external root pin predecessor is invalid"
                        )
                elif (
                    current is None or current.checkpoint_hash != validated.previous_checkpoint_hash
                ):
                    raise ExternalMonotonicRootServiceError("external root compare-and-swap failed")
                result = ExternalRootStoredState(
                    role=self.role,
                    root_authority_id=self.authority_id,
                    root_store_id=self.store_id,
                    subject_authority_id=validated.subject_authority_id,
                    operation_id=validated.operation_id,
                    previous_checkpoint_hash=validated.previous_checkpoint_hash or _ZERO_HASH,
                    checkpoint_contract=validated.checkpoint_contract or "invalid",
                    checkpoint_hash=validated.checkpoint_hash or _ZERO_HASH,
                    checkpoint_json=validated.checkpoint_json or "{}",
                )
                state_json = canonical_json_bytes(result.model_dump(mode="json")).decode()
                sequence = operation_count + 1
                journal_hash = canonical_sha256(
                    {
                        "contract": "rquant-external-monotonic-root-journal-entry/v1",
                        "sequence": sequence,
                        "previous_journal_hash": journal_head,
                        "request_hash": validated.request_hash,
                        "state_hash": canonical_sha256(result),
                    }
                )
                connection.execute(
                    "INSERT INTO root_operation VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        validated.operation_id,
                        sequence,
                        request_json,
                        validated.request_hash,
                        state_json,
                        journal_head,
                        journal_hash,
                    ),
                )
                connection.execute(
                    "INSERT INTO root_state(subject_authority_id, state_json) VALUES (?, ?) "
                    "ON CONFLICT(subject_authority_id) DO UPDATE SET "
                    "state_json=excluded.state_json",
                    (validated.subject_authority_id, state_json),
                )
                connection.execute(
                    "UPDATE root_meta SET operation_count = ?, journal_head = ? "
                    "WHERE singleton = 1",
                    (sequence, journal_hash),
                )
                connection.commit()
                return result
            except BaseException:
                connection.rollback()
                raise

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self, *, create_schema: bool) -> None:
        with self._connect() as connection:
            if create_schema:
                connection.executescript(
                    """
                    PRAGMA application_id=1381057874;
                    PRAGMA user_version=1;
                    CREATE TABLE root_meta(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        role TEXT NOT NULL,
                        authority_id TEXT NOT NULL,
                        store_id TEXT NOT NULL,
                        operation_count INTEGER NOT NULL CHECK(operation_count>=0),
                        journal_head TEXT NOT NULL
                    ) STRICT;
                    CREATE TABLE root_state(
                        subject_authority_id TEXT PRIMARY KEY,
                        state_json TEXT NOT NULL
                    ) STRICT;
                    CREATE TABLE root_operation(
                        operation_id TEXT PRIMARY KEY,
                        sequence INTEGER NOT NULL UNIQUE,
                        request_json TEXT NOT NULL,
                        request_hash TEXT NOT NULL,
                        state_json TEXT NOT NULL,
                        previous_journal_hash TEXT NOT NULL,
                        journal_hash TEXT NOT NULL UNIQUE
                    ) STRICT;
                    """
                )
                connection.execute(
                    "INSERT INTO root_meta VALUES (1, ?, ?, ?, 0, ?)",
                    (self.role, self.authority_id, self.store_id, _ZERO_HASH),
                )
                connection.commit()
            self._audit(connection)

    def _audit(
        self, connection: sqlite3.Connection
    ) -> tuple[dict[str, ExternalRootStoredState], int, str]:
        if (
            connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID
            or connection.execute("PRAGMA user_version").fetchone()[0] != _SCHEMA_VERSION
        ):
            raise ExternalMonotonicRootServiceError("external root backend schema is invalid")
        objects = {
            (str(row[0]), str(row[1]))
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        if objects != {
            ("table", "root_meta"),
            ("table", "root_state"),
            ("table", "root_operation"),
        }:
            raise ExternalMonotonicRootServiceError("external root backend objects are invalid")
        meta = connection.execute(
            "SELECT role, authority_id, store_id, operation_count, journal_head "
            "FROM root_meta WHERE singleton=1"
        ).fetchone()
        if meta is None or tuple(meta[:3]) != (self.role, self.authority_id, self.store_id):
            raise ExternalMonotonicRootServiceError("external root backend identity changed")
        states: dict[str, ExternalRootStoredState] = {}
        journal_head = _ZERO_HASH
        count = 0
        for row in connection.execute(
            "SELECT sequence, request_json, request_hash, state_json, "
            "previous_journal_hash, journal_hash FROM root_operation ORDER BY sequence"
        ):
            sequence, request_json, request_hash, state_json, previous, observed_hash = row
            request = strict_model_validate_canonical_json(
                ExternalMonotonicRootRequest, str(request_json)
            )
            state = strict_model_validate_canonical_json(ExternalRootStoredState, str(state_json))
            count += 1
            expected_hash = canonical_sha256(
                {
                    "contract": "rquant-external-monotonic-root-journal-entry/v1",
                    "sequence": count,
                    "previous_journal_hash": journal_head,
                    "request_hash": request.request_hash,
                    "state_hash": canonical_sha256(state),
                }
            )
            if (
                sequence != count
                or request_hash != request.request_hash
                or previous != journal_head
                or observed_hash != expected_hash
            ):
                raise ExternalMonotonicRootServiceError(
                    "external root backend journal integrity failed"
                )
            states[state.subject_authority_id] = state
            journal_head = expected_hash
        materialized = {
            str(subject): strict_model_validate_canonical_json(
                ExternalRootStoredState, str(state_json)
            )
            for subject, state_json in connection.execute(
                "SELECT subject_authority_id, state_json FROM root_state"
            )
        }
        if materialized != states or (int(meta[3]), str(meta[4])) != (count, journal_head):
            raise ExternalMonotonicRootServiceError(
                "external root backend materialized state conflicts"
            )
        return states, count, journal_head


def _peer_credentials(connection: socket.socket) -> tuple[int, int]:
    if hasattr(socket, "SO_PEERCRED"):
        _pid, uid, gid = struct.unpack(
            "3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        )
        return uid, gid
    if hasattr(connection, "getpeereid"):
        return connection.getpeereid()  # type: ignore[attr-defined,no-any-return]
    if hasattr(socket, "LOCAL_PEERCRED"):
        raw = connection.getsockopt(0, socket.LOCAL_PEERCRED, 76)
        version, uid, group_count = struct.unpack_from("=IIh", raw)
        if version != 0 or group_count < 1:
            raise ExternalMonotonicRootServiceError("external root peer credentials are malformed")
        return uid, struct.unpack_from("=i", raw, 12)[0]
    raise ExternalMonotonicRootServiceError("external root peer credentials are unsupported")


def _receive_exact(connection: socket.socket, size: int) -> bytes:
    payload = bytearray()
    while len(payload) < size:
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise ConnectionError("external root service frame was truncated")
        payload.extend(chunk)
    return bytes(payload)


class ExternalMonotonicRootUnixService:
    def __init__(
        self,
        *,
        configuration: ExternalRootServiceConfiguration,
        backend: PersistentExternalMonotonicRootBackend,
        handler: ExternalMonotonicRootRoleHandler,
        probe_signer: ExternalMonotonicRootSigningCapability,
    ) -> None:
        self.configuration = ExternalRootServiceConfiguration.model_validate(
            configuration, strict=True
        )
        self.backend = backend
        self.handler = handler
        self.probe_signer = probe_signer
        if (backend.role, backend.authority_id, backend.store_id) != (
            self.configuration.role,
            self.configuration.authority_id,
            self.configuration.store_id,
        ) or probe_signer.signature_algorithm != "ed25519":
            raise ExternalMonotonicRootServiceError("external root service binding conflicts")
        self.ready = threading.Event()
        self._drop_response_once = False

    def drop_next_response_after_effect_for_test(self) -> None:
        self._drop_response_once = True

    def bind(self) -> socket.socket:
        if (os.geteuid(), os.getegid()) != (
            self.configuration.service_uid,
            self.configuration.service_gid,
        ):
            raise ExternalMonotonicRootServiceError(
                "external root service process identity is untrusted"
            )
        path = self.configuration.socket_path
        parent = path.parent
        try:
            secure_path_metadata(
                parent,
                allowed_ancestor_uids=frozenset(
                    {0, self.configuration.service_uid, self.configuration.socket_uid}
                ),
                kind="directory",
                expected_uid=self.configuration.socket_uid,
                expected_gid=self.configuration.socket_gid,
                expected_mode=self.configuration.socket_directory_mode,
            )
        except AuthorityPathSecurityError as exc:
            raise ExternalMonotonicRootServiceError(
                "external root socket directory is not private"
            ) from exc
        self._remove_verified_stale_endpoint(path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(os.fspath(path))
            os.chmod(path, self.configuration.socket_mode)
            secure_path_metadata(
                path,
                allowed_ancestor_uids=frozenset(
                    {0, self.configuration.service_uid, self.configuration.socket_uid}
                ),
                kind="socket",
                expected_uid=self.configuration.socket_uid,
                expected_gid=self.configuration.socket_gid,
                expected_mode=self.configuration.socket_mode,
            )
            listener.listen(32)
            listener.settimeout(0.05)
            return listener
        except BaseException:
            listener.close()
            path.unlink(missing_ok=True)
            raise

    def _remove_verified_stale_endpoint(self, path: Path) -> None:
        try:
            path.lstat()
        except FileNotFoundError:
            return
        try:
            secure_path_metadata(
                path,
                allowed_ancestor_uids=frozenset(
                    {0, self.configuration.service_uid, self.configuration.socket_uid}
                ),
                kind="socket",
                expected_uid=self.configuration.socket_uid,
                expected_gid=self.configuration.socket_gid,
                expected_mode=self.configuration.socket_mode,
            )
        except AuthorityPathSecurityError as exc:
            raise ExternalMonotonicRootServiceError(
                "external root socket path cannot be replaced"
            ) from exc
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                probe.settimeout(self.configuration.request_timeout_ms / 1_000)
                probe.connect(os.fspath(path))
        except ConnectionRefusedError:
            path.unlink()
            return
        except OSError as exc:
            raise ExternalMonotonicRootServiceError(
                "external root socket path cannot be safely replaced"
            ) from exc
        raise ExternalMonotonicRootServiceError("external root socket is already active")

    def serve_forever(self, *, stop: threading.Event | None = None) -> None:
        stop_event = stop or threading.Event()
        listener = self.bind()
        self.ready.set()
        try:
            while not stop_event.is_set():
                try:
                    self.serve_once(listener)
                except TimeoutError:
                    continue
        finally:
            listener.close()
            self.configuration.socket_path.unlink(missing_ok=True)
            self.ready.clear()

    def serve_once(self, listener: socket.socket) -> None:
        connection, _ = listener.accept()
        with connection:
            connection.settimeout(self.configuration.request_timeout_ms / 1_000)
            try:
                if _peer_credentials(connection) != (
                    self.configuration.allowed_peer_uid,
                    self.configuration.allowed_peer_gid,
                ):
                    raise ExternalMonotonicRootServiceError(
                        "external root service peer is untrusted"
                    )
                size = struct.unpack("!Q", _receive_exact(connection, 8))[0]
                if size > _MAX_FRAME_BYTES:
                    raise ExternalMonotonicRootServiceError(
                        "external root service request is too large"
                    )
                request_json = _receive_exact(connection, size).decode("utf-8")
                raw = strict_canonical_json_loads(request_json)
                if not isinstance(raw, dict):
                    raise ExternalMonotonicRootServiceError(
                        "external root service request is malformed"
                    )
                if (
                    raw.get("contract")
                    == ExternalRootServiceProbeRequest.model_fields["contract"].default
                ):
                    probe = strict_model_validate_canonical_json(
                        ExternalRootServiceProbeRequest, request_json
                    )
                    response = self._probe(probe)
                    response_json = canonical_json_bytes(response.model_dump(mode="json")).decode()
                else:
                    request = strict_model_validate_canonical_json(
                        ExternalMonotonicRootRequest, request_json
                    )
                    state = self.backend.apply(request)
                    response_json = self.handler.response_json(request, state)
                    if response_json is not None:
                        strict_canonical_json_loads(response_json)
                if self._drop_response_once:
                    self._drop_response_once = False
                    return
                response = b"" if response_json is None else response_json.encode("utf-8")
                if len(response) > _MAX_FRAME_BYTES:
                    raise ExternalMonotonicRootServiceError(
                        "external root service response is too large"
                    )
                connection.sendall(struct.pack("!Q", len(response)) + response)
            except (
                ConnectionError,
                ExternalMonotonicRootServiceError,
                OSError,
                ValidationError,
                ValueError,
                UnicodeError,
            ):
                return

    def _probe(self, request: ExternalRootServiceProbeRequest) -> ExternalRootServiceProbeReceipt:
        unsigned = ExternalRootServiceProbeReceipt(
            challenge_nonce=request.challenge_nonce,
            role=self.configuration.role,
            authority_id=self.configuration.authority_id,
            store_id=self.configuration.store_id,
            rollback_domain_id=self.configuration.rollback_domain_id,
            transport_manifest_hash=self.configuration.transport_manifest_hash,
            capabilities=("current", "pin", "advance"),
            issuer=self.probe_signer.issuer,
            key_id=self.probe_signer.key_id,
            key_purpose=self.probe_signer.key_purpose,
            public_key_fingerprint=self.probe_signer.public_key_fingerprint,
            signature="pending",
        )
        return unsigned.model_copy(
            update={
                "signature": self.probe_signer.sign(
                    namespace=EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def wake(self) -> None:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.connect(os.fspath(self.configuration.socket_path))
        except OSError:
            return


def probe_external_monotonic_root_service(
    manifest: UnixSocketExternalMonotonicRootManifest,
    *,
    verifier: ClosedExternalMonotonicRootVerifier,
    expected_transport_manifest_hash: str,
) -> ExternalRootServiceProbeReceipt:
    if type(verifier) is not ClosedExternalMonotonicRootVerifier:
        raise ExternalMonotonicRootServiceError(
            "external root service probe requires the closed verifier"
        )
    validated = UnixSocketExternalMonotonicRootManifest.model_validate(manifest, strict=True)
    UnixSocketExternalMonotonicRootClient(validated)
    nonce = os.urandom(32).hex()
    request = ExternalRootServiceProbeRequest(challenge_nonce=nonce)
    payload = canonical_json_bytes(request.model_dump(mode="json"))
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(validated.connect_timeout_ms / 1_000)
            connection.connect(os.fspath(validated.socket_path))
            if _peer_credentials(connection) != (validated.peer_uid, validated.peer_gid):
                raise ExternalMonotonicRootServiceError(
                    "external root service probe peer identity changed"
                )
            connection.sendall(struct.pack("!Q", len(payload)) + payload)
            size = struct.unpack("!Q", _receive_exact(connection, 8))[0]
            if size > validated.max_response_bytes:
                raise ExternalMonotonicRootServiceError(
                    "external root service probe response is too large"
                )
            response_json = _receive_exact(connection, size).decode("utf-8")
    except (OSError, UnicodeError, struct.error, ConnectionError) as exc:
        raise ExternalMonotonicRootServiceError(
            "external root service probe transport failed"
        ) from exc
    receipt = strict_model_validate_canonical_json(ExternalRootServiceProbeReceipt, response_json)
    if (
        receipt.challenge_nonce != nonce
        or (receipt.role, receipt.authority_id, receipt.store_id, receipt.rollback_domain_id)
        != (
            validated.role,
            validated.authority_id,
            validated.store_id,
            validated.rollback_domain_id,
        )
        or receipt.transport_manifest_hash != expected_transport_manifest_hash
        or (
            receipt.issuer,
            receipt.key_id,
            receipt.key_purpose,
            receipt.public_key_fingerprint,
        )
        != (
            verifier.issuer,
            verifier.key_id,
            verifier.key_purpose,
            verifier.public_key_fingerprint,
        )
        or not verifier.verify(
            namespace=EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE,
            payload=receipt.signing_bytes(),
            signature=receipt.signature,
        )
    ):
        raise ExternalMonotonicRootServiceError(
            "external root service probe identity or capability is invalid"
        )
    return receipt


__all__ = [
    "ClosedExternalMonotonicRootVerifier",
    "EXTERNAL_ROOT_SERVICE_PROBE_NAMESPACE",
    "ExternalMonotonicRootRoleHandler",
    "ExternalMonotonicRootServiceError",
    "ExternalMonotonicRootUnixService",
    "ExternalRootServiceConfiguration",
    "ExternalRootServiceProbeReceipt",
    "ExternalRootStoredState",
    "OpenSslExternalMonotonicRootSigner",
    "PersistentExternalMonotonicRootBackend",
    "probe_external_monotonic_root_service",
]
