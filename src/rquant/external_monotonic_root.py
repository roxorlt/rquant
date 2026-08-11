"""Closed transport and trust boundary for externally hosted monotonic roots."""

from __future__ import annotations

import os
import socket
import stat
import struct
import time
from pathlib import Path
from typing import Final, Literal, Protocol, Self, final

from pydantic import Field, ValidationError, field_validator, model_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strict_json import (
    canonical_model_json_bytes,
    strict_canonical_json_loads,
)

EXTERNAL_MONOTONIC_ROOT_ADAPTER_ID = "rquant-external-monotonic-root-cas-v1"
EXTERNAL_MONOTONIC_ROOT_TRANSPORT = "unix-socket-v1"
EXTERNAL_MONOTONIC_ROOT_TEST_TRANSPORT = "nonproduction-inprocess-v1"
EXTERNAL_MONOTONIC_ROOT_ZERO_HASH = "0" * 64
_MAX_FRAME_BYTES: Final = 8 * 1024 * 1024


class ExternalMonotonicRootSecurityError(RuntimeError):
    """The external root transport, binding, or signed response is untrusted."""


class ExternalMonotonicRootConfig(RuntimeContractModel):
    """Versioned trust binding for one role-specific external CAS authority."""

    schema_version: Literal[1] = 1
    contract: Literal["rquant-external-monotonic-root-config/v1"] = (
        "rquant-external-monotonic-root-config/v1"
    )
    adapter_id: Literal["rquant-external-monotonic-root-cas-v1"] = (
        EXTERNAL_MONOTONIC_ROOT_ADAPTER_ID
    )
    transport: Literal["unix-socket-v1", "nonproduction-inprocess-v1"]
    transport_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    role: str = Field(min_length=1, max_length=200)
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    root_issuer: str = Field(min_length=1, max_length=200)
    root_key_id: str = Field(min_length=1, max_length=200)
    root_key_purpose: str = Field(min_length=1, max_length=200)
    root_receipt_namespace: str = Field(min_length=1, max_length=300)
    root_signature_algorithm: Literal["ed25519"] = "ed25519"
    root_public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    witness_rollback_domain_id: str = Field(min_length=1, max_length=200)
    local_rollback_domain_id: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_independent_rollback_domain(self) -> Self:
        if self.witness_rollback_domain_id == self.local_rollback_domain_id:
            raise ValueError("external witness must use an independent rollback domain")
        return self

    @property
    def config_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ExternalMonotonicRootRequest(RuntimeContractModel):
    """Canonical role-neutral request sent to an external monotonic CAS service."""

    schema_version: Literal[1] = 1
    contract: Literal["rquant-external-monotonic-root-request/v1"] = (
        "rquant-external-monotonic-root-request/v1"
    )
    kind: Literal["current", "pin", "advance"]
    role: str = Field(min_length=1, max_length=200)
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    subject_authority_id: str = Field(min_length=1, max_length=200)
    challenge_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    checkpoint_contract: str | None = Field(default=None, min_length=1, max_length=300)
    checkpoint_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    checkpoint_json: str | None = None

    @field_validator("checkpoint_json", mode="before")
    @classmethod
    def preserve_canonical_checkpoint_json(cls, value: object) -> object:
        if value is not None:
            if not isinstance(value, str):
                raise ValueError("external root checkpoint JSON must be text")
            try:
                strict_canonical_json_loads(value)
            except (TypeError, ValueError) as exc:
                raise ValueError("external root checkpoint JSON must be canonical") from exc
        return value

    @model_validator(mode="after")
    def validate_shape_and_checkpoint(self) -> Self:
        mutation_values = (
            self.operation_id,
            self.previous_checkpoint_hash,
            self.checkpoint_contract,
            self.checkpoint_hash,
            self.checkpoint_json,
        )
        if self.kind == "current":
            if any(value is not None for value in mutation_values):
                raise ValueError("current request cannot contain mutation fields")
        else:
            if any(value is None for value in mutation_values):
                raise ValueError("mutation request requires all checkpoint fields")
            if (
                self.kind == "pin"
                and self.previous_checkpoint_hash != EXTERNAL_MONOTONIC_ROOT_ZERO_HASH
            ):
                raise ValueError("pin request predecessor must be the zero hash")
            try:
                checkpoint = strict_canonical_json_loads(self.checkpoint_json or "")
            except (TypeError, ValueError) as exc:
                raise ValueError("external root checkpoint JSON must be canonical") from exc
            if (
                not isinstance(checkpoint, dict)
                or checkpoint.get("contract") != self.checkpoint_contract
                or canonical_sha256(checkpoint) != self.checkpoint_hash
            ):
                raise ValueError("external root checkpoint contract or hash is invalid")
        if self.request_hash != self.calculated_request_hash:
            raise ValueError("external root canonical request hash is invalid")
        return self

    @property
    def calculated_request_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"request_hash"}))

    @classmethod
    def close(cls, **values: object) -> ExternalMonotonicRootRequest:
        unsigned = cls.model_construct(request_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH, **values)
        return cls.model_validate(
            {
                **unsigned.model_dump(mode="python"),
                "request_hash": unsigned.calculated_request_hash,
            },
            strict=True,
        )


class ExternalMonotonicRootReceiptIdentity(RuntimeContractModel):
    """Closed signer and authority identity extracted from a role receipt."""

    role: str = Field(min_length=1, max_length=200)
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    closed: Literal[True]
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: str = Field(min_length=1, max_length=200)
    namespace: str = Field(min_length=1, max_length=300)
    signature_algorithm: Literal["ed25519"]
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class ExternalMonotonicRootClient(Protocol):
    """Role-neutral transport for a separately operated monotonic CAS service."""

    @property
    def role(self) -> str: ...

    @property
    def authority_id(self) -> str: ...

    @property
    def store_id(self) -> str: ...

    @property
    def transport(self) -> Literal["unix-socket-v1", "nonproduction-inprocess-v1"]: ...

    @property
    def manifest_hash(self) -> str: ...

    @property
    def rollback_domain_id(self) -> str: ...

    def invoke(self, *, request_json: str) -> str | None: ...


class UnixSocketExternalMonotonicRootManifest(RuntimeContractModel):
    """Closed production transport manifest for one Unix peer service."""

    schema_version: Literal[1] = 1
    contract: Literal["rquant-external-monotonic-root-unix-peer/v1"] = (
        "rquant-external-monotonic-root-unix-peer/v1"
    )
    transport: Literal["unix-socket-v1"] = EXTERNAL_MONOTONIC_ROOT_TRANSPORT
    role: str = Field(min_length=1, max_length=200)
    authority_id: str = Field(min_length=1, max_length=200)
    store_id: str = Field(min_length=1, max_length=200)
    rollback_domain_id: str = Field(min_length=1, max_length=200)
    socket_path: Path
    socket_uid: int = Field(strict=True, ge=0)
    socket_gid: int = Field(strict=True, ge=0)
    socket_mode: Literal[0o600, 0o660] = 0o600
    peer_uid: int = Field(strict=True, ge=0)
    peer_gid: int = Field(strict=True, ge=0)
    connect_timeout_ms: int = Field(strict=True, ge=1, le=30_000)
    max_response_bytes: int = Field(strict=True, ge=1, le=_MAX_FRAME_BYTES)

    @model_validator(mode="after")
    def validate_absolute_socket_path(self) -> Self:
        if not self.socket_path.is_absolute():
            raise ValueError("external root Unix socket path must be absolute")
        return self

    @property
    def manifest_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


@final
class UnixSocketExternalMonotonicRootClient:
    """Length-framed Unix client with path, file mode, and peer credential pinning."""

    def __init__(self, manifest: UnixSocketExternalMonotonicRootManifest) -> None:
        try:
            self._manifest = UnixSocketExternalMonotonicRootManifest.model_validate(
                manifest,
                strict=True,
            )
        except ValidationError as exc:
            raise ExternalMonotonicRootSecurityError(
                "external root Unix peer manifest is invalid"
            ) from exc
        self._validate_socket_path()

    @property
    def role(self) -> str:
        return self._manifest.role

    @property
    def authority_id(self) -> str:
        return self._manifest.authority_id

    @property
    def store_id(self) -> str:
        return self._manifest.store_id

    @property
    def transport(self) -> Literal["unix-socket-v1"]:
        return EXTERNAL_MONOTONIC_ROOT_TRANSPORT

    @property
    def manifest_hash(self) -> str:
        return self._manifest.manifest_hash

    @property
    def rollback_domain_id(self) -> str:
        return self._manifest.rollback_domain_id

    def invoke(self, *, request_json: str) -> str | None:
        try:
            strict_canonical_json_loads(request_json)
        except (TypeError, ValueError) as exc:
            raise ExternalMonotonicRootSecurityError(
                "external root Unix request must be canonical JSON"
            ) from exc
        request_bytes = request_json.encode("utf-8")
        if len(request_bytes) > _MAX_FRAME_BYTES:
            raise ExternalMonotonicRootSecurityError("external root Unix request is too large")
        self._validate_socket_path()
        deadline = time.monotonic() + self._manifest.connect_timeout_ms / 1_000
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(self._remaining(deadline))
                connection.connect(os.fspath(self._manifest.socket_path))
                connection.settimeout(self._remaining(deadline))
                self._validate_peer(connection)
                connection.settimeout(self._remaining(deadline))
                connection.sendall(struct.pack("!Q", len(request_bytes)) + request_bytes)
                size = struct.unpack("!Q", self._receive_exact(connection, 8, deadline))[0]
                if size > self._manifest.max_response_bytes:
                    raise ExternalMonotonicRootSecurityError(
                        "external root Unix response is too large"
                    )
                if size == 0:
                    return None
                response = self._receive_exact(connection, size, deadline).decode("utf-8")
        except ExternalMonotonicRootSecurityError:
            raise
        except (OSError, UnicodeError, struct.error) as exc:
            raise ConnectionError("external root Unix transport failed closed") from exc
        try:
            strict_canonical_json_loads(response)
        except (TypeError, ValueError) as exc:
            raise ExternalMonotonicRootSecurityError(
                "external root Unix response is not canonical JSON"
            ) from exc
        return response

    def _validate_socket_path(self) -> None:
        path = self._manifest.socket_path
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise ExternalMonotonicRootSecurityError(
                "external root Unix socket is unavailable"
            ) from exc
        if (
            not stat.S_ISSOCK(metadata.st_mode)
            or metadata.st_uid != self._manifest.socket_uid
            or metadata.st_gid != self._manifest.socket_gid
            or stat.S_IMODE(metadata.st_mode) != self._manifest.socket_mode
        ):
            raise ExternalMonotonicRootSecurityError(
                "external root Unix socket path, owner, group, or mode is untrusted"
            )

    def _validate_peer(self, connection: socket.socket) -> None:
        if hasattr(connection, "getpeereid"):
            peer_uid, peer_gid = connection.getpeereid()  # type: ignore[attr-defined]
        elif hasattr(socket, "SO_PEERCRED"):
            credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            if len(credentials) != 12:
                raise ExternalMonotonicRootSecurityError(
                    "external root Unix peer credentials are malformed"
                )
            _peer_pid, peer_uid, peer_gid = struct.unpack("3i", credentials)
        elif hasattr(socket, "LOCAL_PEERCRED"):
            credentials = connection.getsockopt(0, socket.LOCAL_PEERCRED, 76)
            if len(credentials) != 76:
                raise ExternalMonotonicRootSecurityError(
                    "external root Unix peer credentials are malformed"
                )
            version, peer_uid, group_count = struct.unpack_from("=IIh", credentials)
            if version != 0 or group_count < 1:
                raise ExternalMonotonicRootSecurityError(
                    "external root Unix peer credentials are malformed"
                )
            peer_gid = struct.unpack_from("=i", credentials, 12)[0]
        else:
            raise ExternalMonotonicRootSecurityError(
                "external root Unix peer credentials are unsupported"
            )
        if (peer_uid, peer_gid) != (self._manifest.peer_uid, self._manifest.peer_gid):
            raise ExternalMonotonicRootSecurityError("external root Unix peer identity changed")

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("external root Unix total deadline expired")
        return remaining

    @classmethod
    def _receive_exact(
        cls,
        connection: socket.socket,
        size: int,
        deadline: float,
    ) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            connection.settimeout(cls._remaining(deadline))
            chunk = connection.recv(size - len(chunks))
            if not chunk:
                raise ConnectionError("external root Unix response was truncated")
            chunks.extend(chunk)
        return bytes(chunks)


class ExternalMonotonicRootSignatureVerifier(Protocol):
    issuer: str
    key_id: str
    key_purpose: str
    signature_algorithm: Literal["ed25519"]
    public_key_fingerprint: str

    def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool: ...


class ExternalMonotonicRootTrustBoundary:
    """Nominal, closed trust boundary shared by role-specific root adapters."""

    def __init__(
        self,
        *,
        config: ExternalMonotonicRootConfig,
        client: ExternalMonotonicRootClient,
        root_verifiers: tuple[ExternalMonotonicRootSignatureVerifier, ...],
    ) -> None:
        try:
            self._config = ExternalMonotonicRootConfig.model_validate(config, strict=True)
        except ValidationError as exc:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root config is invalid"
            ) from exc
        if not isinstance(root_verifiers, tuple) or len(root_verifiers) != 1:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root requires one closed pinned verifier"
            )
        self._client = client
        self._verifier = root_verifiers[0]
        self._require_client_identity()
        self._require_verifier_identity()

    @property
    def config(self) -> ExternalMonotonicRootConfig:
        return self._config

    def invoke(self, request: ExternalMonotonicRootRequest) -> str | None:
        try:
            validated = ExternalMonotonicRootRequest.model_validate(request, strict=True)
        except ValidationError as exc:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root request is invalid"
            ) from exc
        if (
            validated.role != self._config.role
            or validated.root_authority_id != self._config.root_authority_id
            or validated.root_store_id != self._config.root_store_id
        ):
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root request conflicts with the trust binding"
            )
        self._require_client_identity()
        response = self._client.invoke(
            request_json=canonical_model_json_bytes(validated).decode("utf-8")
        )
        self._require_client_identity()
        if validated.kind == "current":
            return response
        if not isinstance(response, str):
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root mutation returned no receipt"
            )
        return response

    def verify_receipt(
        self,
        *,
        identity: ExternalMonotonicRootReceiptIdentity,
        signing_bytes: bytes,
        signature: str,
    ) -> None:
        try:
            validated = ExternalMonotonicRootReceiptIdentity.model_validate(
                identity,
                strict=True,
            )
        except ValidationError as exc:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root receipt identity is invalid"
            ) from exc
        expected = (
            self._config.role,
            self._config.root_authority_id,
            self._config.root_store_id,
            True,
            self._config.root_issuer,
            self._config.root_key_id,
            self._config.root_key_purpose,
            self._config.root_receipt_namespace,
            self._config.root_signature_algorithm,
            self._config.root_public_key_fingerprint,
        )
        observed = (
            validated.role,
            validated.root_authority_id,
            validated.root_store_id,
            validated.closed,
            validated.issuer,
            validated.key_id,
            validated.key_purpose,
            validated.namespace,
            validated.signature_algorithm,
            validated.public_key_fingerprint,
        )
        if observed != expected:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root receipt identity conflicts with the trust binding"
            )
        self._require_verifier_identity()
        try:
            trusted = self._verifier.verify(
                namespace=self._config.root_receipt_namespace,
                payload=signing_bytes,
                signature=signature,
            )
        except Exception as exc:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root receipt verification failed"
            ) from exc
        if not trusted:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root receipt verification failed"
            )

    def _require_client_identity(self) -> None:
        observed = (
            self._client.role,
            self._client.authority_id,
            self._client.store_id,
            self._client.transport,
            self._client.manifest_hash,
            self._client.rollback_domain_id,
        )
        expected = (
            self._config.role,
            self._config.root_authority_id,
            self._config.root_store_id,
            self._config.transport,
            self._config.transport_manifest_hash,
            self._config.witness_rollback_domain_id,
        )
        if observed != expected:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root client identity or rollback domain changed"
            )

    def _require_verifier_identity(self) -> None:
        observed = (
            self._verifier.issuer,
            self._verifier.key_id,
            self._verifier.key_purpose,
            self._verifier.signature_algorithm,
            self._verifier.public_key_fingerprint,
        )
        expected = (
            self._config.root_issuer,
            self._config.root_key_id,
            self._config.root_key_purpose,
            self._config.root_signature_algorithm,
            self._config.root_public_key_fingerprint,
        )
        if observed != expected:
            raise ExternalMonotonicRootSecurityError(
                "external monotonic root verifier conflicts with the trust binding"
            )


__all__ = [
    "EXTERNAL_MONOTONIC_ROOT_ADAPTER_ID",
    "EXTERNAL_MONOTONIC_ROOT_TRANSPORT",
    "EXTERNAL_MONOTONIC_ROOT_TEST_TRANSPORT",
    "EXTERNAL_MONOTONIC_ROOT_ZERO_HASH",
    "ExternalMonotonicRootClient",
    "ExternalMonotonicRootConfig",
    "ExternalMonotonicRootReceiptIdentity",
    "ExternalMonotonicRootRequest",
    "ExternalMonotonicRootSecurityError",
    "ExternalMonotonicRootSignatureVerifier",
    "ExternalMonotonicRootTrustBoundary",
    "UnixSocketExternalMonotonicRootClient",
    "UnixSocketExternalMonotonicRootManifest",
]
