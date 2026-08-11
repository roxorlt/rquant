"""Closed Unix-socket adapter for the resource-journal admission authority.

The Strategy Lab worker can be spawned on an untrusted code path.  This module
keeps the signing authority, its anti-rollback root and resource probes on the
authority side of a Unix socket.  A spawned worker receives only a frozen,
versioned descriptor and canonical JSON bytes.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import socket
import struct
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, ValidationError, model_validator

from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    secure_path_metadata,
)
from rquant.external_monotonic_root import (
    EXTERNAL_MONOTONIC_ROOT_TRANSPORT,
    ExternalMonotonicRootClient,
    ExternalMonotonicRootConfig,
    ExternalMonotonicRootReceiptIdentity,
    ExternalMonotonicRootRequest,
    ExternalMonotonicRootSecurityError,
    ExternalMonotonicRootSignatureVerifier,
    ExternalMonotonicRootTrustBoundary,
    UnixSocketExternalMonotonicRootClient,
)
from rquant.external_monotonic_root_service import ClosedExternalMonotonicRootVerifier
from rquant.resource_admission import (
    AdmissionPolicy,
    AdmissionRequest,
    ResourceReservationIdentity,
    ResourceReservationLease,
    ResourceSnapshot,
)
from rquant.resource_journal_high_water import (
    RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
    RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH,
    ResourceJournalAntiRollbackReceipt,
    ResourceJournalHighWaterCheckpoint,
    ResourceJournalHighWaterError,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_resource_admission import (
    ResourceOperationReceipt,
    ResourceOperationResult,
    ResourceReservationAdmission,
    RuntimeResourceAdmissionError,
    SQLiteResourceAdmissionAuthority,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

LAB_RESOURCE_AUTHORITY_REGISTRY_ID = "rquant.lab-authority.resource-journal"
LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION = 2
LAB_RESOURCE_AUTHORITY_REGISTRY_HASH = hashlib.sha256(
    b"rquant:lab-authority:resource-journal:v2"
).hexdigest()
RESOURCE_AUTHORITY_ADAPTER_MAX_WIRE_BYTES = 1024 * 1024
_FRAME_HEADER_BYTES = 4
_RESOURCE_AUTHORITY_CONTRACT = "rquant-lab-resource-authority-adapter/v1"
_RESOURCE_ROOT_ROLE = "resource_journal_monotonic_root"


class ResourceAuthorityAdapterError(RuntimeError):
    """A closed error returned by the external resource authority boundary."""


class ResourceAuthorityAdapterConfigurationError(ResourceAuthorityAdapterError):
    """The immutable descriptor cannot identify a trusted authority."""


class ResourceAuthorityAdapterTransportError(ResourceAuthorityAdapterError):
    """The Unix transport did not return one complete authenticated response."""


class ResourceAuthorityAdapterRemoteError(ResourceAuthorityAdapterError):
    """The remote authority rejected a request without exposing internal details."""


class _AdapterModel(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ExternalResourceJournalRootConfig(ExternalMonotonicRootConfig):
    """Role-specific binding consumed by the shared external root runtime."""

    role: Literal["resource_journal_monotonic_root"] = _RESOURCE_ROOT_ROLE
    root_key_purpose: Literal["resource-journal-high-water"] = RESOURCE_JOURNAL_HIGH_WATER_PURPOSE
    root_receipt_namespace: Literal["rquant-resource-journal-anti-rollback-root-receipt/v1"] = (
        RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE
    )


class ResourceJournalExternalRootReceipt(_AdapterModel):
    """Fresh outer receipt binding a generic root request to resource state."""

    schema_version: Literal[1] = 1
    contract: Literal["rquant-resource-journal-external-root-receipt/v1"] = (
        "rquant-resource-journal-external-root-receipt/v1"
    )
    role: Literal["resource_journal_monotonic_root"] = _RESOURCE_ROOT_ROLE
    root_authority_id: str = Field(min_length=1, max_length=200)
    root_store_id: str = Field(min_length=1, max_length=200)
    journal_authority_id: str = Field(min_length=1, max_length=200)
    request_kind: Literal["current", "pin", "advance"]
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt: ResourceJournalAntiRollbackReceipt
    closed: Literal[True] = True
    issuer: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    key_purpose: Literal["resource-journal-high-water"] = RESOURCE_JOURNAL_HIGH_WATER_PURPOSE
    namespace: Literal["rquant-resource-journal-anti-rollback-root-receipt/v1"] = (
        RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE
    )
    signature_algorithm: Literal["ed25519"] = "ed25519"
    public_key_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class ExternalResourceJournalMonotonicRootAdapter:
    """Resource-journal role adapter over the shared external CAS runtime."""

    def __init__(
        self,
        *,
        config: ExternalResourceJournalRootConfig,
        client: UnixSocketExternalMonotonicRootClient,
        root_verifiers: tuple[ClosedExternalMonotonicRootVerifier, ...],
    ) -> None:
        if type(client) is not UnixSocketExternalMonotonicRootClient:
            raise ResourceAuthorityAdapterConfigurationError(
                "production external resource root requires the closed Unix peer client"
            )
        if (
            type(root_verifiers) is not tuple
            or len(root_verifiers) != 1
            or type(root_verifiers[0]) is not ClosedExternalMonotonicRootVerifier
        ):
            raise ResourceAuthorityAdapterConfigurationError(
                "production external resource root requires the closed verifier"
            )
        self._initialize(
            config=config,
            client=client,
            root_verifiers=root_verifiers,
            production_ready=True,
        )

    @classmethod
    def for_nonproduction_test(
        cls,
        *,
        config: ExternalResourceJournalRootConfig,
        client: ExternalMonotonicRootClient,
        root_verifiers: tuple[ExternalMonotonicRootSignatureVerifier, ...],
    ) -> ExternalResourceJournalMonotonicRootAdapter:
        instance = cls.__new__(cls)
        instance._initialize(
            config=config,
            client=client,
            root_verifiers=root_verifiers,
            production_ready=False,
        )
        return instance

    def _initialize(
        self,
        *,
        config: ExternalResourceJournalRootConfig,
        client: ExternalMonotonicRootClient,
        root_verifiers: tuple[ExternalMonotonicRootSignatureVerifier, ...],
        production_ready: bool,
    ) -> None:
        try:
            validated = ExternalResourceJournalRootConfig.model_validate(config, strict=True)
            trust = ExternalMonotonicRootTrustBoundary(
                config=validated,
                client=client,
                root_verifiers=root_verifiers,
            )
        except (ValidationError, ExternalMonotonicRootSecurityError, AttributeError) as exc:
            raise ResourceAuthorityAdapterConfigurationError(
                "external resource root adapter configuration is invalid"
            ) from exc
        self._config = validated
        self._trust = trust
        self._production_ready = production_ready

    @property
    def authority_id(self) -> str:
        return self._config.root_authority_id

    @property
    def verifier_fingerprints(self) -> frozenset[str]:
        return frozenset({self._config.root_public_key_fingerprint})

    @property
    def config(self) -> ExternalResourceJournalRootConfig:
        return self._config

    @property
    def production_ready(self) -> bool:
        return self._production_ready

    def current(
        self,
        *,
        journal_authority_id: str,
    ) -> ResourceJournalAntiRollbackReceipt | None:
        request = ExternalMonotonicRootRequest.close(
            kind="current",
            role=self._config.role,
            root_authority_id=self._config.root_authority_id,
            root_store_id=self._config.root_store_id,
            subject_authority_id=journal_authority_id,
            challenge_nonce=secrets.token_hex(32),
        )
        response = self._invoke(request)
        return None if response is None else self._require_verified(response, request=request)

    def pin(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        return self._mutate(
            kind="pin",
            operation_id=operation_id,
            high_water_authority_id=high_water_authority_id,
            journal_authority_id=journal_authority_id,
            previous_checkpoint_hash=RESOURCE_JOURNAL_HIGH_WATER_ZERO_HASH,
            checkpoint=checkpoint,
        )

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        return self._mutate(
            kind="advance",
            operation_id=operation_id,
            high_water_authority_id=high_water_authority_id,
            journal_authority_id=journal_authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
        )

    def _mutate(
        self,
        *,
        kind: Literal["pin", "advance"],
        operation_id: str,
        high_water_authority_id: str,
        journal_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: ResourceJournalHighWaterCheckpoint,
    ) -> ResourceJournalAntiRollbackReceipt:
        request = ExternalMonotonicRootRequest.close(
            kind=kind,
            role=self._config.role,
            root_authority_id=self._config.root_authority_id,
            root_store_id=self._config.root_store_id,
            subject_authority_id=journal_authority_id,
            challenge_nonce=secrets.token_hex(32),
            operation_id=operation_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint_contract=checkpoint.contract,
            checkpoint_hash=checkpoint.checkpoint_hash,
            checkpoint_json=canonical_model_json_bytes(checkpoint).decode("utf-8"),
        )
        response = self._invoke(request)
        if response is None:
            raise ResourceJournalHighWaterError(
                "external resource root mutation returned no receipt"
            )
        receipt = self._require_verified(response, request=request)
        if (
            receipt.operation_id != operation_id
            or receipt.high_water_authority_id != high_water_authority_id
            or receipt.journal_authority_id != journal_authority_id
            or receipt.previous_checkpoint_hash != previous_checkpoint_hash
            or receipt.checkpoint != checkpoint
        ):
            raise ResourceJournalHighWaterError(
                "external resource root receipt conflicts with the request"
            )
        return receipt

    def _invoke(self, request: ExternalMonotonicRootRequest) -> str | None:
        try:
            return self._trust.invoke(request)
        except Exception as exc:
            raise ResourceJournalHighWaterError("external resource root invocation failed") from exc

    def _require_verified(
        self,
        receipt_json: str,
        *,
        request: ExternalMonotonicRootRequest,
    ) -> ResourceJournalAntiRollbackReceipt:
        try:
            external_receipt = strict_model_validate_canonical_json(
                ResourceJournalExternalRootReceipt,
                receipt_json,
            )
            if (
                external_receipt.role != request.role
                or external_receipt.root_authority_id != request.root_authority_id
                or external_receipt.root_store_id != request.root_store_id
                or external_receipt.journal_authority_id != request.subject_authority_id
                or external_receipt.request_kind != request.kind
                or external_receipt.request_hash != request.request_hash
                or external_receipt.challenge_nonce != request.challenge_nonce
            ):
                raise ValueError("external root receipt does not bind the request")
            self._trust.verify_receipt(
                identity=ExternalMonotonicRootReceiptIdentity(
                    role=external_receipt.role,
                    root_authority_id=external_receipt.root_authority_id,
                    root_store_id=external_receipt.root_store_id,
                    closed=external_receipt.closed,
                    issuer=external_receipt.issuer,
                    key_id=external_receipt.key_id,
                    key_purpose=external_receipt.key_purpose,
                    namespace=external_receipt.namespace,
                    signature_algorithm=external_receipt.signature_algorithm,
                    public_key_fingerprint=external_receipt.public_key_fingerprint,
                ),
                signing_bytes=external_receipt.signing_bytes(),
                signature=external_receipt.signature,
            )
        except (TypeError, ValueError, ValidationError, ExternalMonotonicRootSecurityError) as exc:
            raise ResourceJournalHighWaterError(
                "external resource root receipt is invalid"
            ) from exc
        return external_receipt.receipt


class ResourceAuthorityAdapterConfig(_AdapterModel):
    """Immutable descriptor passed through the Lab closed registry.

    ``production`` deliberately requires a separately named anti-rollback root.
    The endpoint itself is only a transport location; identity is bound again in
    every response so replacing a socket with another local authority fails
    closed.
    """

    schema_version: Literal[1] = 1
    mode: Literal["production", "test-standalone"]
    endpoint: Path
    expected_uid: int = Field(ge=0)
    expected_gid: int = Field(ge=0)
    expected_server_uid: int | None = Field(default=None, ge=0)
    expected_server_gid: int | None = Field(default=None, ge=0)
    allowed_peer_uid: int | None = Field(default=None, ge=0)
    allowed_peer_gid: int | None = Field(default=None, ge=0)
    socket_mode: Literal[0o600, 0o660] = 0o600
    socket_directory_mode: Literal[0o700, 0o750] = 0o700
    authority_id: str = Field(min_length=1, max_length=200)
    high_water_authority_id: str | None = Field(default=None, max_length=200)
    external_root_config: ExternalResourceJournalRootConfig | None = None
    trusted_role_inventory_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    timeout_milliseconds: int = Field(default=1_000, ge=1, le=30_000)

    @model_validator(mode="after")
    def validate_closed_production_binding(self) -> ResourceAuthorityAdapterConfig:
        endpoint = Path(self.endpoint)
        if not endpoint.is_absolute() or ".." in endpoint.parts:
            raise ValueError("resource authority endpoint must be an absolute normalized path")
        if len(os.fsencode(endpoint)) > 100:
            raise ValueError("resource authority endpoint exceeds the portable Unix socket limit")
        if (self.expected_server_uid is None) != (self.expected_server_gid is None):
            raise ValueError("resource authority server peer identity must be complete")
        if (self.allowed_peer_uid is None) != (self.allowed_peer_gid is None):
            raise ValueError("resource authority allowed peer identity must be complete")
        if self.mode == "production":
            root = self.external_root_config
            if not self.high_water_authority_id or root is None:
                raise ValueError(
                    "production resource authority requires an anti-rollback root "
                    "runtime and high-water authority"
                )
            if root.transport != EXTERNAL_MONOTONIC_ROOT_TRANSPORT:
                raise ValueError(
                    "production resource authority requires the registered external root transport"
                )
            if (
                len(
                    {
                        self.authority_id,
                        self.high_water_authority_id,
                        root.root_authority_id,
                    }
                )
                != 3
            ):
                raise ValueError(
                    "resource, high-water, and anti-rollback root authorities must be independent"
                )
        elif self.high_water_authority_id is not None or self.external_root_config is not None:
            raise ValueError(
                "test-standalone resource authority cannot configure a production root"
            )
        return self

    @property
    def non_production(self) -> bool:
        return self.mode != "production"

    @property
    def expected_server_identity(self) -> tuple[int, int]:
        return (
            self.expected_uid if self.expected_server_uid is None else self.expected_server_uid,
            self.expected_gid if self.expected_server_gid is None else self.expected_server_gid,
        )

    @property
    def allowed_peer_identity(self) -> tuple[int, int]:
        return (
            self.expected_uid if self.allowed_peer_uid is None else self.allowed_peer_uid,
            self.expected_gid if self.allowed_peer_gid is None else self.allowed_peer_gid,
        )


class ResourceAuthorityAdapterIdentity(_AdapterModel):
    schema_version: Literal[1] = 1
    mode: Literal["production", "test-standalone"]
    authority_id: str = Field(min_length=1, max_length=200)
    high_water_authority_id: str | None = Field(default=None, max_length=200)
    external_root_config: ExternalResourceJournalRootConfig | None = None
    trusted_role_inventory_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_mode(self) -> ResourceAuthorityAdapterIdentity:
        if self.mode == "production" and (
            not self.high_water_authority_id or self.external_root_config is None
        ):
            raise ValueError("production identity requires an external anti-rollback root runtime")
        if self.mode == "test-standalone" and (
            self.high_water_authority_id is not None or self.external_root_config is not None
        ):
            raise ValueError("test-standalone identity cannot carry a production root")
        return self


def parse_resource_authority_adapter_config(
    payload: str,
) -> ResourceAuthorityAdapterConfig:
    if not isinstance(payload, str) or not payload:
        raise ResourceAuthorityAdapterConfigurationError(
            "resource authority V2 configuration is missing"
        )
    encoded = payload.encode("utf-8")
    if len(encoded) > RESOURCE_AUTHORITY_ADAPTER_MAX_WIRE_BYTES:
        raise ResourceAuthorityAdapterConfigurationError(
            "resource authority V2 configuration exceeds the wire bound"
        )
    try:
        strict_canonical_json_loads(encoded)
        return ResourceAuthorityAdapterConfig.model_validate_json(encoded, strict=True)
    except (TypeError, ValueError, ValidationError) as exc:
        raise ResourceAuthorityAdapterConfigurationError(
            "resource authority V2 configuration is malformed"
        ) from exc


class ResourceAuthorityAdapterRequest(_AdapterModel):
    schema_version: Literal[1] = 1
    message_type: Literal["resource-authority-request"] = "resource-authority-request"
    operation: Literal[
        "probe",
        "policy",
        "snapshot",
        "admission",
        "reserve",
        "recheck",
        "release",
        "lookup",
        "lookup-latest",
    ]
    operation_id: str = Field(min_length=1, max_length=500)
    identity: ResourceReservationIdentity | None = None
    admission_request: AdmissionRequest | None = None
    policy: AdmissionPolicy | None = None
    snapshot: ResourceSnapshot | None = None
    lease: ResourceReservationLease | None = None
    lease_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    prior_receipt: ResourceOperationReceipt | None = None
    lease_seconds: int | None = Field(default=None, ge=1, le=3_600)

    @model_validator(mode="after")
    def validate_operation_shape(self) -> ResourceAuthorityAdapterRequest:
        if self.operation in {"probe", "policy", "snapshot", "admission"}:
            if any(
                value is not None
                for value in (
                    self.identity,
                    self.admission_request,
                    self.policy,
                    self.snapshot,
                    self.lease,
                    self.lease_id,
                    self.prior_receipt,
                    self.lease_seconds,
                )
            ):
                raise ValueError(
                    "read-only authority operation carries a mutable reservation payload"
                )
            return self
        if self.operation == "lookup":
            if any(
                value is not None
                for value in (
                    self.identity,
                    self.admission_request,
                    self.policy,
                    self.snapshot,
                    self.lease,
                    self.lease_id,
                    self.prior_receipt,
                    self.lease_seconds,
                )
            ):
                raise ValueError("lookup authority operation carries an unexpected payload")
            return self
        if self.operation == "lookup-latest":
            if (
                self.identity is None
                or self.lease_id is None
                or self.lease_id != canonical_sha256(self.identity)
                or any(
                    value is not None
                    for value in (
                        self.admission_request,
                        self.policy,
                        self.snapshot,
                        self.lease,
                        self.prior_receipt,
                        self.lease_seconds,
                    )
                )
            ):
                raise ValueError("latest lookup authority operation is incomplete")
            return self
        if self.identity is None or self.lease is None:
            raise ValueError("resource authority operation requires identity and lease")
        if self.lease.identity != self.identity:
            raise ValueError("resource authority lease identity conflicts")
        if self.lease_id is not None:
            raise ValueError("resource authority mutation carries an unexpected lease id")
        if self.operation == "reserve":
            if (
                self.admission_request is None
                or self.lease_seconds is None
                or self.prior_receipt is not None
            ):
                raise ValueError("reserve authority operation is incomplete")
            if self.policy is not None or self.snapshot is not None:
                raise ValueError("reserve authority policy and snapshot are server-owned")
            if self.lease.lease_id != canonical_sha256(self.identity):
                raise ValueError("reserve authority lease identity is invalid")
        elif self.operation == "recheck":
            if (
                self.admission_request is None
                or self.lease_seconds is None
                or self.prior_receipt is None
            ):
                raise ValueError("recheck authority operation is incomplete")
            if self.policy is not None or self.snapshot is not None:
                raise ValueError("recheck authority policy and snapshot are server-owned")
        elif self.operation == "release":
            if self.prior_receipt is None or any(
                value is not None
                for value in (
                    self.admission_request,
                    self.policy,
                    self.snapshot,
                    self.lease_seconds,
                )
            ):
                raise ValueError("release authority operation is incomplete")
        return self


class ResourceAuthorityAdapterResponse(_AdapterModel):
    schema_version: Literal[1] = 1
    message_type: Literal["resource-authority-response"] = "resource-authority-response"
    operation: Literal[
        "probe",
        "policy",
        "snapshot",
        "admission",
        "reserve",
        "recheck",
        "release",
        "lookup",
        "lookup-latest",
    ]
    identity: ResourceAuthorityAdapterIdentity
    policy: AdmissionPolicy | None = None
    snapshot: ResourceSnapshot | None = None
    result: ResourceOperationResult | None = None
    capabilities: tuple[Literal["policy", "snapshot", "journal"], ...] | None = None
    error_code: Literal["configuration", "invalid_request", "authority", "integrity"] | None = None

    @model_validator(mode="after")
    def validate_response_shape(self) -> ResourceAuthorityAdapterResponse:
        if self.error_code is not None:
            if any(
                value is not None
                for value in (self.policy, self.snapshot, self.result, self.capabilities)
            ):
                raise ValueError("failed authority response cannot carry result data")
            return self
        if self.operation == "probe":
            if self.capabilities != ("policy", "snapshot", "journal") or any(
                value is not None for value in (self.policy, self.snapshot, self.result)
            ):
                raise ValueError("resource authority probe response is incomplete")
        elif self.operation == "policy":
            if (
                self.policy is None
                or self.snapshot is not None
                or self.result is not None
                or self.capabilities is not None
            ):
                raise ValueError("policy response is incomplete")
        elif self.operation == "snapshot":
            if (
                self.snapshot is None
                or self.policy is not None
                or self.result is not None
                or self.capabilities is not None
            ):
                raise ValueError("snapshot response is incomplete")
        elif self.operation == "admission":
            if (
                self.policy is None
                or self.snapshot is None
                or self.result is not None
                or self.capabilities is not None
            ):
                raise ValueError("admission response is incomplete")
        elif (
            self.result is None
            or self.policy is not None
            or self.snapshot is not None
            or self.capabilities is not None
        ):
            raise ValueError("resource operation response is incomplete")
        return self


def _encode(value: _AdapterModel) -> bytes:
    return canonical_json_bytes(value.model_dump(mode="json", round_trip=True))


def _decode(payload: bytes, *, model: type[_AdapterModel], label: str) -> _AdapterModel:
    if type(payload) is not bytes or len(payload) > RESOURCE_AUTHORITY_ADAPTER_MAX_WIRE_BYTES:
        raise ResourceAuthorityAdapterTransportError(f"{label} violates the wire bound")
    try:
        strict_canonical_json_loads(payload)
        # JSON represents datetimes and paths as strings; canonical byte equality
        # below is the strict boundary, while model parsing restores typed fields.
        result = model.model_validate_json(payload)
        if _encode(result) != payload:
            raise ValueError("wire is not canonical")
        return result
    except (ValidationError, ValueError, TypeError) as exc:
        raise ResourceAuthorityAdapterTransportError(f"{label} is malformed") from exc


def _recv_exact(connection: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            raise ResourceAuthorityAdapterTransportError(
                "resource authority transport closed early"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _send_frame(connection: socket.socket, payload: bytes) -> None:
    if len(payload) > RESOURCE_AUTHORITY_ADAPTER_MAX_WIRE_BYTES:
        raise ResourceAuthorityAdapterTransportError(
            "resource authority payload exceeds the wire bound"
        )
    connection.sendall(struct.pack("!I", len(payload)) + payload)


def _recv_frame(connection: socket.socket, *, label: str) -> bytes:
    length = struct.unpack("!I", _recv_exact(connection, _FRAME_HEADER_BYTES))[0]
    if length > RESOURCE_AUTHORITY_ADAPTER_MAX_WIRE_BYTES:
        raise ResourceAuthorityAdapterTransportError(f"{label} exceeds the wire bound")
    return _recv_exact(connection, length)


def _peer_credentials(connection: socket.socket) -> tuple[int, int]:
    if hasattr(socket, "SO_PEERCRED"):
        raw = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        _pid, uid, gid = struct.unpack("3i", raw)
        return uid, gid
    if hasattr(connection, "getpeereid"):
        uid, gid = connection.getpeereid()  # type: ignore[attr-defined]
        return uid, gid
    if hasattr(socket, "LOCAL_PEERCRED"):
        # Darwin's ``xucred`` starts with version, uid, group count, then gids.
        # CPython currently exposes the option but not socket.getpeereid().
        raw = connection.getsockopt(0, socket.LOCAL_PEERCRED, 16)
        if len(raw) < 16:
            raise ResourceAuthorityAdapterTransportError("Unix peer credentials are malformed")
        _version, uid, group_count, gid = struct.unpack_from("4I", raw)
        if group_count < 1:
            raise ResourceAuthorityAdapterTransportError("Unix peer credentials have no group")
        return uid, gid
    raise ResourceAuthorityAdapterTransportError("Unix peer credentials are unavailable")


def _secure_socket_path(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
) -> None:
    try:
        secure_path_metadata(
            path,
            allowed_ancestor_uids=frozenset({0, expected_uid, os.geteuid()}),
            kind="socket",
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=expected_mode,
        )
    except AuthorityPathSecurityError as exc:
        raise ResourceAuthorityAdapterTransportError(
            "resource authority endpoint is unavailable"
        ) from exc


class ResourceAuthorityJournalClient:
    """Byte-only client resolved from one frozen closed-registry descriptor."""

    def __init__(self, configuration: ResourceAuthorityAdapterConfig) -> None:
        self.configuration = ResourceAuthorityAdapterConfig.model_validate(
            configuration, strict=True
        )

    def _call(self, request: ResourceAuthorityAdapterRequest) -> ResourceAuthorityAdapterResponse:
        payload = _encode(request)
        config = self.configuration
        _secure_socket_path(
            config.endpoint,
            expected_uid=config.expected_uid,
            expected_gid=config.expected_gid,
            expected_mode=config.socket_mode,
        )
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(config.timeout_milliseconds / 1_000)
                connection.connect(str(config.endpoint))
                uid, gid = _peer_credentials(connection)
                if (uid, gid) != config.expected_server_identity:
                    raise ResourceAuthorityAdapterTransportError(
                        "resource authority peer identity is invalid"
                    )
                _send_frame(connection, payload)
                raw = _recv_frame(connection, label="resource authority response")
        except ResourceAuthorityAdapterError:
            raise
        except (OSError, TimeoutError) as exc:
            raise ResourceAuthorityAdapterTransportError(
                "resource authority transport failed"
            ) from exc
        response = _decode(
            raw,
            model=ResourceAuthorityAdapterResponse,
            label="resource authority response",
        )
        if not isinstance(
            response, ResourceAuthorityAdapterResponse
        ):  # pragma: no cover - typed helper
            raise AssertionError("resource authority response type changed")
        self._verify_identity(response.identity)
        if response.operation != request.operation:
            raise ResourceAuthorityAdapterTransportError(
                "resource authority response operation conflicts"
            )
        if response.error_code is not None:
            raise ResourceAuthorityAdapterRemoteError("resource authority rejected the request")
        if (
            response.result is not None
            and response.result.receipt.authority_id != config.authority_id
        ):
            raise ResourceAuthorityAdapterTransportError(
                "resource authority receipt identity conflicts"
            )
        return response

    def _verify_identity(self, identity: ResourceAuthorityAdapterIdentity) -> None:
        config = self.configuration
        if (
            identity.mode != config.mode
            or identity.authority_id != config.authority_id
            or identity.high_water_authority_id != config.high_water_authority_id
            or identity.external_root_config != config.external_root_config
            or identity.trusted_role_inventory_hash != config.trusted_role_inventory_hash
        ):
            raise ResourceAuthorityAdapterTransportError("resource authority identity conflicts")

    def policy(self, *, operation_id: str) -> AdmissionPolicy:
        result = self._call(
            ResourceAuthorityAdapterRequest(operation="policy", operation_id=operation_id)
        )
        if result.policy is None:  # pragma: no cover - response model enforces it
            raise ResourceAuthorityAdapterTransportError("resource authority policy is absent")
        return result.policy

    def probe(self, *, operation_id: str) -> tuple[Literal["policy", "snapshot", "journal"], ...]:
        result = self._call(
            ResourceAuthorityAdapterRequest(operation="probe", operation_id=operation_id)
        )
        if result.capabilities is None:  # pragma: no cover - response model enforces it
            raise ResourceAuthorityAdapterTransportError(
                "resource authority capabilities are absent"
            )
        return result.capabilities

    def snapshot(self, *, operation_id: str) -> ResourceSnapshot:
        result = self._call(
            ResourceAuthorityAdapterRequest(operation="snapshot", operation_id=operation_id)
        )
        if result.snapshot is None:  # pragma: no cover - response model enforces it
            raise ResourceAuthorityAdapterTransportError("resource authority snapshot is absent")
        return result.snapshot

    def admission(self, *, operation_id: str) -> tuple[AdmissionPolicy, ResourceSnapshot]:
        result = self._call(
            ResourceAuthorityAdapterRequest(operation="admission", operation_id=operation_id)
        )
        if result.policy is None or result.snapshot is None:  # pragma: no cover
            raise ResourceAuthorityAdapterTransportError(
                "resource authority admission is incomplete"
            )
        return result.policy, result.snapshot

    def reserve(
        self,
        *,
        operation_id: str,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        lease_seconds: int,
    ) -> ResourceOperationResult:
        lease = _reservation_shell(identity=identity, request=request)
        response = self._call(
            ResourceAuthorityAdapterRequest(
                operation="reserve",
                operation_id=operation_id,
                identity=identity,
                admission_request=request,
                lease=lease,
                lease_seconds=lease_seconds,
            )
        )
        return _required_operation_result(response)

    def recheck(
        self,
        *,
        operation_id: str,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        lease_seconds: int,
        prior_receipt: ResourceOperationReceipt,
    ) -> ResourceOperationResult:
        response = self._call(
            ResourceAuthorityAdapterRequest(
                operation="recheck",
                operation_id=operation_id,
                identity=identity,
                admission_request=request,
                lease=lease,
                lease_seconds=lease_seconds,
                prior_receipt=prior_receipt,
            )
        )
        return _required_operation_result(response)

    def release(
        self,
        *,
        operation_id: str,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        prior_receipt: ResourceOperationReceipt,
    ) -> ResourceOperationResult:
        response = self._call(
            ResourceAuthorityAdapterRequest(
                operation="release",
                operation_id=operation_id,
                identity=identity,
                lease=lease,
                prior_receipt=prior_receipt,
            )
        )
        return _required_operation_result(response)

    def lookup(self, *, operation_id: str) -> ResourceOperationResult:
        return _required_operation_result(
            self._call(
                ResourceAuthorityAdapterRequest(operation="lookup", operation_id=operation_id)
            )
        )

    def lookup_latest(
        self,
        *,
        identity: ResourceReservationIdentity,
        lease_id: str,
    ) -> ResourceOperationResult:
        return _required_operation_result(
            self._call(
                ResourceAuthorityAdapterRequest(
                    operation="lookup-latest",
                    operation_id=_operation_id(
                        operation="lookup-latest",
                        identity=identity,
                        prior=None,
                    ),
                    identity=identity,
                    lease_id=lease_id,
                )
            )
        )


def _required_operation_result(
    response: ResourceAuthorityAdapterResponse,
) -> ResourceOperationResult:
    if response.result is None:  # pragma: no cover - response model enforces it
        raise ResourceAuthorityAdapterTransportError(
            "resource authority operation result is absent"
        )
    return response.result


def _reservation_shell(
    *,
    identity: ResourceReservationIdentity,
    request: AdmissionRequest,
) -> ResourceReservationLease:
    """Build the deterministic lease identity required by the closed request model.

    The authority never trusts this shell for timestamps or resources. It
    replaces it with its own sampled, signed journal result after validation.
    """

    return ResourceReservationLease(
        identity=identity,
        request_hash=canonical_sha256(request),
        expected_memory_bytes=request.expected_memory_bytes,
        expected_disk_bytes=request.expected_disk_bytes,
        expected_quota_units=0,
        granted_at=datetime(1970, 1, 1, tzinfo=UTC),
        expires_at=datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
    )


def _operation_id(
    *, operation: str, identity: ResourceReservationIdentity, prior: str | None
) -> str:
    return canonical_sha256(
        {
            "contract": _RESOURCE_AUTHORITY_CONTRACT,
            "identity": identity.model_dump(mode="json"),
            "operation": operation,
            "prior_receipt_hash": prior,
        }
    )


class LabResourceAuthorityReservationAdapter:
    """Parent-side reservation facade backed by a closed external journal.

    The remote journal is the only source of lease and receipt truth. Every
    mutation first resolves the latest signed effect for the fenced identity,
    so a fresh worker process can continue after a lost response or restart.
    """

    def __init__(self, configuration: ResourceAuthorityAdapterConfig) -> None:
        self._client = ResourceAuthorityJournalClient(configuration)

    @property
    def configuration(self) -> ResourceAuthorityAdapterConfig:
        return self._client.configuration

    def reserve(
        self,
        *,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        policy: AdmissionPolicy,
        snapshot_provider: Callable[[], ResourceSnapshot],
        lease_seconds: int,
        quota_lease_provider: object | None = None,
        lock_wait_timeout_seconds: float | None = None,
        stop_requested: Callable[[], bool] | None = None,
    ) -> ResourceReservationAdmission:
        del quota_lease_provider, lock_wait_timeout_seconds
        if stop_requested is not None and stop_requested():
            raise RuntimeResourceAdmissionError("resource reservation admission cancelled")
        del policy, snapshot_provider
        self._client.reserve(
            operation_id=_operation_id(operation="reserve", identity=identity, prior=None),
            identity=identity,
            request=request,
            lease_seconds=lease_seconds,
        )
        result = self._client.lookup_latest(
            identity=identity,
            lease_id=canonical_sha256(identity),
        )
        if result.released:
            raise RuntimeResourceAdmissionError(
                "resource authority reservation is already terminal"
            )
        return _as_admission(result)

    def recheck(
        self,
        *,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
        request: AdmissionRequest,
        policy: AdmissionPolicy,
        snapshot_provider: Callable[[], ResourceSnapshot],
        lease_seconds: int,
        quota_lease_provider: object | None = None,
        lock_wait_timeout_seconds: float | None = None,
        stop_requested: Callable[[], bool] | None = None,
    ) -> ResourceReservationAdmission:
        del quota_lease_provider, lock_wait_timeout_seconds
        if stop_requested is not None and stop_requested():
            raise RuntimeResourceAdmissionError("resource reservation recheck cancelled")
        validated_lease = ResourceReservationLease.model_validate(lease)
        recheck_operation_id = _operation_id(
            operation="recheck",
            identity=identity,
            prior=canonical_sha256(
                {
                    "lease": validated_lease,
                    "lease_seconds": lease_seconds,
                    "request": request,
                }
            ),
        )
        recovered = self._recover_latest(lease=validated_lease, identity=identity)
        if recovered.released:
            raise RuntimeResourceAdmissionError(
                "resource authority reservation is already terminal"
            )
        current_lease = recovered.lease
        if current_lease is None:
            raise RuntimeResourceAdmissionError("resource authority recovery has no active lease")
        if (
            recovered.receipt.operation_id == recheck_operation_id
            or current_lease != validated_lease
        ):
            return _as_admission(recovered)
        prior = recovered.receipt
        del policy, snapshot_provider
        result = self._client.recheck(
            operation_id=recheck_operation_id,
            lease=current_lease,
            identity=identity,
            request=request,
            lease_seconds=lease_seconds,
            prior_receipt=prior,
        )
        return _as_admission(result)

    def release(
        self,
        lease: ResourceReservationLease,
        *,
        identity: ResourceReservationIdentity,
        lock_wait_timeout_seconds: float | None = None,
    ) -> bool:
        del lock_wait_timeout_seconds
        recovered = self._recover_latest(lease=lease, identity=identity)
        if recovered.released:
            return True
        current_lease = recovered.lease
        if current_lease is None:
            raise RuntimeResourceAdmissionError("resource authority recovery has no active lease")
        prior = recovered.receipt
        result = self._client.release(
            operation_id=_operation_id(
                operation="release", identity=identity, prior=prior.receipt_hash
            ),
            lease=current_lease,
            identity=identity,
            prior_receipt=prior,
        )
        if not result.released:
            raise RuntimeResourceAdmissionError("resource authority release was not terminal")
        return True

    def _recover_latest(
        self,
        *,
        lease: ResourceReservationLease,
        identity: ResourceReservationIdentity,
    ) -> ResourceOperationResult:
        validated_lease = ResourceReservationLease.model_validate(lease)
        validated_identity = ResourceReservationIdentity.model_validate(identity)
        if (
            validated_lease.identity != validated_identity
            or validated_lease.lease_id != canonical_sha256(validated_identity)
        ):
            raise RuntimeResourceAdmissionError(
                "resource operation recovery identity or lease fence conflicts"
            )
        return self._client.lookup_latest(
            identity=validated_identity,
            lease_id=validated_lease.lease_id,
        )


def _as_admission(result: ResourceOperationResult) -> ResourceReservationAdmission:
    if (
        result.decision is None
        or result.request is None
        or result.snapshot is None
        or result.policy is None
        or result.released
    ):
        raise RuntimeResourceAdmissionError("resource authority result is not an admission")
    return ResourceReservationAdmission(
        decision=result.decision,
        request=result.request,
        snapshot=result.snapshot,
        policy=result.policy,
        lease=result.lease,
    )


class ResourceAuthorityJournalSocketServer:
    """Server-side adapter owned by the resource authority service, never a worker."""

    def __init__(
        self,
        *,
        configuration: ResourceAuthorityAdapterConfig,
        authority: SQLiteResourceAdmissionAuthority,
        policy_provider: Callable[[], AdmissionPolicy],
        snapshot_provider: Callable[[], ResourceSnapshot],
        external_root: ExternalResourceJournalMonotonicRootAdapter | None = None,
    ) -> None:
        self.configuration = ResourceAuthorityAdapterConfig.model_validate(
            configuration, strict=True
        )
        self._authority = authority
        self._policy_provider = policy_provider
        self._snapshot_provider = snapshot_provider
        self._external_root = external_root
        self._drop_response_once = False
        self._drop_response_operation: str | None = None
        self._validate_authority_binding()

    def _validate_authority_binding(self) -> None:
        config = self.configuration
        if (
            self._authority.authority_id != config.authority_id
            or self._authority.mode != config.mode
            or self._authority.trusted_role_inventory_hash != config.trusted_role_inventory_hash
        ):
            raise ResourceAuthorityAdapterConfigurationError("resource authority binding conflicts")
        high_water = self._authority.high_water_authority
        if config.mode == "test-standalone":
            if high_water is not None or self._external_root is not None:
                raise ResourceAuthorityAdapterConfigurationError(
                    "standalone resource authority cannot bind a production root client"
                )
            return
        root = self._external_root
        if high_water is None or root is None:
            raise ResourceAuthorityAdapterConfigurationError(
                "production resource authority requires an external root client"
            )
        if type(root) is not ExternalResourceJournalMonotonicRootAdapter:
            raise ResourceAuthorityAdapterConfigurationError(
                "production resource authority root adapter is not registered"
            )
        if not root.production_ready:
            raise ResourceAuthorityAdapterConfigurationError(
                "production resource authority root adapter is not production ready"
            )
        if (
            root.config != config.external_root_config
            or high_water.mode != "production"
            or high_water.authority_id != config.high_water_authority_id
            or high_water.anti_rollback_root_authority_id != root.authority_id
            or high_water.verifier_fingerprints != root.verifier_fingerprints
        ):
            raise ResourceAuthorityAdapterConfigurationError(
                "resource authority external root capability binding conflicts"
            )
        try:
            current = root.current(journal_authority_id=self._authority.authority_id)
        except Exception as exc:
            raise ResourceAuthorityAdapterConfigurationError(
                "external root client capability check failed"
            ) from exc
        if current is not None and (
            current.high_water_authority_id != config.high_water_authority_id
            or current.journal_authority_id != config.authority_id
        ):
            raise ResourceAuthorityAdapterConfigurationError(
                "external root current receipt conflicts with the closed manifest"
            )

    @property
    def external_root(self) -> ExternalResourceJournalMonotonicRootAdapter | None:
        return self._external_root

    @property
    def identity(self) -> ResourceAuthorityAdapterIdentity:
        self._validate_authority_binding()
        return ResourceAuthorityAdapterIdentity(
            mode=self._authority.mode,
            authority_id=self._authority.authority_id,
            high_water_authority_id=self.configuration.high_water_authority_id,
            external_root_config=self.configuration.external_root_config,
            trusted_role_inventory_hash=self._authority.trusted_role_inventory_hash,
        )

    def drop_next_response_after_effect_for_test(self, operation: str | None = None) -> None:
        self._drop_response_once = True
        self._drop_response_operation = operation

    def bind(self) -> socket.socket:
        config = self.configuration
        if (os.geteuid(), os.getegid()) != config.expected_server_identity:
            raise ResourceAuthorityAdapterConfigurationError(
                "resource authority service process identity is untrusted"
            )
        endpoint = config.endpoint
        try:
            secure_path_metadata(
                endpoint.parent,
                allowed_ancestor_uids=frozenset(
                    {0, config.expected_uid, config.expected_server_uid}
                ),
                kind="directory",
                expected_uid=config.expected_uid,
                expected_gid=config.expected_gid,
                expected_mode=config.socket_directory_mode,
            )
        except AuthorityPathSecurityError as exc:
            raise ResourceAuthorityAdapterConfigurationError(
                "resource authority socket directory is not private"
            ) from exc
        self._remove_verified_stale_endpoint(endpoint)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(endpoint))
            os.chmod(endpoint, config.socket_mode)
            _secure_socket_path(
                endpoint,
                expected_uid=config.expected_uid,
                expected_gid=config.expected_gid,
                expected_mode=config.socket_mode,
            )
            listener.listen(32)
            return listener
        except BaseException:
            listener.close()
            endpoint.unlink(missing_ok=True)
            raise

    def _remove_verified_stale_endpoint(self, endpoint: Path) -> None:
        """Remove only a private, unreachable socket left by a previous server.

        The socket directory is private, but blindly unlinking a path here would
        still let a configuration mistake destroy a regular file or replace a
        live authority.  A live endpoint wins; a refused connection is the only
        recoverable stale state.
        """

        try:
            _secure_socket_path(
                endpoint,
                expected_uid=self.configuration.expected_uid,
                expected_gid=self.configuration.expected_gid,
                expected_mode=self.configuration.socket_mode,
            )
        except ResourceAuthorityAdapterTransportError as exc:
            try:
                endpoint.lstat()
            except FileNotFoundError:
                return
            raise ResourceAuthorityAdapterConfigurationError(
                "resource authority endpoint cannot be safely replaced"
            ) from exc

        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                probe.settimeout(self.configuration.timeout_milliseconds / 1_000)
                probe.connect(str(endpoint))
        except ConnectionRefusedError:
            endpoint.unlink()
            return
        except OSError as exc:
            raise ResourceAuthorityAdapterConfigurationError(
                "resource authority endpoint cannot be safely replaced"
            ) from exc
        raise ResourceAuthorityAdapterConfigurationError(
            "resource authority endpoint is already active"
        )

    def serve_once(self, listener: socket.socket) -> None:
        connection, _address = listener.accept()
        with connection:
            connection.settimeout(self.configuration.timeout_milliseconds / 1_000)
            operation: Literal[
                "probe",
                "policy",
                "snapshot",
                "admission",
                "reserve",
                "recheck",
                "release",
                "lookup",
                "lookup-latest",
            ] = "lookup"
            try:
                uid, gid = _peer_credentials(connection)
                if (uid, gid) != self.configuration.allowed_peer_identity:
                    raise ResourceAuthorityAdapterTransportError(
                        "untrusted resource authority peer"
                    )
                request = _decode(
                    _recv_frame(connection, label="resource authority request"),
                    model=ResourceAuthorityAdapterRequest,
                    label="resource authority request",
                )
                if not isinstance(request, ResourceAuthorityAdapterRequest):  # pragma: no cover
                    raise AssertionError("resource authority request type changed")
                operation = request.operation
                response = self._handle(request)
            except ResourceAuthorityAdapterError:
                response = ResourceAuthorityAdapterResponse(
                    operation=operation, identity=self.identity, error_code="invalid_request"
                )
            except (RuntimeResourceAdmissionError, ValidationError, ValueError):
                response = ResourceAuthorityAdapterResponse(
                    operation=operation, identity=self.identity, error_code="authority"
                )
            payload = _encode(response)
            if self._drop_response_once and (
                self._drop_response_operation is None or self._drop_response_operation == operation
            ):
                self._drop_response_once = False
                self._drop_response_operation = None
                return
            try:
                _send_frame(connection, payload)
            except OSError:
                return

    def _handle(self, request: ResourceAuthorityAdapterRequest) -> ResourceAuthorityAdapterResponse:
        identity = self.identity
        if request.operation == "probe":
            return ResourceAuthorityAdapterResponse(
                operation="probe",
                identity=identity,
                capabilities=("policy", "snapshot", "journal"),
            )
        if request.operation == "policy":
            return ResourceAuthorityAdapterResponse(
                operation="policy",
                identity=identity,
                policy=AdmissionPolicy.model_validate(self._policy_provider()),
            )
        if request.operation == "snapshot":
            return ResourceAuthorityAdapterResponse(
                operation="snapshot",
                identity=identity,
                snapshot=ResourceSnapshot.model_validate(self._snapshot_provider()),
            )
        if request.operation == "admission":
            return ResourceAuthorityAdapterResponse(
                operation="admission",
                identity=identity,
                policy=AdmissionPolicy.model_validate(self._policy_provider()),
                snapshot=ResourceSnapshot.model_validate(self._snapshot_provider()),
            )
        if request.operation == "lookup":
            return ResourceAuthorityAdapterResponse(
                operation="lookup",
                identity=identity,
                result=self._authority.lookup(request.operation_id),
            )
        if request.operation == "lookup-latest":
            if request.identity is None or request.lease_id is None:
                raise ResourceAuthorityAdapterConfigurationError(
                    "latest resource authority lookup is incomplete"
                )
            return ResourceAuthorityAdapterResponse(
                operation="lookup-latest",
                identity=identity,
                result=self._authority.lookup_latest(
                    identity=request.identity,
                    lease_id=request.lease_id,
                ),
            )
        if request.identity is None or request.lease is None:
            raise ResourceAuthorityAdapterConfigurationError(
                "resource authority request is incomplete"
            )
        if request.operation == "reserve":
            assert request.admission_request is not None
            assert request.lease_seconds is not None
            result = self._authority.reserve(
                operation_id=request.operation_id,
                identity=request.identity,
                request=request.admission_request,
                policy=AdmissionPolicy.model_validate(self._policy_provider()),
                snapshot_provider=lambda: ResourceSnapshot.model_validate(
                    self._snapshot_provider()
                ),
                lease_seconds=request.lease_seconds,
            )
        elif request.operation == "recheck":
            assert request.admission_request is not None
            assert request.lease_seconds is not None
            assert request.prior_receipt is not None
            result = self._authority.recheck(
                operation_id=request.operation_id,
                lease=request.lease,
                identity=request.identity,
                request=request.admission_request,
                policy=AdmissionPolicy.model_validate(self._policy_provider()),
                snapshot_provider=lambda: ResourceSnapshot.model_validate(
                    self._snapshot_provider()
                ),
                lease_seconds=request.lease_seconds,
                prior_receipt=request.prior_receipt,
            )
        elif request.operation == "release":
            assert request.prior_receipt is not None
            result = self._authority.release(
                operation_id=request.operation_id,
                lease=request.lease,
                identity=request.identity,
                prior_receipt=request.prior_receipt,
            )
        else:  # pragma: no cover - closed model above
            raise ResourceAuthorityAdapterConfigurationError(
                "resource authority operation is unknown"
            )
        return ResourceAuthorityAdapterResponse(
            operation=request.operation,
            identity=identity,
            result=result,
        )


def compose_production_resource_authority_socket_server(
    *,
    configuration: ResourceAuthorityAdapterConfig,
    authority: SQLiteResourceAdmissionAuthority,
    policy_provider: Callable[[], AdmissionPolicy],
    snapshot_provider: Callable[[], ResourceSnapshot],
    external_root_client: UnixSocketExternalMonotonicRootClient,
    external_root_verifiers: tuple[ClosedExternalMonotonicRootVerifier, ...],
) -> ResourceAuthorityJournalSocketServer:
    """Construct production transport only from the closed shared-root capability."""

    validated = ResourceAuthorityAdapterConfig.model_validate(configuration, strict=True)
    if validated.mode != "production":
        raise ResourceAuthorityAdapterConfigurationError(
            "production resource authority composition rejects non-production mode"
        )
    external_root_config = validated.external_root_config
    if external_root_config is None:  # pragma: no cover - model validation enforces it
        raise ResourceAuthorityAdapterConfigurationError(
            "production resource authority root configuration is missing"
        )
    external_root = ExternalResourceJournalMonotonicRootAdapter(
        config=external_root_config,
        client=external_root_client,
        root_verifiers=external_root_verifiers,
    )
    return ResourceAuthorityJournalSocketServer(
        configuration=validated,
        authority=authority,
        policy_provider=policy_provider,
        snapshot_provider=snapshot_provider,
        external_root=external_root,
    )


__all__ = [
    "LAB_RESOURCE_AUTHORITY_REGISTRY_HASH",
    "LAB_RESOURCE_AUTHORITY_REGISTRY_ID",
    "LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION",
    "ExternalResourceJournalMonotonicRootAdapter",
    "ExternalResourceJournalRootConfig",
    "LabResourceAuthorityReservationAdapter",
    "RESOURCE_AUTHORITY_ADAPTER_MAX_WIRE_BYTES",
    "ResourceAuthorityAdapterConfig",
    "ResourceAuthorityAdapterConfigurationError",
    "ResourceAuthorityAdapterError",
    "ResourceAuthorityAdapterIdentity",
    "ResourceAuthorityAdapterRemoteError",
    "ResourceAuthorityAdapterRequest",
    "ResourceAuthorityAdapterResponse",
    "ResourceAuthorityAdapterTransportError",
    "ResourceAuthorityJournalClient",
    "ResourceAuthorityJournalSocketServer",
    "ResourceJournalExternalRootReceipt",
    "compose_production_resource_authority_socket_server",
    "parse_resource_authority_adapter_config",
]
