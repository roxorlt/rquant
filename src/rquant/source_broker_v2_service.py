"""Provider-owned SourceBroker v2 operation ledger and strict wire handling."""

from __future__ import annotations

import base64
import hashlib
import os
import re
import shutil
import socket
import sqlite3
import struct
import subprocess
import tempfile
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock, Thread
from typing import Literal, Protocol, Self, TypeVar, cast, final
from uuid import uuid4

from loguru import logger
from pydantic import ConfigDict, Field, ValidationError, model_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.source_broker_protocol import (
    MAX_SOURCE_BROKER_FRAME_BYTES,
    ServerCredentialsPolicy,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
    require_linux_source_broker_transport,
    validate_socket_endpoint,
    verify_connected_server_authority,
)
from rquant.source_broker_v2 import (
    SOURCE_BROKER_V2_MAX_PAYLOAD_BYTES,
    SOURCE_BROKER_V2_MAX_RECEIPT_BYTES,
    SourceAuthorityKeyring,
    SourceBrokerV2ClaimOnceRequest,
    SourceBrokerV2ClaimOnceResponse,
    SourceBrokerV2ClaimStatus,
    SourceBrokerV2DispatchEnvelope,
    SourceBrokerV2DispatchOutcome,
    SourceBrokerV2DispatchRequest,
    SourceBrokerV2DispatchResponse,
    SourceBrokerV2FinalizeEnvelope,
    SourceBrokerV2FinalizeRequest,
    SourceBrokerV2FinalizeResponse,
    SourceBrokerV2OutboxPhase,
    SourceBrokerV2ReplayRequest,
    SourceBrokerV2ReplayResponse,
    SourceBrokerV2ReplayStatus,
    SourceBrokerV2SagaIntegrityError,
    SourceBrokerV2TransportDeadlineError,
    SourceBrokerV2WireFailure,
    SourceBrokerV2WireRequest,
    SourceBrokerV2WireResponse,
    source_authority_signature_payload,
)
from rquant.strict_json import (
    StrictJsonError,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

SOURCE_BROKER_V2_MAX_WIRE_BYTES = MAX_SOURCE_BROKER_FRAME_BYTES
_UNKNOWN_ERROR = "source provider operation is unknown; reconcile required"
_TERMINAL_STATUSES = frozenset({"success", "failure"})
_ACTIVE_STATUS = "definitively_absent"
_INVOKING_STATUS = "invoking"
_RECONCILE_STATUS = "reconcile_required"
_PROCESS_EPOCH_PID = os.getpid()
_PROCESS_EPOCH_TOKEN = uuid4().hex
_PROVIDER_THREAD_NAME = "rquant-source-broker-v2-provider-call"
_PROVIDER_DEADLINE_RESERVE_SECONDS = 0.01
_RECONCILE_FENCE_SQLITE_TIMEOUT_SECONDS = 0.001
_HASH_PATTERN = r"^[0-9a-f]{64}$"
EXTERNAL_DISPATCH_AUTHORITY_PURPOSE = "rquant-source-broker-v2-external-dispatch-authority/v1"
_EXTERNAL_AUTHORITY_CONTRACT = "rquant-source-broker-v2-external-authority/v1"
_NONPRODUCTION_PROFILE_TOKEN = object()
_ED25519_SIGNATURE_BYTES = 64
_EVENT_EXCEPTION_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]{0,127}$"
_EVENT_OPERATION_HASH_PURPOSE = "rquant-source-broker-v2-security-operation/v1"


class _ServiceModel(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceBrokerV2SecurityEvent(_ServiceModel):
    """Closed, redacted event contract; wire values never cross this boundary."""

    schema_version: Literal[1] = 1
    phase: Literal["claim_once", "dispatch", "source_finalize", "replay", "transport"]
    operation_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    authority_operation: Literal["reserve", "lookup", "complete"] | None = None
    exception_class: str | None = Field(default=None, pattern=_EVENT_EXCEPTION_PATTERN)
    category: Literal[
        "authority_error",
        "authority_lookup",
        "authority_lookup_error",
        "peer_rejected",
        "provider_capacity",
        "provider_deadline",
        "provider_exception",
        "provider_late_outcome",
        "provider_stopped",
        "reconcile",
        "transport_error",
        "write_error",
    ]
    outcome: Literal["absent", "failure", "found", "success", "unknown"] | None = None
    reconcile: bool


@final
class _ProductionSourceBrokerV2EventSink:
    """Fixed production sink that only receives the closed redacted model above."""

    __slots__ = ()

    def __call__(self, event: SourceBrokerV2SecurityEvent) -> None:
        logger.bind(source_broker_v2_security_event=event.model_dump(mode="json")).warning(
            "source_broker_v2_security_event"
        )


@final
class _ProviderInflightLease:
    __slots__ = ("_gate", "_released", "_lock")

    def __init__(self, gate: _ProviderInflightGate) -> None:
        self._gate = gate
        self._released = False
        self._lock = Lock()

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        self._gate._release()  # noqa: SLF001


@final
class _ProviderInflightGate:
    """One exact global gate per provider service, shared by every source and phase."""

    __slots__ = ("_active", "_lock", "_max_inflight", "_stopped")

    def __init__(self, max_inflight: int) -> None:
        self._max_inflight = max_inflight
        self._active = 0
        self._stopped = False
        self._lock = Lock()

    def try_acquire(self) -> _ProviderInflightLease | None:
        with self._lock:
            if self._stopped or self._active >= self._max_inflight:
                return None
            self._active += 1
        return _ProviderInflightLease(self)

    def stop(self) -> None:
        with self._lock:
            self._stopped = True

    @property
    def stopped(self) -> bool:
        with self._lock:
            return self._stopped

    def _release(self) -> None:
        with self._lock:
            if self._active < 1:
                raise RuntimeError("source provider in-flight gate underflow")
            self._active -= 1


class SourceBrokerV2ProviderDispatchResult(_ServiceModel):
    outcome: SourceBrokerV2DispatchOutcome
    response: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_PAYLOAD_BYTES)
    transport_receipt: bytes = Field(
        min_length=1,
        max_length=SOURCE_BROKER_V2_MAX_RECEIPT_BYTES,
    )

    @model_validator(mode="after")
    def validate_canonical_bytes(self) -> SourceBrokerV2ProviderDispatchResult:
        _canonical_hash(self.response, label="provider dispatch response")
        _canonical_hash(self.transport_receipt, label="provider transport receipt")
        return self


class SourceBrokerV2ProviderFinalizeResult(_ServiceModel):
    final_receipt: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_RECEIPT_BYTES)

    @model_validator(mode="after")
    def validate_canonical_bytes(self) -> SourceBrokerV2ProviderFinalizeResult:
        _canonical_hash(self.final_receipt, label="provider final receipt")
        return self


class ExternalDispatchReserveRequest(_ServiceModel):
    schema_version: Literal[1] = 1
    operation_id: str = Field(pattern=_HASH_PATTERN)
    saga_id: str = Field(min_length=1, max_length=128)
    phase: SourceBrokerV2OutboxPhase
    operation_request_hash: str = Field(pattern=_HASH_PATTERN)
    claim_binding_hash: str = Field(pattern=_HASH_PATTERN)
    claim_generation: int = Field(ge=0)
    scheduler_fencing_token: int = Field(ge=0)
    executor_owner_token_hash: str = Field(pattern=_HASH_PATTERN)
    executor_generation: int = Field(ge=0)
    max_external_deadline: datetime
    not_before_takeover_at: datetime

    @property
    def request_binding_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ExternalDispatchAuthorityResponse(_ServiceModel):
    schema_version: Literal[1] = 1
    operation: Literal["reserve", "lookup", "complete"]
    status: Literal["found", "absent", "unknown"]
    operation_id: str = Field(pattern=_HASH_PATTERN)
    request_binding_hash: str = Field(pattern=_HASH_PATTERN)
    authority_generation: int = Field(ge=1)
    authority_fence: str = Field(pattern=_HASH_PATTERN)
    result_json: str | None = Field(default=None, max_length=SOURCE_BROKER_V2_MAX_WIRE_BYTES)
    result_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_result(self) -> ExternalDispatchAuthorityResponse:
        if self.status == "found":
            if self.result_json is None or self.result_hash is None:
                raise ValueError("external dispatch authority FOUND requires a result")
            if (
                _canonical_hash(self.result_bytes(), label="external authority result")
                != self.result_hash
            ):
                raise ValueError("external dispatch authority result hash is invalid")
        elif self.result_json is not None or self.result_hash is not None:
            raise ValueError("external dispatch authority non-FOUND result must be empty")
        return self

    def result_bytes(self) -> bytes:
        if self.result_json is None:
            raise SourceBrokerTransportError("external dispatch authority result is unavailable")
        return self.result_json.encode("utf-8")


class ExternalDispatchCompleteRequest(_ServiceModel):
    schema_version: Literal[1] = 1
    operation_id: str = Field(pattern=_HASH_PATTERN)
    request_binding_hash: str = Field(pattern=_HASH_PATTERN)
    authority_generation: int = Field(ge=1)
    authority_fence: str = Field(pattern=_HASH_PATTERN)
    result_json: str = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_WIRE_BYTES)
    result_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_result(self) -> ExternalDispatchCompleteRequest:
        if _canonical_hash(self.result_json.encode("utf-8"), label="external terminal result") != (
            self.result_hash
        ):
            raise ValueError("external terminal result hash is invalid")
        return self


class ExternalDispatchAuthorityWireRequest(_ServiceModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-broker-v2-external-authority/v1"] = (
        _EXTERNAL_AUTHORITY_CONTRACT
    )
    operation: Literal["reserve", "lookup", "complete"]
    challenge: str = Field(pattern=_HASH_PATTERN)
    payload: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_WIRE_BYTES)
    payload_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_payload(self) -> ExternalDispatchAuthorityWireRequest:
        if _canonical_hash(self.payload, label="external authority request") != self.payload_hash:
            raise ValueError("external authority request payload hash is invalid")
        return self

    @property
    def request_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ExternalDispatchAuthoritySignedResponse(_ServiceModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-broker-v2-external-authority/v1"] = (
        _EXTERNAL_AUTHORITY_CONTRACT
    )
    operation: Literal["reserve", "lookup", "complete"]
    challenge: str = Field(pattern=_HASH_PATTERN)
    request_hash: str = Field(pattern=_HASH_PATTERN)
    authority_id: str = Field(min_length=1, max_length=128)
    key_id: str = Field(min_length=1, max_length=128)
    signature_purpose: Literal["rquant-source-broker-v2-external-dispatch-authority/v1"] = (
        EXTERNAL_DISPATCH_AUTHORITY_PURPOSE
    )
    signature_algorithm: Literal["ed25519"] = "ed25519"
    result: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_WIRE_BYTES)
    result_hash: str = Field(pattern=_HASH_PATTERN)
    signature: str = Field(min_length=1, max_length=512)

    @model_validator(mode="after")
    def validate_result(self) -> ExternalDispatchAuthoritySignedResponse:
        if _canonical_hash(self.result, label="external authority signed result") != (
            self.result_hash
        ):
            raise ValueError("external authority signed result hash is invalid")
        return self

    def signing_bytes(self) -> bytes:
        return canonical_model_json_bytes(self.model_copy(update={"signature": "unsigned"}))


@dataclass(frozen=True)
class _ReconcileFence:
    saga_id: str
    operation_id: str
    phase: SourceBrokerV2OutboxPhase
    operation_request_hash: str
    claim_binding_hash: str
    claim_generation: int
    scheduler_fencing_token: int
    executor_owner_token_hash: str
    executor_generation: int
    max_external_deadline: datetime
    not_before_takeover_at: datetime
    reason: str


class SourceBrokerV2Provider(Protocol):
    def dispatch(
        self,
        request: SourceBrokerV2DispatchRequest,
    ) -> SourceBrokerV2ProviderDispatchResult: ...

    def finalize(
        self,
        request: SourceBrokerV2FinalizeRequest,
    ) -> SourceBrokerV2ProviderFinalizeResult: ...


class ExternalDispatchAuthority(Protocol):
    """Non-rollback authority boundary; only canonical bytes cross this interface."""

    def reserve(self, payload: bytes, *, deadline: float | None = None) -> bytes: ...

    def lookup(self, payload: bytes, *, deadline: float | None = None) -> bytes: ...

    def complete(self, payload: bytes, *, deadline: float | None = None) -> bytes: ...


@final
class UnixSocketExternalDispatchAuthorityClient:
    """Exact production client for the non-rollback dispatch authority."""

    __slots__ = (
        "_endpoint",
        "_expected_authority_id",
        "_public_keys",
        "_server_policy",
        "total_request_deadline_seconds",
    )

    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        server_policy: ServerCredentialsPolicy,
        expected_authority_id: str,
        allowed_public_keys: Mapping[str, bytes],
        total_request_deadline_seconds: float,
    ) -> None:
        if type(endpoint) is not SocketEndpointPolicy:
            raise TypeError("external authority client requires exact endpoint policy")
        if type(server_policy) is not ServerCredentialsPolicy:
            raise TypeError("external authority client requires exact server policy")
        authority_id = expected_authority_id.strip()
        if not authority_id:
            raise ValueError("external authority id must be nonempty")
        if not 0 < total_request_deadline_seconds <= 30:
            raise ValueError("external authority deadline must be positive")
        keys = dict(allowed_public_keys)
        if not keys or any(
            type(key_id) is not str
            or not key_id.strip()
            or type(public_key) is not bytes
            or not public_key
            for key_id, public_key in keys.items()
        ):
            raise ValueError("external authority public-key allowlist is invalid")
        fingerprints: set[str] = set()
        for public_key in keys.values():
            fingerprint = _external_authority_public_key_fingerprint(public_key)
            if fingerprint in fingerprints:
                raise ValueError("external authority public key is duplicated")
            fingerprints.add(fingerprint)
        self._endpoint = endpoint
        self._server_policy = server_policy
        self._expected_authority_id = authority_id
        self._public_keys = keys
        self.total_request_deadline_seconds = total_request_deadline_seconds

    @property
    def allowed_key_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._public_keys))

    def reserve(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        return self._execute("reserve", payload, deadline=deadline)

    def lookup(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        return self._execute("lookup", payload, deadline=deadline)

    def complete(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        return self._execute("complete", payload, deadline=deadline)

    def _execute(
        self,
        operation: Literal["reserve", "lookup", "complete"],
        payload: bytes,
        *,
        deadline: float | None,
    ) -> bytes:
        request_deadline = self._bounded_deadline(deadline)
        _require_deadline(request_deadline, stage="before external authority request parsing")
        request_model = (
            ExternalDispatchCompleteRequest
            if operation == "complete"
            else ExternalDispatchReserveRequest
        )
        try:
            parsed_request = strict_model_validate_canonical_json(request_model, payload)
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerTransportError(
                "external authority request is malformed or noncanonical"
            ) from exc
        challenge = canonical_sha256(
            {
                "contract": "rquant-source-broker-v2-external-challenge/v1",
                "nonce": uuid4().hex,
                "operation": operation,
                "payload_hash": _canonical_hash(payload, label="external authority payload"),
            }
        )
        wire_request = ExternalDispatchAuthorityWireRequest(
            operation=operation,
            challenge=challenge,
            payload=payload,
            payload_hash=_canonical_hash(payload, label="external authority payload"),
        )
        wire = canonical_model_json_bytes(wire_request)
        _require_deadline(request_deadline, stage="after external authority request preparation")
        require_linux_source_broker_transport()
        endpoint_identity = validate_socket_endpoint(self._endpoint)
        _require_deadline(request_deadline, stage="after external endpoint validation")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(
                    _remaining(request_deadline, stage="before external authority connect")
                )
                connection.connect(str(self._endpoint.path))
                _require_deadline(request_deadline, stage="after external authority connect")
                server_pid, server_uid, server_gid = _external_kernel_peer_credentials(connection)
                if not self._server_policy.allows(
                    pid=server_pid,
                    uid=server_uid,
                    gid=server_gid,
                ):
                    raise SourceBrokerTransportError(
                        "connected external authority credentials are not allowed"
                    )
                verify_connected_server_authority(
                    server_pid=server_pid,
                    endpoint=self._endpoint,
                    endpoint_identity=endpoint_identity,
                )
                validate_socket_endpoint(
                    self._endpoint,
                    expected_identity=endpoint_identity,
                )
                self._write_frame(connection, wire, deadline=request_deadline)
                raw_response = self._read_frame(connection, deadline=request_deadline)
                validate_socket_endpoint(
                    self._endpoint,
                    expected_identity=endpoint_identity,
                )
        except SourceBrokerV2TransportDeadlineError:
            raise
        except SourceBrokerTransportError:
            raise
        except (OSError, TimeoutError) as exc:
            raise SourceBrokerTransportError("external authority Unix transport failed") from exc
        _require_deadline(request_deadline, stage="before external authority response parsing")
        try:
            response = strict_model_validate_canonical_json(
                ExternalDispatchAuthoritySignedResponse,
                raw_response,
            )
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerTransportError(
                "external authority response is malformed or noncanonical"
            ) from exc
        if (
            response.operation != operation
            or response.challenge != challenge
            or response.request_hash != wire_request.request_hash
            or response.authority_id != self._expected_authority_id
            or response.signature_purpose != EXTERNAL_DISPATCH_AUTHORITY_PURPOSE
            or response.schema_version != 1
        ):
            raise SourceBrokerV2SagaIntegrityError(
                "external authority signed response binding is invalid"
            )
        public_key = self._public_keys.get(response.key_id)
        if public_key is None:
            raise SourceBrokerV2SagaIntegrityError(
                "external authority response key id is not trusted"
            )
        if not _verify_external_authority_signature(
            public_key=public_key,
            signing_bytes=response.signing_bytes(),
            signature=response.signature,
            deadline=request_deadline,
        ):
            raise SourceBrokerV2SagaIntegrityError(
                "external authority response signature is invalid"
            )
        _require_deadline(request_deadline, stage="after external authority signature verification")
        try:
            result = strict_model_validate_canonical_json(
                ExternalDispatchAuthorityResponse,
                response.result,
            )
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerTransportError(
                "external authority result is malformed or noncanonical"
            ) from exc
        if result.operation != operation:
            raise SourceBrokerV2SagaIntegrityError(
                "external authority result operation binding is invalid"
            )
        if (
            result.operation_id != parsed_request.operation_id
            or result.request_binding_hash != parsed_request.request_binding_hash
        ):
            raise SourceBrokerV2SagaIntegrityError(
                "external authority result request binding is invalid"
            )
        if isinstance(parsed_request, ExternalDispatchCompleteRequest) and (
            result.status != "found"
            or result.authority_generation != parsed_request.authority_generation
            or result.authority_fence != parsed_request.authority_fence
            or result.result_hash != parsed_request.result_hash
            or result.result_bytes() != parsed_request.result_json.encode("utf-8")
        ):
            raise SourceBrokerV2SagaIntegrityError(
                "external authority completion result binding is invalid"
            )
        return canonical_model_json_bytes(result)

    def _bounded_deadline(self, caller_deadline: float | None) -> float:
        own_deadline = time.monotonic() + self.total_request_deadline_seconds
        return own_deadline if caller_deadline is None else min(own_deadline, caller_deadline)

    @staticmethod
    def _write_frame(
        connection: socket.socket,
        payload: bytes,
        *,
        deadline: float,
    ) -> None:
        if not payload or len(payload) > SOURCE_BROKER_V2_MAX_WIRE_BYTES:
            raise SourceBrokerTransportError("external authority frame size is invalid")
        pending = memoryview(len(payload).to_bytes(4, "big") + payload)
        while pending:
            connection.settimeout(_remaining(deadline, stage="before authority frame write"))
            try:
                sent = connection.send(pending)
            except OSError as exc:
                raise SourceBrokerTransportError("external authority frame write failed") from exc
            if type(sent) is not int or sent <= 0 or sent > len(pending):
                raise SourceBrokerTransportError("external authority frame write made no progress")
            pending = pending[sent:]
            _require_deadline(deadline, stage="after authority frame write")

    @classmethod
    def _read_frame(cls, connection: socket.socket, *, deadline: float) -> bytes:
        header = cls._recv_exact(connection, 4, deadline=deadline)
        size = int.from_bytes(header, "big", signed=False)
        if not 0 < size <= SOURCE_BROKER_V2_MAX_WIRE_BYTES:
            raise SourceBrokerTransportError("external authority frame size is invalid")
        return cls._recv_exact(connection, size, deadline=deadline)

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int, *, deadline: float) -> bytes:
        chunks: list[bytes] = []
        remaining_bytes = size
        while remaining_bytes:
            connection.settimeout(_remaining(deadline, stage="before authority frame read"))
            try:
                chunk = connection.recv(remaining_bytes)
            except OSError as exc:
                raise SourceBrokerTransportError("external authority frame read failed") from exc
            if type(chunk) is not bytes or not chunk or len(chunk) > remaining_bytes:
                raise SourceBrokerTransportError("external authority frame is truncated")
            chunks.append(chunk)
            remaining_bytes -= len(chunk)
            _require_deadline(deadline, stage="after authority frame read")
        return b"".join(chunks)


class SourceBrokerV2AuthoritySigner(Protocol):
    authority_id: str
    key_id: str

    def sign(self, signing_bytes: bytes, *, deadline: float | None = None) -> str: ...


class OpenSslSourceBrokerV2AuthoritySigner:
    """Ed25519 signer matching ``SourceAuthorityKeyring`` verification bytes."""

    def __init__(self, *, authority_id: str, key_id: str, private_key_path: Path) -> None:
        if not authority_id.strip() or not key_id.strip():
            raise ValueError("source authority signer identity is invalid")
        path = Path(private_key_path)
        if not path.is_absolute() or not path.is_file():
            raise ValueError("source authority private key path must be an existing absolute file")
        self.authority_id = authority_id
        self.key_id = key_id
        self.private_key_path = path
        self._openssl = _openssl_binary()

    def sign(self, signing_bytes: bytes, *, deadline: float | None = None) -> str:
        if type(signing_bytes) is not bytes or not signing_bytes:
            raise ValueError("source authority signing payload is invalid")
        payload_path: Path | None = None
        signature_path: Path | None = None
        try:
            _require_deadline(deadline, stage="before authority signing setup")
            payload_descriptor, payload_name = tempfile.mkstemp(prefix="rquant-source-v2-")
            signature_descriptor, signature_name = tempfile.mkstemp(prefix="rquant-source-v2-")
            payload_path = Path(payload_name)
            signature_path = Path(signature_name)
            with os.fdopen(payload_descriptor, "wb") as stream:
                stream.write(source_authority_signature_payload(signing_bytes))
            os.close(signature_descriptor)
            _require_deadline(deadline, stage="before authority signing")
            try:
                completed = subprocess.run(
                    (
                        self._openssl,
                        "pkeyutl",
                        "-sign",
                        "-inkey",
                        str(self.private_key_path),
                        "-rawin",
                        "-in",
                        str(payload_path),
                        "-out",
                        str(signature_path),
                    ),
                    check=False,
                    capture_output=True,
                    timeout=_remaining(deadline, stage="before authority signing process"),
                )
            except subprocess.TimeoutExpired as exc:
                raise SourceBrokerV2TransportDeadlineError(
                    "V2 source broker server deadline expired during authority signing"
                ) from exc
            if completed.returncode != 0:
                detail = completed.stderr.decode("utf-8", errors="replace")
                raise SourceBrokerTransportError("source authority signing failed: " + detail)
            _require_deadline(deadline, stage="after authority signing")
            return base64.b64encode(signature_path.read_bytes()).decode("ascii")
        finally:
            for path in (payload_path, signature_path):
                if path is not None:
                    with suppress(OSError):
                        path.unlink()


@dataclass(frozen=True)
class DecodedV2WireRequest:
    wire: SourceBrokerV2WireRequest

    def parse_payload(
        self,
    ) -> (
        SourceBrokerV2ClaimOnceRequest
        | SourceBrokerV2DispatchEnvelope
        | SourceBrokerV2FinalizeEnvelope
        | SourceBrokerV2ReplayRequest
    ):
        model: (
            type[SourceBrokerV2ClaimOnceRequest]
            | type[SourceBrokerV2DispatchEnvelope]
            | type[SourceBrokerV2FinalizeEnvelope]
            | type[SourceBrokerV2ReplayRequest]
        )
        if self.wire.operation == "claim_once":
            model = SourceBrokerV2ClaimOnceRequest
        elif self.wire.operation == "dispatch":
            model = SourceBrokerV2DispatchEnvelope
        elif self.wire.operation == "finalize":
            model = SourceBrokerV2FinalizeEnvelope
        elif self.wire.operation == "replay":
            model = SourceBrokerV2ReplayRequest
        else:  # pragma: no cover - Pydantic closes the union before this branch.
            raise SourceBrokerTransportError("V2 source broker operation is not allowed")
        try:
            return strict_model_validate_canonical_json(model, self.wire.payload)
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerTransportError(
                f"V2 source broker {self.wire.operation} payload is malformed or "
                f"noncanonical: {exc}"
            ) from exc


def decode_v2_wire_request(payload: bytes) -> DecodedV2WireRequest:
    if type(payload) is not bytes or not 0 < len(payload) <= SOURCE_BROKER_V2_MAX_WIRE_BYTES:
        raise SourceBrokerTransportError("V2 source broker wire request size is invalid")
    try:
        request = strict_model_validate_canonical_json(SourceBrokerV2WireRequest, payload)
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise SourceBrokerTransportError(
            f"V2 source broker wire request is malformed or noncanonical: {exc}"
        ) from exc
    return DecodedV2WireRequest(wire=request)


class SourceBrokerV2ProviderService:
    """Durable byte-only provider boundary for claim, dispatch, finalize, and replay."""

    @classmethod
    def create_for_test(
        cls,
        *,
        ledger_path: Path,
        provider: SourceBrokerV2Provider,
        authority_signer: SourceBrokerV2AuthoritySigner,
        authority_keyring: SourceAuthorityKeyring,
        external_dispatch_authority: ExternalDispatchAuthority,
        profile: Literal["nonproduction"],
        busy_timeout_ms: int = 5_000,
        clock: Callable[[], datetime] | None = None,
        max_inflight: int = 1,
        event_sink: Callable[[SourceBrokerV2SecurityEvent], None] | None = None,
    ) -> Self:
        if profile != "nonproduction":
            raise ValueError("test authority requires the explicit nonproduction profile")
        return cls(
            ledger_path=ledger_path,
            provider=provider,
            authority_signer=authority_signer,
            authority_keyring=authority_keyring,
            external_dispatch_authority=None,
            busy_timeout_ms=busy_timeout_ms,
            clock=clock,
            max_inflight=max_inflight,
            _nonproduction_authority=external_dispatch_authority,
            _nonproduction_event_sink=event_sink,
            _profile_token=_NONPRODUCTION_PROFILE_TOKEN,
        )

    def __init__(
        self,
        *,
        ledger_path: Path,
        provider: SourceBrokerV2Provider,
        authority_signer: SourceBrokerV2AuthoritySigner,
        authority_keyring: SourceAuthorityKeyring,
        external_dispatch_authority: UnixSocketExternalDispatchAuthorityClient | None = None,
        busy_timeout_ms: int = 5_000,
        clock: Callable[[], datetime] | None = None,
        max_inflight: int = 1,
        _nonproduction_authority: ExternalDispatchAuthority | None = None,
        _nonproduction_event_sink: Callable[[SourceBrokerV2SecurityEvent], None] | None = None,
        _profile_token: object | None = None,
    ) -> None:
        if type(authority_keyring) is not SourceAuthorityKeyring:
            raise TypeError("source provider service requires SourceAuthorityKeyring")
        if authority_signer.key_id not in authority_keyring.allowed_key_ids:
            raise ValueError("source provider signer key id is not trusted by keyring")
        if authority_signer.authority_id != authority_keyring.expected_authority_id:
            raise ValueError("source provider signer authority id conflicts with keyring")
        if _profile_token is _NONPRODUCTION_PROFILE_TOKEN:
            if external_dispatch_authority is not None or _nonproduction_authority is None:
                raise TypeError("nonproduction authority construction is invalid")
            if _nonproduction_event_sink is not None and not callable(_nonproduction_event_sink):
                raise TypeError("nonproduction event sink must be callable")
            if any(
                not callable(getattr(_nonproduction_authority, operation, None))
                for operation in ("reserve", "lookup", "complete")
            ):
                raise TypeError("test authority must implement the strict bytes protocol")
            selected_authority = _nonproduction_authority
            selected_event_sink = _nonproduction_event_sink or _ProductionSourceBrokerV2EventSink()
            self._profile = "nonproduction"
        else:
            if (
                _nonproduction_authority is not None
                or _nonproduction_event_sink is not None
                or _profile_token is not None
            ):
                raise TypeError("production authority profile cannot be overridden")
            if external_dispatch_authority is None:
                raise ValueError(
                    "production SourceBroker v2 requires an external dispatch authority"
                )
            if type(external_dispatch_authority) is not UnixSocketExternalDispatchAuthorityClient:
                raise TypeError(
                    "production SourceBroker v2 requires the exact Unix external authority client"
                )
            selected_authority = external_dispatch_authority
            selected_event_sink = _ProductionSourceBrokerV2EventSink()
            self._profile = "production"
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 1:
            raise ValueError("source provider busy timeout must be positive")
        if type(max_inflight) is not int or not 1 <= max_inflight <= 64:
            raise ValueError("source provider max_inflight must be between 1 and 64")
        self.ledger_path = Path(ledger_path)
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        self._provider = provider
        self._external_dispatch_authority = selected_authority
        self._signer = authority_signer
        self._authority_keyring = authority_keyring
        self._busy_timeout_ms = busy_timeout_ms
        self._clock = clock or (lambda: datetime.now(UTC))
        self._event_sink = selected_event_sink
        self._provider_gate = _ProviderInflightGate(max_inflight)
        self._process_owner_token = _current_process_owner_token()
        self._process_owner_pid = os.getpid()
        self._reconcile_fences: dict[str, _ReconcileFence] = {}
        self._reconcile_fences_lock = Lock()
        self._initialize()

    def stop(self) -> None:
        """Fail closed for new provider calls; existing workers retain their slots."""

        self._provider_gate.stop()

    def record_transport_event(
        self,
        *,
        category: Literal["peer_rejected", "transport_error", "write_error"],
        error: BaseException,
        operation_id: str | None = None,
    ) -> None:
        self._emit_security_event(
            phase="transport",
            operation_id=operation_id,
            error=error,
            category=category,
            reconcile=False,
        )

    def _emit_security_event(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase | Literal["claim_once", "replay", "transport"],
        category: Literal[
            "authority_error",
            "authority_lookup",
            "authority_lookup_error",
            "peer_rejected",
            "provider_capacity",
            "provider_deadline",
            "provider_exception",
            "provider_late_outcome",
            "provider_stopped",
            "reconcile",
            "transport_error",
            "write_error",
        ],
        reconcile: bool,
        operation_id: str | None = None,
        authority_operation: Literal["reserve", "lookup", "complete"] | None = None,
        error: BaseException | None = None,
        outcome: Literal["absent", "failure", "found", "success", "unknown"] | None = None,
    ) -> None:
        event_phase = phase.value if isinstance(phase, SourceBrokerV2OutboxPhase) else phase
        with suppress(Exception):
            exception_class = type(error).__name__ if error is not None else None
            if (
                exception_class is not None
                and re.fullmatch(
                    _EVENT_EXCEPTION_PATTERN,
                    exception_class,
                )
                is None
            ):
                exception_class = "Exception"
            event = SourceBrokerV2SecurityEvent(
                phase=event_phase,
                operation_hash=(
                    canonical_sha256(
                        {
                            "purpose": _EVENT_OPERATION_HASH_PURPOSE,
                            "operation_id": operation_id,
                        }
                    )
                    if operation_id is not None
                    else None
                ),
                authority_operation=authority_operation,
                exception_class=exception_class,
                category=category,
                outcome=outcome,
                reconcile=reconcile,
            )
            self._event_sink(event)

    def _emit_authority_error(
        self,
        *,
        authority_operation: Literal["reserve", "lookup", "complete"],
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        error: BaseException,
    ) -> None:
        self._emit_security_event(
            phase=phase,
            operation_id=operation_id,
            authority_operation=authority_operation,
            error=error,
            category="authority_error",
            outcome="failure",
            reconcile=True,
        )

    def _acquire_provider_lease_or_reconcile(
        self,
        *,
        operation_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float | None,
    ) -> _ProviderInflightLease:
        lease = self._provider_gate.try_acquire()
        if lease is not None:
            return lease
        stopped = self._provider_gate.stopped
        error = SourceBrokerTransportError(
            "source provider service is stopped" if stopped else _UNKNOWN_ERROR
        )
        self._emit_security_event(
            phase=phase,
            operation_id=operation_id,
            error=error,
            category="provider_stopped" if stopped else "provider_capacity",
            reconcile=True,
        )
        self._mark_unknown_for_operation(
            operation_id,
            "provider service stopped" if stopped else "provider capacity unavailable",
            phase=phase,
            operation_request_hash=operation_request_hash,
            claim_receipt=claim_receipt,
            deadline=deadline,
            error=error,
        )
        raise error

    def claim_once(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        _require_deadline(deadline, stage="before claim payload parsing")
        request = self._parse_model(SourceBrokerV2ClaimOnceRequest, payload, label="claim_once")
        _require_deadline(deadline, stage="after claim payload parsing")
        response = self._claim_once_response(request, deadline=deadline)
        _require_deadline(deadline, stage="after claim persistence")
        return canonical_model_json_bytes(response)

    def dispatch(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        _require_deadline(deadline, stage="before dispatch envelope parsing")
        envelope = self._parse_model(SourceBrokerV2DispatchEnvelope, payload, label="dispatch")
        _require_deadline(deadline, stage="after dispatch envelope parsing")
        self._authority_keyring.require_verified_claim(
            request=_claim_request_from_receipt(envelope.claim_receipt),
            receipt=envelope.claim_receipt,
        )
        if envelope.claim_receipt.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT:
            raise SourceBrokerTransportError("source dispatch lacks a definitive claim grant")
        _require_deadline(deadline, stage="after dispatch claim verification")
        return self._invoke_dispatch(
            envelope.request,
            claim_receipt=envelope.claim_receipt,
            deadline=deadline,
        )

    def finalize(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        _require_deadline(deadline, stage="before finalize envelope parsing")
        envelope = self._parse_model(SourceBrokerV2FinalizeEnvelope, payload, label="finalize")
        _require_deadline(deadline, stage="after finalize envelope parsing")
        self._authority_keyring.require_verified_claim(
            request=_claim_request_from_receipt(envelope.claim_receipt),
            receipt=envelope.claim_receipt,
        )
        if envelope.claim_receipt.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT:
            raise SourceBrokerTransportError("source finalize lacks a definitive claim grant")
        _require_deadline(deadline, stage="after finalize claim verification")
        return self._invoke_finalize(
            envelope.request,
            claim_receipt=envelope.claim_receipt,
            deadline=deadline,
        )

    def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        _require_deadline(deadline, stage="before replay payload parsing")
        request = self._parse_model(SourceBrokerV2ReplayRequest, payload, label="replay")
        _require_deadline(deadline, stage="after replay payload parsing")
        response = self._replay_response(request, deadline=deadline)
        _require_deadline(deadline, stage="after replay persistence")
        return canonical_model_json_bytes(response)

    def handle_wire_request(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        _require_deadline(deadline, stage="before wire request parsing")
        decoded = decode_v2_wire_request(payload)
        request = decoded.wire
        parsed = decoded.parse_payload()
        _require_deadline(deadline, stage="after wire request parsing")
        if request.operation == "claim_once" and isinstance(parsed, SourceBrokerV2ClaimOnceRequest):
            result = self.claim_once(request.payload, deadline=deadline)
        elif request.operation == "dispatch" and isinstance(parsed, SourceBrokerV2DispatchEnvelope):
            result = self.dispatch(request.payload, deadline=deadline)
        elif request.operation == "finalize" and isinstance(parsed, SourceBrokerV2FinalizeEnvelope):
            result = self.finalize(request.payload, deadline=deadline)
        elif request.operation == "replay" and isinstance(parsed, SourceBrokerV2ReplayRequest):
            result = self.replay(request.payload, deadline=deadline)
        else:  # pragma: no cover - discriminator and payload parser keep this unreachable.
            raise SourceBrokerTransportError("V2 source broker wire union is invalid")
        _require_deadline(deadline, stage="after provider service processing")
        response = SourceBrokerV2WireResponse(
            operation=request.operation,
            challenge=request.challenge,
            request_hash=request.request_hash,
            result=result,
            result_hash=_canonical_hash(result, label="wire response result"),
        )
        return canonical_model_json_bytes(response)

    def handle_wire_failure(self, request: SourceBrokerV2WireRequest, error: str) -> bytes:
        failure = SourceBrokerV2WireFailure(
            request_hash=request.request_hash,
            challenge=request.challenge,
            error=_safe_error(error),
        )
        return canonical_model_json_bytes(failure)

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS source_broker_v2_provider_operation (
                    id INTEGER PRIMARY KEY,
                    operation_id TEXT NOT NULL,
                    saga_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    operation_request_hash TEXT NOT NULL,
                    claim_binding_hash TEXT NOT NULL,
                    claim_generation INTEGER NOT NULL,
                    scheduler_fencing_token INTEGER NOT NULL,
                    executor_owner_token_hash TEXT NOT NULL,
                    executor_generation INTEGER NOT NULL,
                    max_external_deadline TEXT NOT NULL,
                    not_before_takeover_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    result_hash TEXT,
                    provider_started_at TEXT,
                    invocation_owner_token TEXT,
                    invocation_owner_pid INTEGER,
                    external_authority_generation INTEGER,
                    external_authority_fence TEXT,
                    external_request_binding_hash TEXT,
                    terminal_at TEXT,
                    unknown_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            _ensure_sqlite_column(
                connection,
                table="source_broker_v2_provider_operation",
                column="invocation_owner_token",
                definition="TEXT",
            )
            _ensure_sqlite_column(
                connection,
                table="source_broker_v2_provider_operation",
                column="invocation_owner_pid",
                definition="INTEGER",
            )
            _ensure_sqlite_column(
                connection,
                table="source_broker_v2_provider_operation",
                column="external_authority_generation",
                definition="INTEGER",
            )
            _ensure_sqlite_column(
                connection,
                table="source_broker_v2_provider_operation",
                column="external_authority_fence",
                definition="TEXT",
            )
            _ensure_sqlite_column(
                connection,
                table="source_broker_v2_provider_operation",
                column="external_request_binding_hash",
                definition="TEXT",
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "source_broker_v2_provider_operation_operation_id_uq "
                "ON source_broker_v2_provider_operation(operation_id)"
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "source_broker_v2_provider_operation_operation_phase_uq "
                "ON source_broker_v2_provider_operation(operation_id, phase)"
            )

    def reconcile_abandoned_invocations_after_listener_acquired(
        self,
        *,
        deadline: float | None = None,
    ) -> None:
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "UPDATE source_broker_v2_provider_operation "
                    "SET status = ?, unknown_reason = COALESCE(unknown_reason, ?), "
                    "updated_at = ? WHERE status = ? AND "
                    "(invocation_owner_token IS NULL OR invocation_owner_token != ?)",
                    (
                        _RECONCILE_STATUS,
                        "provider invocation was active under a previous daemon process epoch",
                        _now_text(self._now()),
                        _INVOKING_STATUS,
                        self._process_owner_token,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _connect(
        self,
        *,
        deadline: float | None = None,
        sqlite_timeout_seconds: float | None = None,
    ) -> sqlite3.Connection:
        sqlite_timeout = (
            self._sqlite_timeout_seconds(deadline)
            if sqlite_timeout_seconds is None
            else max(0.001, sqlite_timeout_seconds)
        )
        connection = sqlite3.connect(
            self.ledger_path,
            timeout=sqlite_timeout,
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            journal_mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()
            if journal_mode is None or not journal_mode or str(journal_mode[0]).lower() != "wal":
                raise SourceBrokerTransportError(
                    "source provider ledger requires SQLite journal_mode WAL"
                )
            connection.execute("PRAGMA synchronous=FULL")
            synchronous = connection.execute("PRAGMA synchronous").fetchone()
            if synchronous is None or not synchronous or int(synchronous[0]) != 2:
                raise SourceBrokerTransportError(
                    "source provider ledger requires SQLite synchronous FULL"
                )
            connection.execute(f"PRAGMA busy_timeout={max(1, int(sqlite_timeout * 1000))}")
            connection.execute("PRAGMA foreign_keys=ON")
            return connection
        except BaseException:
            connection.close()
            raise

    def _sqlite_timeout_seconds(self, deadline: float | None) -> float:
        timeout = self._busy_timeout_ms / 1000
        remaining = _remaining(deadline, stage="before sqlite connection")
        if remaining is None:
            return timeout
        return max(0.001, min(timeout, remaining))

    def _claim_once_response(
        self,
        request: SourceBrokerV2ClaimOnceRequest,
        *,
        deadline: float | None,
    ) -> SourceBrokerV2ClaimOnceResponse:
        if self._has_reconcile_fence_for_claim(request, deadline=deadline):
            _require_deadline(deadline, stage="before claim response signing")
            return self._signed_claim_response(
                request=request,
                status=SourceBrokerV2ClaimStatus.UNKNOWN,
                result=None,
                deadline=deadline,
            )
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_operation(connection, request.operation_id)
                if row is None:
                    status = SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
                    result = None
                    self._insert_claim(connection, request, status=_ACTIVE_STATUS)
                else:
                    self._validate_claim_binding(row, request)
                    status, result = self._claim_status_for_row(connection, row, request)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        _require_deadline(deadline, stage="before claim response signing")
        return self._signed_claim_response(
            request=request,
            status=status,
            result=result,
            deadline=deadline,
        )

    def _claim_status_for_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        request: SourceBrokerV2ClaimOnceRequest,
    ) -> tuple[SourceBrokerV2ClaimStatus, bytes | None]:
        status = str(row["status"])
        if status in _TERMINAL_STATUSES:
            result = self._row_result(row)
            return SourceBrokerV2ClaimStatus(status.upper()), result
        if status in {_INVOKING_STATUS, _RECONCILE_STATUS}:
            if (
                status == _INVOKING_STATUS
                and row["invocation_owner_token"] != self._process_owner_token
            ):
                self._mark_reconcile(connection, request.operation_id, "provider is in progress")
            return SourceBrokerV2ClaimStatus.UNKNOWN, None
        if status != _ACTIVE_STATUS:
            raise SourceBrokerTransportError("source operation ledger status is invalid")
        persisted_takeover = _row_datetime(row, "not_before_takeover_at")
        if (
            row["executor_owner_token_hash"] != request.executor_owner_token_hash
            or row["executor_generation"] != request.executor_generation
        ) and self._now() < persisted_takeover:
            return SourceBrokerV2ClaimStatus.INFLIGHT, None
        if (
            row["executor_owner_token_hash"] != request.executor_owner_token_hash
            or row["executor_generation"] != request.executor_generation
        ):
            connection.execute(
                "UPDATE source_broker_v2_provider_operation SET "
                "executor_owner_token_hash = ?, executor_generation = ?, updated_at = ? "
                "WHERE operation_id = ?",
                (
                    request.executor_owner_token_hash,
                    request.executor_generation,
                    _now_text(self._now()),
                    request.operation_id,
                ),
            )
        return SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT, None

    def _invoke_dispatch(
        self,
        request: SourceBrokerV2DispatchRequest,
        *,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float | None,
    ) -> bytes:
        recovered = self._recover_external_before_invocation(
            operation_id=request.operation_id,
            saga_id=request.saga_id,
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
            operation_request_hash=request.request_hash,
            claim_receipt=claim_receipt,
            deadline=deadline,
        )
        if recovered is not None:
            return recovered
        terminal = self._begin_invocation(
            operation_id=request.operation_id,
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
            operation_request_hash=request.request_hash,
            claim_receipt=claim_receipt,
            deadline=deadline,
        )
        if terminal is not None:
            return terminal
        lease = self._acquire_provider_lease_or_reconcile(
            operation_id=request.operation_id,
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
            operation_request_hash=request.request_hash,
            claim_receipt=claim_receipt,
            deadline=deadline,
        )
        provider_call_started = False
        try:
            reservation = self._reserve_external_authority(
                operation_id=request.operation_id,
                saga_id=request.saga_id,
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_request_hash=request.request_hash,
                claim_receipt=claim_receipt,
                deadline=deadline,
            )
            self._record_external_authority_decision(
                operation_id=request.operation_id,
                reservation=reservation,
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                deadline=deadline,
            )
            if reservation.status == "unknown":
                raise SourceBrokerTransportError(_UNKNOWN_ERROR)
            if reservation.status == "found":
                return self._repair_external_terminal(
                    operation_id=request.operation_id,
                    saga_id=request.saga_id,
                    phase=SourceBrokerV2OutboxPhase.DISPATCH,
                    operation_request_hash=request.request_hash,
                    response=reservation,
                    deadline=deadline,
                )
            self._mark_provider_started(
                operation_id=request.operation_id,
                reservation=reservation,
                deadline=deadline,
            )
            _require_deadline(deadline, stage="before provider dispatch")
            provider_call_started = True
            result = self._provider_call_before_deadline(
                lambda: SourceBrokerV2ProviderDispatchResult.model_validate(
                    self._provider.dispatch(request),
                    strict=True,
                ),
                deadline=deadline,
                stage="provider dispatch",
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_id=request.operation_id,
                lease=lease,
            )
            _require_deadline(deadline, stage="after provider dispatch")
            response = SourceBrokerV2DispatchResponse(
                saga_id=request.saga_id,
                operation_id=request.operation_id,
                call_id=request.call_id,
                request_hash=request.request_hash,
                outcome=result.outcome,
                response=result.response,
                response_hash=_canonical_hash(result.response, label="dispatch response"),
                transport_receipt=result.transport_receipt,
            )
            raw = canonical_model_json_bytes(response)
            self._complete_external_authority(
                operation_id=request.operation_id,
                reservation=reservation,
                result=raw,
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                deadline=deadline,
            )
            self._complete_invocation(
                operation_id=request.operation_id,
                status=result.outcome.value.lower(),
                result=raw,
                deadline=deadline,
            )
            return raw
        except SourceBrokerV2TransportDeadlineError as exc:
            self._mark_unknown_for_operation(
                request.operation_id,
                "provider dispatch deadline",
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_request_hash=request.request_hash,
                claim_receipt=claim_receipt,
                deadline=deadline,
                error=exc,
            )
            raise
        except Exception as exc:
            self._mark_unknown_for_operation(
                request.operation_id,
                "provider dispatch failed",
                phase=SourceBrokerV2OutboxPhase.DISPATCH,
                operation_request_hash=request.request_hash,
                claim_receipt=claim_receipt,
                deadline=deadline,
                error=exc,
            )
            raise SourceBrokerTransportError(_UNKNOWN_ERROR) from exc
        finally:
            if not provider_call_started:
                lease.release()

    def _invoke_finalize(
        self,
        request: SourceBrokerV2FinalizeRequest,
        *,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float | None,
    ) -> bytes:
        recovered = self._recover_external_before_invocation(
            operation_id=request.operation_id,
            saga_id=request.saga_id,
            phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
            operation_request_hash=request.request_hash,
            claim_receipt=claim_receipt,
            deadline=deadline,
        )
        if recovered is not None:
            return recovered
        terminal = self._begin_invocation(
            operation_id=request.operation_id,
            phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
            operation_request_hash=request.request_hash,
            claim_receipt=claim_receipt,
            deadline=deadline,
        )
        if terminal is not None:
            return terminal
        lease = self._acquire_provider_lease_or_reconcile(
            operation_id=request.operation_id,
            phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
            operation_request_hash=request.request_hash,
            claim_receipt=claim_receipt,
            deadline=deadline,
        )
        provider_call_started = False
        try:
            reservation = self._reserve_external_authority(
                operation_id=request.operation_id,
                saga_id=request.saga_id,
                phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                operation_request_hash=request.request_hash,
                claim_receipt=claim_receipt,
                deadline=deadline,
            )
            self._record_external_authority_decision(
                operation_id=request.operation_id,
                reservation=reservation,
                phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                deadline=deadline,
            )
            if reservation.status == "unknown":
                raise SourceBrokerTransportError(_UNKNOWN_ERROR)
            if reservation.status == "found":
                return self._repair_external_terminal(
                    operation_id=request.operation_id,
                    saga_id=request.saga_id,
                    phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                    operation_request_hash=request.request_hash,
                    response=reservation,
                    deadline=deadline,
                )
            self._mark_provider_started(
                operation_id=request.operation_id,
                reservation=reservation,
                deadline=deadline,
            )
            _require_deadline(deadline, stage="before provider finalize")
            provider_call_started = True
            result = self._provider_call_before_deadline(
                lambda: SourceBrokerV2ProviderFinalizeResult.model_validate(
                    self._provider.finalize(request),
                    strict=True,
                ),
                deadline=deadline,
                stage="provider finalize",
                phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                operation_id=request.operation_id,
                lease=lease,
            )
            _require_deadline(deadline, stage="after provider finalize")
            response = SourceBrokerV2FinalizeResponse(
                saga_id=request.saga_id,
                operation_id=request.operation_id,
                request_hash=request.request_hash,
                final_receipt=result.final_receipt,
                final_receipt_hash=_canonical_hash(
                    result.final_receipt,
                    label="final receipt",
                ),
            )
            raw = canonical_model_json_bytes(response)
            self._complete_external_authority(
                operation_id=request.operation_id,
                reservation=reservation,
                result=raw,
                phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                deadline=deadline,
            )
            self._complete_invocation(
                operation_id=request.operation_id,
                status=_DISPATCH_SUCCESS,
                result=raw,
                deadline=deadline,
            )
            return raw
        except SourceBrokerV2TransportDeadlineError as exc:
            self._mark_unknown_for_operation(
                request.operation_id,
                "provider finalize deadline",
                phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                operation_request_hash=request.request_hash,
                claim_receipt=claim_receipt,
                deadline=deadline,
                error=exc,
            )
            raise
        except Exception as exc:
            self._mark_unknown_for_operation(
                request.operation_id,
                "provider finalize failed",
                phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                operation_request_hash=request.request_hash,
                claim_receipt=claim_receipt,
                deadline=deadline,
                error=exc,
            )
            raise SourceBrokerTransportError(_UNKNOWN_ERROR) from exc
        finally:
            if not provider_call_started:
                lease.release()

    def _provider_call_before_deadline(
        self,
        call: Callable[[], _ProviderResultT],
        *,
        deadline: float | None,
        stage: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        lease: _ProviderInflightLease,
    ) -> _ProviderResultT:
        if deadline is None:
            try:
                return call()
            except BaseException as exc:
                self._emit_security_event(
                    phase=phase,
                    operation_id=operation_id,
                    error=exc,
                    category="provider_exception",
                    reconcile=True,
                )
                raise
            finally:
                lease.release()
        try:
            remaining = _remaining(deadline, stage=f"before {stage}")
        except BaseException:
            lease.release()
            raise
        reserve = min(_PROVIDER_DEADLINE_RESERVE_SECONDS, remaining / 2)
        wait_seconds = remaining - reserve
        queue: Queue[tuple[bool, object]] = Queue(maxsize=1)
        abandoned = Event()
        late_event_lock = Lock()
        late_event_emitted = False

        def emit_late_once(ok: bool, value: object) -> None:
            nonlocal late_event_emitted
            with late_event_lock:
                if late_event_emitted:
                    return
                late_event_emitted = True
            self._emit_security_event(
                phase=phase,
                operation_id=operation_id,
                error=value if not ok and isinstance(value, BaseException) else None,
                category="provider_late_outcome",
                outcome="success" if ok else "failure",
                reconcile=True,
            )

        def invoke() -> None:
            outcome: tuple[bool, object]
            try:
                outcome = (True, call())
            except BaseException as exc:  # noqa: BLE001
                outcome = (False, exc)
            try:
                with suppress(Exception):
                    queue.put_nowait(outcome)
                if abandoned.is_set():
                    emit_late_once(*outcome)
            finally:
                lease.release()

        thread = Thread(target=invoke, name=_PROVIDER_THREAD_NAME, daemon=True)
        try:
            thread.start()
        except BaseException:
            lease.release()
            raise
        try:
            ok, value = queue.get(timeout=wait_seconds)
        except Empty as exc:
            abandoned.set()
            with suppress(Empty):
                emit_late_once(*queue.get_nowait())
            deadline_error = SourceBrokerV2TransportDeadlineError(
                f"V2 source broker server deadline expired during {stage}"
            )
            self._emit_security_event(
                phase=phase,
                operation_id=operation_id,
                error=deadline_error,
                category="provider_deadline",
                reconcile=True,
            )
            raise SourceBrokerV2TransportDeadlineError(
                f"V2 source broker server deadline expired during {stage}"
            ) from exc
        if time.monotonic() >= deadline:
            abandoned.set()
            emit_late_once(ok, value)
            deadline_error = SourceBrokerV2TransportDeadlineError(
                f"V2 source broker server deadline expired during {stage}"
            )
            self._emit_security_event(
                phase=phase,
                operation_id=operation_id,
                error=deadline_error,
                category="provider_deadline",
                reconcile=True,
            )
            raise deadline_error
        if ok:
            return cast(_ProviderResultT, value)
        if isinstance(value, BaseException):
            self._emit_security_event(
                phase=phase,
                operation_id=operation_id,
                error=value,
                category="provider_exception",
                reconcile=True,
            )
            raise value
        raise SourceBrokerTransportError(f"source provider {stage} failed")

    def _reserve_external_authority(
        self,
        *,
        operation_id: str,
        saga_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float | None,
    ) -> ExternalDispatchAuthorityResponse:
        request = self._external_authority_request(
            operation_id=operation_id,
            saga_id=saga_id,
            phase=phase,
            operation_request_hash=operation_request_hash,
            claim_receipt=claim_receipt,
        )
        try:
            _require_deadline(deadline, stage="before external dispatch authority reserve")
            raw = self._external_dispatch_authority.reserve(
                canonical_model_json_bytes(request),
                deadline=deadline,
            )
            if type(raw) is not bytes or not 0 < len(raw) <= SOURCE_BROKER_V2_MAX_WIRE_BYTES:
                raise SourceBrokerTransportError("external dispatch authority response is invalid")
            response = self._parse_model(
                ExternalDispatchAuthorityResponse,
                raw,
                label="external dispatch authority reserve",
            )
            if (
                response.operation != "reserve"
                or response.operation_id != operation_id
                or response.request_binding_hash != request.request_binding_hash
            ):
                raise SourceBrokerTransportError("external dispatch authority binding is invalid")
        except Exception as exc:
            self._emit_authority_error(
                authority_operation="reserve",
                phase=phase,
                operation_id=operation_id,
                error=exc,
            )
            if isinstance(exc, SourceBrokerV2TransportDeadlineError):
                raise SourceBrokerV2TransportDeadlineError(
                    "V2 source broker deadline expired during external authority reserve"
                ) from exc
            raise SourceBrokerTransportError(_UNKNOWN_ERROR) from exc
        return response

    def _lookup_external_authority(
        self,
        request: ExternalDispatchReserveRequest,
        *,
        deadline: float | None,
    ) -> ExternalDispatchAuthorityResponse:
        try:
            _require_deadline(deadline, stage="before external dispatch authority lookup")
            raw = self._external_dispatch_authority.lookup(
                canonical_model_json_bytes(request),
                deadline=deadline,
            )
            if type(raw) is not bytes or not 0 < len(raw) <= SOURCE_BROKER_V2_MAX_WIRE_BYTES:
                raise SourceBrokerTransportError("external dispatch authority lookup is invalid")
            response = self._parse_model(
                ExternalDispatchAuthorityResponse,
                raw,
                label="external dispatch authority lookup",
            )
            if (
                response.operation != "lookup"
                or response.operation_id != request.operation_id
                or response.request_binding_hash != request.request_binding_hash
            ):
                raise SourceBrokerTransportError(
                    "external dispatch authority lookup binding is invalid"
                )
        except Exception as exc:
            self._emit_authority_error(
                authority_operation="lookup",
                phase=request.phase,
                operation_id=request.operation_id,
                error=exc,
            )
            if isinstance(exc, SourceBrokerV2TransportDeadlineError):
                raise SourceBrokerV2TransportDeadlineError(
                    "V2 source broker deadline expired during external authority lookup"
                ) from exc
            raise SourceBrokerTransportError(_UNKNOWN_ERROR) from exc
        self._emit_security_event(
            phase=request.phase,
            operation_id=request.operation_id,
            authority_operation="lookup",
            category="authority_lookup",
            outcome=response.status,
            reconcile=response.status == "unknown",
        )
        return response

    @staticmethod
    def _external_authority_request(
        *,
        operation_id: str,
        saga_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
    ) -> ExternalDispatchReserveRequest:
        return ExternalDispatchReserveRequest(
            operation_id=operation_id,
            saga_id=saga_id,
            phase=phase,
            operation_request_hash=operation_request_hash,
            claim_binding_hash=claim_receipt.claim_binding_hash,
            claim_generation=claim_receipt.claim_generation,
            scheduler_fencing_token=claim_receipt.scheduler_fencing_token,
            executor_owner_token_hash=claim_receipt.executor_owner_token_hash,
            executor_generation=claim_receipt.executor_generation,
            max_external_deadline=claim_receipt.max_external_deadline,
            not_before_takeover_at=claim_receipt.not_before_takeover_at,
        )

    def _recover_external_before_invocation(
        self,
        *,
        operation_id: str,
        saga_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float | None,
    ) -> bytes | None:
        process_fence = self._reconcile_fence(operation_id)
        if process_fence is not None:
            self._validate_reconcile_fence_operation(
                process_fence,
                phase=phase,
                operation_request_hash=operation_request_hash,
                saga_id=saga_id,
            )
            self._validate_reconcile_fence_claim_receipt(process_fence, claim_receipt)
        with self._connect(deadline=deadline) as connection:
            row = self._read_operation(connection, operation_id)
            if row is None:
                raise SourceBrokerTransportError("source operation was not claimed")
            self._validate_operation_request(
                row,
                phase,
                operation_request_hash,
                saga_id=saga_id,
            )
            self._validate_claim_receipt_binding(row, claim_receipt)
            status = str(row["status"])
            terminal_loss = False
            if status in _TERMINAL_STATUSES:
                try:
                    return self._row_result(row)
                except SourceBrokerTransportError:
                    terminal_loss = True
            if (
                status not in {_ACTIVE_STATUS, _INVOKING_STATUS, _RECONCILE_STATUS}
                and not terminal_loss
            ):
                raise SourceBrokerTransportError("source operation ledger status is invalid")
            provider_started = row["provider_started_at"] is not None
            same_process_invocation = (
                status == _INVOKING_STATUS
                and row["invocation_owner_token"] == self._process_owner_token
            )
            needs_lookup = (
                terminal_loss
                or not same_process_invocation
                and (
                    process_fence is not None
                    or provider_started
                    or status in {_INVOKING_STATUS, _RECONCILE_STATUS}
                )
            )
            if process_fence is not None:
                needs_lookup = True
            if not needs_lookup:
                return None
            authority_request = self._external_authority_request(
                operation_id=operation_id,
                saga_id=saga_id,
                phase=phase,
                operation_request_hash=operation_request_hash,
                claim_receipt=claim_receipt,
            )
        try:
            observed = self._lookup_external_authority(authority_request, deadline=deadline)
        except Exception as exc:
            self._mark_unknown_for_operation(
                operation_id,
                "external authority lookup failed",
                phase=phase,
                operation_request_hash=operation_request_hash,
                claim_receipt=claim_receipt,
                deadline=deadline,
                error=exc,
            )
            raise SourceBrokerTransportError(_UNKNOWN_ERROR) from exc
        if observed.status == "found":
            return self._repair_external_terminal(
                operation_id=operation_id,
                saga_id=saga_id,
                phase=phase,
                operation_request_hash=operation_request_hash,
                response=observed,
                deadline=deadline,
            )
        if observed.status == "absent" and not provider_started:
            self._reset_unstarted_invocation_for_reserve(
                operation_id=operation_id,
                deadline=deadline,
            )
            return None
        if process_fence is not None:
            self._persist_reconcile_fence_if_possible(process_fence, deadline=deadline)
            raise SourceBrokerTransportError(_UNKNOWN_ERROR)
        reason = (
            "external authority is UNKNOWN"
            if observed.status == "unknown"
            else "external authority is ABSENT after provider start"
        )
        try:
            self._persist_external_reconcile(
                operation_id=operation_id,
                phase=phase,
                observed=observed,
                reason=reason,
                deadline=deadline,
            )
        except sqlite3.Error as exc:
            self._mark_unknown_for_operation(
                operation_id,
                reason,
                phase=phase,
                operation_request_hash=operation_request_hash,
                claim_receipt=claim_receipt,
                deadline=deadline,
            )
            raise SourceBrokerTransportError(_UNKNOWN_ERROR) from exc
        raise SourceBrokerTransportError(_UNKNOWN_ERROR)

    def _reset_unstarted_invocation_for_reserve(
        self,
        *,
        operation_id: str,
        deadline: float | None,
    ) -> None:
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_operation(connection, operation_id)
                if row is None or row["provider_started_at"] is not None:
                    raise SourceBrokerTransportError(_UNKNOWN_ERROR)
                if str(row["status"]) not in {
                    _ACTIVE_STATUS,
                    _INVOKING_STATUS,
                    _RECONCILE_STATUS,
                }:
                    raise SourceBrokerTransportError(_UNKNOWN_ERROR)
                connection.execute(
                    "UPDATE source_broker_v2_provider_operation SET status = ?, "
                    "invocation_owner_token = NULL, invocation_owner_pid = NULL, "
                    "unknown_reason = NULL, updated_at = ? WHERE operation_id = ?",
                    (_ACTIVE_STATUS, _now_text(self._now()), operation_id),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        self._clear_reconcile_fence(operation_id)

    def _persist_external_reconcile(
        self,
        *,
        operation_id: str,
        phase: SourceBrokerV2OutboxPhase,
        observed: ExternalDispatchAuthorityResponse,
        reason: str,
        deadline: float | None,
    ) -> None:
        authority_conflict = False
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_operation(connection, operation_id)
                if row is None:
                    raise SourceBrokerTransportError("source operation disappeared")
                if self._external_fence_conflicts(row, observed):
                    authority_conflict = True
                    reason = "external dispatch authority returned an old or conflicting fence"
                else:
                    connection.execute(
                        "UPDATE source_broker_v2_provider_operation SET "
                        "external_authority_generation = ?, external_authority_fence = ?, "
                        "external_request_binding_hash = ? WHERE operation_id = ?",
                        (
                            observed.authority_generation,
                            observed.authority_fence,
                            observed.request_binding_hash,
                            operation_id,
                        ),
                    )
                self._mark_reconcile(connection, operation_id, reason)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        if authority_conflict:
            self._emit_authority_error(
                authority_operation=observed.operation,
                phase=phase,
                operation_id=operation_id,
                error=SourceBrokerTransportError(
                    "external dispatch authority returned an old or conflicting fence"
                ),
            )

    def _repair_external_terminal(
        self,
        *,
        operation_id: str,
        saga_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        response: ExternalDispatchAuthorityResponse,
        deadline: float | None,
    ) -> bytes:
        try:
            if response.status != "found":
                raise SourceBrokerTransportError(
                    "external authority terminal result is unavailable"
                )
            result = response.result_bytes()
            status = self._external_terminal_status(
                operation_id=operation_id,
                saga_id=saga_id,
                phase=phase,
                operation_request_hash=operation_request_hash,
                result=result,
            )
            result_hash = _canonical_hash(result, label="external authority terminal result")
        except Exception as exc:
            self._emit_authority_error(
                authority_operation=response.operation,
                phase=phase,
                operation_id=operation_id,
                error=exc,
            )
            raise
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_operation(connection, operation_id)
                if row is None:
                    raise SourceBrokerTransportError("source operation disappeared")
                self._validate_operation_request(
                    row,
                    phase,
                    operation_request_hash,
                    saga_id=saga_id,
                )
                if self._external_fence_conflicts(row, response):
                    conflict = SourceBrokerTransportError(
                        "external dispatch authority returned an old or conflicting fence"
                    )
                    self._mark_reconcile(
                        connection,
                        operation_id,
                        str(conflict),
                    )
                    connection.commit()
                    self._emit_authority_error(
                        authority_operation=response.operation,
                        phase=phase,
                        operation_id=operation_id,
                        error=conflict,
                    )
                    raise SourceBrokerTransportError(_UNKNOWN_ERROR) from conflict
                row_status = str(row["status"])
                if row_status in _TERMINAL_STATUSES:
                    try:
                        existing = self._row_result(row)
                    except SourceBrokerTransportError:
                        existing = None
                    if existing is not None:
                        if existing != result or row_status != status:
                            conflict = SourceBrokerTransportError(
                                "external authority terminal conflicts with local terminal"
                            )
                            self._emit_authority_error(
                                authority_operation=response.operation,
                                phase=phase,
                                operation_id=operation_id,
                                error=conflict,
                            )
                            raise conflict
                        connection.commit()
                        self._clear_reconcile_fence(operation_id)
                        return existing
                updated = connection.execute(
                    "UPDATE source_broker_v2_provider_operation SET status = ?, "
                    "result_json = ?, result_hash = ?, external_authority_generation = ?, "
                    "external_authority_fence = ?, external_request_binding_hash = ?, "
                    "terminal_at = ?, invocation_owner_token = NULL, invocation_owner_pid = NULL, "
                    "unknown_reason = NULL, updated_at = ? WHERE operation_id = ?",
                    (
                        status,
                        result.decode("utf-8"),
                        result_hash,
                        response.authority_generation,
                        response.authority_fence,
                        response.request_binding_hash,
                        _now_text(self._now()),
                        _now_text(self._now()),
                        operation_id,
                    ),
                ).rowcount
                if updated != 1:
                    raise SourceBrokerTransportError("external terminal repair failed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        self._clear_reconcile_fence(operation_id)
        return result

    def _external_terminal_status(
        self,
        *,
        operation_id: str,
        saga_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        result: bytes,
    ) -> str:
        if phase is SourceBrokerV2OutboxPhase.DISPATCH:
            recovered = self._parse_model(
                SourceBrokerV2DispatchResponse,
                result,
                label="external dispatch recovery",
            )
            if (
                recovered.operation_id != operation_id
                or recovered.saga_id != saga_id
                or recovered.request_hash != operation_request_hash
            ):
                raise SourceBrokerTransportError("external dispatch recovery binding is invalid")
            return recovered.outcome.value.lower()
        recovered_finalize = self._parse_model(
            SourceBrokerV2FinalizeResponse,
            result,
            label="external finalize recovery",
        )
        if (
            recovered_finalize.operation_id != operation_id
            or recovered_finalize.saga_id != saga_id
            or recovered_finalize.request_hash != operation_request_hash
        ):
            raise SourceBrokerTransportError("external finalize recovery binding is invalid")
        return _DISPATCH_SUCCESS

    @staticmethod
    def _external_fence_conflicts(
        row: sqlite3.Row,
        response: ExternalDispatchAuthorityResponse,
    ) -> bool:
        previous_binding = row["external_request_binding_hash"]
        if previous_binding is not None and previous_binding != response.request_binding_hash:
            return True
        previous_generation = row["external_authority_generation"]
        previous_fence = row["external_authority_fence"]
        if previous_generation is None:
            return False
        numeric_generation = int(previous_generation)
        return response.authority_generation < numeric_generation or (
            response.authority_generation == numeric_generation
            and previous_fence != response.authority_fence
        )

    def _record_external_authority_decision(
        self,
        *,
        operation_id: str,
        reservation: ExternalDispatchAuthorityResponse,
        phase: SourceBrokerV2OutboxPhase,
        deadline: float | None,
    ) -> None:
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_operation(connection, operation_id)
                if row is None or str(row["status"]) != _INVOKING_STATUS:
                    raise SourceBrokerTransportError(
                        "source operation changed before external authority decision"
                    )
                if row["invocation_owner_token"] != self._process_owner_token:
                    raise SourceBrokerTransportError(_UNKNOWN_ERROR)
                previous_generation = row["external_authority_generation"]
                previous_fence = row["external_authority_fence"]
                previous_binding = row["external_request_binding_hash"]
                stale = reservation.operation != "reserve"
                if previous_generation is not None:
                    numeric_generation = int(previous_generation)
                    stale = (
                        reservation.authority_generation < numeric_generation
                        or (
                            reservation.authority_generation == numeric_generation
                            and previous_fence != reservation.authority_fence
                        )
                        or (
                            reservation.status == "absent"
                            and (
                                reservation.authority_generation <= numeric_generation
                                or previous_fence == reservation.authority_fence
                            )
                        )
                    )
                if (
                    previous_binding is not None
                    and previous_binding != reservation.request_binding_hash
                ):
                    stale = True
                if stale:
                    self._mark_reconcile(
                        connection,
                        operation_id,
                        "external dispatch authority returned an old or conflicting fence",
                    )
                    connection.commit()
                    error = SourceBrokerTransportError(_UNKNOWN_ERROR)
                    self._emit_authority_error(
                        authority_operation="reserve",
                        phase=phase,
                        operation_id=operation_id,
                        error=error,
                    )
                    raise error
                next_status = (
                    _RECONCILE_STATUS if reservation.status == "unknown" else _INVOKING_STATUS
                )
                unknown_reason = _UNKNOWN_ERROR if reservation.status == "unknown" else None
                updated = connection.execute(
                    "UPDATE source_broker_v2_provider_operation SET "
                    "external_authority_generation = ?, external_authority_fence = ?, "
                    "external_request_binding_hash = ?, status = ?, unknown_reason = ?, "
                    "updated_at = ? WHERE operation_id = ? AND status = ? "
                    "AND invocation_owner_token = ?",
                    (
                        reservation.authority_generation,
                        reservation.authority_fence,
                        reservation.request_binding_hash,
                        next_status,
                        unknown_reason,
                        _now_text(self._now()),
                        operation_id,
                        _INVOKING_STATUS,
                        self._process_owner_token,
                    ),
                ).rowcount
                if updated != 1:
                    error = SourceBrokerTransportError(
                        "external dispatch authority decision persistence failed"
                    )
                    self._emit_authority_error(
                        authority_operation="reserve",
                        phase=phase,
                        operation_id=operation_id,
                        error=error,
                    )
                    raise error
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _mark_provider_started(
        self,
        *,
        operation_id: str,
        reservation: ExternalDispatchAuthorityResponse,
        deadline: float | None,
    ) -> None:
        if reservation.operation != "reserve" or reservation.status != "absent":
            raise SourceBrokerTransportError(
                "provider invocation lacks a fresh external authority reservation"
            )
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                updated = connection.execute(
                    "UPDATE source_broker_v2_provider_operation SET provider_started_at = ?, "
                    "updated_at = ? WHERE operation_id = ? AND status = ? "
                    "AND invocation_owner_token = ? AND provider_started_at IS NULL "
                    "AND external_authority_generation = ? AND external_authority_fence = ? "
                    "AND external_request_binding_hash = ?",
                    (
                        _now_text(self._now()),
                        _now_text(self._now()),
                        operation_id,
                        _INVOKING_STATUS,
                        self._process_owner_token,
                        reservation.authority_generation,
                        reservation.authority_fence,
                        reservation.request_binding_hash,
                    ),
                ).rowcount
                if updated != 1:
                    raise SourceBrokerTransportError(
                        "provider start could not be fenced by external authority"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _complete_external_authority(
        self,
        *,
        operation_id: str,
        reservation: ExternalDispatchAuthorityResponse,
        result: bytes,
        phase: SourceBrokerV2OutboxPhase,
        deadline: float | None,
    ) -> None:
        try:
            if reservation.status != "absent":
                raise SourceBrokerTransportError(
                    "external dispatch authority completion lacks a fresh reservation"
                )
            result_hash = _canonical_hash(result, label="external authority terminal result")
            request = ExternalDispatchCompleteRequest(
                operation_id=operation_id,
                request_binding_hash=reservation.request_binding_hash,
                authority_generation=reservation.authority_generation,
                authority_fence=reservation.authority_fence,
                result_json=result.decode("utf-8"),
                result_hash=result_hash,
            )
            _require_deadline(deadline, stage="before external dispatch authority completion")
            raw = self._external_dispatch_authority.complete(
                canonical_model_json_bytes(request),
                deadline=deadline,
            )
            if type(raw) is not bytes or not 0 < len(raw) <= SOURCE_BROKER_V2_MAX_WIRE_BYTES:
                raise SourceBrokerTransportError(
                    "external dispatch authority completion is invalid"
                )
            response = self._parse_model(
                ExternalDispatchAuthorityResponse,
                raw,
                label="external dispatch authority completion",
            )
            if (
                response.operation != "complete"
                or response.status != "found"
                or response.operation_id != operation_id
                or response.request_binding_hash != reservation.request_binding_hash
                or response.authority_generation != reservation.authority_generation
                or response.authority_fence != reservation.authority_fence
                or response.result_hash != result_hash
                or response.result_bytes() != result
            ):
                raise SourceBrokerTransportError(
                    "external dispatch authority completion binding is invalid"
                )
        except Exception as exc:
            self._emit_authority_error(
                authority_operation="complete",
                phase=phase,
                operation_id=operation_id,
                error=exc,
            )
            if isinstance(exc, SourceBrokerV2TransportDeadlineError):
                raise SourceBrokerV2TransportDeadlineError(
                    "V2 source broker deadline expired during external authority complete"
                ) from exc
            raise SourceBrokerTransportError(_UNKNOWN_ERROR) from exc

    def _begin_invocation(
        self,
        *,
        operation_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float | None,
    ) -> bytes | None:
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_operation(connection, operation_id)
                if row is None:
                    raise SourceBrokerTransportError("source operation was not claimed")
                self._validate_operation_request(
                    row,
                    phase,
                    operation_request_hash,
                    saga_id=claim_receipt.saga_id,
                )
                self._validate_claim_receipt_binding(row, claim_receipt)
                status = str(row["status"])
                if status in _TERMINAL_STATUSES:
                    result = self._row_result(row)
                    connection.commit()
                    return result
                if status == _RECONCILE_STATUS:
                    raise SourceBrokerTransportError(_UNKNOWN_ERROR)
                if status == _INVOKING_STATUS:
                    if row["invocation_owner_token"] != self._process_owner_token:
                        self._mark_reconcile(
                            connection,
                            operation_id,
                            "provider invocation owner changed before duplicate wait",
                        )
                        connection.commit()
                        raise SourceBrokerTransportError(_UNKNOWN_ERROR)
                    connection.commit()
                    return self._wait_for_terminal(operation_id, deadline=deadline)
                if status != _ACTIVE_STATUS:
                    raise SourceBrokerTransportError("source operation ledger status is invalid")
                connection.execute(
                    "UPDATE source_broker_v2_provider_operation SET "
                    "status = ?, provider_started_at = NULL, invocation_owner_token = ?, "
                    "invocation_owner_pid = ?, updated_at = ? "
                    "WHERE operation_id = ? AND status = ?",
                    (
                        _INVOKING_STATUS,
                        self._process_owner_token,
                        self._process_owner_pid,
                        _now_text(self._now()),
                        operation_id,
                        _ACTIVE_STATUS,
                    ),
                )
                connection.commit()
                return None
            except BaseException:
                connection.rollback()
                raise

    def _wait_for_terminal(self, operation_id: str, *, deadline: float | None) -> bytes:
        busy_deadline = time.monotonic() + (self._busy_timeout_ms / 1000)
        while True:
            self._raise_if_any_reconcile_fence(operation_id, deadline=deadline)
            if deadline is None:
                if time.monotonic() >= busy_deadline:
                    raise SourceBrokerTransportError("source provider operation is still in flight")
            else:
                _require_deadline(deadline, stage="while waiting for duplicate provider result")
            with self._connect(deadline=deadline) as connection:
                row = self._read_operation(connection, operation_id)
                if row is None:
                    raise SourceBrokerTransportError("source operation disappeared")
                status = str(row["status"])
                if status in _TERMINAL_STATUSES:
                    return self._row_result(row)
                if status == _RECONCILE_STATUS:
                    raise SourceBrokerTransportError(_UNKNOWN_ERROR)
            if deadline is None:
                sleep_seconds = min(0.01, max(0.0, busy_deadline - time.monotonic()))
            else:
                remaining = _remaining(
                    deadline,
                    stage="before duplicate provider wait sleep",
                )
                sleep_seconds = min(0.01, remaining if remaining is not None else 0.01)
            if sleep_seconds:
                time.sleep(sleep_seconds)

    def _complete_invocation(
        self,
        *,
        operation_id: str,
        status: str,
        result: bytes,
        deadline: float | None,
    ) -> None:
        self._raise_if_any_reconcile_fence(operation_id, deadline=deadline)
        if status not in _TERMINAL_STATUSES:
            raise SourceBrokerTransportError("source provider terminal status is invalid")
        result_hash = _canonical_hash(result, label="source operation result")
        _require_deadline(deadline, stage="before provider terminal persistence")
        with self._connect(deadline=deadline) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                updated = connection.execute(
                    "UPDATE source_broker_v2_provider_operation SET "
                    "status = ?, result_json = ?, result_hash = ?, terminal_at = ?, "
                    "updated_at = ? WHERE operation_id = ? AND status = ? "
                    "AND invocation_owner_token = ?",
                    (
                        status,
                        result.decode("utf-8"),
                        result_hash,
                        _now_text(self._now()),
                        _now_text(self._now()),
                        operation_id,
                        _INVOKING_STATUS,
                        self._process_owner_token,
                    ),
                ).rowcount
                if updated != 1:
                    raise SourceBrokerTransportError("source operation terminal update failed")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _replay_response(
        self,
        request: SourceBrokerV2ReplayRequest,
        *,
        deadline: float | None,
    ) -> SourceBrokerV2ReplayResponse:
        result: bytes | None = None
        status = SourceBrokerV2ReplayStatus.ABSENT
        process_fence = self._reconcile_fence(request.operation_id)
        if process_fence is not None:
            self._validate_reconcile_fence_operation(
                process_fence,
                phase=request.phase,
                operation_request_hash=request.operation_request_hash,
                saga_id=request.saga_id,
            )
        authority_request: ExternalDispatchReserveRequest | None = None
        provider_started = False
        same_process_invocation = False
        with self._connect(deadline=deadline) as connection:
            row = self._read_operation(connection, request.operation_id)
            if row is not None:
                self._validate_operation_request(
                    row,
                    request.phase,
                    request.operation_request_hash,
                    saga_id=request.saga_id,
                )
                row_status = str(row["status"])
                if row_status in _TERMINAL_STATUSES:
                    try:
                        result = self._row_result(row)
                        status = SourceBrokerV2ReplayStatus.FOUND
                    except SourceBrokerTransportError:
                        provider_started = row["provider_started_at"] is not None
                        authority_request = self._external_authority_request_from_row(row)
                else:
                    provider_started = row["provider_started_at"] is not None
                    same_process_invocation = (
                        row_status == _INVOKING_STATUS
                        and row["invocation_owner_token"] == self._process_owner_token
                    )
                    needs_lookup = (
                        process_fence is not None
                        or provider_started
                        or row_status in {_INVOKING_STATUS, _RECONCILE_STATUS}
                    )
                    if needs_lookup:
                        authority_request = self._external_authority_request_from_row(row)
        if authority_request is not None:
            try:
                observed = self._lookup_external_authority(authority_request, deadline=deadline)
            except Exception:
                observed = None
            if observed is not None and observed.status == "found":
                result = self._repair_external_terminal(
                    operation_id=request.operation_id,
                    saga_id=request.saga_id,
                    phase=request.phase,
                    operation_request_hash=request.operation_request_hash,
                    response=observed,
                    deadline=deadline,
                )
                status = SourceBrokerV2ReplayStatus.FOUND
            elif observed is not None and observed.status == "absent" and not provider_started:
                self._reset_unstarted_invocation_for_reserve(
                    operation_id=request.operation_id,
                    deadline=deadline,
                )
                status = SourceBrokerV2ReplayStatus.ABSENT
            else:
                status = SourceBrokerV2ReplayStatus.UNKNOWN
                if observed is not None and (
                    not same_process_invocation or process_fence is not None
                ):
                    with suppress(sqlite3.Error):
                        self._persist_external_reconcile(
                            operation_id=request.operation_id,
                            phase=request.phase,
                            observed=observed,
                            reason=(
                                "external authority is UNKNOWN"
                                if observed.status == "unknown"
                                else "external authority is ABSENT after provider start"
                            ),
                            deadline=deadline,
                        )
        _require_deadline(deadline, stage="before replay response signing")
        return self._signed_replay_response(
            request=request,
            status=status,
            result=result,
            deadline=deadline,
        )

    @staticmethod
    def _external_authority_request_from_row(
        row: sqlite3.Row,
    ) -> ExternalDispatchReserveRequest:
        return ExternalDispatchReserveRequest(
            operation_id=str(row["operation_id"]),
            saga_id=str(row["saga_id"]),
            phase=SourceBrokerV2OutboxPhase(str(row["phase"])),
            operation_request_hash=str(row["operation_request_hash"]),
            claim_binding_hash=str(row["claim_binding_hash"]),
            claim_generation=int(row["claim_generation"]),
            scheduler_fencing_token=int(row["scheduler_fencing_token"]),
            executor_owner_token_hash=str(row["executor_owner_token_hash"]),
            executor_generation=int(row["executor_generation"]),
            max_external_deadline=_row_datetime(row, "max_external_deadline"),
            not_before_takeover_at=_row_datetime(row, "not_before_takeover_at"),
        )

    def _signed_claim_response(
        self,
        *,
        request: SourceBrokerV2ClaimOnceRequest,
        status: SourceBrokerV2ClaimStatus,
        result: bytes | None,
        deadline: float | None,
    ) -> SourceBrokerV2ClaimOnceResponse:
        response = SourceBrokerV2ClaimOnceResponse(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            phase=request.phase,
            request_hash=request.request_hash,
            operation_request_hash=request.operation_request_hash,
            challenge=request.challenge,
            claim_binding_hash=request.claim_binding_hash,
            claim_generation=request.claim_generation,
            scheduler_fencing_token=request.scheduler_fencing_token,
            executor_owner_token_hash=request.executor_owner_token_hash,
            executor_generation=request.executor_generation,
            max_external_deadline=request.max_external_deadline,
            not_before_takeover_at=request.not_before_takeover_at,
            authority_id=self._signer.authority_id,
            key_id=self._signer.key_id,
            observed_at=self._now(),
            status=status,
            result=result,
            result_hash=None if result is None else _canonical_hash(result, label="claim result"),
            signature="unsigned",
        )
        return response.model_copy(
            update={"signature": self._signer.sign(response.signing_bytes(), deadline=deadline)},
        )

    def _signed_replay_response(
        self,
        *,
        request: SourceBrokerV2ReplayRequest,
        status: SourceBrokerV2ReplayStatus,
        result: bytes | None,
        deadline: float | None,
    ) -> SourceBrokerV2ReplayResponse:
        response = SourceBrokerV2ReplayResponse(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            phase=request.phase,
            request_hash=request.request_hash,
            challenge=request.challenge,
            status=status,
            result=result,
            result_hash=None if result is None else _canonical_hash(result, label="replay result"),
            authority_id=self._signer.authority_id,
            key_id=self._signer.key_id,
            signature="unsigned",
        )
        return response.model_copy(
            update={"signature": self._signer.sign(response.signing_bytes(), deadline=deadline)},
        )

    def _insert_claim(
        self,
        connection: sqlite3.Connection,
        request: SourceBrokerV2ClaimOnceRequest,
        *,
        status: str,
    ) -> None:
        observed = _now_text(self._now())
        connection.execute(
            "INSERT INTO source_broker_v2_provider_operation("
            "operation_id, saga_id, phase, operation_request_hash, claim_binding_hash, "
            "claim_generation, scheduler_fencing_token, executor_owner_token_hash, "
            "executor_generation, max_external_deadline, not_before_takeover_at, status, "
            "created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                request.operation_id,
                request.saga_id,
                request.phase.value,
                request.operation_request_hash,
                request.claim_binding_hash,
                request.claim_generation,
                request.scheduler_fencing_token,
                request.executor_owner_token_hash,
                request.executor_generation,
                _now_text(request.max_external_deadline),
                _now_text(request.not_before_takeover_at),
                status,
                observed,
                observed,
            ),
        )

    def _read_operation(
        self,
        connection: sqlite3.Connection,
        operation_id: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM source_broker_v2_provider_operation WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()

    def _mark_unknown_for_operation(
        self,
        operation_id: str,
        reason: str,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float | None,
        error: BaseException | None = None,
    ) -> None:
        self._emit_security_event(
            phase=phase,
            operation_id=operation_id,
            error=error,
            category="reconcile",
            reconcile=True,
        )
        fence = self._remember_reconcile_fence(
            operation_id=operation_id,
            phase=phase,
            operation_request_hash=operation_request_hash,
            claim_receipt=claim_receipt,
            reason=reason,
        )
        self._persist_reconcile_fence_if_possible(fence, deadline=deadline)

    def _mark_reconcile(
        self,
        connection: sqlite3.Connection,
        operation_id: str,
        reason: str,
    ) -> int:
        return connection.execute(
            "UPDATE source_broker_v2_provider_operation SET status = ?, "
            "unknown_reason = COALESCE(unknown_reason, ?), updated_at = ? "
            "WHERE operation_id = ? AND status NOT IN ('success', 'failure')",
            (_RECONCILE_STATUS, _safe_error(reason), _now_text(self._now()), operation_id),
        ).rowcount

    def _remember_reconcile_fence(
        self,
        *,
        operation_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        reason: str,
    ) -> _ReconcileFence:
        fence = _ReconcileFence(
            saga_id=claim_receipt.saga_id,
            operation_id=operation_id,
            phase=phase,
            operation_request_hash=operation_request_hash,
            claim_binding_hash=claim_receipt.claim_binding_hash,
            claim_generation=claim_receipt.claim_generation,
            scheduler_fencing_token=claim_receipt.scheduler_fencing_token,
            executor_owner_token_hash=claim_receipt.executor_owner_token_hash,
            executor_generation=claim_receipt.executor_generation,
            max_external_deadline=claim_receipt.max_external_deadline.astimezone(UTC),
            not_before_takeover_at=claim_receipt.not_before_takeover_at.astimezone(UTC),
            reason=reason,
        )
        with self._reconcile_fences_lock:
            existing = self._reconcile_fences.setdefault(operation_id, fence)
        return existing

    def _reconcile_fence(self, operation_id: str) -> _ReconcileFence | None:
        with self._reconcile_fences_lock:
            return self._reconcile_fences.get(operation_id)

    def _clear_reconcile_fence(self, operation_id: str) -> None:
        with self._reconcile_fences_lock:
            self._reconcile_fences.pop(operation_id, None)

    def _has_reconcile_fence_for_claim(
        self,
        request: SourceBrokerV2ClaimOnceRequest,
        *,
        deadline: float | None,
    ) -> bool:
        fence = self._reconcile_fence(request.operation_id)
        if fence is None:
            return False
        self._validate_reconcile_fence_claim(fence, request)
        self._persist_reconcile_fence_if_possible(fence, deadline=deadline)
        return True

    def _reconcile_fence_for_replay(
        self,
        request: SourceBrokerV2ReplayRequest,
        *,
        deadline: float | None,
    ) -> _ReconcileFence | None:
        fence = self._reconcile_fence(request.operation_id)
        if fence is None:
            return None
        self._validate_reconcile_fence_operation(
            fence,
            phase=request.phase,
            operation_request_hash=request.operation_request_hash,
            saga_id=request.saga_id,
        )
        self._persist_reconcile_fence_if_possible(fence, deadline=deadline)
        return fence

    def _raise_if_reconcile_fenced(
        self,
        *,
        operation_id: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        claim_receipt: SourceBrokerV2ClaimOnceResponse,
        deadline: float | None,
    ) -> None:
        fence = self._reconcile_fence(operation_id)
        if fence is None:
            return
        self._validate_reconcile_fence_operation(
            fence,
            phase=phase,
            operation_request_hash=operation_request_hash,
            saga_id=claim_receipt.saga_id,
        )
        self._validate_reconcile_fence_claim_receipt(fence, claim_receipt)
        self._persist_reconcile_fence_if_possible(fence, deadline=deadline)
        raise SourceBrokerTransportError(_UNKNOWN_ERROR)

    def _raise_if_any_reconcile_fence(self, operation_id: str, *, deadline: float | None) -> None:
        fence = self._reconcile_fence(operation_id)
        if fence is None:
            return
        self._persist_reconcile_fence_if_possible(fence, deadline=deadline)
        raise SourceBrokerTransportError(_UNKNOWN_ERROR)

    def _persist_reconcile_fence_if_possible(
        self,
        fence: _ReconcileFence,
        *,
        deadline: float | None,
    ) -> bool:
        sqlite_timeout = _RECONCILE_FENCE_SQLITE_TIMEOUT_SECONDS
        if deadline is not None:
            try:
                remaining = _remaining(deadline, stage="before reconcile fence persistence")
            except SourceBrokerV2TransportDeadlineError:
                return False
            if remaining < _RECONCILE_FENCE_SQLITE_TIMEOUT_SECONDS:
                return False
            sqlite_timeout = min(_RECONCILE_FENCE_SQLITE_TIMEOUT_SECONDS, remaining)
        try:
            with self._connect(
                deadline=deadline, sqlite_timeout_seconds=sqlite_timeout
            ) as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._mark_reconcile(connection, fence.operation_id, fence.reason)
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
        except (sqlite3.Error, SourceBrokerV2TransportDeadlineError):
            return False
        self._clear_reconcile_fence(fence.operation_id)
        return True

    def _validate_reconcile_fence_claim(
        self,
        fence: _ReconcileFence,
        request: SourceBrokerV2ClaimOnceRequest,
    ) -> None:
        if (
            fence.saga_id != request.saga_id
            or fence.phase != request.phase
            or fence.operation_request_hash != request.operation_request_hash
            or fence.claim_binding_hash != request.claim_binding_hash
            or fence.claim_generation != request.claim_generation
            or fence.scheduler_fencing_token != request.scheduler_fencing_token
            or fence.executor_owner_token_hash != request.executor_owner_token_hash
            or fence.executor_generation != request.executor_generation
            or _now_text(fence.max_external_deadline) != _now_text(request.max_external_deadline)
            or _now_text(fence.not_before_takeover_at) != _now_text(request.not_before_takeover_at)
        ):
            raise SourceBrokerTransportError("source operation claim binding conflicts")

    def _validate_reconcile_fence_claim_receipt(
        self,
        fence: _ReconcileFence,
        receipt: SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        if (
            fence.saga_id != receipt.saga_id
            or fence.phase != receipt.phase
            or fence.operation_request_hash != receipt.operation_request_hash
            or fence.claim_binding_hash != receipt.claim_binding_hash
            or fence.claim_generation != receipt.claim_generation
            or fence.scheduler_fencing_token != receipt.scheduler_fencing_token
            or fence.executor_owner_token_hash != receipt.executor_owner_token_hash
            or fence.executor_generation != receipt.executor_generation
            or _now_text(fence.max_external_deadline) != _now_text(receipt.max_external_deadline)
            or _now_text(fence.not_before_takeover_at) != _now_text(receipt.not_before_takeover_at)
        ):
            raise SourceBrokerTransportError("source operation claim binding conflicts")

    def _validate_reconcile_fence_operation(
        self,
        fence: _ReconcileFence,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        *,
        saga_id: str,
    ) -> None:
        if (
            fence.saga_id != saga_id
            or fence.phase != phase
            or fence.operation_request_hash != operation_request_hash
        ):
            raise SourceBrokerTransportError("source operation request binding conflicts")

    def _validate_claim_binding(
        self,
        row: sqlite3.Row,
        request: SourceBrokerV2ClaimOnceRequest,
    ) -> None:
        if (
            row["saga_id"] != request.saga_id
            or row["phase"] != request.phase.value
            or row["operation_request_hash"] != request.operation_request_hash
            or row["claim_binding_hash"] != request.claim_binding_hash
            or int(row["claim_generation"]) != request.claim_generation
            or int(row["scheduler_fencing_token"]) != request.scheduler_fencing_token
            or row["max_external_deadline"] != _now_text(request.max_external_deadline)
            or row["not_before_takeover_at"] != _now_text(request.not_before_takeover_at)
        ):
            raise SourceBrokerTransportError("source operation claim binding conflicts")

    def _validate_claim_receipt_binding(
        self,
        row: sqlite3.Row,
        receipt: SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        if (
            row["saga_id"] != receipt.saga_id
            or row["phase"] != receipt.phase.value
            or row["operation_request_hash"] != receipt.operation_request_hash
            or row["claim_binding_hash"] != receipt.claim_binding_hash
            or int(row["claim_generation"]) != receipt.claim_generation
            or int(row["scheduler_fencing_token"]) != receipt.scheduler_fencing_token
            or row["executor_owner_token_hash"] != receipt.executor_owner_token_hash
            or int(row["executor_generation"]) != receipt.executor_generation
            or row["max_external_deadline"] != _now_text(receipt.max_external_deadline)
            or row["not_before_takeover_at"] != _now_text(receipt.not_before_takeover_at)
        ):
            raise SourceBrokerTransportError("source operation claim binding conflicts")

    def _validate_operation_request(
        self,
        row: sqlite3.Row,
        phase: SourceBrokerV2OutboxPhase,
        operation_request_hash: str,
        *,
        saga_id: str | None = None,
    ) -> None:
        if (
            row["phase"] != phase.value
            or row["operation_request_hash"] != operation_request_hash
            or (saga_id is not None and row["saga_id"] != saga_id)
        ):
            raise SourceBrokerTransportError("source operation request binding conflicts")

    def _row_result(self, row: sqlite3.Row) -> bytes:
        result = row["result_json"]
        result_hash = row["result_hash"]
        if type(result) is not str or type(result_hash) is not str:
            raise SourceBrokerTransportError("source operation result is missing")
        raw = result.encode("utf-8")
        if result_hash != _canonical_hash(raw, label="stored source operation result"):
            raise SourceBrokerTransportError("source operation result hash conflicts")
        return raw

    def _parse_model(self, model: type[_ModelT], payload: bytes, *, label: str) -> _ModelT:
        try:
            return strict_model_validate_canonical_json(model, payload)
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerTransportError(
                f"V2 source broker {label} payload is malformed or noncanonical: {exc}"
            ) from exc

    def _now(self) -> datetime:
        observed = self._clock()
        if observed.tzinfo is None or observed.utcoffset() is None:
            raise SourceBrokerTransportError("source provider clock is not timezone-aware")
        return observed.astimezone(UTC)


_ModelT = (
    SourceBrokerV2ClaimOnceRequest
    | SourceBrokerV2DispatchEnvelope
    | SourceBrokerV2FinalizeEnvelope
    | SourceBrokerV2ReplayRequest
)
_ProviderResultT = TypeVar("_ProviderResultT")
_DISPATCH_SUCCESS = SourceBrokerV2DispatchOutcome.SUCCESS.value.lower()


def _current_process_owner_token() -> str:
    global _PROCESS_EPOCH_PID, _PROCESS_EPOCH_TOKEN
    pid = os.getpid()
    if pid != _PROCESS_EPOCH_PID:
        _PROCESS_EPOCH_PID = pid
        _PROCESS_EPOCH_TOKEN = uuid4().hex
    return f"{pid}:{_PROCESS_EPOCH_TOKEN}"


def _ensure_sqlite_column(
    connection: sqlite3.Connection,
    *,
    table: str,
    column: str,
    definition: str,
) -> None:
    columns = {
        str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in columns:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _claim_request_from_receipt(
    receipt: SourceBrokerV2ClaimOnceResponse,
) -> SourceBrokerV2ClaimOnceRequest:
    return SourceBrokerV2ClaimOnceRequest(
        saga_id=receipt.saga_id,
        operation_id=receipt.operation_id,
        phase=receipt.phase,
        operation_request_hash=receipt.operation_request_hash,
        challenge=receipt.challenge,
        claim_binding_hash=receipt.claim_binding_hash,
        claim_generation=receipt.claim_generation,
        scheduler_fencing_token=receipt.scheduler_fencing_token,
        executor_owner_token_hash=receipt.executor_owner_token_hash,
        executor_generation=receipt.executor_generation,
        max_external_deadline=receipt.max_external_deadline,
        not_before_takeover_at=receipt.not_before_takeover_at,
    )


def _canonical_hash(payload: bytes, *, label: str) -> str:
    try:
        return canonical_sha256(strict_canonical_json_loads(payload))
    except (StrictJsonError, TypeError, ValueError) as exc:
        raise SourceBrokerTransportError(f"{label} is not canonical JSON") from exc


def _require_deadline(deadline: float | None, *, stage: str) -> None:
    _remaining(deadline, stage=stage)


def _remaining(deadline: float | None, *, stage: str) -> float | None:
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SourceBrokerV2TransportDeadlineError(
            f"V2 source broker server deadline expired {stage}"
        )
    return remaining


def _row_datetime(row: sqlite3.Row, column: str) -> datetime:
    raw = row[column]
    if type(raw) is not str:
        raise SourceBrokerTransportError("source operation timestamp is invalid")
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise SourceBrokerTransportError("source operation timestamp is invalid") from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise SourceBrokerTransportError("source operation timestamp is invalid")
    return value.astimezone(UTC)


def _safe_error(error: str) -> str:
    clean = " ".join(str(error).split())
    if not clean:
        clean = "source provider operation failed"
    return clean[:256]


def _now_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SourceBrokerTransportError("source provider timestamp is not timezone-aware")
    return value.astimezone(UTC).isoformat()


def _openssl_binary() -> str:
    for candidate in ("/opt/homebrew/bin/openssl", "/usr/bin/openssl", shutil.which("openssl")):
        if candidate and Path(candidate).is_file():
            return candidate
    raise ValueError("openssl is required for source authority signing")


def _external_authority_public_key_fingerprint(public_key: bytes) -> str:
    try:
        checked = subprocess.run(
            (_openssl_binary(), "pkey", "-pubin", "-pubcheck", "-text_pub", "-noout"),
            input=public_key,
            check=False,
            capture_output=True,
            timeout=5,
        )
        if checked.returncode != 0 or b"ED25519" not in checked.stdout.upper():
            raise ValueError("external authority public key is not Ed25519")
        encoded = subprocess.run(
            (_openssl_binary(), "pkey", "-pubin", "-outform", "DER"),
            input=public_key,
            check=False,
            capture_output=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise ValueError("external authority public key is unusable") from exc
    if encoded.returncode != 0 or not encoded.stdout:
        raise ValueError("external authority public key fingerprint is unavailable")
    return hashlib.sha256(encoded.stdout).hexdigest()


def _verify_external_authority_signature(
    *,
    public_key: bytes,
    signing_bytes: bytes,
    signature: str,
    deadline: float,
) -> bool:
    try:
        decoded = base64.b64decode(signature, validate=True)
    except (TypeError, ValueError):
        return False
    if len(decoded) != _ED25519_SIGNATURE_BYTES:
        return False
    payload = source_authority_signature_payload(signing_bytes)
    try:
        with tempfile.TemporaryDirectory(prefix="rquant-external-authority-") as directory_name:
            root = Path(directory_name)
            root.chmod(0o700)
            public_path = root / "public.pem"
            payload_path = root / "payload.bin"
            signature_path = root / "signature.bin"
            public_path.write_bytes(public_key)
            payload_path.write_bytes(payload)
            signature_path.write_bytes(decoded)
            for path in (public_path, payload_path, signature_path):
                path.chmod(0o600)
            completed = subprocess.run(
                (
                    _openssl_binary(),
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-inkey",
                    str(public_path),
                    "-sigfile",
                    str(signature_path),
                    "-rawin",
                    "-in",
                    str(payload_path),
                ),
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=_remaining(deadline, stage="before external signature verification"),
            )
    except subprocess.TimeoutExpired as exc:
        raise SourceBrokerV2TransportDeadlineError(
            "external authority signature verification exceeded the total deadline"
        ) from exc
    except (OSError, ValueError):
        return False
    return completed.returncode == 0


def _external_kernel_peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
    require_linux_source_broker_transport()
    try:
        raw = connection.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_PEERCRED,
            struct.calcsize("3i"),
        )
    except OSError as exc:
        raise SourceBrokerTransportError(
            "external authority peer credentials are unavailable"
        ) from exc
    if type(raw) is not bytes or len(raw) != struct.calcsize("3i"):
        raise SourceBrokerTransportError("external authority peer credentials are malformed")
    pid, uid, gid = struct.unpack("3i", raw)
    if pid <= 0 or uid < 0 or gid < 0:
        raise SourceBrokerTransportError("external authority peer credentials are invalid")
    return pid, uid, gid
