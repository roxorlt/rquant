"""Strict Unix wire boundary for role-local SourceBroker v2 authorities."""

from __future__ import annotations

import hashlib
import math
import os
import socket
import stat
import struct
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal, Self, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from rquant.current_claim_authority import (
    CurrentClaimAuthorityPreflight,
    PersistentCurrentClaimAuthority,
)
from rquant.lab_shard_protocol import LabShardClaimV2
from rquant.replay_lineage_authority import (
    PersistentReplayLineageAuthority,
    ReplayLineageAuthorityPreflight,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.source_broker import ReplayLineageCheckpointReceipt
from rquant.source_broker_protocol import (
    PeerCredentialsPolicy,
    ServerCredentialsPolicy,
    SocketEndpointIdentity,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
    validate_socket_endpoint,
    validate_socket_parent,
    verify_connected_server_authority,
)
from rquant.source_broker_v2_runtime import (
    SourceBrokerV2ProcessIdentity,
    SourceBrokerV2ProcessRole,
)
from rquant.source_operation_contracts import (
    CurrentClaimConsumptionBindingV2,
    CurrentClaimConsumptionV2,
    CurrentClaimPlanIssueV2,
)
from rquant.source_quota_authority import (
    SourceQuotaAuthorityResult,
    SourceQuotaCallOutcome,
    SourceQuotaParentAuthority,
)
from rquant.source_quota_broker_adapter import SourceQuotaParentBindingV2
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    strict_model_validate_canonical_json,
)

SOURCE_BROKER_V2_AUTHORITY_MAX_FRAME_BYTES = 512 * 1024
_HASH_PATTERN = r"^[0-9a-f]{64}$"
ModelT = TypeVar("ModelT", bound=BaseModel)
_AUTHORITY_ROLES = frozenset(
    {
        SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY,
        SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY,
        SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY,
    }
)


class SourceBrokerV2AuthorityServiceError(RuntimeError):
    """A role-local authority wire or process boundary is untrusted."""


class SourceBrokerV2AuthorityRemoteError(SourceBrokerV2AuthorityServiceError):
    """The authenticated role-local authority rejected a request."""


class SourceBrokerV2AuthorityOperation(StrEnum):
    PREFLIGHT = "preflight"
    CURRENT_REPLACE = "current_replace"
    CURRENT_ISSUE = "current_issue"
    CURRENT_VERIFY = "current_verify"
    QUOTA_RESERVE = "quota_reserve"
    QUOTA_RECORD_INTENT = "quota_record_intent"
    QUOTA_AUTHORIZE_DISPATCH = "quota_authorize_dispatch"
    QUOTA_FINALIZE = "quota_finalize"
    QUOTA_UNKNOWN_BEFORE_DISPATCH = "quota_unknown_before_dispatch"
    QUOTA_RELEASE_UNUSED = "quota_release_unused"
    REPLAY_ADVANCE = "replay_advance"
    REPLAY_VERIFY = "replay_verify"


_OPERATIONS_BY_ROLE = {
    SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY: frozenset(
        {
            SourceBrokerV2AuthorityOperation.PREFLIGHT,
            SourceBrokerV2AuthorityOperation.CURRENT_REPLACE,
            SourceBrokerV2AuthorityOperation.CURRENT_ISSUE,
            SourceBrokerV2AuthorityOperation.CURRENT_VERIFY,
        }
    ),
    SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY: frozenset(
        {
            SourceBrokerV2AuthorityOperation.PREFLIGHT,
            SourceBrokerV2AuthorityOperation.QUOTA_RESERVE,
            SourceBrokerV2AuthorityOperation.QUOTA_RECORD_INTENT,
            SourceBrokerV2AuthorityOperation.QUOTA_AUTHORIZE_DISPATCH,
            SourceBrokerV2AuthorityOperation.QUOTA_FINALIZE,
            SourceBrokerV2AuthorityOperation.QUOTA_UNKNOWN_BEFORE_DISPATCH,
            SourceBrokerV2AuthorityOperation.QUOTA_RELEASE_UNUSED,
        }
    ),
    SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY: frozenset(
        {
            SourceBrokerV2AuthorityOperation.PREFLIGHT,
            SourceBrokerV2AuthorityOperation.REPLAY_ADVANCE,
            SourceBrokerV2AuthorityOperation.REPLAY_VERIFY,
        }
    ),
}
_AUTHORITY_TYPE_BY_ROLE = {
    SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY: PersistentCurrentClaimAuthority,
    SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY: SourceQuotaParentAuthority,
    SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY: PersistentReplayLineageAuthority,
}


class _StrictAuthorityServiceModel(RuntimeContractModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
        str_strip_whitespace=False,
    )


class SourceBrokerV2AuthorityPreflightPayload(_StrictAuthorityServiceModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-broker-v2-authority-preflight/v1"] = (
        "rquant-source-broker-v2-authority-preflight/v1"
    )


class SourceBrokerV2AuthorityAck(_StrictAuthorityServiceModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-broker-v2-authority-ack/v1"] = (
        "rquant-source-broker-v2-authority-ack/v1"
    )
    accepted: Literal[True] = True


class SourceBrokerV2CurrentReplacePayload(_StrictAuthorityServiceModel):
    claim: LabShardClaimV2


class SourceBrokerV2CurrentIssuePayload(_StrictAuthorityServiceModel):
    issue: CurrentClaimPlanIssueV2
    now: AwareUtcDatetime


class SourceBrokerV2CurrentVerifyPayload(_StrictAuthorityServiceModel):
    binding: CurrentClaimConsumptionBindingV2
    now: AwareUtcDatetime


class SourceBrokerV2QuotaReservePayload(_StrictAuthorityServiceModel):
    operation_id: str = Field(min_length=1, max_length=200)
    binding: SourceQuotaParentBindingV2
    total_cost: int = Field(strict=True, gt=0)
    now: AwareUtcDatetime
    expires_at: AwareUtcDatetime


class SourceBrokerV2QuotaIntentPayload(_StrictAuthorityServiceModel):
    operation_id: str = Field(min_length=1, max_length=200)
    parent_id: str = Field(min_length=1, max_length=200)
    call_id: str = Field(min_length=1, max_length=200)
    cost: int = Field(strict=True, gt=0)
    now: AwareUtcDatetime


class SourceBrokerV2QuotaCallPayload(_StrictAuthorityServiceModel):
    operation_id: str = Field(min_length=1, max_length=200)
    parent_id: str = Field(min_length=1, max_length=200)
    call_id: str = Field(min_length=1, max_length=200)
    now: AwareUtcDatetime


class SourceBrokerV2QuotaFinalizePayload(SourceBrokerV2QuotaCallPayload):
    outcome: SourceQuotaCallOutcome


class SourceBrokerV2QuotaReleasePayload(_StrictAuthorityServiceModel):
    operation_id: str = Field(min_length=1, max_length=200)
    parent_id: str = Field(min_length=1, max_length=200)
    now: AwareUtcDatetime


class SourceBrokerV2ReplayAdvancePayload(_StrictAuthorityServiceModel):
    operation_id: str = Field(pattern=_HASH_PATTERN)
    replay_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    previous_head_hash: str = Field(pattern=_HASH_PATTERN)
    next_head_hash: str = Field(pattern=_HASH_PATTERN)
    sequence: int = Field(strict=True, ge=1)
    claim_binding_hash: str = Field(pattern=_HASH_PATTERN)


class SourceBrokerV2ReplayVerifyPayload(_StrictAuthorityServiceModel):
    replay_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    head_hash: str = Field(pattern=_HASH_PATTERN)
    sequence: int = Field(strict=True, ge=0)
    receipt: ReplayLineageCheckpointReceipt | None


class SourceBrokerV2AuthorityUnixService:
    """Strict role-local request boundary around exactly one authority instance."""

    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        peer_policy: PeerCredentialsPolicy,
        service_identity: SourceBrokerV2ProcessIdentity,
        authority: object,
        timeout_ms: int = 5_000,
    ) -> None:
        if type(endpoint) is not SocketEndpointPolicy:
            raise TypeError("authority service requires an exact socket endpoint policy")
        if type(peer_policy) is not PeerCredentialsPolicy:
            raise TypeError("authority service requires an exact peer credentials policy")
        if type(service_identity) is not SourceBrokerV2ProcessIdentity:
            raise TypeError("authority service requires an exact process identity")
        expected_type = _AUTHORITY_TYPE_BY_ROLE.get(service_identity.role)
        if expected_type is None or type(authority) is not expected_type:
            raise TypeError("authority service requires the exact role authority")
        if endpoint.owner_uid != service_identity.uid:
            raise ValueError("authority endpoint owner conflicts with its role identity")
        if type(timeout_ms) is not int or timeout_ms < 1 or timeout_ms > 60_000:
            raise ValueError("authority service timeout_ms is invalid")
        self._endpoint = endpoint
        self._peer_policy = peer_policy
        self._service_identity = service_identity
        self._authority = authority
        self._timeout_ms = timeout_ms
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._listener: socket.socket | None = None
        self._startup_error: BaseException | None = None
        self._drop_lock = threading.Lock()
        self._drop_next_response = False

    @property
    def endpoint(self) -> SocketEndpointPolicy:
        return self._endpoint

    @property
    def peer_policy(self) -> PeerCredentialsPolicy:
        return self._peer_policy

    @property
    def service_identity(self) -> SourceBrokerV2ProcessIdentity:
        return self._service_identity

    def wait_ready(self, *, timeout: float) -> bool:
        if type(timeout) is not float or timeout <= 0:
            raise ValueError("authority readiness timeout must be a positive float")
        observed = self._ready.wait(timeout)
        if observed and self._startup_error is not None:
            raise SourceBrokerV2AuthorityServiceError(
                "authority service failed before readiness"
            ) from self._startup_error
        return observed

    def drop_next_response_after_effect_for_test(self) -> None:
        with self._drop_lock:
            self._drop_next_response = True

    def serve_forever(self) -> None:
        endpoint_identity: SocketEndpointIdentity | None = None
        try:
            self._require_process_identity()
            validate_socket_parent(self._endpoint)
            self._remove_stale_endpoint()
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._listener = listener
            listener.bind(str(self._endpoint.path))
            os.chown(
                self._endpoint.path,
                self._endpoint.owner_uid,
                self._endpoint.group_gid,
            )
            os.chmod(self._endpoint.path, self._endpoint.mode)
            endpoint_identity = validate_socket_endpoint(self._endpoint)
            listener.listen(16)
            listener.settimeout(0.1)
            self._ready.set()
            while not self._stop.is_set():
                try:
                    connection, _address = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if self._stop.is_set():
                        break
                    raise
                with connection:
                    self._serve_connection(connection)
        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()
            if not self._stop.is_set():
                raise
        finally:
            listener = self._listener
            self._listener = None
            if listener is not None:
                with suppress(OSError):
                    listener.close()
            if endpoint_identity is not None:
                with suppress(OSError, SourceBrokerTransportError):
                    validate_socket_endpoint(
                        self._endpoint,
                        expected_identity=endpoint_identity,
                    )
                    self._endpoint.path.unlink()

    def shutdown(self) -> None:
        self._stop.set()
        listener = self._listener
        if listener is not None:
            with suppress(OSError):
                listener.close()

    def _require_process_identity(self) -> None:
        if os.geteuid() != self._service_identity.uid or os.getegid() != self._service_identity.gid:
            raise SourceBrokerV2AuthorityServiceError(
                "authority service process identity does not match its role"
            )

    def _remove_stale_endpoint(self) -> None:
        try:
            observed = os.lstat(self._endpoint.path)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise SourceBrokerV2AuthorityServiceError(
                "authority socket endpoint is unavailable"
            ) from exc
        if (
            not stat.S_ISSOCK(observed.st_mode)
            or observed.st_uid != self._endpoint.owner_uid
            or observed.st_gid != self._endpoint.group_gid
            or stat.S_IMODE(observed.st_mode) != self._endpoint.mode
        ):
            raise SourceBrokerV2AuthorityServiceError(
                "authority stale socket endpoint is untrusted"
            )
        self._endpoint.path.unlink()

    def _serve_connection(self, connection: socket.socket) -> None:
        deadline = time.monotonic() + self._timeout_ms / 1_000
        try:
            pid, uid, gid = _kernel_peer_credentials(connection)
            if not self._peer_policy.allows(pid=pid, uid=uid, gid=gid):
                return
            request_bytes = _read_frame_before_deadline(connection, deadline=deadline)
            request = decode_authority_request(request_bytes)
        except (OSError, SourceBrokerV2AuthorityServiceError):
            return
        try:
            response: SourceBrokerV2AuthorityWireResponse | SourceBrokerV2AuthorityWireFailure
            response = self.handle_request(request)
        except Exception:
            response = SourceBrokerV2AuthorityWireFailure(
                role=request.role,
                operation=request.operation,
                challenge=request.challenge,
                request_hash=request.request_hash,
                error="authority request rejected",
            )
        with self._drop_lock:
            if self._drop_next_response:
                self._drop_next_response = False
                return
        with suppress(OSError, SourceBrokerV2AuthorityServiceError):
            _write_frame_before_deadline(
                connection,
                encode_authority_response(response),
                deadline=deadline,
            )

    def handle_request(
        self,
        request: SourceBrokerV2AuthorityWireRequest,
    ) -> SourceBrokerV2AuthorityWireResponse:
        if type(request) is not SourceBrokerV2AuthorityWireRequest:
            raise SourceBrokerV2AuthorityServiceError("authority request type is invalid")
        if request.role is not self._service_identity.role:
            raise SourceBrokerV2AuthorityServiceError(
                "authority request role does not match this process"
            )
        try:
            validated = SourceBrokerV2AuthorityWireRequest.model_validate(request, strict=True)
        except (TypeError, ValueError, ValidationError) as exc:
            raise SourceBrokerV2AuthorityServiceError("authority request is invalid") from exc
        result = self._dispatch(validated)
        return SourceBrokerV2AuthorityWireResponse.from_result(
            request=validated,
            result=result,
        )

    def _dispatch(self, request: SourceBrokerV2AuthorityWireRequest) -> BaseModel:
        operation = request.operation
        if operation is SourceBrokerV2AuthorityOperation.PREFLIGHT:
            _decode_payload(SourceBrokerV2AuthorityPreflightPayload, request)
            if type(self._authority) is SourceQuotaParentAuthority:
                self._authority.audit()
                return SourceBrokerV2AuthorityAck()
            return self._authority.preflight()
        if type(self._authority) is PersistentCurrentClaimAuthority:
            return self._dispatch_current(request)
        if type(self._authority) is SourceQuotaParentAuthority:
            return self._dispatch_quota(request)
        if type(self._authority) is PersistentReplayLineageAuthority:
            return self._dispatch_replay(request)
        raise SourceBrokerV2AuthorityServiceError("authority process role changed")

    def _dispatch_current(self, request: SourceBrokerV2AuthorityWireRequest) -> BaseModel:
        operation = request.operation
        if operation is SourceBrokerV2AuthorityOperation.CURRENT_REPLACE:
            payload = _decode_payload(SourceBrokerV2CurrentReplacePayload, request)
            return self._authority.replace_current(payload.claim)
        if operation is SourceBrokerV2AuthorityOperation.CURRENT_ISSUE:
            payload = _decode_payload(SourceBrokerV2CurrentIssuePayload, request)
            return self._authority.issue_plan_once(issue=payload.issue, now=payload.now)
        if operation is SourceBrokerV2AuthorityOperation.CURRENT_VERIFY:
            payload = _decode_payload(SourceBrokerV2CurrentVerifyPayload, request)
            return self._authority.verify_current(binding=payload.binding, now=payload.now)
        raise SourceBrokerV2AuthorityServiceError("current authority operation changed")

    def _dispatch_quota(self, request: SourceBrokerV2AuthorityWireRequest) -> BaseModel:
        operation = request.operation
        if operation is SourceBrokerV2AuthorityOperation.QUOTA_RESERVE:
            payload = _decode_payload(SourceBrokerV2QuotaReservePayload, request)
            binding = payload.binding
            return self._authority.reserve_parent(
                operation_id=payload.operation_id,
                parent_id=binding.parent_id,
                source=binding.source,
                owner=binding.owner,
                total_cost=payload.total_cost,
                now=payload.now,
                expires_at=payload.expires_at,
                claim_binding_hash=binding.claim_binding_hash,
                claim_generation=binding.claim_generation,
                scheduler_fencing_token=binding.scheduler_fencing_token,
            )
        if operation is SourceBrokerV2AuthorityOperation.QUOTA_RECORD_INTENT:
            payload = _decode_payload(SourceBrokerV2QuotaIntentPayload, request)
            return self._authority.record_intent(**payload.model_dump(mode="python"))
        if operation in {
            SourceBrokerV2AuthorityOperation.QUOTA_AUTHORIZE_DISPATCH,
            SourceBrokerV2AuthorityOperation.QUOTA_UNKNOWN_BEFORE_DISPATCH,
        }:
            payload = _decode_payload(SourceBrokerV2QuotaCallPayload, request)
            method = (
                self._authority.authorize_dispatch
                if operation is SourceBrokerV2AuthorityOperation.QUOTA_AUTHORIZE_DISPATCH
                else self._authority.terminalize_unknown_before_dispatch
            )
            return method(**payload.model_dump(mode="python"))
        if operation is SourceBrokerV2AuthorityOperation.QUOTA_FINALIZE:
            payload = _decode_payload(SourceBrokerV2QuotaFinalizePayload, request)
            return self._authority.finalize(**payload.model_dump(mode="python"))
        if operation is SourceBrokerV2AuthorityOperation.QUOTA_RELEASE_UNUSED:
            payload = _decode_payload(SourceBrokerV2QuotaReleasePayload, request)
            return self._authority.release_unused(**payload.model_dump(mode="python"))
        raise SourceBrokerV2AuthorityServiceError("quota authority operation changed")

    def _dispatch_replay(self, request: SourceBrokerV2AuthorityWireRequest) -> BaseModel:
        operation = request.operation
        if operation is SourceBrokerV2AuthorityOperation.REPLAY_ADVANCE:
            payload = _decode_payload(SourceBrokerV2ReplayAdvancePayload, request)
            return self._authority.compare_and_advance(**payload.model_dump(mode="python"))
        if operation is SourceBrokerV2AuthorityOperation.REPLAY_VERIFY:
            payload = _decode_payload(SourceBrokerV2ReplayVerifyPayload, request)
            self._authority.verify_current(**payload.model_dump(mode="python"))
            return SourceBrokerV2AuthorityAck()
        raise SourceBrokerV2AuthorityServiceError("replay authority operation changed")


@dataclass(frozen=True)
class _AuthorityInvocation:
    response: SourceBrokerV2AuthorityWireResponse
    deadline: float


class _SourceBrokerV2AuthorityUnixClient:
    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        server_policy: ServerCredentialsPolicy,
        role: SourceBrokerV2ProcessRole,
        timeout_ms: int,
    ) -> None:
        if type(endpoint) is not SocketEndpointPolicy:
            raise TypeError("authority client requires an exact socket endpoint policy")
        if type(server_policy) is not ServerCredentialsPolicy:
            raise TypeError("authority client requires an exact server credentials policy")
        if role not in _AUTHORITY_ROLES:
            raise ValueError("authority client role is invalid")
        if type(timeout_ms) is not int or timeout_ms < 1 or timeout_ms > 60_000:
            raise ValueError("authority client timeout_ms is invalid")
        if endpoint.owner_uid != server_policy.expected_uid or endpoint.group_gid < 0:
            raise ValueError("authority client endpoint and server identity conflict")
        self._endpoint = endpoint
        self._server_policy = server_policy
        self._role = role
        self._timeout_ms = timeout_ms

    @property
    def endpoint(self) -> SocketEndpointPolicy:
        return self._endpoint

    @property
    def server_policy(self) -> ServerCredentialsPolicy:
        return self._server_policy

    def _invoke(
        self,
        *,
        operation: SourceBrokerV2AuthorityOperation,
        payload: BaseModel,
        deadline: float | None = None,
    ) -> _AuthorityInvocation:
        client_deadline = time.monotonic() + self._timeout_ms / 1_000
        if deadline is None:
            deadline = client_deadline
        else:
            if type(deadline) not in {int, float} or not math.isfinite(deadline):
                raise ValueError("authority request deadline is invalid")
            deadline = min(client_deadline, float(deadline))
        self._require_deadline_remaining(deadline, stage="before request encoding")
        try:
            request = SourceBrokerV2AuthorityWireRequest.from_payload(
                role=self._role,
                operation=operation,
                challenge=os.urandom(32).hex(),
                payload=payload,
            )
            wire_request = encode_authority_request(request)
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after request encoding")
            raise
        self._require_deadline_remaining(deadline, stage="after request encoding")
        try:
            self._require_deadline_remaining(deadline, stage="before endpoint validation")
            endpoint_identity = validate_socket_endpoint(self._endpoint)
            self._require_deadline_remaining(deadline, stage="after endpoint validation")
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                _set_socket_deadline(connection, deadline=deadline)
                connection.connect(str(self._endpoint.path))
                self._require_deadline_remaining(deadline, stage="after connect")
                self._require_deadline_remaining(
                    deadline,
                    stage="before server identity verification",
                )
                pid, uid, gid = _kernel_peer_credentials(connection)
                if not self._server_policy.allows(pid=pid, uid=uid, gid=gid):
                    raise SourceBrokerV2AuthorityServiceError(
                        "authority server process identity changed"
                    )
                verify_connected_server_authority(
                    server_pid=pid,
                    endpoint=self._endpoint,
                    endpoint_identity=endpoint_identity,
                )
                self._require_deadline_remaining(
                    deadline,
                    stage="after server identity verification",
                )
                self._require_deadline_remaining(deadline, stage="before request write")
                _write_frame_before_deadline(
                    connection,
                    wire_request,
                    deadline=deadline,
                )
                self._require_deadline_remaining(deadline, stage="after request write")
                self._require_deadline_remaining(deadline, stage="before response read")
                raw_response = _read_frame_before_deadline(connection, deadline=deadline)
                self._require_deadline_remaining(deadline, stage="after response read")
        except (OSError, SourceBrokerTransportError) as exc:
            try:
                self._require_deadline_remaining(deadline, stage="after transport failure")
            except SourceBrokerV2AuthorityServiceError as deadline_exc:
                raise deadline_exc from exc
            raise SourceBrokerV2AuthorityServiceError(
                "authority Unix service is unavailable or untrusted"
            ) from exc
        self._require_deadline_remaining(deadline, stage="before canonical response decode")
        try:
            response = decode_authority_response(raw_response)
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after canonical response decode")
            raise
        self._require_deadline_remaining(deadline, stage="after canonical response decode")
        self._require_deadline_remaining(deadline, stage="before response binding")
        try:
            _require_response_binding(request=request, response=response)
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after response binding")
            raise
        self._require_deadline_remaining(deadline, stage="after response binding")
        if type(response) is SourceBrokerV2AuthorityWireFailure:
            self._require_deadline_remaining(deadline, stage="before remote failure return")
            raise SourceBrokerV2AuthorityRemoteError(response.error)
        return _AuthorityInvocation(response=response, deadline=deadline)

    def _accept_typed_result(
        self,
        model: type[ModelT],
        invocation: _AuthorityInvocation,
    ) -> ModelT:
        deadline = invocation.deadline
        self._require_deadline_remaining(deadline, stage="before typed result decode")
        try:
            result = decode_authority_result(model, invocation.response)
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after typed result decode")
            raise
        self._require_deadline_remaining(deadline, stage="after typed result decode")
        self._require_deadline_remaining(deadline, stage="before result signature verification")
        try:
            self._verify_typed_result_signature(result)
        except BaseException:
            self._require_deadline_remaining(
                deadline,
                stage="after result signature verification",
            )
            raise
        self._require_deadline_remaining(
            deadline,
            stage="after result signature verification",
        )
        self._require_deadline_remaining(deadline, stage="before final result return")
        return result

    @staticmethod
    def _verify_typed_result_signature(result: BaseModel) -> None:
        signature: object | None = None
        if type(result) is CurrentClaimConsumptionV2:
            signature = result.signed_plan.signature
        elif type(result) is SourceQuotaAuthorityResult:
            signature = result.receipt.signature
        elif type(result) is ReplayLineageCheckpointReceipt:
            signature = result.signature
        if signature is not None and (type(signature) is not str or not signature):
            raise SourceBrokerV2AuthorityServiceError("authority result signature is unavailable")

    @staticmethod
    def _require_deadline_remaining(deadline: float, *, stage: str) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SourceBrokerV2AuthorityServiceError(f"authority request deadline expired {stage}")
        return remaining


class SourceBrokerV2SourceQuotaUnixClient(_SourceBrokerV2AuthorityUnixClient):
    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        server_policy: ServerCredentialsPolicy,
        timeout_ms: int,
    ) -> None:
        super().__init__(
            endpoint=endpoint,
            server_policy=server_policy,
            role=SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY,
            timeout_ms=timeout_ms,
        )

    def preflight(self, *, deadline: float | None = None) -> SourceBrokerV2AuthorityAck:
        return self._accept_typed_result(
            SourceBrokerV2AuthorityAck,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.PREFLIGHT,
                payload=SourceBrokerV2AuthorityPreflightPayload(),
                deadline=deadline,
            ),
        )

    def reserve_parent(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        total_cost: int,
        now: datetime,
        expires_at: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._accept_typed_result(
            SourceQuotaAuthorityResult,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.QUOTA_RESERVE,
                payload=SourceBrokerV2QuotaReservePayload(
                    operation_id=operation_id,
                    binding=binding,
                    total_cost=total_cost,
                    now=now,
                    expires_at=expires_at,
                ),
            ),
        )

    def record_intent(
        self,
        *,
        operation_id: str,
        parent_id: str,
        call_id: str,
        cost: int,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._accept_typed_result(
            SourceQuotaAuthorityResult,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.QUOTA_RECORD_INTENT,
                payload=SourceBrokerV2QuotaIntentPayload(
                    operation_id=operation_id,
                    parent_id=parent_id,
                    call_id=call_id,
                    cost=cost,
                    now=now,
                ),
            ),
        )

    def authorize_dispatch(
        self,
        *,
        operation_id: str,
        parent_id: str,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._quota_call(
            operation=SourceBrokerV2AuthorityOperation.QUOTA_AUTHORIZE_DISPATCH,
            operation_id=operation_id,
            parent_id=parent_id,
            call_id=call_id,
            now=now,
        )

    def terminalize_unknown_before_dispatch(
        self,
        *,
        operation_id: str,
        parent_id: str,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._quota_call(
            operation=SourceBrokerV2AuthorityOperation.QUOTA_UNKNOWN_BEFORE_DISPATCH,
            operation_id=operation_id,
            parent_id=parent_id,
            call_id=call_id,
            now=now,
        )

    def finalize(
        self,
        *,
        operation_id: str,
        parent_id: str,
        call_id: str,
        outcome: SourceQuotaCallOutcome,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._accept_typed_result(
            SourceQuotaAuthorityResult,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.QUOTA_FINALIZE,
                payload=SourceBrokerV2QuotaFinalizePayload(
                    operation_id=operation_id,
                    parent_id=parent_id,
                    call_id=call_id,
                    outcome=outcome,
                    now=now,
                ),
            ),
        )

    def release_unused(
        self,
        *,
        operation_id: str,
        parent_id: str,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._accept_typed_result(
            SourceQuotaAuthorityResult,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.QUOTA_RELEASE_UNUSED,
                payload=SourceBrokerV2QuotaReleasePayload(
                    operation_id=operation_id,
                    parent_id=parent_id,
                    now=now,
                ),
            ),
        )

    def _quota_call(
        self,
        *,
        operation: SourceBrokerV2AuthorityOperation,
        operation_id: str,
        parent_id: str,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._accept_typed_result(
            SourceQuotaAuthorityResult,
            self._invoke(
                operation=operation,
                payload=SourceBrokerV2QuotaCallPayload(
                    operation_id=operation_id,
                    parent_id=parent_id,
                    call_id=call_id,
                    now=now,
                ),
            ),
        )


class SourceBrokerV2CurrentClaimUnixClient(_SourceBrokerV2AuthorityUnixClient):
    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        server_policy: ServerCredentialsPolicy,
        timeout_ms: int,
    ) -> None:
        super().__init__(
            endpoint=endpoint,
            server_policy=server_policy,
            role=SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY,
            timeout_ms=timeout_ms,
        )

    def preflight(self, *, deadline: float | None = None) -> CurrentClaimAuthorityPreflight:
        return self._accept_typed_result(
            CurrentClaimAuthorityPreflight,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.PREFLIGHT,
                payload=SourceBrokerV2AuthorityPreflightPayload(),
                deadline=deadline,
            ),
        )

    def replace_current(self, claim: LabShardClaimV2) -> LabShardClaimV2:
        return self._accept_typed_result(
            LabShardClaimV2,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.CURRENT_REPLACE,
                payload=SourceBrokerV2CurrentReplacePayload(claim=claim),
            ),
        )

    def issue_plan_once(
        self,
        *,
        issue: CurrentClaimPlanIssueV2,
        now: datetime,
    ) -> CurrentClaimConsumptionV2:
        return self._accept_typed_result(
            CurrentClaimConsumptionV2,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.CURRENT_ISSUE,
                payload=SourceBrokerV2CurrentIssuePayload(issue=issue, now=now),
            ),
        )

    def verify_current(
        self,
        *,
        binding: CurrentClaimConsumptionBindingV2,
        now: datetime,
    ) -> CurrentClaimConsumptionV2:
        return self._accept_typed_result(
            CurrentClaimConsumptionV2,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.CURRENT_VERIFY,
                payload=SourceBrokerV2CurrentVerifyPayload(binding=binding, now=now),
            ),
        )


class SourceBrokerV2ReplayLineageUnixClient(_SourceBrokerV2AuthorityUnixClient):
    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        server_policy: ServerCredentialsPolicy,
        timeout_ms: int,
    ) -> None:
        super().__init__(
            endpoint=endpoint,
            server_policy=server_policy,
            role=SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY,
            timeout_ms=timeout_ms,
        )

    def preflight(self, *, deadline: float | None = None) -> ReplayLineageAuthorityPreflight:
        return self._accept_typed_result(
            ReplayLineageAuthorityPreflight,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.PREFLIGHT,
                payload=SourceBrokerV2AuthorityPreflightPayload(),
                deadline=deadline,
            ),
        )

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        replay_authority_id: str,
        lineage_id: str,
        previous_head_hash: str,
        next_head_hash: str,
        sequence: int,
        claim_binding_hash: str,
    ) -> ReplayLineageCheckpointReceipt:
        return self._accept_typed_result(
            ReplayLineageCheckpointReceipt,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.REPLAY_ADVANCE,
                payload=SourceBrokerV2ReplayAdvancePayload(
                    operation_id=operation_id,
                    replay_authority_id=replay_authority_id,
                    lineage_id=lineage_id,
                    previous_head_hash=previous_head_hash,
                    next_head_hash=next_head_hash,
                    sequence=sequence,
                    claim_binding_hash=claim_binding_hash,
                ),
            ),
        )

    def verify_current(
        self,
        *,
        replay_authority_id: str,
        lineage_id: str,
        head_hash: str,
        sequence: int,
        receipt: ReplayLineageCheckpointReceipt | None,
    ) -> None:
        self._accept_typed_result(
            SourceBrokerV2AuthorityAck,
            self._invoke(
                operation=SourceBrokerV2AuthorityOperation.REPLAY_VERIFY,
                payload=SourceBrokerV2ReplayVerifyPayload(
                    replay_authority_id=replay_authority_id,
                    lineage_id=lineage_id,
                    head_hash=head_hash,
                    sequence=sequence,
                    receipt=receipt,
                ),
            ),
        )


class SourceBrokerV2AuthorityWireRequest(_StrictAuthorityServiceModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-broker-v2-authority-request/v1"] = (
        "rquant-source-broker-v2-authority-request/v1"
    )
    role: SourceBrokerV2ProcessRole
    operation: SourceBrokerV2AuthorityOperation
    challenge: str = Field(pattern=_HASH_PATTERN)
    payload_json: str = Field(min_length=2, max_length=SOURCE_BROKER_V2_AUTHORITY_MAX_FRAME_BYTES)
    payload_hash: str = Field(pattern=_HASH_PATTERN)
    request_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def from_payload(
        cls,
        *,
        role: SourceBrokerV2ProcessRole,
        operation: SourceBrokerV2AuthorityOperation,
        challenge: str,
        payload: BaseModel,
    ) -> SourceBrokerV2AuthorityWireRequest:
        payload_bytes = _canonical_model_bytes(payload)
        payload_json = payload_bytes.decode("utf-8")
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        request_hash = _request_hash(
            role=role,
            operation=operation,
            challenge=challenge,
            payload_hash=payload_hash,
        )
        return cls(
            role=role,
            operation=operation,
            challenge=challenge,
            payload_json=payload_json,
            payload_hash=payload_hash,
            request_hash=request_hash,
        )

    @model_validator(mode="after")
    def validate_binding(self) -> Self:
        if self.role not in _AUTHORITY_ROLES or self.operation not in _OPERATIONS_BY_ROLE.get(
            self.role,
            frozenset(),
        ):
            raise ValueError("authority role and operation binding is invalid")
        try:
            payload = self.payload_json.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("authority payload is not UTF-8") from exc
        if hashlib.sha256(payload).hexdigest() != self.payload_hash:
            raise ValueError("authority payload hash binding changed")
        if self.request_hash != _request_hash(
            role=self.role,
            operation=self.operation,
            challenge=self.challenge,
            payload_hash=self.payload_hash,
        ):
            raise ValueError("authority request hash binding changed")
        return self


class SourceBrokerV2AuthorityWireResponse(_StrictAuthorityServiceModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-broker-v2-authority-response/v1"] = (
        "rquant-source-broker-v2-authority-response/v1"
    )
    ok: Literal[True] = True
    role: SourceBrokerV2ProcessRole
    operation: SourceBrokerV2AuthorityOperation
    challenge: str = Field(pattern=_HASH_PATTERN)
    request_hash: str = Field(pattern=_HASH_PATTERN)
    result_json: str = Field(min_length=2, max_length=SOURCE_BROKER_V2_AUTHORITY_MAX_FRAME_BYTES)
    result_hash: str = Field(pattern=_HASH_PATTERN)

    @classmethod
    def from_result(
        cls,
        *,
        request: SourceBrokerV2AuthorityWireRequest,
        result: BaseModel,
    ) -> SourceBrokerV2AuthorityWireResponse:
        result_bytes = _canonical_model_bytes(result)
        return cls(
            role=request.role,
            operation=request.operation,
            challenge=request.challenge,
            request_hash=request.request_hash,
            result_json=result_bytes.decode("utf-8"),
            result_hash=hashlib.sha256(result_bytes).hexdigest(),
        )

    @model_validator(mode="after")
    def validate_result_hash(self) -> Self:
        if hashlib.sha256(self.result_json.encode("utf-8")).hexdigest() != self.result_hash:
            raise ValueError("authority response result hash binding changed")
        return self


class SourceBrokerV2AuthorityWireFailure(_StrictAuthorityServiceModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-source-broker-v2-authority-failure/v1"] = (
        "rquant-source-broker-v2-authority-failure/v1"
    )
    ok: Literal[False] = False
    role: SourceBrokerV2ProcessRole
    operation: SourceBrokerV2AuthorityOperation
    challenge: str = Field(pattern=_HASH_PATTERN)
    request_hash: str = Field(pattern=_HASH_PATTERN)
    error: str = Field(min_length=1, max_length=256)


def encode_authority_request(request: SourceBrokerV2AuthorityWireRequest) -> bytes:
    try:
        validated = SourceBrokerV2AuthorityWireRequest.model_validate(request, strict=True)
        return _bounded(_canonical_model_bytes(validated), label="request")
    except (TypeError, ValueError, ValidationError) as exc:
        raise SourceBrokerV2AuthorityServiceError(
            "authority request role, operation, or canonical binding is invalid"
        ) from exc


def decode_authority_request(payload: bytes) -> SourceBrokerV2AuthorityWireRequest:
    return _decode_exact(SourceBrokerV2AuthorityWireRequest, payload, label="request")


def encode_authority_response(
    response: SourceBrokerV2AuthorityWireResponse | SourceBrokerV2AuthorityWireFailure,
) -> bytes:
    model = (
        SourceBrokerV2AuthorityWireFailure
        if isinstance(response, SourceBrokerV2AuthorityWireFailure)
        else SourceBrokerV2AuthorityWireResponse
    )
    try:
        validated = model.model_validate(response, strict=True)
        return _bounded(_canonical_model_bytes(validated), label="response")
    except (TypeError, ValueError, ValidationError) as exc:
        raise SourceBrokerV2AuthorityServiceError(
            "authority response canonical binding is invalid"
        ) from exc


def decode_authority_response(
    payload: bytes,
) -> SourceBrokerV2AuthorityWireResponse | SourceBrokerV2AuthorityWireFailure:
    bounded = _bounded(payload, label="response")
    try:
        decoded = strict_model_validate_canonical_json(
            SourceBrokerV2AuthorityWireFailure,
            bounded,
        )
    except (StrictJsonError, ValidationError, ValueError, TypeError):
        return _decode_exact(SourceBrokerV2AuthorityWireResponse, bounded, label="response")
    if encode_authority_response(decoded) != bounded:
        raise SourceBrokerV2AuthorityServiceError("authority response is not canonical")
    return decoded


def decode_authority_result(
    model: type[ModelT],
    response: SourceBrokerV2AuthorityWireResponse,
) -> ModelT:
    try:
        result = strict_model_validate_canonical_json(model, response.result_json)
        if _canonical_model_bytes(result).decode("utf-8") != response.result_json:
            raise ValueError("authority result is not canonical")
        return result
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise SourceBrokerV2AuthorityServiceError("authority result is untrusted") from exc


def _decode_payload(
    model: type[ModelT],
    request: SourceBrokerV2AuthorityWireRequest,
) -> ModelT:
    try:
        decoded = strict_model_validate_canonical_json(model, request.payload_json)
        if _canonical_model_bytes(decoded).decode("utf-8") != request.payload_json:
            raise ValueError("authority payload is not canonical")
        return decoded
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise SourceBrokerV2AuthorityServiceError(
            "authority request payload is not canonical or valid"
        ) from exc


def _decode_exact(
    model: type[ModelT],
    payload: bytes,
    *,
    label: str,
) -> ModelT:
    bounded = _bounded(payload, label=label)
    try:
        decoded = strict_model_validate_canonical_json(model, bounded)
        if _canonical_model_bytes(decoded) != bounded:
            raise ValueError(f"authority {label} is not canonical")
        return decoded
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise SourceBrokerV2AuthorityServiceError(
            f"authority {label} is not canonical or valid"
        ) from exc


def _canonical_model_bytes(model: BaseModel) -> bytes:
    return canonical_json_bytes(model.model_dump(mode="json", round_trip=True))


def _bounded(payload: bytes, *, label: str) -> bytes:
    if (
        type(payload) is not bytes
        or not 0 < len(payload) <= SOURCE_BROKER_V2_AUTHORITY_MAX_FRAME_BYTES
    ):
        raise SourceBrokerV2AuthorityServiceError(f"authority {label} exceeds the wire bound")
    return payload


def _request_hash(
    *,
    role: SourceBrokerV2ProcessRole,
    operation: SourceBrokerV2AuthorityOperation,
    challenge: str,
    payload_hash: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-source-broker-v2-authority-request-binding/v1",
            "role": role,
            "operation": operation,
            "challenge": challenge,
            "payload_hash": payload_hash,
        }
    )


def _kernel_peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
    if not hasattr(socket, "SO_PEERCRED"):
        raise SourceBrokerV2AuthorityServiceError(
            "Linux SO_PEERCRED is required for authority transport"
        )
    try:
        raw = connection.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_PEERCRED,
            struct.calcsize("3i"),
        )
        pid, uid, gid = struct.unpack("3i", raw)
    except (OSError, struct.error) as exc:
        raise SourceBrokerV2AuthorityServiceError(
            "authority peer credentials are unavailable"
        ) from exc
    if pid <= 0 or uid < 0 or gid < 0:
        raise SourceBrokerV2AuthorityServiceError("authority peer credentials are invalid")
    return pid, uid, gid


def _set_socket_deadline(connection: socket.socket, *, deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SourceBrokerV2AuthorityServiceError("authority request deadline expired")
    connection.settimeout(remaining)


def _read_frame_before_deadline(connection: socket.socket, *, deadline: float) -> bytes:
    header = _recv_exact(connection, 4, deadline=deadline)
    size = int.from_bytes(header, "big")
    if size < 1 or size > SOURCE_BROKER_V2_AUTHORITY_MAX_FRAME_BYTES:
        raise SourceBrokerV2AuthorityServiceError("authority frame size is invalid")
    return _recv_exact(connection, size, deadline=deadline)


def _write_frame_before_deadline(
    connection: socket.socket,
    payload: bytes,
    *,
    deadline: float,
) -> None:
    bounded = _bounded(payload, label="frame")
    _set_socket_deadline(connection, deadline=deadline)
    try:
        connection.sendall(len(bounded).to_bytes(4, "big") + bounded)
    except (OSError, TimeoutError) as exc:
        raise SourceBrokerV2AuthorityServiceError(
            "authority frame write failed or timed out"
        ) from exc


def _recv_exact(
    connection: socket.socket,
    size: int,
    *,
    deadline: float,
) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        _set_socket_deadline(connection, deadline=deadline)
        try:
            chunk = connection.recv(remaining)
        except (OSError, TimeoutError) as exc:
            raise SourceBrokerV2AuthorityServiceError(
                "authority frame read failed or timed out"
            ) from exc
        if not chunk:
            raise SourceBrokerV2AuthorityServiceError("authority frame ended early")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _require_response_binding(
    *,
    request: SourceBrokerV2AuthorityWireRequest,
    response: SourceBrokerV2AuthorityWireResponse | SourceBrokerV2AuthorityWireFailure,
) -> None:
    if (
        response.role is not request.role
        or response.operation is not request.operation
        or response.challenge != request.challenge
        or response.request_hash != request.request_hash
    ):
        raise SourceBrokerV2AuthorityServiceError(
            "authority response does not bind the exact request"
        )


__all__ = [
    "SOURCE_BROKER_V2_AUTHORITY_MAX_FRAME_BYTES",
    "SourceBrokerV2AuthorityAck",
    "SourceBrokerV2AuthorityOperation",
    "SourceBrokerV2AuthorityPreflightPayload",
    "SourceBrokerV2AuthorityRemoteError",
    "SourceBrokerV2AuthorityServiceError",
    "SourceBrokerV2AuthorityWireFailure",
    "SourceBrokerV2AuthorityWireRequest",
    "SourceBrokerV2AuthorityWireResponse",
    "SourceBrokerV2AuthorityUnixService",
    "SourceBrokerV2CurrentClaimUnixClient",
    "SourceBrokerV2ReplayLineageUnixClient",
    "SourceBrokerV2SourceQuotaUnixClient",
    "decode_authority_request",
    "decode_authority_response",
    "decode_authority_result",
    "encode_authority_request",
    "encode_authority_response",
]
