"""Durable V2 saga boundary for one external source call.

This module intentionally does not import or modify the legacy ``SourceBroker``
runtime.  V1 combines quota reservation with provider dispatch, whereas V2 owns
those effects as separately journaled stages.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import math
import os
import shutil
import socket
import sqlite3
import struct
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from threading import Event, Thread
from typing import Literal, Protocol
from uuid import uuid4

from pydantic import ConfigDict, Field, ValidationError, model_validator

from rquant.current_claim_authority import PersistentCurrentClaimAuthority
from rquant.replay_lineage_authority import PersistentReplayLineageAuthority
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.source_broker import ReplayLineageCheckpointReceipt
from rquant.source_broker_protocol import (
    MAX_SOURCE_BROKER_FRAME_BYTES,
    ServerCredentialsPolicy,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
    require_linux_source_broker_transport,
    validate_socket_endpoint,
    verify_connected_server_authority,
)
from rquant.source_operation_contracts import (
    CurrentClaimConsumptionV2,
    CurrentClaimPlanIssueV2,
)
from rquant.source_quota_authority import SourceQuotaAuthorityResult, SourceQuotaCallOutcome
from rquant.source_quota_broker_adapter import (
    SourceQuotaBrokerAdapterV2,
    SourceQuotaBrokerPhaseV2,
    SourceQuotaBrokerReceiptV2,
    SourceQuotaParentBindingV2,
    decode_source_quota_broker_receipt_v2,
    encode_source_quota_broker_receipt_v2,
)
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

SOURCE_BROKER_V2_CONTRACT = "rquant-source-broker-saga/v2"
SOURCE_BROKER_V2_MAX_PAYLOAD_BYTES = 256 * 1024
SOURCE_BROKER_V2_MAX_RECEIPT_BYTES = 512 * 1024
SOURCE_BROKER_V2_AUTHORITY_PURPOSE = "rquant-source-authority-receipt"
SOURCE_BROKER_V2_AUTHORITY_NAMESPACE = "rquant-source-authority-receipt/v2"
_ZERO_HASH = "0" * 64
_ED25519_SIGNATURE_BYTES = 64
_PRODUCTION_SAGA_GRAPH_TOKEN = object()


class _StrictV2Model(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceBrokerV2SagaError(RuntimeError):
    """Base failure for the closed V2 source saga."""


class SourceBrokerV2SagaIntegrityError(SourceBrokerV2SagaError):
    """Durable saga evidence is malformed, foreign, or has been modified."""


class SourceBrokerV2SagaConflictError(SourceBrokerV2SagaError):
    """A stale generation or incompatible idempotent request was rejected."""


class SourceBrokerV2SagaUnavailableError(SourceBrokerV2SagaError):
    """A closed external authority or transport is temporarily unavailable."""


class SourceBrokerV2TransportDeadlineError(SourceBrokerTransportError):
    """The single monotonic V2 Unix request budget has been exhausted."""


class SourceBrokerV2SagaReconcileRequiredError(SourceBrokerV2SagaError):
    """Dispatch may have occurred and must be reconciled before compensation."""


class SourceBrokerV2SagaRepairRequiredError(SourceBrokerV2SagaIntegrityError):
    """Local materialization conflicts with an external authority and needs repair."""


class SourceBrokerV2SagaState(StrEnum):
    """Forward-only durable lifecycle for a single source call."""

    CLAIMED = "claimed"
    PARENT_RESERVED = "parent_reserved"
    CALL_INTENT = "call_intent"
    CALL_TERMINALIZED = "call_terminalized"
    DISPATCH_AUTHORIZED = "dispatch_authorized"
    DISPATCH_OUTCOME = "dispatch_outcome"
    SOURCE_FINALIZED = "source_finalized"
    QUOTA_TERMINAL = "quota_terminal"
    PARENT_RELEASED = "parent_released"
    COMPENSATED = "compensated"
    LINEAGE_PUBLISHED = "lineage_published"
    COMPLETE = "complete"
    DISPATCH_UNKNOWN = "dispatch_unknown"
    RECONCILE_REQUIRED = "reconcile_required"


class SourceBrokerV2OutboxPhase(StrEnum):
    CLAIM = "claim"
    RESERVE_PARENT = "reserve_parent"
    RECORD_INTENT = "record_intent"
    AUTHORIZE_DISPATCH = "authorize_dispatch"
    DISPATCH = "dispatch"
    SOURCE_FINALIZE = "source_finalize"
    QUOTA_FINALIZE = "quota_finalize"
    UNKNOWN_BEFORE_DISPATCH = "unknown_before_dispatch"
    RELEASE_UNUSED = "release_unused"
    LINEAGE = "lineage"


class SourceBrokerV2DispatchOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class SourceBrokerV2ReplayStatus(StrEnum):
    FOUND = "FOUND"
    ABSENT = "ABSENT"
    UNKNOWN = "UNKNOWN"


class SourceBrokerV2ClaimStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    INFLIGHT = "INFLIGHT"
    UNKNOWN = "UNKNOWN"
    DEFINITIVELY_ABSENT = "DEFINITIVELY_ABSENT"


class SourceBrokerV2ClaimOnceRequest(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-claim-once/v2"] = "rquant-source-broker-claim-once/v2"
    saga_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: SourceBrokerV2OutboxPhase
    operation_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    executor_owner_token_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    executor_generation: int = Field(strict=True, ge=1)
    max_external_deadline: datetime
    not_before_takeover_at: datetime

    @model_validator(mode="after")
    def validate_claim_once(self) -> SourceBrokerV2ClaimOnceRequest:
        if self.phase not in {
            SourceBrokerV2OutboxPhase.DISPATCH,
            SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        }:
            raise ValueError("source claim_once only supports dispatch and finalize")
        deadline = _normalize_contract_time(
            self.max_external_deadline,
            label="max_external_deadline",
        )
        takeover = _normalize_contract_time(
            self.not_before_takeover_at,
            label="not_before_takeover_at",
        )
        if takeover < deadline:
            raise ValueError("takeover cannot precede the maximum external deadline")
        object.__setattr__(self, "max_external_deadline", deadline)
        object.__setattr__(self, "not_before_takeover_at", takeover)
        return self

    @property
    def request_hash(self) -> str:
        return _model_hash(self)


class SourceBrokerV2ClaimOnceResponse(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-claim-once-response/v2"] = (
        "rquant-source-broker-claim-once-response/v2"
    )
    saga_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: SourceBrokerV2OutboxPhase
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    executor_owner_token_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    executor_generation: int = Field(strict=True, ge=1)
    max_external_deadline: datetime
    not_before_takeover_at: datetime
    authority_id: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature_purpose: Literal["rquant-source-authority-receipt"] = (
        "rquant-source-authority-receipt"
    )
    observed_at: datetime
    status: SourceBrokerV2ClaimStatus
    result: bytes | None = Field(default=None, max_length=SOURCE_BROKER_V2_MAX_RECEIPT_BYTES)
    result_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_claim_response(self) -> SourceBrokerV2ClaimOnceResponse:
        deadline = _normalize_contract_time(
            self.max_external_deadline,
            label="max_external_deadline",
        )
        takeover = _normalize_contract_time(
            self.not_before_takeover_at,
            label="not_before_takeover_at",
        )
        observed = _normalize_contract_time(self.observed_at, label="observed_at")
        if takeover < deadline:
            raise ValueError("takeover cannot precede the maximum external deadline")
        terminal = self.status in {
            SourceBrokerV2ClaimStatus.SUCCESS,
            SourceBrokerV2ClaimStatus.FAILURE,
        }
        if terminal:
            if self.result is None or self.result_hash is None:
                raise ValueError("terminal source claim must include its canonical result")
            _require_canonical_json_bytes(self.result, label="source claim terminal result")
            if self.result_hash != canonical_sha256(strict_canonical_json_loads(self.result)):
                raise ValueError("source claim result hash conflicts")
        elif self.result is not None or self.result_hash is not None:
            raise ValueError("nonterminal source claim cannot include a result")
        object.__setattr__(self, "max_external_deadline", deadline)
        object.__setattr__(self, "not_before_takeover_at", takeover)
        object.__setattr__(self, "observed_at", observed)
        return self

    @property
    def receipt_hash(self) -> str:
        return _model_hash(self)

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class SourceBrokerV2ReplayRequest(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-replay/v2"] = "rquant-source-broker-replay/v2"
    saga_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: SourceBrokerV2OutboxPhase
    operation_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_phase(self) -> SourceBrokerV2ReplayRequest:
        if self.phase not in {
            SourceBrokerV2OutboxPhase.DISPATCH,
            SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        }:
            raise ValueError("source replay only supports dispatch and finalize")
        return self

    @property
    def request_hash(self) -> str:
        return _model_hash(self)


class SourceBrokerV2ReplayResponse(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-replay-response/v2"] = (
        "rquant-source-broker-replay-response/v2"
    )
    saga_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: SourceBrokerV2OutboxPhase
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: SourceBrokerV2ReplayStatus
    result: bytes | None = Field(default=None, max_length=SOURCE_BROKER_V2_MAX_RECEIPT_BYTES)
    result_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    authority_id: str = Field(min_length=1, max_length=200)
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature_purpose: Literal["rquant-source-authority-receipt"] = (
        "rquant-source-authority-receipt"
    )
    signature: str = Field(min_length=1, max_length=16_384)

    @model_validator(mode="after")
    def validate_result(self) -> SourceBrokerV2ReplayResponse:
        has_result = self.result is not None or self.result_hash is not None
        if self.status is SourceBrokerV2ReplayStatus.FOUND:
            if self.result is None or self.result_hash is None:
                raise ValueError("found source replay must carry its canonical result")
            _require_canonical_json_bytes(self.result, label="source replay result")
            if self.result_hash != canonical_sha256(strict_canonical_json_loads(self.result)):
                raise ValueError("source replay result hash conflicts")
        elif has_result:
            raise ValueError("non-found source replay cannot carry a result")
        return self

    @classmethod
    def from_result(
        cls,
        *,
        request: SourceBrokerV2ReplayRequest,
        result: bytes | None,
        authority_id: str,
        key_id: str,
        signature: str,
    ) -> SourceBrokerV2ReplayResponse:
        return cls(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            phase=request.phase,
            request_hash=request.request_hash,
            challenge=request.challenge,
            status=(
                SourceBrokerV2ReplayStatus.ABSENT
                if result is None
                else SourceBrokerV2ReplayStatus.FOUND
            ),
            result=result,
            result_hash=(
                None if result is None else canonical_sha256(strict_canonical_json_loads(result))
            ),
            authority_id=authority_id,
            key_id=key_id,
            signature=signature,
        )

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class SourceBrokerV2DispatchRequest(_StrictV2Model):
    """One opaque but fully-bound request sent to the future V2 source daemon."""

    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-dispatch/v2"] = "rquant-source-broker-dispatch/v2"
    saga_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    call_id: str = Field(min_length=1, max_length=200)
    attempt_identity_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_PAYLOAD_BYTES)
    claim_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    dispatch_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_payload(self) -> SourceBrokerV2DispatchRequest:
        _require_canonical_json_bytes(self.payload, label="source dispatch payload")
        if self.dispatch_payload_hash != canonical_sha256(
            strict_canonical_json_loads(self.payload)
        ):
            raise ValueError("source dispatch payload hash conflicts with canonical payload")
        return self

    @property
    def request_hash(self) -> str:
        return _model_hash(self)


class SourceBrokerV2DispatchResponse(_StrictV2Model):
    """Canonical response evidence from one idempotent source dispatch."""

    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-dispatch-response/v2"] = (
        "rquant-source-broker-dispatch-response/v2"
    )
    saga_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    call_id: str = Field(min_length=1, max_length=200)
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome: SourceBrokerV2DispatchOutcome
    response: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_PAYLOAD_BYTES)
    response_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    transport_receipt: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_RECEIPT_BYTES)

    @model_validator(mode="after")
    def validate_response(self) -> SourceBrokerV2DispatchResponse:
        _require_canonical_json_bytes(self.response, label="source response")
        _require_canonical_json_bytes(self.transport_receipt, label="source dispatch receipt")
        if self.response_hash != canonical_sha256(strict_canonical_json_loads(self.response)):
            raise ValueError("source response_hash conflicts with canonical response")
        return self

    @property
    def evidence_hash(self) -> str:
        return _model_hash(self)


class SourceBrokerV2FinalizeRequest(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-finalize/v2"] = "rquant-source-broker-finalize/v2"
    saga_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    dispatch_evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def request_hash(self) -> str:
        return _model_hash(self)


class SourceBrokerV2FinalizeResponse(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-finalize-response/v2"] = (
        "rquant-source-broker-finalize-response/v2"
    )
    saga_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    final_receipt: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_RECEIPT_BYTES)
    final_receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_final_receipt(self) -> SourceBrokerV2FinalizeResponse:
        _require_canonical_json_bytes(self.final_receipt, label="source final receipt")
        if self.final_receipt_hash != canonical_sha256(
            strict_canonical_json_loads(self.final_receipt)
        ):
            raise ValueError("source final receipt hash conflicts with canonical receipt")
        return self

    @property
    def evidence_hash(self) -> str:
        return _model_hash(self)


class SourceBrokerV2DispatchEnvelope(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-dispatch-envelope/v2"] = (
        "rquant-source-broker-dispatch-envelope/v2"
    )
    request: SourceBrokerV2DispatchRequest
    claim_receipt: SourceBrokerV2ClaimOnceResponse

    @model_validator(mode="after")
    def validate_envelope(self) -> SourceBrokerV2DispatchEnvelope:
        _require_source_grant(
            receipt=self.claim_receipt,
            operation_id=self.request.operation_id,
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
            operation_request_hash=self.request.request_hash,
            claim_binding_hash=self.request.claim_binding_hash,
        )
        return self


class SourceBrokerV2FinalizeEnvelope(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-finalize-envelope/v2"] = (
        "rquant-source-broker-finalize-envelope/v2"
    )
    request: SourceBrokerV2FinalizeRequest
    claim_receipt: SourceBrokerV2ClaimOnceResponse

    @model_validator(mode="after")
    def validate_envelope(self) -> SourceBrokerV2FinalizeEnvelope:
        _require_source_grant(
            receipt=self.claim_receipt,
            operation_id=self.request.operation_id,
            phase=SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
            operation_request_hash=self.request.request_hash,
            claim_binding_hash=self.request.claim_binding_hash,
        )
        return self


class SourceBrokerV2SagaRequest(_StrictV2Model):
    """Closed request to run one source call; no callback/provider crosses it."""

    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-saga-request/v2"] = (
        "rquant-source-broker-saga-request/v2"
    )
    saga_id: str = Field(min_length=1, max_length=200)
    claim_issue: CurrentClaimPlanIssueV2
    quota_binding: SourceQuotaParentBindingV2
    parent_total_cost: int = Field(strict=True, ge=1)
    call_id: str = Field(min_length=1, max_length=200)
    call_cost: int = Field(strict=True, ge=1)
    payload: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_PAYLOAD_BYTES)
    lineage_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_request(self) -> SourceBrokerV2SagaRequest:
        _require_canonical_json_bytes(self.payload, label="saga source payload")
        issue = self.claim_issue
        plan = issue.unsigned_plan
        binding = issue.binding
        request = plan.source_intent.resource_request
        if issue.binding_hash != binding.binding_hash:
            raise ValueError("claim issue binding hash is invalid")
        if request.requested_calls != 1 or request.cost_per_call != self.call_cost:
            raise ValueError("one V2 saga requires exactly one matching source call")
        if self.parent_total_cost != self.call_cost:
            raise ValueError("one V2 saga must reserve exactly its one call cost")
        if (
            self.quota_binding.claim_binding_hash != binding.binding_hash
            or self.quota_binding.claim_generation != binding.attempt_binding.claim_generation
            or self.quota_binding.scheduler_fencing_token
            != binding.attempt_binding.scheduler_fencing_token
            or self.quota_binding.source != request.source
        ):
            raise ValueError("quota parent binding conflicts with current-claim plan")
        return self

    @property
    def request_hash(self) -> str:
        return _model_hash(self)


class SourceBrokerV2SagaSnapshot(_StrictV2Model):
    schema_version: Literal[2] = 2
    saga_id: str = Field(min_length=1, max_length=200)
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: SourceBrokerV2SagaState
    dispatch_outcome: SourceBrokerV2DispatchOutcome | None = None
    reconcile_reason: str | None = Field(default=None, max_length=500)


class SourceAuthorityKeyring:
    """Closed Ed25519 trust root for one source authority and its rotations."""

    def __init__(
        self,
        *,
        expected_authority_id: str,
        allowed_public_keys: Mapping[str, bytes],
        expected_purpose: str,
        expected_schema_version: int,
    ) -> None:
        authority_id = expected_authority_id.strip()
        keys = dict(allowed_public_keys)
        if not authority_id:
            raise ValueError("source authority id must be nonempty")
        if expected_purpose != SOURCE_BROKER_V2_AUTHORITY_PURPOSE:
            raise ValueError("source authority keyring purpose is invalid")
        if type(expected_schema_version) is not int or expected_schema_version != 2:
            raise ValueError("source authority keyring schema version is invalid")
        if not keys or any(
            type(key_id) is not str
            or not key_id.strip()
            or type(public_key) is not bytes
            or not public_key
            for key_id, public_key in keys.items()
        ):
            raise ValueError("source authority public-key allowlist is invalid")
        fingerprints: set[str] = set()
        for public_key in keys.values():
            _validate_source_authority_public_key(public_key)
            fingerprint = _source_authority_public_key_fingerprint(public_key)
            if fingerprint in fingerprints:
                raise ValueError("source authority public key is duplicated across key ids")
            fingerprints.add(fingerprint)
        self.expected_authority_id = authority_id
        self.expected_purpose = expected_purpose
        self.expected_schema_version = expected_schema_version
        self._public_keys = keys

    @property
    def allowed_key_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._public_keys))

    def require_verified_claim(
        self,
        *,
        request: SourceBrokerV2ClaimOnceRequest,
        receipt: SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        if (
            receipt.schema_version != self.expected_schema_version
            or receipt.signature_purpose != self.expected_purpose
            or receipt.signature_algorithm != "ed25519"
            or receipt.authority_id != self.expected_authority_id
            or receipt.saga_id != request.saga_id
            or receipt.operation_id != request.operation_id
            or receipt.phase is not request.phase
            or receipt.request_hash != request.request_hash
            or receipt.operation_request_hash != request.operation_request_hash
            or receipt.challenge != request.challenge
            or receipt.claim_binding_hash != request.claim_binding_hash
            or receipt.claim_generation != request.claim_generation
            or receipt.scheduler_fencing_token != request.scheduler_fencing_token
            or receipt.executor_owner_token_hash != request.executor_owner_token_hash
            or receipt.executor_generation != request.executor_generation
            or receipt.max_external_deadline != request.max_external_deadline
            or receipt.not_before_takeover_at != request.not_before_takeover_at
        ):
            raise SourceBrokerV2SagaIntegrityError(
                "source authority claim receipt binding is invalid"
            )
        self._require_signature(
            key_id=receipt.key_id,
            signing_bytes=receipt.signing_bytes(),
            signature=receipt.signature,
        )

    def require_verified_replay(
        self,
        *,
        request: SourceBrokerV2ReplayRequest,
        receipt: SourceBrokerV2ReplayResponse,
    ) -> None:
        if (
            receipt.schema_version != self.expected_schema_version
            or receipt.signature_purpose != self.expected_purpose
            or receipt.signature_algorithm != "ed25519"
            or receipt.authority_id != self.expected_authority_id
            or receipt.operation_id != request.operation_id
            or receipt.saga_id != request.saga_id
            or receipt.phase is not request.phase
            or receipt.request_hash != request.request_hash
            or receipt.challenge != request.challenge
        ):
            raise SourceBrokerV2SagaIntegrityError(
                "source authority replay receipt binding is invalid"
            )
        self._require_signature(
            key_id=receipt.key_id,
            signing_bytes=receipt.signing_bytes(),
            signature=receipt.signature,
        )

    def _require_signature(
        self,
        *,
        key_id: str,
        signing_bytes: bytes,
        signature: str,
    ) -> None:
        public_key = self._public_keys.get(key_id)
        if public_key is None:
            raise SourceBrokerV2SagaIntegrityError("source authority receipt key id is not trusted")
        if not _verify_source_authority_signature(
            public_key=public_key,
            signing_bytes=signing_bytes,
            signature=signature,
        ):
            raise SourceBrokerV2SagaIntegrityError("source authority receipt signature is invalid")


class SourceBrokerV2WireRequest(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-unix-request/v2"] = (
        "rquant-source-broker-unix-request/v2"
    )
    operation: Literal["claim_once", "dispatch", "finalize", "replay"]
    challenge: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_RECEIPT_BYTES)
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_payload(self) -> SourceBrokerV2WireRequest:
        _require_canonical_json_bytes(self.payload, label="V2 Unix request payload")
        if self.payload_hash != canonical_sha256(strict_canonical_json_loads(self.payload)):
            raise ValueError("V2 Unix request payload hash conflicts")
        return self

    @property
    def request_hash(self) -> str:
        return _model_hash(self)


class SourceBrokerV2WireResponse(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-unix-response/v2"] = (
        "rquant-source-broker-unix-response/v2"
    )
    ok: Literal[True] = True
    operation: Literal["claim_once", "dispatch", "finalize", "replay"]
    challenge: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result: bytes = Field(min_length=1, max_length=SOURCE_BROKER_V2_MAX_RECEIPT_BYTES)
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_result(self) -> SourceBrokerV2WireResponse:
        _require_canonical_json_bytes(self.result, label="V2 Unix response result")
        if self.result_hash != canonical_sha256(strict_canonical_json_loads(self.result)):
            raise ValueError("V2 Unix response result hash conflicts")
        return self


class SourceBrokerV2WireFailure(_StrictV2Model):
    schema_version: Literal[2] = 2
    contract: Literal["rquant-source-broker-unix-failure/v2"] = (
        "rquant-source-broker-unix-failure/v2"
    )
    ok: Literal[False] = False
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenge: str = Field(pattern=r"^[0-9a-f]{64}$")
    error: str = Field(min_length=1, max_length=256)


class SourceBrokerV2Transport(Protocol):
    """Closed byte-only source boundary used after quota dispatch authorization."""

    def dispatch(self, payload: bytes, *, deadline: float | None = None) -> bytes: ...

    def finalize(self, payload: bytes, *, deadline: float | None = None) -> bytes: ...

    def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes: ...

    def claim_once(self, payload: bytes, *, deadline: float | None = None) -> bytes: ...


class SourceBrokerV2UnixClient:
    """Authenticated, bounded Unix client for the isolated V2 source endpoint."""

    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        server_policy: ServerCredentialsPolicy,
        total_request_deadline_seconds: float,
        source_authority_keyring: SourceAuthorityKeyring,
        max_attempts: int = 2,
    ) -> None:
        if type(endpoint) is not SocketEndpointPolicy:
            raise TypeError("source client requires the exact socket endpoint policy")
        if type(server_policy) is not ServerCredentialsPolicy:
            raise TypeError("source client requires the exact server credentials policy")
        if not 0 < total_request_deadline_seconds <= 30:
            raise ValueError("source client request deadline must be positive")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 5:
            raise ValueError("source client retry budget is invalid")
        if type(source_authority_keyring) is not SourceAuthorityKeyring:
            raise TypeError("source client requires the exact source authority keyring")
        self._endpoint = endpoint
        self._server_policy = server_policy
        self.total_request_deadline_seconds = total_request_deadline_seconds
        self._max_attempts = max_attempts
        self.source_authority_keyring = source_authority_keyring

    def dispatch(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        deadline = self._request_deadline(deadline)
        self._require_deadline_remaining(deadline, stage="before dispatch request parsing")
        try:
            envelope = strict_model_validate_canonical_json(
                SourceBrokerV2DispatchEnvelope,
                payload,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after dispatch request parsing")
            raise
        self._require_deadline_remaining(deadline, stage="after dispatch request parsing")
        self._require_deadline_remaining(deadline, stage="before dispatch grant verification")
        try:
            self.source_authority_keyring.require_verified_claim(
                request=_claim_request_from_receipt(envelope.claim_receipt),
                receipt=envelope.claim_receipt,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after dispatch grant verification")
            raise
        self._require_deadline_remaining(deadline, stage="after dispatch grant verification")
        raw = self._execute(operation="dispatch", payload=payload, deadline=deadline)
        self._require_deadline_remaining(deadline, stage="before dispatch response parsing")
        try:
            response = strict_model_validate_canonical_json(
                SourceBrokerV2DispatchResponse,
                raw,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after dispatch response parsing")
            raise
        self._require_deadline_remaining(deadline, stage="after dispatch response parsing")
        if (
            response.saga_id != envelope.request.saga_id
            or response.operation_id != envelope.request.operation_id
            or response.call_id != envelope.request.call_id
            or response.request_hash != envelope.request.request_hash
        ):
            raise SourceBrokerV2SagaIntegrityError("V2 Unix dispatch response binding is invalid")
        accepted = canonical_model_json_bytes(response)
        self._require_deadline_remaining(deadline, stage="before dispatch response acceptance")
        return accepted

    def finalize(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        deadline = self._request_deadline(deadline)
        self._require_deadline_remaining(deadline, stage="before finalize request parsing")
        try:
            envelope = strict_model_validate_canonical_json(
                SourceBrokerV2FinalizeEnvelope,
                payload,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after finalize request parsing")
            raise
        self._require_deadline_remaining(deadline, stage="after finalize request parsing")
        self._require_deadline_remaining(deadline, stage="before finalize grant verification")
        try:
            self.source_authority_keyring.require_verified_claim(
                request=_claim_request_from_receipt(envelope.claim_receipt),
                receipt=envelope.claim_receipt,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after finalize grant verification")
            raise
        self._require_deadline_remaining(deadline, stage="after finalize grant verification")
        raw = self._execute(operation="finalize", payload=payload, deadline=deadline)
        self._require_deadline_remaining(deadline, stage="before finalize response parsing")
        try:
            response = strict_model_validate_canonical_json(
                SourceBrokerV2FinalizeResponse,
                raw,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after finalize response parsing")
            raise
        self._require_deadline_remaining(deadline, stage="after finalize response parsing")
        if (
            response.saga_id != envelope.request.saga_id
            or response.operation_id != envelope.request.operation_id
            or response.request_hash != envelope.request.request_hash
        ):
            raise SourceBrokerV2SagaIntegrityError("V2 Unix finalize response binding is invalid")
        accepted = canonical_model_json_bytes(response)
        self._require_deadline_remaining(deadline, stage="before finalize response acceptance")
        return accepted

    def replay(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        deadline = self._request_deadline(deadline)
        self._require_deadline_remaining(deadline, stage="before replay request parsing")
        try:
            request = strict_model_validate_canonical_json(
                SourceBrokerV2ReplayRequest,
                payload,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after replay request parsing")
            raise
        self._require_deadline_remaining(deadline, stage="after replay request parsing")
        raw = self._execute(operation="replay", payload=payload, deadline=deadline)
        self._require_deadline_remaining(deadline, stage="before replay response parsing")
        try:
            response = strict_model_validate_canonical_json(
                SourceBrokerV2ReplayResponse,
                raw,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after replay response parsing")
            raise
        self._require_deadline_remaining(deadline, stage="after replay response parsing")
        self._require_deadline_remaining(deadline, stage="before replay response verification")
        try:
            self.source_authority_keyring.require_verified_replay(
                request=request,
                receipt=response,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after replay response verification")
            raise
        self._require_deadline_remaining(deadline, stage="after replay response verification")
        accepted = canonical_model_json_bytes(response)
        self._require_deadline_remaining(deadline, stage="before replay response acceptance")
        return accepted

    def claim_once(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        deadline = self._request_deadline(deadline)
        self._require_deadline_remaining(deadline, stage="before claim request parsing")
        try:
            request = strict_model_validate_canonical_json(
                SourceBrokerV2ClaimOnceRequest,
                payload,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after claim request parsing")
            raise
        self._require_deadline_remaining(deadline, stage="after claim request parsing")
        raw = self._execute(operation="claim_once", payload=payload, deadline=deadline)
        self._require_deadline_remaining(deadline, stage="before claim response parsing")
        try:
            response = strict_model_validate_canonical_json(
                SourceBrokerV2ClaimOnceResponse,
                raw,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after claim response parsing")
            raise
        self._require_deadline_remaining(deadline, stage="after claim response parsing")
        self._require_deadline_remaining(deadline, stage="before claim response verification")
        try:
            self.source_authority_keyring.require_verified_claim(
                request=request,
                receipt=response,
            )
        except BaseException:
            self._require_deadline_remaining(deadline, stage="after claim response verification")
            raise
        self._require_deadline_remaining(deadline, stage="after claim response verification")
        accepted = canonical_model_json_bytes(response)
        self._require_deadline_remaining(deadline, stage="before claim response acceptance")
        return accepted

    def _execute(
        self,
        *,
        operation: Literal["claim_once", "dispatch", "finalize", "replay"],
        payload: bytes,
        deadline: float | None = None,
    ) -> bytes:
        deadline = self._request_deadline(deadline)
        self._require_deadline_remaining(deadline, stage="before Unix request preparation")
        _require_canonical_json_bytes(payload, label="V2 Unix operation payload")
        require_linux_source_broker_transport()
        challenge = canonical_sha256(
            {
                "contract": "rquant-source-broker-unix-challenge/v2",
                "nonce": uuid4().hex,
                "operation": operation,
                "payload_hash": canonical_sha256(strict_canonical_json_loads(payload)),
            }
        )
        request = SourceBrokerV2WireRequest(
            operation=operation,
            challenge=challenge,
            payload=payload,
            payload_hash=canonical_sha256(strict_canonical_json_loads(payload)),
        )
        wire = canonical_model_json_bytes(request)
        self._require_deadline_remaining(deadline, stage="after Unix request preparation")
        last_error: BaseException | None = None
        attempt_budget = self._max_attempts if operation in {"claim_once", "replay"} else 1
        for attempt in range(attempt_budget):
            self._require_deadline_remaining(deadline, stage="before endpoint validation")
            try:
                endpoint_identity = validate_socket_endpoint(self._endpoint)
                self._require_deadline_remaining(deadline, stage="after endpoint validation")
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                    remaining = self._require_deadline_remaining(
                        deadline,
                        stage="before connect",
                    )
                    connection.settimeout(remaining)
                    connection.connect(str(self._endpoint.path))
                    self._require_deadline_remaining(deadline, stage="after connect")
                    server_pid, server_uid, server_gid = _v2_kernel_peer_credentials(connection)
                    if not self._server_policy.allows(
                        pid=server_pid,
                        uid=server_uid,
                        gid=server_gid,
                    ):
                        raise SourceBrokerTransportError(
                            "connected V2 source server credentials are not allowed"
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
                    self._require_deadline_remaining(
                        deadline,
                        stage="after server and endpoint identity verification",
                    )
                    self._write_frame_before_deadline(
                        connection,
                        wire,
                        deadline=deadline,
                    )
                    raw_response = self._read_frame_before_deadline(
                        connection,
                        deadline=deadline,
                    )
                    self._require_deadline_remaining(
                        deadline,
                        stage="after complete response read",
                    )
                self._require_deadline_remaining(
                    deadline,
                    stage="before wire response parsing",
                )
                try:
                    response = _decode_v2_wire_response(raw_response)
                except BaseException:
                    self._require_deadline_remaining(
                        deadline,
                        stage="after wire response parsing",
                    )
                    raise
                self._require_deadline_remaining(
                    deadline,
                    stage="after wire response parsing",
                )
                if isinstance(response, SourceBrokerV2WireFailure):
                    if (
                        response.request_hash != request.request_hash
                        or response.challenge != challenge
                    ):
                        raise SourceBrokerV2SagaIntegrityError(
                            "V2 Unix failure response binding is invalid"
                        )
                    raise SourceBrokerV2SagaConflictError(response.error)
                if (
                    response.operation != operation
                    or response.request_hash != request.request_hash
                    or response.challenge != challenge
                ):
                    raise SourceBrokerV2SagaIntegrityError("V2 Unix response binding is invalid")
                self._require_deadline_remaining(
                    deadline,
                    stage="before wire response acceptance",
                )
                return response.result
            except SourceBrokerV2TransportDeadlineError:
                raise
            except (SourceBrokerV2SagaIntegrityError, SourceBrokerV2SagaConflictError):
                raise
            except (OSError, TimeoutError, SourceBrokerTransportError) as exc:
                last_error = exc
                try:
                    self._require_deadline_remaining(
                        deadline,
                        stage="after transport failure",
                    )
                except SourceBrokerV2TransportDeadlineError as deadline_exc:
                    raise deadline_exc from exc
                if attempt + 1 < attempt_budget:
                    remaining = self._require_deadline_remaining(
                        deadline,
                        stage="before transport retry delay",
                    )
                    time.sleep(min(0.05 * (2**attempt), remaining))
        self._require_deadline_remaining(deadline, stage="after transport attempts")
        raise SourceBrokerV2SagaUnavailableError(
            "V2 source Unix transport exhausted its total request deadline"
        ) from last_error

    def _request_deadline(self, deadline: float | None) -> float:
        configured_deadline = time.monotonic() + self.total_request_deadline_seconds
        if deadline is None:
            return configured_deadline
        if type(deadline) not in {float, int} or not math.isfinite(deadline) or deadline <= 0:
            raise ValueError("source client deadline must be an absolute monotonic value")
        return min(configured_deadline, float(deadline))

    @staticmethod
    def _require_deadline_remaining(deadline: float, *, stage: str) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SourceBrokerV2TransportDeadlineError(
                f"V2 source broker transport deadline expired {stage}"
            )
        return remaining

    def _write_frame_before_deadline(
        self,
        connection: socket.socket,
        payload: bytes,
        *,
        deadline: float,
    ) -> None:
        if not payload or len(payload) > MAX_SOURCE_BROKER_FRAME_BYTES:
            raise SourceBrokerTransportError("source broker frame size is invalid")
        frame = memoryview(len(payload).to_bytes(4, "big") + payload)
        while frame:
            remaining = self._require_deadline_remaining(
                deadline,
                stage="before source frame write",
            )
            connection.settimeout(remaining)
            try:
                sent = connection.send(frame)
            except OSError as exc:
                raise SourceBrokerTransportError("source broker frame write failed") from exc
            if type(sent) is not int or sent <= 0 or sent > len(frame):
                raise SourceBrokerTransportError("source broker frame write made no progress")
            frame = frame[sent:]
            self._require_deadline_remaining(
                deadline,
                stage="after source frame write",
            )

    def _read_frame_before_deadline(
        self,
        connection: socket.socket,
        *,
        deadline: float,
    ) -> bytes:
        header = self._recv_exact_before_deadline(
            connection,
            4,
            deadline=deadline,
        )
        size = int.from_bytes(header, "big", signed=False)
        if not 0 < size <= MAX_SOURCE_BROKER_FRAME_BYTES:
            raise SourceBrokerTransportError("source broker frame size is invalid")
        return self._recv_exact_before_deadline(
            connection,
            size,
            deadline=deadline,
        )

    def _recv_exact_before_deadline(
        self,
        connection: socket.socket,
        size: int,
        *,
        deadline: float,
    ) -> bytes:
        chunks: list[bytes] = []
        remaining_bytes = size
        while remaining_bytes:
            remaining_time = self._require_deadline_remaining(
                deadline,
                stage="before source frame read",
            )
            connection.settimeout(remaining_time)
            try:
                chunk = connection.recv(remaining_bytes)
            except OSError as exc:
                raise SourceBrokerTransportError("source broker frame read failed") from exc
            if type(chunk) is not bytes or not chunk or len(chunk) > remaining_bytes:
                raise SourceBrokerTransportError("source broker frame is truncated")
            chunks.append(chunk)
            remaining_bytes -= len(chunk)
            self._require_deadline_remaining(
                deadline,
                stage="after source frame read",
            )
        return b"".join(chunks)


class _UnixSourceQuotaAdapter:
    """Closed production facade over the scheduler's quota Unix client."""

    __slots__ = ("_client", "adapter_id")

    def __init__(self, *, client: object, adapter_id: str) -> None:
        from rquant.source_broker_v2_authority_service import SourceBrokerV2SourceQuotaUnixClient

        if type(client) is not SourceBrokerV2SourceQuotaUnixClient:
            raise TypeError("production quota bridge requires the exact Unix quota client")
        if not adapter_id or len(adapter_id) > 200:
            raise ValueError("production quota bridge adapter id is invalid")
        self._client = client
        self.adapter_id = adapter_id

    def reserve_parent(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        total_cost: int,
        now: datetime,
        expires_at: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._receipt(
            phase=SourceQuotaBrokerPhaseV2.RESERVE_PARENT,
            operation_id=operation_id,
            binding=binding,
            result=self._client.reserve_parent(
                operation_id=operation_id,
                binding=binding,
                total_cost=total_cost,
                now=now,
                expires_at=expires_at,
            ),
        )

    def record_intent(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        call_id: str,
        cost: int,
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._receipt(
            phase=SourceQuotaBrokerPhaseV2.RECORD_INTENT,
            operation_id=operation_id,
            binding=binding,
            result=self._client.record_intent(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                call_id=call_id,
                cost=cost,
                now=now,
            ),
        )

    def authorize_dispatch(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._receipt(
            phase=SourceQuotaBrokerPhaseV2.AUTHORIZE_DISPATCH,
            operation_id=operation_id,
            binding=binding,
            result=self._client.authorize_dispatch(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                call_id=call_id,
                now=now,
            ),
        )

    def finalize(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        call_id: str,
        outcome: Literal["SUCCESS", "FAILURE", "UNKNOWN"],
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._receipt(
            phase=SourceQuotaBrokerPhaseV2.FINALIZE,
            operation_id=operation_id,
            binding=binding,
            result=self._client.finalize(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                call_id=call_id,
                outcome=SourceQuotaCallOutcome(outcome),
                now=now,
            ),
        )

    def unknown_before_dispatch(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._receipt(
            phase=SourceQuotaBrokerPhaseV2.UNKNOWN_BEFORE_DISPATCH,
            operation_id=operation_id,
            binding=binding,
            result=self._client.terminalize_unknown_before_dispatch(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                call_id=call_id,
                now=now,
            ),
        )

    def release_unused(
        self,
        *,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        now: datetime,
    ) -> SourceQuotaBrokerReceiptV2:
        return self._receipt(
            phase=SourceQuotaBrokerPhaseV2.RELEASE_UNUSED,
            operation_id=operation_id,
            binding=binding,
            result=self._client.release_unused(
                operation_id=operation_id,
                parent_id=binding.parent_id,
                now=now,
            ),
        )

    def _receipt(
        self,
        *,
        phase: SourceQuotaBrokerPhaseV2,
        operation_id: str,
        binding: SourceQuotaParentBindingV2,
        result: object,
    ) -> SourceQuotaBrokerReceiptV2:
        native = SourceQuotaAuthorityResult.model_validate(result, strict=True)
        return SourceQuotaBrokerReceiptV2(
            adapter_id=self.adapter_id,
            phase=phase,
            operation_id=operation_id,
            binding=binding,
            authority_result=native,
        )


@dataclass(frozen=True, slots=True)
class _ProductionSagaComponents:
    current_claim: object
    quota: object
    lineage: object
    authority_keyring: object
    source_keyring: object
    source: object

    @property
    def identity(self) -> tuple[int, int, int, int, int, int]:
        return tuple(
            id(component)
            for component in (
                self.current_claim,
                self.quota,
                self.lineage,
                self.authority_keyring,
                self.source_keyring,
                self.source,
            )
        )


class _ProductionSagaGraph:
    """Uncopyable capability tying one Saga to the attested Unix client graph."""

    __slots__ = (
        "_clients",
        "_evidence",
        "_original_components",
        "_original_component_identity",
        "_runtime",
        "_runtime_identity",
        "_clients_identity",
        "binding_hash",
        "current_claim",
        "quota",
        "lineage",
        "source",
        "source_keyring",
    )

    def __init__(
        self,
        *,
        _token: object,
        runtime: object,
        scheduler_clients: object,
    ) -> None:
        from rquant.source_broker_v2_authority import SourceBrokerV2SchedulerClients
        from rquant.source_broker_v2_authority_service import (
            SourceBrokerV2CurrentClaimUnixClient,
            SourceBrokerV2ReplayLineageUnixClient,
            SourceBrokerV2SourceQuotaUnixClient,
        )
        from rquant.source_broker_v2_runner import _validated_production_authority_evidence
        from rquant.source_broker_v2_runtime import SourceBrokerV2AuthorityRuntime

        if _token is not _PRODUCTION_SAGA_GRAPH_TOKEN:
            raise TypeError("production saga graph must come from the controlled factory")
        if type(runtime) is not SourceBrokerV2AuthorityRuntime:
            raise TypeError("production saga requires the exact authority runtime")
        if type(scheduler_clients) is not SourceBrokerV2SchedulerClients:
            raise TypeError("production saga requires exact scheduler composition clients")
        evidence = _validated_production_authority_evidence(runtime, scheduler_clients)
        if (
            type(scheduler_clients.current_claim) is not SourceBrokerV2CurrentClaimUnixClient
            or type(scheduler_clients.source_quota) is not SourceBrokerV2SourceQuotaUnixClient
            or type(scheduler_clients.replay_lineage) is not SourceBrokerV2ReplayLineageUnixClient
            or type(scheduler_clients.source_client) is not SourceBrokerV2UnixClient
            or type(scheduler_clients.source_authority_keyring) is not SourceAuthorityKeyring
            or scheduler_clients.source_client.source_authority_keyring
            is not scheduler_clients.source_authority_keyring
        ):
            raise TypeError("production saga graph contains an untrusted Unix client or keyring")
        self._runtime = runtime
        self._clients = scheduler_clients
        self._runtime_identity = id(runtime)
        self._clients_identity = id(scheduler_clients)
        self._evidence = evidence
        self._original_components = _ProductionSagaComponents(
            current_claim=scheduler_clients.current_claim,
            quota=scheduler_clients.source_quota,
            lineage=scheduler_clients.replay_lineage,
            authority_keyring=scheduler_clients.authority_keyring,
            source_keyring=scheduler_clients.source_authority_keyring,
            source=scheduler_clients.source_client,
        )
        self._original_component_identity = self._original_components.identity
        self.binding_hash = scheduler_clients.binding_hash
        self.current_claim = self._original_components.current_claim
        self.quota = self._original_components.quota
        self.lineage = self._original_components.lineage
        self.source = self._original_components.source
        self.source_keyring = self._original_components.source_keyring

    def __copy__(self) -> None:
        raise TypeError("production saga graph cannot be copied")

    def __deepcopy__(self, _memo: object) -> None:
        raise TypeError("production saga graph cannot be copied")

    def require_live(self) -> _ProductionSagaComponents:
        from rquant.source_broker_v2_runner import _validated_production_authority_evidence

        if (
            id(self._runtime) != self._runtime_identity
            or id(self._clients) != self._clients_identity
        ):
            raise TypeError("production saga runtime or clients changed after composition")
        current_evidence = _validated_production_authority_evidence(
            self._runtime,
            self._clients,
        )
        original = self._original_components
        if current_evidence != self._evidence:
            raise TypeError("production saga authority evidence changed after composition")
        if (
            original.identity != self._original_component_identity
            or self._clients.current_claim is not original.current_claim
            or self._clients.source_quota is not original.quota
            or self._clients.replay_lineage is not original.lineage
            or self._clients.authority_keyring is not original.authority_keyring
            or self._clients.source_authority_keyring is not original.source_keyring
            or self._clients.source_client is not original.source
            or self.current_claim is not original.current_claim
            or self.quota is not original.quota
            or self.lineage is not original.lineage
            or self.source is not original.source
            or self.source_keyring is not original.source_keyring
            or original.source.source_authority_keyring is not original.source_keyring
        ):
            raise TypeError("production saga graph components changed after composition")
        return original


class SourceBrokerV2Saga:
    """SQLite-backed source call saga with a persisted outbox for every effect.

    The public constructor is deliberately production-only.  Tests may use
    :meth:`for_nonproduction` to exercise recovery against finite local doubles;
    no scheduler, worker, callback, import path, pickle, or provider object is
    accepted by either construction path.
    """

    def __init__(
        self,
        path: Path,
        *,
        saga_id: str,
        current_claim_authority: PersistentCurrentClaimAuthority,
        quota_adapter: SourceQuotaBrokerAdapterV2,
        transport: SourceBrokerV2UnixClient,
        lineage_authority: PersistentReplayLineageAuthority,
        source_authority_keyring: SourceAuthorityKeyring,
        busy_timeout_ms: int = 5_000,
        executor_lease_seconds: float = 30.0,
        source_takeover_grace_seconds: float = 5.0,
    ) -> None:
        if type(transport) is not SourceBrokerV2UnixClient:
            raise TypeError("production V2 saga requires the exact V2 Unix client")
        minimum_lease = transport.total_request_deadline_seconds + source_takeover_grace_seconds
        if executor_lease_seconds < minimum_lease:
            raise ValueError(
                "production executor lease must cover the closed source client deadline "
                "plus takeover safety grace"
            )
        del (
            path,
            saga_id,
            current_claim_authority,
            quota_adapter,
            lineage_authority,
            source_authority_keyring,
            busy_timeout_ms,
        )
        raise TypeError(
            "production V2 saga requires for_production(runtime, scheduler_clients); "
            "tests must use for_nonproduction"
        )

    @classmethod
    def for_production(
        cls,
        path: Path,
        *,
        saga_id: str,
        runtime: object,
        scheduler_clients: object,
        busy_timeout_ms: int = 5_000,
        executor_lease_seconds: float | None = None,
    ) -> SourceBrokerV2Saga:
        """Create a production Saga from one preflighted, public-only Unix graph."""

        graph = _ProductionSagaGraph(
            _token=_PRODUCTION_SAGA_GRAPH_TOKEN,
            runtime=runtime,
            scheduler_clients=scheduler_clients,
        )
        configured_lease = (
            runtime.executor_lease_seconds
            if executor_lease_seconds is None
            else executor_lease_seconds
        )
        minimum_lease = (
            graph.source.total_request_deadline_seconds + runtime.source_takeover_grace_seconds
        )
        if configured_lease < minimum_lease:
            raise ValueError(
                "production executor lease must cover the closed source client deadline "
                "plus takeover safety grace"
            )
        instance = cls.__new__(cls)
        instance._configure(
            path=path,
            saga_id=saga_id,
            current_claim_authority=graph.current_claim,
            quota_adapter=_UnixSourceQuotaAdapter(
                client=graph.quota,
                adapter_id=f"source-broker-v2-unix-quota-{graph.binding_hash}",
            ),
            transport=graph.source,
            lineage_authority=graph.lineage,
            source_authority_keyring=graph.source_keyring,
            busy_timeout_ms=busy_timeout_ms,
            executor_lease_seconds=configured_lease,
            executor_wait_seconds=5.0,
            source_request_deadline_seconds=graph.source.total_request_deadline_seconds,
            source_takeover_grace_seconds=runtime.source_takeover_grace_seconds,
            production_graph=graph,
        )
        return instance

    @classmethod
    def for_nonproduction(
        cls,
        path: Path,
        *,
        saga_id: str,
        current_claim_authority: object,
        quota_adapter: SourceQuotaBrokerAdapterV2,
        transport: SourceBrokerV2Transport,
        lineage_authority: object,
        source_authority_keyring: SourceAuthorityKeyring | None = None,
        busy_timeout_ms: int = 5_000,
        executor_lease_seconds: float = 30.0,
        executor_wait_seconds: float = 5.0,
        source_request_deadline_seconds: float = 0.25,
        source_takeover_grace_seconds: float = 0.05,
    ) -> SourceBrokerV2Saga:
        """Build an explicitly nonproduction instance for deterministic recovery tests."""

        if type(quota_adapter) is not SourceQuotaBrokerAdapterV2:
            raise TypeError("V2 saga requires SourceQuotaBrokerAdapterV2")
        if source_authority_keyring is None:
            source_authority_keyring = getattr(transport, "source_authority_keyring", None)
        if type(source_authority_keyring) is not SourceAuthorityKeyring:
            raise TypeError("V2 saga requires SourceAuthorityKeyring")
        instance = cls.__new__(cls)
        instance._configure(
            path=path,
            saga_id=saga_id,
            current_claim_authority=current_claim_authority,
            quota_adapter=quota_adapter,
            transport=transport,
            lineage_authority=lineage_authority,
            source_authority_keyring=source_authority_keyring,
            busy_timeout_ms=busy_timeout_ms,
            executor_lease_seconds=executor_lease_seconds,
            executor_wait_seconds=executor_wait_seconds,
            source_request_deadline_seconds=source_request_deadline_seconds,
            source_takeover_grace_seconds=source_takeover_grace_seconds,
            production_graph=None,
        )
        return instance

    def _configure(
        self,
        *,
        path: Path,
        saga_id: str,
        current_claim_authority: object,
        quota_adapter: SourceQuotaBrokerAdapterV2 | _UnixSourceQuotaAdapter,
        transport: SourceBrokerV2Transport,
        lineage_authority: object,
        source_authority_keyring: SourceAuthorityKeyring,
        busy_timeout_ms: int,
        executor_lease_seconds: float,
        executor_wait_seconds: float,
        source_request_deadline_seconds: float,
        source_takeover_grace_seconds: float,
        production_graph: _ProductionSagaGraph | None,
    ) -> None:
        if not saga_id.strip():
            raise ValueError("saga_id must be nonempty")
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        if executor_lease_seconds <= 0 or executor_wait_seconds <= 0:
            raise ValueError("executor lease and wait durations must be positive")
        if source_request_deadline_seconds <= 0 or source_takeover_grace_seconds < 0:
            raise ValueError("source deadline must be positive and grace cannot be negative")
        if type(quota_adapter) not in {SourceQuotaBrokerAdapterV2, _UnixSourceQuotaAdapter}:
            raise TypeError("V2 saga requires an exact quota bridge")
        if production_graph is None:
            if type(quota_adapter) is not SourceQuotaBrokerAdapterV2:
                raise TypeError("nonproduction V2 saga requires SourceQuotaBrokerAdapterV2")
        elif (
            type(production_graph) is not _ProductionSagaGraph
            or type(quota_adapter) is not _UnixSourceQuotaAdapter
            or transport is not production_graph.source
            or source_authority_keyring is not production_graph.source_keyring
            or current_claim_authority is not production_graph.current_claim
            or lineage_authority is not production_graph.lineage
            or quota_adapter._client is not production_graph.quota
        ):
            raise TypeError("production V2 saga graph was replaced during construction")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.saga_id = saga_id
        self._current_claim_authority = current_claim_authority
        self._quota_adapter = quota_adapter
        self._transport = transport
        self._lineage_authority = lineage_authority
        self._source_authority_keyring = source_authority_keyring
        self._busy_timeout_ms = busy_timeout_ms
        self._executor_owner_token = uuid4().hex
        self._executor_lease_seconds = executor_lease_seconds
        self._executor_wait_seconds = executor_wait_seconds
        self._source_request_deadline_seconds = source_request_deadline_seconds
        self._source_takeover_grace_seconds = source_takeover_grace_seconds
        self._production_graph = production_graph
        self._production_binding_hash = (
            None if production_graph is None else production_graph.binding_hash
        )
        self._initialize()

    @property
    def production_binding_hash(self) -> str | None:
        return self._production_binding_hash

    def advance(
        self, request: SourceBrokerV2SagaRequest, *, now: datetime
    ) -> SourceBrokerV2SagaSnapshot:
        """Run/recover every safe stage; ambiguity remains explicit and terminal."""

        self._require_live_production_graph()
        validated = self._require_request(request)
        snapshot = self._ensure_saga(validated)
        if snapshot.state is SourceBrokerV2SagaState.RECONCILE_REQUIRED:
            return snapshot
        if snapshot.state is SourceBrokerV2SagaState.COMPLETE and snapshot.dispatch_outcome is None:
            return self.compensate_before_dispatch(validated, now=now)
        try:
            claim = self._claim(validated, now=now)
            binding = validated.quota_binding
            self._quota(
                SourceBrokerV2OutboxPhase.RESERVE_PARENT,
                validated,
                now=now,
                invoke=lambda op, observed: self._quota_adapter.reserve_parent(
                    operation_id=op,
                    binding=binding,
                    total_cost=validated.parent_total_cost,
                    now=observed,
                    expires_at=claim.signed_plan.lease_expires_at,
                ),
            )
            self._transition(SourceBrokerV2SagaState.PARENT_RESERVED)
            self._quota(
                SourceBrokerV2OutboxPhase.RECORD_INTENT,
                validated,
                now=now,
                invoke=lambda op, observed: self._quota_adapter.record_intent(
                    operation_id=op,
                    binding=binding,
                    call_id=validated.call_id,
                    cost=validated.call_cost,
                    now=observed,
                ),
            )
            self._transition(SourceBrokerV2SagaState.CALL_INTENT)
            self._quota(
                SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
                validated,
                now=now,
                invoke=lambda op, observed: self._quota_adapter.authorize_dispatch(
                    operation_id=op,
                    binding=binding,
                    call_id=validated.call_id,
                    now=observed,
                ),
            )
            self._transition(SourceBrokerV2SagaState.DISPATCH_AUTHORIZED)
            dispatch = self._dispatch(validated, claim=claim)
        except SourceBrokerV2SagaReconcileRequiredError:
            reconciled = self.snapshot()
            if reconciled.state is not SourceBrokerV2SagaState.RECONCILE_REQUIRED:
                raise
            return reconciled

        self._transition(
            SourceBrokerV2SagaState.DISPATCH_OUTCOME,
            outcome=dispatch.outcome,
        )
        finalized = self._source_finalize(validated, dispatch=dispatch)
        self._transition(SourceBrokerV2SagaState.SOURCE_FINALIZED)
        self._quota(
            SourceBrokerV2OutboxPhase.QUOTA_FINALIZE,
            validated,
            now=now,
            invoke=lambda op, observed: self._quota_adapter.finalize(
                operation_id=op,
                binding=validated.quota_binding,
                call_id=validated.call_id,
                outcome=dispatch.outcome.value,
                now=observed,
            ),
        )
        self._transition(SourceBrokerV2SagaState.QUOTA_TERMINAL, outcome=dispatch.outcome)
        released = self._quota(
            SourceBrokerV2OutboxPhase.RELEASE_UNUSED,
            validated,
            now=now,
            invoke=lambda op, observed: self._quota_adapter.release_unused(
                operation_id=op,
                binding=validated.quota_binding,
                now=observed,
            ),
        )
        next_state = (
            SourceBrokerV2SagaState.COMPENSATED
            if released.authority_result.parent.state.value == "COMPENSATED"
            else SourceBrokerV2SagaState.PARENT_RELEASED
        )
        self._transition(next_state, outcome=dispatch.outcome)
        self._publish_lineage(validated, claim=claim, dispatch=dispatch, finalized=finalized)
        self._transition(SourceBrokerV2SagaState.LINEAGE_PUBLISHED, outcome=dispatch.outcome)
        self._transition(SourceBrokerV2SagaState.COMPLETE, outcome=dispatch.outcome)
        return self.snapshot()

    def compensate_before_dispatch(
        self,
        request: SourceBrokerV2SagaRequest,
        *,
        now: datetime,
    ) -> SourceBrokerV2SagaSnapshot:
        """Compensate only while durable evidence proves dispatch was impossible."""

        self._require_live_production_graph()
        validated = self._require_request(request)
        snapshot = self._ensure_saga(validated)
        allowed_states = {
            SourceBrokerV2SagaState.CLAIMED,
            SourceBrokerV2SagaState.PARENT_RESERVED,
            SourceBrokerV2SagaState.CALL_INTENT,
            SourceBrokerV2SagaState.CALL_TERMINALIZED,
            SourceBrokerV2SagaState.PARENT_RELEASED,
            SourceBrokerV2SagaState.COMPENSATED,
        }
        if snapshot.state is SourceBrokerV2SagaState.COMPLETE:
            if snapshot.dispatch_outcome is not None:
                return snapshot
        elif snapshot.state not in allowed_states:
            raise SourceBrokerV2SagaReconcileRequiredError(
                "dispatch was authorized or may have been attempted; compensation is unsafe"
            )
        claim = self._claim(validated, now=now)
        binding = validated.quota_binding
        self._quota(
            SourceBrokerV2OutboxPhase.RESERVE_PARENT,
            validated,
            now=now,
            invoke=lambda op, observed: self._quota_adapter.reserve_parent(
                operation_id=op,
                binding=binding,
                total_cost=validated.parent_total_cost,
                now=observed,
                expires_at=claim.signed_plan.lease_expires_at,
            ),
        )
        self._transition(SourceBrokerV2SagaState.PARENT_RESERVED)
        self._quota(
            SourceBrokerV2OutboxPhase.RECORD_INTENT,
            validated,
            now=now,
            invoke=lambda op, observed: self._quota_adapter.record_intent(
                operation_id=op,
                binding=binding,
                call_id=validated.call_id,
                cost=validated.call_cost,
                now=observed,
            ),
        )
        self._transition(SourceBrokerV2SagaState.CALL_INTENT)
        self._quota(
            SourceBrokerV2OutboxPhase.UNKNOWN_BEFORE_DISPATCH,
            validated,
            now=now,
            invoke=lambda op, observed: self._quota_adapter.unknown_before_dispatch(
                operation_id=op,
                binding=binding,
                call_id=validated.call_id,
                now=observed,
            ),
        )
        self._transition(SourceBrokerV2SagaState.CALL_TERMINALIZED)
        self._quota(
            SourceBrokerV2OutboxPhase.RELEASE_UNUSED,
            validated,
            now=now,
            invoke=lambda op, observed: self._quota_adapter.release_unused(
                operation_id=op,
                binding=binding,
                now=observed,
            ),
        )
        self._transition(SourceBrokerV2SagaState.PARENT_RELEASED)
        self._transition(SourceBrokerV2SagaState.COMPENSATED)
        self._transition(SourceBrokerV2SagaState.COMPLETE)
        return self.snapshot()

    def reconcile(
        self, request: SourceBrokerV2SagaRequest, *, now: datetime
    ) -> SourceBrokerV2SagaSnapshot:
        """Retry the exact durable dispatch operation after an ambiguous response loss.

        This intentionally permits no new payload or operation identity.  The
        source authority/transport must replay the same canonical operation.
        """

        self._require_live_production_graph()
        validated = self._require_request(request)
        snapshot = self._ensure_saga(validated)
        if snapshot.state is not SourceBrokerV2SagaState.RECONCILE_REQUIRED:
            return self.advance(validated, now=now)
        phase = SourceBrokerV2OutboxPhase.DISPATCH
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_outbox(
                    connection,
                    operation_id=self._operation_id(phase),
                    phase=phase,
                )
                if row["status"] != "pending":
                    raise SourceBrokerV2SagaIntegrityError(
                        "reconcile-required saga has no pending dispatch evidence"
                    )
                connection.execute(
                    "UPDATE source_broker_v2_saga SET state = ?, reconcile_reason = NULL "
                    "WHERE saga_id = ? AND state = ?",
                    (
                        SourceBrokerV2SagaState.DISPATCH_AUTHORIZED.value,
                        self.saga_id,
                        SourceBrokerV2SagaState.RECONCILE_REQUIRED.value,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return self.advance(validated, now=now)

    def snapshot(self) -> SourceBrokerV2SagaSnapshot:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT request_hash, state, dispatch_outcome, reconcile_reason "
                "FROM source_broker_v2_saga WHERE saga_id = ?",
                (self.saga_id,),
            ).fetchone()
        if row is None:
            raise SourceBrokerV2SagaConflictError("saga has not been created")
        return self._snapshot_from_row(row)

    def _claim(
        self,
        request: SourceBrokerV2SagaRequest,
        *,
        now: datetime,
    ) -> CurrentClaimConsumptionV2:
        phase = SourceBrokerV2OutboxPhase.CLAIM
        operation_id = self._operation_id(phase)
        payload = canonical_model_json_bytes(request.claim_issue)

        def invoke(_persisted_payload: bytes) -> bytes:
            try:
                graph = self._production_graph
                if graph is not None:
                    receipt = graph.current_claim.issue_plan_once(
                        issue=request.claim_issue,
                        now=now,
                    )
                else:
                    issue_once = getattr(self._current_claim_authority, "issue_plan_once", None)
                    if not callable(issue_once):
                        raise SourceBrokerV2SagaUnavailableError(
                            "current-claim authority is unavailable"
                        )
                    receipt = issue_once(issue=request.claim_issue, now=now)
                validated = CurrentClaimConsumptionV2.model_validate(receipt, strict=True)
                if (
                    validated.binding != request.claim_issue.binding
                    or validated.binding_hash != request.claim_issue.binding_hash
                    or validated.signed_plan.signing_payload()
                    != request.claim_issue.unsigned_plan.signing_payload()
                ):
                    raise SourceBrokerV2SagaIntegrityError(
                        "current-claim authority returned a foreign plan receipt"
                    )
                return canonical_model_json_bytes(validated)
            except SourceBrokerV2SagaError:
                raise
            except Exception as exc:
                raise SourceBrokerV2SagaUnavailableError(
                    "current-claim authority failed while issuing the source plan"
                ) from exc

        raw = self._apply_outbox(
            phase=phase, operation_id=operation_id, payload=payload, invoke=invoke
        )
        try:
            receipt = strict_model_validate_canonical_json(CurrentClaimConsumptionV2, raw)
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError(
                "stored current-claim receipt is invalid"
            ) from exc
        if receipt.binding != request.claim_issue.binding:
            raise SourceBrokerV2SagaIntegrityError("stored claim receipt was rebound")
        try:
            graph = self._production_graph
            if graph is not None:
                native_value = graph.current_claim.verify_current(
                    binding=receipt.binding,
                    now=now,
                )
            else:
                verify_current = getattr(self._current_claim_authority, "verify_current", None)
                if not callable(verify_current):
                    raise SourceBrokerV2SagaUnavailableError(
                        "current-claim authority cannot verify its native receipt"
                    )
                native_value = verify_current(binding=receipt.binding, now=now)
            native = CurrentClaimConsumptionV2.model_validate(native_value, strict=True)
        except (ConnectionError, OSError, TimeoutError) as exc:
            raise SourceBrokerV2SagaUnavailableError(
                "current-claim authority verification is unavailable"
            ) from exc
        except Exception as exc:
            raise SourceBrokerV2SagaConflictError(
                "current claim generation or fencing token is no longer current"
            ) from exc
        if native != receipt:
            raise SourceBrokerV2SagaIntegrityError(
                "stored claim receipt conflicts with current-claim authority"
            )
        return receipt

    def _quota(
        self,
        phase: SourceBrokerV2OutboxPhase,
        request: SourceBrokerV2SagaRequest,
        *,
        now: datetime,
        invoke: Callable[[str, datetime], SourceQuotaBrokerReceiptV2],
    ) -> SourceQuotaBrokerReceiptV2:
        operation_id = self._operation_id(phase)
        payload = canonical_json_bytes(
            {
                "binding": request.quota_binding.model_dump(mode="json", round_trip=True),
                "call_cost": request.call_cost,
                "call_id": request.call_id,
                "now": request.claim_issue.binding.not_before.isoformat(),
                "parent_total_cost": request.parent_total_cost,
                "phase": phase.value,
            }
        )

        def apply(persisted_payload: bytes) -> bytes:
            try:
                receipt = invoke(operation_id, _outbox_now(persisted_payload))
                validated = SourceQuotaBrokerReceiptV2.model_validate(receipt, strict=True)
                if (
                    validated.operation_id != operation_id
                    or validated.binding != request.quota_binding
                ):
                    raise SourceBrokerV2SagaIntegrityError(
                        "quota adapter returned a foreign receipt"
                    )
                return encode_source_quota_broker_receipt_v2(validated)
            except SourceBrokerV2SagaError:
                raise
            except Exception as exc:
                raise SourceBrokerV2SagaUnavailableError(
                    f"quota adapter failed during {phase.value}"
                ) from exc

        raw = self._apply_outbox(
            phase=phase, operation_id=operation_id, payload=payload, invoke=apply
        )
        try:
            receipt = decode_source_quota_broker_receipt_v2(raw)
        except Exception as exc:
            raise SourceBrokerV2SagaIntegrityError("stored quota receipt is invalid") from exc
        if receipt.operation_id != operation_id or receipt.binding != request.quota_binding:
            raise SourceBrokerV2SagaIntegrityError("stored quota receipt was rebound")
        persisted_payload = self._stored_outbox_payload(
            phase=phase,
            operation_id=operation_id,
        )
        try:
            native = SourceQuotaBrokerReceiptV2.model_validate(
                invoke(operation_id, _outbox_now(persisted_payload)),
                strict=True,
            )
        except (ConnectionError, OSError, TimeoutError) as exc:
            raise SourceBrokerV2SagaUnavailableError(
                f"quota authority verification is unavailable during {phase.value}"
            ) from exc
        except Exception as exc:
            raise SourceBrokerV2SagaIntegrityError(
                f"quota native chain failed verification during {phase.value}"
            ) from exc
        if native != receipt:
            raise SourceBrokerV2SagaIntegrityError(
                f"stored quota receipt conflicts with native {phase.value} replay"
            )
        return receipt

    def _dispatch(
        self,
        request: SourceBrokerV2SagaRequest,
        *,
        claim: CurrentClaimConsumptionV2,
    ) -> SourceBrokerV2DispatchResponse:
        phase = SourceBrokerV2OutboxPhase.DISPATCH
        operation_id = self._operation_id(phase)
        signed_plan = claim.signed_plan
        dispatch_request = SourceBrokerV2DispatchRequest(
            saga_id=self.saga_id,
            operation_id=operation_id,
            call_id=request.call_id,
            attempt_identity_hash=signed_plan.attempt_identity_hash,
            claim_plan_hash=signed_plan.plan_hash,
            claim_binding_hash=claim.binding_hash,
            manifest_hash=signed_plan.manifest_hash,
            payload=request.payload,
            claim_payload_hash=signed_plan.payload_hash,
            dispatch_payload_hash=canonical_sha256(strict_canonical_json_loads(request.payload)),
        )
        payload = canonical_model_json_bytes(dispatch_request)

        def authorize(
            _persisted_payload: bytes,
            owner_generation: int,
            _invoke_started: bool,
        ) -> tuple[bytes | None, bytes | None]:
            source_claim = self._source_claim_once(
                request=request,
                phase=phase,
                operation_id=operation_id,
                operation_request_hash=dispatch_request.request_hash,
                owner_generation=owner_generation,
            )
            if source_claim.status in {
                SourceBrokerV2ClaimStatus.SUCCESS,
                SourceBrokerV2ClaimStatus.FAILURE,
            }:
                return source_claim.result, None
            if source_claim.status in {
                SourceBrokerV2ClaimStatus.INFLIGHT,
                SourceBrokerV2ClaimStatus.UNKNOWN,
            }:
                raise SourceBrokerV2SagaReconcileRequiredError(
                    f"source dispatch is {source_claim.status.value.lower()}"
                )
            envelope = SourceBrokerV2DispatchEnvelope(
                request=dispatch_request,
                claim_receipt=source_claim,
            )
            return None, canonical_model_json_bytes(envelope)

        def invoke(invocation_payload: bytes) -> bytes:
            try:
                strict_model_validate_canonical_json(
                    SourceBrokerV2DispatchEnvelope,
                    invocation_payload,
                )
                raw = self._transport.dispatch(invocation_payload)
                response = strict_model_validate_canonical_json(SourceBrokerV2DispatchResponse, raw)
                if (
                    response.saga_id != self.saga_id
                    or response.operation_id != operation_id
                    or response.call_id != request.call_id
                    or response.request_hash != dispatch_request.request_hash
                ):
                    raise SourceBrokerV2SagaIntegrityError(
                        "source dispatch response binding is invalid"
                    )
                return canonical_model_json_bytes(response)
            except SourceBrokerV2SagaError:
                raise
            except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
                raise SourceBrokerV2SagaIntegrityError(
                    "source dispatch response is malformed"
                ) from exc
            except Exception as exc:
                raise SourceBrokerV2SagaUnavailableError(
                    "source dispatch response is unknown"
                ) from exc

        try:
            raw = self._apply_outbox(
                phase=phase,
                operation_id=operation_id,
                payload=payload,
                invoke=invoke,
                authorize=authorize,
                verify=lambda stored: self._verify_source_result(
                    stored=stored,
                    phase=phase,
                    operation_id=operation_id,
                    operation_request_hash=dispatch_request.request_hash,
                ),
            )
        except (
            SourceBrokerV2SagaReconcileRequiredError,
            SourceBrokerV2SagaUnavailableError,
            ConnectionError,
            OSError,
            TimeoutError,
        ) as exc:
            current = self.snapshot()
            if current.state is SourceBrokerV2SagaState.DISPATCH_AUTHORIZED:
                self._transition(
                    SourceBrokerV2SagaState.DISPATCH_UNKNOWN,
                    reconcile_reason=str(exc),
                )
                current = self.snapshot()
            if current.state is SourceBrokerV2SagaState.DISPATCH_UNKNOWN:
                self._transition(
                    SourceBrokerV2SagaState.RECONCILE_REQUIRED,
                    reconcile_reason=str(exc),
                )
            raise SourceBrokerV2SagaReconcileRequiredError(
                "source dispatch may have occurred; reconcile before quota terminalization"
            ) from exc
        try:
            response = strict_model_validate_canonical_json(SourceBrokerV2DispatchResponse, raw)
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError(
                "stored source dispatch response is invalid"
            ) from exc
        return response

    def _source_finalize(
        self,
        request: SourceBrokerV2SagaRequest,
        *,
        dispatch: SourceBrokerV2DispatchResponse,
    ) -> SourceBrokerV2FinalizeResponse:
        phase = SourceBrokerV2OutboxPhase.SOURCE_FINALIZE
        operation_id = self._operation_id(phase)
        finalize_request = SourceBrokerV2FinalizeRequest(
            saga_id=self.saga_id,
            operation_id=operation_id,
            dispatch_evidence_hash=dispatch.evidence_hash,
            claim_binding_hash=request.claim_issue.binding_hash,
        )
        payload = canonical_model_json_bytes(finalize_request)

        def authorize(
            _persisted_payload: bytes,
            owner_generation: int,
            _invoke_started: bool,
        ) -> tuple[bytes | None, bytes | None]:
            source_claim = self._source_claim_once(
                request=request,
                phase=phase,
                operation_id=operation_id,
                operation_request_hash=finalize_request.request_hash,
                owner_generation=owner_generation,
            )
            if source_claim.status in {
                SourceBrokerV2ClaimStatus.SUCCESS,
                SourceBrokerV2ClaimStatus.FAILURE,
            }:
                return source_claim.result, None
            if source_claim.status in {
                SourceBrokerV2ClaimStatus.INFLIGHT,
                SourceBrokerV2ClaimStatus.UNKNOWN,
            }:
                raise SourceBrokerV2SagaReconcileRequiredError(
                    f"source finalize is {source_claim.status.value.lower()}"
                )
            envelope = SourceBrokerV2FinalizeEnvelope(
                request=finalize_request,
                claim_receipt=source_claim,
            )
            return None, canonical_model_json_bytes(envelope)

        def invoke(invocation_payload: bytes) -> bytes:
            try:
                strict_model_validate_canonical_json(
                    SourceBrokerV2FinalizeEnvelope,
                    invocation_payload,
                )
                raw = self._transport.finalize(invocation_payload)
                response = strict_model_validate_canonical_json(SourceBrokerV2FinalizeResponse, raw)
                if (
                    response.saga_id != self.saga_id
                    or response.operation_id != operation_id
                    or response.request_hash != finalize_request.request_hash
                ):
                    raise SourceBrokerV2SagaIntegrityError(
                        "source finalize response binding is invalid"
                    )
                return canonical_model_json_bytes(response)
            except SourceBrokerV2SagaError:
                raise
            except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
                raise SourceBrokerV2SagaIntegrityError(
                    "source finalize response is malformed"
                ) from exc
            except Exception as exc:
                raise SourceBrokerV2SagaUnavailableError(
                    "source finalize response is unknown"
                ) from exc

        raw = self._apply_outbox(
            phase=phase,
            operation_id=operation_id,
            payload=payload,
            invoke=invoke,
            authorize=authorize,
            verify=lambda stored: self._verify_source_result(
                stored=stored,
                phase=phase,
                operation_id=operation_id,
                operation_request_hash=finalize_request.request_hash,
            ),
        )
        try:
            return strict_model_validate_canonical_json(SourceBrokerV2FinalizeResponse, raw)
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError(
                "stored source finalize response is invalid"
            ) from exc

    def _publish_lineage(
        self,
        request: SourceBrokerV2SagaRequest,
        *,
        claim: CurrentClaimConsumptionV2,
        dispatch: SourceBrokerV2DispatchResponse,
        finalized: SourceBrokerV2FinalizeResponse,
    ) -> bytes:
        phase = SourceBrokerV2OutboxPhase.LINEAGE
        operation_id = self._operation_id(phase)
        payload = canonical_json_bytes(
            {
                "claim_binding_hash": claim.binding_hash,
                "dispatch_evidence_hash": dispatch.evidence_hash,
                "finalize_evidence_hash": finalized.evidence_hash,
                "lineage_authority_id": request.lineage_authority_id,
                "lineage_id": request.lineage_id,
                "operation_id": operation_id,
                "saga_id": self.saga_id,
            }
        )

        next_head = canonical_sha256(
            {
                "dispatch": dispatch.evidence_hash,
                "finalize": finalized.evidence_hash,
                "saga_id": self.saga_id,
            }
        )

        def invoke(_persisted_payload: bytes) -> bytes:
            try:
                graph = self._production_graph
                if graph is not None:
                    receipt = graph.lineage.compare_and_advance(
                        operation_id=operation_id,
                        replay_authority_id=request.lineage_authority_id,
                        lineage_id=request.lineage_id,
                        previous_head_hash=_ZERO_HASH,
                        next_head_hash=next_head,
                        sequence=1,
                        claim_binding_hash=claim.binding_hash,
                    )
                else:
                    compare = getattr(self._lineage_authority, "compare_and_advance", None)
                    if not callable(compare):
                        raise SourceBrokerV2SagaUnavailableError("lineage authority is unavailable")
                    receipt = compare(
                        operation_id=operation_id,
                        replay_authority_id=request.lineage_authority_id,
                        lineage_id=request.lineage_id,
                        previous_head_hash=_ZERO_HASH,
                        next_head_hash=next_head,
                        sequence=1,
                        claim_binding_hash=claim.binding_hash,
                    )
                raw = canonical_model_json_bytes(receipt)
                _require_canonical_json_bytes(raw, label="lineage receipt")
                return raw
            except SourceBrokerV2SagaError:
                raise
            except Exception as exc:
                raise SourceBrokerV2SagaUnavailableError("lineage authority failed") from exc

        raw = self._apply_outbox(
            phase=phase, operation_id=operation_id, payload=payload, invoke=invoke
        )
        try:
            receipt = strict_model_validate_canonical_json(
                ReplayLineageCheckpointReceipt,
                raw,
            )
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError("stored lineage receipt is invalid") from exc
        if (
            receipt.operation_id != operation_id
            or receipt.replay_authority_id != request.lineage_authority_id
            or receipt.lineage_id != request.lineage_id
            or receipt.next_head_hash != next_head
            or receipt.sequence != 1
            or receipt.claim_binding_hash != claim.binding_hash
        ):
            raise SourceBrokerV2SagaIntegrityError("stored lineage receipt was rebound")
        try:
            graph = self._production_graph
            if graph is not None:
                graph.lineage.verify_current(
                    replay_authority_id=request.lineage_authority_id,
                    lineage_id=request.lineage_id,
                    head_hash=next_head,
                    sequence=1,
                    receipt=receipt,
                )
            else:
                verify_current = getattr(self._lineage_authority, "verify_current", None)
                if not callable(verify_current):
                    raise SourceBrokerV2SagaUnavailableError(
                        "lineage authority cannot verify its native receipt"
                    )
                verify_current(
                    replay_authority_id=request.lineage_authority_id,
                    lineage_id=request.lineage_id,
                    head_hash=next_head,
                    sequence=1,
                    receipt=receipt,
                )
        except (ConnectionError, OSError, TimeoutError) as exc:
            raise SourceBrokerV2SagaUnavailableError(
                "lineage authority verification is unavailable"
            ) from exc
        except Exception as exc:
            raise SourceBrokerV2SagaIntegrityError(
                "stored lineage receipt conflicts with native authority"
            ) from exc
        return raw

    def _ensure_source_window(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
    ) -> tuple[datetime, datetime]:
        now = datetime.now(UTC)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_outbox(
                    connection,
                    operation_id=operation_id,
                    phase=phase,
                )
                deadline = _optional_executor_time(
                    row["max_external_deadline"],
                    label="source maximum external deadline",
                )
                takeover_at = _optional_executor_time(
                    row["not_before_takeover_at"],
                    label="source takeover boundary",
                )
                if deadline is None and takeover_at is None:
                    deadline = now + timedelta(seconds=self._source_request_deadline_seconds)
                    takeover_at = deadline + timedelta(seconds=self._source_takeover_grace_seconds)
                    updated = connection.execute(
                        "UPDATE source_broker_v2_outbox SET max_external_deadline = ?, "
                        "not_before_takeover_at = ? WHERE operation_id = ? "
                        "AND status = 'pending' AND executor_owner_token = ? "
                        "AND executor_generation = ?",
                        (
                            deadline.isoformat(),
                            takeover_at.isoformat(),
                            operation_id,
                            self._executor_owner_token,
                            owner_generation,
                        ),
                    ).rowcount
                    if updated != 1:
                        raise SourceBrokerV2SagaConflictError(
                            "outbox executor lost ownership before source claim"
                        )
                elif deadline is None or takeover_at is None:
                    raise SourceBrokerV2SagaIntegrityError(
                        "source external timing evidence is incomplete"
                    )
                connection.commit()
                return deadline, takeover_at
            except BaseException:
                connection.rollback()
                raise

    def _source_claim_once(
        self,
        *,
        request: SourceBrokerV2SagaRequest,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
        owner_generation: int,
    ) -> SourceBrokerV2ClaimOnceResponse:
        deadline, takeover_at = self._ensure_source_window(
            phase=phase,
            operation_id=operation_id,
            owner_generation=owner_generation,
        )
        attempt = request.claim_issue.binding.attempt_binding
        claim_request = SourceBrokerV2ClaimOnceRequest(
            saga_id=self.saga_id,
            operation_id=operation_id,
            phase=phase,
            operation_request_hash=operation_request_hash,
            challenge=self._source_challenge(operation_id=operation_id, phase=phase),
            claim_binding_hash=request.claim_issue.binding_hash,
            claim_generation=attempt.claim_generation,
            scheduler_fencing_token=attempt.scheduler_fencing_token,
            executor_owner_token_hash=canonical_sha256(
                {"executor_owner_token": self._executor_owner_token}
            ),
            executor_generation=owner_generation,
            max_external_deadline=deadline,
            not_before_takeover_at=takeover_at,
        )
        response = self._invoke_source_claim_once(claim_request)
        self._persist_source_claim(
            phase=phase,
            operation_id=operation_id,
            owner_generation=owner_generation,
            response=response,
        )
        return response

    def _invoke_source_claim_once(
        self,
        request: SourceBrokerV2ClaimOnceRequest,
    ) -> SourceBrokerV2ClaimOnceResponse:
        try:
            raw = self._transport.claim_once(canonical_model_json_bytes(request))
            response = strict_model_validate_canonical_json(
                SourceBrokerV2ClaimOnceResponse,
                raw,
            )
        except SourceBrokerV2SagaError:
            raise
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError(
                "source claim_once response is malformed"
            ) from exc
        except Exception as exc:
            raise SourceBrokerV2SagaReconcileRequiredError(
                "source claim_once authority is unavailable; dispatch is forbidden"
            ) from exc
        if (
            response.saga_id != request.saga_id
            or response.operation_id != request.operation_id
            or response.phase is not request.phase
            or response.request_hash != request.request_hash
            or response.operation_request_hash != request.operation_request_hash
            or response.challenge != request.challenge
            or response.claim_binding_hash != request.claim_binding_hash
            or response.claim_generation != request.claim_generation
            or response.scheduler_fencing_token != request.scheduler_fencing_token
            or response.executor_owner_token_hash != request.executor_owner_token_hash
            or response.executor_generation != request.executor_generation
            or response.max_external_deadline != request.max_external_deadline
            or response.not_before_takeover_at != request.not_before_takeover_at
        ):
            raise SourceBrokerV2SagaIntegrityError("source claim_once response binding is invalid")
        self._source_authority_keyring.require_verified_claim(
            request=request,
            receipt=response,
        )
        self._validate_source_terminal_result(
            phase=request.phase,
            operation_id=request.operation_id,
            operation_request_hash=request.operation_request_hash,
            response=response,
        )
        return response

    def _persist_source_claim(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
        response: SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        raw = canonical_model_json_bytes(response)
        receipt_hash = response.receipt_hash
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_outbox(
                    connection,
                    operation_id=operation_id,
                    phase=phase,
                )
                if row["status"] != "pending":
                    raise SourceBrokerV2SagaConflictError(
                        "cannot attach source claim to an applied outbox"
                    )
                if (
                    row["executor_owner_token"] != self._executor_owner_token
                    or row["executor_generation"] != owner_generation
                ):
                    raise SourceBrokerV2SagaConflictError(
                        "outbox executor lost ownership before source claim persistence"
                    )
                existing = connection.execute(
                    "SELECT receipt_json FROM source_broker_v2_source_receipt "
                    "WHERE receipt_hash = ?",
                    (receipt_hash,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO source_broker_v2_source_receipt("
                        "receipt_hash, operation_id, saga_id, phase, status, receipt_json"
                        ") VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            receipt_hash,
                            operation_id,
                            self.saga_id,
                            phase.value,
                            response.status.value,
                            raw.decode("utf-8"),
                        ),
                    )
                elif existing["receipt_json"] != raw.decode("utf-8"):
                    raise SourceBrokerV2SagaIntegrityError("source receipt hash was rebound")
                assignments = "source_observation_json = ?, source_observation_hash = ?"
                values: list[object] = [raw.decode("utf-8"), receipt_hash]
                if response.status is SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT:
                    assignments += ", source_grant_json = ?, source_grant_hash = ?"
                    values.extend((raw.decode("utf-8"), receipt_hash))
                values.extend(
                    (
                        operation_id,
                        self._executor_owner_token,
                        owner_generation,
                    )
                )
                updated = connection.execute(
                    f"UPDATE source_broker_v2_outbox SET {assignments} "
                    "WHERE operation_id = ? AND status = 'pending' "
                    "AND executor_owner_token = ? AND executor_generation = ?",
                    tuple(values),
                ).rowcount
                if updated != 1:
                    raise SourceBrokerV2SagaConflictError(
                        "outbox executor lost ownership while persisting source claim"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _validate_source_terminal_result(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
        response: SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        if response.status not in {
            SourceBrokerV2ClaimStatus.SUCCESS,
            SourceBrokerV2ClaimStatus.FAILURE,
        }:
            return
        if response.result is None:
            raise SourceBrokerV2SagaIntegrityError("terminal source claim omitted its result")
        try:
            if phase is SourceBrokerV2OutboxPhase.DISPATCH:
                result = strict_model_validate_canonical_json(
                    SourceBrokerV2DispatchResponse,
                    response.result,
                )
                expected = SourceBrokerV2ClaimStatus(result.outcome.value)
                if response.status is not expected:
                    raise ValueError("dispatch terminal status conflicts with its result")
            else:
                result = strict_model_validate_canonical_json(
                    SourceBrokerV2FinalizeResponse,
                    response.result,
                )
                if response.status is not SourceBrokerV2ClaimStatus.SUCCESS:
                    raise ValueError("source finalize cannot terminalize as failure")
            if (
                result.saga_id != self.saga_id
                or result.operation_id != operation_id
                or result.request_hash != operation_request_hash
            ):
                raise ValueError("terminal source result is foreign")
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError("source terminal result is invalid") from exc

    def _replay_source_operation(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
    ) -> bytes | None:
        request = SourceBrokerV2ReplayRequest(
            saga_id=self.saga_id,
            operation_id=operation_id,
            phase=phase,
            operation_request_hash=operation_request_hash,
            challenge=self._source_challenge(operation_id=operation_id, phase=phase),
        )
        try:
            raw = self._transport.replay(canonical_model_json_bytes(request))
            response = strict_model_validate_canonical_json(SourceBrokerV2ReplayResponse, raw)
        except SourceBrokerV2SagaError:
            raise
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError("source replay response is malformed") from exc
        except Exception as exc:
            raise SourceBrokerV2SagaUnavailableError(
                "source replay authority is unavailable"
            ) from exc
        if (
            response.saga_id != self.saga_id
            or response.operation_id != operation_id
            or response.phase is not phase
            or response.request_hash != request.request_hash
            or response.challenge != request.challenge
        ):
            raise SourceBrokerV2SagaIntegrityError("source replay response binding is invalid")
        self._source_authority_keyring.require_verified_replay(
            request=request,
            receipt=response,
        )
        if response.status is SourceBrokerV2ReplayStatus.UNKNOWN:
            raise SourceBrokerV2SagaReconcileRequiredError(
                "source authority cannot determine the operation outcome"
            )
        return response.result

    def _verify_source_result(
        self,
        *,
        stored: bytes,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
    ) -> bytes:
        with self._connect() as connection:
            row = self._read_outbox(
                connection,
                operation_id=operation_id,
                phase=phase,
            )
            raw_observation = row["source_observation_json"]
        if type(raw_observation) is not str:
            raise SourceBrokerV2SagaRepairRequiredError(
                f"applied {phase.value} lacks a native source observation"
            )
        try:
            prior = strict_model_validate_canonical_json(
                SourceBrokerV2ClaimOnceResponse,
                raw_observation.encode("utf-8"),
            )
            lookup = SourceBrokerV2ClaimOnceRequest(
                saga_id=prior.saga_id,
                operation_id=prior.operation_id,
                phase=prior.phase,
                operation_request_hash=prior.operation_request_hash,
                challenge=self._source_challenge(
                    operation_id=prior.operation_id,
                    phase=prior.phase,
                ),
                claim_binding_hash=prior.claim_binding_hash,
                claim_generation=prior.claim_generation,
                scheduler_fencing_token=prior.scheduler_fencing_token,
                executor_owner_token_hash=prior.executor_owner_token_hash,
                executor_generation=prior.executor_generation,
                max_external_deadline=prior.max_external_deadline,
                not_before_takeover_at=prior.not_before_takeover_at,
            )
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError(
                "stored source observation cannot be replayed"
            ) from exc
        response = self._invoke_source_claim_once(lookup)
        if response.status not in {
            SourceBrokerV2ClaimStatus.SUCCESS,
            SourceBrokerV2ClaimStatus.FAILURE,
        }:
            raise SourceBrokerV2SagaRepairRequiredError(
                f"source authority is not terminal for applied {phase.value} operation"
            )
        if response.result != stored:
            raise SourceBrokerV2SagaIntegrityError(
                f"stored {phase.value} receipt conflicts with source authority"
            )
        self._persist_applied_source_observation(
            phase=phase,
            operation_id=operation_id,
            response=response,
        )
        return stored

    def _source_challenge(
        self,
        *,
        operation_id: str,
        phase: SourceBrokerV2OutboxPhase,
    ) -> str:
        return canonical_sha256(
            {
                "contract": "rquant-source-authority-challenge/v2",
                "nonce": uuid4().hex,
                "operation_id": operation_id,
                "phase": phase.value,
                "saga_id": self.saga_id,
            }
        )

    def _persist_applied_source_observation(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        response: SourceBrokerV2ClaimOnceResponse,
    ) -> None:
        raw = canonical_model_json_bytes(response)
        receipt_hash = response.receipt_hash
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_outbox(
                    connection,
                    operation_id=operation_id,
                    phase=phase,
                )
                if row["status"] != "applied":
                    raise SourceBrokerV2SagaConflictError(
                        "source verification raced an unapplied outbox"
                    )
                existing = connection.execute(
                    "SELECT receipt_json FROM source_broker_v2_source_receipt "
                    "WHERE receipt_hash = ?",
                    (receipt_hash,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO source_broker_v2_source_receipt("
                        "receipt_hash, operation_id, saga_id, phase, status, receipt_json"
                        ") VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            receipt_hash,
                            operation_id,
                            self.saga_id,
                            phase.value,
                            response.status.value,
                            raw.decode("utf-8"),
                        ),
                    )
                elif existing["receipt_json"] != raw.decode("utf-8"):
                    raise SourceBrokerV2SagaIntegrityError(
                        "source verification receipt hash was rebound"
                    )
                connection.execute(
                    "UPDATE source_broker_v2_outbox SET source_observation_json = ?, "
                    "source_observation_hash = ? WHERE operation_id = ? AND status = 'applied'",
                    (raw.decode("utf-8"), receipt_hash, operation_id),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _initialize(self) -> None:
        with (
            self._schema_init_lock(),
            self._schema_connection() as connection,
        ):
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS source_broker_v2_saga (
                        saga_id TEXT PRIMARY KEY,
                        request_json TEXT NOT NULL,
                        request_hash TEXT NOT NULL,
                        state TEXT NOT NULL,
                        dispatch_outcome TEXT,
                        reconcile_reason TEXT
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS source_broker_v2_outbox (
                        operation_id TEXT PRIMARY KEY,
                        saga_id TEXT NOT NULL,
                        phase TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        payload_hash TEXT NOT NULL,
                        idempotency_hash TEXT NOT NULL,
                        status TEXT NOT NULL,
                        result_json TEXT,
                        result_hash TEXT,
                        executor_owner_token TEXT,
                        executor_generation INTEGER NOT NULL DEFAULT 0,
                        executor_lease_expires_at TEXT,
                        executor_heartbeat_at TEXT,
                        invoke_started INTEGER NOT NULL DEFAULT 0,
                        dispatch_started_at TEXT,
                        max_external_deadline TEXT,
                        not_before_takeover_at TEXT,
                        source_grant_json TEXT,
                        source_grant_hash TEXT,
                        source_observation_json TEXT,
                        source_observation_hash TEXT,
                        UNIQUE(saga_id, phase),
                        FOREIGN KEY(saga_id) REFERENCES source_broker_v2_saga(saga_id)
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS source_broker_v2_outbox_saga_phase "
                    "ON source_broker_v2_outbox(saga_id, phase)"
                )
                columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(source_broker_v2_outbox)"
                    ).fetchall()
                }
                for name in (
                    "dispatch_started_at",
                    "max_external_deadline",
                    "not_before_takeover_at",
                    "source_grant_json",
                    "source_grant_hash",
                    "source_observation_json",
                    "source_observation_hash",
                ):
                    if name not in columns:
                        connection.execute(
                            f"ALTER TABLE source_broker_v2_outbox ADD COLUMN {name} TEXT"
                        )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS source_broker_v2_source_receipt (
                        receipt_hash TEXT PRIMARY KEY,
                        operation_id TEXT NOT NULL,
                        saga_id TEXT NOT NULL,
                        phase TEXT NOT NULL,
                        status TEXT NOT NULL,
                        receipt_json TEXT NOT NULL,
                        FOREIGN KEY(operation_id)
                            REFERENCES source_broker_v2_outbox(operation_id),
                        FOREIGN KEY(saga_id)
                            REFERENCES source_broker_v2_saga(saga_id)
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS source_broker_v2_source_receipt_operation "
                    "ON source_broker_v2_source_receipt(operation_id, receipt_hash)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS source_broker_v2_source_receipt_saga "
                    "ON source_broker_v2_source_receipt(saga_id, operation_id)"
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS source_broker_v2_production_binding (
                        saga_id TEXT PRIMARY KEY,
                        binding_hash TEXT NOT NULL CHECK(length(binding_hash) = 64)
                    )
                    """
                )
                self._bind_production_graph(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @contextmanager
    def _schema_init_lock(self) -> Iterator[None]:
        lock_path = self.path.with_name(f".{self.path.name}.init.lock")
        descriptor = os.open(
            lock_path,
            os.O_CLOEXEC | os.O_CREAT | os.O_NOFOLLOW | os.O_RDWR,
            0o600,
        )
        deadline = time.monotonic() + self._busy_timeout_ms / 1_000
        locked = False
        try:
            while not locked:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    locked = True
                except BlockingIOError as exc:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise SourceBrokerV2SagaUnavailableError(
                            "timed out waiting for the V2 saga schema initialization lock"
                        ) from exc
                    time.sleep(min(0.01, remaining))
            yield
        finally:
            if locked:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @contextmanager
    def _schema_connection(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            yield connection

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.path, timeout=self._busy_timeout_ms / 1_000, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        connection.execute("PRAGMA synchronous = FULL")
        try:
            yield connection
        finally:
            connection.close()

    def _require_request(self, request: SourceBrokerV2SagaRequest) -> SourceBrokerV2SagaRequest:
        try:
            validated = SourceBrokerV2SagaRequest.model_validate(request, strict=True)
            raw = canonical_model_json_bytes(validated)
            decoded = strict_model_validate_canonical_json(SourceBrokerV2SagaRequest, raw)
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError("saga request is malformed") from exc
        if decoded != validated or decoded.saga_id != self.saga_id:
            raise SourceBrokerV2SagaConflictError("saga request identity conflicts with this owner")
        if decoded.claim_issue.binding.operation_id != self._operation_id(
            SourceBrokerV2OutboxPhase.CLAIM
        ):
            raise SourceBrokerV2SagaConflictError("current-claim operation id is not deterministic")
        return decoded

    def _require_live_production_graph(self) -> None:
        graph = self._production_graph
        if graph is None:
            return
        original = graph.require_live()
        if (
            self._current_claim_authority is not original.current_claim
            or self._lineage_authority is not original.lineage
            or self._transport is not original.source
            or self._source_authority_keyring is not original.source_keyring
            or type(self._quota_adapter) is not _UnixSourceQuotaAdapter
            or self._quota_adapter._client is not original.quota
        ):
            raise TypeError("production saga graph was replaced after construction")

    def _bind_production_graph(self, connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT binding_hash FROM source_broker_v2_production_binding WHERE saga_id = ?",
            (self.saga_id,),
        ).fetchone()
        expected = self._production_binding_hash
        if expected is None:
            if row is not None:
                raise SourceBrokerV2SagaIntegrityError(
                    "production graph binding cannot be reopened by a nonproduction saga"
                )
            return
        if row is None:
            existing_saga = connection.execute(
                "SELECT 1 FROM source_broker_v2_saga WHERE saga_id = ?",
                (self.saga_id,),
            ).fetchone()
            if existing_saga is not None:
                raise SourceBrokerV2SagaIntegrityError(
                    "production graph binding is missing for an existing saga"
                )
            connection.execute(
                "INSERT INTO source_broker_v2_production_binding(saga_id, binding_hash) "
                "VALUES (?, ?)",
                (self.saga_id, expected),
            )
        elif row["binding_hash"] != expected:
            raise SourceBrokerV2SagaIntegrityError(
                "production graph binding conflicts with the durable saga binding"
            )

    def _ensure_saga(self, request: SourceBrokerV2SagaRequest) -> SourceBrokerV2SagaSnapshot:
        raw = canonical_model_json_bytes(request)
        request_hash = request.request_hash
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT request_json, request_hash, state, dispatch_outcome, reconcile_reason "
                    "FROM source_broker_v2_saga WHERE saga_id = ?",
                    (self.saga_id,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        "INSERT INTO source_broker_v2_saga("
                        "saga_id, request_json, request_hash, state, "
                        "dispatch_outcome, reconcile_reason"
                        ") VALUES (?, ?, ?, ?, NULL, NULL)",
                        (
                            self.saga_id,
                            raw.decode("utf-8"),
                            request_hash,
                            SourceBrokerV2SagaState.CLAIMED.value,
                        ),
                    )
                    row = connection.execute(
                        "SELECT request_json, request_hash, state, "
                        "dispatch_outcome, reconcile_reason "
                        "FROM source_broker_v2_saga WHERE saga_id = ?",
                        (self.saga_id,),
                    ).fetchone()
                if row is None:
                    raise SourceBrokerV2SagaIntegrityError("saga insert disappeared")
                if type(row["request_json"]) is not str or type(row["request_hash"]) is not str:
                    raise SourceBrokerV2SagaIntegrityError("saga storage types are invalid")
                stored = strict_model_validate_canonical_json(
                    SourceBrokerV2SagaRequest,
                    row["request_json"].encode("utf-8"),
                )
                if stored != request or row["request_hash"] != request_hash:
                    raise SourceBrokerV2SagaConflictError("saga request was rebound or rolled back")
                snapshot = self._snapshot_from_row(row)
                self._validate_outbox_chain(connection, snapshot)
                connection.commit()
                return snapshot
            except BaseException:
                connection.rollback()
                raise

    def _operation_id(self, phase: SourceBrokerV2OutboxPhase) -> str:
        return canonical_sha256(
            {"contract": SOURCE_BROKER_V2_CONTRACT, "phase": phase.value, "saga_id": self.saga_id}
        )

    def _apply_outbox(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        payload: bytes,
        invoke: Callable[[bytes], bytes],
        recover: Callable[[bytes], bytes | None] | None = None,
        verify: Callable[[bytes], bytes] | None = None,
        authorize: Callable[[bytes, int, bool], tuple[bytes | None, bytes | None]] | None = None,
    ) -> bytes:
        _require_canonical_json_bytes(payload, label="V2 outbox payload")
        payload_hash = canonical_sha256(strict_canonical_json_loads(payload))
        idempotency_hash = _outbox_payload_hash(payload)
        existing = self._begin_outbox(
            phase=phase,
            operation_id=operation_id,
            payload=payload,
            payload_hash=payload_hash,
            idempotency_hash=idempotency_hash,
        )
        if existing is not None:
            return existing if verify is None else verify(existing)
        owner_generation, persisted_payload, invoke_started = self._acquire_outbox_lease(
            phase=phase,
            operation_id=operation_id,
        )
        if owner_generation == -1:
            return persisted_payload if verify is None else verify(persisted_payload)
        try:
            invoke_payload: bytes | None = persisted_payload
            if authorize is not None:
                result, invoke_payload = authorize(
                    persisted_payload,
                    owner_generation,
                    invoke_started,
                )
            else:
                result = recover(persisted_payload) if recover is not None else None
            if result is None:
                if invoke_payload is None:
                    raise SourceBrokerV2SagaIntegrityError(
                        f"{phase.value} authorization omitted its invocation envelope"
                    )
                self._mark_invoke_started(
                    phase=phase,
                    operation_id=operation_id,
                    owner_generation=owner_generation,
                )
                self._heartbeat_outbox(
                    phase=phase,
                    operation_id=operation_id,
                    owner_generation=owner_generation,
                )
                result = self._invoke_with_heartbeat(
                    phase=phase,
                    operation_id=operation_id,
                    owner_generation=owner_generation,
                    payload=invoke_payload,
                    invoke=invoke,
                )
            elif authorize is None and not invoke_started:
                self._mark_invoke_started(
                    phase=phase,
                    operation_id=operation_id,
                    owner_generation=owner_generation,
                )
            _require_canonical_json_bytes(result, label="V2 outbox result")
            result_hash = canonical_sha256(strict_canonical_json_loads(result))
            self._heartbeat_outbox(
                phase=phase,
                operation_id=operation_id,
                owner_generation=owner_generation,
            )
            self._after_external_effect(phase)
        except Exception:
            self._release_outbox_lease(
                phase=phase,
                operation_id=operation_id,
                owner_generation=owner_generation,
            )
            raise
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_outbox(connection, operation_id=operation_id, phase=phase)
                if row["status"] == "applied":
                    recovered = self._outbox_result(row)
                    connection.commit()
                    return recovered
                if row["idempotency_hash"] != idempotency_hash:
                    raise SourceBrokerV2SagaIntegrityError("outbox payload changed during apply")
                updated = connection.execute(
                    "UPDATE source_broker_v2_outbox SET status = 'applied', "
                    "result_json = ?, result_hash = ? "
                    "WHERE operation_id = ? AND status = 'pending' "
                    "AND executor_owner_token = ? AND executor_generation = ?",
                    (
                        result.decode("utf-8"),
                        result_hash,
                        operation_id,
                        self._executor_owner_token,
                        owner_generation,
                    ),
                ).rowcount
                if updated != 1:
                    raise SourceBrokerV2SagaConflictError("outbox ownership changed during apply")
                connection.commit()
                return result if verify is None else verify(result)
            except BaseException:
                connection.rollback()
                raise

    def _stored_outbox_payload(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
    ) -> bytes:
        with self._connect() as connection:
            row = self._read_outbox(
                connection,
                operation_id=operation_id,
                phase=phase,
            )
            payload_json = row["payload_json"]
        if type(payload_json) is not str:
            raise SourceBrokerV2SagaIntegrityError("stored outbox payload is unavailable")
        payload = payload_json.encode("utf-8")
        _require_canonical_json_bytes(payload, label="stored V2 outbox payload")
        return payload

    def _after_external_effect(self, phase: SourceBrokerV2OutboxPhase) -> None:
        """Test seam for the exact commit-before-response-loss boundary."""

    def _before_external_effect(self, phase: SourceBrokerV2OutboxPhase) -> None:
        """Test seam after durable ownership but before an external invocation."""

    def _wait_for_heartbeat(
        self,
        stop: Event,
        interval: float,
        *,
        phase: SourceBrokerV2OutboxPhase,
    ) -> bool:
        del phase
        return stop.wait(interval)

    def _invoke_with_heartbeat(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
        payload: bytes,
        invoke: Callable[[bytes], bytes],
    ) -> bytes:
        self._before_external_effect(phase)
        stop = Event()
        failures: list[BaseException] = []
        interval = self._executor_lease_seconds / 3

        def renew() -> None:
            while not self._wait_for_heartbeat(stop, interval, phase=phase):
                try:
                    self._heartbeat_outbox(
                        phase=phase,
                        operation_id=operation_id,
                        owner_generation=owner_generation,
                    )
                except BaseException as exc:
                    failures.append(exc)
                    stop.set()
                    return

        heartbeat = Thread(
            target=renew,
            name=f"rquant-source-broker-v2-heartbeat-{operation_id[:12]}",
            daemon=True,
        )
        heartbeat.start()
        try:
            result = invoke(payload)
        finally:
            stop.set()
            heartbeat.join(timeout=max(0.1, interval * 2))
        if heartbeat.is_alive():
            raise SourceBrokerV2SagaUnavailableError(
                f"outbox heartbeat did not stop after {phase.value}"
            )
        if failures:
            raise SourceBrokerV2SagaUnavailableError(
                f"outbox heartbeat failed during {phase.value}"
            ) from failures[0]
        return result

    def _begin_outbox(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        payload: bytes,
        payload_hash: str,
        idempotency_hash: str,
    ) -> bytes | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM source_broker_v2_outbox WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        "INSERT INTO source_broker_v2_outbox("
                        "operation_id, saga_id, phase, payload_json, payload_hash, "
                        "idempotency_hash, status, result_json, result_hash, "
                        "executor_owner_token, executor_generation, "
                        "executor_lease_expires_at, executor_heartbeat_at, invoke_started"
                        ") VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, NULL, "
                        "NULL, 0, NULL, NULL, 0)",
                        (
                            operation_id,
                            self.saga_id,
                            phase.value,
                            payload.decode("utf-8"),
                            payload_hash,
                            idempotency_hash,
                        ),
                    )
                    connection.commit()
                    return None
                self._validate_outbox_row(
                    row, expected_phase=phase, expected_operation_id=operation_id
                )
                if row["idempotency_hash"] != idempotency_hash:
                    raise SourceBrokerV2SagaConflictError("outbox operation was rebound")
                if row["status"] == "applied":
                    result = self._outbox_result(row)
                    connection.commit()
                    return result
                connection.commit()
                return None
            except BaseException:
                connection.rollback()
                raise

    def _acquire_outbox_lease(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
    ) -> tuple[int, bytes, bool]:
        deadline = time.monotonic() + self._executor_wait_seconds
        while True:
            current_time = datetime.now(UTC)
            lease_expires_at = current_time + timedelta(seconds=self._executor_lease_seconds)
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    row = self._read_outbox(
                        connection,
                        operation_id=operation_id,
                        phase=phase,
                    )
                    if row["status"] == "applied":
                        result = self._outbox_result(row)
                        connection.commit()
                        return -1, result, True
                    owner = row["executor_owner_token"]
                    stored_expiry = _optional_executor_time(
                        row["executor_lease_expires_at"],
                        label="executor lease expiry",
                    )
                    can_acquire = (
                        owner is None
                        or owner == self._executor_owner_token
                        or stored_expiry is None
                        or stored_expiry <= current_time
                    )
                    if can_acquire:
                        generation = int(row["executor_generation"])
                        if owner != self._executor_owner_token:
                            generation += 1
                        updated = connection.execute(
                            "UPDATE source_broker_v2_outbox SET executor_owner_token = ?, "
                            "executor_generation = ?, executor_lease_expires_at = ?, "
                            "executor_heartbeat_at = ? WHERE operation_id = ? "
                            "AND status = 'pending' AND executor_generation = ?",
                            (
                                self._executor_owner_token,
                                generation,
                                lease_expires_at.isoformat(),
                                current_time.isoformat(),
                                operation_id,
                                row["executor_generation"],
                            ),
                        ).rowcount
                        if updated == 1:
                            payload_json = row["payload_json"]
                            if type(payload_json) is not str:
                                raise SourceBrokerV2SagaIntegrityError(
                                    "pending outbox payload is unavailable"
                                )
                            payload = payload_json.encode("utf-8")
                            _require_canonical_json_bytes(
                                payload,
                                label="pending V2 outbox payload",
                            )
                            connection.commit()
                            return generation, payload, bool(row["invoke_started"])
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            if time.monotonic() >= deadline:
                raise SourceBrokerV2SagaUnavailableError(
                    f"outbox executor lease is held during {phase.value}"
                )
            time.sleep(min(0.01, self._executor_wait_seconds))

    def _mark_invoke_started(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_outbox(
                    connection,
                    operation_id=operation_id,
                    phase=phase,
                )
                if row["status"] == "applied":
                    connection.commit()
                    return
                if phase in {
                    SourceBrokerV2OutboxPhase.DISPATCH,
                    SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                }:
                    updated = connection.execute(
                        "UPDATE source_broker_v2_outbox SET invoke_started = 1, "
                        "dispatch_started_at = COALESCE(dispatch_started_at, ?) "
                        "WHERE operation_id = ? AND status = 'pending' "
                        "AND executor_owner_token = ? AND executor_generation = ?",
                        (
                            datetime.now(UTC).isoformat(),
                            operation_id,
                            self._executor_owner_token,
                            owner_generation,
                        ),
                    ).rowcount
                else:
                    updated = connection.execute(
                        "UPDATE source_broker_v2_outbox SET invoke_started = 1 "
                        "WHERE operation_id = ? AND status = 'pending' "
                        "AND executor_owner_token = ? AND executor_generation = ?",
                        (operation_id, self._executor_owner_token, owner_generation),
                    ).rowcount
                if updated != 1:
                    raise SourceBrokerV2SagaConflictError(
                        "outbox executor lost ownership before invocation"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _heartbeat_outbox(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
    ) -> None:
        current_time = datetime.now(UTC)
        expires_at = current_time + timedelta(seconds=self._executor_lease_seconds)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._read_outbox(
                    connection,
                    operation_id=operation_id,
                    phase=phase,
                )
                updated = connection.execute(
                    "UPDATE source_broker_v2_outbox SET executor_heartbeat_at = ?, "
                    "executor_lease_expires_at = ? WHERE operation_id = ? "
                    "AND status = 'pending' AND executor_owner_token = ? "
                    "AND executor_generation = ?",
                    (
                        current_time.isoformat(),
                        expires_at.isoformat(),
                        operation_id,
                        self._executor_owner_token,
                        owner_generation,
                    ),
                ).rowcount
                if updated != 1:
                    raise SourceBrokerV2SagaConflictError(
                        "outbox executor lost ownership before heartbeat"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _release_outbox_lease(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._read_outbox(
                    connection,
                    operation_id=operation_id,
                    phase=phase,
                )
                connection.execute(
                    "UPDATE source_broker_v2_outbox SET executor_owner_token = NULL, "
                    "executor_lease_expires_at = NULL WHERE operation_id = ? "
                    "AND status = 'pending' AND executor_owner_token = ? "
                    "AND executor_generation = ?",
                    (operation_id, self._executor_owner_token, owner_generation),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _transition(
        self,
        state: SourceBrokerV2SagaState,
        *,
        outcome: SourceBrokerV2DispatchOutcome | None = None,
        reconcile_reason: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT request_hash, state, dispatch_outcome, reconcile_reason "
                    "FROM source_broker_v2_saga WHERE saga_id = ?",
                    (self.saga_id,),
                ).fetchone()
                if row is None:
                    raise SourceBrokerV2SagaConflictError("saga is missing")
                current = self._snapshot_from_row(row)
                if current.state == state:
                    if outcome not in {None, current.dispatch_outcome}:
                        raise SourceBrokerV2SagaConflictError("saga outcome was rebound")
                    connection.commit()
                    return
                if (
                    current.state
                    not in {
                        SourceBrokerV2SagaState.RECONCILE_REQUIRED,
                        SourceBrokerV2SagaState.DISPATCH_UNKNOWN,
                    }
                    and state in _STATE_RANK
                    and _STATE_RANK.get(current.state, -1) > _STATE_RANK.get(state, -1)
                ):
                    connection.commit()
                    return
                if state not in _FORWARD_STATES[current.state]:
                    raise SourceBrokerV2SagaConflictError(
                        f"illegal V2 saga transition {current.state.value}->{state.value}"
                    )
                if current.dispatch_outcome is not None and outcome not in {
                    None,
                    current.dispatch_outcome,
                }:
                    raise SourceBrokerV2SagaConflictError("saga dispatch outcome was rebound")
                stored_outcome = current.dispatch_outcome or outcome
                if (
                    state
                    in {
                        SourceBrokerV2SagaState.DISPATCH_OUTCOME,
                        SourceBrokerV2SagaState.SOURCE_FINALIZED,
                        SourceBrokerV2SagaState.QUOTA_TERMINAL,
                        SourceBrokerV2SagaState.PARENT_RELEASED,
                        SourceBrokerV2SagaState.LINEAGE_PUBLISHED,
                        SourceBrokerV2SagaState.COMPLETE,
                    }
                    and stored_outcome is None
                    and not (
                        state is SourceBrokerV2SagaState.COMPLETE
                        and current.state is SourceBrokerV2SagaState.COMPENSATED
                    )
                    and not (
                        state is SourceBrokerV2SagaState.PARENT_RELEASED
                        and current.state is SourceBrokerV2SagaState.CALL_TERMINALIZED
                    )
                ):
                    raise SourceBrokerV2SagaIntegrityError(
                        "post-dispatch state lacks outcome evidence"
                    )
                connection.execute(
                    "UPDATE source_broker_v2_saga SET state = ?, "
                    "dispatch_outcome = ?, reconcile_reason = ? "
                    "WHERE saga_id = ?",
                    (
                        state.value,
                        None if stored_outcome is None else stored_outcome.value,
                        reconcile_reason
                        if reconcile_reason is not None
                        else current.reconcile_reason,
                        self.saga_id,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _read_outbox(
        self,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        phase: SourceBrokerV2OutboxPhase,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM source_broker_v2_outbox WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        if row is None:
            raise SourceBrokerV2SagaIntegrityError("required outbox effect is missing")
        self._validate_outbox_row(row, expected_phase=phase, expected_operation_id=operation_id)
        return row

    def _outbox_result(self, row: sqlite3.Row) -> bytes:
        self._validate_outbox_row(
            row,
            expected_phase=SourceBrokerV2OutboxPhase(row["phase"]),
            expected_operation_id=row["operation_id"],
        )
        if row["status"] != "applied" or type(row["result_json"]) is not str:
            raise SourceBrokerV2SagaIntegrityError("outbox does not have a durable result")
        raw = row["result_json"].encode("utf-8")
        _require_canonical_json_bytes(raw, label="stored V2 outbox result")
        if row["result_hash"] != canonical_sha256(strict_canonical_json_loads(raw)):
            raise SourceBrokerV2SagaIntegrityError("stored V2 outbox result hash conflicts")
        return raw

    def _validate_outbox_row(
        self,
        row: sqlite3.Row,
        *,
        expected_phase: SourceBrokerV2OutboxPhase,
        expected_operation_id: str,
    ) -> None:
        required_text = (
            "operation_id",
            "saga_id",
            "phase",
            "payload_json",
            "payload_hash",
            "idempotency_hash",
            "status",
        )
        if any(type(row[key]) is not str for key in required_text):
            raise SourceBrokerV2SagaIntegrityError("outbox SQLite types are invalid")
        if (
            type(row["executor_generation"]) is not int
            or int(row["executor_generation"]) < 0
            or type(row["invoke_started"]) is not int
            or int(row["invoke_started"]) not in {0, 1}
        ):
            raise SourceBrokerV2SagaIntegrityError("outbox executor SQLite types are invalid")
        owner = row["executor_owner_token"]
        if owner is not None and (type(owner) is not str or not owner):
            raise SourceBrokerV2SagaIntegrityError("outbox executor owner is malformed")
        _optional_executor_time(
            row["executor_lease_expires_at"],
            label="executor lease expiry",
        )
        _optional_executor_time(
            row["executor_heartbeat_at"],
            label="executor heartbeat",
        )
        dispatch_started = _optional_executor_time(
            row["dispatch_started_at"],
            label="source dispatch start",
        )
        max_deadline = _optional_executor_time(
            row["max_external_deadline"],
            label="source maximum external deadline",
        )
        takeover_at = _optional_executor_time(
            row["not_before_takeover_at"],
            label="source takeover boundary",
        )
        source_phase = expected_phase in {
            SourceBrokerV2OutboxPhase.DISPATCH,
            SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        }
        if source_phase:
            if (max_deadline is None) != (takeover_at is None):
                raise SourceBrokerV2SagaIntegrityError(
                    "source external timing evidence is incomplete"
                )
            if max_deadline is not None and takeover_at is not None:
                if takeover_at < max_deadline:
                    raise SourceBrokerV2SagaIntegrityError(
                        "source takeover boundary precedes its external deadline"
                    )
                if dispatch_started is not None and dispatch_started > max_deadline:
                    raise SourceBrokerV2SagaIntegrityError(
                        "source invocation started after its persisted deadline"
                    )
        elif any(
            row[key] is not None
            for key in (
                "dispatch_started_at",
                "max_external_deadline",
                "not_before_takeover_at",
                "source_grant_json",
                "source_grant_hash",
                "source_observation_json",
                "source_observation_hash",
            )
        ):
            raise SourceBrokerV2SagaIntegrityError(
                "non-source outbox contains source execution evidence"
            )
        for prefix in ("source_grant", "source_observation"):
            raw_value = row[f"{prefix}_json"]
            hash_value = row[f"{prefix}_hash"]
            if (raw_value is None) != (hash_value is None):
                raise SourceBrokerV2SagaIntegrityError(f"{prefix} evidence is incomplete")
            if raw_value is not None:
                if type(raw_value) is not str or type(hash_value) is not str:
                    raise SourceBrokerV2SagaIntegrityError(f"{prefix} SQLite types are invalid")
                raw_claim = raw_value.encode("utf-8")
                _require_canonical_json_bytes(raw_claim, label=prefix)
                if hash_value != canonical_sha256(strict_canonical_json_loads(raw_claim)):
                    raise SourceBrokerV2SagaIntegrityError(f"{prefix} hash conflicts")
                try:
                    claim = strict_model_validate_canonical_json(
                        SourceBrokerV2ClaimOnceResponse,
                        raw_claim,
                    )
                except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
                    raise SourceBrokerV2SagaIntegrityError(
                        f"{prefix} receipt is malformed"
                    ) from exc
                if (
                    claim.saga_id != self.saga_id
                    or claim.operation_id != expected_operation_id
                    or claim.phase is not expected_phase
                ):
                    raise SourceBrokerV2SagaIntegrityError(f"{prefix} receipt is foreign")
                if prefix == "source_grant" and (
                    claim.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
                ):
                    raise SourceBrokerV2SagaIntegrityError(
                        "source grant is not definitive-absence evidence"
                    )
        if row["operation_id"] != expected_operation_id or row["saga_id"] != self.saga_id:
            raise SourceBrokerV2SagaIntegrityError("outbox owner identity conflicts")
        if row["phase"] != expected_phase.value or row["status"] not in {"pending", "applied"}:
            raise SourceBrokerV2SagaIntegrityError("outbox lifecycle state is invalid")
        payload = row["payload_json"].encode("utf-8")
        _require_canonical_json_bytes(payload, label="stored V2 outbox payload")
        if row["payload_hash"] != canonical_sha256(strict_canonical_json_loads(payload)):
            raise SourceBrokerV2SagaIntegrityError("stored V2 outbox payload hash conflicts")
        if row["idempotency_hash"] != _outbox_payload_hash(payload):
            raise SourceBrokerV2SagaIntegrityError("stored V2 outbox idempotency hash conflicts")
        if row["status"] == "pending":
            if row["result_json"] is not None or row["result_hash"] is not None:
                raise SourceBrokerV2SagaIntegrityError("pending outbox unexpectedly has a result")
        elif type(row["result_json"]) is not str or type(row["result_hash"]) is not str:
            raise SourceBrokerV2SagaIntegrityError("applied outbox result SQLite types are invalid")

    def _validate_outbox_chain(
        self,
        connection: sqlite3.Connection,
        snapshot: SourceBrokerV2SagaSnapshot,
    ) -> None:
        rows = connection.execute(
            "SELECT * FROM source_broker_v2_outbox WHERE saga_id = ? ORDER BY phase",
            (self.saga_id,),
        ).fetchall()
        known = {phase.value: phase for phase in SourceBrokerV2OutboxPhase}
        for row in rows:
            phase = known.get(row["phase"] if type(row["phase"]) is str else "")
            if phase is None:
                raise SourceBrokerV2SagaIntegrityError("unknown V2 outbox phase")
            expected = self._operation_id(phase)
            self._validate_outbox_row(row, expected_phase=phase, expected_operation_id=expected)
            if row["status"] == "applied":
                self._outbox_result(row)
        required = _required_phases(snapshot)
        present = {str(row["phase"]): row for row in rows}
        for phase in required:
            row = present.get(phase.value)
            if row is None or row["status"] != "applied":
                raise SourceBrokerV2SagaIntegrityError(
                    f"saga state {snapshot.state.value} lacks applied {phase.value} evidence"
                )
        source_receipts = connection.execute(
            "SELECT receipt_hash, operation_id, phase, status, receipt_json "
            "FROM source_broker_v2_source_receipt WHERE saga_id = ?",
            (self.saga_id,),
        ).fetchall()
        for row in source_receipts:
            if any(
                type(row[key]) is not str
                for key in (
                    "receipt_hash",
                    "operation_id",
                    "phase",
                    "status",
                    "receipt_json",
                )
            ):
                raise SourceBrokerV2SagaIntegrityError("source receipt SQLite types are invalid")
            try:
                receipt = strict_model_validate_canonical_json(
                    SourceBrokerV2ClaimOnceResponse,
                    row["receipt_json"].encode("utf-8"),
                )
                phase = SourceBrokerV2OutboxPhase(row["phase"])
            except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
                raise SourceBrokerV2SagaIntegrityError(
                    "source receipt history is malformed"
                ) from exc
            if (
                row["receipt_hash"] != receipt.receipt_hash
                or row["operation_id"] != receipt.operation_id
                or row["status"] != receipt.status.value
                or receipt.saga_id != self.saga_id
                or receipt.phase is not phase
                or receipt.operation_id != self._operation_id(phase)
            ):
                raise SourceBrokerV2SagaIntegrityError("source receipt history binding is invalid")

    def _snapshot_from_row(self, row: sqlite3.Row) -> SourceBrokerV2SagaSnapshot:
        try:
            if type(row["request_hash"]) is not str or type(row["state"]) is not str:
                raise TypeError("snapshot SQLite types are invalid")
            outcome = row["dispatch_outcome"]
            reason = row["reconcile_reason"]
            if outcome is not None and type(outcome) is not str:
                raise TypeError("snapshot outcome SQLite type is invalid")
            if reason is not None and type(reason) is not str:
                raise TypeError("snapshot reconcile reason SQLite type is invalid")
            return SourceBrokerV2SagaSnapshot(
                saga_id=self.saga_id,
                request_hash=row["request_hash"],
                state=SourceBrokerV2SagaState(row["state"]),
                dispatch_outcome=(
                    None if outcome is None else SourceBrokerV2DispatchOutcome(outcome)
                ),
                reconcile_reason=reason,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise SourceBrokerV2SagaIntegrityError("stored saga snapshot is invalid") from exc


_FORWARD_STATES: dict[SourceBrokerV2SagaState, frozenset[SourceBrokerV2SagaState]] = {
    SourceBrokerV2SagaState.CLAIMED: frozenset(
        {
            SourceBrokerV2SagaState.PARENT_RESERVED,
        }
    ),
    SourceBrokerV2SagaState.PARENT_RESERVED: frozenset(
        {
            SourceBrokerV2SagaState.CALL_INTENT,
        }
    ),
    SourceBrokerV2SagaState.CALL_INTENT: frozenset(
        {
            SourceBrokerV2SagaState.DISPATCH_AUTHORIZED,
            SourceBrokerV2SagaState.CALL_TERMINALIZED,
        }
    ),
    SourceBrokerV2SagaState.CALL_TERMINALIZED: frozenset(
        {
            SourceBrokerV2SagaState.PARENT_RELEASED,
        }
    ),
    SourceBrokerV2SagaState.DISPATCH_AUTHORIZED: frozenset(
        {
            SourceBrokerV2SagaState.DISPATCH_OUTCOME,
            SourceBrokerV2SagaState.DISPATCH_UNKNOWN,
        }
    ),
    SourceBrokerV2SagaState.DISPATCH_UNKNOWN: frozenset(
        {
            SourceBrokerV2SagaState.RECONCILE_REQUIRED,
        }
    ),
    SourceBrokerV2SagaState.DISPATCH_OUTCOME: frozenset(
        {
            SourceBrokerV2SagaState.SOURCE_FINALIZED,
        }
    ),
    SourceBrokerV2SagaState.SOURCE_FINALIZED: frozenset(
        {
            SourceBrokerV2SagaState.QUOTA_TERMINAL,
        }
    ),
    SourceBrokerV2SagaState.QUOTA_TERMINAL: frozenset(
        {
            SourceBrokerV2SagaState.PARENT_RELEASED,
            SourceBrokerV2SagaState.COMPENSATED,
        }
    ),
    SourceBrokerV2SagaState.PARENT_RELEASED: frozenset(
        {
            SourceBrokerV2SagaState.COMPENSATED,
            SourceBrokerV2SagaState.LINEAGE_PUBLISHED,
            SourceBrokerV2SagaState.COMPLETE,
        }
    ),
    SourceBrokerV2SagaState.COMPENSATED: frozenset(
        {
            SourceBrokerV2SagaState.LINEAGE_PUBLISHED,
            SourceBrokerV2SagaState.COMPLETE,
        }
    ),
    SourceBrokerV2SagaState.LINEAGE_PUBLISHED: frozenset(
        {
            SourceBrokerV2SagaState.COMPLETE,
        }
    ),
    SourceBrokerV2SagaState.COMPLETE: frozenset(),
    SourceBrokerV2SagaState.RECONCILE_REQUIRED: frozenset(),
}

_REQUIRED_APPLIED_PHASES: dict[SourceBrokerV2SagaState, tuple[SourceBrokerV2OutboxPhase, ...]] = {
    SourceBrokerV2SagaState.CLAIMED: (),
    SourceBrokerV2SagaState.PARENT_RESERVED: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
    ),
    SourceBrokerV2SagaState.CALL_INTENT: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
    ),
    SourceBrokerV2SagaState.CALL_TERMINALIZED: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.UNKNOWN_BEFORE_DISPATCH,
    ),
    SourceBrokerV2SagaState.DISPATCH_AUTHORIZED: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
    ),
    SourceBrokerV2SagaState.DISPATCH_UNKNOWN: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
    ),
    SourceBrokerV2SagaState.RECONCILE_REQUIRED: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
    ),
    SourceBrokerV2SagaState.DISPATCH_OUTCOME: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
        SourceBrokerV2OutboxPhase.DISPATCH,
    ),
    SourceBrokerV2SagaState.SOURCE_FINALIZED: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
        SourceBrokerV2OutboxPhase.DISPATCH,
        SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
    ),
    SourceBrokerV2SagaState.QUOTA_TERMINAL: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
        SourceBrokerV2OutboxPhase.DISPATCH,
        SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        SourceBrokerV2OutboxPhase.QUOTA_FINALIZE,
    ),
    SourceBrokerV2SagaState.PARENT_RELEASED: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
        SourceBrokerV2OutboxPhase.DISPATCH,
        SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        SourceBrokerV2OutboxPhase.QUOTA_FINALIZE,
        SourceBrokerV2OutboxPhase.RELEASE_UNUSED,
    ),
    SourceBrokerV2SagaState.COMPENSATED: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.UNKNOWN_BEFORE_DISPATCH,
        SourceBrokerV2OutboxPhase.RELEASE_UNUSED,
    ),
    SourceBrokerV2SagaState.LINEAGE_PUBLISHED: (
        SourceBrokerV2OutboxPhase.CLAIM,
        SourceBrokerV2OutboxPhase.RESERVE_PARENT,
        SourceBrokerV2OutboxPhase.RECORD_INTENT,
        SourceBrokerV2OutboxPhase.AUTHORIZE_DISPATCH,
        SourceBrokerV2OutboxPhase.DISPATCH,
        SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
        SourceBrokerV2OutboxPhase.QUOTA_FINALIZE,
        SourceBrokerV2OutboxPhase.RELEASE_UNUSED,
        SourceBrokerV2OutboxPhase.LINEAGE,
    ),
    SourceBrokerV2SagaState.COMPLETE: (),
}

_STATE_RANK: dict[SourceBrokerV2SagaState, int] = {
    SourceBrokerV2SagaState.CLAIMED: 0,
    SourceBrokerV2SagaState.PARENT_RESERVED: 1,
    SourceBrokerV2SagaState.CALL_INTENT: 2,
    SourceBrokerV2SagaState.CALL_TERMINALIZED: 3,
    SourceBrokerV2SagaState.DISPATCH_AUTHORIZED: 3,
    SourceBrokerV2SagaState.DISPATCH_OUTCOME: 4,
    SourceBrokerV2SagaState.SOURCE_FINALIZED: 5,
    SourceBrokerV2SagaState.QUOTA_TERMINAL: 6,
    SourceBrokerV2SagaState.PARENT_RELEASED: 7,
    SourceBrokerV2SagaState.COMPENSATED: 8,
    SourceBrokerV2SagaState.LINEAGE_PUBLISHED: 9,
    SourceBrokerV2SagaState.COMPLETE: 10,
}


def _required_phases(snapshot: SourceBrokerV2SagaSnapshot) -> tuple[SourceBrokerV2OutboxPhase, ...]:
    if snapshot.state is SourceBrokerV2SagaState.COMPLETE:
        if snapshot.dispatch_outcome is None:
            return (
                SourceBrokerV2OutboxPhase.CLAIM,
                SourceBrokerV2OutboxPhase.RESERVE_PARENT,
                SourceBrokerV2OutboxPhase.RECORD_INTENT,
                SourceBrokerV2OutboxPhase.UNKNOWN_BEFORE_DISPATCH,
                SourceBrokerV2OutboxPhase.RELEASE_UNUSED,
            )
        return _REQUIRED_APPLIED_PHASES[SourceBrokerV2SagaState.LINEAGE_PUBLISHED]
    if (
        snapshot.state is SourceBrokerV2SagaState.COMPENSATED
        and snapshot.dispatch_outcome is not None
    ):
        return _REQUIRED_APPLIED_PHASES[SourceBrokerV2SagaState.PARENT_RELEASED]
    return _REQUIRED_APPLIED_PHASES.get(snapshot.state, ())


def _require_canonical_json_bytes(payload: bytes, *, label: str) -> None:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > SOURCE_BROKER_V2_MAX_RECEIPT_BYTES
    ):
        raise ValueError(f"{label} violates the V2 wire bound")
    strict_canonical_json_loads(payload)


def _normalize_contract_time(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def source_authority_signature_payload(signing_bytes: bytes) -> bytes:
    _require_canonical_json_bytes(signing_bytes, label="source authority signing bytes")
    return canonical_json_bytes(
        {
            "contract": "rquant-ed25519-domain-separation/v1",
            "namespace": SOURCE_BROKER_V2_AUTHORITY_NAMESPACE,
            "purpose": SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
            "schema_version": 2,
            "payload_sha256": hashlib.sha256(signing_bytes).hexdigest(),
        }
    )


def _source_authority_openssl() -> str:
    for candidate in (
        "/opt/homebrew/bin/openssl",
        "/usr/bin/openssl",
        shutil.which("openssl"),
    ):
        if candidate and Path(candidate).is_file():
            return candidate
    raise ValueError("openssl is required for source authority Ed25519 verification")


def _validate_source_authority_public_key(public_key: bytes) -> None:
    try:
        completed = subprocess.run(
            (
                _source_authority_openssl(),
                "pkey",
                "-pubin",
                "-pubcheck",
                "-text_pub",
                "-noout",
            ),
            input=public_key,
            check=False,
            capture_output=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise ValueError("source authority public key is unusable") from exc
    if completed.returncode != 0 or b"ED25519" not in completed.stdout.upper():
        raise ValueError("source authority public key is not Ed25519")


def _source_authority_public_key_fingerprint(public_key: bytes) -> str:
    completed = subprocess.run(
        (_source_authority_openssl(), "pkey", "-pubin", "-outform", "DER"),
        input=public_key,
        check=False,
        capture_output=True,
        timeout=5,
    )
    if completed.returncode != 0 or not completed.stdout:
        raise ValueError("source authority public key fingerprint is unavailable")
    return hashlib.sha256(completed.stdout).hexdigest()


def _verify_source_authority_signature(
    *,
    public_key: bytes,
    signing_bytes: bytes,
    signature: str,
) -> bool:
    try:
        decoded = base64.b64decode(signature, validate=True)
    except (TypeError, ValueError):
        return False
    if len(decoded) != _ED25519_SIGNATURE_BYTES:
        return False
    payload = source_authority_signature_payload(signing_bytes)
    try:
        with tempfile.TemporaryDirectory(prefix="rquant-source-authority-") as directory_name:
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
                    _source_authority_openssl(),
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
                timeout=5,
            )
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return False
    return completed.returncode == 0


def _require_source_grant(
    *,
    receipt: SourceBrokerV2ClaimOnceResponse,
    operation_id: str,
    phase: SourceBrokerV2OutboxPhase,
    operation_request_hash: str,
    claim_binding_hash: str,
) -> None:
    if (
        receipt.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
        or receipt.operation_id != operation_id
        or receipt.phase is not phase
        or receipt.operation_request_hash != operation_request_hash
        or receipt.claim_binding_hash != claim_binding_hash
    ):
        raise ValueError("source invocation lacks a fenced definitive-absence grant")
    # The authority claim request itself binds the source operation request hash.
    # Its signed receipt is transported whole; the exact client verifies the signature.
    if not receipt.signature:
        raise ValueError("source invocation grant is not signed or bound")


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


def _decode_v2_wire_response(
    payload: bytes,
) -> SourceBrokerV2WireResponse | SourceBrokerV2WireFailure:
    _require_canonical_json_bytes(payload, label="V2 Unix wire response")
    try:
        decoded = strict_canonical_json_loads(payload)
        if not isinstance(decoded, dict) or type(decoded.get("ok")) is not bool:
            raise TypeError("V2 Unix wire response is not a tagged object")
        model = SourceBrokerV2WireResponse if decoded["ok"] else SourceBrokerV2WireFailure
        return strict_model_validate_canonical_json(model, payload)
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise SourceBrokerV2SagaIntegrityError(
            "V2 Unix wire response is malformed or noncanonical"
        ) from exc


def _v2_kernel_peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
    try:
        raw = connection.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_PEERCRED,
            struct.calcsize("3i"),
        )
        pid, uid, gid = struct.unpack("3i", raw)
    except (OSError, struct.error) as exc:
        raise SourceBrokerTransportError("V2 source server credentials are unavailable") from exc
    if pid <= 0 or uid < 0 or gid < 0:
        raise SourceBrokerTransportError("V2 source server credentials are invalid")
    return pid, uid, gid


def _outbox_now(payload: bytes) -> datetime:
    try:
        decoded = strict_canonical_json_loads(payload)
        value = decoded["now"]
        if type(value) is not str:
            raise TypeError("outbox time is not text")
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("outbox time is not timezone-aware")
        return parsed
    except (KeyError, TypeError, ValueError, StrictJsonError) as exc:
        raise SourceBrokerV2SagaIntegrityError("quota outbox time is invalid") from exc


def _optional_executor_time(value: object, *, label: str) -> datetime | None:
    if value is None:
        return None
    if type(value) is not str:
        raise SourceBrokerV2SagaIntegrityError(f"{label} SQLite type is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SourceBrokerV2SagaIntegrityError(f"{label} is malformed") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SourceBrokerV2SagaIntegrityError(f"{label} is not timezone-aware")
    return parsed


def _outbox_payload_hash(payload: bytes) -> str:
    try:
        decoded = strict_canonical_json_loads(payload)
        if not isinstance(decoded, dict):
            raise TypeError("outbox payload is not an object")
        stable = dict(decoded)
        stable.pop("now", None)
        return canonical_sha256(stable)
    except (TypeError, StrictJsonError) as exc:
        raise SourceBrokerV2SagaIntegrityError("outbox payload is malformed") from exc


def _model_hash(value: RuntimeContractModel) -> str:
    return hashlib.sha256(canonical_model_json_bytes(value)).hexdigest()
