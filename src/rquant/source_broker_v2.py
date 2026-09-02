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
import select
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import weakref
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import Any, Literal, Protocol
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
from rquant.source_broker_v2_heartbeat import (
    HEARTBEAT_PROTOCOL_VERSION,
    HEARTBEAT_SELECT_SQL,
    HEARTBEAT_UPDATE_SQL,  # noqa: F401 - re-exported so both sides share one object
    FrameReader,
    HeartbeatOwnershipError,
    HeartbeatProtocolError,
    encode_frame,
    heartbeat_write,
    open_saga_connection,
    stable_row_digest,
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
SOURCE_BROKER_V2_CLAIM_ATTEMPT_CONTRACT = "rquant-source-broker-claim-attempt/v1"
_ZERO_HASH = "0" * 64
_ED25519_SIGNATURE_BYTES = 64
_PRODUCTION_SAGA_GRAPH_TOKEN = object()
# One in-flight renewal may legitimately spend a whole ``busy_timeout_ms``
# waiting for the write lock at ``BEGIN IMMEDIATE``, and then an equal
# allowance on the durable tail that follows it: a ``synchronous = FULL``
# commit plus the passive checkpoint that closing the connection performs.
# ``busy_timeout_ms`` is the only tolerance this module states for one SQLite
# operation on this file, so the tail is given the same window as the wait.  A
# shutdown that waits less than the body it is waiting on cannot tell a slow
# heartbeat from a stuck one.
#
# The helper sleeps in ``select`` on a descriptor this process can make
# readable at will, so it wakes the instant the end frame is written; this
# budget therefore covers only a renewal that had already begun, and no longer
# has a term for the interval between them.
_HEARTBEAT_SHUTDOWN_LOCK_WINDOWS = 2
# Escalation timeouts, in the order they are spent.  Both are wall clock and
# both are generous by three orders of magnitude against what was measured on
# this tree: SIGTERM to exit is ~1-5ms, SIGKILL to a reaped ``returncode`` is
# ~1-2ms.  What they buy is that the shutdown ends at a number written here,
# never at how long a stuck fsync takes.
_HEARTBEAT_TERMINATE_SECONDS = 0.25
_HEARTBEAT_FINAL_REAP_SECONDS = 5.0
# Closing the parent's write ends is the only exit signal the helper needs, and
# it acts on it within a few milliseconds; this is the window before the same
# escalation runs.
_HEARTBEAT_EOF_SECONDS = 0.25
# The floor under the start and session-ack budgets.  Helper start to ``ready``
# was measured at 12-19ms across both versions on both platforms, so two
# seconds is not a guess about the host - it is a bound wide enough that only a
# genuinely broken start reaches it.
_HEARTBEAT_HANDSHAKE_FLOOR_SECONDS = 2.0
# A helper with no session left waits this long before exiting on its own, so
# an idle saga does not keep a process for the life of the interpreter.  Two
# leases is long enough that no ordinary sequence of invocations pays for a
# restart, which the restart-count assertions in the tests measure directly.
_HEARTBEAT_IDLE_EXIT_FLOOR_SECONDS = 30.0
# The argv that follows ``sys.executable``.  ``-I`` drops every ``PYTHON*``
# variable, the user site directory and the script directory from ``sys.path``,
# and ``-m`` makes the helper module its own ``__main__`` - so nothing the
# parent's entry point does is re-executed here.  Nothing secret is in it: a
# command line is readable through ``ps`` and ``/proc/<pid>/cmdline`` by any
# process of the same user, so the database path and the owner token travel in
# the config frame instead.
_HEARTBEAT_HELPER_COMMAND: tuple[str, ...] = (
    "-I",
    "-m",
    "rquant.source_broker_v2_heartbeat",
)
# Captured at import, from the constant rather than from the seam.  Rebinding
# the module attribute above would otherwise move both sides of the comparison
# at once and the production guard would wave the replacement through.
_FROZEN_HELPER_COMMAND: tuple[str, ...] = _HEARTBEAT_HELPER_COMMAND


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
    SOURCE_FINALIZE_RECONCILE_REQUIRED = "source_finalize_reconcile_required"
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


def source_effect_operation_id(
    *,
    saga_id: str,
    phase: SourceBrokerV2OutboxPhase,
) -> str:
    return canonical_sha256(
        {"contract": SOURCE_BROKER_V2_CONTRACT, "phase": phase.value, "saga_id": saga_id}
    )


def source_claim_attempt_id(
    *,
    effect_operation_id: str,
    executor_owner_token_hash: str,
    executor_generation: int,
    max_external_deadline: datetime,
    not_before_takeover_at: datetime,
) -> str:
    if executor_generation < 1:
        raise ValueError("source claim attempt generation must be positive")
    return canonical_sha256(
        {
            "contract": SOURCE_BROKER_V2_CLAIM_ATTEMPT_CONTRACT,
            "effect_operation_id": effect_operation_id,
            "executor_owner_token_hash": executor_owner_token_hash,
            "executor_generation": executor_generation,
            "max_external_deadline": _normalize_contract_time(
                max_external_deadline,
                label="max_external_deadline",
            ).isoformat(),
            "not_before_takeover_at": _normalize_contract_time(
                not_before_takeover_at,
                label="not_before_takeover_at",
            ).isoformat(),
        }
    )


@dataclass(frozen=True, slots=True)
class _SourceClaimAttempt:
    attempt_id: str
    executor_owner_token_hash: str
    executor_generation: int
    max_external_deadline: datetime
    not_before_takeover_at: datetime


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

    @property
    def receipt_hash(self) -> str:
        return _model_hash(self)


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


def _no_heartbeat_stage(stage: str) -> None:
    """Stage sink for the heartbeats nothing is watching."""

    del stage


def _heartbeat_environ() -> dict[str, str]:
    """The helper's whole environment, built up from nothing.

    Started from an empty dict rather than filtered from ``os.environ`` so a
    variable is present only because a line here put it there.  ``PATH`` is a
    constant because ``sys.executable`` is absolute and only libc fallbacks
    read it; the locale is pinned so the helper's formatting does not depend on
    the host's; the two temporary-directory variables are forwarded only when
    already set, because SQLite spills there and a test's private root must
    stay private.  No ``RQUANT_*``, no token, no ``HOME``.

    ``-I`` makes this belt and braces: even an injected ``PYTHONPATH`` would be
    ignored by the interpreter.
    """

    environ = {"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"}
    for name in ("TMPDIR", "SQLITE_TMPDIR"):
        value = os.environ.get(name)
        if value is not None:
            environ[name] = value
    return environ


def _process_create_time(pid: int) -> str | None:
    """A diagnostic stamp for an orphan, never a liveness decision.

    The ``Popen`` object is the reliable identity - ``poll`` only ever answers
    for this process's own child and pid reuse cannot fool it - so this is
    written into the failure message and compared by eye, nothing more.  It is
    read from ``/proc`` where that exists and left unknown elsewhere rather
    than shelling out inside a bounded teardown.
    """

    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
            stat = handle.read()
    except OSError:
        return None
    # The comm field is parenthesised and may itself contain spaces and
    # parentheses, so fields are counted from after its last ``)``.
    _, _, tail = stat.rpartition(")")
    fields = tail.split()
    if len(fields) < 20:
        return None
    return fields[19]


@dataclass
class _HeartbeatOrphan:
    """A helper that was sent SIGKILL and whose exit was never observed."""

    popen: subprocess.Popen[bytes]
    pid: int
    create_time: str | None
    killed_at: float
    operation_id: str

    def describe(self) -> str:
        stamp = "unknown" if self.create_time is None else self.create_time
        return (
            f"heartbeat helper pid {self.pid} (created {stamp}) was sent SIGKILL "
            f"during {self.operation_id} and the kernel has not reported its exit"
        )


@dataclass
class _HeartbeatResources:
    """The parent's end of one helper: three descriptors and the child.

    Deliberately holds no reference back to the saga.  ``weakref.finalize``
    keeps its arguments alive, so a state object that pointed at its owner
    would keep the owner alive too and the finalizer would never run while the
    process is up - which is the whole reason it exists.  ``orphans`` is the
    saga's own list, shared by identity: a finalizer that has to kill a helper
    must be able to record the outcome, and a plain list points at nobody.
    """

    orphans: list[_HeartbeatOrphan]
    popen: subprocess.Popen[bytes] | None = None
    ctrl_w: int | None = None
    status_r: int | None = None
    stop_w: int | None = None
    operation_id: str = "saga-close"


@dataclass
class _HeartbeatCloseOutcome:
    """What one bounded close did, as values rather than as an exception."""

    closed: bool = True
    pid: int | None = None
    returncode: int | None = None
    escalation: str = "none"
    orphaned: bool = False
    seconds: float = 0.0

    def describe(self) -> str:
        return (
            f"heartbeat helper close ended after {self.seconds:.3f}s "
            f"(pid {self.pid}, returncode {self.returncode}, "
            f"escalation {self.escalation!r}, orphaned {self.orphaned})"
        )


def _close_descriptor(state: _HeartbeatResources, name: str) -> None:
    descriptor = getattr(state, name)
    if descriptor is None:
        return
    setattr(state, name, None)
    with suppress(OSError):
        os.close(descriptor)


def _escalate_heartbeat_child(
    state: _HeartbeatResources,
    *,
    terminate_seconds: float,
    kill_seconds: float,
) -> tuple[int | None, str]:
    """SIGTERM, then SIGKILL, each with its own timeout.  Never raises.

    The orphan is registered *before* the kill rather than after the wait.  A
    signal this process has sent but whose effect it has not observed is
    exactly the state the next entry has to refuse to run in, and a registry
    written afterwards has a window where that state exists and is unrecorded.
    """

    popen = state.popen
    if popen is None:  # pragma: no cover - callers check first
        return None, "none"
    with suppress(OSError):
        popen.terminate()
    try:
        return popen.wait(timeout=terminate_seconds), "sigterm"
    except subprocess.TimeoutExpired:
        pass
    orphan = _HeartbeatOrphan(
        popen=popen,
        pid=popen.pid,
        create_time=_process_create_time(popen.pid),
        killed_at=time.time(),
        operation_id=state.operation_id,
    )
    state.orphans.append(orphan)
    with suppress(OSError):
        popen.kill()
    try:
        returncode = popen.wait(timeout=kill_seconds)
    except subprocess.TimeoutExpired:
        # SIGKILL does not preempt an uninterruptible kernel I/O, and this
        # method will not pretend otherwise by waiting again with no bound.
        # The process is recorded and every later entry fails closed until its
        # exit is observed.
        return None, "orphan"
    state.orphans.remove(orphan)
    return returncode, "sigkill"


def _close_resources(state: _HeartbeatResources) -> _HeartbeatCloseOutcome:
    """Release one helper, bounded, idempotent, and without raising.

    Split out of the saga on purpose: this is what ``weakref.finalize`` is
    given, and a bound method would hand the finalizer a strong reference to
    the very object whose collection is supposed to trigger it.

    Only an observed ``returncode`` counts as closed.  A helper that survived
    SIGKILL leaves the state populated, so a later ``poll`` can still find its
    exit and a second call can finish the job.
    """

    started = time.monotonic()
    popen = state.popen
    if popen is None:
        return _HeartbeatCloseOutcome(pid=None, returncode=None, escalation="none")
    # Closing every write end this process holds is the helper's exit signal -
    # it is sitting in ``select`` on the read ends and gets EOF at once.
    _close_descriptor(state, "ctrl_w")
    _close_descriptor(state, "stop_w")
    returncode = popen.poll()
    escalation = "clean"
    if returncode is None:
        try:
            returncode = popen.wait(timeout=_HEARTBEAT_EOF_SECONDS)
        except subprocess.TimeoutExpired:
            returncode, escalation = _escalate_heartbeat_child(
                state,
                terminate_seconds=_HEARTBEAT_TERMINATE_SECONDS,
                kill_seconds=_HEARTBEAT_FINAL_REAP_SECONDS,
            )
    _close_descriptor(state, "status_r")
    orphaned = returncode is None
    if not orphaned:
        state.popen = None
    return _HeartbeatCloseOutcome(
        closed=not orphaned,
        pid=popen.pid,
        returncode=returncode,
        escalation=escalation,
        orphaned=orphaned,
        seconds=time.monotonic() - started,
    )


class _HeartbeatFailure:
    """One failed renewal, carried across the process boundary as primitives.

    No exception object is ever serialized in either direction.  SQLite's own
    ``sqlite_errorcode`` is what the caller branches on; the type name and the
    truncated message are for a human.  Matching on message text is what the
    code replaces, and rebuilding here keeps that closed.
    """

    __slots__ = ("detail", "errorcode", "failure_type")

    def __init__(self, *, errorcode: int | None, failure_type: str, detail: str) -> None:
        self.errorcode = errorcode
        self.failure_type = failure_type
        self.detail = detail

    def rebuild(self) -> BaseException:
        if self.failure_type == HeartbeatOwnershipError.__name__:
            # The renewal guard matched no row, which is the same loss of
            # ownership the in-process renewal has always reported, and it must
            # keep reaching the caller as that classification.
            return SourceBrokerV2SagaConflictError(
                "outbox executor lost ownership before heartbeat"
            )
        error = _HeartbeatRenewalError(f"{self.failure_type}: {self.detail}")
        error.sqlite_errorcode = self.errorcode
        return error

    def describe(self) -> str:
        return f"{self.failure_type}(errorcode={self.errorcode}): {self.detail}"


class _HeartbeatRenewalError(SourceBrokerV2SagaError):
    """A renewal that failed in the helper, rebuilt from its structured report."""

    sqlite_errorcode: int | None = None


@dataclass
class _HeartbeatSessionOutcome:
    """Everything one invocation window can be asked about afterwards.

    ``acked`` and ``renewal_ok`` are two fields, not one, and the difference
    decides whether a helper lives.  ``acked`` is the protocol question - did
    the end frame come back - and it is the only gate on escalation.
    ``renewal_ok`` is the business question - did every renewal succeed - and
    it only decides which error the caller sees.  Folded together, an ordinary
    renewal failure would terminate and kill a helper that is healthy, sitting
    idle, and about to serve the next invocation.
    """

    token: str
    child_pid: int | None = None
    acked: bool = False
    renewal_ok: bool = True
    ticks: int = 0
    last_stage: str = "idle"
    failure: _HeartbeatFailure | None = None
    child_returncode: int | None = None
    child_alive: bool = False
    shutdown_seconds: float = 0.0
    escalation: str = "clean"
    restarts: int = 0
    orphaned: bool = False
    transport: str | None = None
    first_digest: str | None = None
    last_digest: str | None = None
    digest_changed: bool = False
    digest_mismatch: bool = False
    observed_digest: str | None = None

    def describe(self) -> str:
        failure = "none" if self.failure is None else self.failure.describe()
        return (
            f"outbox heartbeat session {self.token[:12]} ended after "
            f"{self.shutdown_seconds:.3f}s (helper pid {self.child_pid}, "
            f"returncode {self.child_returncode}, escalation {self.escalation!r}, "
            f"acked {self.acked}, renewal_ok {self.renewal_ok}, "
            f"{self.ticks} tick(s), last stage {self.last_stage!r}, "
            f"restarts {self.restarts}, orphaned {self.orphaned}, "
            f"transport {self.transport!r}, digest_changed {self.digest_changed}, "
            f"digest_mismatch {self.digest_mismatch}, failure {failure})"
        )


class _HeartbeatEndOfFile:
    """The sentinel a status read returns when the helper's end is gone."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return "<heartbeat status EOF>"


_HEARTBEAT_EOF = _HeartbeatEndOfFile()


class _HeartbeatHelper:
    """The parent's handle on one long-lived helper process.

    Holds descriptors and a frame reader and nothing else - in particular no
    reference to the saga, so the saga stays collectable and its finalizer
    stays reachable.  Every read here is bounded by a deadline the caller
    supplies and every write is a single small frame; this class starts no
    thread and never waits without a timeout.
    """

    __slots__ = ("_reader", "pid", "state")

    def __init__(self, state: _HeartbeatResources) -> None:
        self.state = state
        popen = state.popen
        assert popen is not None
        self.pid = popen.pid
        assert state.status_r is not None
        self._reader = FrameReader(state.status_r)

    def send(self, frame: dict[str, Any]) -> None:
        """Write one control frame.  A dead helper surfaces as BrokenPipeError."""

        descriptor = self.state.ctrl_w
        if descriptor is None:
            raise BrokenPipeError("heartbeat control pipe is already closed")
        payload = encode_frame(frame)
        while payload:
            payload = payload[os.write(descriptor, payload) :]

    def poll(self) -> int | None:
        popen = self.state.popen
        return None if popen is None else popen.poll()

    def poll_status(self, deadline: float) -> dict[str, Any] | _HeartbeatEndOfFile | None:
        """Next status frame, the EOF sentinel, or ``None`` once past ``deadline``."""

        descriptor = self.state.status_r
        if descriptor is None:  # pragma: no cover - callers close last
            return _HEARTBEAT_EOF
        while True:
            frame = self._reader.take_buffered()
            if frame is not None:
                return frame
            if self._reader.at_eof:
                return _HEARTBEAT_EOF
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            ready, _, _ = select.select([descriptor], [], [], remaining)
            if not ready:
                return None
            if not self._reader.fill() and self._reader.at_eof:
                return _HEARTBEAT_EOF

    def drain_status(self) -> list[dict[str, Any]]:
        """Whatever has already arrived, read with a zero-second poll."""

        frames: list[dict[str, Any]] = []
        descriptor = self.state.status_r
        while True:
            try:
                frame = self._reader.take_buffered()
            except HeartbeatProtocolError:
                return frames
            if frame is not None:
                frames.append(frame)
                continue
            if descriptor is None or self._reader.at_eof:
                return frames
            ready, _, _ = select.select([descriptor], [], [], 0)
            if not ready:
                return frames
            with suppress(OSError):
                if not self._reader.fill():
                    return frames
                continue
            return frames

    def reap(self) -> tuple[int | None, str]:
        return _escalate_heartbeat_child(
            self.state,
            terminate_seconds=_HEARTBEAT_TERMINATE_SECONDS,
            kill_seconds=_HEARTBEAT_FINAL_REAP_SECONDS,
        )

    def release(self) -> _HeartbeatCloseOutcome:
        return _close_resources(self.state)


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
        # A NaN passes every comparison above - it is neither ``<= 0`` nor
        # ``> 0`` - and would then be divided into a renewal interval, encoded
        # into a session frame and handed to ``select`` as a timeout.  Refusing
        # it at construction is the first of three guards; the frame encoder
        # (``allow_nan=False``) and the helper's own check are the other two.
        if not all(
            math.isfinite(value)
            for value in (
                executor_lease_seconds,
                executor_wait_seconds,
                source_request_deadline_seconds,
                source_takeover_grace_seconds,
            )
        ):
            raise ValueError("executor and source durations must be finite")
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
        # One non-blocking gate per saga instance.  It creates no thread, but
        # two threads racing the same instance can no longer open two windows
        # against one lease, and ``close`` uses the same gate to refuse to pull
        # descriptors out from under a running session.
        self._heartbeat_gate = Lock()
        self._orphaned_heartbeat_children: list[_HeartbeatOrphan] = []
        self._heartbeat_state = _HeartbeatResources(orphans=self._orphaned_heartbeat_children)
        self._heartbeat_child: _HeartbeatHelper | None = None
        self._last_heartbeat_session: _HeartbeatSessionOutcome | None = None
        self._heartbeat_close_outcome: _HeartbeatCloseOutcome | None = None
        # A backstop, not the path: ``close`` is.  The callable and the state
        # are module-level and hold no reference to this saga, which is what
        # lets the saga be collected at all - a bound method here would keep it
        # alive and the finalizer would never run.
        self._heartbeat_finalizer = weakref.finalize(
            self, _close_resources, self._heartbeat_state
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
        try:
            finalized = self._source_finalize(validated, dispatch=dispatch)
        except (
            SourceBrokerV2SagaReconcileRequiredError,
            SourceBrokerV2SagaUnavailableError,
            ConnectionError,
            OSError,
            TimeoutError,
        ) as exc:
            self._transition(
                SourceBrokerV2SagaState.SOURCE_FINALIZE_RECONCILE_REQUIRED,
                outcome=dispatch.outcome,
                reconcile_reason=str(exc),
            )
            return self.snapshot()
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
            replay = self._replay_source_operation(
                phase=phase,
                operation_id=operation_id,
                operation_request_hash=dispatch_request.request_hash,
            )
            if replay.status is SourceBrokerV2ReplayStatus.UNKNOWN:
                raise SourceBrokerV2SagaReconcileRequiredError(
                    "source authority cannot determine the dispatch outcome"
                )
            if replay.status is SourceBrokerV2ReplayStatus.FOUND:
                self._persist_source_replay_observation(
                    phase=phase,
                    operation_id=operation_id,
                    owner_generation=owner_generation,
                    response=replay,
                )
                return replay.result, None
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
            replay = self._replay_source_operation(
                phase=phase,
                operation_id=operation_id,
                operation_request_hash=finalize_request.request_hash,
            )
            if replay.status is SourceBrokerV2ReplayStatus.UNKNOWN:
                raise SourceBrokerV2SagaReconcileRequiredError(
                    "source authority cannot determine the finalize outcome"
                )
            if replay.status is SourceBrokerV2ReplayStatus.FOUND:
                self._persist_source_replay_observation(
                    phase=phase,
                    operation_id=operation_id,
                    owner_generation=owner_generation,
                    response=replay,
                )
                return replay.result, None
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

    def _ensure_source_attempt(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
    ) -> _SourceClaimAttempt:
        now = datetime.now(UTC)
        owner_hash = canonical_sha256({"executor_owner_token": self._executor_owner_token})
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_outbox(
                    connection,
                    operation_id=operation_id,
                    phase=phase,
                )
                attempt = self._source_attempt_from_outbox(
                    row,
                    effect_operation_id=operation_id,
                )
                if attempt is None:
                    prior = self._legacy_claim_observation(row)
                    if prior is not None:
                        if (
                            prior.saga_id != self.saga_id
                            or prior.operation_id != operation_id
                            or prior.phase is not phase
                        ):
                            raise SourceBrokerV2SagaIntegrityError(
                                "legacy source attempt evidence is foreign"
                            )
                        attempt_owner_hash = prior.executor_owner_token_hash
                        attempt_generation = prior.executor_generation
                        deadline = prior.max_external_deadline
                        takeover_at = prior.not_before_takeover_at
                    else:
                        deadline = _optional_executor_time(
                            row["max_external_deadline"],
                            label="source maximum external deadline",
                        )
                        takeover_at = _optional_executor_time(
                            row["not_before_takeover_at"],
                            label="source takeover boundary",
                        )
                        if (deadline is None) != (takeover_at is None):
                            raise SourceBrokerV2SagaIntegrityError(
                                "source external timing evidence is incomplete"
                            )
                        if deadline is None or (
                            phase is SourceBrokerV2OutboxPhase.SOURCE_FINALIZE
                            and takeover_at is not None
                            and now >= takeover_at
                        ):
                            deadline = now + timedelta(
                                seconds=self._source_request_deadline_seconds
                            )
                            takeover_at = deadline + timedelta(
                                seconds=self._source_takeover_grace_seconds
                            )
                        attempt_owner_hash = owner_hash
                        attempt_generation = owner_generation
                    if takeover_at is None:
                        raise SourceBrokerV2SagaIntegrityError(
                            "source takeover boundary is missing"
                        )
                    attempt = _SourceClaimAttempt(
                        attempt_id=source_claim_attempt_id(
                            effect_operation_id=operation_id,
                            executor_owner_token_hash=attempt_owner_hash,
                            executor_generation=attempt_generation,
                            max_external_deadline=deadline,
                            not_before_takeover_at=takeover_at,
                        ),
                        executor_owner_token_hash=attempt_owner_hash,
                        executor_generation=attempt_generation,
                        max_external_deadline=deadline,
                        not_before_takeover_at=takeover_at,
                    )
                    updated = connection.execute(
                        "UPDATE source_broker_v2_outbox SET source_attempt_id = ?, "
                        "source_attempt_owner_hash = ?, source_attempt_generation = ?, "
                        "max_external_deadline = ?, not_before_takeover_at = ? "
                        "WHERE operation_id = ? AND status = 'pending' "
                        "AND executor_owner_token = ? AND executor_generation = ? "
                        "AND source_attempt_id IS NULL",
                        (
                            attempt.attempt_id,
                            attempt.executor_owner_token_hash,
                            attempt.executor_generation,
                            attempt.max_external_deadline.isoformat(),
                            attempt.not_before_takeover_at.isoformat(),
                            operation_id,
                            self._executor_owner_token,
                            owner_generation,
                        ),
                    ).rowcount
                    if updated != 1:
                        raise SourceBrokerV2SagaConflictError(
                            "outbox executor lost ownership before source attempt persistence"
                        )
                has_observation = row["source_observation_json"] is not None
                has_grant = row["source_grant_json"] is not None
                invocation_started = bool(row["invoke_started"]) or (
                    row["dispatch_started_at"] is not None
                )
                if (has_observation or has_grant) and not invocation_started:
                    if now < attempt.not_before_takeover_at:
                        raise SourceBrokerV2SagaReconcileRequiredError(
                            "source claim attempt remains fenced until takeover"
                        )
                    if phase is not SourceBrokerV2OutboxPhase.SOURCE_FINALIZE:
                        raise SourceBrokerV2SagaReconcileRequiredError(
                            "expired source dispatch requires reconciliation"
                        )
                    deadline = now + timedelta(seconds=self._source_request_deadline_seconds)
                    takeover_at = deadline + timedelta(seconds=self._source_takeover_grace_seconds)
                    replacement = _SourceClaimAttempt(
                        attempt_id=source_claim_attempt_id(
                            effect_operation_id=operation_id,
                            executor_owner_token_hash=owner_hash,
                            executor_generation=owner_generation,
                            max_external_deadline=deadline,
                            not_before_takeover_at=takeover_at,
                        ),
                        executor_owner_token_hash=owner_hash,
                        executor_generation=owner_generation,
                        max_external_deadline=deadline,
                        not_before_takeover_at=takeover_at,
                    )
                    updated = connection.execute(
                        "UPDATE source_broker_v2_outbox SET source_attempt_id = ?, "
                        "source_attempt_owner_hash = ?, source_attempt_generation = ?, "
                        "max_external_deadline = ?, not_before_takeover_at = ?, "
                        "source_grant_json = NULL, source_grant_hash = NULL, "
                        "source_observation_json = NULL, source_observation_hash = NULL "
                        "WHERE operation_id = ? "
                        "AND status = 'pending' AND executor_owner_token = ? "
                        "AND executor_generation = ? AND source_attempt_id = ? "
                        "AND invoke_started = 0 AND dispatch_started_at IS NULL",
                        (
                            replacement.attempt_id,
                            replacement.executor_owner_token_hash,
                            replacement.executor_generation,
                            deadline.isoformat(),
                            takeover_at.isoformat(),
                            operation_id,
                            self._executor_owner_token,
                            owner_generation,
                            attempt.attempt_id,
                        ),
                    ).rowcount
                    if updated != 1:
                        raise SourceBrokerV2SagaConflictError(
                            "outbox executor lost ownership before source attempt recovery"
                        )
                    attempt = replacement
                connection.commit()
                return attempt
            except BaseException:
                connection.rollback()
                raise

    def _source_attempt_from_outbox(
        self,
        row: sqlite3.Row,
        *,
        effect_operation_id: str,
    ) -> _SourceClaimAttempt | None:
        values = (
            row["source_attempt_id"],
            row["source_attempt_owner_hash"],
            row["source_attempt_generation"],
        )
        if all(value is None for value in values):
            return None
        if any(value is None for value in values):
            raise SourceBrokerV2SagaIntegrityError("source claim attempt evidence is incomplete")
        attempt_id, owner_hash, generation = values
        if (
            type(attempt_id) is not str
            or type(owner_hash) is not str
            or type(generation) is not int
            or generation < 1
        ):
            raise SourceBrokerV2SagaIntegrityError("source claim attempt SQLite types are invalid")
        deadline = _optional_executor_time(
            row["max_external_deadline"],
            label="source maximum external deadline",
        )
        takeover_at = _optional_executor_time(
            row["not_before_takeover_at"],
            label="source takeover boundary",
        )
        if deadline is None or takeover_at is None:
            raise SourceBrokerV2SagaIntegrityError(
                "source claim attempt timing evidence is incomplete"
            )
        expected = source_claim_attempt_id(
            effect_operation_id=effect_operation_id,
            executor_owner_token_hash=owner_hash,
            executor_generation=generation,
            max_external_deadline=deadline,
            not_before_takeover_at=takeover_at,
        )
        if attempt_id != expected:
            raise SourceBrokerV2SagaIntegrityError(
                "source claim attempt identity conflicts with its immutable binding"
            )
        return _SourceClaimAttempt(
            attempt_id=attempt_id,
            executor_owner_token_hash=owner_hash,
            executor_generation=generation,
            max_external_deadline=deadline,
            not_before_takeover_at=takeover_at,
        )

    def _legacy_claim_observation(
        self,
        row: sqlite3.Row,
    ) -> SourceBrokerV2ClaimOnceResponse | None:
        raw = row["source_grant_json"] or row["source_observation_json"]
        if raw is None:
            return None
        if type(raw) is not str:
            raise SourceBrokerV2SagaIntegrityError(
                "legacy source attempt evidence has an invalid SQLite type"
            )
        try:
            return strict_model_validate_canonical_json(
                SourceBrokerV2ClaimOnceResponse,
                raw.encode("utf-8"),
            )
        except (StrictJsonError, ValidationError, ValueError, TypeError):
            return None

    def _source_claim_once(
        self,
        *,
        request: SourceBrokerV2SagaRequest,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
        owner_generation: int,
    ) -> SourceBrokerV2ClaimOnceResponse:
        source_attempt = self._ensure_source_attempt(
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
            executor_owner_token_hash=source_attempt.executor_owner_token_hash,
            executor_generation=source_attempt.executor_generation,
            max_external_deadline=source_attempt.max_external_deadline,
            not_before_takeover_at=source_attempt.not_before_takeover_at,
        )
        response = self._invoke_source_claim_once(claim_request)
        self._persist_source_claim(
            phase=phase,
            operation_id=operation_id,
            owner_generation=owner_generation,
            attempt=source_attempt,
            response=response,
        )
        current_owner_hash = canonical_sha256({"executor_owner_token": self._executor_owner_token})
        if (
            response.status is SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
            and source_attempt.executor_owner_token_hash != current_owner_hash
            and response.observed_at < source_attempt.not_before_takeover_at
        ):
            raise SourceBrokerV2SagaReconcileRequiredError(
                "source claim attempt remains fenced until takeover"
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
        attempt: _SourceClaimAttempt,
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
                persisted_attempt = self._source_attempt_from_outbox(
                    row,
                    effect_operation_id=operation_id,
                )
                if persisted_attempt != attempt or (
                    response.operation_id != operation_id
                    or response.executor_owner_token_hash != attempt.executor_owner_token_hash
                    or response.executor_generation != attempt.executor_generation
                    or response.max_external_deadline != attempt.max_external_deadline
                    or response.not_before_takeover_at != attempt.not_before_takeover_at
                ):
                    raise SourceBrokerV2SagaIntegrityError(
                        "source claim response rebound its durable attempt"
                    )
                self._append_source_receipt(
                    connection,
                    phase=phase,
                    operation_id=operation_id,
                    attempt_id=attempt.attempt_id,
                    status=response.status.value,
                    receipt_hash=receipt_hash,
                    raw=raw,
                )
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
        expected = self._validate_source_result_binding(
            phase=phase,
            operation_id=operation_id,
            operation_request_hash=operation_request_hash,
            result_bytes=response.result,
        )
        if response.status is not expected:
            raise SourceBrokerV2SagaIntegrityError(
                "source terminal status conflicts with its result"
            )

    def _validate_source_result_binding(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
        result_bytes: bytes,
    ) -> SourceBrokerV2ClaimStatus:
        try:
            if phase is SourceBrokerV2OutboxPhase.DISPATCH:
                result = strict_model_validate_canonical_json(
                    SourceBrokerV2DispatchResponse,
                    result_bytes,
                )
                expected = SourceBrokerV2ClaimStatus(result.outcome.value)
            else:
                result = strict_model_validate_canonical_json(
                    SourceBrokerV2FinalizeResponse,
                    result_bytes,
                )
                expected = SourceBrokerV2ClaimStatus.SUCCESS
            if (
                result.saga_id != self.saga_id
                or result.operation_id != operation_id
                or result.request_hash != operation_request_hash
            ):
                raise ValueError("terminal source result is foreign")
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError("source terminal result is invalid") from exc
        return expected

    def _replay_source_operation(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        operation_request_hash: str,
    ) -> SourceBrokerV2ReplayResponse:
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
        if response.status is SourceBrokerV2ReplayStatus.FOUND:
            if response.result is None:
                raise SourceBrokerV2SagaIntegrityError("found source replay omitted its result")
            self._validate_source_result_binding(
                phase=phase,
                operation_id=operation_id,
                operation_request_hash=operation_request_hash,
                result_bytes=response.result,
            )
        return response

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
        prior = self._decode_source_observation(raw_observation.encode("utf-8"))
        if isinstance(prior, SourceBrokerV2ReplayResponse):
            replay_request = SourceBrokerV2ReplayRequest(
                saga_id=self.saga_id,
                operation_id=operation_id,
                phase=phase,
                operation_request_hash=operation_request_hash,
                challenge=prior.challenge,
            )
            if (
                prior.saga_id != self.saga_id
                or prior.operation_id != operation_id
                or prior.phase is not phase
                or prior.request_hash != replay_request.request_hash
                or prior.status is not SourceBrokerV2ReplayStatus.FOUND
                or prior.result != stored
            ):
                raise SourceBrokerV2SagaIntegrityError(
                    "stored source replay observation is invalid"
                )
            self._source_authority_keyring.require_verified_replay(
                request=replay_request,
                receipt=prior,
            )
            fresh = self._replay_source_operation(
                phase=phase,
                operation_id=operation_id,
                operation_request_hash=operation_request_hash,
            )
            if fresh.status is not SourceBrokerV2ReplayStatus.FOUND or fresh.result != stored:
                raise SourceBrokerV2SagaRepairRequiredError(
                    f"source authority is not terminal for applied {phase.value} operation"
                )
            self._persist_applied_source_observation(
                phase=phase,
                operation_id=operation_id,
                attempt_id=None,
                response=fresh,
            )
            return stored
        try:
            if (
                prior.saga_id != self.saga_id
                or prior.operation_id != operation_id
                or prior.phase is not phase
                or prior.operation_request_hash != operation_request_hash
            ):
                raise ValueError("stored source claim observation is foreign")
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
            attempt_id=source_claim_attempt_id(
                effect_operation_id=operation_id,
                executor_owner_token_hash=response.executor_owner_token_hash,
                executor_generation=response.executor_generation,
                max_external_deadline=response.max_external_deadline,
                not_before_takeover_at=response.not_before_takeover_at,
            ),
            response=response,
        )
        return stored

    def _decode_source_observation(
        self,
        raw: bytes,
    ) -> SourceBrokerV2ClaimOnceResponse | SourceBrokerV2ReplayResponse:
        try:
            decoded = strict_canonical_json_loads(raw)
            if not isinstance(decoded, Mapping):
                raise TypeError("source observation is not an object")
            contract = decoded.get("contract")
            if contract == "rquant-source-broker-claim-once-response/v2":
                return strict_model_validate_canonical_json(
                    SourceBrokerV2ClaimOnceResponse,
                    raw,
                )
            if contract == "rquant-source-broker-replay-response/v2":
                return strict_model_validate_canonical_json(
                    SourceBrokerV2ReplayResponse,
                    raw,
                )
            raise ValueError("source observation contract is not allowed")
        except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
            raise SourceBrokerV2SagaIntegrityError(
                "stored source observation is malformed"
            ) from exc

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
        attempt_id: str | None,
        response: SourceBrokerV2ClaimOnceResponse | SourceBrokerV2ReplayResponse,
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
                self._append_source_receipt(
                    connection,
                    phase=phase,
                    operation_id=operation_id,
                    attempt_id=attempt_id,
                    status=response.status.value,
                    receipt_hash=receipt_hash,
                    raw=raw,
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

    def _persist_source_replay_observation(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
        response: SourceBrokerV2ReplayResponse,
    ) -> None:
        if response.status is not SourceBrokerV2ReplayStatus.FOUND:
            raise SourceBrokerV2SagaIntegrityError(
                "only a found replay can become source observation evidence"
            )
        raw = canonical_model_json_bytes(response)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._read_outbox(
                    connection,
                    operation_id=operation_id,
                    phase=phase,
                )
                if (
                    row["status"] != "pending"
                    or row["executor_owner_token"] != self._executor_owner_token
                    or row["executor_generation"] != owner_generation
                ):
                    raise SourceBrokerV2SagaConflictError(
                        "outbox executor lost ownership before replay persistence"
                    )
                self._append_source_receipt(
                    connection,
                    phase=phase,
                    operation_id=operation_id,
                    attempt_id=None,
                    status=response.status.value,
                    receipt_hash=response.receipt_hash,
                    raw=raw,
                )
                updated = connection.execute(
                    "UPDATE source_broker_v2_outbox SET source_observation_json = ?, "
                    "source_observation_hash = ? WHERE operation_id = ? "
                    "AND status = 'pending' AND executor_owner_token = ? "
                    "AND executor_generation = ?",
                    (
                        raw.decode("utf-8"),
                        response.receipt_hash,
                        operation_id,
                        self._executor_owner_token,
                        owner_generation,
                    ),
                ).rowcount
                if updated != 1:
                    raise SourceBrokerV2SagaConflictError(
                        "outbox executor lost ownership while persisting source replay"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _append_source_receipt(
        self,
        connection: sqlite3.Connection,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        attempt_id: str | None,
        status: str,
        receipt_hash: str,
        raw: bytes,
    ) -> None:
        existing = connection.execute(
            "SELECT operation_id, attempt_id, saga_id, phase, status, receipt_json "
            "FROM source_broker_v2_source_receipt WHERE receipt_hash = ?",
            (receipt_hash,),
        ).fetchone()
        values = (
            operation_id,
            attempt_id,
            self.saga_id,
            phase.value,
            status,
            raw.decode("utf-8"),
        )
        if existing is None:
            connection.execute(
                "INSERT INTO source_broker_v2_source_receipt("
                "receipt_hash, operation_id, attempt_id, saga_id, phase, status, receipt_json"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                (receipt_hash, *values),
            )
        elif tuple(existing) != values:
            raise SourceBrokerV2SagaIntegrityError("source receipt hash was rebound")

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
                        source_attempt_id TEXT,
                        source_attempt_owner_hash TEXT,
                        source_attempt_generation INTEGER,
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
                    "source_attempt_id",
                    "source_attempt_owner_hash",
                    "source_grant_json",
                    "source_grant_hash",
                    "source_observation_json",
                    "source_observation_hash",
                ):
                    if name not in columns:
                        connection.execute(
                            f"ALTER TABLE source_broker_v2_outbox ADD COLUMN {name} TEXT"
                        )
                if "source_attempt_generation" not in columns:
                    connection.execute(
                        "ALTER TABLE source_broker_v2_outbox "
                        "ADD COLUMN source_attempt_generation INTEGER"
                    )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS source_broker_v2_source_receipt (
                        receipt_hash TEXT PRIMARY KEY,
                        operation_id TEXT NOT NULL,
                        attempt_id TEXT,
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
                receipt_columns = {
                    str(row[1])
                    for row in connection.execute(
                        "PRAGMA table_info(source_broker_v2_source_receipt)"
                    ).fetchall()
                }
                if "attempt_id" not in receipt_columns:
                    connection.execute(
                        "ALTER TABLE source_broker_v2_source_receipt ADD COLUMN attempt_id TEXT"
                    )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS source_broker_v2_source_receipt_operation "
                    "ON source_broker_v2_source_receipt(operation_id, receipt_hash)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS source_broker_v2_source_receipt_saga "
                    "ON source_broker_v2_source_receipt(saga_id, operation_id)"
                )
                self._upgrade_legacy_source_evidence(connection)
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

    def _upgrade_legacy_source_evidence(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        rows = connection.execute(
            "SELECT * FROM source_broker_v2_outbox WHERE saga_id = ? "
            "AND phase IN ('dispatch', 'source_finalize') "
            "AND source_attempt_id IS NULL",
            (self.saga_id,),
        ).fetchall()
        for row in rows:
            prior = self._legacy_claim_observation(row)
            if prior is None:
                continue
            phase = SourceBrokerV2OutboxPhase(str(row["phase"]))
            if (
                prior.saga_id != self.saga_id
                or prior.operation_id != row["operation_id"]
                or prior.phase is not phase
            ):
                raise SourceBrokerV2SagaIntegrityError("legacy source observation is foreign")
            attempt_id = source_claim_attempt_id(
                effect_operation_id=prior.operation_id,
                executor_owner_token_hash=prior.executor_owner_token_hash,
                executor_generation=prior.executor_generation,
                max_external_deadline=prior.max_external_deadline,
                not_before_takeover_at=prior.not_before_takeover_at,
            )
            connection.execute(
                "UPDATE source_broker_v2_outbox SET source_attempt_id = ?, "
                "source_attempt_owner_hash = ?, source_attempt_generation = ?, "
                "max_external_deadline = ?, not_before_takeover_at = ? "
                "WHERE operation_id = ? AND source_attempt_id IS NULL",
                (
                    attempt_id,
                    prior.executor_owner_token_hash,
                    prior.executor_generation,
                    prior.max_external_deadline.isoformat(),
                    prior.not_before_takeover_at.isoformat(),
                    prior.operation_id,
                ),
            )
        receipts = connection.execute(
            "SELECT receipt_hash, receipt_json FROM source_broker_v2_source_receipt "
            "WHERE saga_id = ? AND attempt_id IS NULL",
            (self.saga_id,),
        ).fetchall()
        for row in receipts:
            receipt = self._decode_source_observation(str(row["receipt_json"]).encode("utf-8"))
            if isinstance(receipt, SourceBrokerV2ReplayResponse):
                continue
            attempt_id = source_claim_attempt_id(
                effect_operation_id=receipt.operation_id,
                executor_owner_token_hash=receipt.executor_owner_token_hash,
                executor_generation=receipt.executor_generation,
                max_external_deadline=receipt.max_external_deadline,
                not_before_takeover_at=receipt.not_before_takeover_at,
            )
            connection.execute(
                "UPDATE source_broker_v2_source_receipt SET attempt_id = ? "
                "WHERE receipt_hash = ? AND attempt_id IS NULL",
                (attempt_id, row["receipt_hash"]),
            )

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
        # The connection shape lives in the heartbeat module so the helper
        # process and this one open the same file the same way; there is one
        # definition of ``busy_timeout`` and ``synchronous`` for the whole saga
        # rather than one per caller.
        connection = open_saga_connection(str(self.path), self._busy_timeout_ms)
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
        return source_effect_operation_id(saga_id=self.saga_id, phase=phase)

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

    def _heartbeat_helper_command(self) -> tuple[str, ...]:
        """Seam: the argv that follows ``sys.executable`` when a helper starts.

        A command line is the only thing that crosses this process boundary -
        no import path, no pickle, no object - which is why the seam is safe to
        expose at all.  Tests point it at a single-file fault helper; the guard
        at the one place it is used refuses that in production.
        """

        return _HEARTBEAT_HELPER_COMMAND

    def _heartbeat_shutdown_budget(self) -> float:
        return _HEARTBEAT_SHUTDOWN_LOCK_WINDOWS * self._busy_timeout_ms / 1_000

    def _heartbeat_handshake_budget(self) -> float:
        return max(_HEARTBEAT_HANDSHAKE_FLOOR_SECONDS, self._busy_timeout_ms / 1_000)

    def _heartbeat_idle_exit_seconds(self) -> float:
        return max(_HEARTBEAT_IDLE_EXIT_FLOOR_SECONDS, 2 * self._executor_lease_seconds)

    def _describe_orphaned_heartbeat_children(self) -> str:
        return "; ".join(orphan.describe() for orphan in self._orphaned_heartbeat_children)

    def _sweep_orphaned_heartbeat_children(self) -> int:
        """Non-blocking: ask each recorded orphan whether it has exited yet.

        ``poll`` is the reliable question because these are this process's own
        children, so the answer cannot be confused by pid reuse, and it never
        waits.  Returns how many were confirmed gone.
        """

        cleared = 0
        for orphan in list(self._orphaned_heartbeat_children):
            if orphan.popen.poll() is not None:
                self._orphaned_heartbeat_children.remove(orphan)
                cleared += 1
        return cleared

    def _probe_saga_write_lock(self) -> None:
        """Take the file's write lock and let it go: proof it is actually free.

        A killed helper's exit is not the same fact as its write lock being
        released - a process stuck in uninterruptible I/O still holds it, and a
        logical lease expiring does not release a kernel lock.  This is the
        only evidence that says so, and it is bounded by ``busy_timeout``
        because that is what ``BEGIN IMMEDIATE`` waits with.
        """

        try:
            connection = open_saga_connection(str(self.path), self._busy_timeout_ms)
        except sqlite3.Error as exc:
            raise SourceBrokerV2SagaUnavailableError(
                "saga database could not be opened to prove its write lock is free"
            ) from exc
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("ROLLBACK")
        except sqlite3.Error as exc:
            raise SourceBrokerV2SagaUnavailableError(
                "saga write lock is still held after a heartbeat helper was killed"
            ) from exc
        finally:
            with suppress(sqlite3.Error):
                connection.close()

    def _require_takeable_saga(self) -> None:
        """Entry gate: no unconfirmed orphan, and the write lock really free.

        Runs before anything else this method does, and before any external
        effect, so a saga instance that once failed to kill a helper refuses
        every later invocation until that helper's exit is observed - and even
        then only after the lock itself has been taken and released.
        """

        cleared = self._sweep_orphaned_heartbeat_children()
        if self._orphaned_heartbeat_children:
            raise SourceBrokerV2SagaUnavailableError(
                self._describe_orphaned_heartbeat_children()
            )
        if cleared:
            self._probe_saga_write_lock()

    def _start_helper_once(self, *, operation_id: str) -> _HeartbeatHelper:
        """Start one helper and complete its handshake, or own nothing.

        Six raw descriptors exist between the first ``os.pipe`` and the moment
        ``Popen`` returns, and every failure path below closes exactly the ones
        this call created.  After a successful spawn the three child ends are
        closed immediately - the parent keeping a copy of ``stop_r`` would mean
        the helper never sees EOF, and keeping ``status_w`` would mean the
        parent never sees one either.
        """

        state = self._heartbeat_state
        if state.popen is not None:
            raise SourceBrokerV2SagaUnavailableError(
                "a heartbeat helper is already running for this saga"
            )
        command = self._heartbeat_helper_command()
        if self._production_graph is not None and command != _FROZEN_HELPER_COMMAND:
            # Compared against the constant captured at import, not against the
            # seam: rebinding the module attribute would move both sides.
            raise TypeError("production saga heartbeat helper command was replaced")
        ctrl_r, ctrl_w = os.pipe()
        status_r, status_w = os.pipe()
        stop_r, stop_w = os.pipe()
        popen: subprocess.Popen[bytes] | None = None
        try:
            popen = subprocess.Popen(  # noqa: S603 - argv is a frozen constant
                [sys.executable, *command,
                 "--control-fd", str(ctrl_r),
                 "--status-fd", str(status_w),
                 "--stop-fd", str(stop_r)],
                pass_fds=(ctrl_r, status_w, stop_r),
                close_fds=True,
                start_new_session=True,
                env=_heartbeat_environ(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except BaseException as exc:
            for descriptor in (ctrl_r, ctrl_w, status_r, status_w, stop_r, stop_w):
                with suppress(OSError):
                    os.close(descriptor)
            raise SourceBrokerV2SagaUnavailableError(
                "outbox heartbeat helper could not be started"
            ) from exc
        for descriptor in (ctrl_r, status_w, stop_r):
            with suppress(OSError):
                os.close(descriptor)
        state.popen = popen
        state.ctrl_w = ctrl_w
        state.status_r = status_r
        state.stop_w = stop_w
        state.operation_id = operation_id
        helper = _HeartbeatHelper(state)
        try:
            helper.send(
                {
                    "t": "config",
                    "protocol": HEARTBEAT_PROTOCOL_VERSION,
                    "db": str(self.path),
                    "saga_id": self.saga_id,
                    "owner_token": self._executor_owner_token,
                    "busy_timeout_ms": self._busy_timeout_ms,
                    "lease_seconds": self._executor_lease_seconds,
                    "idle_exit_seconds": self._heartbeat_idle_exit_seconds(),
                }
            )
            frame = helper.poll_status(time.monotonic() + self._heartbeat_handshake_budget())
            if (
                not isinstance(frame, dict)
                or frame.get("t") != "ready"
                or frame.get("protocol") != HEARTBEAT_PROTOCOL_VERSION
                or type(frame.get("pid")) is not int
                or frame["pid"] != popen.pid
            ):
                raise SourceBrokerV2SagaUnavailableError(
                    f"outbox heartbeat helper did not report ready ({frame!r})"
                )
        except BaseException as exc:
            # One abort for every way the handshake can fail - a write that
            # hits a dead helper, a timeout, an EOF, a frame of the wrong
            # shape or a pid that is not this child's.  It is bounded and it
            # leaves no descriptor and no process behind.
            self._discard_heartbeat_child(helper)
            if isinstance(exc, SourceBrokerV2SagaError):
                raise
            raise SourceBrokerV2SagaUnavailableError(
                "outbox heartbeat helper handshake failed"
            ) from exc
        self._heartbeat_child = helper
        return helper

    def _discard_heartbeat_child(self, helper: _HeartbeatHelper) -> _HeartbeatCloseOutcome:
        """Bounded release of a helper this saga will not use again."""

        outcome = helper.release()
        if self._heartbeat_child is helper:
            self._heartbeat_child = None
        return outcome

    def _open_heartbeat_session(
        self,
        *,
        token: str,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
        owner_generation: int,
        interval: float,
    ) -> tuple[_HeartbeatHelper, int]:
        """Get an acknowledged session, restarting the helper at most once.

        Three detectors, because only the first is a pre-check and a pre-check
        on another process is a race by construction: ``poll`` before the
        write, ``BrokenPipeError`` from the write itself, and a timeout or EOF
        while waiting for the ack.  Every failure here happens *before* the
        external invocation, so a saga that cannot confirm a session makes zero
        external calls rather than running one unprotected.
        """

        restarts = 0
        while True:
            helper = self._heartbeat_child
            if helper is not None and helper.poll() is not None:
                self._discard_heartbeat_child(helper)
                helper = None
            try:
                if helper is None:
                    if restarts:
                        # A replacement is only allowed once the previous
                        # helper's exit has been observed and the write lock
                        # itself has been proven free.
                        self._probe_saga_write_lock()
                    helper = self._start_helper_once(operation_id=operation_id)
                helper.send(
                    {
                        "t": "session",
                        "token": token,
                        "phase": phase.value,
                        "operation_id": operation_id,
                        "owner_generation": owner_generation,
                        "interval_seconds": interval,
                    }
                )
                deadline = time.monotonic() + self._heartbeat_handshake_budget()
                acked = False
                while True:
                    frame = helper.poll_status(deadline)
                    if not isinstance(frame, dict):
                        break
                    if frame.get("t") == "session-ack" and frame.get("token") == token:
                        acked = True
                        break
                if acked:
                    return helper, restarts
                reason = "outbox heartbeat helper did not acknowledge its session"
            except SourceBrokerV2SagaUnavailableError as exc:
                # A start that failed has already released everything it made;
                # it is still worth one replacement, which is what A3 is.
                reason = str(exc)
                helper = None
            except (BrokenPipeError, OSError, HeartbeatProtocolError) as exc:
                reason = f"outbox heartbeat helper session could not be sent ({exc!r})"
            except ValueError:
                # A non-finite interval is refused by the encoder, before any
                # external effect.  That is a configuration fault, not a dead
                # helper, so it is not something to restart into.
                if helper is not None:
                    self._discard_heartbeat_child(helper)
                raise
            outcome = _HeartbeatCloseOutcome()
            if helper is not None:
                outcome = self._discard_heartbeat_child(helper)
            if outcome.orphaned or self._orphaned_heartbeat_children:
                # A3 ends here rather than starting a second helper: an
                # unconfirmed kill is exactly the state a new writer must not
                # be started into.
                raise SourceBrokerV2SagaUnavailableError(
                    f"{reason}; {self._describe_orphaned_heartbeat_children()}"
                )
            if restarts >= 1:
                raise SourceBrokerV2SagaUnavailableError(reason)
            restarts += 1

    def _absorb_heartbeat_frame(
        self, outcome: _HeartbeatSessionOutcome, frame: dict[str, Any]
    ) -> None:
        kind = frame.get("t")
        if kind == "tick":
            if type(frame.get("n")) is int:
                outcome.ticks = max(outcome.ticks, frame["n"])
            if type(frame.get("stage")) is str:
                outcome.last_stage = frame["stage"]
            digest = frame.get("digest")
            if type(digest) is str:
                if outcome.first_digest is None:
                    outcome.first_digest = digest
                outcome.last_digest = digest
                if digest != outcome.first_digest:
                    outcome.digest_changed = True
            return
        if kind == "failed" and frame.get("token") == outcome.token:
            outcome.renewal_ok = False
            errorcode = frame.get("errorcode")
            outcome.failure = _HeartbeatFailure(
                errorcode=errorcode if type(errorcode) is int else None,
                failure_type=str(frame.get("type", "Exception")),
                detail=str(frame.get("detail", "")),
            )
            return
        if kind == "end-ack" and frame.get("token") == outcome.token:
            outcome.acked = True
            if type(frame.get("ticks")) is int:
                outcome.ticks = frame["ticks"]
            if type(frame.get("last_stage")) is str:
                outcome.last_stage = frame["last_stage"]
            if frame.get("last_outcome") != "ok":
                outcome.renewal_ok = False
            for name in ("first_digest", "last_digest"):
                value = frame.get(name)
                if value is None or type(value) is str:
                    setattr(outcome, name, value)
            if frame.get("digest_changed") is True:
                outcome.digest_changed = True

    def _close_heartbeat_session(
        self,
        helper: _HeartbeatHelper,
        token: str,
        invoke_error: BaseException | None,
        *,
        restarts: int,
    ) -> _HeartbeatSessionOutcome:
        """End one session within a fixed budget.  Contains no ``raise``.

        Every transport failure - a broken pipe on the end frame, an EOF, a
        malformed frame, a timeout - becomes a field on the outcome and the
        bounded reap continues.  Raising from here would displace the caller's
        own exception, so the decisions are made by the caller afterwards.
        """

        outcome = _HeartbeatSessionOutcome(
            token=token, child_pid=helper.pid, restarts=restarts
        )
        started = time.monotonic()
        deadline = started + self._heartbeat_shutdown_budget()
        try:
            helper.send({"t": "end", "token": token})
        except (BrokenPipeError, OSError, ValueError) as exc:
            outcome.transport = f"send-end: {exc!r}"
        while outcome.transport is None and not outcome.acked:
            try:
                frame = helper.poll_status(deadline)
            except (OSError, HeartbeatProtocolError) as exc:
                outcome.transport = f"poll-end-ack: {exc!r}"
                break
            if frame is _HEARTBEAT_EOF:
                outcome.transport = "poll-end-ack: end of file"
                break
            if frame is None:
                break
            if isinstance(frame, dict):
                # A ``failed`` frame only records that a renewal failed and a
                # ``tick`` only updates the digest; neither ends this wait.
                # Only the acknowledgement for this token does, and the helper
                # answers an end frame unconditionally, so an ordinary renewal
                # failure still costs one pipe round trip rather than a
                # terminate and a kill.
                self._absorb_heartbeat_frame(outcome, frame)
        if not outcome.acked:
            returncode, escalation = helper.reap()
            outcome.child_returncode = returncode
            outcome.escalation = escalation
            outcome.orphaned = returncode is None
        for frame in helper.drain_status():
            self._absorb_heartbeat_frame(outcome, frame)
        if outcome.child_returncode is None and not outcome.orphaned:
            outcome.child_returncode = helper.poll()
        outcome.child_alive = outcome.child_returncode is None and not outcome.orphaned
        if not outcome.child_alive:
            # A helper that is gone takes its descriptors with it; a live one
            # keeps them, because it is going to serve the next session.
            self._discard_heartbeat_child(helper)
        outcome.shutdown_seconds = time.monotonic() - started
        if invoke_error is not None:
            # The caller's exception is the one that has to survive intact, so
            # the diagnosis rides along as a note (PEP 678): it changes neither
            # the type, the args, the string, nor anything an ``except`` can
            # match on.  ``add_note`` itself is suppressed for the same reason.
            with suppress(Exception):
                invoke_error.add_note(outcome.describe())
        return outcome

    def _observe_outbox_digest(
        self,
        *,
        phase: SourceBrokerV2OutboxPhase,
        operation_id: str,
    ) -> str:
        """Recompute the window digest here, under the write lock.

        Taken inside ``BEGIN IMMEDIATE`` for the same reason the helper takes
        it there: a digest read without the lock could be changed between the
        read and the comparison by exactly the writer it is meant to catch.
        """

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(HEARTBEAT_SELECT_SQL, (operation_id,))
                row = cursor.fetchone()
                if row is None:
                    raise SourceBrokerV2SagaIntegrityError("required outbox effect is missing")
                self._validate_outbox_row(
                    row, expected_phase=phase, expected_operation_id=operation_id
                )
                return stable_row_digest(row, cursor.description)
            finally:
                connection.rollback()

    def close(self, *, reason: str = "saga-closed") -> _HeartbeatCloseOutcome:
        """Release this saga's heartbeat helper.  Idempotent and bounded.

        Bounded by ``T_close`` = EOF window + terminate + kill.  Idempotent in
        both directions: a second call after a clean close returns the same
        outcome, and a call after an orphaned one tries again, because an
        orphan is precisely the state a later ``poll`` may resolve.

        It raises only after the cleanup has finished, and never from a
        ``finally``.  A session that is still running holds the gate, and this
        refuses rather than pulling descriptors out from under it.
        """

        if not self._heartbeat_gate.acquire(blocking=False):
            raise SourceBrokerV2SagaConflictError(
                "outbox heartbeat session is still open; close is refused"
            )
        try:
            self._heartbeat_state.operation_id = reason
            outcome = _close_resources(self._heartbeat_state)
            self._heartbeat_child = None
            if not outcome.orphaned:
                self._heartbeat_close_outcome = outcome
                self._heartbeat_finalizer.detach()
        finally:
            self._heartbeat_gate.release()
        self._sweep_orphaned_heartbeat_children()
        if outcome.orphaned or self._orphaned_heartbeat_children:
            raise SourceBrokerV2SagaUnavailableError(
                f"{outcome.describe()}; {self._describe_orphaned_heartbeat_children()}"
            )
        return outcome

    def __enter__(self) -> SourceBrokerV2Saga:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, traceback: object) -> bool:
        try:
            self.close()
        except BaseException as close_error:
            if exc is None:
                raise
            # There is already an exception on its way out of the block and it
            # is the one the caller reasons about; replacing it with a cleanup
            # failure would hide the reason the block ended.
            with suppress(Exception):
                exc.add_note(f"saga close after the block's failure: {close_error!r}")
        return False

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
        self._require_takeable_saga()
        if not self._heartbeat_gate.acquire(blocking=False):
            raise SourceBrokerV2SagaConflictError("outbox heartbeat session already open")
        try:
            token = uuid4().hex
            interval = self._executor_lease_seconds / 3
            helper, restarts = self._open_heartbeat_session(
                token=token,
                phase=phase,
                operation_id=operation_id,
                owner_generation=owner_generation,
                interval=interval,
            )
            invoke_error: BaseException | None = None
            try:
                # A session start may legitimately spend seconds - a restart
                # costs a terminate, a kill and a second handshake - and the
                # lease that covers this invocation was last renewed before all
                # of it.  One owner-and-generation guarded renewal here means
                # the window opens on a lease this executor demonstrably still
                # holds, rather than on one that may already have been taken.
                self._heartbeat_outbox(
                    phase=phase,
                    operation_id=operation_id,
                    owner_generation=owner_generation,
                )
                result = invoke(payload)
            except BaseException as exc:
                invoke_error = exc
                raise
            finally:
                outcome = self._close_heartbeat_session(
                    helper, token, invoke_error, restarts=restarts
                )
                self._last_heartbeat_session = outcome
        finally:
            self._heartbeat_gate.release()

        # Every raise below is outside the ``finally`` above, and the order is
        # fixed: an unconfirmed kill outranks an unacknowledged session, which
        # outranks a failed renewal, which outranks the digest - so a lease
        # that was legitimately taken over is still reported as the conflict it
        # has always been, not as a tamper.
        if outcome.orphaned:
            raise SourceBrokerV2SagaUnavailableError(outcome.describe())
        if not outcome.acked:
            raise SourceBrokerV2SagaUnavailableError(outcome.describe())
        if not outcome.renewal_ok:
            cause = (
                None if outcome.failure is None else outcome.failure.rebuild()
            )
            raise SourceBrokerV2SagaUnavailableError(
                f"outbox heartbeat failed during {phase.value}"
            ) from cause
        if outcome.ticks >= 1:
            # With no tick there is no sample, and this session's resolution is
            # then exactly what it was before there was a helper at all: the
            # full validation chain on both edges of the window and nothing in
            # between.  Comparing a digest that was never taken would fail
            # every ordinary invocation, because the production lease makes
            # zero ticks the common case.
            outcome.observed_digest = self._observe_outbox_digest(
                phase=phase, operation_id=operation_id
            )
            outcome.digest_mismatch = (
                outcome.last_digest != outcome.first_digest
                or outcome.last_digest != outcome.observed_digest
            )
            if outcome.digest_changed or outcome.digest_mismatch:
                raise SourceBrokerV2SagaUnavailableError(
                    f"outbox row changed during {phase.value}"
                ) from SourceBrokerV2SagaIntegrityError(outcome.describe())
        if not outcome.child_alive:
            raise SourceBrokerV2SagaUnavailableError(outcome.describe())
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
                source_phase = phase in {
                    SourceBrokerV2OutboxPhase.DISPATCH,
                    SourceBrokerV2OutboxPhase.SOURCE_FINALIZE,
                }
                started_at = datetime.now(UTC)
                if source_phase:
                    prior_started_at = _optional_executor_time(
                        row["dispatch_started_at"],
                        label="source dispatch start",
                    )
                    attempt = self._source_attempt_from_outbox(
                        row,
                        effect_operation_id=operation_id,
                    )
                    raw_grant = row["source_grant_json"]
                    if attempt is None or type(raw_grant) is not str:
                        raise SourceBrokerV2SagaIntegrityError(
                            "source invocation lacks its persisted grant attempt"
                        )
                    try:
                        grant = strict_model_validate_canonical_json(
                            SourceBrokerV2ClaimOnceResponse,
                            raw_grant.encode("utf-8"),
                        )
                    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
                        raise SourceBrokerV2SagaIntegrityError(
                            "source invocation grant is malformed"
                        ) from exc
                    grant_attempt_id = source_claim_attempt_id(
                        effect_operation_id=operation_id,
                        executor_owner_token_hash=grant.executor_owner_token_hash,
                        executor_generation=grant.executor_generation,
                        max_external_deadline=grant.max_external_deadline,
                        not_before_takeover_at=grant.not_before_takeover_at,
                    )
                    if (
                        grant.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
                        or grant.operation_id != operation_id
                        or grant.phase is not phase
                        or grant_attempt_id != attempt.attempt_id
                    ):
                        raise SourceBrokerV2SagaIntegrityError(
                            "source invocation grant is not bound to the active attempt"
                        )
                    max_deadline = grant.max_external_deadline
                    if prior_started_at is None and started_at > max_deadline:
                        raise SourceBrokerV2SagaReconcileRequiredError(
                            "source invocation did not start before its persisted deadline"
                        )
                    updated = connection.execute(
                        "UPDATE source_broker_v2_outbox SET invoke_started = 1, "
                        "dispatch_started_at = COALESCE(dispatch_started_at, ?) "
                        "WHERE operation_id = ? AND status = 'pending' "
                        "AND executor_owner_token = ? AND executor_generation = ?",
                        (
                            started_at.isoformat(),
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
        mark_stage: Callable[[str], None] = _no_heartbeat_stage,
    ) -> str:
        """Renew this executor's lease once, synchronously, in this process.

        The statement, its owner and generation guard and its rowcount check
        are the shared ones the helper runs - the same objects, not a copy - so
        a renewal written here and a renewal written there cannot drift apart.
        The full validation chain runs here because this process has it; the
        helper runs the structural subset and samples the rest by digest.
        """

        current_time = datetime.now(UTC)
        expires_at = current_time + timedelta(seconds=self._executor_lease_seconds)
        mark_stage("connect")
        with self._connect() as connection:
            try:
                return heartbeat_write(
                    connection,
                    now_iso=current_time.isoformat(),
                    expires_iso=expires_at.isoformat(),
                    operation_id=operation_id,
                    owner_token=self._executor_owner_token,
                    owner_generation=owner_generation,
                    phase=phase.value,
                    saga_id=self.saga_id,
                    validate=lambda row: self._validate_outbox_row(
                        row, expected_phase=phase, expected_operation_id=operation_id
                    ),
                    mark_stage=mark_stage,
                )
            except HeartbeatOwnershipError as exc:
                raise SourceBrokerV2SagaConflictError(
                    "outbox executor lost ownership before heartbeat"
                ) from exc
            except HeartbeatProtocolError as exc:
                # The shared write reports a missing row in its own vocabulary;
                # in this process that has always been an integrity failure and
                # callers match on it.
                raise SourceBrokerV2SagaIntegrityError("required outbox effect is missing") from exc

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
                        SourceBrokerV2SagaState.SOURCE_FINALIZE_RECONCILE_REQUIRED,
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
                stored_reconcile_reason = (
                    reconcile_reason if reconcile_reason is not None else current.reconcile_reason
                )
                if (
                    current.state is SourceBrokerV2SagaState.SOURCE_FINALIZE_RECONCILE_REQUIRED
                    and state is SourceBrokerV2SagaState.SOURCE_FINALIZED
                ):
                    stored_reconcile_reason = None
                connection.execute(
                    "UPDATE source_broker_v2_saga SET state = ?, "
                    "dispatch_outcome = ?, reconcile_reason = ? "
                    "WHERE saga_id = ?",
                    (
                        state.value,
                        None if stored_outcome is None else stored_outcome.value,
                        stored_reconcile_reason,
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
        source_attempt: _SourceClaimAttempt | None = None
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
            source_attempt = self._source_attempt_from_outbox(
                row,
                effect_operation_id=expected_operation_id,
            )
        elif any(
            row[key] is not None
            for key in (
                "dispatch_started_at",
                "max_external_deadline",
                "not_before_takeover_at",
                "source_attempt_id",
                "source_attempt_owner_hash",
                "source_attempt_generation",
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
                raw_receipt = raw_value.encode("utf-8")
                _require_canonical_json_bytes(raw_receipt, label=prefix)
                if hash_value != canonical_sha256(strict_canonical_json_loads(raw_receipt)):
                    raise SourceBrokerV2SagaIntegrityError(f"{prefix} hash conflicts")
                if prefix == "source_grant":
                    try:
                        receipt: SourceBrokerV2ClaimOnceResponse | SourceBrokerV2ReplayResponse = (
                            strict_model_validate_canonical_json(
                                SourceBrokerV2ClaimOnceResponse,
                                raw_receipt,
                            )
                        )
                    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
                        raise SourceBrokerV2SagaIntegrityError(
                            "source_grant receipt is malformed"
                        ) from exc
                else:
                    receipt = self._decode_source_observation(raw_receipt)
                if (
                    receipt.saga_id != self.saga_id
                    or receipt.operation_id != expected_operation_id
                    or receipt.phase is not expected_phase
                ):
                    raise SourceBrokerV2SagaIntegrityError(f"{prefix} receipt is foreign")
                if isinstance(receipt, SourceBrokerV2ReplayResponse):
                    if (
                        prefix != "source_observation"
                        or receipt.status is not SourceBrokerV2ReplayStatus.FOUND
                    ):
                        raise SourceBrokerV2SagaIntegrityError(
                            "source replay is not terminal observation evidence"
                        )
                    continue
                if source_attempt is None:
                    raise SourceBrokerV2SagaIntegrityError(
                        f"{prefix} claim lacks durable attempt evidence"
                    )
                receipt_attempt_id = source_claim_attempt_id(
                    effect_operation_id=expected_operation_id,
                    executor_owner_token_hash=receipt.executor_owner_token_hash,
                    executor_generation=receipt.executor_generation,
                    max_external_deadline=receipt.max_external_deadline,
                    not_before_takeover_at=receipt.not_before_takeover_at,
                )
                if receipt_attempt_id != source_attempt.attempt_id:
                    raise SourceBrokerV2SagaIntegrityError(
                        f"{prefix} claim is not bound to the active attempt"
                    )
                if prefix == "source_grant" and (
                    receipt.status is not SourceBrokerV2ClaimStatus.DEFINITIVELY_ABSENT
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
            "SELECT receipt_hash, operation_id, attempt_id, phase, status, receipt_json "
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
                receipt = self._decode_source_observation(row["receipt_json"].encode("utf-8"))
                phase = SourceBrokerV2OutboxPhase(row["phase"])
                outbox = present.get(phase.value)
                if outbox is None:
                    raise ValueError("source receipt has no outbox operation")
                if phase is SourceBrokerV2OutboxPhase.DISPATCH:
                    source_request: (
                        SourceBrokerV2DispatchRequest | SourceBrokerV2FinalizeRequest
                    ) = strict_model_validate_canonical_json(
                        SourceBrokerV2DispatchRequest,
                        str(outbox["payload_json"]).encode("utf-8"),
                    )
                else:
                    source_request = strict_model_validate_canonical_json(
                        SourceBrokerV2FinalizeRequest,
                        str(outbox["payload_json"]).encode("utf-8"),
                    )
                if isinstance(receipt, SourceBrokerV2ReplayResponse):
                    replay_request = SourceBrokerV2ReplayRequest(
                        saga_id=receipt.saga_id,
                        operation_id=receipt.operation_id,
                        phase=receipt.phase,
                        operation_request_hash=source_request.request_hash,
                        challenge=receipt.challenge,
                    )
                    self._source_authority_keyring.require_verified_replay(
                        request=replay_request,
                        receipt=receipt,
                    )
                    if receipt.result is None:
                        raise ValueError("source replay history omitted its result")
                    self._validate_source_result_binding(
                        phase=phase,
                        operation_id=receipt.operation_id,
                        operation_request_hash=source_request.request_hash,
                        result_bytes=receipt.result,
                    )
                else:
                    if receipt.operation_request_hash != source_request.request_hash:
                        raise ValueError("source claim history request is foreign")
                    self._source_authority_keyring.require_verified_claim(
                        request=_claim_request_from_receipt(receipt),
                        receipt=receipt,
                    )
                    self._validate_source_terminal_result(
                        phase=phase,
                        operation_id=receipt.operation_id,
                        operation_request_hash=source_request.request_hash,
                        response=receipt,
                    )
            except (
                SourceBrokerV2SagaIntegrityError,
                StrictJsonError,
                ValidationError,
                ValueError,
                TypeError,
            ) as exc:
                raise SourceBrokerV2SagaIntegrityError(
                    "source receipt history is malformed"
                ) from exc
            expected_operation_id = self._operation_id(phase)
            expected_attempt_id = (
                None
                if isinstance(receipt, SourceBrokerV2ReplayResponse)
                else source_claim_attempt_id(
                    effect_operation_id=expected_operation_id,
                    executor_owner_token_hash=receipt.executor_owner_token_hash,
                    executor_generation=receipt.executor_generation,
                    max_external_deadline=receipt.max_external_deadline,
                    not_before_takeover_at=receipt.not_before_takeover_at,
                )
            )
            if (
                row["receipt_hash"] != receipt.receipt_hash
                or row["operation_id"] != expected_operation_id
                or row["attempt_id"] != expected_attempt_id
                or row["status"] != receipt.status.value
                or receipt.saga_id != self.saga_id
                or receipt.phase is not phase
                or receipt.operation_id != expected_operation_id
                or (
                    isinstance(receipt, SourceBrokerV2ReplayResponse)
                    and receipt.status is not SourceBrokerV2ReplayStatus.FOUND
                )
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
            SourceBrokerV2SagaState.SOURCE_FINALIZE_RECONCILE_REQUIRED,
            SourceBrokerV2SagaState.SOURCE_FINALIZED,
        }
    ),
    SourceBrokerV2SagaState.SOURCE_FINALIZE_RECONCILE_REQUIRED: frozenset(
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
    SourceBrokerV2SagaState.SOURCE_FINALIZE_RECONCILE_REQUIRED: (
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
    SourceBrokerV2SagaState.SOURCE_FINALIZE_RECONCILE_REQUIRED: 5,
    SourceBrokerV2SagaState.SOURCE_FINALIZED: 6,
    SourceBrokerV2SagaState.QUOTA_TERMINAL: 7,
    SourceBrokerV2SagaState.PARENT_RELEASED: 8,
    SourceBrokerV2SagaState.COMPENSATED: 9,
    SourceBrokerV2SagaState.LINEAGE_PUBLISHED: 10,
    SourceBrokerV2SagaState.COMPLETE: 11,
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
