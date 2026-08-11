"""Durable parent reservation authority for source quota allocation.

The authority deliberately shares the v3 :mod:`source_quota_store` SQLite file.
It creates only its own tables, while reserving, consuming, and releasing the
existing ``quota_lease`` and ``quota_usage`` rows in the same transaction.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal, Protocol

from pydantic import Field, model_validator

from rquant.external_monotonic_root import (
    EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
    ExternalMonotonicRootRequest,
    UnixSocketExternalMonotonicRootClient,
)
from rquant.external_monotonic_root_service import ClosedExternalMonotonicRootVerifier
from rquant.resource_admission import SourceQuotaLease
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.source_quota_external_root_adapter import (
    SourceQuotaExternalCheckpoint,
    SourceQuotaExternalMonotonicRootAdapter,
    SourceQuotaExternalRootConfig,
    SourceQuotaExternalRootReceipt,
    SourceQuotaExternalRootSecurityError,
)
from rquant.source_quota_store import (
    SourceQuotaConflictError,
    SourceQuotaExhaustedError,
    SourceQuotaStore,
)
from rquant.strict_json import (
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

LEGACY_SOURCE_QUOTA_CLAIM_BINDING_HASH = canonical_sha256(
    {"contract": "rquant-source-quota-legacy-claim-binding/v1"}
)
LEGACY_SOURCE_QUOTA_CLAIM_GENERATION = 1
LEGACY_SOURCE_QUOTA_SCHEDULER_FENCING_TOKEN = 1
SOURCE_QUOTA_JOURNAL_ZERO_HASH = "0" * 64


class SourceQuotaAuthorityError(RuntimeError):
    """Base error for parent-reservation authority failures."""


class SourceQuotaAuthorityConflictError(SourceQuotaConflictError):
    """An authority request conflicts with durable state or idempotency."""


class SourceQuotaAuthorityIntegrityError(SourceQuotaAuthorityError):
    """Persisted signed journal data fails validation."""


class SourceQuotaAuthorityRepairState(RuntimeContractModel):
    status: Literal["repair_required"] = "repair_required"
    reason: Literal[
        "legacy_database_without_external_binding",
        "external_root_missing",
        "external_root_ahead_without_pending_proof",
        "external_root_diverges_at_same_high_water",
        "local_state_ahead_of_external_root",
        "external_binding_changed",
    ]
    authority_id: str = Field(min_length=1)
    local_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class SourceQuotaAuthorityRepairRequiredError(SourceQuotaAuthorityIntegrityError):
    """External monotonic state cannot be reconciled without operator repair."""

    def __init__(self, state: SourceQuotaAuthorityRepairState) -> None:
        self.state = state
        super().__init__(f"source quota authority repair required: {state.reason}")


class SourceQuotaParentState(StrEnum):
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    COMPENSATED = "COMPENSATED"


class SourceQuotaCallState(StrEnum):
    INTENT = "INTENT"
    DISPATCH_AUTHORIZED = "DISPATCH_AUTHORIZED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    UNKNOWN = "UNKNOWN"
    CANCELLED_BEFORE_DISPATCH = "CANCELLED_BEFORE_DISPATCH"


class SourceQuotaCallOutcome(StrEnum):
    """Outcome evidence that keeps pre-dispatch uncertainty distinct."""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    UNKNOWN = "UNKNOWN"
    UNKNOWN_BEFORE_DISPATCH = "UNKNOWN_BEFORE_DISPATCH"


class SourceQuotaOperationKind(StrEnum):
    RESERVE_PARENT = "reserve_parent"
    RECORD_INTENT = "record_intent"
    AUTHORIZE_DISPATCH = "authorize_dispatch"
    FINALIZE = "finalize"
    UNKNOWN_BEFORE_DISPATCH = "unknown_before_dispatch"
    CANCEL = "cancel"
    RELEASE_UNUSED = "release_unused"


class SourceQuotaReceiptSigner(Protocol):
    """A detached receipt signer; Ed25519 adapters can implement this protocol."""

    key_id: str

    def sign(self, payload: bytes) -> str: ...

    def verify(self, payload: bytes, signature: str) -> bool: ...


class SourceQuotaOperationReceipt(RuntimeContractModel):
    """Signed, timeless proof of one committed durable effect."""

    authority_id: str = Field(min_length=1)
    operation_id: str = Field(min_length=1)
    effect_key: str = Field(min_length=1)
    operation: SourceQuotaOperationKind
    claim_binding_hash: str = Field(
        default=LEGACY_SOURCE_QUOTA_CLAIM_BINDING_HASH,
        pattern=r"^[0-9a-f]{64}$",
    )
    claim_generation: int = Field(
        default=LEGACY_SOURCE_QUOTA_CLAIM_GENERATION,
        strict=True,
        ge=1,
    )
    scheduler_fencing_token: int = Field(
        default=LEGACY_SOURCE_QUOTA_SCHEDULER_FENCING_TOKEN,
        strict=True,
        ge=1,
    )
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1)
    signature: str = Field(min_length=1)

    def signing_bytes(self) -> bytes:
        """Return domain-separated canonical bytes without wall-clock metadata."""

        payload = {
            "authority_id": self.authority_id,
            "contract": "rquant-source-quota-operation-receipt/v1",
            "effect_key": self.effect_key,
            "key_id": self.key_id,
            "operation": self.operation.value,
            "operation_id": self.operation_id,
            "claim_binding_hash": self.claim_binding_hash,
            "claim_generation": self.claim_generation,
            "payload_hash": self.payload_hash,
            "result_hash": self.result_hash,
            "scheduler_fencing_token": self.scheduler_fencing_token,
        }
        return _canonical_json_bytes(payload)


class SourceQuotaCallAllocation(RuntimeContractModel):
    call_id: str = Field(min_length=1)
    parent_id: str = Field(min_length=1)
    cost: int = Field(strict=True, gt=0)
    state: SourceQuotaCallState
    outcome: SourceQuotaCallOutcome | None = None
    intended_at: AwareUtcDatetime
    authorized_at: AwareUtcDatetime | None = None
    finalized_at: AwareUtcDatetime | None = None
    usage_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_lifecycle(self) -> SourceQuotaCallAllocation:
        dispatched = self.state in {
            SourceQuotaCallState.DISPATCH_AUTHORIZED,
            SourceQuotaCallState.SUCCESS,
            SourceQuotaCallState.FAILURE,
            SourceQuotaCallState.UNKNOWN,
        }
        has_authorization_evidence = self.authorized_at is not None and self.usage_id is not None
        if dispatched and not has_authorization_evidence:
            raise ValueError("call authorization and quota usage must agree with call state")
        if not dispatched and (self.authorized_at is not None or self.usage_id is not None):
            raise ValueError("call authorization and quota usage must agree with call state")
        if self.state in {
            SourceQuotaCallState.SUCCESS,
            SourceQuotaCallState.FAILURE,
            SourceQuotaCallState.UNKNOWN,
        }:
            expected = SourceQuotaCallOutcome(self.state.value)
            if self.outcome is not expected or self.finalized_at is None:
                raise ValueError("dispatched terminal call outcome conflicts with call state")
        elif self.state is SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH:
            if self.outcome is SourceQuotaCallOutcome.UNKNOWN_BEFORE_DISPATCH:
                if self.finalized_at is None:
                    raise ValueError("pre-dispatch unknown requires terminal timestamp")
            elif self.outcome is not None or self.finalized_at is None:
                raise ValueError("cancelled call has invalid terminal outcome")
        elif self.outcome is not None or self.finalized_at is not None:
            raise ValueError("nonterminal call cannot have an outcome")
        if self.authorized_at is not None and self.authorized_at < self.intended_at:
            raise ValueError("call lifecycle timestamp order is invalid")
        if self.finalized_at is not None and self.finalized_at < self.intended_at:
            raise ValueError("call lifecycle timestamp order is invalid")
        if (
            self.authorized_at is not None
            and self.finalized_at is not None
            and self.finalized_at < self.authorized_at
        ):
            raise ValueError("call lifecycle timestamp order is invalid")
        return self


class SourceQuotaParentSnapshot(RuntimeContractModel):
    parent_id: str = Field(min_length=1)
    source: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    claim_binding_hash: str = Field(
        default=LEGACY_SOURCE_QUOTA_CLAIM_BINDING_HASH,
        pattern=r"^[0-9a-f]{64}$",
    )
    claim_generation: int = Field(
        default=LEGACY_SOURCE_QUOTA_CLAIM_GENERATION,
        strict=True,
        ge=1,
    )
    scheduler_fencing_token: int = Field(
        default=LEGACY_SOURCE_QUOTA_SCHEDULER_FENCING_TOKEN,
        strict=True,
        ge=1,
    )
    lease_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    total_cost: int = Field(strict=True, gt=0)
    reserved_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    window_id: str = Field(min_length=1)
    window_start: AwareUtcDatetime
    window_end: AwareUtcDatetime
    reset_at: AwareUtcDatetime
    capacity: int = Field(strict=True, gt=0)
    state: SourceQuotaParentState
    closed_at: AwareUtcDatetime | None = None
    reserved_cost: int = Field(strict=True, gt=0)
    consumed_cost: int = Field(strict=True, ge=0)
    unused_released: int = Field(strict=True, ge=0)
    calls: tuple[SourceQuotaCallAllocation, ...] = ()

    @model_validator(mode="after")
    def validate_accounting(self) -> SourceQuotaParentSnapshot:
        if not self.window_start <= self.reserved_at < self.window_end:
            raise ValueError("parent reservation is outside its quota window")
        if self.expires_at <= self.reserved_at or self.expires_at > self.reset_at:
            raise ValueError("parent expiry conflicts with its quota contract")
        if self.reset_at != self.window_end:
            raise ValueError("parent quota reset conflicts with its quota window")
        if self.total_cost > self.capacity:
            raise ValueError("parent reservation exceeds its quota capacity")
        if self.reserved_cost != self.total_cost:
            raise ValueError("parent reserved cost must equal its total cost")
        if self.consumed_cost > self.reserved_cost:
            raise ValueError("parent consumption exceeds reservation")
        if self.state in {SourceQuotaParentState.CLOSED, SourceQuotaParentState.COMPENSATED}:
            if self.closed_at is None:
                raise ValueError("closed parent requires close timestamp")
            if self.reserved_cost != self.consumed_cost + self.unused_released:
                raise ValueError("closed parent accounting invariant fails")
        elif self.closed_at is not None or self.unused_released != 0:
            raise ValueError("open parent cannot report released capacity")
        return self


class SourceQuotaAuthorityResult(RuntimeContractModel):
    receipt: SourceQuotaOperationReceipt
    parent: SourceQuotaParentSnapshot
    call: SourceQuotaCallAllocation | None = None

    @model_validator(mode="after")
    def validate_receipt_result(self) -> SourceQuotaAuthorityResult:
        if self.receipt.result_hash != _result_hash(self.parent, self.call):
            raise ValueError("receipt result hash conflicts with authority result")
        return self


_TERMINAL_CALL_STATES = frozenset(
    {
        SourceQuotaCallState.SUCCESS.value,
        SourceQuotaCallState.FAILURE.value,
        SourceQuotaCallState.UNKNOWN.value,
        SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH.value,
    }
)
_UNTERMINATED_CALL_STATES = (
    SourceQuotaCallState.INTENT.value,
    SourceQuotaCallState.DISPATCH_AUTHORIZED.value,
)

_PARENT_REPLAY_TRANSITIONS: dict[SourceQuotaParentState, frozenset[SourceQuotaParentState]] = {
    SourceQuotaParentState.OPEN: frozenset(
        {
            SourceQuotaParentState.OPEN,
            SourceQuotaParentState.CLOSED,
            SourceQuotaParentState.COMPENSATED,
        }
    ),
    SourceQuotaParentState.CLOSED: frozenset({SourceQuotaParentState.CLOSED}),
    SourceQuotaParentState.COMPENSATED: frozenset({SourceQuotaParentState.COMPENSATED}),
}

_CALL_REPLAY_TRANSITIONS: dict[SourceQuotaCallState, frozenset[SourceQuotaCallState]] = {
    SourceQuotaCallState.INTENT: frozenset(
        {
            SourceQuotaCallState.INTENT,
            SourceQuotaCallState.DISPATCH_AUTHORIZED,
            SourceQuotaCallState.SUCCESS,
            SourceQuotaCallState.FAILURE,
            SourceQuotaCallState.UNKNOWN,
            SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH,
        }
    ),
    SourceQuotaCallState.DISPATCH_AUTHORIZED: frozenset(
        {
            SourceQuotaCallState.DISPATCH_AUTHORIZED,
            SourceQuotaCallState.SUCCESS,
            SourceQuotaCallState.FAILURE,
            SourceQuotaCallState.UNKNOWN,
        }
    ),
    SourceQuotaCallState.SUCCESS: frozenset({SourceQuotaCallState.SUCCESS}),
    SourceQuotaCallState.FAILURE: frozenset({SourceQuotaCallState.FAILURE}),
    SourceQuotaCallState.UNKNOWN: frozenset({SourceQuotaCallState.UNKNOWN}),
    SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH: frozenset(
        {SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH}
    ),
}


def _canonical_json_bytes(value: Mapping[str, object]) -> bytes:
    encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return encoded.encode("utf-8")


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _normalize_now(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _parse_stored_time(value: object) -> datetime:
    try:
        return _normalize_now(datetime.fromisoformat(value))
    except (TypeError, ValueError) as exc:
        raise SourceQuotaAuthorityIntegrityError("stored quota timestamp is invalid") from exc


def _require_stored_quota_int(value: object) -> int:
    if type(value) is not int:
        raise SourceQuotaAuthorityIntegrityError("stored quota numeric value is not an integer")
    return value


def _require_nonempty(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} must be nonempty")
    return normalized


def _require_claim_binding(
    *,
    claim_binding_hash: str,
    claim_generation: int,
    scheduler_fencing_token: int,
) -> tuple[str, int, int]:
    if (
        not isinstance(claim_binding_hash, str)
        or len(claim_binding_hash) != 64
        or any(character not in "0123456789abcdef" for character in claim_binding_hash)
    ):
        raise ValueError("claim_binding_hash must be a lowercase sha256")
    if type(claim_generation) is not int or claim_generation < 1:
        raise ValueError("claim_generation must be a positive int")
    if type(scheduler_fencing_token) is not int or scheduler_fencing_token < 1:
        raise ValueError("scheduler_fencing_token must be a positive int")
    return claim_binding_hash, claim_generation, scheduler_fencing_token


def _quota_contract_result_payload(parent: SourceQuotaParentSnapshot) -> dict[str, object]:
    return {
        "capacity": parent.capacity,
        "claim_binding_hash": parent.claim_binding_hash,
        "claim_generation": parent.claim_generation,
        "expires_at": _iso(parent.expires_at),
        "lease_id": parent.lease_id,
        "owner": parent.owner,
        "parent_id": parent.parent_id,
        "reserved_cost": parent.reserved_cost,
        "reset_at": _iso(parent.reset_at),
        "source": parent.source,
        "scheduler_fencing_token": parent.scheduler_fencing_token,
        "total_cost": parent.total_cost,
        "window_end": _iso(parent.window_end),
        "window_id": parent.window_id,
        "window_start": _iso(parent.window_start),
    }


def _reserve_request_payload(
    *,
    parent_id: str,
    source: str,
    owner: str,
    total_cost: int,
    expires_at: datetime,
    claim_binding_hash: str,
    claim_generation: int,
    scheduler_fencing_token: int,
) -> dict[str, object]:
    return {
        "claim_binding_hash": claim_binding_hash,
        "claim_generation": claim_generation,
        "expires_at": _iso(expires_at),
        "owner": owner,
        "parent_id": parent_id,
        "source": source,
        "scheduler_fencing_token": scheduler_fencing_token,
        "total_cost": total_cost,
    }


def _request_hash(operation: SourceQuotaOperationKind, payload: Mapping[str, object]) -> str:
    return canonical_sha256({"operation": operation.value, "request": payload})


def _result_hash(
    parent: SourceQuotaParentSnapshot,
    call: SourceQuotaCallAllocation | None,
) -> str:
    """Hash the timeless business effect and its immutable quota contract.

    Authority-assigned lifecycle timestamps remain in the separately signed
    result evidence.  The receipt intentionally excludes process and clock
    metadata while binding the parent and frozen quota contract that made the
    durable effect valid.
    """

    return canonical_sha256(
        {
            "parent": {
                "parent_id": parent.parent_id,
                "source": parent.source,
                "owner": parent.owner,
                "claim_binding_hash": parent.claim_binding_hash,
                "claim_generation": parent.claim_generation,
                "quota_contract": _quota_contract_result_payload(parent),
                "total_cost": parent.total_cost,
                "state": parent.state.value,
                "reserved_cost": parent.reserved_cost,
                "consumed_cost": parent.consumed_cost,
                "unused_released": parent.unused_released,
                "scheduler_fencing_token": parent.scheduler_fencing_token,
                "calls": [
                    {
                        "call_id": allocation.call_id,
                        "parent_id": allocation.parent_id,
                        "cost": allocation.cost,
                        "state": allocation.state.value,
                        "outcome": None if allocation.outcome is None else allocation.outcome.value,
                    }
                    for allocation in sorted(
                        parent.calls,
                        key=lambda allocation: allocation.call_id,
                    )
                ],
            },
            "call": None
            if call is None
            else {
                "call_id": call.call_id,
                "parent_id": call.parent_id,
                "cost": call.cost,
                "state": call.state.value,
                "outcome": None if call.outcome is None else call.outcome.value,
            },
        }
    )


def _replay_payload_hash(result_json: str) -> str:
    return hashlib.sha256(result_json.encode("utf-8")).hexdigest()


def _result_json(
    parent: SourceQuotaParentSnapshot,
    call: SourceQuotaCallAllocation | None,
) -> str:
    return json.dumps(
        {
            "call": None if call is None else call.model_dump(mode="json"),
            "parent": parent.model_dump(mode="json"),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _replay_payload_signing_bytes(
    *,
    authority_id: str,
    operation_id: str,
    effect_key: str,
    operation: str,
    payload_hash: str,
    result_hash: str,
    result_integrity_hash: str,
    key_id: str,
) -> bytes:
    return _canonical_json_bytes(
        {
            "authority_id": authority_id,
            "contract": "rquant-source-quota-operation-replay-integrity/v1",
            "effect_key": effect_key,
            "key_id": key_id,
            "operation": operation,
            "operation_id": operation_id,
            "payload_hash": payload_hash,
            "result_hash": result_hash,
            "result_integrity_hash": result_integrity_hash,
        }
    )


def _operation_chain_hash(
    *,
    parent_id: str,
    parent_ordinal: int,
    previous_operation_hash: str,
    global_ordinal: int,
    previous_global_hash: str,
    operation_id: str,
    effect_key: str,
    operation: str,
    payload_hash: str,
    request_hash: str,
    result_hash: str,
    result_integrity_hash: str,
    result_integrity_signature: str,
    receipt_json: str,
) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-source-quota-parent-journal-entry/v1",
            "effect_key": effect_key,
            "global_ordinal": global_ordinal,
            "operation": operation,
            "operation_id": operation_id,
            "parent_id": parent_id,
            "parent_ordinal": parent_ordinal,
            "payload_hash": payload_hash,
            "previous_global_hash": previous_global_hash,
            "previous_operation_hash": previous_operation_hash,
            "receipt_json_hash": hashlib.sha256(receipt_json.encode("utf-8")).hexdigest(),
            "request_hash": request_hash,
            "result_hash": result_hash,
            "result_integrity_hash": result_integrity_hash,
            "result_integrity_signature": result_integrity_signature,
        }
    )


def _parent_materialized_hash(parent: SourceQuotaParentSnapshot) -> str:
    return canonical_sha256(
        {
            "contract": "rquant-source-quota-parent-materialized-state/v1",
            "parent": parent.model_dump(mode="json"),
        }
    )


def _parent_clock_high_water(parent: SourceQuotaParentSnapshot) -> datetime:
    observed = [parent.reserved_at]
    if parent.closed_at is not None:
        observed.append(parent.closed_at)
    for allocation in parent.calls:
        observed.append(allocation.intended_at)
        if allocation.authorized_at is not None:
            observed.append(allocation.authorized_at)
        if allocation.finalized_at is not None:
            observed.append(allocation.finalized_at)
    return max(observed)


def _parent_checkpoint_signing_bytes(
    *,
    authority_id: str,
    parent_id: str,
    operation_count: int,
    head_operation_hash: str,
    materialized_state_hash: str,
    clock_high_water: str,
    key_id: str,
) -> bytes:
    return _canonical_json_bytes(
        {
            "authority_id": authority_id,
            "clock_high_water": clock_high_water,
            "contract": "rquant-source-quota-parent-checkpoint/v1",
            "head_operation_hash": head_operation_hash,
            "key_id": key_id,
            "materialized_state_hash": materialized_state_hash,
            "operation_count": operation_count,
            "parent_id": parent_id,
        }
    )


def _global_checkpoint_signing_bytes(
    *,
    authority_id: str,
    journal_count: int,
    mutation_counter: int,
    global_head_hash: str,
    clock_high_water: str | None,
    key_id: str,
) -> bytes:
    return _canonical_json_bytes(
        {
            "authority_id": authority_id,
            "clock_high_water": clock_high_water,
            "contract": "rquant-source-quota-global-checkpoint/v1",
            "global_head_hash": global_head_hash,
            "journal_count": journal_count,
            "key_id": key_id,
            "mutation_counter": mutation_counter,
        }
    )


class SourceQuotaParentAuthority:
    """Own the parent reservation and its dispatch-authorized child calls."""

    def __init__(
        self,
        path: Path,
        *,
        authority_id: str,
        signer: SourceQuotaReceiptSigner,
        external_root_config: SourceQuotaExternalRootConfig,
        external_root_client: UnixSocketExternalMonotonicRootClient,
        external_root_verifiers: tuple[ClosedExternalMonotonicRootVerifier, ...],
        busy_timeout_ms: int = 5_000,
    ) -> None:
        try:
            root = SourceQuotaExternalMonotonicRootAdapter(
                config=external_root_config,
                client=external_root_client,
                root_verifiers=external_root_verifiers,
            )
        except SourceQuotaExternalRootSecurityError as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "production source quota authority requires the closed external root"
            ) from exc
        self._configure(
            path=path,
            authority_id=authority_id,
            signer=signer,
            root=root,
            mode="production",
            busy_timeout_ms=busy_timeout_ms,
        )
        schema_existed = self._initialize()
        self._synchronize_external_root(schema_existed=schema_existed)

    @classmethod
    def for_nonproduction_standalone(
        cls,
        path: Path,
        *,
        authority_id: str,
        signer: SourceQuotaReceiptSigner,
        busy_timeout_ms: int = 5_000,
    ) -> SourceQuotaParentAuthority:
        instance = cls.__new__(cls)
        instance._configure(
            path=path,
            authority_id=authority_id,
            signer=signer,
            root=None,
            mode="test-standalone",
            busy_timeout_ms=busy_timeout_ms,
        )
        instance._initialize()
        return instance

    @classmethod
    def for_nonproduction_external_test(
        cls,
        path: Path,
        *,
        authority_id: str,
        signer: SourceQuotaReceiptSigner,
        external_root: SourceQuotaExternalMonotonicRootAdapter,
        busy_timeout_ms: int = 5_000,
    ) -> SourceQuotaParentAuthority:
        if (
            type(external_root) is not SourceQuotaExternalMonotonicRootAdapter
            or external_root.production_ready
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "test external source quota authority requires the explicit nonproduction adapter"
            )
        instance = cls.__new__(cls)
        instance._configure(
            path=path,
            authority_id=authority_id,
            signer=signer,
            root=external_root,
            mode="test-external",
            busy_timeout_ms=busy_timeout_ms,
        )
        schema_existed = instance._initialize()
        instance._synchronize_external_root(schema_existed=schema_existed)
        return instance

    def _configure(
        self,
        *,
        path: Path,
        authority_id: str,
        signer: SourceQuotaReceiptSigner,
        root: SourceQuotaExternalMonotonicRootAdapter | None,
        mode: Literal["production", "test-external", "test-standalone"],
        busy_timeout_ms: int,
    ) -> None:
        self.path = Path(path)
        self.authority_id = _require_nonempty(authority_id, label="authority_id")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        key_id = _require_nonempty(signer.key_id, label="signer.key_id")
        if not callable(signer.sign) or not callable(signer.verify):
            raise ValueError("signer must provide sign and verify methods")
        self._signer = signer
        self._key_id = key_id
        self._root = root
        self._mode = mode
        self._external_binding_hash = (
            EXTERNAL_MONOTONIC_ROOT_ZERO_HASH
            if root is None
            else canonical_sha256(
                {
                    "authority_id": self.authority_id,
                    "contract": "rquant-source-quota-external-binding/v1",
                    "local_rollback_domain_id": root.config.local_rollback_domain_id,
                    "root_config_hash": root.config.config_hash,
                    "signer_key_id": key_id,
                }
            )
        )
        self._store = SourceQuotaStore(self.path, busy_timeout_ms=busy_timeout_ms)

    def reserve_parent(
        self,
        *,
        operation_id: str,
        parent_id: str,
        source: str,
        owner: str,
        total_cost: int,
        now: datetime,
        expires_at: datetime,
        claim_binding_hash: str = LEGACY_SOURCE_QUOTA_CLAIM_BINDING_HASH,
        claim_generation: int = LEGACY_SOURCE_QUOTA_CLAIM_GENERATION,
        scheduler_fencing_token: int = LEGACY_SOURCE_QUOTA_SCHEDULER_FENCING_TOKEN,
    ) -> SourceQuotaAuthorityResult:
        identifier = _require_nonempty(parent_id, label="parent_id")
        normalized_source = _require_nonempty(source, label="source")
        normalized_owner = _require_nonempty(owner, label="owner")
        if type(total_cost) is not int or total_cost < 1:
            raise ValueError("total_cost must be a positive int")
        observed = _normalize_now(now)
        expires = _normalize_now(expires_at)
        binding_hash, generation, fencing_token = _require_claim_binding(
            claim_binding_hash=claim_binding_hash,
            claim_generation=claim_generation,
            scheduler_fencing_token=scheduler_fencing_token,
        )

        payload = _reserve_request_payload(
            parent_id=identifier,
            source=normalized_source,
            owner=normalized_owner,
            total_cost=total_cost,
            expires_at=expires,
            claim_binding_hash=binding_hash,
            claim_generation=generation,
            scheduler_fencing_token=fencing_token,
        )

        def apply(
            connection: sqlite3.Connection,
        ) -> tuple[SourceQuotaParentSnapshot, SourceQuotaCallAllocation | None]:
            existing = self._parent_row(connection, identifier)
            if existing is not None:
                raise SourceQuotaAuthorityConflictError("parent reservation already exists")
            self._reject_clock_rollback(connection, observed)
            window = SourceQuotaStore._active_window(connection, normalized_source, observed)
            reset = _parse_stored_time(window["resets_at"])
            if expires <= observed or expires > reset:
                raise ValueError("expires_at must be after now and no later than quota reset")
            remaining = self._remaining_in_window(connection, window, observed)
            if remaining < total_cost:
                raise SourceQuotaExhaustedError(
                    f"quota exhausted: requested={total_cost}, remaining={remaining}"
                )
            lease = SourceQuotaLease(
                source=normalized_source,
                owner=normalized_owner,
                units=total_cost,
                granted_at=observed,
                expires_at=expires,
                quota_reset_at=reset,
            )
            connection.execute(
                """
                INSERT INTO quota_lease(
                    lease_id, source, window_id, owner, units, used_units,
                    granted_at, expires_at, quota_reset_at, released_at
                ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL)
                """,
                (
                    lease.lease_id,
                    lease.source,
                    window["window_id"],
                    lease.owner,
                    lease.units,
                    _iso(lease.granted_at),
                    _iso(lease.expires_at),
                    _iso(lease.quota_reset_at),
                ),
            )
            connection.execute(
                """
                INSERT INTO source_parent_reservation(
                    parent_id, source, owner, claim_binding_hash, claim_generation,
                    scheduler_fencing_token, lease_id, total_cost, reserved_at, state, closed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', NULL)
                """,
                (
                    identifier,
                    normalized_source,
                    normalized_owner,
                    binding_hash,
                    generation,
                    fencing_token,
                    lease.lease_id,
                    total_cost,
                    _iso(observed),
                ),
            )
            return self._snapshot_parent(connection, identifier), None

        return self._operate(
            operation_id=operation_id,
            effect_key=f"reserve-parent:{identifier}",
            operation=SourceQuotaOperationKind.RESERVE_PARENT,
            payload=payload,
            apply=apply,
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
        identifier = _require_nonempty(parent_id, label="parent_id")
        call_identifier = _require_nonempty(call_id, label="call_id")
        if type(cost) is not int or cost < 1:
            raise ValueError("cost must be a positive int")
        observed = _normalize_now(now)
        payload = {"parent_id": identifier, "call_id": call_identifier, "cost": cost}

        def apply(
            connection: sqlite3.Connection,
        ) -> tuple[SourceQuotaParentSnapshot, SourceQuotaCallAllocation | None]:
            parent = self._require_open_parent(connection, identifier)
            lease = self._require_parent_lease(connection, parent)
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(parent["reserved_at"]),
            )
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(lease["granted_at"]),
            )
            self._reject_clock_rollback(connection, observed)
            existing = self._call_row(connection, call_identifier)
            if existing is not None:
                raise SourceQuotaAuthorityConflictError("call allocation already exists")
            allocated = _require_stored_quota_int(
                connection.execute(
                    "SELECT COALESCE(SUM(cost), 0) FROM source_call_allocation WHERE parent_id = ?",
                    (identifier,),
                ).fetchone()[0]
            )
            if allocated + cost > _require_stored_quota_int(parent["total_cost"]):
                raise SourceQuotaAuthorityConflictError(
                    "call allocation exceeds parent reservation"
                )
            connection.execute(
                """
                INSERT INTO source_call_allocation(
                    call_id, parent_id, cost, state, outcome, intended_at,
                    authorized_at, finalized_at, usage_id
                ) VALUES (?, ?, ?, 'INTENT', NULL, ?, NULL, NULL, NULL)
                """,
                (call_identifier, identifier, cost, _iso(observed)),
            )
            return (
                self._snapshot_parent(connection, identifier),
                self._snapshot_call(connection, call_identifier),
            )

        return self._operate(
            operation_id=operation_id,
            effect_key=f"record-intent:{identifier}:{call_identifier}",
            operation=SourceQuotaOperationKind.RECORD_INTENT,
            payload=payload,
            apply=apply,
        )

    def authorize_dispatch(
        self,
        *,
        operation_id: str,
        parent_id: str,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        identifier = _require_nonempty(parent_id, label="parent_id")
        call_identifier = _require_nonempty(call_id, label="call_id")
        observed = _normalize_now(now)
        payload = {"parent_id": identifier, "call_id": call_identifier}

        def apply(
            connection: sqlite3.Connection,
        ) -> tuple[SourceQuotaParentSnapshot, SourceQuotaCallAllocation | None]:
            parent = self._require_open_parent(connection, identifier)
            call = self._require_call(connection, call_identifier, identifier)
            if call["state"] != SourceQuotaCallState.INTENT.value:
                raise SourceQuotaAuthorityConflictError(
                    "call must be INTENT before dispatch authorization"
                )
            lease = self._require_parent_lease(connection, parent)
            if lease["released_at"] is not None:
                raise SourceQuotaAuthorityConflictError("parent quota lease is not active")
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(parent["reserved_at"]),
            )
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(lease["granted_at"]),
            )
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(call["intended_at"]),
            )
            self._reject_clock_rollback(connection, observed)
            expires = _parse_stored_time(lease["expires_at"])
            if expires <= observed:
                raise SourceQuotaAuthorityConflictError("parent quota lease has expired")
            lease_used_units = _require_stored_quota_int(lease["used_units"])
            call_cost = _require_stored_quota_int(call["cost"])
            lease_units = _require_stored_quota_int(lease["units"])
            if lease_used_units + call_cost > lease_units:
                raise SourceQuotaAuthorityConflictError(
                    "dispatch consumption exceeds parent reservation"
                )
            usage_id = canonical_sha256(
                {
                    "call_id": call_identifier,
                    "contract": "rquant-source-parent-quota-usage/v1",
                    "parent_id": identifier,
                }
            )
            connection.execute(
                "UPDATE quota_lease SET used_units = used_units + ? WHERE lease_id = ?",
                (call["cost"], parent["lease_id"]),
            )
            connection.execute(
                """
                INSERT INTO quota_usage(usage_id, lease_id, units, consumed_at)
                VALUES (?, ?, ?, ?)
                """,
                (usage_id, parent["lease_id"], call["cost"], _iso(observed)),
            )
            changed = connection.execute(
                """
                UPDATE source_call_allocation
                SET state = 'DISPATCH_AUTHORIZED', authorized_at = ?, usage_id = ?
                WHERE call_id = ? AND parent_id = ? AND state = 'INTENT'
                """,
                (_iso(observed), usage_id, call_identifier, identifier),
            )
            if changed.rowcount != 1:
                raise SourceQuotaAuthorityConflictError("dispatch authorization CAS failed")
            return (
                self._snapshot_parent(connection, identifier),
                self._snapshot_call(connection, call_identifier),
            )

        return self._operate(
            operation_id=operation_id,
            effect_key=f"authorize-dispatch:{identifier}:{call_identifier}",
            operation=SourceQuotaOperationKind.AUTHORIZE_DISPATCH,
            payload=payload,
            apply=apply,
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
        if outcome is SourceQuotaCallOutcome.UNKNOWN_BEFORE_DISPATCH:
            raise ValueError("UNKNOWN_BEFORE_DISPATCH must be terminalized before dispatch")
        if outcome not in {
            SourceQuotaCallOutcome.SUCCESS,
            SourceQuotaCallOutcome.FAILURE,
            SourceQuotaCallOutcome.UNKNOWN,
        }:
            raise ValueError("finalize requires a dispatched terminal outcome")
        identifier = _require_nonempty(parent_id, label="parent_id")
        call_identifier = _require_nonempty(call_id, label="call_id")
        observed = _normalize_now(now)
        payload = {"parent_id": identifier, "call_id": call_identifier, "outcome": outcome.value}

        def apply(
            connection: sqlite3.Connection,
        ) -> tuple[SourceQuotaParentSnapshot, SourceQuotaCallAllocation | None]:
            self._require_open_parent(connection, identifier)
            call = self._require_call(connection, call_identifier, identifier)
            if call["state"] != SourceQuotaCallState.DISPATCH_AUTHORIZED.value:
                raise SourceQuotaAuthorityConflictError(
                    "call must be DISPATCH_AUTHORIZED before finalization"
                )
            authorized_at = call["authorized_at"]
            if authorized_at is None:
                raise SourceQuotaAuthorityIntegrityError("authorized call is missing its timestamp")
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(call["intended_at"]),
            )
            self._require_lifecycle_time(observed, _parse_stored_time(authorized_at))
            self._reject_clock_rollback(connection, observed)
            changed = connection.execute(
                """
                UPDATE source_call_allocation
                SET state = ?, outcome = ?, finalized_at = ?
                WHERE call_id = ? AND parent_id = ? AND state = 'DISPATCH_AUTHORIZED'
                """,
                (outcome.value, outcome.value, _iso(observed), call_identifier, identifier),
            )
            if changed.rowcount != 1:
                raise SourceQuotaAuthorityConflictError("finalization CAS failed")
            return (
                self._snapshot_parent(connection, identifier),
                self._snapshot_call(connection, call_identifier),
            )

        return self._operate(
            operation_id=operation_id,
            effect_key=f"finalize:{identifier}:{call_identifier}",
            operation=SourceQuotaOperationKind.FINALIZE,
            payload=payload,
            apply=apply,
        )

    def terminalize_unknown_before_dispatch(
        self,
        *,
        operation_id: str,
        parent_id: str,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._terminalize_before_dispatch(
            operation_id=operation_id,
            parent_id=parent_id,
            call_id=call_id,
            now=now,
            unknown=True,
        )

    def cancel(
        self,
        *,
        operation_id: str,
        parent_id: str,
        call_id: str,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        return self._terminalize_before_dispatch(
            operation_id=operation_id,
            parent_id=parent_id,
            call_id=call_id,
            now=now,
            unknown=False,
        )

    def release_unused(
        self,
        *,
        operation_id: str,
        parent_id: str,
        now: datetime,
    ) -> SourceQuotaAuthorityResult:
        identifier = _require_nonempty(parent_id, label="parent_id")
        observed = _normalize_now(now)
        payload = {"parent_id": identifier}

        def apply(
            connection: sqlite3.Connection,
        ) -> tuple[SourceQuotaParentSnapshot, SourceQuotaCallAllocation | None]:
            parent = self._require_open_parent(connection, identifier)
            unresolved = connection.execute(
                """
                SELECT call_id FROM source_call_allocation
                WHERE parent_id = ? AND state IN (?, ?)
                LIMIT 1
                """,
                (identifier, *_UNTERMINATED_CALL_STATES),
            ).fetchone()
            if unresolved is not None:
                raise SourceQuotaAuthorityConflictError(
                    f"parent has unresolved call {unresolved['call_id']}"
                )
            closing = connection.execute(
                """
                UPDATE source_parent_reservation SET state = 'CLOSING'
                WHERE parent_id = ? AND state = 'OPEN'
                """,
                (identifier,),
            )
            if closing.rowcount != 1:
                raise SourceQuotaAuthorityConflictError("parent release CAS failed")
            lease = connection.execute(
                "SELECT * FROM quota_lease WHERE lease_id = ?", (parent["lease_id"],)
            ).fetchone()
            if lease is None or lease["released_at"] is not None:
                raise SourceQuotaAuthorityConflictError("parent quota lease is not active")
            granted = _parse_stored_time(lease["granted_at"])
            if observed < granted:
                raise SourceQuotaAuthorityConflictError("release precedes parent reservation")
            self._reject_clock_rollback(connection, observed)
            consumed = _require_stored_quota_int(lease["used_units"])
            unused = _require_stored_quota_int(parent["total_cost"]) - consumed
            if unused < 0:
                raise SourceQuotaAuthorityIntegrityError(
                    "parent lease consumption exceeds reservation"
                )
            connection.execute(
                "UPDATE quota_lease SET released_at = ? WHERE lease_id = ? AND released_at IS NULL",
                (_iso(observed), parent["lease_id"]),
            )
            final_state = (
                SourceQuotaParentState.COMPENSATED.value
                if unused > 0
                else SourceQuotaParentState.CLOSED.value
            )
            connection.execute(
                """
                UPDATE source_parent_reservation SET state = ?, closed_at = ?
                WHERE parent_id = ? AND state = 'CLOSING'
                """,
                (final_state, _iso(observed), identifier),
            )
            return self._snapshot_parent(connection, identifier), None

        return self._operate(
            operation_id=operation_id,
            effect_key=f"release-unused:{identifier}",
            operation=SourceQuotaOperationKind.RELEASE_UNUSED,
            payload=payload,
            apply=apply,
        )

    def get_parent(self, parent_id: str) -> SourceQuotaParentSnapshot | None:
        identifier = _require_nonempty(parent_id, label="parent_id")
        with self._store._connect() as connection:
            connection.execute("BEGIN")
            try:
                if self._parent_row(connection, identifier) is None:
                    connection.rollback()
                    return None
                snapshot = self._snapshot_parent(connection, identifier)
                connection.commit()
                return snapshot
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _drop_journal_guard_triggers(connection: sqlite3.Connection) -> None:
        for suffix in ("insert", "update", "delete"):
            connection.execute(f"DROP TRIGGER IF EXISTS source_quota_operation_guard_{suffix}")

    @staticmethod
    def _create_journal_guard_triggers(connection: sqlite3.Connection) -> None:
        for event in ("INSERT", "UPDATE", "DELETE"):
            connection.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS source_quota_operation_guard_{event.lower()}
                AFTER {event} ON source_quota_operation
                BEGIN
                    UPDATE source_quota_global_checkpoint
                    SET mutation_counter = mutation_counter + 1
                    WHERE singleton = 1;
                END
                """
            )

    def _full_audit(self, connection: sqlite3.Connection, *, repair: bool) -> None:
        """Validate every signed row and materialize trusted incremental anchors."""

        if repair:
            self._drop_journal_guard_triggers(connection)
        rows = connection.execute(
            "SELECT rowid AS legacy_ordinal, * FROM source_quota_operation ORDER BY rowid"
        ).fetchall()
        previous_by_parent: dict[str, SourceQuotaAuthorityResult] = {}
        count_by_parent: dict[str, int] = {}
        head_by_parent: dict[str, str] = {}
        global_head = SOURCE_QUOTA_JOURNAL_ZERO_HASH
        final_by_parent: dict[str, SourceQuotaAuthorityResult] = {}
        for global_ordinal, row in enumerate(rows, start=1):
            self._validate_replay_payload_binding(row)
            result = self._validate_journal_row(connection, row)
            operation = SourceQuotaOperationKind(row["operation"])
            parent_id = result.parent.parent_id
            if row["effect_key"] != self._expected_effect_key(
                operation, result.parent, result.call
            ):
                raise SourceQuotaAuthorityIntegrityError("stored journal chain effect conflicts")
            previous = previous_by_parent.get(parent_id)
            self._validate_journal_chain_step(previous, operation, result)
            parent_ordinal = count_by_parent.get(parent_id, 0) + 1
            previous_operation_hash = head_by_parent.get(parent_id, SOURCE_QUOTA_JOURNAL_ZERO_HASH)
            operation_hash = _operation_chain_hash(
                parent_id=parent_id,
                parent_ordinal=parent_ordinal,
                previous_operation_hash=previous_operation_hash,
                global_ordinal=global_ordinal,
                previous_global_hash=global_head,
                operation_id=self._require_journal_text(row, "operation_id"),
                effect_key=self._require_journal_text(row, "effect_key"),
                operation=self._require_journal_text(row, "operation"),
                payload_hash=self._require_journal_text(row, "payload_hash"),
                request_hash=self._require_journal_text(row, "request_hash"),
                result_hash=self._require_journal_text(row, "result_hash"),
                result_integrity_hash=self._require_journal_text(row, "result_integrity_hash"),
                result_integrity_signature=self._require_journal_text(
                    row, "result_integrity_signature"
                ),
                receipt_json=self._require_journal_text(row, "receipt_json"),
            )
            if repair:
                connection.execute(
                    """
                    UPDATE source_quota_operation
                    SET parent_id = ?, parent_ordinal = ?, previous_operation_hash = ?,
                        global_ordinal = ?, previous_global_hash = ?, operation_hash = ?
                    WHERE rowid = ?
                    """,
                    (
                        parent_id,
                        parent_ordinal,
                        previous_operation_hash,
                        global_ordinal,
                        global_head,
                        operation_hash,
                        row["legacy_ordinal"],
                    ),
                )
            elif (
                row["parent_id"] != parent_id
                or type(row["parent_ordinal"]) is not int
                or row["parent_ordinal"] != parent_ordinal
                or row["previous_operation_hash"] != previous_operation_hash
                or type(row["global_ordinal"]) is not int
                or row["global_ordinal"] != global_ordinal
                or row["previous_global_hash"] != global_head
                or row["operation_hash"] != operation_hash
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal chain hash or ordinal conflicts"
                )
            previous_by_parent[parent_id] = result
            count_by_parent[parent_id] = parent_ordinal
            head_by_parent[parent_id] = operation_hash
            final_by_parent[parent_id] = result
            global_head = operation_hash

        expected_parent_ids = set(final_by_parent)
        stored_parent_ids = {
            row[0]
            for row in connection.execute(
                "SELECT parent_id FROM source_quota_parent_checkpoint"
            ).fetchall()
        }
        if repair:
            connection.execute("DELETE FROM source_quota_parent_checkpoint")
        elif stored_parent_ids != expected_parent_ids:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain parent checkpoint set conflicts"
            )

        clock_high_water: datetime | None = None
        for parent_id, result in final_by_parent.items():
            durable = self._snapshot_parent(connection, parent_id)
            if durable != result.parent:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal chain does not reach durable high-water"
                )
            parent_clock = _parent_clock_high_water(durable)
            if clock_high_water is None or parent_clock > clock_high_water:
                clock_high_water = parent_clock
            if repair:
                self._write_parent_checkpoint(
                    connection,
                    parent=durable,
                    operation_count=count_by_parent[parent_id],
                    head_operation_hash=head_by_parent[parent_id],
                )
            else:
                self._validate_parent_checkpoint(
                    connection,
                    parent_id,
                    expected_parent=durable,
                    validate_global=False,
                )

        expected_clock = None if clock_high_water is None else _iso(clock_high_water)
        if repair:
            self._write_global_checkpoint(
                connection,
                journal_count=len(rows),
                mutation_counter=len(rows),
                global_head_hash=global_head,
                clock_high_water=expected_clock,
            )
        else:
            checkpoint = self._validate_global_checkpoint(connection)
            if (
                checkpoint["journal_count"] != len(rows)
                or checkpoint["mutation_counter"] != len(rows)
                or checkpoint["global_head_hash"] != global_head
                or checkpoint["clock_high_water"] != expected_clock
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal chain global high-water conflicts"
                )

    def audit(self) -> None:
        """Run the intentionally O(history) signed journal and materialized-state audit."""

        if self._root is not None:
            self._synchronize_external_root()
        with self._store._connect() as connection:
            connection.execute("BEGIN")
            try:
                self._full_audit(connection, repair=False)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _write_parent_checkpoint(
        self,
        connection: sqlite3.Connection,
        *,
        parent: SourceQuotaParentSnapshot,
        operation_count: int,
        head_operation_hash: str,
    ) -> None:
        materialized_state_hash = _parent_materialized_hash(parent)
        clock_high_water = _iso(_parent_clock_high_water(parent))
        signing_bytes = _parent_checkpoint_signing_bytes(
            authority_id=self.authority_id,
            parent_id=parent.parent_id,
            operation_count=operation_count,
            head_operation_hash=head_operation_hash,
            materialized_state_hash=materialized_state_hash,
            clock_high_water=clock_high_water,
            key_id=self._key_id,
        )
        signature = self._signer.sign(signing_bytes)
        if not self._signer.verify(signing_bytes, signature):
            raise SourceQuotaAuthorityIntegrityError(
                "signer returned an unverifiable parent checkpoint"
            )
        connection.execute(
            """
            INSERT INTO source_quota_parent_checkpoint(
                parent_id, operation_count, head_operation_hash,
                materialized_state_hash, clock_high_water, key_id, signature
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(parent_id) DO UPDATE SET
                operation_count = excluded.operation_count,
                head_operation_hash = excluded.head_operation_hash,
                materialized_state_hash = excluded.materialized_state_hash,
                clock_high_water = excluded.clock_high_water,
                key_id = excluded.key_id,
                signature = excluded.signature
            """,
            (
                parent.parent_id,
                operation_count,
                head_operation_hash,
                materialized_state_hash,
                clock_high_water,
                self._key_id,
                signature,
            ),
        )

    def _write_global_checkpoint(
        self,
        connection: sqlite3.Connection,
        *,
        journal_count: int,
        mutation_counter: int,
        global_head_hash: str,
        clock_high_water: str | None,
    ) -> None:
        signing_bytes = _global_checkpoint_signing_bytes(
            authority_id=self.authority_id,
            journal_count=journal_count,
            mutation_counter=mutation_counter,
            global_head_hash=global_head_hash,
            clock_high_water=clock_high_water,
            key_id=self._key_id,
        )
        signature = self._signer.sign(signing_bytes)
        if not self._signer.verify(signing_bytes, signature):
            raise SourceQuotaAuthorityIntegrityError(
                "signer returned an unverifiable global checkpoint"
            )
        connection.execute(
            """
            INSERT INTO source_quota_global_checkpoint(
                singleton, journal_count, mutation_counter, global_head_hash,
                clock_high_water, key_id, signature
            ) VALUES (1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(singleton) DO UPDATE SET
                journal_count = excluded.journal_count,
                mutation_counter = excluded.mutation_counter,
                global_head_hash = excluded.global_head_hash,
                clock_high_water = excluded.clock_high_water,
                key_id = excluded.key_id,
                signature = excluded.signature
            """,
            (
                journal_count,
                mutation_counter,
                global_head_hash,
                clock_high_water,
                self._key_id,
                signature,
            ),
        )

    def _validate_global_checkpoint(self, connection: sqlite3.Connection) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM source_quota_global_checkpoint WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain global checkpoint is missing"
            )
        try:
            journal_count = _require_stored_quota_int(row["journal_count"])
            mutation_counter = _require_stored_quota_int(row["mutation_counter"])
            global_head_hash = self._require_journal_text(row, "global_head_hash")
            key_id = self._require_journal_text(row, "key_id")
            signature = self._require_journal_text(row, "signature")
            clock_high_water = row["clock_high_water"]
            if clock_high_water is not None:
                if type(clock_high_water) is not str:
                    raise SourceQuotaAuthorityIntegrityError(
                        "stored journal chain clock high-water must be text"
                    )
                _parse_stored_time(clock_high_water)
            signing_bytes = _global_checkpoint_signing_bytes(
                authority_id=self.authority_id,
                journal_count=journal_count,
                mutation_counter=mutation_counter,
                global_head_hash=global_head_hash,
                clock_high_water=clock_high_water,
                key_id=key_id,
            )
            verified = key_id == self._key_id and self._signer.verify(signing_bytes, signature)
        except SourceQuotaAuthorityIntegrityError:
            raise
        except Exception as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain checkpoint hash/signature verification failed"
            ) from exc
        if (
            journal_count < 0
            or mutation_counter != journal_count
            or len(global_head_hash) != 64
            or not verified
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain checkpoint hash/signature conflicts"
            )
        return row

    def _validate_parent_checkpoint(
        self,
        connection: sqlite3.Connection,
        parent_id: str,
        *,
        expected_parent: SourceQuotaParentSnapshot | None = None,
        validate_global: bool = True,
        validate_materialized: bool = True,
    ) -> sqlite3.Row | None:
        if validate_global:
            self._validate_global_checkpoint(connection)
        row = connection.execute(
            "SELECT * FROM source_quota_parent_checkpoint WHERE parent_id = ?",
            (parent_id,),
        ).fetchone()
        durable_row = self._parent_row(connection, parent_id)
        if row is None:
            if durable_row is None:
                return None
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain parent checkpoint is missing"
            )
        if durable_row is None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain checkpoint parent is missing"
            )
        parent = (
            expected_parent or self._snapshot_parent(connection, parent_id)
            if validate_materialized
            else None
        )
        try:
            operation_count = _require_stored_quota_int(row["operation_count"])
            head_operation_hash = self._require_journal_text(row, "head_operation_hash")
            materialized_state_hash = self._require_journal_text(row, "materialized_state_hash")
            clock_high_water = self._require_journal_text(row, "clock_high_water")
            key_id = self._require_journal_text(row, "key_id")
            signature = self._require_journal_text(row, "signature")
            _parse_stored_time(clock_high_water)
            signing_bytes = _parent_checkpoint_signing_bytes(
                authority_id=self.authority_id,
                parent_id=parent_id,
                operation_count=operation_count,
                head_operation_hash=head_operation_hash,
                materialized_state_hash=materialized_state_hash,
                clock_high_water=clock_high_water,
                key_id=key_id,
            )
            verified = key_id == self._key_id and self._signer.verify(signing_bytes, signature)
        except SourceQuotaAuthorityIntegrityError:
            raise
        except Exception as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain parent checkpoint signature verification failed"
            ) from exc
        if not verified:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain parent checkpoint signature conflicts"
            )
        if operation_count < 1 or len(head_operation_hash) != 64:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain parent checkpoint shape conflicts"
            )
        if validate_materialized and (
            parent is None
            or materialized_state_hash != _parent_materialized_hash(parent)
            or clock_high_water != _iso(_parent_clock_high_water(parent))
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain does not reach durable high-water"
            )
        return row

    def get_call(self, call_id: str) -> SourceQuotaCallAllocation | None:
        identifier = _require_nonempty(call_id, label="call_id")
        with self._store._connect() as connection:
            if self._call_row(connection, identifier) is None:
                return None
            return self._snapshot_call(connection, identifier)

    def _terminalize_before_dispatch(
        self,
        *,
        operation_id: str,
        parent_id: str,
        call_id: str,
        now: datetime,
        unknown: bool,
    ) -> SourceQuotaAuthorityResult:
        identifier = _require_nonempty(parent_id, label="parent_id")
        call_identifier = _require_nonempty(call_id, label="call_id")
        observed = _normalize_now(now)
        kind = (
            SourceQuotaOperationKind.UNKNOWN_BEFORE_DISPATCH
            if unknown
            else SourceQuotaOperationKind.CANCEL
        )
        payload = {"parent_id": identifier, "call_id": call_identifier}

        def apply(
            connection: sqlite3.Connection,
        ) -> tuple[SourceQuotaParentSnapshot, SourceQuotaCallAllocation | None]:
            parent = self._require_open_parent(connection, identifier)
            call = self._require_call(connection, call_identifier, identifier)
            if call["state"] != SourceQuotaCallState.INTENT.value:
                raise SourceQuotaAuthorityConflictError(
                    "call must be INTENT before pre-dispatch terminalization"
                )
            lease = self._require_parent_lease(connection, parent)
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(parent["reserved_at"]),
            )
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(lease["granted_at"]),
            )
            self._require_lifecycle_time(
                observed,
                _parse_stored_time(call["intended_at"]),
            )
            self._reject_clock_rollback(connection, observed)
            outcome = SourceQuotaCallOutcome.UNKNOWN_BEFORE_DISPATCH.value if unknown else None
            changed = connection.execute(
                """
                UPDATE source_call_allocation
                SET state = 'CANCELLED_BEFORE_DISPATCH', outcome = ?, finalized_at = ?
                WHERE call_id = ? AND parent_id = ? AND state = 'INTENT'
                """,
                (outcome, _iso(observed), call_identifier, identifier),
            )
            if changed.rowcount != 1:
                raise SourceQuotaAuthorityConflictError("pre-dispatch terminalization CAS failed")
            return (
                self._snapshot_parent(connection, identifier),
                self._snapshot_call(connection, call_identifier),
            )

        effect_prefix = "unknown-before-dispatch" if unknown else "cancel"
        return self._operate(
            operation_id=operation_id,
            effect_key=f"{effect_prefix}:{identifier}:{call_identifier}",
            operation=kind,
            payload=payload,
            apply=apply,
        )

    def _initialize(self) -> bool:
        statements = (
            """
            CREATE TABLE IF NOT EXISTS source_parent_reservation (
                parent_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                owner TEXT NOT NULL,
                claim_binding_hash TEXT NOT NULL CHECK(length(claim_binding_hash) = 64),
                claim_generation INTEGER NOT NULL CHECK(claim_generation > 0),
                scheduler_fencing_token INTEGER NOT NULL CHECK(scheduler_fencing_token > 0),
                lease_id TEXT NOT NULL UNIQUE REFERENCES quota_lease(lease_id),
                total_cost INTEGER NOT NULL CHECK(total_cost > 0),
                reserved_at TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('OPEN', 'CLOSING', 'CLOSED', 'COMPENSATED')),
                closed_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS source_call_allocation (
                call_id TEXT PRIMARY KEY,
                parent_id TEXT NOT NULL REFERENCES source_parent_reservation(parent_id),
                cost INTEGER NOT NULL CHECK(cost > 0),
                state TEXT NOT NULL CHECK(state IN (
                    'INTENT', 'DISPATCH_AUTHORIZED', 'SUCCESS', 'FAILURE', 'UNKNOWN',
                    'CANCELLED_BEFORE_DISPATCH'
                )),
                outcome TEXT CHECK(outcome IN (
                    'SUCCESS', 'FAILURE', 'UNKNOWN', 'UNKNOWN_BEFORE_DISPATCH'
                )),
                intended_at TEXT NOT NULL,
                authorized_at TEXT,
                finalized_at TEXT,
                usage_id TEXT UNIQUE REFERENCES quota_usage(usage_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS source_quota_operation (
                operation_id TEXT PRIMARY KEY,
                effect_key TEXT NOT NULL UNIQUE,
                operation TEXT NOT NULL,
                parent_id TEXT NOT NULL,
                parent_ordinal INTEGER NOT NULL CHECK(parent_ordinal > 0),
                previous_operation_hash TEXT NOT NULL CHECK(length(previous_operation_hash) = 64),
                global_ordinal INTEGER NOT NULL CHECK(global_ordinal > 0),
                previous_global_hash TEXT NOT NULL CHECK(length(previous_global_hash) = 64),
                payload_hash TEXT NOT NULL CHECK(length(payload_hash) = 64),
                request_hash TEXT CHECK(length(request_hash) = 64),
                result_hash TEXT NOT NULL CHECK(length(result_hash) = 64),
                result_json TEXT NOT NULL,
                result_integrity_hash TEXT CHECK(length(result_integrity_hash) = 64),
                result_integrity_signature TEXT,
                receipt_json TEXT NOT NULL,
                operation_hash TEXT NOT NULL CHECK(length(operation_hash) = 64)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS source_quota_global_checkpoint (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                journal_count INTEGER NOT NULL CHECK(journal_count >= 0),
                mutation_counter INTEGER NOT NULL CHECK(mutation_counter >= 0),
                global_head_hash TEXT NOT NULL CHECK(length(global_head_hash) = 64),
                clock_high_water TEXT,
                key_id TEXT NOT NULL,
                signature TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS source_quota_parent_checkpoint (
                parent_id TEXT PRIMARY KEY,
                operation_count INTEGER NOT NULL CHECK(operation_count > 0),
                head_operation_hash TEXT NOT NULL CHECK(length(head_operation_hash) = 64),
                materialized_state_hash TEXT NOT NULL CHECK(length(materialized_state_hash) = 64),
                clock_high_water TEXT NOT NULL,
                key_id TEXT NOT NULL,
                signature TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS source_quota_external_root_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                binding_hash TEXT NOT NULL CHECK(length(binding_hash) = 64),
                config_hash TEXT NOT NULL CHECK(length(config_hash) = 64),
                acknowledged_checkpoint_hash TEXT NOT NULL
                    CHECK(length(acknowledged_checkpoint_hash) = 64),
                acknowledged_receipt_json TEXT,
                pending_request_json TEXT
            )
            """,
        )
        with self._store._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 3:
                    raise SourceQuotaAuthorityIntegrityError("source quota store must be schema v3")
                operation_existed = (
                    connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type = 'table' AND name = 'source_quota_operation'"
                    ).fetchone()
                    is not None
                )
                original_operation_columns = (
                    {
                        row[1]
                        for row in connection.execute(
                            "PRAGMA table_info(source_quota_operation)"
                        ).fetchall()
                    }
                    if operation_existed
                    else set()
                )
                global_checkpoint_existed = (
                    connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type = 'table' AND name = 'source_quota_global_checkpoint'"
                    ).fetchone()
                    is not None
                )
                global_checkpoint_row_existed = (
                    global_checkpoint_existed
                    and connection.execute(
                        "SELECT 1 FROM source_quota_global_checkpoint WHERE singleton = 1"
                    ).fetchone()
                    is not None
                )
                existing_guard_triggers = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master "
                        "WHERE type = 'trigger' AND name LIKE "
                        "'source_quota_operation_guard_%'"
                    ).fetchall()
                }
                for statement in statements:
                    connection.execute(statement)
                parent_schema = "PRAGMA table_info(source_parent_reservation)"
                parent_rows = connection.execute(parent_schema).fetchall()
                parent_columns = {row[1] for row in parent_rows}
                if "claim_binding_hash" not in parent_columns:
                    connection.execute(
                        "ALTER TABLE source_parent_reservation ADD COLUMN "
                        "claim_binding_hash TEXT NOT NULL DEFAULT '"
                        f"{LEGACY_SOURCE_QUOTA_CLAIM_BINDING_HASH}'"
                    )
                if "claim_generation" not in parent_columns:
                    connection.execute(
                        "ALTER TABLE source_parent_reservation ADD COLUMN "
                        f"claim_generation INTEGER NOT NULL DEFAULT "
                        f"{LEGACY_SOURCE_QUOTA_CLAIM_GENERATION}"
                    )
                if "scheduler_fencing_token" not in parent_columns:
                    connection.execute(
                        "ALTER TABLE source_parent_reservation ADD COLUMN "
                        "scheduler_fencing_token INTEGER NOT NULL DEFAULT "
                        f"{LEGACY_SOURCE_QUOTA_SCHEDULER_FENCING_TOKEN}"
                    )
                operation_schema = "PRAGMA table_info(source_quota_operation)"
                operation_rows = connection.execute(operation_schema).fetchall()
                operation_columns = {row[1] for row in operation_rows}
                if "result_integrity_hash" not in operation_columns:
                    connection.execute(
                        "ALTER TABLE source_quota_operation ADD COLUMN result_integrity_hash TEXT"
                    )
                if "result_integrity_signature" not in operation_columns:
                    connection.execute(
                        "ALTER TABLE source_quota_operation "
                        "ADD COLUMN result_integrity_signature TEXT"
                    )
                if "request_hash" not in operation_columns:
                    connection.execute(
                        "ALTER TABLE source_quota_operation ADD COLUMN request_hash TEXT"
                    )
                chain_columns = {
                    "parent_id": "TEXT",
                    "parent_ordinal": "INTEGER",
                    "previous_operation_hash": "TEXT",
                    "global_ordinal": "INTEGER",
                    "previous_global_hash": "TEXT",
                    "operation_hash": "TEXT",
                }
                for column, column_type in chain_columns.items():
                    if column not in operation_columns:
                        connection.execute(
                            f"ALTER TABLE source_quota_operation ADD COLUMN {column} {column_type}"
                        )
                self._backfill_legacy_operation_integrity(connection)
                metadata_was_complete = operation_existed and set(chain_columns) <= (
                    original_operation_columns
                )
                expected_guard_triggers = {
                    "source_quota_operation_guard_insert",
                    "source_quota_operation_guard_update",
                    "source_quota_operation_guard_delete",
                }
                if metadata_was_complete and not (
                    global_checkpoint_existed and global_checkpoint_row_existed
                ):
                    raise SourceQuotaAuthorityIntegrityError(
                        "stored journal chain global checkpoint is missing"
                    )
                if not metadata_was_complete:
                    self._full_audit(connection, repair=True)
                elif existing_guard_triggers != expected_guard_triggers:
                    self._full_audit(connection, repair=False)
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "source_quota_operation_parent_ordinal_uq "
                    "ON source_quota_operation(parent_id, parent_ordinal)"
                )
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "source_quota_operation_global_ordinal_uq "
                    "ON source_quota_operation(global_ordinal)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS source_call_allocation_parent_time_call_idx "
                    "ON source_call_allocation(parent_id, intended_at, call_id)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS source_call_allocation_parent_cost_idx "
                    "ON source_call_allocation(parent_id, cost)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS source_call_allocation_parent_state_call_idx "
                    "ON source_call_allocation(parent_id, state, call_id)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS quota_usage_lease_usage_idx "
                    "ON quota_usage(lease_id, usage_id)"
                )
                self._create_journal_guard_triggers(connection)
                if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 3:
                    raise SourceQuotaAuthorityIntegrityError(
                        "authority must not change quota schema version"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return operation_existed

    def _external_checkpoint(
        self,
        connection: sqlite3.Connection,
    ) -> SourceQuotaExternalCheckpoint:
        checkpoint = self._validate_global_checkpoint(connection)
        try:
            signature = self._require_journal_text(checkpoint, "signature")
            clock_high_water = checkpoint["clock_high_water"]
            if clock_high_water is not None and type(clock_high_water) is not str:
                raise SourceQuotaAuthorityIntegrityError(
                    "global checkpoint clock high-water must be text"
                )
            return SourceQuotaExternalCheckpoint(
                source_quota_authority_id=self.authority_id,
                binding_hash=self._external_binding_hash,
                journal_count=_require_stored_quota_int(checkpoint["journal_count"]),
                mutation_counter=_require_stored_quota_int(checkpoint["mutation_counter"]),
                global_head_hash=self._require_journal_text(
                    checkpoint,
                    "global_head_hash",
                ),
                clock_high_water=clock_high_water,
                local_checkpoint_signature_hash=hashlib.sha256(
                    signature.encode("utf-8")
                ).hexdigest(),
            )
        except SourceQuotaAuthorityIntegrityError:
            raise
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "global checkpoint cannot be bound to the external root"
            ) from exc

    def _repair_required(
        self,
        *,
        reason: Literal[
            "legacy_database_without_external_binding",
            "external_root_missing",
            "external_root_ahead_without_pending_proof",
            "external_root_diverges_at_same_high_water",
            "local_state_ahead_of_external_root",
            "external_binding_changed",
        ],
        local_checkpoint_hash: str,
        root_checkpoint_hash: str,
    ) -> SourceQuotaAuthorityRepairRequiredError:
        return SourceQuotaAuthorityRepairRequiredError(
            SourceQuotaAuthorityRepairState(
                reason=reason,
                authority_id=self.authority_id,
                local_checkpoint_hash=local_checkpoint_hash,
                root_checkpoint_hash=root_checkpoint_hash,
            )
        )

    def _external_state_row(self, connection: sqlite3.Connection) -> sqlite3.Row | None:
        row = connection.execute(
            "SELECT * FROM source_quota_external_root_state WHERE singleton = 1"
        ).fetchone()
        if row is None:
            return None
        values = {
            "binding_hash": row["binding_hash"],
            "config_hash": row["config_hash"],
            "acknowledged_checkpoint_hash": row["acknowledged_checkpoint_hash"],
        }
        if any(type(value) is not str or len(value) != 64 for value in values.values()):
            raise SourceQuotaAuthorityIntegrityError(
                "source quota external root state is malformed"
            )
        for field in ("acknowledged_receipt_json", "pending_request_json"):
            if row[field] is not None and type(row[field]) is not str:
                raise SourceQuotaAuthorityIntegrityError(
                    "source quota external root state JSON must be text"
                )
        return row

    def _require_external_state_binding(self, row: sqlite3.Row) -> None:
        root = self._root
        if root is None:
            raise SourceQuotaAuthorityIntegrityError("external root is not configured")
        if (
            row["binding_hash"] != self._external_binding_hash
            or row["config_hash"] != root.config.config_hash
        ):
            raise self._repair_required(
                reason="external_binding_changed",
                local_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
                root_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
            )
        acknowledged = row["acknowledged_checkpoint_hash"]
        receipt_json = row["acknowledged_receipt_json"]
        if acknowledged == EXTERNAL_MONOTONIC_ROOT_ZERO_HASH:
            if receipt_json is not None:
                raise SourceQuotaAuthorityIntegrityError(
                    "uninitialized external root state cannot contain a receipt"
                )
            return
        if type(receipt_json) is not str:
            raise SourceQuotaAuthorityIntegrityError(
                "acknowledged external root state is missing its receipt"
            )
        try:
            receipt = root.verify_stored_receipt(
                receipt_json,
                source_quota_authority_id=self.authority_id,
            )
        except SourceQuotaExternalRootSecurityError as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "acknowledged external root receipt is untrusted"
            ) from exc
        if (
            receipt.checkpoint.checkpoint_hash != acknowledged
            or receipt.checkpoint.binding_hash != self._external_binding_hash
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "acknowledged external root receipt diverges from local binding"
            )

    def _invoke_external_current(self) -> SourceQuotaExternalRootReceipt | None:
        root = self._root
        if root is None:
            return None
        try:
            return root.current(source_quota_authority_id=self.authority_id)
        except SourceQuotaExternalRootSecurityError as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "source quota external root is unavailable or untrusted"
            ) from exc

    def _parse_pending_request(self, value: object) -> ExternalMonotonicRootRequest:
        if type(value) is not str:
            raise SourceQuotaAuthorityIntegrityError(
                "source quota external pending request must be text"
            )
        try:
            request = strict_model_validate_canonical_json(ExternalMonotonicRootRequest, value)
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "source quota external pending request is malformed"
            ) from exc
        if (
            request.kind not in {"pin", "advance"}
            or request.subject_authority_id != self.authority_id
            or request.role != self._root.config.role  # type: ignore[union-attr]
            or request.root_authority_id != self._root.config.root_authority_id  # type: ignore[union-attr]
            or request.root_store_id != self._root.config.root_store_id  # type: ignore[union-attr]
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "source quota external pending request binding diverges"
            )
        return request

    def _synchronize_external_root(self, *, schema_existed: bool | None = None) -> None:
        root = self._root
        if root is None:
            return
        with self._store._connect() as connection:
            local = self._external_checkpoint(connection)
            state = self._external_state_row(connection)
        if state is None:
            if schema_existed is not False or local.journal_count != 0:
                raise self._repair_required(
                    reason="legacy_database_without_external_binding",
                    local_checkpoint_hash=local.checkpoint_hash,
                    root_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
                )
            rooted = self._invoke_external_current()
            if rooted is not None:
                raise self._repair_required(
                    reason="external_root_ahead_without_pending_proof",
                    local_checkpoint_hash=local.checkpoint_hash,
                    root_checkpoint_hash=rooted.checkpoint.checkpoint_hash,
                )
            operation_id = canonical_sha256(
                {
                    "authority_id": self.authority_id,
                    "binding_hash": self._external_binding_hash,
                    "checkpoint_hash": local.checkpoint_hash,
                    "contract": "rquant-source-quota-external-pin/v1",
                }
            )
            request = root.build_mutation_request(
                kind="pin",
                operation_id=operation_id,
                source_quota_authority_id=self.authority_id,
                previous_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
                checkpoint=local,
            )
            with self._store._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    if self._external_state_row(connection) is not None:
                        raise SourceQuotaAuthorityConflictError(
                            "source quota external binding initialization raced"
                        )
                    connection.execute(
                        """
                        INSERT INTO source_quota_external_root_state(
                            singleton, binding_hash, config_hash,
                            acknowledged_checkpoint_hash, acknowledged_receipt_json,
                            pending_request_json
                        ) VALUES (1, ?, ?, ?, NULL, ?)
                        """,
                        (
                            self._external_binding_hash,
                            root.config.config_hash,
                            EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
                            canonical_model_json_bytes(request).decode("utf-8"),
                        ),
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
            self._complete_external_pending()
        else:
            self._require_external_state_binding(state)
            if state["pending_request_json"] is not None:
                self._complete_external_pending()

        with self._store._connect() as connection:
            local = self._external_checkpoint(connection)
            state = self._external_state_row(connection)
            if state is None:
                raise SourceQuotaAuthorityIntegrityError(
                    "source quota external binding disappeared"
                )
            self._require_external_state_binding(state)
            acknowledged = state["acknowledged_checkpoint_hash"]
            if state["pending_request_json"] is not None:
                raise SourceQuotaAuthorityIntegrityError(
                    "source quota external pending request was not completed"
                )
        rooted = self._invoke_external_current()
        if rooted is None:
            raise self._repair_required(
                reason="external_root_missing",
                local_checkpoint_hash=local.checkpoint_hash,
                root_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
            )
        root_hash = rooted.checkpoint.checkpoint_hash
        if acknowledged == local.checkpoint_hash == root_hash:
            return
        if rooted.checkpoint.journal_count > local.journal_count:
            reason = "external_root_ahead_without_pending_proof"
        elif rooted.checkpoint.journal_count < local.journal_count:
            reason = "local_state_ahead_of_external_root"
        else:
            reason = "external_root_diverges_at_same_high_water"
        raise self._repair_required(
            reason=reason,
            local_checkpoint_hash=local.checkpoint_hash,
            root_checkpoint_hash=root_hash,
        )

    def _complete_external_pending(self) -> None:
        root = self._root
        if root is None:
            return
        with self._store._connect() as connection:
            state = self._external_state_row(connection)
            if state is None:
                raise SourceQuotaAuthorityIntegrityError("source quota external binding is missing")
            self._require_external_state_binding(state)
            pending_json = state["pending_request_json"]
            if pending_json is None:
                return
            request = self._parse_pending_request(pending_json)
            local = self._external_checkpoint(connection)
            acknowledged = state["acknowledged_checkpoint_hash"]
        if request.checkpoint_hash != local.checkpoint_hash:
            raise SourceQuotaAuthorityIntegrityError(
                "source quota external pending checkpoint diverges from local state"
            )
        rooted = self._invoke_external_current()
        if rooted is not None and rooted.checkpoint.checkpoint_hash not in {
            acknowledged,
            request.checkpoint_hash,
        }:
            raise self._repair_required(
                reason="external_root_diverges_at_same_high_water",
                local_checkpoint_hash=local.checkpoint_hash,
                root_checkpoint_hash=rooted.checkpoint.checkpoint_hash,
            )
        if rooted is None and request.kind != "pin":
            raise self._repair_required(
                reason="external_root_missing",
                local_checkpoint_hash=local.checkpoint_hash,
                root_checkpoint_hash=EXTERNAL_MONOTONIC_ROOT_ZERO_HASH,
            )
        try:
            receipt = root.invoke_mutation(request)
        except SourceQuotaExternalRootSecurityError as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "source quota external root advance failed closed"
            ) from exc
        if receipt.checkpoint != local:
            raise SourceQuotaAuthorityIntegrityError(
                "source quota external root acknowledged a divergent checkpoint"
            )
        receipt_json = canonical_model_json_bytes(receipt).decode("utf-8")
        with self._store._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = self._external_state_row(connection)
                if current is None:
                    raise SourceQuotaAuthorityIntegrityError(
                        "source quota external binding disappeared"
                    )
                self._require_external_state_binding(current)
                if current["pending_request_json"] != pending_json:
                    raise SourceQuotaAuthorityConflictError(
                        "source quota external pending request changed concurrently"
                    )
                if self._external_checkpoint(connection) != local:
                    raise SourceQuotaAuthorityConflictError(
                        "source quota local checkpoint changed during external advance"
                    )
                connection.execute(
                    """
                    UPDATE source_quota_external_root_state
                    SET acknowledged_checkpoint_hash = ?,
                        acknowledged_receipt_json = ?, pending_request_json = NULL
                    WHERE singleton = 1
                    """,
                    (local.checkpoint_hash, receipt_json),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _require_external_acknowledgement(
        self,
        connection: sqlite3.Connection,
        checkpoint: SourceQuotaExternalCheckpoint,
    ) -> str:
        state = self._external_state_row(connection)
        if state is None:
            raise SourceQuotaAuthorityIntegrityError("source quota external binding is missing")
        self._require_external_state_binding(state)
        if state["pending_request_json"] is not None:
            raise SourceQuotaAuthorityConflictError(
                "source quota external root has an unresolved pending request"
            )
        acknowledged = state["acknowledged_checkpoint_hash"]
        if acknowledged != checkpoint.checkpoint_hash:
            raise SourceQuotaAuthorityConflictError(
                "source quota external acknowledgement is stale"
            )
        return acknowledged

    def _stage_external_advance(
        self,
        connection: sqlite3.Connection,
        *,
        local_operation_id: str,
        previous_checkpoint_hash: str,
        checkpoint: SourceQuotaExternalCheckpoint,
    ) -> None:
        root = self._root
        if root is None:
            return
        external_operation_id = canonical_sha256(
            {
                "authority_id": self.authority_id,
                "binding_hash": self._external_binding_hash,
                "checkpoint_hash": checkpoint.checkpoint_hash,
                "contract": "rquant-source-quota-external-advance/v1",
                "local_operation_id": local_operation_id,
                "previous_checkpoint_hash": previous_checkpoint_hash,
            }
        )
        request = root.build_mutation_request(
            kind="advance",
            operation_id=external_operation_id,
            source_quota_authority_id=self.authority_id,
            previous_checkpoint_hash=previous_checkpoint_hash,
            checkpoint=checkpoint,
        )
        changed = connection.execute(
            """
            UPDATE source_quota_external_root_state
            SET pending_request_json = ?
            WHERE singleton = 1 AND pending_request_json IS NULL
                AND acknowledged_checkpoint_hash = ?
            """,
            (
                canonical_model_json_bytes(request).decode("utf-8"),
                previous_checkpoint_hash,
            ),
        )
        if changed.rowcount != 1:
            raise SourceQuotaAuthorityConflictError(
                "source quota external advance staging CAS failed"
            )

    def _operate(
        self,
        *,
        operation_id: str,
        effect_key: str,
        operation: SourceQuotaOperationKind,
        payload: Mapping[str, object],
        apply: Callable[
            [sqlite3.Connection],
            tuple[SourceQuotaParentSnapshot, SourceQuotaCallAllocation | None],
        ],
    ) -> SourceQuotaAuthorityResult:
        identifier = _require_nonempty(operation_id, label="operation_id")
        key = _require_nonempty(effect_key, label="effect_key")
        request_hash = _request_hash(operation, payload)
        if self._root is not None:
            self._synchronize_external_root()
        committed_result: SourceQuotaAuthorityResult | None = None
        with self._store._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                payload_parent_id = payload.get("parent_id")
                if type(payload_parent_id) is not str:
                    raise SourceQuotaAuthorityIntegrityError(
                        "quota operation parent identity is malformed"
                    )
                existing = connection.execute(
                    "SELECT * FROM source_quota_operation WHERE operation_id = ?", (identifier,)
                ).fetchone()
                if existing is not None:
                    result = self._replay_operation(connection, existing)
                    if (
                        existing["request_hash"] != request_hash
                        or existing["operation"] != operation.value
                    ):
                        raise SourceQuotaAuthorityConflictError("operation_id payload conflicts")
                    if existing["effect_key"] != key:
                        raise SourceQuotaAuthorityConflictError("operation_id effect key conflicts")
                    connection.rollback()
                    return result
                effect = connection.execute(
                    "SELECT * FROM source_quota_operation WHERE effect_key = ?", (key,)
                ).fetchone()
                if effect is not None:
                    self._replay_operation(connection, effect)
                    raise SourceQuotaAuthorityConflictError(
                        "effect key is already bound to another operation"
                    )
                global_checkpoint = self._validate_global_checkpoint(connection)
                external_previous_checkpoint_hash = None
                if self._root is not None:
                    external_previous_checkpoint_hash = self._require_external_acknowledgement(
                        connection,
                        self._external_checkpoint(connection),
                    )
                parent_checkpoint = self._validate_parent_checkpoint(
                    connection,
                    payload_parent_id,
                    validate_global=False,
                    validate_materialized=False,
                )
                parent, call = apply(connection)
                self._validate_operation_result_shape(operation, parent, call)
                self._validate_immutable_result_evidence(connection, operation, parent, call)
                payload_hash = canonical_sha256(self._operation_payload(operation, parent, call))
                result_hash = _result_hash(parent, call)
                unsigned = SourceQuotaOperationReceipt(
                    authority_id=self.authority_id,
                    operation_id=identifier,
                    effect_key=key,
                    operation=operation,
                    claim_binding_hash=parent.claim_binding_hash,
                    claim_generation=parent.claim_generation,
                    scheduler_fencing_token=parent.scheduler_fencing_token,
                    payload_hash=payload_hash,
                    result_hash=result_hash,
                    key_id=self._key_id,
                    signature="pending",
                )
                signature = self._signer.sign(unsigned.signing_bytes())
                receipt = unsigned.model_copy(update={"signature": signature})
                if not self._signer.verify(receipt.signing_bytes(), receipt.signature):
                    raise SourceQuotaAuthorityIntegrityError(
                        "signer returned an unverifiable receipt"
                    )
                result = SourceQuotaAuthorityResult(receipt=receipt, parent=parent, call=call)
                result_json = _result_json(parent, call)
                result_integrity_hash = _replay_payload_hash(result_json)
                result_integrity_signature = self._signer.sign(
                    _replay_payload_signing_bytes(
                        authority_id=self.authority_id,
                        operation_id=identifier,
                        effect_key=key,
                        operation=operation.value,
                        payload_hash=payload_hash,
                        result_hash=result_hash,
                        result_integrity_hash=result_integrity_hash,
                        key_id=self._key_id,
                    )
                )
                if not self._signer.verify(
                    _replay_payload_signing_bytes(
                        authority_id=self.authority_id,
                        operation_id=identifier,
                        effect_key=key,
                        operation=operation.value,
                        payload_hash=payload_hash,
                        result_hash=result_hash,
                        result_integrity_hash=result_integrity_hash,
                        key_id=self._key_id,
                    ),
                    result_integrity_signature,
                ):
                    raise SourceQuotaAuthorityIntegrityError(
                        "signer returned an unverifiable replay payload binding"
                    )
                parent_ordinal = (
                    1
                    if parent_checkpoint is None
                    else _require_stored_quota_int(parent_checkpoint["operation_count"]) + 1
                )
                previous_operation_hash = (
                    SOURCE_QUOTA_JOURNAL_ZERO_HASH
                    if parent_checkpoint is None
                    else self._require_journal_text(parent_checkpoint, "head_operation_hash")
                )
                journal_count = _require_stored_quota_int(global_checkpoint["journal_count"])
                mutation_counter = _require_stored_quota_int(global_checkpoint["mutation_counter"])
                previous_global_hash = self._require_journal_text(
                    global_checkpoint, "global_head_hash"
                )
                global_ordinal = journal_count + 1
                operation_hash = _operation_chain_hash(
                    parent_id=parent.parent_id,
                    parent_ordinal=parent_ordinal,
                    previous_operation_hash=previous_operation_hash,
                    global_ordinal=global_ordinal,
                    previous_global_hash=previous_global_hash,
                    operation_id=identifier,
                    effect_key=key,
                    operation=operation.value,
                    payload_hash=payload_hash,
                    request_hash=request_hash,
                    result_hash=result_hash,
                    result_integrity_hash=result_integrity_hash,
                    result_integrity_signature=result_integrity_signature,
                    receipt_json=receipt.model_dump_json(),
                )
                connection.execute(
                    """
                    INSERT INTO source_quota_operation(
                        operation_id, effect_key, operation, parent_id, parent_ordinal,
                        previous_operation_hash, global_ordinal, previous_global_hash,
                        payload_hash, request_hash, result_hash, result_json,
                        result_integrity_hash, result_integrity_signature, receipt_json,
                        operation_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        identifier,
                        key,
                        operation.value,
                        parent.parent_id,
                        parent_ordinal,
                        previous_operation_hash,
                        global_ordinal,
                        previous_global_hash,
                        payload_hash,
                        request_hash,
                        result_hash,
                        result_json,
                        result_integrity_hash,
                        result_integrity_signature,
                        receipt.model_dump_json(),
                        operation_hash,
                    ),
                )
                actual_mutation_counter = _require_stored_quota_int(
                    connection.execute(
                        "SELECT mutation_counter FROM source_quota_global_checkpoint "
                        "WHERE singleton = 1"
                    ).fetchone()[0]
                )
                if actual_mutation_counter != mutation_counter + 1:
                    raise SourceQuotaAuthorityIntegrityError(
                        "journal guard did not observe exactly one append"
                    )
                self._write_parent_checkpoint(
                    connection,
                    parent=parent,
                    operation_count=parent_ordinal,
                    head_operation_hash=operation_hash,
                )
                old_clock_raw = global_checkpoint["clock_high_water"]
                new_clock = _parent_clock_high_water(parent)
                if old_clock_raw is not None:
                    old_clock = _parse_stored_time(old_clock_raw)
                    if old_clock > new_clock:
                        new_clock = old_clock
                self._write_global_checkpoint(
                    connection,
                    journal_count=global_ordinal,
                    mutation_counter=actual_mutation_counter,
                    global_head_hash=operation_hash,
                    clock_high_water=_iso(new_clock),
                )
                if external_previous_checkpoint_hash is not None:
                    self._stage_external_advance(
                        connection,
                        local_operation_id=identifier,
                        previous_checkpoint_hash=external_previous_checkpoint_hash,
                        checkpoint=self._external_checkpoint(connection),
                    )
                connection.commit()
                committed_result = result
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise SourceQuotaAuthorityConflictError(
                    "quota authority unique constraint conflicts"
                ) from exc
            except BaseException:
                connection.rollback()
                raise
        if self._root is not None:
            self._complete_external_pending()
        if committed_result is None:
            raise SourceQuotaAuthorityIntegrityError(
                "source quota operation committed without a result"
            )
        return committed_result

    def _backfill_legacy_operation_integrity(self, connection: sqlite3.Connection) -> None:
        legacy_rows = connection.execute(
            """
            SELECT * FROM source_quota_operation
            WHERE result_integrity_hash IS NULL OR result_integrity_signature IS NULL
            """
        ).fetchall()
        for row in legacy_rows:
            result = self._validate_journal_row(connection, row, validate_request_hash=False)
            canonical_result_json = _result_json(result.parent, result.call)
            if row["result_json"] != canonical_result_json:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal result JSON is not canonical"
                )
            result_integrity_hash = _replay_payload_hash(canonical_result_json)
            result_integrity_signature = self._signer.sign(
                _replay_payload_signing_bytes(
                    authority_id=self.authority_id,
                    operation_id=row["operation_id"],
                    effect_key=row["effect_key"],
                    operation=row["operation"],
                    payload_hash=row["payload_hash"],
                    result_hash=row["result_hash"],
                    result_integrity_hash=result_integrity_hash,
                    key_id=self._key_id,
                )
            )
            if not self._signer.verify(
                _replay_payload_signing_bytes(
                    authority_id=self.authority_id,
                    operation_id=row["operation_id"],
                    effect_key=row["effect_key"],
                    operation=row["operation"],
                    payload_hash=row["payload_hash"],
                    result_hash=row["result_hash"],
                    result_integrity_hash=result_integrity_hash,
                    key_id=self._key_id,
                ),
                result_integrity_signature,
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "signer returned an unverifiable replay payload binding"
                )
            connection.execute(
                """
                UPDATE source_quota_operation
                SET result_integrity_hash = ?, result_integrity_signature = ?
                WHERE operation_id = ?
                """,
                (result_integrity_hash, result_integrity_signature, row["operation_id"]),
            )
        request_rows = connection.execute(
            "SELECT * FROM source_quota_operation WHERE request_hash IS NULL"
        ).fetchall()
        for row in request_rows:
            result = self._validate_journal_row(connection, row, validate_request_hash=False)
            operation = SourceQuotaOperationKind(row["operation"])
            connection.execute(
                "UPDATE source_quota_operation SET request_hash = ? WHERE operation_id = ?",
                (
                    _request_hash(
                        operation,
                        self._operation_request_payload(operation, result.parent, result.call),
                    ),
                    row["operation_id"],
                ),
            )

    def _replay_operation(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> SourceQuotaAuthorityResult:
        self._validate_replay_payload_binding(row)
        result = self._validate_journal_row(connection, row)
        self._validate_global_checkpoint(connection)
        self._validate_journal_chain(connection, row, result)
        return result

    def _validate_replay_payload_binding(self, row: sqlite3.Row) -> None:
        try:
            operation_id = self._require_journal_text(row, "operation_id")
            effect_key = self._require_journal_text(row, "effect_key")
            operation = self._require_journal_text(row, "operation")
            payload_hash = self._require_journal_text(row, "payload_hash")
            result_hash = self._require_journal_text(row, "result_hash")
            result_json = self._require_journal_text(row, "result_json")
            result_integrity_hash = self._require_journal_text(row, "result_integrity_hash")
            result_integrity_signature = self._require_journal_text(
                row, "result_integrity_signature"
            )
            if _replay_payload_hash(result_json) != result_integrity_hash:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal replay payload hash conflicts"
                )
            verified = self._signer.verify(
                _replay_payload_signing_bytes(
                    authority_id=self.authority_id,
                    operation_id=operation_id,
                    effect_key=effect_key,
                    operation=operation,
                    payload_hash=payload_hash,
                    result_hash=result_hash,
                    result_integrity_hash=result_integrity_hash,
                    key_id=self._key_id,
                ),
                result_integrity_signature,
            )
        except SourceQuotaAuthorityIntegrityError:
            raise
        except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal replay payload binding is malformed"
            ) from exc
        except Exception as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal replay payload signature verification failed"
            ) from exc
        if not verified:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal replay payload hash or signature conflicts"
            )

    @staticmethod
    def _require_journal_text(row: sqlite3.Row, field: str) -> str:
        value = row[field]
        if not isinstance(value, str):
            raise SourceQuotaAuthorityIntegrityError(f"stored journal {field} must be text")
        return value

    def _validate_journal_chain(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        replayed: SourceQuotaAuthorityResult,
    ) -> None:
        """Validate target membership through signed O(1) global/parent anchors."""

        parent_id = replayed.parent.parent_id
        parent_checkpoint = self._validate_parent_checkpoint(
            connection,
            parent_id,
            validate_global=False,
        )
        if parent_checkpoint is None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain parent checkpoint is missing"
            )
        try:
            stored_parent_id = self._require_journal_text(row, "parent_id")
            parent_ordinal = _require_stored_quota_int(row["parent_ordinal"])
            previous_operation_hash = self._require_journal_text(row, "previous_operation_hash")
            global_ordinal = _require_stored_quota_int(row["global_ordinal"])
            previous_global_hash = self._require_journal_text(row, "previous_global_hash")
            operation_hash = self._require_journal_text(row, "operation_hash")
            expected_operation_hash = _operation_chain_hash(
                parent_id=stored_parent_id,
                parent_ordinal=parent_ordinal,
                previous_operation_hash=previous_operation_hash,
                global_ordinal=global_ordinal,
                previous_global_hash=previous_global_hash,
                operation_id=self._require_journal_text(row, "operation_id"),
                effect_key=self._require_journal_text(row, "effect_key"),
                operation=self._require_journal_text(row, "operation"),
                payload_hash=self._require_journal_text(row, "payload_hash"),
                request_hash=self._require_journal_text(row, "request_hash"),
                result_hash=self._require_journal_text(row, "result_hash"),
                result_integrity_hash=self._require_journal_text(row, "result_integrity_hash"),
                result_integrity_signature=self._require_journal_text(
                    row, "result_integrity_signature"
                ),
                receipt_json=self._require_journal_text(row, "receipt_json"),
            )
            parent_count = _require_stored_quota_int(parent_checkpoint["operation_count"])
            global_checkpoint = connection.execute(
                "SELECT journal_count FROM source_quota_global_checkpoint WHERE singleton = 1"
            ).fetchone()
            if global_checkpoint is None:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal chain global checkpoint is missing"
                )
            global_count = _require_stored_quota_int(global_checkpoint["journal_count"])
        except SourceQuotaAuthorityIntegrityError:
            raise
        except Exception as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain membership is malformed"
            ) from exc
        if (
            stored_parent_id != parent_id
            or parent_ordinal < 1
            or parent_ordinal > parent_count
            or global_ordinal < 1
            or global_ordinal > global_count
            or len(previous_operation_hash) != 64
            or len(previous_global_hash) != 64
            or operation_hash != expected_operation_hash
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain hash or ordinal conflicts"
            )
        if (
            parent_ordinal == parent_count
            and operation_hash != parent_checkpoint["head_operation_hash"]
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain does not reach parent checkpoint"
            )

    @staticmethod
    def _expected_effect_key(
        operation: SourceQuotaOperationKind,
        parent: SourceQuotaParentSnapshot,
        call: SourceQuotaCallAllocation | None,
    ) -> str:
        if operation is SourceQuotaOperationKind.RESERVE_PARENT:
            return f"reserve-parent:{parent.parent_id}"
        if operation is SourceQuotaOperationKind.RELEASE_UNUSED:
            return f"release-unused:{parent.parent_id}"
        if call is None:
            raise SourceQuotaAuthorityIntegrityError("stored journal chain call is missing")
        prefixes = {
            SourceQuotaOperationKind.RECORD_INTENT: "record-intent",
            SourceQuotaOperationKind.AUTHORIZE_DISPATCH: "authorize-dispatch",
            SourceQuotaOperationKind.FINALIZE: "finalize",
            SourceQuotaOperationKind.UNKNOWN_BEFORE_DISPATCH: "unknown-before-dispatch",
            SourceQuotaOperationKind.CANCEL: "cancel",
        }
        try:
            return f"{prefixes[operation]}:{parent.parent_id}:{call.call_id}"
        except KeyError as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain operation is invalid"
            ) from exc

    @staticmethod
    def _validate_journal_chain_step(
        previous: SourceQuotaAuthorityResult | None,
        operation: SourceQuotaOperationKind,
        current: SourceQuotaAuthorityResult,
    ) -> None:
        parent = current.parent
        call = current.call
        if previous is None:
            if (
                operation is not SourceQuotaOperationKind.RESERVE_PARENT
                or call is not None
                or parent.state is not SourceQuotaParentState.OPEN
                or parent.calls
                or parent.consumed_cost != 0
            ):
                raise SourceQuotaAuthorityIntegrityError("stored journal chain order conflicts")
            return
        prior_parent = previous.parent
        if operation is SourceQuotaOperationKind.RESERVE_PARENT:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain has duplicate reservation"
            )
        if not SourceQuotaParentAuthority._same_parent_contract(prior_parent, parent):
            raise SourceQuotaAuthorityIntegrityError("stored journal chain parent conflicts")
        prior_calls = {allocation.call_id: allocation for allocation in prior_parent.calls}
        current_calls = {allocation.call_id: allocation for allocation in parent.calls}
        if len(current_calls) != len(parent.calls) or len(prior_calls) != len(prior_parent.calls):
            raise SourceQuotaAuthorityIntegrityError("stored journal chain call identity conflicts")
        if operation is SourceQuotaOperationKind.RELEASE_UNUSED:
            if (
                call is not None
                or prior_parent.state is not SourceQuotaParentState.OPEN
                or parent.state
                not in {SourceQuotaParentState.CLOSED, SourceQuotaParentState.COMPENSATED}
                or parent.calls != prior_parent.calls
                or parent.consumed_cost != prior_parent.consumed_cost
            ):
                raise SourceQuotaAuthorityIntegrityError("stored journal chain release conflicts")
            return
        if (
            call is None
            or prior_parent.state is not SourceQuotaParentState.OPEN
            or parent.state is not SourceQuotaParentState.OPEN
            or parent.closed_at is not None
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain call transition conflicts"
            )
        if operation is SourceQuotaOperationKind.RECORD_INTENT:
            if (
                call.call_id in prior_calls
                or call.state is not SourceQuotaCallState.INTENT
                or current_calls != {**prior_calls, call.call_id: call}
                or parent.consumed_cost != prior_parent.consumed_cost
            ):
                raise SourceQuotaAuthorityIntegrityError("stored journal chain intent conflicts")
            return
        prior_call = prior_calls.get(call.call_id)
        if prior_call is None or current_calls != {**prior_calls, call.call_id: call}:
            raise SourceQuotaAuthorityIntegrityError("stored journal chain call set conflicts")
        if operation is SourceQuotaOperationKind.AUTHORIZE_DISPATCH:
            if (
                prior_call.state is not SourceQuotaCallState.INTENT
                or call.state is not SourceQuotaCallState.DISPATCH_AUTHORIZED
                or parent.consumed_cost != prior_parent.consumed_cost + call.cost
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal chain authorization conflicts"
                )
            return
        if operation is SourceQuotaOperationKind.FINALIZE:
            valid_transition = (
                prior_call.state is SourceQuotaCallState.DISPATCH_AUTHORIZED
                and call.state
                in {
                    SourceQuotaCallState.SUCCESS,
                    SourceQuotaCallState.FAILURE,
                    SourceQuotaCallState.UNKNOWN,
                }
            )
        elif operation in {
            SourceQuotaOperationKind.UNKNOWN_BEFORE_DISPATCH,
            SourceQuotaOperationKind.CANCEL,
        }:
            valid_transition = (
                prior_call.state is SourceQuotaCallState.INTENT
                and call.state is SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH
            )
        else:
            valid_transition = False
        if not valid_transition or parent.consumed_cost != prior_parent.consumed_cost:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal chain terminal transition conflicts"
            )

    @staticmethod
    def _same_parent_contract(
        left: SourceQuotaParentSnapshot,
        right: SourceQuotaParentSnapshot,
    ) -> bool:
        return (
            left.parent_id,
            left.source,
            left.owner,
            left.claim_binding_hash,
            left.claim_generation,
            left.scheduler_fencing_token,
            left.lease_id,
            left.total_cost,
            left.reserved_at,
            left.expires_at,
            left.window_id,
            left.window_start,
            left.window_end,
            left.reset_at,
            left.capacity,
            left.reserved_cost,
        ) == (
            right.parent_id,
            right.source,
            right.owner,
            right.claim_binding_hash,
            right.claim_generation,
            right.scheduler_fencing_token,
            right.lease_id,
            right.total_cost,
            right.reserved_at,
            right.expires_at,
            right.window_id,
            right.window_start,
            right.window_end,
            right.reset_at,
            right.capacity,
            right.reserved_cost,
        )

    def _validate_journal_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        validate_request_hash: bool = True,
    ) -> SourceQuotaAuthorityResult:
        try:
            operation_id = self._require_journal_text(row, "operation_id")
            effect_key = self._require_journal_text(row, "effect_key")
            operation_text = self._require_journal_text(row, "operation")
            payload_hash = self._require_journal_text(row, "payload_hash")
            result_hash = self._require_journal_text(row, "result_hash")
            result_json = self._require_journal_text(row, "result_json")
            receipt_json = self._require_journal_text(row, "receipt_json")
            if validate_request_hash:
                self._require_journal_text(row, "request_hash")
            receipt = SourceQuotaOperationReceipt.model_validate_json(receipt_json)
            data = json.loads(result_json)
            if not isinstance(data, dict):
                raise TypeError("result is not an object")
            parent = SourceQuotaParentSnapshot.model_validate(data["parent"])
            raw_call = data["call"]
            call = None if raw_call is None else SourceQuotaCallAllocation.model_validate(raw_call)
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored quota operation journal is malformed"
            ) from exc
        if receipt.authority_id != self.authority_id or receipt.key_id != self._key_id:
            raise SourceQuotaAuthorityIntegrityError("stored journal receipt authority conflicts")
        if receipt.operation_id != operation_id or receipt.effect_key != effect_key:
            raise SourceQuotaAuthorityIntegrityError("stored journal receipt identity conflicts")
        if receipt.operation.value != operation_text:
            raise SourceQuotaAuthorityIntegrityError("stored journal receipt operation conflicts")
        if (
            receipt.claim_binding_hash != parent.claim_binding_hash
            or receipt.claim_generation != parent.claim_generation
            or receipt.scheduler_fencing_token != parent.scheduler_fencing_token
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal receipt claim binding conflicts"
            )
        if receipt.payload_hash != payload_hash or receipt.result_hash != result_hash:
            raise SourceQuotaAuthorityIntegrityError("stored journal receipt hash conflicts")
        try:
            verified = self._signer.verify(receipt.signing_bytes(), receipt.signature)
        except Exception as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal receipt signature verification failed"
            ) from exc
        if not verified:
            raise SourceQuotaAuthorityIntegrityError("stored journal receipt signature is invalid")
        try:
            operation = SourceQuotaOperationKind(operation_text)
        except ValueError as exc:
            raise SourceQuotaAuthorityIntegrityError("stored journal operation is invalid") from exc
        self._validate_operation_result_shape(operation, parent, call)
        self._validate_immutable_result_evidence(connection, operation, parent, call)
        if _result_hash(parent, call) != row["result_hash"]:
            raise SourceQuotaAuthorityIntegrityError("stored journal result hash conflicts")
        if (
            canonical_sha256(self._operation_payload(operation, parent, call))
            != row["payload_hash"]
        ):
            raise SourceQuotaAuthorityIntegrityError("stored journal payload hash conflicts")
        if validate_request_hash and (
            not isinstance(row["request_hash"], str)
            or _request_hash(operation, self._operation_request_payload(operation, parent, call))
            != row["request_hash"]
        ):
            raise SourceQuotaAuthorityIntegrityError("stored journal request hash conflicts")
        try:
            return SourceQuotaAuthorityResult(receipt=receipt, parent=parent, call=call)
        except ValueError as exc:
            raise SourceQuotaAuthorityIntegrityError("stored journal result is invalid") from exc

    @staticmethod
    def _operation_payload(
        operation: SourceQuotaOperationKind,
        parent: SourceQuotaParentSnapshot,
        call: SourceQuotaCallAllocation | None,
    ) -> dict[str, object]:
        if operation is SourceQuotaOperationKind.RESERVE_PARENT:
            if call is not None:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal reserve result has call evidence"
                )
            return {
                "claim_binding_hash": parent.claim_binding_hash,
                "claim_generation": parent.claim_generation,
                "expires_at": _iso(parent.expires_at),
                "parent_id": parent.parent_id,
                "source": parent.source,
                "owner": parent.owner,
                "scheduler_fencing_token": parent.scheduler_fencing_token,
                "total_cost": parent.total_cost,
                "quota_contract": {
                    "capacity": parent.capacity,
                    "reset_at": _iso(parent.reset_at),
                    "window_end": _iso(parent.window_end),
                    "window_id": parent.window_id,
                    "window_start": _iso(parent.window_start),
                },
            }
        if operation is SourceQuotaOperationKind.RELEASE_UNUSED:
            if call is not None:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal release result has call evidence"
                )
            return {"parent_id": parent.parent_id}
        if call is None:
            raise SourceQuotaAuthorityIntegrityError("stored journal call result is missing")
        if operation is SourceQuotaOperationKind.RECORD_INTENT:
            return {
                "parent_id": call.parent_id,
                "call_id": call.call_id,
                "cost": call.cost,
            }
        if operation is SourceQuotaOperationKind.FINALIZE:
            if call.outcome is None:
                raise SourceQuotaAuthorityIntegrityError("stored journal final outcome is missing")
            return {
                "parent_id": call.parent_id,
                "call_id": call.call_id,
                "outcome": call.outcome.value,
            }
        return {"parent_id": call.parent_id, "call_id": call.call_id}

    @staticmethod
    def _operation_request_payload(
        operation: SourceQuotaOperationKind,
        parent: SourceQuotaParentSnapshot,
        call: SourceQuotaCallAllocation | None,
    ) -> dict[str, object]:
        if operation is SourceQuotaOperationKind.RESERVE_PARENT:
            if call is not None:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal reserve result has call evidence"
                )
            return _reserve_request_payload(
                parent_id=parent.parent_id,
                source=parent.source,
                owner=parent.owner,
                total_cost=parent.total_cost,
                expires_at=parent.expires_at,
                claim_binding_hash=parent.claim_binding_hash,
                claim_generation=parent.claim_generation,
                scheduler_fencing_token=parent.scheduler_fencing_token,
            )
        return SourceQuotaParentAuthority._operation_payload(operation, parent, call)

    @staticmethod
    def _validate_operation_result_shape(
        operation: SourceQuotaOperationKind,
        parent: SourceQuotaParentSnapshot,
        call: SourceQuotaCallAllocation | None,
    ) -> None:
        if parent.state is SourceQuotaParentState.CLOSING:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal result cannot contain a closing parent"
            )
        if len(parent.calls) != len({allocation.call_id for allocation in parent.calls}):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal result contains duplicate call evidence"
            )
        if call is not None:
            matching_calls = [
                allocation for allocation in parent.calls if allocation.call_id == call.call_id
            ]
            if len(matching_calls) != 1 or matching_calls[0] != call:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence call conflicts with parent call set"
                )
        if operation is SourceQuotaOperationKind.RESERVE_PARENT:
            if call is not None or parent.state is not SourceQuotaParentState.OPEN or parent.calls:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal reserve result shape conflicts"
                )
            return
        if operation is SourceQuotaOperationKind.RELEASE_UNUSED:
            if (
                call is not None
                or parent.state
                not in {SourceQuotaParentState.CLOSED, SourceQuotaParentState.COMPENSATED}
                or parent.closed_at is None
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal release result shape conflicts"
                )
            return
        if call is None or parent.state is not SourceQuotaParentState.OPEN:
            raise SourceQuotaAuthorityIntegrityError("stored journal call result shape conflicts")
        if operation is SourceQuotaOperationKind.RECORD_INTENT:
            if call.state is not SourceQuotaCallState.INTENT:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal intent result shape conflicts"
                )
            return
        if operation is SourceQuotaOperationKind.AUTHORIZE_DISPATCH:
            if call.state is not SourceQuotaCallState.DISPATCH_AUTHORIZED:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal dispatch authorization result shape conflicts"
                )
            return
        if operation is SourceQuotaOperationKind.FINALIZE:
            if call.state not in {
                SourceQuotaCallState.SUCCESS,
                SourceQuotaCallState.FAILURE,
                SourceQuotaCallState.UNKNOWN,
            } or call.outcome is not SourceQuotaCallOutcome(call.state.value):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal final result shape conflicts"
                )
            return
        if operation is SourceQuotaOperationKind.UNKNOWN_BEFORE_DISPATCH:
            if (
                call.state is not SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH
                or call.outcome is not SourceQuotaCallOutcome.UNKNOWN_BEFORE_DISPATCH
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal pre-dispatch unknown result shape conflicts"
                )
            return
        if operation is SourceQuotaOperationKind.CANCEL:
            if (
                call.state is not SourceQuotaCallState.CANCELLED_BEFORE_DISPATCH
                or call.outcome is not None
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal cancel result shape conflicts"
                )
            return
        raise SourceQuotaAuthorityIntegrityError("stored journal operation is invalid")

    def _validate_immutable_result_evidence(
        self,
        connection: sqlite3.Connection,
        operation: SourceQuotaOperationKind,
        parent: SourceQuotaParentSnapshot,
        call: SourceQuotaCallAllocation | None,
    ) -> None:
        durable_parent = self._parent_row(connection, parent.parent_id)
        if durable_parent is None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence parent is missing"
            )
        try:
            durable_total_cost = _require_stored_quota_int(durable_parent["total_cost"])
            durable_claim_generation = _require_stored_quota_int(durable_parent["claim_generation"])
            durable_fencing_token = _require_stored_quota_int(
                durable_parent["scheduler_fencing_token"]
            )
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence parent reservation is malformed"
            ) from exc
        if (
            durable_parent["source"] != parent.source
            or durable_parent["owner"] != parent.owner
            or durable_parent["claim_binding_hash"] != parent.claim_binding_hash
            or durable_claim_generation != parent.claim_generation
            or durable_fencing_token != parent.scheduler_fencing_token
            or durable_parent["lease_id"] != parent.lease_id
            or durable_total_cost != parent.total_cost
            or durable_parent["reserved_at"] != _iso(parent.reserved_at)
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence conflicts with parent reservation"
            )
        self._validate_parent_replay_transition(parent, durable_parent)
        durable_lease = connection.execute(
            "SELECT * FROM quota_lease WHERE lease_id = ?", (parent.lease_id,)
        ).fetchone()
        if durable_lease is None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence lease is missing"
            )
        try:
            durable_lease_units = _require_stored_quota_int(durable_lease["units"])
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence quota lease is malformed"
            ) from exc
        if (
            durable_lease["source"] != parent.source
            or durable_lease["owner"] != parent.owner
            or durable_lease["window_id"] != parent.window_id
            or durable_lease_units != parent.total_cost
            or durable_lease["granted_at"] != _iso(parent.reserved_at)
            or durable_lease["expires_at"] != _iso(parent.expires_at)
            or durable_lease["quota_reset_at"] != _iso(parent.reset_at)
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence conflicts with quota lease"
            )
        durable_window = connection.execute(
            "SELECT * FROM quota_window WHERE source = ? AND window_id = ?",
            (parent.source, parent.window_id),
        ).fetchone()
        if durable_window is None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence window is missing"
            )
        try:
            durable_window_units = _require_stored_quota_int(durable_window["total_units"])
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence quota window is malformed"
            ) from exc
        if (
            durable_window["source"] != parent.source
            or durable_window["window_id"] != parent.window_id
            or durable_window["starts_at"] != _iso(parent.window_start)
            or durable_window["resets_at"] != _iso(parent.window_end)
            or durable_window_units != parent.capacity
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence conflicts with quota window"
            )
        for allocation in parent.calls:
            self._validate_immutable_call_evidence(connection, parent, allocation)
        if call is not None:
            if call.parent_id != parent.parent_id:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence call parent conflicts"
                )
            self._validate_immutable_call_evidence(connection, parent, call)
        self._validate_current_terminal_ledger(connection, durable_parent, durable_lease)
        if operation is SourceQuotaOperationKind.RELEASE_UNUSED:
            self._validate_release_evidence(connection, parent, durable_parent, durable_lease)

    @staticmethod
    def _validate_parent_replay_transition(
        parent: SourceQuotaParentSnapshot,
        durable_parent: sqlite3.Row,
    ) -> None:
        try:
            durable_state = SourceQuotaParentState(durable_parent["state"])
        except ValueError as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence parent state is invalid"
            ) from exc
        if durable_state not in _PARENT_REPLAY_TRANSITIONS[parent.state]:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence parent transition conflicts"
            )
        durable_closed_at = durable_parent["closed_at"]
        if durable_state in {
            SourceQuotaParentState.CLOSED,
            SourceQuotaParentState.COMPENSATED,
        }:
            if not isinstance(durable_closed_at, str):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence closed parent timestamp is missing"
                )
            try:
                _parse_stored_time(durable_closed_at)
            except ValueError as exc:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence closed parent timestamp is invalid"
                ) from exc
        elif durable_closed_at is not None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence open parent has a close timestamp"
            )
        if parent.state in {
            SourceQuotaParentState.CLOSED,
            SourceQuotaParentState.COMPENSATED,
        } and durable_closed_at != _iso(parent.closed_at):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence conflicts with parent close"
            )

    def _validate_immutable_call_evidence(
        self,
        connection: sqlite3.Connection,
        parent: SourceQuotaParentSnapshot,
        allocation: SourceQuotaCallAllocation,
    ) -> None:
        durable_call = self._call_row(connection, allocation.call_id)
        if durable_call is None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence call is missing"
            )
        try:
            durable_allocation = self._call_from_row(durable_call)
            durable_cost = _require_stored_quota_int(durable_call["cost"])
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence durable call is malformed"
            ) from exc
        if (
            durable_call["parent_id"] != parent.parent_id
            or durable_call["parent_id"] != allocation.parent_id
            or durable_cost != allocation.cost
            or durable_call["intended_at"] != _iso(allocation.intended_at)
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence conflicts with call allocation"
            )
        if durable_allocation.state not in _CALL_REPLAY_TRANSITIONS[allocation.state]:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence call transition conflicts"
            )
        if allocation.state.value in _TERMINAL_CALL_STATES and durable_allocation != allocation:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence conflicts with terminal call"
            )
        if allocation.authorized_at is not None:
            usage_id = allocation.usage_id
            if (
                usage_id is None
                or durable_call["authorized_at"] != _iso(allocation.authorized_at)
                or durable_call["usage_id"] != usage_id
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence conflicts with call authorization"
                )
            durable_usage = connection.execute(
                "SELECT * FROM quota_usage WHERE usage_id = ?", (usage_id,)
            ).fetchone()
            if durable_usage is None:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence usage is missing"
                )
            try:
                durable_usage_units = _require_stored_quota_int(durable_usage["units"])
            except (TypeError, ValueError) as exc:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence usage is malformed"
                ) from exc
            if (
                durable_usage["lease_id"] != parent.lease_id
                or durable_usage_units != allocation.cost
                or durable_usage["consumed_at"] != _iso(allocation.authorized_at)
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence conflicts with quota usage"
                )
        if allocation.finalized_at is not None and durable_call["finalized_at"] != _iso(
            allocation.finalized_at
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence conflicts with call finalization"
            )

    def _validate_current_terminal_ledger(
        self,
        connection: sqlite3.Connection,
        durable_parent: sqlite3.Row,
        durable_lease: sqlite3.Row,
    ) -> None:
        try:
            parent_state = SourceQuotaParentState(durable_parent["state"])
        except ValueError as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence parent state is invalid"
            ) from exc
        if parent_state not in {
            SourceQuotaParentState.CLOSED,
            SourceQuotaParentState.COMPENSATED,
        }:
            self._validate_current_open_ledger(connection, durable_parent, durable_lease)
            return

        closed_at = durable_parent["closed_at"]
        released_at = durable_lease["released_at"]
        if not isinstance(closed_at, str) or not isinstance(released_at, str):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal ledger timestamp is missing"
            )
        try:
            closed_time = _parse_stored_time(closed_at)
            released_time = _parse_stored_time(released_at)
            reserved_time = _parse_stored_time(durable_parent["reserved_at"])
            granted_time = _parse_stored_time(durable_lease["granted_at"])
            expires_time = _parse_stored_time(durable_lease["expires_at"])
            total_cost = _require_stored_quota_int(durable_parent["total_cost"])
            lease_units = _require_stored_quota_int(durable_lease["units"])
            used_units = _require_stored_quota_int(durable_lease["used_units"])
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal ledger is malformed"
            ) from exc
        if (
            closed_at != released_at
            or total_cost <= 0
            or lease_units != total_cost
            or used_units < 0
            or used_units > total_cost
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal ledger conflicts"
            )
        if closed_time < reserved_time or released_time < granted_time:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal ledger lifecycle conflicts"
            )

        try:
            durable_calls = tuple(
                self._call_from_row(row)
                for row in connection.execute(
                    """
                    SELECT * FROM source_call_allocation
                    WHERE parent_id = ? ORDER BY intended_at, call_id
                    """,
                    (durable_parent["parent_id"],),
                ).fetchall()
            )
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence durable call is malformed"
            ) from exc
        if any(call.state.value not in _TERMINAL_CALL_STATES for call in durable_calls):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal ledger has a nonterminal call"
            )
        for call in durable_calls:
            authorized_at = call.authorized_at
            finalized_at = call.finalized_at
            if (
                call.intended_at < reserved_time
                or call.intended_at < granted_time
                or (
                    authorized_at is not None
                    and (
                        authorized_at < call.intended_at
                        or authorized_at < reserved_time
                        or authorized_at < granted_time
                        or authorized_at >= expires_time
                    )
                )
                or (
                    finalized_at is not None
                    and (
                        finalized_at < call.intended_at
                        or (authorized_at is not None and finalized_at < authorized_at)
                        or finalized_at < reserved_time
                        or finalized_at < granted_time
                        or closed_time < finalized_at
                    )
                )
            ):
                raise SourceQuotaAuthorityIntegrityError(
                    "stored journal immutable evidence terminal ledger lifecycle conflicts"
                )
        allocated_cost = sum(call.cost for call in durable_calls)
        if allocated_cost > total_cost:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal allocation exceeds reservation"
            )

        dispatched_calls = tuple(
            call
            for call in durable_calls
            if call.state
            in {
                SourceQuotaCallState.SUCCESS,
                SourceQuotaCallState.FAILURE,
                SourceQuotaCallState.UNKNOWN,
            }
        )
        expected_usage_ids = [call.usage_id for call in dispatched_calls]
        usage_rows = connection.execute(
            "SELECT * FROM quota_usage WHERE lease_id = ? ORDER BY usage_id",
            (durable_parent["lease_id"],),
        ).fetchall()
        durable_usage_ids = [row["usage_id"] for row in usage_rows]
        if (
            any(usage_id is None for usage_id in expected_usage_ids)
            or len(expected_usage_ids) != len(set(expected_usage_ids))
            or len(durable_usage_ids) != len(set(durable_usage_ids))
            or set(expected_usage_ids) != set(durable_usage_ids)
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal ledger usage set conflicts"
            )
        usage_by_id = {row["usage_id"]: row for row in usage_rows}
        try:
            usage_units = 0
            for call in dispatched_calls:
                usage = usage_by_id[call.usage_id]
                units = _require_stored_quota_int(usage["units"])
                if (
                    usage["lease_id"] != durable_parent["lease_id"]
                    or units != call.cost
                    or usage["consumed_at"] != _iso(call.authorized_at)
                ):
                    raise SourceQuotaAuthorityIntegrityError(
                        "stored journal immutable evidence terminal ledger usage conflicts"
                    )
                usage_units += units
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal ledger usage is malformed"
            ) from exc
        dispatched_cost = sum(call.cost for call in dispatched_calls)
        unused_units = total_cost - used_units
        expected_state = (
            SourceQuotaParentState.COMPENSATED
            if unused_units > 0
            else SourceQuotaParentState.CLOSED
        )
        if (
            parent_state is not expected_state
            or used_units != dispatched_cost
            or used_units != usage_units
            or total_cost != used_units + unused_units
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence terminal ledger accounting conflicts"
            )

    def _validate_current_open_ledger(
        self,
        connection: sqlite3.Connection,
        durable_parent: sqlite3.Row,
        durable_lease: sqlite3.Row,
    ) -> None:
        if durable_parent["closed_at"] is not None or durable_lease["released_at"] is not None:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence open ledger is closed"
            )
        try:
            total_cost = _require_stored_quota_int(durable_parent["total_cost"])
            lease_units = _require_stored_quota_int(durable_lease["units"])
            used_units = _require_stored_quota_int(durable_lease["used_units"])
            reserved_time = _parse_stored_time(durable_parent["reserved_at"])
            granted_time = _parse_stored_time(durable_lease["granted_at"])
            expires_time = _parse_stored_time(durable_lease["expires_at"])
            durable_calls = tuple(
                self._call_from_row(row)
                for row in connection.execute(
                    """
                    SELECT * FROM source_call_allocation
                    WHERE parent_id = ? ORDER BY intended_at, call_id
                    """,
                    (durable_parent["parent_id"],),
                ).fetchall()
            )
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence open ledger is malformed"
            ) from exc
        has_pre_reservation_call = any(
            call.intended_at < reserved_time or call.intended_at < granted_time
            for call in durable_calls
        )
        has_expired_dispatch = any(
            call.authorized_at is not None and call.authorized_at >= expires_time
            for call in durable_calls
        )
        if (
            total_cost <= 0
            or lease_units != total_cost
            or used_units < 0
            or used_units > total_cost
            or has_pre_reservation_call
            or has_expired_dispatch
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence open ledger conflicts"
            )
        dispatched_calls = tuple(
            call
            for call in durable_calls
            if call.state
            in {
                SourceQuotaCallState.DISPATCH_AUTHORIZED,
                SourceQuotaCallState.SUCCESS,
                SourceQuotaCallState.FAILURE,
                SourceQuotaCallState.UNKNOWN,
            }
        )
        expected_usage_ids = [call.usage_id for call in dispatched_calls]
        usage_rows = connection.execute(
            "SELECT * FROM quota_usage WHERE lease_id = ? ORDER BY usage_id",
            (durable_parent["lease_id"],),
        ).fetchall()
        durable_usage_ids = [row["usage_id"] for row in usage_rows]
        if (
            any(usage_id is None for usage_id in expected_usage_ids)
            or len(expected_usage_ids) != len(set(expected_usage_ids))
            or len(durable_usage_ids) != len(set(durable_usage_ids))
            or set(expected_usage_ids) != set(durable_usage_ids)
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence open ledger usage set conflicts"
            )
        usage_by_id = {row["usage_id"]: row for row in usage_rows}
        try:
            usage_units = 0
            for call in dispatched_calls:
                usage = usage_by_id[call.usage_id]
                units = _require_stored_quota_int(usage["units"])
                if (
                    usage["lease_id"] != durable_parent["lease_id"]
                    or units != call.cost
                    or usage["consumed_at"] != _iso(call.authorized_at)
                ):
                    raise SourceQuotaAuthorityIntegrityError(
                        "stored journal immutable evidence open ledger usage conflicts"
                    )
                usage_units += units
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence open ledger usage is malformed"
            ) from exc
        dispatched_cost = sum(call.cost for call in dispatched_calls)
        if (
            sum(call.cost for call in durable_calls) > total_cost
            or used_units != dispatched_cost
            or used_units != usage_units
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence open ledger accounting conflicts"
            )

    @staticmethod
    def _remaining_in_window(
        connection: sqlite3.Connection,
        window: sqlite3.Row,
        now: datetime,
    ) -> int:
        capacity = _require_stored_quota_int(window["total_units"])
        consumed = 0
        reserved = 0
        for lease in connection.execute(
            """
            SELECT quota_lease.*, EXISTS(
                SELECT 1 FROM quota_attempt
                WHERE quota_attempt.lease_id = quota_lease.lease_id
                AND quota_attempt.outcome = 'pending'
            ) AS has_pending_attempt
            FROM quota_lease
            WHERE source = ? AND window_id = ?
            """,
            (window["source"], window["window_id"]),
        ).fetchall():
            units = _require_stored_quota_int(lease["units"])
            used_units = _require_stored_quota_int(lease["used_units"])
            if used_units < 0 or used_units > units:
                raise SourceQuotaAuthorityIntegrityError(
                    "stored quota lease consumption conflicts with its units"
                )
            consumed += used_units
            if lease["released_at"] is None and (
                _parse_stored_time(lease["expires_at"]) > now or lease["has_pending_attempt"] == 1
            ):
                reserved += units - used_units
        return capacity - consumed - reserved

    def _validate_release_evidence(
        self,
        connection: sqlite3.Connection,
        parent: SourceQuotaParentSnapshot,
        durable_parent: sqlite3.Row,
        durable_lease: sqlite3.Row,
    ) -> None:
        try:
            durable_used_units = _require_stored_quota_int(durable_lease["used_units"])
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence release ledger is malformed"
            ) from exc
        if (
            durable_parent["state"] != parent.state.value
            or durable_parent["closed_at"] != _iso(parent.closed_at)
            or durable_lease["released_at"] != _iso(parent.closed_at)
            or durable_used_units != parent.consumed_cost
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence conflicts with release ledger"
            )
        try:
            durable_calls = tuple(
                self._call_from_row(row)
                for row in connection.execute(
                    """
                    SELECT * FROM source_call_allocation
                    WHERE parent_id = ? ORDER BY intended_at, call_id
                    """,
                    (parent.parent_id,),
                ).fetchall()
            )
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence release call is malformed"
            ) from exc
        if parent.calls != durable_calls:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence release call evidence conflicts"
            )
        if any(allocation.state.value not in _TERMINAL_CALL_STATES for allocation in durable_calls):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence release has a nonterminal call"
            )
        dispatched_calls = tuple(
            allocation
            for allocation in durable_calls
            if allocation.state
            in {
                SourceQuotaCallState.SUCCESS,
                SourceQuotaCallState.FAILURE,
                SourceQuotaCallState.UNKNOWN,
            }
        )
        usage_rows = connection.execute(
            "SELECT * FROM quota_usage WHERE lease_id = ? ORDER BY usage_id",
            (parent.lease_id,),
        ).fetchall()
        expected_usage_ids = {allocation.usage_id for allocation in dispatched_calls}
        durable_usage_ids = {row["usage_id"] for row in usage_rows}
        if (
            None in expected_usage_ids
            or len(usage_rows) != len(durable_usage_ids)
            or expected_usage_ids != durable_usage_ids
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence release usage set conflicts"
            )
        dispatched_cost = sum(allocation.cost for allocation in dispatched_calls)
        try:
            usage_units = sum(_require_stored_quota_int(row["units"]) for row in usage_rows)
        except (TypeError, ValueError) as exc:
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence release usage is malformed"
            ) from exc
        if (
            durable_used_units != dispatched_cost
            or dispatched_cost != usage_units
            or parent.total_cost != durable_used_units + parent.unused_released
            or parent.reserved_cost != parent.consumed_cost + parent.unused_released
        ):
            raise SourceQuotaAuthorityIntegrityError(
                "stored journal immutable evidence release accounting conflicts"
            )

    def _reject_clock_rollback(
        self,
        connection: sqlite3.Connection,
        observed_at: datetime,
    ) -> None:
        checkpoint = self._validate_global_checkpoint(connection)
        trusted = checkpoint["clock_high_water"]
        if trusted is not None and observed_at < _parse_stored_time(trusted):
            raise SourceQuotaAuthorityConflictError("authority clock rollback")

    @staticmethod
    def _require_lifecycle_time(observed_at: datetime, lower_bound: datetime) -> None:
        if observed_at < lower_bound:
            raise ValueError("call lifecycle timestamp order is invalid")

    @staticmethod
    def _require_parent_lease(
        connection: sqlite3.Connection,
        parent: sqlite3.Row,
    ) -> sqlite3.Row:
        lease = connection.execute(
            "SELECT * FROM quota_lease WHERE lease_id = ?", (parent["lease_id"],)
        ).fetchone()
        if lease is None:
            raise SourceQuotaAuthorityIntegrityError("parent quota lease is missing")
        return lease

    @staticmethod
    def _parent_row(connection: sqlite3.Connection, parent_id: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM source_parent_reservation WHERE parent_id = ?", (parent_id,)
        ).fetchone()

    @staticmethod
    def _call_row(connection: sqlite3.Connection, call_id: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM source_call_allocation WHERE call_id = ?", (call_id,)
        ).fetchone()

    def _require_open_parent(self, connection: sqlite3.Connection, parent_id: str) -> sqlite3.Row:
        parent = self._parent_row(connection, parent_id)
        if parent is None:
            raise SourceQuotaAuthorityConflictError("parent reservation does not exist")
        if parent["state"] != SourceQuotaParentState.OPEN.value:
            raise SourceQuotaAuthorityConflictError("parent reservation is not open")
        return parent

    def _require_call(
        self,
        connection: sqlite3.Connection,
        call_id: str,
        parent_id: str,
    ) -> sqlite3.Row:
        call = self._call_row(connection, call_id)
        if call is None or call["parent_id"] != parent_id:
            raise SourceQuotaAuthorityConflictError("call allocation does not belong to parent")
        return call

    def _snapshot_parent(
        self,
        connection: sqlite3.Connection,
        parent_id: str,
    ) -> SourceQuotaParentSnapshot:
        parent = self._parent_row(connection, parent_id)
        if parent is None:
            raise SourceQuotaAuthorityIntegrityError("parent disappeared during transaction")
        lease = self._require_parent_lease(connection, parent)
        window = connection.execute(
            "SELECT * FROM quota_window WHERE source = ? AND window_id = ?",
            (lease["source"], lease["window_id"]),
        ).fetchone()
        if window is None:
            raise SourceQuotaAuthorityIntegrityError("parent quota window is missing")
        calls = tuple(
            self._call_from_row(row)
            for row in connection.execute(
                """
                SELECT * FROM source_call_allocation
                WHERE parent_id = ? ORDER BY intended_at, call_id
                """,
                (parent_id,),
            ).fetchall()
        )
        state = SourceQuotaParentState(parent["state"])
        consumed = _require_stored_quota_int(lease["used_units"])
        unused = (
            _require_stored_quota_int(parent["total_cost"]) - consumed
            if state
            in {
                SourceQuotaParentState.CLOSED,
                SourceQuotaParentState.COMPENSATED,
            }
            else 0
        )
        return SourceQuotaParentSnapshot(
            parent_id=parent["parent_id"],
            source=parent["source"],
            owner=parent["owner"],
            claim_binding_hash=parent["claim_binding_hash"],
            claim_generation=parent["claim_generation"],
            scheduler_fencing_token=parent["scheduler_fencing_token"],
            lease_id=parent["lease_id"],
            total_cost=parent["total_cost"],
            reserved_at=parent["reserved_at"],
            expires_at=lease["expires_at"],
            window_id=window["window_id"],
            window_start=window["starts_at"],
            window_end=window["resets_at"],
            reset_at=lease["quota_reset_at"],
            capacity=window["total_units"],
            state=state,
            closed_at=parent["closed_at"],
            reserved_cost=parent["total_cost"],
            consumed_cost=consumed,
            unused_released=unused,
            calls=calls,
        )

    def _snapshot_call(
        self,
        connection: sqlite3.Connection,
        call_id: str,
    ) -> SourceQuotaCallAllocation:
        row = self._call_row(connection, call_id)
        if row is None:
            raise SourceQuotaAuthorityIntegrityError("call disappeared during transaction")
        return self._call_from_row(row)

    @staticmethod
    def _call_from_row(row: sqlite3.Row) -> SourceQuotaCallAllocation:
        return SourceQuotaCallAllocation(
            call_id=row["call_id"],
            parent_id=row["parent_id"],
            cost=row["cost"],
            state=row["state"],
            outcome=row["outcome"],
            intended_at=row["intended_at"],
            authorized_at=row["authorized_at"],
            finalized_at=row["finalized_at"],
            usage_id=row["usage_id"],
        )


__all__ = [
    "SourceQuotaAuthorityConflictError",
    "SourceQuotaAuthorityError",
    "SourceQuotaAuthorityIntegrityError",
    "SourceQuotaAuthorityRepairRequiredError",
    "SourceQuotaAuthorityRepairState",
    "SourceQuotaAuthorityResult",
    "SourceQuotaCallAllocation",
    "SourceQuotaCallOutcome",
    "SourceQuotaCallState",
    "SourceQuotaOperationKind",
    "SourceQuotaOperationReceipt",
    "SourceQuotaParentAuthority",
    "SourceQuotaParentSnapshot",
    "SourceQuotaParentState",
    "SourceQuotaReceiptSigner",
]
