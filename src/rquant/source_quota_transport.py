"""Transport-call quota accounting backed by the durable source quota ledger."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol, TypeVar

from pydantic import Field, model_validator

from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.source_quota_store import (
    SourceQuotaAttempt,
    SourceQuotaAttemptOutcome,
    SourceQuotaConflictError,
    SourceQuotaStore,
)

_T = TypeVar("_T")


class SourceTransportCallReceipt(RuntimeContractModel):
    """Auditable proof that one real transport call consumed one quota unit."""

    source: str = Field(min_length=1)
    logical_request_id: str = Field(min_length=1)
    api_name: str = Field(min_length=1)
    call_ordinal: int = Field(strict=True, ge=1)
    attempt_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    outcome: SourceQuotaAttemptOutcome
    dispatched_at: datetime
    committed_at: datetime


class SourceTransportUsageReceipt(RuntimeContractModel):
    """Aggregate of the real calls made for one logical source request."""

    source: str = Field(min_length=1)
    logical_request_id: str = Field(min_length=1)
    actual_call_count: int = Field(strict=True, ge=1)
    call_receipts: tuple[SourceTransportCallReceipt, ...]

    @model_validator(mode="after")
    def validate_calls(self) -> SourceTransportUsageReceipt:
        if self.actual_call_count != len(self.call_receipts):
            raise ValueError("actual_call_count does not match call_receipts")
        if any(
            receipt.source != self.source or receipt.logical_request_id != self.logical_request_id
            for receipt in self.call_receipts
        ):
            raise ValueError("transport receipts do not match their aggregate")
        return self


class SourceTransportObserver(Protocol):
    """Small observer protocol suitable for adapters and a future source broker."""

    def observe(self, api_name: str, call: Callable[[], _T]) -> _T: ...

    def current_receipts(self) -> tuple[SourceTransportCallReceipt, ...]: ...

    def request_attempts(self, logical_request_id: str) -> tuple[SourceQuotaAttempt, ...]: ...

    def request_outcome(self, logical_request_id: str) -> SourceQuotaAttemptOutcome | None: ...


@dataclass
class _TransportScope:
    logical_request_id: str
    next_ordinal: int = 1
    receipts: list[SourceTransportCallReceipt] = field(default_factory=list)


class QuotaBoundTransportObserver:
    """Charge and dispatch durably immediately before each external call."""

    def __init__(
        self,
        *,
        path: Path | None = None,
        store: SourceQuotaStore | None = None,
        source: str,
        quota_units_per_window: int,
        window_kind: Literal["day", "minute"],
        clock: Callable[[], datetime],
    ) -> None:
        if (path is None) == (store is None):
            raise ValueError("exactly one of path or store is required")
        normalized_source = source.strip()
        if not normalized_source:
            raise ValueError("source must be nonempty")
        if quota_units_per_window < 1:
            raise ValueError("quota_units_per_window must be positive")
        self.store = store or SourceQuotaStore(Path(path))  # type: ignore[arg-type]
        self.source = normalized_source
        self.quota_units_per_window = quota_units_per_window
        self.window_kind = window_kind
        self._clock = clock
        self._scope: ContextVar[_TransportScope | None] = ContextVar(
            f"quota_transport_scope:{normalized_source}", default=None
        )

    @staticmethod
    def _stable_id(payload: object) -> str:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        return hashlib.sha256(encoded).hexdigest()

    @contextmanager
    def scope(
        self,
        *,
        logical_request_id: str,
        observed_at: datetime,
    ) -> Iterator[None]:
        identifier = logical_request_id.strip()
        if not identifier:
            raise ValueError("logical_request_id must be nonempty")
        if self._scope.get() is not None:
            raise SourceQuotaConflictError("transport quota scopes cannot be nested")
        normalize_aware_utc(observed_at)
        token: Token[_TransportScope | None] = self._scope.set(
            _TransportScope(logical_request_id=identifier)
        )
        try:
            yield
        finally:
            self._scope.reset(token)

    def current_receipts(self) -> tuple[SourceTransportCallReceipt, ...]:
        scope = self._scope.get()
        if scope is None:
            raise SourceQuotaConflictError("transport call requires an active quota scope")
        return tuple(scope.receipts)

    def get_call_attempt(
        self,
        *,
        logical_request_id: str,
        api_name: str,
        call_ordinal: int = 1,
    ) -> SourceQuotaAttempt | None:
        if call_ordinal < 1:
            raise ValueError("call_ordinal must be positive")
        return self.store.get_attempt(
            self._attempt_id(
                logical_request_id=logical_request_id,
                api_name=api_name,
                call_ordinal=call_ordinal,
            )
        )

    def request_attempts(self, logical_request_id: str) -> tuple[SourceQuotaAttempt, ...]:
        return self.store.list_transport_attempts(
            source=self.source,
            logical_request_id=logical_request_id,
        )

    def request_outcome(self, logical_request_id: str) -> SourceQuotaAttemptOutcome | None:
        attempts = self.request_attempts(logical_request_id)
        if not attempts:
            return None
        outcomes = {attempt.outcome for attempt in attempts}
        for outcome in (
            SourceQuotaAttemptOutcome.PENDING,
            SourceQuotaAttemptOutcome.UNKNOWN,
            SourceQuotaAttemptOutcome.FAILURE,
            SourceQuotaAttemptOutcome.SUCCESS,
        ):
            if outcome in outcomes:
                return outcome
        raise AssertionError("source quota attempt outcome is not exhaustive")

    def remaining(self, *, now: datetime) -> int:
        return self.store.remaining(self.source, now=now)

    def _now(self) -> datetime:
        return normalize_aware_utc(self._clock())

    def _attempt_id(
        self,
        *,
        logical_request_id: str,
        api_name: str,
        call_ordinal: int,
    ) -> str:
        return self._stable_id(
            {
                "protocol": "source-transport-attempt-v1",
                "source": self.source,
                "logical_request_id": logical_request_id,
                "api_name": api_name,
                "call_ordinal": call_ordinal,
            }
        )

    def observe(self, api_name: str, call: Callable[[], _T]) -> _T:
        scope = self._scope.get()
        if scope is None:
            raise SourceQuotaConflictError("transport call requires an active quota scope")
        normalized_api = api_name.strip()
        if not normalized_api:
            raise ValueError("api_name must be nonempty")
        ordinal = scope.next_ordinal
        scope.next_ordinal += 1
        attempt_id = self._attempt_id(
            logical_request_id=scope.logical_request_id,
            api_name=normalized_api,
            call_ordinal=ordinal,
        )
        attempt, created = self.store.begin_transport_dispatch(
            source=self.source,
            owner=f"transport:{attempt_id}",
            attempt_id=attempt_id,
            logical_request_id=scope.logical_request_id,
            api_name=normalized_api,
            call_ordinal=ordinal,
            units=1,
            total_units=self.quota_units_per_window,
            window_kind=self.window_kind,
            clock=self._clock,
        )
        if not created and attempt.outcome is not SourceQuotaAttemptOutcome.PENDING:
            raise SourceQuotaConflictError(
                f"transport attempt already completed: {attempt.attempt_id}"
            )
        if not created:
            raise SourceQuotaConflictError(
                f"transport attempt dispatch is uncertain: {attempt.attempt_id}"
            )
        dispatched = attempt
        try:
            result = call()
        except Exception:
            completed = self.store.commit_attempt(
                attempt.attempt_id,
                outcome=SourceQuotaAttemptOutcome.FAILURE,
                now=self._now(),
            )
            scope.receipts.append(
                SourceTransportCallReceipt(
                    source=self.source,
                    logical_request_id=scope.logical_request_id,
                    api_name=normalized_api,
                    call_ordinal=ordinal,
                    attempt_id=completed.attempt_id,
                    outcome=completed.outcome,
                    dispatched_at=dispatched.dispatched_at,
                    committed_at=completed.committed_at,
                )
            )
            raise
        completed = self.store.commit_attempt(
            attempt.attempt_id,
            outcome=SourceQuotaAttemptOutcome.SUCCESS,
            now=self._now(),
        )
        scope.receipts.append(
            SourceTransportCallReceipt(
                source=self.source,
                logical_request_id=scope.logical_request_id,
                api_name=normalized_api,
                call_ordinal=ordinal,
                attempt_id=completed.attempt_id,
                outcome=completed.outcome,
                dispatched_at=dispatched.dispatched_at,
                committed_at=completed.committed_at,
            )
        )
        return result


__all__ = [
    "QuotaBoundTransportObserver",
    "SourceTransportCallReceipt",
    "SourceTransportObserver",
    "SourceTransportUsageReceipt",
]
