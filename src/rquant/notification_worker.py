"""Pure notification worker runtime over the durable signal bus."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Protocol, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.delivery_contracts import DeliveryChannel, OutboxRecord, OutboxStatus
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalEnvelope

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class UnknownDeliveryOutcomeError(RuntimeError):
    """The provider may have delivered, so the active lease must not be retried."""


class ConfirmedDeliveryFailureError(RuntimeError):
    """The provider proves it did not accept the delivery, so retry is safe."""


class NotificationProvider(Protocol):
    """Injected channel adapter; concrete providers own all network behavior."""

    def deliver(self, delivery: NotificationDelivery) -> str:
        """Return a durable provider receipt or raise an explicit failure."""


class NotificationItemOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"
    NOT_ATTEMPTED = "not_attempted"


class NotificationDelivery(RuntimeContractModel):
    signal: SignalEnvelope
    record: OutboxRecord
    deadline: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_delivery(self) -> Self:
        if self.record.status is not OutboxStatus.LEASED:
            raise ValueError("notification delivery requires a leased outbox record")
        if self.record.signal_id != self.signal.signal_id:
            raise ValueError("signal does not match the leased outbox record")
        if self.record.lease_until != self.deadline:
            raise ValueError("deadline must match the active lease deadline")
        return self


class NotificationItemResult(RuntimeContractModel):
    outbox_id: Sha256
    signal_id: Sha256
    channel: DeliveryChannel
    attempt_no: int = Field(ge=1)
    outcome: NotificationItemOutcome
    observed_at: AwareUtcDatetime
    provider_receipt: str | None = Field(default=None, min_length=1)
    error: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.outcome is NotificationItemOutcome.SUCCEEDED:
            if self.provider_receipt is None or self.error is not None:
                raise ValueError("successful notification requires only a provider receipt")
        elif self.error is None or self.provider_receipt is not None:
            raise ValueError("non-success notification requires only an error")
        return self


class NotificationRunSummary(RuntimeContractModel):
    worker_id: str = Field(min_length=1)
    started_at: AwareUtcDatetime
    finished_at: AwareUtcDatetime
    lease_seconds: float = Field(gt=0)
    requested_limit: int = Field(ge=1)
    claimed_count: int = Field(ge=0)
    succeeded_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    unknown_count: int = Field(ge=0)
    not_attempted_count: int = Field(ge=0)
    items: tuple[NotificationItemResult, ...]

    @model_validator(mode="after")
    def validate_summary(self) -> Self:
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must be at or after started_at")
        if self.claimed_count != len(self.items):
            raise ValueError("claimed_count must equal item count")
        expected = {
            NotificationItemOutcome.SUCCEEDED: self.succeeded_count,
            NotificationItemOutcome.FAILED: self.failed_count,
            NotificationItemOutcome.UNKNOWN: self.unknown_count,
            NotificationItemOutcome.NOT_ATTEMPTED: self.not_attempted_count,
        }
        for outcome, count in expected.items():
            if count != sum(item.outcome is outcome for item in self.items):
                raise ValueError(f"{outcome.value}_count does not match items")
        return self


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _error_text(error: BaseException) -> str:
    message = str(error).strip() or "no detail"
    return f"{type(error).__name__}: {message}"


def _result(
    record: OutboxRecord,
    *,
    outcome: NotificationItemOutcome,
    observed_at: datetime,
    provider_receipt: str | None = None,
    error: str | None = None,
) -> NotificationItemResult:
    return NotificationItemResult(
        outbox_id=record.outbox_id,
        signal_id=record.signal_id,
        channel=record.target.channel,
        attempt_no=record.attempt_count,
        outcome=outcome,
        observed_at=observed_at,
        provider_receipt=provider_receipt,
        error=error,
    )


def _complete_known_failure(
    store: SignalBusStore,
    record: OutboxRecord,
    *,
    worker_id: str,
    completed_at: datetime,
    error: str,
) -> NotificationItemResult:
    try:
        store.complete_failure(
            record.outbox_id,
            worker_id=worker_id,
            attempt_no=record.attempt_count,
            completed_at=completed_at,
            error=error,
        )
    except Exception as write_error:
        return _result(
            record,
            outcome=NotificationItemOutcome.UNKNOWN,
            observed_at=completed_at,
            error=f"failure write-back unknown: {_error_text(write_error)}",
        )
    return _result(
        record,
        outcome=NotificationItemOutcome.FAILED,
        observed_at=completed_at,
        error=error,
    )


def _record_unknown(
    store: SignalBusStore,
    record: OutboxRecord,
    *,
    worker_id: str,
    observed_at: datetime,
    error: str,
    provider_receipt: str | None = None,
) -> NotificationItemResult:
    try:
        store.record_unknown_delivery(
            record.outbox_id,
            worker_id=worker_id,
            attempt_no=record.attempt_count,
            observed_at=observed_at,
            reason=error,
            provider_receipt=provider_receipt,
        )
    except Exception as write_error:
        error = f"{error}; unknown evidence write failed: {_error_text(write_error)}"
    return _result(
        record,
        outcome=NotificationItemOutcome.UNKNOWN,
        observed_at=observed_at,
        error=error,
    )


def _release_not_attempted(
    store: SignalBusStore,
    record: OutboxRecord,
    *,
    worker_id: str,
    observed_at: datetime,
    reason: str,
) -> NotificationItemResult:
    try:
        store.release_unattempted(
            record.outbox_id,
            worker_id=worker_id,
            attempt_no=record.attempt_count,
            released_at=observed_at,
            reason=reason,
        )
    except Exception as write_error:
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=observed_at,
            error=f"unattempted release failed: {_error_text(write_error)}",
        )
    return _result(
        record,
        outcome=NotificationItemOutcome.NOT_ATTEMPTED,
        observed_at=observed_at,
        error=reason,
    )


def run_notification_batch(
    store: SignalBusStore,
    providers: Mapping[DeliveryChannel, NotificationProvider],
    *,
    worker_id: str,
    now: datetime,
    lease_for: timedelta,
    limit: int,
    clock: Callable[[], datetime] | None = None,
) -> NotificationRunSummary:
    """Claim and deliver one bounded batch without owning provider or retry policy."""

    started_at = _utc(now)
    provider_by_channel = dict(providers)
    current_time = clock or (lambda: datetime.now(UTC))
    claimed = store.claim_due(
        worker_id,
        now=started_at,
        lease_for=lease_for,
        limit=limit,
    )
    items: list[NotificationItemResult] = []
    cursor_time = started_at

    for record in claimed:
        cursor_time = max(cursor_time, _utc(current_time()))
        assert record.lease_until is not None
        if cursor_time >= record.lease_until or cursor_time >= record.expires_at:
            items.append(
                _release_not_attempted(
                    store,
                    record,
                    worker_id=worker_id,
                    observed_at=cursor_time,
                    reason="batch lease elapsed before provider call",
                )
            )
            continue
        try:
            signal = store.signal(record.signal_id)
            if signal is None:
                raise RuntimeError("leased signal is missing")
            delivery = NotificationDelivery(
                signal=signal,
                record=record,
                deadline=record.lease_until,
            )
        except Exception as error:
            items.append(
                _release_not_attempted(
                    store,
                    record,
                    worker_id=worker_id,
                    observed_at=cursor_time,
                    reason=f"delivery preparation failed: {_error_text(error)}",
                )
            )
            continue
        provider = provider_by_channel.get(record.target.channel)
        if provider is None:
            completed_at = _utc(current_time())
            cursor_time = completed_at
            items.append(
                _complete_known_failure(
                    store,
                    record,
                    worker_id=worker_id,
                    completed_at=completed_at,
                    error=(f"no provider configured for {record.target.channel.value}"),
                )
            )
            continue

        try:
            receipt = provider.deliver(delivery)
        except ConfirmedDeliveryFailureError as error:
            completed_at = _utc(current_time())
            cursor_time = completed_at
            items.append(
                _complete_known_failure(
                    store,
                    record,
                    worker_id=worker_id,
                    completed_at=completed_at,
                    error=_error_text(error),
                )
            )
            continue
        except Exception as error:
            completed_at = _utc(current_time())
            cursor_time = completed_at
            items.append(
                _record_unknown(
                    store,
                    record,
                    worker_id=worker_id,
                    observed_at=completed_at,
                    error=_error_text(error),
                )
            )
            continue

        completed_at = _utc(current_time())
        cursor_time = completed_at
        if not isinstance(receipt, str) or not receipt.strip():
            items.append(
                _record_unknown(
                    store,
                    record,
                    worker_id=worker_id,
                    observed_at=completed_at,
                    error="provider returned an empty or invalid receipt after delivery",
                )
            )
            continue
        try:
            store.complete_success(
                record.outbox_id,
                worker_id=worker_id,
                attempt_no=record.attempt_count,
                completed_at=completed_at,
                provider_receipt=receipt,
            )
        except Exception as error:
            items.append(
                _record_unknown(
                    store,
                    record,
                    worker_id=worker_id,
                    observed_at=completed_at,
                    error=f"success write-back unknown: {_error_text(error)}",
                    provider_receipt=receipt,
                )
            )
        else:
            items.append(
                _result(
                    record,
                    outcome=NotificationItemOutcome.SUCCEEDED,
                    observed_at=completed_at,
                    provider_receipt=receipt,
                )
            )

    finished_at = _utc(current_time())
    succeeded_count = sum(item.outcome is NotificationItemOutcome.SUCCEEDED for item in items)
    failed_count = sum(item.outcome is NotificationItemOutcome.FAILED for item in items)
    unknown_count = sum(item.outcome is NotificationItemOutcome.UNKNOWN for item in items)
    not_attempted_count = sum(
        item.outcome is NotificationItemOutcome.NOT_ATTEMPTED for item in items
    )
    return NotificationRunSummary(
        worker_id=worker_id,
        started_at=started_at,
        finished_at=finished_at,
        lease_seconds=lease_for.total_seconds(),
        requested_limit=limit,
        claimed_count=len(claimed),
        succeeded_count=succeeded_count,
        failed_count=failed_count,
        unknown_count=unknown_count,
        not_attempted_count=not_attempted_count,
        items=tuple(items),
    )
