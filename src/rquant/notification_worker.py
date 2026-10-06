"""Pure notification worker runtime over the durable signal bus."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Protocol, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.delivery_contracts import DeliveryChannel, OutboxRecord, OutboxStatus
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import (
    CurrentSignalEnvelope,
    SignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)

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
    signal: SignalEnvelopeFamily
    record: OutboxRecord
    deadline: AwareUtcDatetime

    @field_validator("signal", mode="before")
    @classmethod
    def dispatch_signal_family(cls, value: object) -> SignalEnvelopeFamily:
        if isinstance(value, (SignalEnvelope, CurrentSignalEnvelope)):
            return value
        if isinstance(value, (Mapping, str, bytes, bytearray)):
            return parse_signal_envelope(value)
        raise TypeError("signal must be a stored signal envelope")

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


def _run_price_notification_item(
    store: object,
    provider: object,
    event: object,
    record: OutboxRecord,
    *,
    activation: object,
    worker_id: str,
    now: datetime,
    clock: Callable[[], datetime],
) -> NotificationItemResult:
    from rquant.price_alert_runtime_projection import (
        PriceAlertAuthorityConflict,
        PriceAlertAuthorityUnavailable,
        PriceAlertDeliveryRejected,
    )
    from rquant.runtime_notification_providers import (
        RecipientScopedNotificationProvider,
        SuppressedNotificationProvider,
    )

    try:
        authority = store.price_alert_delivery_authority()
        if authority is None:
            raise PriceAlertAuthorityUnavailable("price authority has not been applied")
        if type(provider) not in {
            RecipientScopedNotificationProvider,
            SuppressedNotificationProvider,
        }:
            raise TypeError("price delivery requires an actual recipient-scoped provider")
        prepared = provider.prepare_price(event, record)
    except Exception:
        return _release_not_attempted(
            store,
            record,
            worker_id=worker_id,
            observed_at=now,
            reason="price notification static preparation is unavailable",
        )
    admitted_at = max(now, _utc(clock()))
    try:
        admitted = store.admit_price_alert_delivery(
            record,
            activation=activation,
            worker_id=worker_id,
            expected_revision=authority.authority_revision,
            admitted_at=admitted_at,
        )
    except (
        PriceAlertAuthorityConflict,
        PriceAlertDeliveryRejected,
        PriceAlertAuthorityUnavailable,
    ):
        try:
            current = store.price_alert_delivery_authority()
            cancelled = (
                None
                if current is None
                else store.cancel_price_unadmitted(
                    record.outbox_id,
                    worker_id=worker_id,
                    expected_revision=current.authority_revision,
                    cancelled_at=admitted_at,
                )
            )
        except (PriceAlertAuthorityConflict, PriceAlertAuthorityUnavailable):
            cancelled = None
        except Exception:
            cancelled = None
        if cancelled is not None:
            return _result(
                record,
                outcome=NotificationItemOutcome.NOT_ATTEMPTED,
                observed_at=admitted_at,
                error="price event cancelled before admission",
            )
        return _release_not_attempted(
            store,
            record,
            worker_id=worker_id,
            observed_at=admitted_at,
            reason="price authority changed or is unavailable",
        )
    except Exception:
        try:
            committed = store.price_alert_send_admission(record.outbox_id, record.attempt_count)
        except Exception:
            committed = True
        if committed is not None:
            return _record_unknown(
                store,
                record,
                worker_id=worker_id,
                observed_at=admitted_at,
                error="price admission commit outcome is uncertain; no automatic resend",
            )
        return _release_not_attempted(
            store,
            record,
            worker_id=worker_id,
            observed_at=admitted_at,
            reason="price admission was not committed",
        )
    if admitted is None:
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=admitted_at,
            error="persisted price admission cannot authorize another provider call",
        )
    try:
        receipt = provider.deliver_price(
            prepared, admitted, store=store, record=record, now=max(admitted_at, _utc(clock()))
        )
    except ConfirmedDeliveryFailureError:
        return _complete_known_failure(
            store,
            record,
            worker_id=worker_id,
            completed_at=max(admitted_at, _utc(clock())),
            error="provider rejected price delivery",
        )
    except Exception:
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=max(admitted_at, _utc(clock())),
            error="price notification delivery outcome is unknown",
        )
    completed_at = max(admitted_at, _utc(clock()))
    if not isinstance(receipt, str) or not receipt.strip():
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=completed_at,
            error="provider returned an invalid price receipt",
        )
    try:
        store.complete_success(
            record.outbox_id,
            worker_id=worker_id,
            attempt_no=record.attempt_count,
            completed_at=completed_at,
            provider_receipt=receipt,
        )
    except Exception:
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=completed_at,
            error="price success write-back is unknown",
            provider_receipt=receipt,
        )
    return _result(
        record,
        outcome=NotificationItemOutcome.SUCCEEDED,
        observed_at=completed_at,
        provider_receipt=receipt,
    )


def _run_condition_notification_item(
    store: object,
    provider: object,
    event: object,
    record: OutboxRecord,
    *,
    activation: object,
    worker_id: str,
    now: datetime,
    clock: Callable[[], datetime],
) -> NotificationItemResult:
    from rquant.condition_alert_runtime_projection import (
        ConditionAlertAuthorityConflict,
        ConditionAlertAuthorityUnavailable,
        ConditionAlertDeliveryRejected,
    )
    from rquant.runtime_notification_providers import (
        RecipientScopedNotificationProvider,
        SuppressedNotificationProvider,
    )

    try:
        authority = store.condition_alert_delivery_authority()
        if authority is None:
            raise ConditionAlertAuthorityUnavailable("condition authority has not been applied")
        if type(provider) not in {
            RecipientScopedNotificationProvider,
            SuppressedNotificationProvider,
        }:
            raise TypeError("condition delivery requires an actual recipient-scoped provider")
        prepared = provider.prepare_condition(event, record)
    except Exception:
        return _release_not_attempted(
            store,
            record,
            worker_id=worker_id,
            observed_at=now,
            reason="condition notification static preparation is unavailable",
        )
    admitted_at = max(now, _utc(clock()))
    try:
        admitted = store.admit_condition_alert_delivery(
            record,
            activation=activation,
            worker_id=worker_id,
            expected_revision=authority.authority_revision,
            admitted_at=admitted_at,
        )
    except (
        ConditionAlertAuthorityConflict,
        ConditionAlertDeliveryRejected,
        ConditionAlertAuthorityUnavailable,
    ):
        try:
            current = store.condition_alert_delivery_authority()
            cancelled = (
                None
                if current is None
                else store.cancel_condition_unadmitted(
                    record.outbox_id,
                    worker_id=worker_id,
                    expected_revision=current.authority_revision,
                    cancelled_at=admitted_at,
                )
            )
        except (ConditionAlertAuthorityConflict, ConditionAlertAuthorityUnavailable):
            cancelled = None
        except Exception:
            cancelled = None
        if cancelled is not None:
            return _result(
                record,
                outcome=NotificationItemOutcome.NOT_ATTEMPTED,
                observed_at=admitted_at,
                error="condition event cancelled before admission",
            )
        return _release_not_attempted(
            store,
            record,
            worker_id=worker_id,
            observed_at=admitted_at,
            reason="condition authority changed or is unavailable",
        )
    except Exception:
        try:
            committed = store.condition_alert_send_admission(record.outbox_id, record.attempt_count)
        except Exception:
            committed = True
        if committed is not None:
            return _record_unknown(
                store,
                record,
                worker_id=worker_id,
                observed_at=admitted_at,
                error="condition admission commit outcome is uncertain; no automatic resend",
            )
        return _release_not_attempted(
            store,
            record,
            worker_id=worker_id,
            observed_at=admitted_at,
            reason="condition admission was not committed",
        )
    if admitted is None:
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=admitted_at,
            error="persisted condition admission cannot authorize another provider call",
        )
    try:
        receipt = provider.deliver_condition(
            prepared, admitted, store=store, record=record, now=max(admitted_at, _utc(clock()))
        )
    except ConfirmedDeliveryFailureError:
        return _complete_known_failure(
            store,
            record,
            worker_id=worker_id,
            completed_at=max(admitted_at, _utc(clock())),
            error="provider rejected condition delivery",
        )
    except Exception:
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=max(admitted_at, _utc(clock())),
            error="condition notification delivery outcome is unknown",
        )
    completed_at = max(admitted_at, _utc(clock()))
    if not isinstance(receipt, str) or not receipt.strip():
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=completed_at,
            error="provider returned an invalid condition receipt",
        )
    try:
        store.complete_success(
            record.outbox_id,
            worker_id=worker_id,
            attempt_no=record.attempt_count,
            completed_at=completed_at,
            provider_receipt=receipt,
        )
    except Exception:
        return _record_unknown(
            store,
            record,
            worker_id=worker_id,
            observed_at=completed_at,
            error="condition success write-back is unknown",
            provider_receipt=receipt,
        )
    return _result(
        record,
        outcome=NotificationItemOutcome.SUCCEEDED,
        observed_at=completed_at,
        provider_receipt=receipt,
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
    price_activation: object | None = None,
    condition_activation: object | None = None,
) -> NotificationRunSummary:
    """Claim and deliver one bounded batch without owning provider or retry policy."""

    started_at = _utc(now)
    provider_by_channel = dict(providers)
    current_time = clock or (lambda: datetime.now(UTC))
    if condition_activation is not None:
        from rquant.notification_state import NotificationStateStore

        if type(store) is not NotificationStateStore:
            raise TypeError("condition execution requires the original notifier store")
        include_price = False
        if price_activation is not None:
            from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation
            from rquant.price_alert_runtime_projection import (
                PriceAlertAuthorityUnavailable,
                _fresh_authority,
            )

            binding = require_verified_price_alert_activation(price_activation, "notifier")
            if binding.delivery_enabled:
                try:
                    _fresh_authority(store.price_alert_delivery_authority(), started_at)
                    include_price = True
                except PriceAlertAuthorityUnavailable:
                    pass
        claimed = store.claim_due_with_condition_activation(
            worker_id,
            activation=condition_activation,
            now=started_at,
            lease_for=lease_for,
            limit=limit,
            include_price=include_price,
        )
    elif price_activation is None:
        claimed = store.claim_due(worker_id, now=started_at, lease_for=lease_for, limit=limit)
    else:
        from rquant.notification_state import NotificationStateStore

        if type(store) is not NotificationStateStore:
            raise TypeError("price notification execution requires the original notifier store")
        claimed = store.claim_due_with_price_activation(
            worker_id, activation=price_activation, now=started_at, lease_for=lease_for, limit=limit
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
        if condition_activation is not None:
            from rquant.condition_alert_route import ConditionAlertBusEventRecord

            event = store.notification_event(record.signal_id)
            if type(event) is ConditionAlertBusEventRecord:
                result = _run_condition_notification_item(
                    store,
                    provider_by_channel.get(record.target.channel),
                    event,
                    record,
                    activation=condition_activation,
                    worker_id=worker_id,
                    now=cursor_time,
                    clock=current_time,
                )
                items.append(result)
                cursor_time = max(cursor_time, result.observed_at)
                continue
        if price_activation is not None:
            from rquant.price_alert_route import PriceAlertBusEventRecord

            event = store.notification_event(record.signal_id)
            if type(event) is PriceAlertBusEventRecord:
                result = _run_price_notification_item(
                    store,
                    provider_by_channel.get(record.target.channel),
                    event,
                    record,
                    activation=price_activation,
                    worker_id=worker_id,
                    now=cursor_time,
                    clock=current_time,
                )
                items.append(result)
                cursor_time = max(cursor_time, result.observed_at)
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
