"""Typed, idempotent notification-outbox producer for daily-close stages."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta

from pydantic import Field, model_validator

from rquant.delivery_contracts import DeliveryTarget, RouterDisposition, RouterReceipt
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.signal_bus import SignalBusStore, canonical_delivery_targets
from rquant.signal_contracts import SignalAction, SignalEnvelope


class DailyNotificationOutboxReceipt(RuntimeContractModel):
    """The durable signal and delivery records created for one daily event."""

    signal_receipt: RouterReceipt
    outbox_ids: tuple[str, ...] = Field(default_factory=tuple)

    @model_validator(mode="after")
    def validate_outbox_ids(self) -> DailyNotificationOutboxReceipt:
        if len(self.outbox_ids) != len(set(self.outbox_ids)):
            raise ValueError("daily notification outbox ids must be unique")
        return self

    @property
    def signal_id(self) -> str:
        return self.signal_receipt.signal_id


class DailyNotificationProducerError(RuntimeError):
    """A daily notification cannot be durably bound to its requested signal."""


def build_daily_error_signal(
    *,
    component: str,
    error: BaseException,
    trade_date: date,
    observed_at: datetime,
    producer_commit: str,
    ttl: timedelta = timedelta(days=7),
) -> SignalEnvelope:
    """Create a typed, de-duplicated error signal without exposing exception text."""
    normalized_component = component.strip()
    if not normalized_component:
        raise ValueError("daily error component must not be empty")
    if ttl <= timedelta(0):
        raise ValueError("daily error signal ttl must be positive")
    now = normalize_aware_utc(observed_at)
    error_type = type(error).__name__
    identity = {
        "contract": "daily-close-cli-error/v1",
        "component": normalized_component,
        "error_type": error_type,
        "trade_date": trade_date.isoformat(),
    }
    return SignalEnvelope(
        schema_version=1,
        strategy_id="daily-close-error",
        strategy_version="daily-close-dag/v1",
        parameter_fingerprint=canonical_sha256(
            {
                "contract": "daily-close-error-parameters/v1",
                "component": normalized_component,
            }
        ),
        dataset_snapshot_id=canonical_sha256(
            {"contract": "daily-close-error-dataset/v1", "trade_date": trade_date.isoformat()}
        ),
        feature_snapshot_id=canonical_sha256(identity),
        event_time=now,
        available_at=now,
        candidate_id=f"daily-error:{trade_date.isoformat()}:{normalized_component}:{error_type}",
        action=SignalAction.WATCH,
        reason_codes=("daily_stage_error",),
        evidence={
            "component": normalized_component,
            "error_type": error_type,
            "trade_date": trade_date.isoformat(),
        },
        expires_at=now + ttl,
        producer_commit=producer_commit,
    )


class DailyNotificationProducer:
    """Persist signals and delivery outbox rows, never performing provider I/O."""

    def __init__(
        self,
        *,
        signal_bus: SignalBusStore,
        targets: Iterable[DeliveryTarget] = (),
    ) -> None:
        self._signal_bus = signal_bus
        self._targets = canonical_delivery_targets(targets)

    def emit(
        self,
        signal: SignalEnvelope,
        *,
        received_at: datetime,
    ) -> DailyNotificationOutboxReceipt:
        receipt = self._signal_bus.ingest(signal, received_at=received_at)
        if (
            receipt.signal_id != signal.signal_id
            or receipt.disposition
            not in {RouterDisposition.ACCEPTED, RouterDisposition.DUPLICATE}
        ):
            raise DailyNotificationProducerError(
                "daily notification signal ingest rejected; refusing to route"
            )
        stored_signal = self._signal_bus.signal(signal.signal_id)
        if stored_signal != signal:
            raise DailyNotificationProducerError(
                "daily notification signal payload does not match accepted identity"
            )
        outbox = self._signal_bus.route(signal.signal_id, self._targets, now=received_at)
        if any(
            record.signal_id != signal.signal_id
            or record.outbox_id != record.target.delivery_key(signal.signal_id)
            for record in outbox
        ):
            raise DailyNotificationProducerError(
                "daily notification outbox binding changed after routing"
            )
        outbox_ids = tuple(record.outbox_id for record in outbox)
        if any(identifier is None for identifier in outbox_ids):
            raise RuntimeError("daily notification outbox record has no identity")
        return DailyNotificationOutboxReceipt(
            signal_receipt=receipt,
            outbox_ids=tuple(
                sorted(identifier for identifier in outbox_ids if identifier is not None)
            ),
        )
