from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.daily_notification_producer import (
    DailyNotificationProducer,
    DailyNotificationProducerError,
)
from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    RouterDisposition,
    RouterReceipt,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 8, 3, 9, 1, tzinfo=UTC)


def _signal() -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="daily-close-summary",
        strategy_version="daily-close-dag/v1",
        parameter_fingerprint="a" * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=NOW,
        available_at=NOW,
        candidate_id="daily-summary:2026-08-03",
        action=SignalAction.WATCH,
        reason_codes=("daily_summary",),
        evidence={"trade_date": "2026-08-03"},
        expires_at=NOW + timedelta(days=1),
        producer_commit="d" * 40,
    )


def test_producer_persists_idempotent_typed_delivery_outbox(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    producer = DailyNotificationProducer(
        signal_bus=bus,
        targets=(DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
    )

    first = producer.emit(_signal(), received_at=NOW)
    replay = producer.emit(_signal(), received_at=NOW + timedelta(seconds=1))

    assert first.signal_id == replay.signal_id
    assert first.outbox_ids == replay.outbox_ids
    assert len(first.outbox_ids) == 1
    assert bus.outbox_record(first.outbox_ids[0]) is not None


def test_conflicting_signal_id_is_quarantined_without_borrowing_old_outbox(tmp_path: Path) -> None:
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    producer = DailyNotificationProducer(
        signal_bus=bus,
        targets=(DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
    )
    original = _signal()
    first = producer.emit(original, received_at=NOW)
    conflicting = original.model_copy(update={"candidate_id": "daily-summary:tampered"})

    with pytest.raises(DailyNotificationProducerError, match="ingest rejected"):
        producer.emit(conflicting, received_at=NOW + timedelta(seconds=1))

    assert bus.outbox_records(signal_id=original.signal_id) == (
        bus.outbox_record(first.outbox_ids[0]),
    )
    assert len(bus.quarantines(original.signal_id)) == 1


def test_mismatched_accepted_receipt_cannot_route_another_signal() -> None:
    class PoisonedBus:
        route_called = False

        def ingest(self, _signal: SignalEnvelope, *, received_at: datetime) -> RouterReceipt:
            return RouterReceipt(
                signal_id="f" * 64,
                disposition=RouterDisposition.ACCEPTED,
                global_sequence=1,
                received_at=received_at,
            )

        def route(self, *_args: object, **_kwargs: object) -> tuple[object, ...]:
            self.route_called = True
            return ()

    bus = PoisonedBus()
    producer = DailyNotificationProducer(signal_bus=bus)  # type: ignore[arg-type]

    with pytest.raises(DailyNotificationProducerError, match="ingest rejected"):
        producer.emit(_signal(), received_at=NOW)

    assert not bus.route_called


def test_accepted_signal_id_cannot_borrow_a_different_stored_signal() -> None:
    requested = _signal()
    stale = requested.model_copy(update={"candidate_id": "daily-summary:stale"})

    class SignalSwappingBus:
        route_called = False

        def ingest(self, signal: SignalEnvelope, *, received_at: datetime) -> RouterReceipt:
            return RouterReceipt(
                signal_id=signal.signal_id,
                disposition=RouterDisposition.ACCEPTED,
                global_sequence=1,
                received_at=received_at,
            )

        def signal(self, signal_id: str) -> SignalEnvelope:
            assert signal_id == requested.signal_id
            return stale

        def route(self, *_args: object, **_kwargs: object) -> tuple[object, ...]:
            self.route_called = True
            return ()

    bus = SignalSwappingBus()
    producer = DailyNotificationProducer(signal_bus=bus)  # type: ignore[arg-type]

    with pytest.raises(DailyNotificationProducerError, match="payload does not match"):
        producer.emit(requested, received_at=NOW)

    assert not bus.route_called
