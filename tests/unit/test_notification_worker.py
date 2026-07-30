from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.delivery_contracts import (
    DeliveryChannel,
    DeliveryTarget,
    OutboxStatus,
)
from rquant.notification_worker import (
    ConfirmedDeliveryFailureError,
    NotificationDelivery,
    NotificationItemOutcome,
    NotificationProvider,
    UnknownDeliveryOutcomeError,
    run_notification_batch,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope

NOW = datetime(2026, 7, 31, 2, 0, tzinfo=UTC)


def _signal(seed: str) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="2.0.0",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=NOW - timedelta(seconds=2),
        available_at=NOW,
        candidate_id=f"60000{ord(seed) % 10}.SH",
        action=SignalAction.WATCH,
        reason_codes=("same-minute-volume",),
        evidence={"ratio": 1.8},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


def _store(path: Path) -> SignalBusStore:
    return SignalBusStore(
        path,
        retry_base_delay=timedelta(seconds=5),
        retry_max_delay=timedelta(seconds=30),
        max_attempts=3,
    )


def _route(
    store: SignalBusStore,
    seed: str,
    *,
    channel: DeliveryChannel,
) -> str:
    signal = _signal(seed)
    store.ingest(signal, received_at=NOW)
    record = store.route(
        signal.signal_id,
        (DeliveryTarget(recipient_id=f"recipient-{seed}", channel=channel),),
        now=NOW,
    )[0]
    return record.outbox_id


class RecordingProvider(NotificationProvider):
    def __init__(
        self,
        outcome: str | Exception | BaseException | Callable[[NotificationDelivery], str],
    ) -> None:
        self.outcome = outcome
        self.deliveries: list[NotificationDelivery] = []

    def deliver(self, delivery: NotificationDelivery) -> str:
        self.deliveries.append(delivery)
        if callable(self.outcome):
            return self.outcome(delivery)
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


def _clock(*values: datetime) -> Callable[[], datetime]:
    iterator = iter(values)
    return lambda: next(iterator)


def test_worker_maps_both_channels_and_persists_successes(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    first_id = _route(store, "a", channel=DeliveryChannel.PUSHDEER)
    second_id = _route(store, "e", channel=DeliveryChannel.PUSHPLUS)
    pushdeer = RecordingProvider("pushdeer:receipt-1")
    pushplus = RecordingProvider("pushplus:receipt-1")

    summary = run_notification_batch(
        store,
        {
            DeliveryChannel.PUSHDEER: pushdeer,
            DeliveryChannel.PUSHPLUS: pushplus,
        },
        worker_id="notifier-1",
        now=NOW,
        lease_for=timedelta(seconds=30),
        limit=10,
        clock=_clock(
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=3),
        ),
    )

    assert summary.claimed_count == 2
    assert summary.succeeded_count == 2
    assert summary.failed_count == 0
    assert summary.unknown_count == 0
    assert summary.finished_at == NOW + timedelta(seconds=3)
    assert {item.outbox_id for item in summary.items} == {first_id, second_id}
    assert all(item.outcome is NotificationItemOutcome.SUCCEEDED for item in summary.items)
    assert pushdeer.deliveries[0].record.target.channel is DeliveryChannel.PUSHDEER
    assert pushplus.deliveries[0].record.target.channel is DeliveryChannel.PUSHPLUS
    assert pushdeer.deliveries[0].signal.signal_id == pushdeer.deliveries[0].record.signal_id
    assert pushdeer.deliveries[0].deadline == NOW + timedelta(seconds=30)
    assert store.outbox_record(first_id).status is OutboxStatus.SUCCEEDED  # type: ignore[union-attr]
    assert store.outbox_record(second_id).status is OutboxStatus.SUCCEEDED  # type: ignore[union-attr]


def test_known_failure_is_isolated_and_completed_as_failure(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    failed_id = _route(store, "a", channel=DeliveryChannel.PUSHDEER)
    succeeded_id = _route(store, "e", channel=DeliveryChannel.PUSHPLUS)
    pushdeer = RecordingProvider(
        ConfirmedDeliveryFailureError("provider rejected request before acceptance")
    )
    pushplus = RecordingProvider("pushplus:ok")

    summary = run_notification_batch(
        store,
        {
            DeliveryChannel.PUSHDEER: pushdeer,
            DeliveryChannel.PUSHPLUS: pushplus,
        },
        worker_id="notifier-1",
        now=NOW,
        lease_for=timedelta(seconds=30),
        limit=10,
        clock=_clock(
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=3),
        ),
    )

    assert summary.succeeded_count == 1
    assert summary.failed_count == 1
    assert summary.unknown_count == 0
    assert store.outbox_record(failed_id).status is OutboxStatus.RETRY  # type: ignore[union-attr]
    assert store.outbox_record(succeeded_id).status is OutboxStatus.SUCCEEDED  # type: ignore[union-attr]
    failed = next(item for item in summary.items if item.outbox_id == failed_id)
    assert failed.outcome is NotificationItemOutcome.FAILED
    assert failed.error == (
        "ConfirmedDeliveryFailureError: provider rejected request before acceptance"
    )


@pytest.mark.parametrize(
    "outcome",
    [TimeoutError("response timed out"), ConnectionResetError("connection reset"), ""],
)
def test_ambiguous_provider_failure_or_empty_receipt_never_auto_retries(
    tmp_path: Path,
    outcome: str | BaseException,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    outbox_id = _route(store, "a", channel=DeliveryChannel.PUSHDEER)

    summary = run_notification_batch(
        store,
        {DeliveryChannel.PUSHDEER: RecordingProvider(outcome)},
        worker_id="notifier-1",
        now=NOW,
        lease_for=timedelta(seconds=2),
        limit=1,
        clock=_clock(NOW, NOW + timedelta(seconds=1), NOW + timedelta(seconds=1)),
    )

    assert summary.unknown_count == 1
    assert store.outbox_record(outbox_id).status is OutboxStatus.LEASED  # type: ignore[union-attr]
    assert len(store.unknown_deliveries(outbox_id)) == 1
    store.recover_expired_leases(now=NOW + timedelta(seconds=2))
    assert store.outbox_record(outbox_id).status is OutboxStatus.DEAD_LETTER  # type: ignore[union-attr]


def test_unknown_outcome_keeps_lease_for_dead_letter_recovery(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    outbox_id = _route(store, "a", channel=DeliveryChannel.PUSHDEER)
    provider = RecordingProvider(UnknownDeliveryOutcomeError("request timed out after send"))

    summary = run_notification_batch(
        store,
        {DeliveryChannel.PUSHDEER: provider},
        worker_id="notifier-1",
        now=NOW,
        lease_for=timedelta(seconds=10),
        limit=1,
        clock=_clock(NOW, NOW + timedelta(seconds=1), NOW + timedelta(seconds=2)),
    )

    assert summary.unknown_count == 1
    assert summary.items[0].outcome is NotificationItemOutcome.UNKNOWN
    assert store.outbox_record(outbox_id).status is OutboxStatus.LEASED  # type: ignore[union-attr]
    assert store.attempts(outbox_id) == ()

    recovered = store.recover_expired_leases(now=NOW + timedelta(seconds=10))

    assert recovered[0].status is OutboxStatus.DEAD_LETTER
    assert (
        run_notification_batch(
            store,
            {DeliveryChannel.PUSHDEER: provider},
            worker_id="notifier-2",
            now=NOW + timedelta(seconds=11),
            lease_for=timedelta(seconds=10),
            limit=1,
            clock=_clock(NOW + timedelta(seconds=12)),
        ).claimed_count
        == 0
    )
    assert len(provider.deliveries) == 1


def test_process_crash_propagates_and_leaves_active_lease(tmp_path: Path) -> None:
    class SimulatedCrash(BaseException):
        pass

    store = _store(tmp_path / "signal-bus.sqlite3")
    outbox_id = _route(store, "a", channel=DeliveryChannel.PUSHDEER)
    provider = RecordingProvider(SimulatedCrash())

    with pytest.raises(SimulatedCrash):
        run_notification_batch(
            store,
            {DeliveryChannel.PUSHDEER: provider},
            worker_id="notifier-1",
            now=NOW,
            lease_for=timedelta(seconds=10),
            limit=1,
            clock=_clock(NOW + timedelta(seconds=1)),
        )

    assert store.outbox_record(outbox_id).status is OutboxStatus.LEASED  # type: ignore[union-attr]
    assert store.attempts(outbox_id) == ()


def test_missing_provider_is_a_known_failure_and_batch_limit_is_respected(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    first_id = _route(store, "a", channel=DeliveryChannel.PUSHDEER)
    _route(store, "e", channel=DeliveryChannel.PUSHPLUS)

    summary = run_notification_batch(
        store,
        {},
        worker_id="notifier-1",
        now=NOW,
        lease_for=timedelta(seconds=30),
        limit=1,
        clock=_clock(NOW, NOW + timedelta(seconds=1), NOW + timedelta(seconds=2)),
    )

    assert summary.claimed_count == 1
    assert summary.failed_count == 1
    assert summary.items[0].outbox_id == first_id
    assert "no provider configured for pushdeer" in (summary.items[0].error or "")
    assert store.outbox_record(first_id).status is OutboxStatus.RETRY  # type: ignore[union-attr]
    assert len(store.outbox_records(status=OutboxStatus.PENDING)) == 1


def test_worker_does_not_call_provider_after_batch_item_lease_expires(tmp_path: Path) -> None:
    store = _store(tmp_path / "signal-bus.sqlite3")
    _route(store, "a", channel=DeliveryChannel.PUSHDEER)
    second_id = _route(store, "e", channel=DeliveryChannel.PUSHPLUS)
    pushdeer = RecordingProvider("pushdeer:ok")
    pushplus = RecordingProvider("pushplus:must-not-run")

    summary = run_notification_batch(
        store,
        {
            DeliveryChannel.PUSHDEER: pushdeer,
            DeliveryChannel.PUSHPLUS: pushplus,
        },
        worker_id="notifier-1",
        now=NOW,
        lease_for=timedelta(seconds=2),
        limit=2,
        clock=_clock(
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=2),
        ),
    )

    assert summary.succeeded_count == 1
    assert summary.not_attempted_count == 1
    assert pushplus.deliveries == []
    second = store.outbox_record(second_id)
    assert second is not None and second.status is OutboxStatus.RETRY
    assert second.attempt_count == 0
