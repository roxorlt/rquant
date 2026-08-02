from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.notification_state import (
    NotificationReplicationError,
    NotificationStateStore,
)
from rquant.signal_bus import SignalBusRoutedRecord, SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    publish_signal_bus_prefix,
)
from rquant.signal_router_runtime import (
    RouteSourceDescriptor,
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteCursorStore,
    SourceSnapshot,
    route_runner_signals,
)
from rquant.strategy_runner import RunnerSignalRecord

NOW = datetime(2026, 7, 31, 2, 30, tzinfo=UTC)
POLICY = "a" * 64


def _signal(seed: str = "b", candidate_id: str = "600000.SH") -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=NOW - timedelta(seconds=1),
        available_at=NOW,
        candidate_id=candidate_id,
        action=SignalAction.WATCH,
        reason_codes=("notification-state-test",),
        evidence={},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="e" * 40,
    )


class _Source:
    def __init__(self, signals: tuple[SignalEnvelope, ...]) -> None:
        self.signals = signals

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        records = tuple(
            RunnerSignalRecord(sequence=index, signal=signal)
            for index, signal in enumerate(self.signals, start=1)
            if index > after_sequence
        )
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(
                descriptor=RouteSourceDescriptor(
                    source_id="n-shape-v1",
                    generation_id="f" * 64,
                    strategy_spec_fingerprint="1" * 64,
                    first_sequence=1,
                    high_watermark=len(self.signals),
                )
            ),
            after_sequence=after_sequence,
            limit=limit,
            records=records[:limit],
        )


def _published_source(
    tmp_path: Path,
    *,
    signals: tuple[SignalEnvelope, ...] | None = None,
) -> ReadonlySignalRouteSpool:
    signals = signals or (_signal(),)
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    route_runner_signals(
        source_id="n-shape-v1",
        source=_Source(signals),
        bus=bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "cursor.sqlite3",
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=NOW,
        target_resolver=lambda _signal: RoutingDecision.route(
            routing_policy_fingerprint=POLICY,
            targets=(
                DeliveryTarget(
                    recipient_id="admin",
                    channel=DeliveryChannel.PUSHDEER,
                ),
            ),
        ),
        limit=len(signals),
    )
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    return ReadonlySignalRouteSpool(root)


def test_notification_state_replicates_routed_prefix_into_owned_outbox(
    tmp_path: Path,
) -> None:
    source = _published_source(tmp_path)
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )

    first = store.replicate(descriptor, records, observed_at=NOW)
    replay = store.replicate(descriptor, (), observed_at=NOW + timedelta(seconds=1))

    assert first.started_after_sequence == 0
    assert first.ended_at_sequence == 1
    assert first.replicated_count == 1
    assert replay.replicated_count == 0
    assert replay.ended_at_sequence == 1
    assert store.signal(1) == _signal()
    assert store.outbox_records()[0].target == DeliveryTarget(
        recipient_id="admin",
        channel=DeliveryChannel.PUSHDEER,
    )


def test_notification_state_rolls_back_signal_outbox_and_cursor_together(
    tmp_path: Path,
) -> None:
    source = _published_source(tmp_path)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=1,
        limit=10,
    )

    class FaultyStore(NotificationStateStore):
        def _after_replicated_signal(self) -> None:
            raise RuntimeError("injected replication failure")

    store = FaultyStore(tmp_path / "notification-state.sqlite3")
    with pytest.raises(RuntimeError, match="injected"):
        store.replicate(descriptor, records, observed_at=NOW)

    assert store.replication_cursor().last_global_sequence == 0
    assert store.outbox_records() == ()
    assert store.signal(1) is None
    assert store.serving_snapshot(observed_at=NOW, history_limit=10).payload.routes == ()


def test_notification_state_replay_preserves_exact_immutable_route_receipt(
    tmp_path: Path,
) -> None:
    source = _published_source(tmp_path)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")

    first = store.replicate(descriptor, records, observed_at=NOW)
    replay = store.replicate(
        descriptor,
        records,
        observed_at=NOW + timedelta(seconds=1),
    )

    assert first.replicated_count == 1
    assert replay.replicated_count == 0
    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)
    assert snapshot.payload.routes == (records[0].receipt,)

    conflicting = SignalBusRoutedRecord.model_validate(
        records[0].model_copy(
            update={
                "receipt": records[0].receipt.model_copy(update={"decision_fingerprint": "9" * 64})
            }
        )
    )
    with pytest.raises(NotificationReplicationError, match="route receipt conflicts"):
        store.replicate(
            descriptor,
            (conflicting,),
            observed_at=NOW + timedelta(seconds=2),
        )

    with sqlite3.connect(store.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                """
                UPDATE notification_source_route_receipt
                SET receipt_json = '{}'
                WHERE global_sequence = 1
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "DELETE FROM notification_source_route_receipt WHERE global_sequence = 1"
            )


def test_notification_state_serving_snapshot_is_atomic_closed_and_explicitly_truncated(
    tmp_path: Path,
) -> None:
    signals = (
        _signal("2", "600000.SH"),
        _signal("3", "000001.SZ"),
    )
    source = _published_source(tmp_path, signals=signals)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.replicate(descriptor, records, observed_at=NOW)

    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=1)

    assert snapshot.visible_signal_count == 2
    assert snapshot.returned_signal_count == 1
    assert snapshot.omitted_signal_count == 1
    assert snapshot.truncated is True
    assert tuple(record.global_sequence for record in snapshot.payload.signals) == (2,)
    assert tuple(route.signal_id for route in snapshot.payload.routes) == (signals[1].signal_id,)
    assert tuple(delivery.signal_id for delivery in snapshot.payload.deliveries) == (
        signals[1].signal_id,
    )
    assert all(record.signal.available_at <= NOW for record in snapshot.payload.signals)
    assert all(route.routed_at <= NOW for route in snapshot.payload.routes)
    assert all(delivery.updated_at <= NOW for delivery in snapshot.payload.deliveries)


def test_notification_state_serving_snapshot_excludes_future_mutable_delivery_state(
    tmp_path: Path,
) -> None:
    source = _published_source(tmp_path)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.replicate(descriptor, records, observed_at=NOW)
    store.claim_due(
        worker_id="future-worker",
        now=NOW + timedelta(seconds=1),
        lease_for=timedelta(seconds=30),
        limit=1,
    )

    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)

    assert len(snapshot.payload.signals) == 1
    assert len(snapshot.payload.routes) == 1
    assert snapshot.payload.deliveries == ()
