from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from rquant.notification_state import NotificationStateStore
from rquant.runtime_builder_signal import _signal_source_result
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_alert_projection import (
    AlertAckAuthoritySnapshot,
    build_ack_source_projections,
    build_alert_read_projections,
)
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    ServingSignalRecord,
    build_serving_read_models,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalEnvelope
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    publish_signal_bus_prefix,
)
from tests.unit.test_notification_state import NOW, _published_source, _signal


def _snapshot(
    tmp_path: Path,
    *,
    signal_count: int = 1,
    history_limit: int = 10,
    observe_source: bool = True,
    as_of: datetime = NOW,
):
    if signal_count:
        signals = tuple(_signal(str(index + 2), f"{index:06d}.SZ") for index in range(signal_count))
        source = _published_source(tmp_path, signals=signals)
    else:
        bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
        root = tmp_path / "signal-spool"
        publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
        source = ReadonlySignalRouteSpool(root)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0, through_sequence=descriptor.high_watermark, limit=10
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.replicate(
        descriptor,
        records,
        observed_at=as_of,
        source_inspected_at=as_of if observe_source else None,
    )
    return store, store.serving_snapshot(observed_at=as_of, history_limit=history_limit)


def _alert_projections(
    snapshot, *, observed_at: datetime = NOW, activated_at: datetime | None = None
):
    source_result = _signal_source_result(snapshot, published_at=observed_at)
    receipts = tuple(
        projection
        for projection in source_result.payload.projections
        if projection.table_name == "signal_observed_prefix"
    )
    acknowledgment = AlertAckAuthoritySnapshot.create(
        activated_at=activated_at or observed_at - timedelta(days=1), rows=()
    )
    bound = tuple(
        ServingProjectionInput.bind(
            projection, owner_dataset_id="signals", owner_generation_id=source_result.generation_id
        )
        for projection in (
            *receipts,
            *build_ack_source_projections(acknowledgment, observed_at=observed_at),
        )
    )
    return build_alert_read_projections(
        ServingReadModelInput(
            observed_at=observed_at,
            signals=snapshot.payload.signals,
            projections=bound,
        ),
        signal_generation_id=source_result.generation_id,
    )


def _signal_coverage(projections):
    rows = next(
        projection.rows
        for projection in projections
        if projection.table_name == "alert_source_coverage"
    )
    return {row["source"]: row for row in rows}


def test_verified_signal_prefix_is_only_partial_without_upstream_completion(tmp_path: Path) -> None:
    _store, snapshot = _snapshot(tmp_path)

    assert snapshot.signal_observed_prefix is not None
    assert snapshot.signal_observed_prefix.source_high_watermark == 1
    coverage = _signal_coverage(_alert_projections(snapshot))
    assert coverage["signal"]["state"] == "unavailable"
    assert coverage["signal"]["row_count"] == 1
    assert coverage["monitor_event"]["state"] == "unavailable"
    assert coverage["surge_event"]["state"] == "unavailable"
    overview = next(
        item for item in _alert_projections(snapshot) if item.table_name == "alert_overview"
    )
    assert overview.rows[0]["unacknowledged_count"] is None
    result = _signal_source_result(snapshot, published_at=NOW)
    receipt_projection = next(
        item for item in result.payload.projections if item.table_name == "signal_observed_prefix"
    )
    physical = build_serving_read_models(
        ServingReadModelInput(
            observed_at=NOW,
            signals=snapshot.payload.signals,
            projections=(
                ServingProjectionInput.bind(
                    receipt_projection,
                    owner_dataset_id="signals",
                    owner_generation_id=result.generation_id,
                ),
            ),
        )
    )["signal_observed_prefix"]
    assert physical["source_high_watermark"].tolist() == [1]


def test_missing_observation_never_proves_zero_events(tmp_path: Path) -> None:
    _store, snapshot = _snapshot(tmp_path, signal_count=0, observe_source=False)
    assert snapshot.signal_observed_prefix is None
    assert _signal_coverage(_alert_projections(snapshot))["signal"]["state"] == "unavailable"


def test_new_empty_spool_cannot_prove_30_days_of_zero_events(tmp_path: Path) -> None:
    _store, snapshot = _snapshot(tmp_path, signal_count=0)
    assert snapshot.signal_observed_prefix is not None
    assert snapshot.signal_observed_prefix.source_high_watermark == 0
    assert snapshot.signal_observed_prefix.window_row_count == 0
    coverage = _signal_coverage(_alert_projections(snapshot))
    assert coverage["signal"]["state"] == "unavailable"
    assert coverage["signal"]["row_count"] == 0


def test_spool_behind_bus_cannot_prove_source_complete(tmp_path: Path) -> None:
    _published_source(
        tmp_path,
        signals=(_signal("2", "000001.SZ"), _signal("3", "000002.SZ")),
    )
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    lag_root = tmp_path / "lagging-signal-spool"
    summary = publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(lag_root), limit=1)
    assert summary.source_high_watermark == 2
    assert summary.published_high_watermark == 1
    source = ReadonlySignalRouteSpool(lag_root)
    descriptor = source.source_descriptor()
    store = NotificationStateStore(tmp_path / "lagging-notification-state.sqlite3")
    store.replicate(
        descriptor,
        source.routed_after_global_sequence(
            after_sequence=0, through_sequence=descriptor.high_watermark, limit=10
        ),
        observed_at=NOW,
        source_inspected_at=NOW,
    )
    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)
    assert _signal_coverage(_alert_projections(snapshot))["signal"]["state"] == "unavailable"


def test_history_truncation_withholds_signal_receipt(tmp_path: Path) -> None:
    _store, snapshot = _snapshot(tmp_path, signal_count=2, history_limit=1)
    assert snapshot.truncated
    assert snapshot.signal_observed_prefix is None
    assert _signal_coverage(_alert_projections(snapshot))["signal"]["state"] == "unavailable"


def test_sequence_gap_and_missing_cursor_withhold_source_proof(tmp_path: Path) -> None:
    store, _snapshot_before = _snapshot(tmp_path, signal_count=2)
    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND tbl_name IN ('signal_envelope', 'notification_source_route_receipt')"
        ).fetchall():
            connection.execute(f'DROP TRIGGER "{name}"')
        connection.execute(
            "DELETE FROM notification_source_route_receipt WHERE global_sequence = 1"
        )
        connection.execute("DELETE FROM signal_envelope WHERE global_sequence = 1")
    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)
    assert snapshot.signal_observed_prefix is None

    with sqlite3.connect(store.path) as connection:
        connection.execute("DELETE FROM notification_replication_source")
    assert store.serving_snapshot(observed_at=NOW, history_limit=10).signal_observed_prefix is None


def test_source_generation_change_withholds_proof(tmp_path: Path) -> None:
    store, _snapshot_before = _snapshot(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE notification_source_observation SET source_generation_id = ?",
            ("f" * 64,),
        )
    assert store.serving_snapshot(observed_at=NOW, history_limit=10).signal_observed_prefix is None


def test_serving_refuses_a_receipt_with_changed_envelope_digest_or_owner(tmp_path: Path) -> None:
    _store, snapshot = _snapshot(tmp_path)
    result = _signal_source_result(snapshot, published_at=NOW)
    receipt = next(
        item for item in result.payload.projections if item.table_name == "signal_observed_prefix"
    )
    ack = AlertAckAuthoritySnapshot.create(activated_at=NOW - timedelta(days=1), rows=())
    for damaged, owner in (
        (True, result.generation_id),
        (False, "f" * 64),
    ):
        row = dict(receipt.rows[0])
        if damaged:
            row["window_rows_sha256"] = "0" * 64
        proof = ServingProjectionPayload(
            table_name="signal_observed_prefix",
            available_at=NOW,
            rows=(row,),
        )
        projections = (
            ServingProjectionInput.bind(
                proof,
                owner_dataset_id="signals",
                owner_generation_id=owner,
            ),
            *(
                ServingProjectionInput.bind(
                    item, owner_dataset_id="signals", owner_generation_id=result.generation_id
                )
                for item in build_ack_source_projections(ack, observed_at=NOW)
            ),
        )
        derived = build_alert_read_projections(
            ServingReadModelInput(
                observed_at=NOW,
                signals=snapshot.payload.signals,
                projections=projections,
            ),
            signal_generation_id=result.generation_id,
        )
        assert _signal_coverage(derived)["signal"]["state"] == "unavailable"


def test_high_watermark_mismatch_withholds_proof(tmp_path: Path) -> None:
    store, _snapshot_before = _snapshot(tmp_path)
    with sqlite3.connect(store.path) as connection:
        connection.execute("UPDATE notification_source_observation SET source_high_watermark = 0")
    assert store.serving_snapshot(observed_at=NOW, history_limit=10).signal_observed_prefix is None


def test_idle_observation_advances_only_after_bounded_interval(tmp_path: Path) -> None:
    store, _snapshot_before = _snapshot(tmp_path, signal_count=0)
    source = ReadonlySignalRouteSpool(tmp_path / "signal-spool")
    descriptor = source.source_descriptor()
    with sqlite3.connect(store.path) as connection:
        before = connection.execute(
            "SELECT revision FROM notification_state_revision WHERE singleton = 1"
        ).fetchone()[0]
    store.replicate(
        descriptor,
        (),
        observed_at=NOW + timedelta(seconds=2),
        source_inspected_at=NOW + timedelta(seconds=2),
    )
    with sqlite3.connect(store.path) as connection:
        idle = connection.execute(
            "SELECT revision FROM notification_state_revision WHERE singleton = 1"
        ).fetchone()[0]
    store.replicate(
        descriptor,
        (),
        observed_at=NOW + timedelta(seconds=61),
        source_inspected_at=NOW + timedelta(seconds=61),
    )
    with sqlite3.connect(store.path) as connection:
        refreshed = connection.execute(
            "SELECT revision FROM notification_state_revision WHERE singleton = 1"
        ).fetchone()[0]
    assert idle == before
    assert refreshed == before + 1


def test_window_excludes_signals_before_the_shanghai_30_day_start(tmp_path: Path) -> None:
    old_at = NOW - timedelta(days=31)
    old = SignalEnvelope.model_validate(
        {
            **_signal("2", "000001.SZ").model_dump(mode="python", exclude={"signal_id"}),
            "event_time": old_at,
            "available_at": old_at + timedelta(minutes=1),
            "expires_at": old_at + timedelta(minutes=10),
        }
    )
    recent = _signal("3", "000002.SZ")
    source = _published_source(tmp_path, signals=(old, recent))
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0, through_sequence=descriptor.high_watermark, limit=10
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.replicate(descriptor, records, observed_at=NOW, source_inspected_at=NOW)
    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)

    assert snapshot.signal_observed_prefix is not None
    assert snapshot.signal_observed_prefix.prefix_row_count == 2
    assert snapshot.signal_observed_prefix.window_row_count == 1
    assert _signal_coverage(_alert_projections(snapshot))["signal"]["row_count"] == 1


def test_receipt_window_uses_source_observation_not_later_snapshot_clock(tmp_path: Path) -> None:
    store, _snapshot_before = _snapshot(tmp_path)
    later = NOW + timedelta(minutes=2)
    snapshot = store.serving_snapshot(observed_at=later, history_limit=10)
    assert snapshot.signal_observed_prefix is not None
    assert snapshot.signal_observed_prefix.window_end == NOW
    assert snapshot.signal_observed_prefix.window_start == datetime(
        2026, 7, 2, tzinfo=UTC
    ) - timedelta(hours=8)


def test_coverage_digest_excludes_events_after_observed_prefix_cutoff(tmp_path: Path) -> None:
    _store, snapshot = _snapshot(tmp_path)
    late_at = NOW + timedelta(minutes=1)
    late = SignalEnvelope.model_validate(
        {
            **_signal("3", "000002.SZ").model_dump(mode="python", exclude={"signal_id"}),
            "event_time": late_at,
            "available_at": late_at,
            "expires_at": late_at + timedelta(minutes=10),
        }
    )
    result = _signal_source_result(snapshot, published_at=NOW)
    receipt = next(
        item for item in result.payload.projections if item.table_name == "signal_observed_prefix"
    )
    later = NOW + timedelta(minutes=2)
    ack = AlertAckAuthoritySnapshot.create(activated_at=NOW - timedelta(days=1), rows=())
    projections = tuple(
        ServingProjectionInput.bind(
            item, owner_dataset_id="signals", owner_generation_id=result.generation_id
        )
        for item in (receipt, *build_ack_source_projections(ack, observed_at=later))
    )
    derived = build_alert_read_projections(
        ServingReadModelInput(
            observed_at=later,
            signals=(
                *snapshot.payload.signals,
                ServingSignalRecord(global_sequence=2, signal=late),
            ),
            projections=projections,
        ),
        signal_generation_id=result.generation_id,
    )
    timeline = next(item.rows for item in derived if item.table_name == "alert_event")
    coverage = _signal_coverage(derived)["signal"]
    within_window = tuple(row for row in timeline if row["occurred_at"] <= NOW.isoformat())
    assert len(timeline) == 2
    assert coverage["state"] == "unavailable"
    assert coverage["row_count"] == 1
    assert coverage["row_digest"] == canonical_sha256(
        {"contract": "alert-observed-rows/v1", "source": "signal", "rows": within_window}
    )


def test_activation_after_source_observation_cannot_be_complete(tmp_path: Path) -> None:
    _store, snapshot = _snapshot(tmp_path)
    projections = _alert_projections(
        snapshot,
        observed_at=NOW + timedelta(seconds=2),
        activated_at=NOW + timedelta(seconds=1),
    )
    assert _signal_coverage(projections)["signal"]["state"] == "unavailable"
