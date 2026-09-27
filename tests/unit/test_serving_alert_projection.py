from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant import serving_alert_projection
from rquant.alert_ack import stable_alert_id
from rquant.notification_state import NotificationStateStore
from rquant.page_control import AckAlert, AlertAcknowledgment, PageControlOutbox
from rquant.serving_alert_projection import (
    AlertAckAuthoritySnapshot,
    build_ack_source_projections,
    build_alert_read_projections,
)
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    PageProjectionSourceIntegrityError,
    SignalPageProjectionProducer,
    _ReadonlyPageControlAuditReader,
)
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    ServingSignalRecord,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from tests.unit.test_serving_page_projection_source import _signal_projection_database

NOW = datetime(2026, 9, 27, 2, 10, tzinfo=UTC)
GENERATION = "a" * 64


def _signal() -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="2.0.0",
        parameter_fingerprint="a" * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=NOW - timedelta(minutes=5),
        available_at=NOW - timedelta(minutes=4),
        candidate_id="600000.SH",
        action=SignalAction.WATCH,
        reason_codes=("volume",),
        evidence={"ratio": 1.8},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


def _projection(table_name: str, row: dict[str, object]) -> ServingProjectionInput:
    payload = ServingProjectionPayload(table_name=table_name, available_at=NOW, rows=(row,))
    return ServingProjectionInput.bind(
        payload,
        owner_dataset_id="signals",
        owner_generation_id=GENERATION,
    )


def _by_name(
    projections: tuple[ServingProjectionPayload, ...],
) -> dict[str, ServingProjectionPayload]:
    return {item.table_name: item for item in projections}


def test_fixed_serving_input_generates_only_trigger_alerts_and_unknown_count() -> None:
    signal = _signal()
    monitor = {
        "trade_date": "2026-09-27",
        "trigger_time": "2026-09-27T02:03:00+00:00",
        "ts_code": "600001.SH",
        "level": "attack_break_high",
        "trigger_price": 11.5,
        "level_price": 11.0,
        "trigger_type": "break",
        "pool": "趋势",
    }
    surge = {
        "trade_date": "2026-09-27",
        "confirmed_at": "10:04",
        "ts_code": "300001.SZ",
        "name": "测试",
        "theme": "题材",
        "price": 11.5,
        "pct_chg": 3.1,
        "cum_amount": 12_000_000.0,
        "rel_cum": 1.8,
        "room_to_limit_pct": None,
        "status": "confirmed",
    }
    source = ServingReadModelInput(
        observed_at=NOW,
        signals=(ServingSignalRecord(global_sequence=1, signal=signal),),
        projections=(
            _projection("monitor_event", monitor),
            _projection("surge_event", surge),
            _projection(
                "legacy_notification",
                {
                    "record_key": "sent-1",
                    "sent_at": "2026-09-27T02:04:00+00:00",
                    "scene_label": "价位提醒",
                    "channel_label": "PushDeer",
                    "submitted": True,
                },
            ),
        ),
    )

    projections = _by_name(build_alert_read_projections(source))
    alerts = projections["alert_event"].rows
    assert {row["alert_id"] for row in alerts} == {
        stable_alert_id("signal", signal),
        stable_alert_id("monitor_event", monitor),
        stable_alert_id("surge_event", surge),
    }
    assert {row["source"] for row in alerts} == {"signal", "monitor_event", "surge_event"}
    assert {row["state"] for row in projections["alert_source_coverage"].rows} == {"unavailable"}
    assert projections["alert_overview"].rows[0]["unacknowledged_count"] is None
    assert projections["alert_overview"].rows[0]["state"] == "ack_unavailable"


def test_duplicate_source_identity_with_distinct_time_spelling_is_not_published() -> None:
    monitor = {
        "trade_date": "2026-09-27",
        "trigger_time": "2026-09-27T02:03:00+00:00",
        "ts_code": "600001.SH",
        "level": "attack_break_high",
        "trigger_price": 11.5,
        "level_price": 11.0,
        "trigger_type": "break",
        "pool": "趋势",
    }
    payload = ServingProjectionPayload(
        table_name="monitor_event",
        available_at=NOW,
        rows=(
            monitor,
            {**monitor, "trigger_time": "2026-09-27T10:03:00+08:00"},
        ),
    )
    source = ServingReadModelInput(
        observed_at=NOW,
        projections=(
            ServingProjectionInput.bind(
                payload, owner_dataset_id="signals", owner_generation_id=GENERATION
            ),
        ),
    )
    projections = _by_name(build_alert_read_projections(source))
    assert projections["alert_overview"].rows[0]["unacknowledged_count"] is None
    monitor_coverage = next(
        row for row in projections["alert_source_coverage"].rows if row["source"] == "monitor_event"
    )
    assert monitor_coverage["state"] == "unavailable"
    assert not projections["alert_event"].rows


def test_ready_ack_rows_still_require_source_coverage_for_a_count() -> None:
    signal = _signal()
    acknowledgment = AlertAcknowledgment(
        alert_id=stable_alert_id("signal", signal),
        confirmation_id="ack-first",
        actor_id="alice",
        confirmed_at=NOW - timedelta(minutes=1),
        generation_id=GENERATION,
    )
    snapshot = AlertAckAuthoritySnapshot.create(
        activated_at=NOW - timedelta(days=1), rows=(acknowledgment,)
    )
    source = ServingReadModelInput(
        observed_at=NOW,
        signals=(ServingSignalRecord(global_sequence=1, signal=signal),),
        projections=tuple(
            ServingProjectionInput.bind(
                item, owner_dataset_id="signals", owner_generation_id=GENERATION
            )
            for item in build_ack_source_projections(snapshot, observed_at=NOW)
        ),
    )
    projections = _by_name(build_alert_read_projections(source))
    assert projections["alert_event"].rows[0]["confirmation_id"] == "ack-first"
    assert projections["alert_event"].rows[0]["eligible"] is False
    assert projections["alert_overview"].rows[0]["state"] == "source_incomplete"
    assert projections["alert_overview"].rows[0]["unacknowledged_count"] is None


def test_pre_activation_event_cannot_inherit_a_confirmation() -> None:
    signal = _signal()
    acknowledgment = AlertAcknowledgment(
        alert_id=stable_alert_id("signal", signal),
        confirmation_id="ack-first",
        actor_id="alice",
        confirmed_at=NOW - timedelta(minutes=1),
        generation_id=GENERATION,
    )
    snapshot = AlertAckAuthoritySnapshot.create(
        activated_at=NOW - timedelta(minutes=2), rows=(acknowledgment,)
    )
    source = ServingReadModelInput(
        observed_at=NOW,
        signals=(ServingSignalRecord(global_sequence=1, signal=signal),),
        projections=tuple(
            ServingProjectionInput.bind(
                item, owner_dataset_id="signals", owner_generation_id=GENERATION
            )
            for item in build_ack_source_projections(snapshot, observed_at=NOW)
        ),
    )
    projections = _by_name(build_alert_read_projections(source))
    assert projections["alert_event"].rows[0]["confirmation_id"] is None


def test_ack_digest_mismatch_refuses_derived_serving_projection() -> None:
    snapshot = AlertAckAuthoritySnapshot.create(activated_at=NOW - timedelta(days=1), rows=())
    state, acknowledgment = build_ack_source_projections(snapshot, observed_at=NOW)
    corrupted = ServingProjectionPayload(
        table_name="alert_ack_state",
        available_at=state.available_at,
        rows=({**state.rows[0], "rows_sha256": "0" * 64},),
    )
    source = ServingReadModelInput(
        observed_at=NOW,
        projections=tuple(
            ServingProjectionInput.bind(
                item, owner_dataset_id="signals", owner_generation_id=GENERATION
            )
            for item in (corrupted, acknowledgment)
        ),
    )
    with pytest.raises(ValueError, match="digest|count"):
        build_alert_read_projections(source)


def test_alert_row_byte_bound_degrades_the_source_without_a_false_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal = _signal()
    source = ServingReadModelInput(
        observed_at=NOW,
        signals=(ServingSignalRecord(global_sequence=1, signal=signal),),
    )
    monkeypatch.setattr(serving_alert_projection, "_MAX_ALERT_EVENT_BYTES", 1, raising=False)
    projections = _by_name(build_alert_read_projections(source))
    assert projections["alert_event"].rows == ()
    signal_coverage = next(
        row for row in projections["alert_source_coverage"].rows if row["source"] == "signal"
    )
    assert signal_coverage["reason"] == "event_bound_exceeded"
    assert projections["alert_overview"].rows[0]["unacknowledged_count"] is None


def test_page_control_snapshot_reads_activation_and_unique_rows_together(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_alert_ack(datetime.now(UTC) - timedelta(days=1))
    command = AckAlert(
        command_id="ack-first",
        requested_at=datetime.now(UTC),
        generation_id=GENERATION,
        alert_id="b" * 64,
        actor_id="alice",
    )
    outbox.enqueue_verified_ack(command)
    claim = outbox.claim_records(limit=1, owner_id="test-consumer")[0]
    outbox.complete_ack(claim)
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with pytest.raises(RuntimeError, match="snapshot"):
        reader.alert_ack_snapshot()
    with reader.snapshot():
        snapshot = reader.alert_ack_snapshot()
    assert snapshot is not None
    assert snapshot.activated_at is not None
    assert snapshot.row_count == 1
    assert snapshot.rows[0].alert_id == command.alert_id
    assert snapshot.rows[0].confirmation_id == command.command_id
    assert len(snapshot.rows_sha256) == 64


def test_old_page_control_without_ack_tables_is_explicitly_unavailable(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "old.sqlite3")
    with sqlite3.connect(outbox.path) as connection:
        connection.execute("DROP TABLE page_control_alert_ack")
        connection.execute("DROP TABLE page_control_alert_activation")
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot():
        assert reader.alert_ack_snapshot() is None


def test_signal_page_source_carries_optional_ack_snapshot_in_one_generation(tmp_path: Path) -> None:
    database = tmp_path / "replica.duckdb"
    _signal_projection_database(database)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    source = DuckDBSignalPageProjectionSource(database, page_control_outbox=outbox)
    snapshot = source(NOW)
    projections = _by_name(snapshot.projections)
    assert projections["alert_ack_state"].rows[0]["state"] == "unavailable"
    assert "alert_ack" not in projections
    outbox.activate_alert_ack(NOW - timedelta(days=1))
    activated = _by_name(source(NOW).projections)
    assert activated["alert_ack_state"].rows[0]["state"] == "ready"
    assert activated["alert_ack_state"].rows[0]["row_count"] == 0
    assert activated["alert_ack"].rows == ()


def test_page_source_failure_revokes_previous_confirmation_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "replica.duckdb"
    _signal_projection_database(database)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    observed = datetime.now(UTC) + timedelta(seconds=2)
    outbox.activate_alert_ack(observed - timedelta(days=1))
    command = AckAlert(
        command_id="ack-first",
        requested_at=observed - timedelta(seconds=1),
        generation_id=GENERATION,
        alert_id="b" * 64,
        actor_id="alice",
    )
    outbox.enqueue_verified_ack(command)
    outbox.complete_ack(outbox.claim_records(limit=1, owner_id="test-consumer")[0])
    source = DuckDBSignalPageProjectionSource(database, page_control_outbox=outbox)
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(source=source, store=store)
    producer.publish(observed)
    initial = _by_name(
        store.serving_snapshot(observed_at=observed, history_limit=1).payload.projections
    )
    assert initial["alert_ack"].rows[0]["confirmation_id"] == command.command_id

    def fail_read(_observed: datetime) -> None:
        raise PageProjectionSourceIntegrityError("source unavailable")

    monkeypatch.setattr(source, "_build_snapshot", fail_read)
    later = observed + timedelta(seconds=3)
    producer.publish(later)
    fallback = _by_name(
        store.serving_snapshot(observed_at=later, history_limit=1).payload.projections
    )
    assert fallback["alert_ack_state"].rows[0]["state"] == "unavailable"
    assert "alert_ack" not in fallback
