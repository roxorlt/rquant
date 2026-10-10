import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from rquant.notification_state import NotificationStateStore
from rquant.price_alert_route import install_price_alert_history
from rquant.signal_route_spool import (
    ReadonlyNotificationEventRouteSpool,
    SignalRouteSpool,
    publish_mixed_notification_bus_prefix,
)
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_route_spool import legacy_route, mixed_fixture


def replication_fixture(tmp_path: Path):
    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    spool = SignalRouteSpool(tmp_path / "spool")
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    reader = ReadonlyNotificationEventRouteSpool(tmp_path / "spool")
    source = reader.source_descriptor()
    records = reader.routed_after_global_sequence(
        after_sequence=0, through_sequence=3, limit=100, observed_at=AT
    )
    state = NotificationStateStore(tmp_path / "notify.sqlite3")
    install_price_alert_history(state)
    return producer, bus, spool, reader, state, source, records


def test_mixed_uses_single_original_cursor_outbox_and_replays_without_price_cap(
    tmp_path: Path,
) -> None:
    producer, bus, spool, reader, state, source, records = replication_fixture(tmp_path)
    copied = state.replicate_mixed_notification_events(
        source, records, observed_at=AT, source_inspected_at=AT
    )
    assert copied.replicated_count == 3
    assert state.replication_cursor().last_global_sequence == 3
    assert state.notification_event(records[1].event_id).event == records[1].event
    assert len(state.outbox_records()) == 3
    assert (
        state.replicate_mixed_notification_events(
            source, records, observed_at=AT, source_inspected_at=AT
        ).replicated_count
        == 0
    )
    with pytest.raises((TypeError, ValueError)):
        state.replicate(source, records, observed_at=AT)
    _four = legacy_route(bus, "d", 3)
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    state.replicate_mixed_notification_events(
        reader.source_descriptor(),
        reader.routed_after_global_sequence(
            after_sequence=3, through_sequence=4, limit=100, observed_at=AT
        ),
        observed_at=AT,
        source_inspected_at=AT,
    )
    assert state.replication_cursor().last_global_sequence == 4
    claimed = state.claim_due("old", now=AT, lease_for=timedelta(seconds=10), limit=10)
    assert len(claimed) == 3
    assert all(item.signal_id != records[1].event_id for item in claimed)
    producer.close()


@pytest.mark.parametrize(
    "failure", ["missing_price", "changed_source", "future", "gap", "mid_write"]
)
def test_bad_mixed_batch_or_mid_write_leaves_original_cursor_and_all_rows_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    producer, bus, spool, reader, state, source, records = replication_fixture(tmp_path)
    before = state.replication_cursor()
    if failure in ("missing_price", "gap"):
        records = (records[0], records[2])
    elif failure == "changed_source":
        source = source.model_copy(update={"generation_id": "a" * 64})
    elif failure == "future":
        records = (
            records[0],
            records[1].model_copy(update={"received_at": AT + timedelta(seconds=1)}),
            records[2],
        )
    else:
        monkeypatch.setattr(
            state,
            "_after_replicated_signal",
            lambda: (_ for _ in ()).throw(OSError("replicate failure")),
        )
    with pytest.raises((TypeError, ValueError, RuntimeError, OSError)):
        state.replicate_mixed_notification_events(
            source, records, observed_at=AT, source_inspected_at=AT
        )
    assert state.replication_cursor() == before
    assert state.outbox_records() == ()
    with sqlite3.connect(state.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM signal_envelope").fetchone()[0] == 0
        assert (
            connection.execute("SELECT COUNT(*) FROM price_alert_route_receipt").fetchone()[0] == 0
        )
    producer.close()


def test_idle_mixed_reads_preserve_original_observation_write_throttle(tmp_path: Path) -> None:
    producer, bus, spool, reader, state, source, records = replication_fixture(tmp_path)
    state.replicate_mixed_notification_events(
        source, records, observed_at=AT, source_inspected_at=AT
    )
    with sqlite3.connect(state.path) as connection:
        original = connection.execute(
            "SELECT revision FROM notification_state_revision WHERE singleton=1"
        ).fetchone()[0]
    for seconds in range(1, 60, 5):
        current = AT + timedelta(seconds=seconds)
        state.replicate_mixed_notification_events(
            source, (), observed_at=current, source_inspected_at=current
        )
    with sqlite3.connect(state.path) as connection:
        assert (
            connection.execute(
                "SELECT revision FROM notification_state_revision WHERE singleton=1"
            ).fetchone()[0]
            == original
        )
    current = AT + timedelta(seconds=60)
    state.replicate_mixed_notification_events(
        source, (), observed_at=current, source_inspected_at=current
    )
    with sqlite3.connect(state.path) as connection:
        assert (
            connection.execute(
                "SELECT revision FROM notification_state_revision WHERE singleton=1"
            ).fetchone()[0]
            == original + 1
        )
    producer.close()
