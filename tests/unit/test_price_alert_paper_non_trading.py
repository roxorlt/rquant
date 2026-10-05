import sqlite3
from pathlib import Path

import pytest

from rquant.paper_signal_consumer import (
    PaperSignalConsumerStateStore,
    consume_notification_events_to_paper,
)
from rquant.signal_route_spool import (
    ReadonlyNotificationEventRouteSpool,
    SignalRouteSpool,
    publish_mixed_notification_bus_prefix,
)
from tests.unit.test_paper_signal_consumer import _queue
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_route_spool import legacy_route, mixed_fixture


@pytest.mark.parametrize("source_kind", ["bus", "spool"])
def test_price_is_non_trading_in_one_original_cursor_and_off_strategy_continues(
    tmp_path: Path, source_kind: str
) -> None:
    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    spool = SignalRouteSpool(tmp_path / "signal-spool")
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    source = bus if source_kind == "bus" else ReadonlyNotificationEventRouteSpool(spool.paths.root)
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    state.install_mixed_notification_history()
    queue = _queue(tmp_path / "queue.sqlite3")
    summary = consume_notification_events_to_paper(source, queue, state, observed_at=AT, limit=100)
    assert (
        summary.ended_at_sequence == 3
        and summary.delegated_count == 2
        and summary.ignored_non_trading_count == 1
    )
    ignored = state.non_trading_receipt(2)
    assert ignored.status == "ignored_non_trading" and ignored.record.event == price.event
    assert state.receipt(2) is None and queue.record(price.event.event_id) is None
    assert queue.record(one.signal_id) is not None and queue.record(three.signal_id) is not None
    assert state.non_trading_receipt(2) == ignored
    # Original frozen entry still rejects the dedicated price family.
    with pytest.raises((TypeError, ValueError)):
        queue.ingest(price.event, received_at=AT)
    four = legacy_route(bus, "d", 3)
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    resumed = consume_notification_events_to_paper(source, queue, state, observed_at=AT, limit=100)
    assert resumed.ended_at_sequence == 4 and queue.record(four.signal_id) is not None
    assert state.non_trading_receipt(2) == ignored
    producer.close()


@pytest.mark.parametrize("fault", ["gap", "changed_source", "future", "missing_install"])
def test_full_bad_batch_is_rejected_before_cursor_queue_or_receipt(
    tmp_path: Path, monkeypatch, fault: str
) -> None:
    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    if fault != "missing_install":
        state.install_mixed_notification_history()
    queue = _queue(tmp_path / "queue.sqlite3")
    original = bus.notification_events_after_global_sequence

    def broken(**kwargs):
        rows = original(**kwargs)
        if fault == "gap":
            return (rows[0], rows[2])
        if fault == "changed_source":
            return (rows[0], rows[1].model_copy(update={"bus_generation_id": "a" * 64}), rows[2])
        if fault == "future":
            from datetime import timedelta

            return (
                rows[0],
                rows[1].model_copy(update={"received_at": AT + timedelta(seconds=1)}),
                rows[2],
            )
        return rows

    monkeypatch.setattr(bus, "notification_events_after_global_sequence", broken)
    before = state.cursor()
    with pytest.raises((TypeError, ValueError, RuntimeError)):
        consume_notification_events_to_paper(bus, queue, state, observed_at=AT, limit=100)
    assert state.cursor() == before and queue.record(one.signal_id) is None
    with sqlite3.connect(state.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_consumer_receipt").fetchone()[0] == 0
    producer.close()


def test_non_trading_receipt_and_cursor_commit_together_and_resume(
    tmp_path: Path, monkeypatch
) -> None:
    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    state.install_mixed_notification_history()
    queue = _queue(tmp_path / "queue.sqlite3")
    monkeypatch.setattr(
        state, "_before_non_trading_commit", lambda: (_ for _ in ()).throw(OSError("failed"))
    )
    with pytest.raises(OSError):
        consume_notification_events_to_paper(bus, queue, state, observed_at=AT, limit=100)
    assert state.cursor().last_global_sequence == 1 and state.non_trading_receipt(2) is None
    monkeypatch.setattr(state, "_before_non_trading_commit", lambda: None)
    assert (
        consume_notification_events_to_paper(
            bus, queue, state, observed_at=AT, limit=100
        ).ended_at_sequence
        == 3
    )
    assert state.non_trading_receipt(2).record.event_id == price.event.event_id
    producer.close()


@pytest.mark.parametrize("kind", ["paper_consumer", "paper_broker"])
def test_both_actual_runtime_builders_use_mixed_history_without_price_flags(
    tmp_path: Path, kind: str
) -> None:
    from rquant.runtime_builder_paper import paper_broker_builder, paper_consumer_builder
    from rquant.runtime_service_entrypoint import RuntimeServiceKind
    from tests.unit.test_runtime_builder_paper import _manifest

    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    spool = SignalRouteSpool(tmp_path / "signal-spool")
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    state.install_mixed_notification_history()
    manifest = _manifest(tmp_path, RuntimeServiceKind(kind))
    builder = (
        paper_consumer_builder(clock=lambda: AT)
        if kind == "paper_consumer"
        else paper_broker_builder(
            clock=lambda: AT,
            quote_resolver=lambda *args: (_ for _ in ()).throw(
                AssertionError("price must not execute")
            ),
            trade_date_resolver=lambda now: now.date(),
        )
    )
    assert builder(manifest)().output_sequence == 3
    assert state.non_trading_receipt(2).record.event_id == price.event.event_id
    with sqlite3.connect(tmp_path / "queue.sqlite3") as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        # Inspect the original queue, rather than a fake broker or substitute consumer.
        assert "paper_signal_queue" in tables
        assert connection.execute("SELECT COUNT(*) FROM paper_signal_queue").fetchone()[0] == 2
    if kind == "paper_broker":
        with sqlite3.connect(tmp_path / "broker.sqlite3") as connection:
            assert connection.execute("SELECT COUNT(*) FROM paper_order").fetchone()[0] == 0
    producer.close()


def test_private_consumer_parent_is_checked_as_the_database_parent(tmp_path: Path) -> None:
    public = tmp_path / "parent"
    public.mkdir(mode=0o755)
    public.chmod(0o755)
    private = public / "private"
    private.mkdir(mode=0o700)
    state = PaperSignalConsumerStateStore(private / "consumer.sqlite3")
    state.install_mixed_notification_history()
    assert state.non_trading_receipt(1) is None
