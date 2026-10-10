from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.paper_signal_consumer import (
    PaperSignalConsumerSourceError,
    PaperSignalConsumerStateStore,
    PaperSignalReceiptStatus,
    consume_signal_bus_to_paper,
)
from rquant.paper_signal_worker import (
    PaperSignalPolicy,
    PaperSignalQueueStatus,
    PaperSignalQueueStore,
)
from rquant.signal_bus import SignalBusSourceDescriptor, SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from tests.unit.test_signal_bus import CommitWatcher

NOW = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)


def _condition_paper_world(tmp_path: Path):
    from rquant.signal_route_spool import (
        ReadonlyNotificationEventRouteSpool,
        publish_mixed_notification_bus_prefix,
    )
    from tests.unit.test_price_alert_event_contracts import AT
    from tests.unit.test_signal_route_spool import _condition_mixed_world

    producer, bus, spool, originals = _condition_mixed_world(tmp_path)
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    reader = ReadonlyNotificationEventRouteSpool(spool.paths.root)
    queue = _queue(tmp_path / "condition-paper-queue.sqlite3")
    state = PaperSignalConsumerStateStore(tmp_path / "condition-paper-state.sqlite3")
    state.install_condition_notification_history()
    return producer, bus, reader, queue, state


def test_condition_paper_receipt_is_non_trading_atomic_and_reopens_before_later_legacy(
    tmp_path: Path,
) -> None:
    from rquant.paper_signal_consumer import consume_notification_events_to_paper
    from tests.unit.test_price_alert_event_contracts import AT

    producer, bus, reader, queue, state = _condition_paper_world(tmp_path)
    summary = consume_notification_events_to_paper(reader, queue, state, observed_at=AT, limit=3)
    assert summary.ended_at_sequence == 3
    assert summary.ignored_non_trading_count == 2
    original = state.condition_non_trading_receipt(3)
    assert original.receipt_schema == "paper-condition-non-trading/v1"
    assert queue.record(original.record.event_id) is None
    reopened = PaperSignalConsumerStateStore(state.path)
    assert reopened.condition_non_trading_receipt(3) == original
    assert (
        reopened.complete_condition_non_trading(
            original.record, reader.source_descriptor(), completed_at=AT + timedelta(seconds=1)
        )
        == original
    )
    following = consume_notification_events_to_paper(
        reader, queue, reopened, observed_at=AT, limit=3
    )
    assert following.ended_at_sequence == 4
    assert following.delegated_count == 1
    assert len(reopened.receipts()) == 2
    assert queue.record(original.record.event_id) is None
    producer.close()


@pytest.mark.parametrize("failure", ["source", "future", "payload", "gap", "subclass"])
def test_condition_paper_bad_batch_rejects_before_source_observation_or_any_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from rquant.condition_alert_route import ConditionAlertBusEventRecord
    from rquant.paper_signal_consumer import consume_notification_events_to_paper
    from tests.unit.test_price_alert_event_contracts import AT

    producer, bus, reader, queue, state = _condition_paper_world(tmp_path)
    rows = reader.notification_events_after_global_sequence(
        after_sequence=0, through_sequence=4, observed_at=AT, limit=4
    )
    record = rows[2]
    if failure == "source":
        record = record.model_copy(update={"bus_generation_id": "f" * 64})
    elif failure == "future":
        record = record.model_copy(update={"received_at": AT + timedelta(seconds=1)})
    elif failure == "payload":
        record = record.model_copy(update={"payload_json": "{}"})
    elif failure == "gap":
        record = record.model_copy(update={"global_sequence": 4})
    else:

        class Substitute(ConditionAlertBusEventRecord):
            pass

        record = Substitute.model_validate_json(record.wire_bytes())
    monkeypatch.setattr(
        reader,
        "notification_events_after_global_sequence",
        lambda **_: (rows[0], rows[1], record, rows[3]),
    )
    before = state.cursor()
    with pytest.raises((TypeError, ValueError, RuntimeError)):
        consume_notification_events_to_paper(reader, queue, state, observed_at=AT, limit=4)
    assert state.cursor() == before
    assert state.receipts() == ()
    assert state.condition_non_trading_receipt(3) is None
    for row in (rows[0], rows[3]):
        assert queue.record(row.signal_id) is None
    producer.close()


def test_condition_paper_receipt_write_failure_does_not_advance_consumed_cursor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.paper_signal_consumer import consume_notification_events_to_paper
    from tests.unit.test_price_alert_event_contracts import AT

    producer, bus, reader, queue, state = _condition_paper_world(tmp_path)
    consume_notification_events_to_paper(reader, queue, state, observed_at=AT, limit=2)
    before = state.cursor()
    monkeypatch.setattr(
        state,
        "_before_non_trading_commit",
        lambda: (_ for _ in ()).throw(OSError("receipt commit failure")),
    )
    with pytest.raises(OSError):
        consume_notification_events_to_paper(reader, queue, state, observed_at=AT, limit=1)
    assert state.cursor() == before
    assert state.condition_non_trading_receipt(3) is None
    producer.close()


@pytest.mark.parametrize("point", ["before_receipt", "after_commit_ack_loss"])
def test_condition_paper_native_receipt_insert_and_committed_ack_loss_recover_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    from collections.abc import Iterator
    from contextlib import contextmanager

    from rquant.paper_signal_consumer import consume_notification_events_to_paper
    from tests.unit.test_price_alert_event_contracts import AT

    producer, _, reader, queue, state = _condition_paper_world(tmp_path)
    try:
        consume_notification_events_to_paper(reader, queue, state, observed_at=AT, limit=2)
        original_cursor = state.cursor()
        record = reader.notification_events_after_global_sequence(
            after_sequence=2, through_sequence=3, observed_at=AT, limit=1
        )[0]
        source = reader.source_descriptor()
        with monkeypatch.context() as faults:
            if point == "before_receipt":
                original_connect = state._connect

                def denied_receipt_connection() -> sqlite3.Connection:
                    connection = original_connect()

                    def authorizer(
                        operation: int,
                        table: str | None,
                        column: str | None,
                        database: str | None,
                        trigger: str | None,
                    ) -> int:
                        return (
                            sqlite3.SQLITE_DENY
                            if operation == sqlite3.SQLITE_INSERT
                            and table == "paper_condition_non_trading_receipt"
                            else sqlite3.SQLITE_OK
                        )

                    connection.set_authorizer(authorizer)
                    return connection

                faults.setattr(state, "_connect", denied_receipt_connection)
            else:
                original_transaction = state._write_transaction

                @contextmanager
                def committed_ack_loss() -> Iterator[sqlite3.Connection]:
                    with original_transaction() as connection:
                        yield connection
                    raise OSError("synthetic loss after actual SQLite commit")

                faults.setattr(state, "_write_transaction", committed_ack_loss)
            with pytest.raises((sqlite3.DatabaseError, OSError)):
                state.complete_condition_non_trading(record, source, completed_at=AT)
        reopened = PaperSignalConsumerStateStore(state.path)
        original_receipt = reopened.condition_non_trading_receipt(3)
        if point == "before_receipt":
            assert original_receipt is None and reopened.cursor() == original_cursor
        else:
            assert original_receipt is not None and reopened.cursor().last_global_sequence == 3
        summary = consume_notification_events_to_paper(
            reader, queue, reopened, observed_at=AT, limit=3
        )
        actual = reopened.condition_non_trading_receipt(3)
        assert actual is not None and (original_receipt is None or actual == original_receipt)
        assert summary.delegated_count == 1 and reopened.cursor().last_global_sequence == 4
        assert queue.record(actual.record.event_id) is None
        replay = consume_notification_events_to_paper(
            reader, queue, reopened, observed_at=AT, limit=3
        )
        assert (
            replay.delegated_count == replay.ignored_non_trading_count == replay.replayed_count == 0
        )
        assert replay.watermark_advanced is False
    finally:
        producer.close()


def _bus(path: Path) -> SignalBusStore:
    return SignalBusStore(path)


def _queue(path: Path) -> PaperSignalQueueStore:
    return PaperSignalQueueStore(
        path,
        policy=PaperSignalPolicy(
            account_id="paper-main",
            execution_lag=timedelta(minutes=1),
            action_quantities={
                SignalAction.B_INTENT: 1_000,
                SignalAction.REDUCE: 500,
                SignalAction.S_INTENT: 1_000,
            },
            producer_commit="a" * 40,
        ),
    )


def _signal(
    seed: str,
    *,
    action: SignalAction = SignalAction.B_INTENT,
    available_at: datetime = NOW,
) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64,
        event_time=available_at - timedelta(seconds=5),
        available_at=available_at,
        candidate_id=f"60000{ord(seed) % 10}.SH",
        action=action,
        reason_codes=("test",),
        evidence={"seed": seed},
        expires_at=available_at + timedelta(minutes=5),
        producer_commit="d" * 40,
    )


def test_consumer_delegates_every_signal_in_global_order_and_preserves_watch(
    tmp_path: Path,
) -> None:
    bus = _bus(tmp_path / "bus.sqlite3")
    queue = _queue(tmp_path / "queue.sqlite3")
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    buy = _signal("a")
    watch = _signal("e", action=SignalAction.WATCH)
    bus.ingest(buy, received_at=NOW)
    bus.ingest(watch, received_at=NOW)

    summary = consume_signal_bus_to_paper(
        bus,
        queue,
        state,
        observed_at=NOW,
        limit=10,
    )

    assert summary.started_after_sequence == 0
    assert summary.ended_at_sequence == 2
    assert summary.delegated_count == 2
    assert summary.replayed_count == 0
    assert state.cursor().last_global_sequence == 2
    assert [receipt.global_sequence for receipt in state.receipts()] == [1, 2]
    assert all(receipt.status is PaperSignalReceiptStatus.DELEGATED for receipt in state.receipts())
    assert queue.record(buy.signal_id).status is PaperSignalQueueStatus.PENDING  # type: ignore[union-attr]
    assert queue.record(watch.signal_id).status is PaperSignalQueueStatus.IGNORED  # type: ignore[union-attr]


def test_future_signal_blocks_itself_and_later_sequence(tmp_path: Path) -> None:
    bus = _bus(tmp_path / "bus.sqlite3")
    queue = _queue(tmp_path / "queue.sqlite3")
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    future = _signal("a", available_at=NOW + timedelta(minutes=2))
    later = _signal("e")
    bus.ingest(future, received_at=NOW)
    bus.ingest(later, received_at=NOW)

    early = consume_signal_bus_to_paper(
        bus,
        queue,
        state,
        observed_at=NOW,
        limit=10,
    )
    visible = consume_signal_bus_to_paper(
        bus,
        queue,
        state,
        observed_at=NOW + timedelta(minutes=2),
        limit=10,
    )

    assert early.delegated_count == 0
    assert early.has_deferred_signals is True
    assert visible.delegated_count == 2
    assert state.cursor().last_global_sequence == 2


def test_crash_after_paper_ingest_replays_without_duplicate_durable_delegation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus = _bus(tmp_path / "bus.sqlite3")
    queue_path = tmp_path / "queue.sqlite3"
    queue = _queue(queue_path)
    state_path = tmp_path / "consumer.sqlite3"
    state = PaperSignalConsumerStateStore(state_path)
    signal = _signal("a")
    bus.ingest(signal, received_at=NOW)

    monkeypatch.setattr(
        state,
        "_after_paper_ingest",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("injected crash")),
    )
    with pytest.raises(RuntimeError, match="injected crash"):
        consume_signal_bus_to_paper(
            bus,
            queue,
            state,
            observed_at=NOW,
            limit=10,
        )

    assert queue.record(signal.signal_id) is not None
    assert state.cursor().last_global_sequence == 0
    assert state.receipt(1).status is PaperSignalReceiptStatus.BOUND  # type: ignore[union-attr]

    reopened_state = PaperSignalConsumerStateStore(state_path)
    replayed = consume_signal_bus_to_paper(
        bus,
        _queue(queue_path),
        reopened_state,
        observed_at=NOW + timedelta(seconds=1),
        limit=10,
    )

    assert replayed.delegated_count == 1
    assert replayed.replayed_count == 1
    assert reopened_state.cursor().last_global_sequence == 1
    assert len(reopened_state.receipts()) == 1
    assert reopened_state.receipt(1).status is PaperSignalReceiptStatus.DELEGATED  # type: ignore[union-attr]


def test_consumer_rejects_generation_drift_and_high_watermark_rollback(
    tmp_path: Path,
) -> None:
    bus_path = tmp_path / "bus.sqlite3"
    bus = _bus(bus_path)
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    queue = _queue(tmp_path / "queue.sqlite3")
    bus.ingest(_signal("a"), received_at=NOW)
    consume_signal_bus_to_paper(
        bus,
        queue,
        state,
        observed_at=NOW,
        limit=10,
    )

    bus_path.unlink()
    rebuilt = _bus(bus_path)
    with pytest.raises(PaperSignalConsumerSourceError, match="generation"):
        consume_signal_bus_to_paper(
            rebuilt,
            queue,
            state,
            observed_at=NOW + timedelta(seconds=1),
            limit=10,
        )

    rollback_bus_path = tmp_path / "rollback-bus.sqlite3"
    rollback_bus = _bus(rollback_bus_path)
    rollback_state = PaperSignalConsumerStateStore(tmp_path / "rollback-state.sqlite3")
    rollback_queue = _queue(tmp_path / "rollback-queue.sqlite3")
    rollback_bus.ingest(_signal("e"), received_at=NOW)
    rollback_bus.ingest(
        _signal("f", available_at=NOW + timedelta(minutes=2)),
        received_at=NOW,
    )
    consume_signal_bus_to_paper(
        rollback_bus,
        rollback_queue,
        rollback_state,
        observed_at=NOW,
        limit=10,
    )
    # A rolled-back source is modelled as a *consistent* restore from an older snapshot:
    # the rows above the restore point are gone and the watermark matches them again.
    # Codex round-2 ruling 5 makes the bus itself fail closed on a watermark that
    # disagrees with its rows, so metadata-only tampering never reaches this consumer.
    with sqlite3.connect(rollback_bus_path) as connection:
        connection.execute("DELETE FROM signal_envelope WHERE global_sequence > 1")
        connection.execute(
            """
            UPDATE signal_bus_metadata
            SET metadata_value = '1'
            WHERE metadata_key = 'signal_high_watermark'
            """
        )

    with pytest.raises(PaperSignalConsumerSourceError, match="high watermark"):
        consume_signal_bus_to_paper(
            rollback_bus,
            rollback_queue,
            rollback_state,
            observed_at=NOW + timedelta(seconds=1),
            limit=10,
        )


def test_consumer_detects_sequence_truncation_and_rejects_naive_time(
    tmp_path: Path,
) -> None:
    bus_path = tmp_path / "bus.sqlite3"
    bus = _bus(bus_path)
    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    queue = _queue(tmp_path / "queue.sqlite3")
    bus.ingest(_signal("a"), received_at=NOW)
    bus.ingest(_signal("e"), received_at=NOW)
    bus.ingest(_signal("f"), received_at=NOW)
    with sqlite3.connect(bus_path) as connection:
        connection.execute("DELETE FROM signal_envelope WHERE global_sequence = 2")

    with pytest.raises(PaperSignalConsumerSourceError, match="gap|truncated"):
        consume_signal_bus_to_paper(
            bus,
            queue,
            state,
            observed_at=NOW,
            limit=10,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        consume_signal_bus_to_paper(
            bus,
            queue,
            state,
            observed_at=NOW.replace(tzinfo=None),
            limit=10,
        )


def test_concurrent_consumers_share_one_ordered_cursor(tmp_path: Path) -> None:
    bus = _bus(tmp_path / "bus.sqlite3")
    queue_path = tmp_path / "queue.sqlite3"
    state_path = tmp_path / "consumer.sqlite3"
    for seed in ("a", "e", "f"):
        bus.ingest(_signal(seed), received_at=NOW)

    def consume() -> int:
        summary = consume_signal_bus_to_paper(
            bus,
            _queue(queue_path),
            PaperSignalConsumerStateStore(state_path),
            observed_at=NOW,
            limit=10,
        )
        return summary.ended_at_sequence

    with ThreadPoolExecutor(max_workers=2) as pool:
        endings = tuple(pool.map(lambda _index: consume(), range(2)))

    state = PaperSignalConsumerStateStore(state_path)
    assert max(endings) == 3
    assert state.cursor().last_global_sequence == 3
    assert [receipt.global_sequence for receipt in state.receipts()] == [1, 2, 3]
    assert all(
        _queue(queue_path).record(_signal(seed).signal_id) is not None for seed in ("a", "e", "f")
    )


# ---------------------------------------------------------------------------------------
# #271: what one idle `observe_source` costs the host
# ---------------------------------------------------------------------------------------

#: Two seconds, the production interval of `paper-broker.shadow-main.v1`.
BROKER_INTERVAL = timedelta(seconds=2)
IDLE_ITERATIONS = 60


def _source_descriptor(high_watermark: int) -> SignalBusSourceDescriptor:
    return SignalBusSourceDescriptor(
        generation_id="a" * 64,
        first_global_sequence=1,
        high_watermark=high_watermark,
    )


def test_observing_an_unchanged_signal_source_commits_nothing_at_all(tmp_path: Path) -> None:
    """Sixty idle observations, zero commits, and the row byte-identical afterwards.

    The UPDATE this replaces set the watermark, which had not moved, and `updated_at`,
    which was the broker's own loop clock — so it rewrote the row on every iteration of a
    two-second loop, on a `journal_mode=WAL`, `synchronous=FULL` database, all day (#271).
    """

    path = tmp_path / "consumer.sqlite3"
    state = PaperSignalConsumerStateStore(path)
    first = state.observe_source(_source_descriptor(3), observed_at=NOW)

    assert first.watermark_advanced is True
    assert first.cursor.observed_high_watermark == 3

    watcher = CommitWatcher(path, tables=("paper_consumer_source", "paper_consumer_receipt"))
    try:
        before = watcher.stamp()
        idle = [
            state.observe_source(
                _source_descriptor(3),
                # A fresh clock on every iteration: the whole point is that the row no
                # longer carries it.
                observed_at=NOW + BROKER_INTERVAL * (index + 1),
            )
            for index in range(IDLE_ITERATIONS)
        ]
        after = watcher.stamp()
    finally:
        watcher.close()

    assert after == before, "an idle source observation committed to the consumer state"
    assert [item.watermark_advanced for item in idle] == [False] * IDLE_ITERATIONS
    assert [item.cursor.observed_high_watermark for item in idle] == [3] * IDLE_ITERATIONS
    # `updated_at` now means "when the watermark last advanced", and the first observation
    # is when that was.
    assert idle[-1].cursor.updated_at == NOW


def test_a_signal_source_that_grows_writes_exactly_once(tmp_path: Path) -> None:
    """One commit for the one observation that had something to record, none on either side."""

    path = tmp_path / "consumer.sqlite3"
    state = PaperSignalConsumerStateStore(path)
    state.observe_source(_source_descriptor(3), observed_at=NOW)

    watcher = CommitWatcher(path, tables=("paper_consumer_source",))
    try:
        before = watcher.stamp()
        state.observe_source(_source_descriptor(3), observed_at=NOW + BROKER_INTERVAL)
        quiet = watcher.stamp()
        grown = state.observe_source(_source_descriptor(4), observed_at=NOW + BROKER_INTERVAL * 2)
        wrote = watcher.stamp()
        state.observe_source(_source_descriptor(4), observed_at=NOW + BROKER_INTERVAL * 3)
        settled = watcher.stamp()
    finally:
        watcher.close()

    assert quiet == before
    assert wrote != quiet
    assert settled == wrote
    assert grown.watermark_advanced is True
    assert grown.cursor.observed_high_watermark == 4
    assert grown.cursor.updated_at == NOW + BROKER_INTERVAL * 2


def test_the_gate_leaves_every_observation_check_running_on_an_unchanged_source(
    tmp_path: Path,
) -> None:
    """Skipping the write does not skip a single verification the observation used to make.

    Each of these is refused against a row whose watermark is exactly the stored one — the
    case that now writes nothing — so the checks demonstrably still run.
    """

    state = PaperSignalConsumerStateStore(tmp_path / "consumer.sqlite3")
    state.observe_source(_source_descriptor(3), observed_at=NOW)

    with pytest.raises(PaperSignalConsumerSourceError, match="rolled back"):
        state.observe_source(_source_descriptor(2), observed_at=NOW + BROKER_INTERVAL)
    with pytest.raises(PaperSignalConsumerSourceError, match="generation changed"):
        state.observe_source(
            _source_descriptor(3).model_copy(update={"generation_id": "b" * 64}),
            observed_at=NOW + BROKER_INTERVAL,
        )
    with pytest.raises(PaperSignalConsumerSourceError, match="source id changed"):
        state.observe_source(
            _source_descriptor(3).model_copy(update={"source_id": "other-bus/v1"}),
            observed_at=NOW + BROKER_INTERVAL,
        )
    assert state.cursor().observed_high_watermark == 3


def test_a_drained_consumer_pass_commits_nothing_and_says_so(tmp_path: Path) -> None:
    """The same property through `consume_signal_bus_to_paper`, which is what the role runs."""

    bus = _bus(tmp_path / "bus.sqlite3")
    queue = _queue(tmp_path / "queue.sqlite3")
    path = tmp_path / "consumer.sqlite3"
    state = PaperSignalConsumerStateStore(path)
    bus.ingest(_signal("a"), received_at=NOW)

    drained = consume_signal_bus_to_paper(bus, queue, state, observed_at=NOW, limit=10)
    assert drained.watermark_advanced is True

    watcher = CommitWatcher(path, tables=("paper_consumer_source", "paper_consumer_receipt"))
    try:
        before = watcher.stamp()
        idle = [
            consume_signal_bus_to_paper(
                bus,
                queue,
                state,
                observed_at=NOW + BROKER_INTERVAL * (index + 1),
                limit=10,
            )
            for index in range(IDLE_ITERATIONS)
        ]
        after = watcher.stamp()
        bus.ingest(_signal("e"), received_at=NOW + BROKER_INTERVAL * 61)
        grown = consume_signal_bus_to_paper(
            bus,
            queue,
            state,
            observed_at=NOW + BROKER_INTERVAL * 61,
            limit=10,
        )
        wrote = watcher.stamp()
    finally:
        watcher.close()

    assert after == before, "an idle paper consumer pass committed to its state database"
    assert [item.watermark_advanced for item in idle] == [False] * IDLE_ITERATIONS
    assert [item.delegated_count for item in idle] == [0] * IDLE_ITERATIONS
    assert grown.watermark_advanced is True
    assert grown.delegated_count == 1
    assert wrote != after
