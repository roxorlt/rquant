from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.paper_broker import BrokerExecutionContext, PaperBrokerStore
from rquant.paper_contracts import PaperOrderStatus, PaperRejectReason
from rquant.paper_signal_worker import (
    PaperQuoteSnapshot,
    PaperSignalPolicy,
    PaperSignalQueueStatus,
    PaperSignalQueueStore,
    run_paper_signal_batch,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from tests.paper_cost_fixtures import paper_cost_policy, paper_instrument_context
from tests.unit.test_paper_broker import (
    _downgrade_v3_initial_execution_evidence,
    _ledger_rows,
)
from tests.unit.test_paper_broker import (
    _intent as _broker_intent,
)
from tests.unit.test_paper_broker import (
    _quote as _broker_execution_quote,
)

ACCOUNT_ID = "paper-main"
SIGNAL_TIME = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
EXECUTION_TIME = SIGNAL_TIME + timedelta(minutes=1)
TRADE_DATE = date(2026, 7, 31)
NEXT_TRADE_DATE = date(2026, 8, 3)


def _policy(
    *,
    buy_quantity: int = 1_000,
    producer_commit: str = "a" * 40,
) -> PaperSignalPolicy:
    return PaperSignalPolicy(
        account_id=ACCOUNT_ID,
        execution_lag=timedelta(minutes=1),
        action_quantities={
            SignalAction.B_INTENT: buy_quantity,
            SignalAction.REDUCE: 500,
            SignalAction.S_INTENT: 1_000,
        },
        producer_commit=producer_commit,
    )


def _signal(
    action: SignalAction = SignalAction.B_INTENT,
    *,
    seed: str = "b",
    event_time: datetime = SIGNAL_TIME,
    entry_signal_id: str | None = None,
    tranche_fraction: float | None = None,
) -> SignalEnvelope:
    if tranche_fraction is None and action is SignalAction.S_INTENT:
        tranche_fraction = 1.0
    elif tranche_fraction is None and action is SignalAction.REDUCE:
        tranche_fraction = 0.5
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=event_time,
        available_at=event_time + timedelta(seconds=5),
        candidate_id="600000.SH",
        action=action,
        reason_codes=("test",),
        evidence={
            **({} if entry_signal_id is None else {"entry_signal_id": entry_signal_id}),
            **({} if tranche_fraction is None else {"sell_tranche_fraction": tranche_fraction}),
        },
        expires_at=event_time + timedelta(minutes=5),
        producer_commit="e" * 40,
    )


def _quote(
    *,
    price: str = "10.00",
    available_at: datetime = EXECUTION_TIME,
    acquisition_available_date: date | None = NEXT_TRADE_DATE,
) -> PaperQuoteSnapshot:
    return PaperQuoteSnapshot(
        ts_code="600000.SH",
        event_time=available_at,
        available_at=available_at,
        context=BrokerExecutionContext(
            executable_price=Decimal(price),
            acquisition_available_date=acquisition_available_date,
            instrument_context=paper_instrument_context("600000.SH"),
        ),
        producer_commit="f" * 40,
    )


def _broker(path: Path) -> PaperBrokerStore:
    return PaperBrokerStore(
        path,
        account_id=ACCOUNT_ID,
        initial_cash=Decimal("100000"),
        cost_policy=paper_cost_policy(),
    )


def test_signal_waits_until_next_executable_minute_then_fills(tmp_path: Path) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    signal = _signal()
    queued = queue.ingest(signal, received_at=signal.available_at)

    early = run_paper_signal_batch(
        queue,
        broker,
        now=signal.available_at,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail("quote requested before due time"),
        limit=10,
    )
    completed = run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(),
        limit=10,
    )

    assert queued.due_at == EXECUTION_TIME
    assert early.due_count == 0
    assert completed.completed_count == 1
    record = queue.record(signal.signal_id)
    assert record is not None and record.status is PaperSignalQueueStatus.COMPLETED
    assert record.order is not None and record.order.status is PaperOrderStatus.FILLED
    assert len(broker.fills(record.order.order_id)) == 1


def test_worker_does_not_prepare_ack_or_fill_a_quarantined_legacy_ledger(
    tmp_path: Path,
) -> None:
    broker_path = tmp_path / "broker.sqlite3"
    seeded = _broker(broker_path)
    seeded.submit_intent(
        _broker_intent(quantity=1_000),
        decision_time=SIGNAL_TIME,
        trade_date=TRADE_DATE,
        quote=_broker_execution_quote("10.00", executable_quantity=500),
    )
    _downgrade_v3_initial_execution_evidence(broker_path)
    broker = _broker(broker_path)
    before = _ledger_rows(broker_path)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    signal = _signal(seed="9")
    queue.ingest(signal, received_at=signal.available_at)

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail(
            "quarantined broker must fail before preparing executable evidence"
        ),
        limit=10,
    )

    record = queue.record(signal.signal_id)
    assert summary.completed_count == 0
    assert summary.failed_count == 1
    assert record is not None and record.status is PaperSignalQueueStatus.PENDING
    assert record.order is None and record.execution_id is None
    assert record.last_error is not None and "quarantin" in record.last_error
    assert _ledger_rows(broker_path) == before


def test_watch_signal_is_explicitly_ignored_and_policy_is_bound(tmp_path: Path) -> None:
    path = tmp_path / "queue.sqlite3"
    queue = PaperSignalQueueStore(path, policy=_policy())
    signal = _signal(SignalAction.WATCH)

    ignored = queue.ingest(signal, received_at=signal.available_at)

    assert ignored.status is PaperSignalQueueStatus.IGNORED
    assert ignored.last_error == "action watch is not executable by paper broker"
    with pytest.raises(ValueError, match="paper signal policy"):
        PaperSignalQueueStore(path, policy=_policy(buy_quantity=2_000))


def test_queue_and_broker_restart_across_commit_preserves_execution_provenance(
    tmp_path: Path,
) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    broker_path = tmp_path / "broker.sqlite3"
    old_commit = "a" * 40
    new_commit = "9" * 40
    old_queue = PaperSignalQueueStore(
        queue_path,
        policy=_policy(producer_commit=old_commit),
    )
    broker = _broker(broker_path)
    old_signal = _signal(seed="1")
    old_queue.ingest(old_signal, received_at=old_signal.available_at)
    old_result = run_paper_signal_batch(
        old_queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(),
        limit=10,
    )

    new_queue = PaperSignalQueueStore(
        queue_path,
        policy=_policy(producer_commit=new_commit),
    )
    reopened_broker = _broker(broker_path)
    new_signal = _signal(
        seed="2",
        event_time=SIGNAL_TIME + timedelta(minutes=2),
    )
    new_queue.ingest(new_signal, received_at=new_signal.available_at)
    new_execution_time = EXECUTION_TIME + timedelta(minutes=2)
    new_result = run_paper_signal_batch(
        new_queue,
        reopened_broker,
        now=new_execution_time,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: PaperQuoteSnapshot(
            ts_code="600000.SH",
            event_time=new_execution_time,
            available_at=new_execution_time,
            context=BrokerExecutionContext(
                executable_price=Decimal("10.50"),
                acquisition_available_date=NEXT_TRADE_DATE,
                instrument_context=paper_instrument_context("600000.SH"),
            ),
            producer_commit=new_commit,
        ),
        limit=10,
    )

    old_record = new_queue.record(old_signal.signal_id)
    new_record = new_queue.record(new_signal.signal_id)
    assert old_result.completed_count == 1
    assert new_result.completed_count == 1
    assert old_record is not None and old_record.intent is not None
    assert new_record is not None and new_record.intent is not None
    assert old_record.intent.producer_commit == old_commit
    assert new_record.intent.producer_commit == new_commit
    assert len(reopened_broker.fills()) == 2
    with sqlite3.connect(broker_path) as connection:
        fill_commits = connection.execute(
            """
            SELECT json_extract(i.payload_json, '$.producer_commit')
            FROM paper_fill AS f
            JOIN paper_order AS o ON o.order_id = f.order_id
            JOIN paper_intent AS i ON i.intent_id = o.intent_id
            ORDER BY f.executed_at, f.fill_id
            """
        ).fetchall()
    assert fill_commits == [(old_commit,), (new_commit,)]


def test_legacy_commit_bound_policy_fingerprint_migrates_without_losing_guard(
    tmp_path: Path,
) -> None:
    path = tmp_path / "queue.sqlite3"
    old_policy = _policy(producer_commit="a" * 40)
    PaperSignalQueueStore(path, policy=old_policy)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE paper_signal_metadata SET policy_fingerprint = ?
            WHERE singleton = 1
            """,
            (old_policy.provenance_fingerprint,),
        )

    new_policy = _policy(producer_commit="9" * 40)
    PaperSignalQueueStore(path, policy=new_policy)
    with sqlite3.connect(path) as connection:
        migrated = connection.execute(
            """
            SELECT policy_fingerprint FROM paper_signal_metadata
            WHERE singleton = 1
            """
        ).fetchone()

    assert migrated == (new_policy.semantic_fingerprint,)
    with pytest.raises(ValueError, match="paper signal policy"):
        PaperSignalQueueStore(path, policy=_policy(buy_quantity=2_000))


def test_quote_failure_keeps_signal_pending_for_bounded_retry(tmp_path: Path) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: (_ for _ in ()).throw(TimeoutError("quote timeout")),
        limit=10,
    )

    record = queue.record(signal.signal_id)
    assert summary.failed_count == 1
    assert record is not None and record.status is PaperSignalQueueStatus.PENDING
    assert record.last_error == "TimeoutError: quote timeout"
    assert broker.fills() == ()


def test_crash_after_broker_fill_reuses_prepared_intent_without_double_fill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    broker = _broker(tmp_path / "broker.sqlite3")
    queue = PaperSignalQueueStore(queue_path, policy=_policy())
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)

    monkeypatch.setattr(
        queue,
        "complete",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("queue crash")),
    )
    first = run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(),
        limit=10,
    )
    assert first.failed_count == 1
    assert len(broker.fills()) == 1

    reopened = PaperSignalQueueStore(queue_path, policy=_policy())
    second = run_paper_signal_batch(
        reopened,
        broker,
        now=EXECUTION_TIME + timedelta(seconds=1),
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail("prepared quote must be reused"),
        limit=10,
    )

    assert second.completed_count == 1
    assert len(broker.fills()) == 1


def test_crash_after_prepare_before_broker_refreshes_quote_before_execution(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)
    old = queue.prepare(
        signal.signal_id,
        quote=_quote(price="10.00"),
        prepared_at=EXECUTION_TIME,
    )
    resumed_at = EXECUTION_TIME + timedelta(seconds=30)
    resolver_calls: list[datetime] = []

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=resumed_at,
        trade_date=TRADE_DATE,
        quote_resolver=lambda _signal, observed_at: (
            resolver_calls.append(observed_at),
            _quote(price="11.00", available_at=resumed_at),
        )[1],
        limit=10,
    )

    record = queue.record(signal.signal_id)
    assert summary.completed_count == 1
    assert resolver_calls == [resumed_at]
    assert record is not None and record.quote is not None and record.intent is not None
    assert record.quote.snapshot_id != old.quote.snapshot_id  # type: ignore[union-attr]
    assert broker.fills()[0].price == Decimal("11.0000")


def test_same_day_sell_is_recorded_as_t_plus_one_rejection(tmp_path: Path) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    buy = _signal(seed="b")
    queue.ingest(buy, received_at=buy.available_at)
    run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(),
        limit=10,
    )
    sell = _signal(
        SignalAction.S_INTENT,
        seed="f",
        event_time=SIGNAL_TIME + timedelta(minutes=2),
        entry_signal_id=buy.signal_id,
    )
    queue.ingest(sell, received_at=sell.available_at)

    run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME + timedelta(minutes=2),
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(
            price="10.50",
            available_at=EXECUTION_TIME + timedelta(minutes=2),
            acquisition_available_date=None,
        ),
        limit=10,
    )

    record = queue.record(sell.signal_id)
    assert record is not None and record.order is not None
    assert record.order.status is PaperOrderStatus.REJECTED
    assert record.order.reject_reason is PaperRejectReason.T_PLUS_ONE


def test_sell_prepare_carries_entry_signal_provenance_into_immutable_intent(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    entry = _signal(seed="1")
    queue.ingest(entry, received_at=entry.available_at)
    run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(),
        limit=10,
    )
    sell = _signal(
        SignalAction.S_INTENT,
        seed="2",
        event_time=SIGNAL_TIME + timedelta(days=3),
        entry_signal_id=entry.signal_id,
    )
    queue.ingest(sell, received_at=sell.available_at)
    due_at = sell.event_time + timedelta(minutes=1)

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=due_at,
        trade_date=NEXT_TRADE_DATE,
        quote_resolver=lambda *_args: _quote(available_at=due_at, acquisition_available_date=None),
        limit=10,
    )
    prepared = queue.record(sell.signal_id)

    assert summary.completed_count == 1
    assert prepared is not None and prepared.intent is not None
    assert prepared.intent.entry_signal_id == entry.signal_id
    assert prepared.intent.sell_quantity_authority is not None
    assert prepared.intent.sell_quantity_authority.exit_signal_id == sell.signal_id


def test_sell_prepare_fails_closed_without_entry_signal_provenance(tmp_path: Path) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    sell = _signal(SignalAction.S_INTENT, seed="3")
    queue.ingest(sell, received_at=sell.available_at)

    with pytest.raises(ValueError, match="entry_signal_id"):
        queue.prepare(
            sell.signal_id,
            quote=_quote(acquisition_available_date=None),
            prepared_at=EXECUTION_TIME,
        )


def test_future_quote_is_rejected_without_preparing_mutable_intent(tmp_path: Path) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(available_at=EXECUTION_TIME + timedelta(seconds=1)),
        limit=10,
    )

    assert summary.failed_count == 1
    record = queue.record(signal.signal_id)
    assert record is not None and record.status is PaperSignalQueueStatus.PENDING
    assert record.intent is None


def test_worker_reduce_then_sell_uses_authoritative_remaining_quantity(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    buy = _signal(seed="1")
    queue.ingest(buy, received_at=buy.available_at)
    run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(),
        limit=10,
    )

    reduce_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)
    reduce = _signal(
        SignalAction.REDUCE,
        seed="2",
        event_time=reduce_time,
        entry_signal_id=buy.signal_id,
        tranche_fraction=0.5,
    )
    queue.ingest(reduce, received_at=reduce.available_at)
    reduce_at = reduce_time + timedelta(minutes=1)
    run_paper_signal_batch(
        queue,
        broker,
        now=reduce_at,
        trade_date=NEXT_TRADE_DATE,
        quote_resolver=lambda *_args: _quote(
            price="12.00",
            available_at=reduce_at,
            acquisition_available_date=None,
        ),
        limit=10,
    )

    sell = _signal(
        SignalAction.S_INTENT,
        seed="3",
        event_time=reduce_time + timedelta(minutes=2),
        entry_signal_id=buy.signal_id,
        tranche_fraction=1.0,
    )
    queue.ingest(sell, received_at=sell.available_at)
    sell_at = sell.event_time + timedelta(minutes=1)
    summary = run_paper_signal_batch(
        queue,
        broker,
        now=sell_at,
        trade_date=NEXT_TRADE_DATE,
        quote_resolver=lambda *_args: _quote(
            price="12.50",
            available_at=sell_at,
            acquisition_available_date=None,
        ),
        limit=10,
    )

    reduce_record = queue.record(reduce.signal_id)
    sell_record = queue.record(sell.signal_id)
    assert summary.completed_count == 1
    assert reduce_record is not None and reduce_record.intent is not None
    assert reduce_record.intent.quantity == 500
    assert sell_record is not None and sell_record.intent is not None
    assert sell_record.intent.quantity == 500
    assert sell_record.order is not None
    assert sell_record.order.status is PaperOrderStatus.FILLED
    assert broker.reconcile().open_lot_quantity == 0


def test_worker_does_not_submit_reduce_that_would_close_one_lot_position(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(
        tmp_path / "queue.sqlite3",
        policy=_policy(buy_quantity=100),
    )
    broker = _broker(tmp_path / "broker.sqlite3")
    buy = _signal(seed="4")
    queue.ingest(buy, received_at=buy.available_at)
    run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: _quote(),
        limit=10,
    )
    reduce_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)
    reduce = _signal(
        SignalAction.REDUCE,
        seed="5",
        event_time=reduce_time,
        entry_signal_id=buy.signal_id,
        tranche_fraction=0.5,
    )
    queue.ingest(reduce, received_at=reduce.available_at)

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=reduce_time + timedelta(minutes=1),
        trade_date=NEXT_TRADE_DATE,
        quote_resolver=lambda *_args: _quote(
            price="12.00",
            available_at=reduce_time + timedelta(minutes=1),
            acquisition_available_date=None,
        ),
        limit=10,
    )

    record = queue.record(reduce.signal_id)
    assert summary.completed_count == 0
    assert summary.failed_count == 0
    assert record is not None and record.status is PaperSignalQueueStatus.IGNORED
    assert record.last_error == "REDUCE has no legal partial 100-share lot"
    assert broker.reconcile().open_lot_quantity == 100


def test_prepared_signal_expires_without_losing_frozen_audit_evidence(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)
    prepared = queue.prepare(
        signal.signal_id,
        quote=_quote(),
        prepared_at=EXECUTION_TIME,
    )

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=signal.expires_at,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail(
            "expired PREPARED record must not refresh before broker absence is confirmed"
        ),
        limit=10,
    )
    expired = queue.record(signal.signal_id)

    assert prepared.status is PaperSignalQueueStatus.PREPARED
    assert summary.due_count == 1
    assert summary.completed_count == 0
    assert summary.failed_count == 0
    assert expired is not None and expired.status is PaperSignalQueueStatus.EXPIRED
    assert expired.execution_id == prepared.execution_id
    assert expired.quote == prepared.quote
    assert expired.intent == prepared.intent
    assert expired.order is None


def test_expired_prepared_signal_recovers_broker_commit_before_queue_completion(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)
    prepared = queue.prepare(
        signal.signal_id,
        quote=_quote(),
        prepared_at=EXECUTION_TIME,
    )
    assert prepared.intent is not None
    assert prepared.execution_id is not None
    committed = broker.submit_intent(
        prepared.intent,
        execution_id=prepared.execution_id,
        decision_time=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote=prepared.quote.context,
    )

    recovered = run_paper_signal_batch(
        queue,
        broker,
        now=signal.expires_at + timedelta(seconds=1),
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail(
            "authoritative broker recovery must precede expiry and quote refresh"
        ),
        limit=10,
    )

    record = queue.record(signal.signal_id)
    assert recovered.completed_count == 1
    assert recovered.failed_count == 0
    assert record is not None and record.status is PaperSignalQueueStatus.COMPLETED
    assert record.order == committed
    assert len(broker.fills(committed.order_id)) == 1


def test_prepared_recovery_rejects_same_intent_committed_under_another_execution_id(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)
    prepared = queue.prepare(
        signal.signal_id,
        quote=_quote(),
        prepared_at=EXECUTION_TIME,
    )
    assert prepared.intent is not None
    assert prepared.execution_id is not None
    broker.submit_intent(
        prepared.intent,
        execution_id="9" * 64,
        decision_time=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote=prepared.quote.context,
    )

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=signal.expires_at + timedelta(seconds=1),
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail(
            "execution identity conflict must fail before quote refresh"
        ),
        limit=10,
    )

    record = queue.record(signal.signal_id)
    assert summary.completed_count == 0
    assert summary.failed_count == 1
    assert record is not None and record.status is PaperSignalQueueStatus.PREPARED
    assert record.order is None
    assert record.last_error is not None
    assert "execution" in record.last_error or "identity" in record.last_error


def test_prepared_recovery_reconciles_broker_before_queue_completion(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    broker_path = tmp_path / "broker.sqlite3"
    broker = _broker(broker_path)
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)
    prepared = queue.prepare(
        signal.signal_id,
        quote=_quote(),
        prepared_at=EXECUTION_TIME,
    )
    assert prepared.intent is not None
    assert prepared.execution_id is not None
    broker.submit_intent(
        prepared.intent,
        execution_id=prepared.execution_id,
        decision_time=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote=prepared.quote.context,
    )
    with sqlite3.connect(broker_path) as connection:
        connection.execute(
            "UPDATE broker_account SET cash = cash + 1 WHERE account_id = ?",
            (ACCOUNT_ID,),
        )

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=signal.expires_at + timedelta(seconds=1),
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail(
            "authoritative broker recovery must precede quote refresh"
        ),
        limit=10,
    )

    record = queue.record(signal.signal_id)
    assert summary.completed_count == 0
    assert summary.failed_count == 1
    assert record is not None and record.status is PaperSignalQueueStatus.PREPARED
    assert record.order is None
    assert record.last_error is not None
    assert "reconcile" in record.last_error or "cash" in record.last_error


def test_prepared_recovery_rejects_execution_identity_from_another_intent(
    tmp_path: Path,
) -> None:
    queue_path = tmp_path / "queue.sqlite3"
    queue = PaperSignalQueueStore(queue_path, policy=_policy())
    broker = _broker(tmp_path / "broker.sqlite3")
    first_signal = _signal(seed="1")
    queue.ingest(first_signal, received_at=first_signal.available_at)
    first_prepared = queue.prepare(
        first_signal.signal_id,
        quote=_quote(),
        prepared_at=EXECUTION_TIME,
    )
    assert first_prepared.intent is not None
    assert first_prepared.execution_id is not None
    first_order = broker.submit_intent(
        first_prepared.intent,
        execution_id=first_prepared.execution_id,
        decision_time=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote=first_prepared.quote.context,
    )
    queue.complete(
        first_signal.signal_id,
        order=first_order,
        completed_at=EXECUTION_TIME,
    )

    second_signal = _signal(seed="2")
    queue.ingest(second_signal, received_at=second_signal.available_at)
    queue.prepare(
        second_signal.signal_id,
        quote=_quote(),
        prepared_at=EXECUTION_TIME,
    )
    with sqlite3.connect(queue_path) as connection:
        connection.execute(
            "DELETE FROM paper_signal_queue WHERE signal_id = ?",
            (first_signal.signal_id,),
        )
        connection.execute(
            "UPDATE paper_signal_queue SET execution_id = ? WHERE signal_id = ?",
            (first_prepared.execution_id, second_signal.signal_id),
        )

    summary = run_paper_signal_batch(
        queue,
        broker,
        now=EXECUTION_TIME,
        trade_date=TRADE_DATE,
        quote_resolver=lambda *_args: pytest.fail(
            "identity conflict must fail before quote refresh"
        ),
        limit=10,
    )

    record = queue.record(second_signal.signal_id)
    assert summary.completed_count == 0
    assert summary.failed_count == 1
    assert record is not None and record.status is PaperSignalQueueStatus.PREPARED
    assert record.order is None
    assert record.last_error is not None
    assert "identity" in record.last_error or "intent" in record.last_error
