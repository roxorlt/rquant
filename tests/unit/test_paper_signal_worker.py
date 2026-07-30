from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.paper_broker import BrokerCostPolicy, BrokerExecutionContext, PaperBrokerStore
from rquant.paper_contracts import PaperOrderStatus, PaperRejectReason
from rquant.paper_signal_worker import (
    PaperQuoteSnapshot,
    PaperSignalPolicy,
    PaperSignalQueueStatus,
    PaperSignalQueueStore,
    run_paper_signal_batch,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope

ACCOUNT_ID = "paper-main"
SIGNAL_TIME = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
EXECUTION_TIME = SIGNAL_TIME + timedelta(minutes=1)
TRADE_DATE = date(2026, 7, 31)
NEXT_TRADE_DATE = date(2026, 8, 3)


def _policy(*, buy_quantity: int = 1_000) -> PaperSignalPolicy:
    return PaperSignalPolicy(
        account_id=ACCOUNT_ID,
        execution_lag=timedelta(minutes=1),
        action_quantities={
            SignalAction.B_INTENT: buy_quantity,
            SignalAction.REDUCE: 500,
            SignalAction.S_INTENT: 1_000,
        },
        producer_commit="a" * 40,
    )


def _signal(
    action: SignalAction = SignalAction.B_INTENT,
    *,
    seed: str = "b",
    event_time: datetime = SIGNAL_TIME,
) -> SignalEnvelope:
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
        evidence={},
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
        ),
        producer_commit="f" * 40,
    )


def _broker(path: Path) -> PaperBrokerStore:
    return PaperBrokerStore(
        path,
        account_id=ACCOUNT_ID,
        initial_cash=Decimal("100000"),
        cost_policy=BrokerCostPolicy(
            commission_rate=Decimal("0.0003"),
            minimum_commission=Decimal("5"),
            sell_stamp_tax_rate=Decimal("0.001"),
        ),
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


def test_watch_signal_is_explicitly_ignored_and_policy_is_bound(tmp_path: Path) -> None:
    path = tmp_path / "queue.sqlite3"
    queue = PaperSignalQueueStore(path, policy=_policy())
    signal = _signal(SignalAction.WATCH)

    ignored = queue.ingest(signal, received_at=signal.available_at)

    assert ignored.status is PaperSignalQueueStatus.IGNORED
    assert ignored.last_error == "action watch is not executable by paper broker"
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


def test_prepared_signal_expires_without_losing_frozen_audit_evidence(
    tmp_path: Path,
) -> None:
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite3", policy=_policy())
    signal = _signal()
    queue.ingest(signal, received_at=signal.available_at)
    prepared = queue.prepare(
        signal.signal_id,
        quote=_quote(),
        prepared_at=EXECUTION_TIME,
    )

    due = queue.due_records(now=signal.expires_at, limit=10)
    expired = queue.record(signal.signal_id)

    assert prepared.status is PaperSignalQueueStatus.PREPARED
    assert due == ()
    assert expired is not None and expired.status is PaperSignalQueueStatus.EXPIRED
    assert expired.quote == prepared.quote
    assert expired.intent == prepared.intent
    assert expired.order is None
