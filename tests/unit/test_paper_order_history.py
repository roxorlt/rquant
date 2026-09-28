"""Trusted, bounded paper order history from a single SQLite snapshot."""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import rquant.paper_broker as broker_module
from rquant.paper_broker import PaperBrokerReconciliationError
from rquant.paper_contracts import PaperOrderStatus, PaperOrderType, PaperRejectReason
from tests.paper_cost_fixtures import paper_cost_policy
from tests.unit.test_paper_broker import (
    BUY_DATE,
    BUY_TIME,
    _intent,
    _quote,
    _store,
)


def test_empty_history_is_a_trusted_window(tmp_path: Path) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())

    snapshot = store.recent_order_history(as_of=BUY_TIME)

    assert snapshot.account_id == store.account_id
    assert snapshot.as_of == BUY_TIME
    assert snapshot.ledger_revision >= 1
    assert snapshot.total_orders == 0
    assert snapshot.has_more is False
    assert snapshot.orders == ()
    assert snapshot.fills == ()

    store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    changed = store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))
    assert changed.ledger_revision > snapshot.ledger_revision


def test_window_keeps_all_fills_and_rejects_without_inventing_trades(tmp_path: Path) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=400),
    )
    store.apply_execution(
        partial.order_id,
        execution_id="9" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=600,
        quote=_quote("11.00", executable_quantity=600),
        price_snapshot_id="9" * 64,
    )
    rejected = store.submit_intent(
        _intent(signal_seed="d", event_time=BUY_TIME + timedelta(minutes=2)),
        decision_time=BUY_TIME + timedelta(minutes=2, seconds=2),
        trade_date=BUY_DATE,
        quote=_quote("10.00", suspended=True),
    )

    snapshot = store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=3))

    assert snapshot.total_orders == 2
    assert [row.order_id for row in snapshot.orders] == [rejected.order_id, partial.order_id]
    assert snapshot.orders[0].status is PaperOrderStatus.REJECTED
    assert snapshot.orders[0].reject_reason is PaperRejectReason.SUSPENDED
    assert snapshot.orders[1].average_fill_price == Decimal("10.6000")
    assert [(fill.order_id, fill.sequence) for fill in snapshot.fills] == [
        (partial.order_id, 1),
        (partial.order_id, 2),
    ]
    assert [fill.persisted_at for fill in snapshot.fills] == [
        BUY_TIME,
        BUY_TIME + timedelta(minutes=1),
    ]


def test_recent_window_reports_true_total_and_truncation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    monkeypatch.setattr(broker_module, "_MAX_HISTORY_ORDERS", 2)
    orders = [
        store.submit_intent(
            _intent(
                signal_seed=seed,
                order_type=PaperOrderType.LIMIT,
                limit_price=Decimal("9.90"),
                event_time=BUY_TIME + timedelta(minutes=index),
            ),
            decision_time=BUY_TIME + timedelta(minutes=index, seconds=2),
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )
        for index, seed in enumerate("abc")
    ]

    snapshot = store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=3))

    assert snapshot.total_orders == 3
    assert snapshot.has_more is True
    assert [row.order_id for row in snapshot.orders] == [orders[2].order_id, orders[1].order_id]
    assert snapshot.fills == ()


def test_overfull_fill_window_fails_instead_of_publishing_partial_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path / "paper.sqlite3", paper_cost_policy())
    partial = store.submit_intent(
        _intent(quantity=1_000),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", executable_quantity=400),
    )
    store.apply_execution(
        partial.order_id,
        execution_id="9" * 64,
        executed_at=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quantity=600,
        quote=_quote("11.00", executable_quantity=600),
        price_snapshot_id="9" * 64,
    )
    monkeypatch.setattr(broker_module, "_MAX_HISTORY_FILLS", 1)

    with pytest.raises(PaperBrokerReconciliationError, match="fill.*capacity"):
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=2))


def test_tampered_order_is_not_read_as_valid_history(tmp_path: Path) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    store.submit_intent(
        _intent(), decision_time=BUY_TIME, trade_date=BUY_DATE, quote=_quote("10.00")
    )
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE paper_order SET status = 'PENDING'")

    with pytest.raises(PaperBrokerReconciliationError):
        store.recent_order_history(as_of=BUY_TIME + timedelta(minutes=1))


def test_writer_commit_during_history_read_does_not_split_count_and_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, paper_cost_policy())
    writer = _store(path, paper_cost_policy())
    original_connect = store._connect
    committed = False

    def interleaved_connect() -> sqlite3.Connection:
        nonlocal committed
        connection = original_connect()

        def on_query(statement: str) -> None:
            nonlocal committed
            if committed or not statement.startswith("SELECT count(*) FROM paper_order"):
                return
            committed = True
            writer.submit_intent(
                _intent(
                    order_type=PaperOrderType.LIMIT,
                    limit_price=Decimal("9.90"),
                    event_time=BUY_TIME + timedelta(minutes=1),
                ),
                decision_time=BUY_TIME + timedelta(minutes=1, seconds=2),
                trade_date=BUY_DATE,
                quote=_quote("10.00"),
            )

        connection.set_trace_callback(on_query)
        return connection

    monkeypatch.setattr(store, "_connect", interleaved_connect)
    cutoff = BUY_TIME + timedelta(minutes=2)

    first = store.recent_order_history(as_of=cutoff)
    after = writer.recent_order_history(as_of=cutoff)

    assert committed is True
    assert first.total_orders == 0
    assert first.orders == ()
    assert after.total_orders == 1
