from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.paper_broker import (
    BrokerCostPolicy,
    BrokerExecutionContext,
    DuplicateIntentConflictError,
    PaperBrokerReconciliationError,
    PaperBrokerStore,
)
from rquant.paper_contracts import (
    PaperOrderIntent,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectReason,
    PaperSide,
)

ACCOUNT_ID = "paper-main"
BUY_TIME = datetime(2026, 7, 31, 1, 31, tzinfo=UTC)
BUY_DATE = date(2026, 7, 31)
NEXT_TRADE_DATE = date(2026, 8, 3)
PRICE_SNAPSHOT_ID = "b" * 64
PRODUCER_COMMIT = "c" * 40


@pytest.fixture
def cost_policy() -> BrokerCostPolicy:
    return BrokerCostPolicy(
        commission_rate=Decimal("0.0003"),
        minimum_commission=Decimal("5.00"),
        sell_stamp_tax_rate=Decimal("0.001"),
        buy_slippage_bps=Decimal("0"),
        sell_slippage_bps=Decimal("0"),
    )


def _intent(
    *,
    side: PaperSide = PaperSide.BUY,
    quantity: int = 1_000,
    order_type: PaperOrderType = PaperOrderType.MARKET,
    limit_price: Decimal | None = None,
    signal_seed: str = "a",
    event_time: datetime = BUY_TIME - timedelta(seconds=2),
) -> PaperOrderIntent:
    return PaperOrderIntent(
        signal_id=signal_seed * 64,
        account_id=ACCOUNT_ID,
        ts_code="600000.SH",
        side=side,
        order_type=order_type,
        quantity=quantity,
        limit_price=limit_price,
        event_time=event_time,
        available_at=event_time + timedelta(seconds=1),
        expires_at=event_time + timedelta(minutes=5),
        earliest_execution_at=event_time + timedelta(seconds=1),
        price_snapshot_id=PRICE_SNAPSHOT_ID,
        producer_commit=PRODUCER_COMMIT,
    )


def _quote(
    price: str,
    *,
    available_date: date | None = NEXT_TRADE_DATE,
    suspended: bool = False,
    limit_locked: bool = False,
    risk_rejected: bool = False,
) -> BrokerExecutionContext:
    return BrokerExecutionContext(
        executable_price=Decimal(price),
        acquisition_available_date=available_date,
        suspended=suspended,
        limit_locked=limit_locked,
        risk_rejected=risk_rejected,
    )


def _store(
    path: Path,
    cost_policy: BrokerCostPolicy,
    *,
    initial_cash: str = "100000",
) -> PaperBrokerStore:
    return PaperBrokerStore(
        path,
        account_id=ACCOUNT_ID,
        initial_cash=Decimal(initial_cash),
        cost_policy=cost_policy,
    )


def test_buy_fills_atomically_and_accounts_for_minimum_commission(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)

    order = store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    snapshot = store.account_snapshot(
        as_of=BUY_TIME,
        market_prices={"600000.SH": Decimal("10.20")},
    )

    assert order.status is PaperOrderStatus.FILLED
    assert order.average_fill_price == Decimal("10.0000")
    assert len(store.fills(order.order_id)) == 1
    assert store.fills(order.order_id)[0].commission == Decimal("5.00")
    assert snapshot.cash == Decimal("89995.00")
    assert snapshot.holdings[0].quantity == 1_000
    assert snapshot.holdings[0].available_quantity == 0
    assert snapshot.holdings[0].frozen_quantity == 1_000
    assert snapshot.holdings[0].average_cost == Decimal("10.005")
    assert snapshot.unrealized_pnl == Decimal("195.000")
    assert snapshot.nav == Decimal("100195.00")


def test_retry_after_process_reopen_is_idempotent(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    path = tmp_path / "paper.sqlite3"
    intent = _intent()
    first = _store(path, cost_policy).submit_intent(
        intent,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    reopened = _store(path, cost_policy)
    second = reopened.submit_intent(
        intent,
        decision_time=BUY_TIME + timedelta(seconds=10),
        trade_date=BUY_DATE,
        quote=_quote("99.00"),
    )

    assert second == first
    assert len(reopened.fills(first.order_id)) == 1
    assert reopened.account_snapshot(
        as_of=BUY_TIME,
        market_prices={"600000.SH": Decimal("10.00")},
    ).cash == Decimal("89995.00")


def test_same_day_sell_is_rejected_by_t_plus_one(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    order = store.submit_intent(
        _intent(side=PaperSide.SELL, signal_seed="d"),
        decision_time=BUY_TIME + timedelta(minutes=1),
        trade_date=BUY_DATE,
        quote=_quote("10.50", available_date=None),
    )

    assert order.status is PaperOrderStatus.REJECTED
    assert order.reject_reason is PaperRejectReason.T_PLUS_ONE
    assert store.fills(order.order_id) == ()


def test_next_trading_date_sell_fills_and_realizes_pnl(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)

    order = store.submit_intent(
        _intent(
            side=PaperSide.SELL,
            signal_seed="d",
            event_time=sell_time - timedelta(seconds=2),
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("11.00", available_date=None),
    )
    snapshot = store.account_snapshot(as_of=sell_time, market_prices={})

    assert order.status is PaperOrderStatus.FILLED
    assert store.fills(order.order_id)[0].tax == Decimal("11.00")
    assert snapshot.cash == Decimal("100979.00")
    assert snapshot.realized_pnl == Decimal("979.000")
    assert snapshot.holdings == ()
    assert snapshot.nav == Decimal("100979.00")


def test_insufficient_cash_and_position_are_explicit_rejections(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy, initial_cash="1000")

    buy = store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    sell = store.submit_intent(
        _intent(side=PaperSide.SELL, signal_seed="d"),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00", available_date=None),
    )

    assert buy.reject_reason is PaperRejectReason.INSUFFICIENT_CASH
    assert sell.reject_reason is PaperRejectReason.INSUFFICIENT_POSITION
    assert store.account_snapshot(as_of=BUY_TIME, market_prices={}).cash == Decimal("1000")


def test_limit_price_miss_remains_accepted_without_reserving_cash(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)

    order = store.submit_intent(
        _intent(order_type=PaperOrderType.LIMIT, limit_price=Decimal("9.90")),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    assert order.status is PaperOrderStatus.ACCEPTED
    assert order.filled_quantity == 0
    assert store.fills(order.order_id) == ()
    assert store.account_snapshot(as_of=BUY_TIME, market_prices={}).cash == Decimal("100000")


@pytest.mark.parametrize(
    ("quote", "reason"),
    [
        (_quote("10.00", suspended=True), PaperRejectReason.SUSPENDED),
        (_quote("10.00", limit_locked=True), PaperRejectReason.LIMIT_LOCKED),
        (_quote("10.00", risk_rejected=True), PaperRejectReason.RISK_REJECTED),
    ],
)
def test_market_constraints_are_persisted_as_rejections(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
    quote: BrokerExecutionContext,
    reason: PaperRejectReason,
) -> None:
    store = _store(tmp_path / f"{reason.value}.sqlite3", cost_policy)

    order = store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=quote,
    )

    assert order.status is PaperOrderStatus.REJECTED
    assert order.reject_reason is reason


def test_same_intent_id_with_different_payload_is_a_conflict(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    intent = _intent()
    store.submit_intent(
        intent,
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    conflicting = PaperOrderIntent.model_construct(
        **{**intent.model_dump(mode="python"), "quantity": 2_000}
    )

    with pytest.raises(DuplicateIntentConflictError, match="intent_id"):
        store.submit_intent(
            conflicting,
            decision_time=BUY_TIME,
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )


def test_injected_failure_rolls_back_intent_order_fill_cash_and_lot(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    intent = _intent()

    def explode(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("injected failure")

    monkeypatch.setattr(store, "_before_commit", explode)
    with pytest.raises(RuntimeError, match="injected failure"):
        store.submit_intent(
            intent,
            decision_time=BUY_TIME,
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )

    reopened = _store(path, cost_policy)
    assert reopened.order_for_intent(intent.intent_id) is None
    assert reopened.fills() == ()
    assert reopened.account_snapshot(as_of=BUY_TIME, market_prices={}).cash == Decimal("100000")


def test_reconcile_independently_checks_end_of_day_ledger(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    sell_time = datetime(2026, 8, 3, 1, 31, tzinfo=UTC)
    store.submit_intent(
        _intent(
            side=PaperSide.SELL,
            signal_seed="d",
            event_time=sell_time - timedelta(seconds=2),
        ),
        decision_time=sell_time,
        trade_date=NEXT_TRADE_DATE,
        quote=_quote("11.00", available_date=None),
    )

    report = store.reconcile()

    assert report.is_consistent is True
    assert report.order_count == 2
    assert report.fill_count == 2
    assert report.open_lot_quantity == 0
    assert report.cash == Decimal("100979.00")
    assert report.realized_pnl == Decimal("979.000")


def test_reconcile_detects_a_missing_buy_lot(tmp_path: Path, cost_policy: BrokerCostPolicy) -> None:
    path = tmp_path / "paper.sqlite3"
    store = _store(path, cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM paper_lot")

    with pytest.raises(PaperBrokerReconciliationError, match="buy fill.*lot"):
        store.reconcile()


def test_store_initialization_is_idempotent_and_enables_wal(
    tmp_path: Path, cost_policy: BrokerCostPolicy
) -> None:
    path = tmp_path / "paper.sqlite3"
    first = _store(path, cost_policy)
    second = _store(path, cost_policy)

    assert first.account_snapshot(as_of=BUY_TIME, market_prices={}) == second.account_snapshot(
        as_of=BUY_TIME,
        market_prices={},
    )
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        money_types = connection.execute(
            "SELECT typeof(initial_cash), typeof(cash), typeof(realized_pnl) FROM broker_account"
        ).fetchone()
    assert money_types == ("text", "text", "text")


def test_reopen_rejects_cost_policy_drift(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    path = tmp_path / "paper.sqlite3"
    _store(path, cost_policy)
    changed = cost_policy.model_copy(update={"commission_rate": Decimal("0.0005")})

    with pytest.raises(ValueError, match="cost_policy"):
        _store(path, changed)


def test_submit_rejects_previsible_decision_and_mismatched_trade_date(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    intent = _intent()

    with pytest.raises(ValueError, match="available_at"):
        store.submit_intent(
            intent,
            decision_time=intent.available_at - timedelta(microseconds=1),
            trade_date=BUY_DATE,
            quote=_quote("10.00"),
        )
    with pytest.raises(ValueError, match="trade_date"):
        store.submit_intent(
            intent,
            decision_time=BUY_TIME,
            trade_date=NEXT_TRADE_DATE,
            quote=_quote("10.00"),
        )


def test_account_snapshot_rejects_as_of_before_latest_ledger_event(
    tmp_path: Path,
    cost_policy: BrokerCostPolicy,
) -> None:
    store = _store(tmp_path / "paper.sqlite3", cost_policy)
    store.submit_intent(
        _intent(),
        decision_time=BUY_TIME,
        trade_date=BUY_DATE,
        quote=_quote("10.00"),
    )

    with pytest.raises(ValueError, match="as_of.*latest ledger event"):
        store.account_snapshot(
            as_of=BUY_TIME - timedelta(seconds=1),
            market_prices={"600000.SH": Decimal("10.00")},
        )
