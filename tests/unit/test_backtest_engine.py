"""Hand-checked daily portfolio backtest cases."""

from __future__ import annotations

from datetime import date

import pytest

from rquant.backtest import BacktestConfig, Bar, CostModel, run_backtest
from rquant.portfolio import PortfolioCandidate, PortfolioWeightRule

D1, D2, D3 = date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)
FREE = CostModel(commission_rate=0, commission_min=0, stamp_tax_sell=0, transfer_rate=0)


def _cfg(n: int = 1, costs: CostModel = FREE) -> BacktestConfig:
    return BacktestConfig(capital=10_000, weights=PortfolioWeightRule(max_positions=n), costs=costs)


def _c(code: str) -> PortfolioCandidate:
    return PortfolioCandidate(ts_code=code)


def test_signal_buys_next_open_in_whole_lots_and_marks_to_close() -> None:
    bars = {D1: {"A": Bar(open=10, close=10, pre_close=10)},
            D2: {"A": Bar(open=10.5, close=11, pre_close=10)}}
    result = run_backtest([D1, D2], bars, {D1: [_c("A")]}, _cfg())
    buy = result.orders[0]
    assert (buy.trade_date, buy.quantity, buy.price) == (D2, 900, 10.5)  # 10000/10.5 → 952 → 900
    assert result.days[1].nav == pytest.approx((10_000 - 9450 + 900 * 11) / 10_000)


def test_limit_up_open_is_rejected_and_limit_down_blocks_selling() -> None:
    bars = {D1: {"A": Bar(open=10, close=10, pre_close=10)},
            D2: {"A": Bar(open=11, close=11, pre_close=10)}}
    result = run_backtest([D1, D2], bars, {D1: [_c("A")]}, _cfg())
    assert result.orders[0].reason == "涨停开盘"
    bars = {D1: {"A": Bar(open=10, close=10, pre_close=10), "B": Bar(open=5, close=5, pre_close=5)},
            D2: {"A": Bar(open=10, close=10, pre_close=10), "B": Bar(open=5, close=5, pre_close=5)},
            D3: {"A": Bar(open=9, close=9, pre_close=10), "B": Bar(open=5, close=5, pre_close=5)}}
    result = run_backtest([D1, D2, D3], bars, {D1: [_c("A")], D2: [_c("B")]}, _cfg())
    sell = [o for o in result.orders if o.side == "sell"][0]
    assert (sell.trade_date, sell.reason) == (D3, "跌停开盘")


def test_suspended_stock_is_not_traded() -> None:
    bars = {D1: {"A": Bar(open=10, close=10, pre_close=10)}, D2: {}}
    result = run_backtest([D1, D2], bars, {D1: [_c("A")]}, _cfg())
    assert result.orders[0].reason == "停牌"
    assert result.days[-1].nav == 1.0


def test_costs_follow_the_model() -> None:
    costs = CostModel()
    assert costs.fee("buy", 10_000) == 5.1  # min commission 5 + transfer 0.1
    assert costs.fee("sell", 100_000) == pytest.approx(25 + 50 + 1)


def test_rebalance_every_n_signals() -> None:
    flat = Bar(open=10, close=10, pre_close=10)
    bars = {d: {"A": flat, "B": flat} for d in (D1, D2, D3)}
    result = run_backtest([D1, D2, D3], bars, {D1: [_c("A")], D2: [_c("B")]},
                          _cfg().model_copy(update={"rebalance_every": 2}))
    assert {o.ts_code for o in result.orders} == {"A"}
