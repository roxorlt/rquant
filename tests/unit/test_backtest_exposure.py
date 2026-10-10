from __future__ import annotations

from datetime import date

import pytest

from rquant.backtest import BacktestConfig, Bar, run_backtest
from rquant.backtest.exposure import industry_exposure
from rquant.backtest.store import save
from rquant.portfolio import PortfolioCandidate, PortfolioWeightRule


def test_exposure_vs_pool(tmp_path) -> None:
    d1, d2 = date(2026, 1, 5), date(2026, 1, 6)
    flat = Bar(open=10, close=10, pre_close=10)
    bars = {d: {"A": flat, "B": flat, "C": flat} for d in (d1, d2)}
    signals = {d1: [PortfolioCandidate(ts_code=c, rank_score=s)
                    for c, s in (("A", 3), ("B", 2), ("C", 1))]}
    cfg = BacktestConfig(capital=10_000, weights=PortfolioWeightRule(
        max_positions=1, method="rank_score"))
    run = save(run_backtest([d1, d2], bars, signals, cfg), preset="p", start=d1, end=d2,
               root=tmp_path, industries={"A": "银行", "B": "银行", "C": "白酒"},
               pool_last=["A", "B", "C"])
    rows = {r.industry: r for r in industry_exposure(run)}
    assert rows["银行"].weight == pytest.approx(0.9, abs=0.01)  # 900 shares after fees
    assert rows["银行"].pool_weight == pytest.approx(2 / 3, abs=1e-5)
    assert rows["白酒"].deviation == pytest.approx(-1 / 3, abs=1e-5)
