"""Backtest perf block: hand-checked on a tiny ledger."""

from __future__ import annotations

import pytest

from rquant.web.backtest_perf import backtest_perf, daily_returns


def _t(day: str, ret: float) -> dict:
    return {"exit_time": f"{day} 14:55:00", "ret_pct": ret}


def test_trades_on_one_day_are_averaged_then_compounded() -> None:
    perf = backtest_perf([_t("2026-01-05", 10), _t("2026-01-05", -2), _t("2026-01-06", -10)])
    assert perf is not None
    assert list(daily_returns([_t("2026-01-05", 10), _t("2026-01-05", -2)])) == [0.04]
    assert perf.total_return == pytest.approx(1.04 * 0.9 - 1)
    assert perf.max_drawdown == pytest.approx(-0.1)
    assert [p.nav for p in perf.nav] == pytest.approx([1.04, 0.936])
    assert [(m.year, m.month) for m in perf.monthly] == [(2026, 1)]


def test_open_or_empty_ledger_has_no_perf() -> None:
    assert backtest_perf([{"exit_time": None, "ret_pct": 1.0}]) is None


def test_benchmark_excess_on_shared_days() -> None:
    closes = [(5, 100), (6, 101), (7, 99.99)]
    bench = [{"trade_date": f"2026-01-0{d}", "close": c} for d, c in closes]
    perf = backtest_perf([_t("2026-01-06", 2), _t("2026-01-07", 0)], ("000300.SH", bench))
    assert perf is not None and perf.benchmark is not None
    assert perf.benchmark.total_return == pytest.approx(-0.0001)
    assert perf.benchmark.excess_return == pytest.approx(1.02 / 0.9999 - 1)
    assert perf.nav[-1].benchmark_nav == pytest.approx(0.9999)
