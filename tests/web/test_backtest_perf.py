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
