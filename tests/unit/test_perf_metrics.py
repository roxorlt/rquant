"""C7.1 offline, hand-checkable performance contracts."""

from __future__ import annotations

import json
import math
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from rquant.perf import (
    Fill,
    annualized_turnover,
    build_round_trips,
    equity_curve,
    monthly_returns,
    performance_summary,
    relative_metrics,
    return_distribution,
    rolling_metrics,
    streaks,
    summarize_round_trips,
)


def _returns(values: list[float], dates: list[str] | None = None) -> pd.Series:
    dates = dates or [f"2026-01-{day:02d}" for day in range(2, 2 + len(values))]
    return pd.Series(values, index=pd.to_datetime(dates), dtype="float64")


def test_equity_curve_includes_initial_capital_in_drawdown_peak() -> None:
    curve = equity_curve(_returns([-0.1, 0.2, -0.25, 0.5]))
    assert curve.nav.tolist() == pytest.approx([0.9, 1.08, 0.81, 1.215])
    assert curve.drawdown.tolist() == pytest.approx([-0.1, 0.0, -0.25, 0.0])
    assert curve.max_drawdown == pytest.approx(-0.25)
    assert curve.max_drawdown_duration == 1


def test_longest_underwater_duration_counts_observations_until_recovery() -> None:
    curve = equity_curve(_returns([0.1, -0.1, 0.0, 0.1, 0.02]))
    assert curve.max_drawdown == pytest.approx(-0.1)
    assert curve.max_drawdown_duration == 3


def test_summary_matches_frozen_independently_calculated_reference() -> None:
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "perf_reference_v1.json").read_text()
    )
    actual = performance_summary(_returns(fixture["daily_returns"]))
    for field, expected in fixture["summary"].items():
        assert getattr(actual, field) == pytest.approx(expected)


def test_summary_undefined_ratios_and_no_trades_are_none() -> None:
    one = performance_summary(_returns([0.0]))
    assert one.annualized_volatility is None
    assert one.sharpe is None
    assert one.sortino is None
    assert one.calmar is None
    assert one.win_rate is None
    assert one.payoff_ratio is None
    assert performance_summary(_returns([0.1, 0.1])).payoff_ratio is None


def test_summary_uses_effective_daily_risk_free_from_annual_rate() -> None:
    risk_free_annual = 1.01**252 - 1
    summary = performance_summary(_returns([0.01, 0.02, 0.0]), risk_free_annual)
    expected_std = 0.01
    assert summary.sharpe == pytest.approx(0.0, abs=1e-8)
    assert summary.annualized_volatility == pytest.approx(expected_std * math.sqrt(252))


@pytest.mark.parametrize(
    "bad",
    [
        _returns([float("nan")]),
        _returns([float("inf")]),
        _returns([-1.01]),
        pd.Series([0.01, 0.02], index=pd.to_datetime(["2026-01-02"] * 2)),
        _returns([0.01, 0.02], ["2026-01-03", "2026-01-02"]),
    ],
)
def test_invalid_return_evidence_is_rejected(bad: pd.Series) -> None:
    with pytest.raises(ValueError):
        performance_summary(bad)


def test_monthly_matrix_compounds_actual_observations_and_leaves_missing_months_blank() -> None:
    matrix = monthly_returns(_returns([0.1, -0.1, 0.2], ["2025-12-30", "2025-12-31", "2026-02-02"]))
    assert matrix.loc[2025, 12] == pytest.approx(-0.01)
    assert math.isnan(matrix.loc[2026, 1])
    assert matrix.loc[2026, 2] == pytest.approx(0.2)


def test_rolling_metrics_have_no_partial_window() -> None:
    output = rolling_metrics(_returns([0.1, -0.1, 0.0]), window=2)
    assert math.isnan(output.iloc[0]["volatility"])
    assert math.isnan(output.iloc[0]["sharpe"])
    assert output.iloc[1]["volatility"] == pytest.approx(0.1 * math.sqrt(2) * math.sqrt(252))
    assert output.iloc[1]["sharpe"] == pytest.approx(0.0)


def test_relative_metrics_align_only_shared_dates_without_forward_fill() -> None:
    strategy = _returns([0.1, 0.0, -0.1], ["2026-01-02", "2026-01-05", "2026-01-06"])
    benchmark = _returns([0.02, 0.03, 0.04], ["2026-01-05", "2026-01-06", "2026-01-07"])
    result = relative_metrics(strategy, benchmark)
    assert result.aligned_observations == 2
    assert result.excess_total_return == pytest.approx(0.9 / (1.02 * 1.03) - 1)
    assert result.beta == pytest.approx(-10.0)
    assert result.alpha == pytest.approx(0.2 * 252)
    assert result.tracking_error == pytest.approx(0.11 / math.sqrt(2) * math.sqrt(252))


def test_relative_metrics_report_undefined_regression_or_information_ratio() -> None:
    strategy = _returns([0.01, 0.02])
    flat_benchmark = _returns([0.0, 0.0])
    result = relative_metrics(strategy, flat_benchmark)
    assert result.alpha is None
    assert result.beta is None
    assert result.tracking_error is not None
    assert relative_metrics(strategy, strategy).information_ratio is None
    assert relative_metrics(strategy, _returns([0.0], ["2026-02-02"])).aligned_observations == 0


def test_annualized_turnover_uses_one_way_traded_value_over_prior_equity() -> None:
    buy = _returns([100.0, 0.0])
    sell = _returns([0.0, 200.0])
    equity = _returns([1000.0, 1000.0])
    assert annualized_turnover(buy, sell, equity) == pytest.approx(37.8)
    with pytest.raises(ValueError, match="dates"):
        annualized_turnover(buy, sell, _returns([1000.0], ["2026-01-02"]))
    with pytest.raises(ValueError, match="equity"):
        annualized_turnover(buy, sell, _returns([0.0, 1000.0]))


def test_fifo_round_trips_allocate_partial_fees_and_group_by_symbol_industry_duration() -> None:
    ledger = build_round_trips(
        [
            Fill(date(2026, 1, 2), "600000.SH", "银行", "buy", 100, 10.0, 1.0),
            Fill(date(2026, 1, 5), "600000.SH", "银行", "sell", 40, 12.0, 0.8),
            Fill(date(2026, 1, 8), "600000.SH", "银行", "sell", 60, 9.0, 1.2),
            Fill(date(2026, 1, 8), "000001.SZ", "银行", "buy", 10, 5.0, 0.0),
        ]
    )
    assert [trip.net_pnl for trip in ledger.closed] == pytest.approx([78.8, -61.8])
    assert [trip.holding_days for trip in ledger.closed] == [3, 6]
    assert ledger.open_quantity == {"000001.SZ": 10.0}
    summary = summarize_round_trips(ledger.closed)
    assert summary.overall.net_pnl == pytest.approx(17.0)
    assert summary.overall.win_rate == pytest.approx(0.5)
    assert summary.by_symbol["600000.SH"].count == 2
    assert summary.by_industry["银行"].count == 2
    assert set(summary.by_holding_days) == {3, 6}


def test_sell_without_position_is_rejected_instead_of_becoming_a_short() -> None:
    with pytest.raises(ValueError, match="open quantity"):
        build_round_trips([Fill(date(2026, 1, 2), "600000.SH", "银行", "sell", 1, 10.0, 0.0)])


def test_round_trips_match_oldest_buy_first_across_two_lots() -> None:
    ledger = build_round_trips(
        [
            Fill(date(2026, 1, 2), "600000.SH", "银行", "buy", 2, 10.0, 2.0),
            Fill(date(2026, 1, 3), "600000.SH", "银行", "buy", 3, 20.0, 3.0),
            Fill(date(2026, 1, 4), "600000.SH", "银行", "sell", 4, 30.0, 4.0),
        ]
    )
    assert [trip.quantity for trip in ledger.closed] == [2, 2]
    assert [trip.net_pnl for trip in ledger.closed] == pytest.approx([36.0, 16.0])
    assert ledger.open_quantity == {"600000.SH": 1}


def test_round_trip_share_quantities_must_be_whole_for_a_shares() -> None:
    with pytest.raises(ValueError, match="whole shares"):
        build_round_trips([Fill(date(2026, 1, 2), "600000.SH", "银行", "buy", 0.5, 10.0, 0.0)])


def test_round_trip_rejects_datetime_that_would_truncate_cross_day_holding() -> None:
    with pytest.raises(ValueError, match="pure date"):
        build_round_trips(
            [
                Fill(datetime(2026, 1, 2, 23, 59), "600000.SH", "银行", "buy", 1, 10.0, 0.0),
                Fill(datetime(2026, 1, 3, 0, 1), "600000.SH", "银行", "sell", 1, 11.0, 0.0),
            ]
        )


def test_distribution_covers_every_return_and_zero_breaks_streaks() -> None:
    returns = _returns([0.1, -0.1, -0.2, 0.0, 0.2, 0.3])
    distribution = return_distribution(returns, edges=[-0.2, 0.0, 0.2, 0.3])
    assert [bucket.count for bucket in distribution.bins] == [2, 2, 2]
    assert distribution.count == 6
    run = streaks(returns)
    assert run.longest_win == 2
    assert run.longest_loss == 2
    assert run.current_win == 2
    assert run.current_loss == 0
    with pytest.raises(ValueError, match="outside"):
        return_distribution(returns, edges=[-0.1, 0.3])
