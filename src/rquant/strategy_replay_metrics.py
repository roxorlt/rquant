"""Pure summary tables shared by research adapters and dashboard callers."""

from __future__ import annotations

import pandas as pd


def _pct_mean(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return round(float(clean.mean()), 4)


def _pct_median(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return round(float(clean.median()), 4)


def _win_rate(series: pd.Series) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None
    return round(float((clean > 0).mean() * 100), 4)


def _bool_rate(series: pd.Series) -> float | None:
    clean = series.dropna()
    if clean.empty:
        return None
    return round(float(clean.astype(bool).mean() * 100), 4)


def auction_gap_metric_rows(
    baseline: pd.DataFrame,
    minute_trades: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize auction baseline and minute replay results."""
    candidates_count = len(baseline)
    baseline_ret = (
        baseline["next_open_ret_pct"]
        if "next_open_ret_pct" in baseline.columns
        else pd.Series(dtype=float)
    )
    minute_ret = (
        minute_trades["ret_pct"] if "ret_pct" in minute_trades.columns else pd.Series(dtype=float)
    )
    baseline_hit = (
        baseline["hit_limit_up_today"]
        if "hit_limit_up_today" in baseline.columns
        else pd.Series(dtype=bool)
    )
    baseline_high_ret = (
        baseline["intraday_high_ret_pct"]
        if "intraday_high_ret_pct" in baseline.columns
        else pd.Series(dtype=float)
    )
    baseline_close_ret = (
        baseline["day_close_ret_pct"]
        if "day_close_ret_pct" in baseline.columns
        else pd.Series(dtype=float)
    )
    minute_hit = (
        minute_trades["b_hit_limit_up_today"]
        if "b_hit_limit_up_today" in minute_trades.columns
        else pd.Series(dtype=bool)
    )
    weak_exit_rate = None
    if not minute_trades.empty and "exit_reason" in minute_trades.columns:
        weak_exit_rate = round(
            float(minute_trades["exit_reason"].fillna("").eq("next_auction_weak").mean() * 100),
            4,
        )
    rows = pd.DataFrame(
        [
            {
                "策略": "竞价直接B/次日开盘S",
                "候选": candidates_count,
                "交易": candidates_count,
                "触发率%": 100.0 if candidates_count else None,
                "当日上板率%": _bool_rate(baseline_hit),
                "当日最高均值%": _pct_mean(baseline_high_ret),
                "当日收盘均值%": _pct_mean(baseline_close_ret),
                "平均收益%": _pct_mean(baseline_ret),
                "中位收益%": _pct_median(baseline_ret),
                "胜率%": _win_rate(baseline_ret),
                "弱竞价退出%": None,
            },
            {
                "策略": "竞价候选/分钟B/S",
                "候选": candidates_count,
                "交易": len(minute_trades),
                "触发率%": round(len(minute_trades) / candidates_count * 100, 4)
                if candidates_count
                else None,
                "当日上板率%": _bool_rate(minute_hit),
                "当日最高均值%": None,
                "当日收盘均值%": None,
                "平均收益%": _pct_mean(minute_ret),
                "中位收益%": _pct_median(minute_ret),
                "胜率%": _win_rate(minute_ret),
                "弱竞价退出%": weak_exit_rate,
            },
        ]
    )
    metric_columns = rows.columns.difference(["策略", "候选", "交易"], sort=False)
    return rows.astype({column: "float64" for column in metric_columns})


def growth_board_metric_rows(
    trades: pd.DataFrame,
    *,
    strategy_name: str = "科创/创业放量追击",
) -> pd.DataFrame:
    """Summarize one growth-board minute replay result."""
    ret = trades["ret_pct"] if "ret_pct" in trades.columns else pd.Series(dtype=float)
    hit = (
        trades["hit_limit_up_today"]
        if "hit_limit_up_today" in trades.columns
        else pd.Series(dtype=bool)
    )
    rows = pd.DataFrame(
        [
            {
                "策略": strategy_name,
                "交易": len(trades),
                "当日上板率%": _bool_rate(hit),
                "平均收益%": _pct_mean(ret),
                "中位收益%": _pct_median(ret),
                "胜率%": _win_rate(ret),
            }
        ]
    )
    metric_columns = rows.columns.difference(["策略", "交易"], sort=False)
    return rows.astype({column: "float64" for column in metric_columns})
