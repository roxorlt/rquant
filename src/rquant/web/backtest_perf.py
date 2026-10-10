"""Performance block for a published backtest, derived from its trade ledger.

The published ``strategy_trade`` rows are per-trade returns, not a portfolio NAV.
We use one transparent approximation: each exit day's return is the equal-weight
mean of the trades closed that day (full capital rotates into those trades).
The page labels it as such; a real portfolio NAV arrives with the portfolio
backtest module.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from rquant.perf import equity_curve, monthly_returns, performance_summary, relative_metrics
from rquant.web.models import BacktestPerf, BenchmarkStats, MonthlyReturn, NavPoint

METHOD = "逐笔等权复利（按卖出日聚合，近似）"


def daily_returns(trades: Iterable[dict[str, Any]]) -> pd.Series:
    rows = [
        (pd.Timestamp(t["exit_time"]).normalize(), float(t["ret_pct"]) / 100)
        for t in trades
        if t.get("exit_time") is not None and t.get("ret_pct") is not None
    ]
    if not rows:
        return pd.Series(dtype="float64", index=pd.DatetimeIndex([]))
    frame = pd.DataFrame(rows, columns=["day", "ret"])
    series = frame.groupby("day")["ret"].mean().sort_index()
    series.index = pd.DatetimeIndex(series.index).tz_localize(None)
    return series.clip(lower=-1.0)


def _num(value: float | None) -> float | None:
    return None if value is None or pd.isna(value) else float(value)


def benchmark_returns(rows: Iterable[dict[str, Any]]) -> pd.Series:
    """Daily close-to-close returns of one benchmark from ``benchmark_daily`` rows."""
    closes = pd.Series(
        {pd.Timestamp(r["trade_date"]): float(r["close"]) for r in rows}, dtype="float64"
    ).sort_index()
    return closes.pct_change().dropna()


def backtest_perf(
    trades: Iterable[dict[str, Any]],
    benchmark: tuple[str, Iterable[dict[str, Any]]] | None = None,
) -> BacktestPerf | None:
    return perf_from_returns(daily_returns(trades), benchmark)


def perf_from_returns(
    returns: pd.Series,
    benchmark: tuple[str, Iterable[dict[str, Any]]] | None = None,
    method: str = METHOD,
) -> BacktestPerf | None:
    if returns.empty:
        return None
    bench_stats: BenchmarkStats | None = None
    bench_nav: pd.Series | None = None
    if benchmark is not None:
        code, rows = benchmark
        bench = benchmark_returns(rows)
        window = bench.loc[returns.index.min() : returns.index.max()]
        if len(window) >= 2:
            # A trade-ledger NAV is flat on days nothing closed.
            returns = returns.reindex(window.index.union(returns.index), fill_value=0.0)
            rel = relative_metrics(returns, window)
            bench_nav = (1 + window).cumprod()
            bench_stats = BenchmarkStats(
                code=code,
                total_return=float(bench_nav.iloc[-1] - 1),
                excess_return=_num(rel.excess_total_return),
                alpha=_num(rel.alpha),
                beta=_num(rel.beta),
                information_ratio=_num(rel.information_ratio),
            )
    summary = performance_summary(returns)
    curve = equity_curve(returns)
    nav = [
        NavPoint(
            date=day.date(),
            nav=float(n),
            drawdown=float(d),
            benchmark_nav=(
                None if bench_nav is None or day not in bench_nav.index
                else float(bench_nav.loc[day])
            ),
        )
        for day, n, d in zip(curve.nav.index, curve.nav, curve.drawdown, strict=True)
    ]
    months = monthly_returns(returns)
    monthly = [
        MonthlyReturn(year=int(year), month=int(month), ret=float(value))
        for year, row in months.iterrows()
        for month, value in row.items()
        if not pd.isna(value)
    ]
    return BacktestPerf(
        method=method,
        days=summary.observations,
        total_return=_num(summary.total_return),
        annualized_return=_num(summary.annualized_return),
        annualized_volatility=_num(summary.annualized_volatility),
        sharpe=_num(summary.sharpe),
        sortino=_num(summary.sortino),
        calmar=_num(summary.calmar),
        max_drawdown=_num(summary.max_drawdown),
        max_drawdown_days=summary.max_drawdown_duration,
        win_rate=_num(summary.win_rate),
        payoff_ratio=_num(summary.payoff_ratio),
        nav=nav,
        monthly=monthly,
        benchmark=bench_stats,
    )
