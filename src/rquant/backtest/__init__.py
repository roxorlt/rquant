"""Offline daily portfolio replay using the shared paper execution ledger."""

from rquant.backtest.benchmark import (
    BenchmarkComparison,
    BenchmarkDay,
    BenchmarkSeries,
    BenchmarkSourceError,
    compare_backtest_to_benchmark,
    load_benchmark_series,
)
from rquant.backtest.contracts import (
    BacktestDayInput,
    BacktestDayResult,
    BacktestDecision,
    BacktestInstrument,
    BacktestOrder,
    BacktestRequest,
    BacktestResult,
    RankingSnapshot,
    RebalanceRule,
    SkippedTarget,
    SSECalendar,
    TradeConditions,
)
from rquant.backtest.report import BacktestHtmlReport, render_backtest_html
from rquant.backtest.runner import run_portfolio_backtest

__all__ = [
    "BacktestDayInput",
    "BacktestDayResult",
    "BacktestDecision",
    "BacktestInstrument",
    "BacktestHtmlReport",
    "BacktestOrder",
    "BacktestRequest",
    "BacktestResult",
    "BenchmarkComparison",
    "BenchmarkDay",
    "BenchmarkSeries",
    "BenchmarkSourceError",
    "RankingSnapshot",
    "RebalanceRule",
    "SSECalendar",
    "SkippedTarget",
    "TradeConditions",
    "compare_backtest_to_benchmark",
    "load_benchmark_series",
    "render_backtest_html",
    "run_portfolio_backtest",
]
