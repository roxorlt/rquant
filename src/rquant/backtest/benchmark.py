"""Retrospective index closes for an offline daily portfolio comparison."""

from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Literal, Self

import duckdb
import pandas as pd
from pydantic import Field, model_validator

from rquant.backtest.contracts import BacktestResult, Sha256, SSECalendar
from rquant.perf import PerformanceSummary, RelativeMetrics, performance_summary, relative_metrics
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

SUPPORTED_BENCHMARK_CODES = frozenset(
    {"000001.SH", "399001.SZ", "399006.SZ", "000300.SH", "000905.SH", "000852.SH"}
)
_SOURCE_MODE = "retrospective_daily_bar"
_SOURCE_TABLE = "index_daily_bar"


class BenchmarkSourceError(ValueError):
    """The frozen index facts cannot form an exact comparison series."""


class BenchmarkDay(RuntimeContractModel):
    trade_date: date
    close: float = Field(gt=0, allow_inf_nan=False)
    daily_return: float = Field(gt=-1, allow_inf_nan=False)
    normalized_nav: float = Field(gt=0, allow_inf_nan=False)


class BenchmarkSeries(RuntimeContractModel):
    source_mode: Literal["retrospective_daily_bar"] = "retrospective_daily_bar"
    source_table: Literal["index_daily_bar"] = "index_daily_bar"
    source_identity: Sha256
    ts_code: str = Field(min_length=1)
    calendar_source_identity: Sha256
    backtest_content_hash: Sha256
    baseline_trade_date: date
    baseline_close: float = Field(gt=0, allow_inf_nan=False)
    days: tuple[BenchmarkDay, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_curve(self) -> Self:
        dates = tuple(day.trade_date for day in self.days)
        if self.baseline_trade_date >= dates[0] or tuple(sorted(set(dates))) != dates:
            raise ValueError("benchmark dates must follow the baseline in order without duplicates")
        previous_close = self.baseline_close
        for day in self.days:
            if day.daily_return != day.close / previous_close - 1:
                raise ValueError("benchmark daily return disagrees with index closes")
            if day.normalized_nav != day.close / self.baseline_close:
                raise ValueError("benchmark normalized NAV disagrees with index closes")
            previous_close = day.close
        if self.source_identity != _source_identity(
            self.ts_code,
            self.calendar_source_identity,
            (
                (self.baseline_trade_date, self.baseline_close),
                *((day.trade_date, day.close) for day in self.days),
            ),
        ):
            raise ValueError("benchmark source identity disagrees with index closes")
        return self


class BenchmarkComparison(RuntimeContractModel):
    strategy_performance: PerformanceSummary
    benchmark_performance: PerformanceSummary
    relative: RelativeMetrics


def _source_identity(
    ts_code: str,
    calendar_source_identity: str,
    rows: tuple[tuple[date, float], ...],
) -> str:
    return canonical_sha256(
        {
            "source_mode": _SOURCE_MODE,
            "source_table": _SOURCE_TABLE,
            "ts_code": ts_code,
            "calendar_source_identity": calendar_source_identity,
            "closes": rows,
        }
    )


def _required_dates(calendar: SSECalendar, result: BacktestResult) -> tuple[date, ...]:
    if result.calendar_source_identity != calendar.source_identity:
        raise BenchmarkSourceError("backtest and benchmark calendar source identities differ")
    result_dates = tuple(day.trade_date for day in result.days)
    try:
        first_index = calendar.dates.index(result_dates[0])
    except ValueError as exc:
        raise BenchmarkSourceError("backtest dates are absent from the SSE calendar") from exc
    if first_index == 0:
        raise BenchmarkSourceError("SSE calendar has no previous trading day for baseline")
    if calendar.dates[first_index : first_index + len(result_dates)] != result_dates:
        raise BenchmarkSourceError("backtest dates must be consecutive SSE trading days")
    return (calendar.dates[first_index - 1], *result_dates)


def _read_closes(
    frozen_path: Path, ts_code: str, start: date, end: date
) -> list[tuple[date, float | None]]:
    if not frozen_path.is_file():
        raise BenchmarkSourceError("an existing frozen DuckDB file is required")
    try:
        connection = duckdb.connect(str(frozen_path), read_only=True)
    except duckdb.Error as exc:
        raise BenchmarkSourceError("frozen DuckDB cannot be opened read-only") from exc
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            rows = connection.execute(
                "SELECT trade_date, close FROM index_daily_bar "
                "WHERE ts_code = ? AND trade_date BETWEEN ? AND ? ORDER BY trade_date",
                [ts_code, start, end],
            ).fetchall()
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        return rows
    except duckdb.Error as exc:
        raise BenchmarkSourceError("frozen index_daily_bar is unavailable") from exc
    finally:
        connection.close()


def load_benchmark_series(
    frozen_path: Path,
    calendar: SSECalendar,
    result: BacktestResult,
    *,
    ts_code: str = "000300.SH",
) -> BenchmarkSeries:
    """Load exact close facts from one frozen read-only snapshot; no historical timing claim."""
    if ts_code not in SUPPORTED_BENCHMARK_CODES:
        raise BenchmarkSourceError(f"unsupported benchmark index: {ts_code}")
    required_dates = _required_dates(calendar, result)
    rows = _read_closes(frozen_path, ts_code, required_dates[0], required_dates[-1])
    closes: dict[date, float] = {}
    for trade_date, close in rows:
        if trade_date in closes:
            raise BenchmarkSourceError(f"duplicate benchmark row for {trade_date}")
        if trade_date not in required_dates:
            raise BenchmarkSourceError(f"unexpected benchmark date: {trade_date}")
        if (
            isinstance(close, bool)
            or not isinstance(close, (int, float))
            or not math.isfinite(close)
            or close <= 0
        ):
            raise BenchmarkSourceError(f"missing or invalid benchmark close for {trade_date}")
        closes[trade_date] = float(close)
    baseline_date = required_dates[0]
    if baseline_date not in closes:
        raise BenchmarkSourceError(f"missing benchmark baseline close for {baseline_date}")
    missing = [trade_date for trade_date in required_dates[1:] if trade_date not in closes]
    if missing:
        raise BenchmarkSourceError(f"missing benchmark close for {missing[0]}")
    baseline_close = closes[baseline_date]
    previous_close = baseline_close
    days: list[BenchmarkDay] = []
    for trade_date in required_dates[1:]:
        close = closes[trade_date]
        daily_return = close / previous_close - 1
        normalized_nav = close / baseline_close
        if (
            not math.isfinite(daily_return)
            or not math.isfinite(normalized_nav)
            or daily_return <= -1
            or normalized_nav <= 0
        ):
            raise BenchmarkSourceError(f"invalid benchmark return for {trade_date}")
        days.append(
            BenchmarkDay(
                trade_date=trade_date,
                close=close,
                daily_return=daily_return,
                normalized_nav=normalized_nav,
            )
        )
        previous_close = close
    ordered_rows = tuple((trade_date, closes[trade_date]) for trade_date in required_dates)
    assert result.content_hash is not None
    return BenchmarkSeries(
        source_identity=_source_identity(ts_code, calendar.source_identity, ordered_rows),
        ts_code=ts_code,
        calendar_source_identity=calendar.source_identity,
        backtest_content_hash=result.content_hash,
        baseline_trade_date=baseline_date,
        baseline_close=baseline_close,
        days=tuple(days),
    )


def compare_backtest_to_benchmark(
    result: BacktestResult, benchmark: BenchmarkSeries
) -> BenchmarkComparison:
    """Compare ledger daily returns and exact index dates with the existing metrics library."""
    if result.content_hash != benchmark.backtest_content_hash:
        raise BenchmarkSourceError("benchmark is bound to a different backtest result")
    if result.calendar_source_identity != benchmark.calendar_source_identity:
        raise BenchmarkSourceError("benchmark calendar source differs from backtest result")
    if result.status != "complete" or any(
        day.account is None or day.daily_return is None or day.normalized_nav is None
        for day in result.days
    ):
        raise BenchmarkSourceError("backtest result lacks a complete account NAV series")
    dates = tuple(day.trade_date for day in result.days)
    if dates != tuple(day.trade_date for day in benchmark.days):
        raise BenchmarkSourceError("benchmark dates must match backtest dates exactly")
    index = pd.DatetimeIndex(dates)
    strategy_returns = pd.Series(
        (float(day.daily_return) for day in result.days), index=index, dtype="float64"
    )
    benchmark_returns = pd.Series(
        (day.daily_return for day in benchmark.days), index=index, dtype="float64"
    )
    return BenchmarkComparison(
        strategy_performance=performance_summary(strategy_returns),
        benchmark_performance=performance_summary(benchmark_returns),
        relative=relative_metrics(strategy_returns, benchmark_returns),
    )
