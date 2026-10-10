"""Pure daily-return performance calculations; public rates are decimal fractions."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class EquityCurve:
    nav: pd.Series
    drawdown: pd.Series
    max_drawdown: float | None
    max_drawdown_duration: int | None


@dataclass(frozen=True)
class PerformanceSummary:
    observations: int
    total_return: float | None
    annualized_return: float | None
    annualized_volatility: float | None
    sharpe: float | None
    sortino: float | None
    calmar: float | None
    max_drawdown: float | None
    max_drawdown_duration: int | None
    win_rate: float | None
    payoff_ratio: float | None


@dataclass(frozen=True)
class RelativeMetrics:
    aligned_observations: int
    excess_total_return: float | None
    excess_annualized_return: float | None
    alpha: float | None
    beta: float | None
    tracking_error: float | None
    information_ratio: float | None


@dataclass(frozen=True)
class HistogramBin:
    lower: float
    upper: float
    count: int


@dataclass(frozen=True)
class ReturnDistribution:
    count: int
    mean: float | None
    median: float | None
    p05: float | None
    p95: float | None
    bins: tuple[HistogramBin, ...]


@dataclass(frozen=True)
class StreakSummary:
    longest_win: int
    longest_loss: int
    current_win: int
    current_loss: int


def _validate_daily(values: pd.Series, *, name: str, minimum: float | None = None) -> pd.Series:
    if not isinstance(values, pd.Series) or not isinstance(values.index, pd.DatetimeIndex):
        raise ValueError(f"{name} must be a pandas Series with daily DatetimeIndex")
    if (
        not values.index.is_unique
        or not values.index.is_monotonic_increasing
        or values.index.tz is not None
        or not (values.index == values.index.normalize()).all()
    ):
        raise ValueError(f"{name} dates must be unique, sorted, timezone-naive daily dates")
    if not pd.api.types.is_numeric_dtype(values.dtype) or pd.api.types.is_bool_dtype(values.dtype):
        raise ValueError(f"{name} must contain numeric values")
    numeric = values.astype("float64")
    if not np.isfinite(numeric.to_numpy()).all():
        raise ValueError(f"{name} must contain only finite values; missing data is not filled")
    if minimum is not None and (numeric < minimum).any():
        raise ValueError(f"{name} must be at least {minimum}")
    return numeric


def _daily_risk_free(risk_free_annual: float) -> float:
    if not math.isfinite(risk_free_annual) or risk_free_annual <= -1:
        raise ValueError("risk_free_annual must be finite and greater than -1")
    return (1 + risk_free_annual) ** (1 / TRADING_DAYS_PER_YEAR) - 1


def _annualized_return(total: float, observations: int) -> float | None:
    if observations == 0:
        return None
    return (1 + total) ** (TRADING_DAYS_PER_YEAR / observations) - 1


def equity_curve(returns: pd.Series) -> EquityCurve:
    """Compound close-to-close daily returns from NAV=1; duration counts underwater observations."""
    daily = _validate_daily(returns, name="returns", minimum=-1)
    if daily.empty:
        empty = pd.Series(dtype="float64", index=daily.index)
        return EquityCurve(empty, empty.copy(), None, None)
    nav = (1 + daily).cumprod()
    peak = np.maximum.accumulate(np.r_[1.0, nav.to_numpy()])[1:]
    drawdown = pd.Series(nav.to_numpy() / peak - 1, index=daily.index, dtype="float64")
    duration = longest = 0
    for value in drawdown:
        duration = duration + 1 if value < 0 else 0
        longest = max(longest, duration)
    return EquityCurve(nav, drawdown, float(drawdown.min()), longest)


def performance_summary(returns: pd.Series, risk_free_annual: float = 0.0) -> PerformanceSummary:
    """Use 252 observations/year; Sharpe and Sortino use arithmetic daily excess returns."""
    daily = _validate_daily(returns, name="returns", minimum=-1)
    risk_free_daily = _daily_risk_free(risk_free_annual)
    count = len(daily)
    if count == 0:
        return PerformanceSummary(0, *(None for _ in range(10)))
    curve = equity_curve(daily)
    total = float(curve.nav.iloc[-1] - 1)
    annualized = _annualized_return(total, count)
    volatility = float(daily.std(ddof=1) * math.sqrt(TRADING_DAYS_PER_YEAR)) if count >= 2 else None
    excess = daily - risk_free_daily
    mean_excess = float(excess.mean())
    sharpe = (
        mean_excess * TRADING_DAYS_PER_YEAR / volatility
        if volatility is not None and volatility > 0
        else None
    )
    downside = float(np.sqrt(np.mean(np.minimum(excess.to_numpy(), 0) ** 2)))
    sortino = (
        mean_excess * math.sqrt(TRADING_DAYS_PER_YEAR) / downside
        if count >= 2 and downside > 0
        else None
    )
    drawdown = curve.max_drawdown
    calmar = annualized / abs(drawdown) if drawdown is not None and drawdown < 0 else None
    gains = daily[daily > 0]
    losses = daily[daily < 0]
    decisive = len(gains) + len(losses)
    win_rate = len(gains) / decisive if decisive else None
    payoff_ratio = float(gains.mean() / abs(losses.mean())) if len(gains) and len(losses) else None
    return PerformanceSummary(
        count,
        total,
        annualized,
        volatility,
        sharpe,
        sortino,
        calmar,
        drawdown,
        curve.max_drawdown_duration,
        win_rate,
        payoff_ratio,
    )


def monthly_returns(returns: pd.Series) -> pd.DataFrame:
    """Calendar-year × month compound returns; absent months remain NaN."""
    daily = _validate_daily(returns, name="returns", minimum=-1)
    if daily.empty:
        return pd.DataFrame(columns=range(1, 13), dtype="float64")
    years = range(int(daily.index.year.min()), int(daily.index.year.max()) + 1)
    output = pd.DataFrame(index=years, columns=range(1, 13), dtype="float64")
    for period, group in daily.groupby(daily.index.to_period("M")):
        output.loc[period.year, period.month] = float((1 + group).prod() - 1)
    return output


def rolling_metrics(
    returns: pd.Series, *, window: int, risk_free_annual: float = 0.0
) -> pd.DataFrame:
    """Full-window annualized volatility and Sharpe on observed trading dates."""
    daily = _validate_daily(returns, name="returns", minimum=-1)
    if isinstance(window, bool) or not isinstance(window, int) or window < 2:
        raise ValueError("window must be an integer of at least 2 observations")
    risk_free_daily = _daily_risk_free(risk_free_annual)
    rolling = daily.rolling(window=window, min_periods=window)
    vol = rolling.std(ddof=1) * math.sqrt(TRADING_DAYS_PER_YEAR)
    sharpe = (rolling.mean() - risk_free_daily) * TRADING_DAYS_PER_YEAR / vol.where(vol > 0)
    return pd.DataFrame({"volatility": vol, "sharpe": sharpe}, index=daily.index)


def relative_metrics(
    strategy_returns: pd.Series,
    benchmark_returns: pd.Series,
    risk_free_annual: float = 0.0,
) -> RelativeMetrics:
    """Compare only exact shared dates; alpha is 252 × daily OLS intercept."""
    strategy = _validate_daily(strategy_returns, name="strategy_returns", minimum=-1)
    benchmark = _validate_daily(benchmark_returns, name="benchmark_returns", minimum=-1)
    risk_free_daily = _daily_risk_free(risk_free_annual)
    common = strategy.index.intersection(benchmark.index)
    count = len(common)
    if count == 0:
        return RelativeMetrics(0, *(None for _ in range(6)))
    aligned_strategy = strategy.loc[common]
    aligned_benchmark = benchmark.loc[common]
    strategy_nav = float((1 + aligned_strategy).prod())
    benchmark_nav = float((1 + aligned_benchmark).prod())
    excess_total = strategy_nav / benchmark_nav - 1 if benchmark_nav > 0 else None
    excess_annualized = (
        _annualized_return(excess_total, count) if excess_total is not None else None
    )
    if count < 2:
        return RelativeMetrics(count, excess_total, excess_annualized, None, None, None, None)
    active = aligned_strategy - aligned_benchmark
    tracking_error = float(active.std(ddof=1) * math.sqrt(TRADING_DAYS_PER_YEAR))
    information_ratio = (
        float(active.mean()) * TRADING_DAYS_PER_YEAR / tracking_error
        if tracking_error > 0
        else None
    )
    benchmark_excess = aligned_benchmark - risk_free_daily
    variance = float(benchmark_excess.var(ddof=1))
    beta = (
        float((aligned_strategy - risk_free_daily).cov(benchmark_excess) / variance)
        if variance > 0
        else None
    )
    alpha = (
        float((aligned_strategy - risk_free_daily).mean() - beta * benchmark_excess.mean())
        * TRADING_DAYS_PER_YEAR
        if beta is not None
        else None
    )
    return RelativeMetrics(
        count, excess_total, excess_annualized, alpha, beta, tracking_error, information_ratio
    )


def annualized_turnover(
    buy_notional: pd.Series, sell_notional: pd.Series, prior_equity: pd.Series
) -> float | None:
    """One-way daily turnover=max(buy,sell)/prior equity; annualize the observed mean."""
    buy = _validate_daily(buy_notional, name="buy_notional", minimum=0)
    sell = _validate_daily(sell_notional, name="sell_notional", minimum=0)
    equity = _validate_daily(prior_equity, name="prior_equity", minimum=0)
    if not buy.index.equals(sell.index) or not buy.index.equals(equity.index):
        raise ValueError("turnover input dates must match exactly")
    if (equity <= 0).any():
        raise ValueError("prior equity must be positive")
    if buy.empty:
        return None
    return float((np.maximum(buy.to_numpy(), sell.to_numpy()) / equity.to_numpy()).mean() * 252)


def return_distribution(returns: pd.Series, *, edges: Sequence[float]) -> ReturnDistribution:
    """Half-open bins with the last upper edge inclusive; refuse out-of-range evidence."""
    daily = _validate_daily(returns, name="returns", minimum=-1)
    limits = np.asarray(edges, dtype="float64")
    if len(limits) < 2 or not np.isfinite(limits).all() or not (np.diff(limits) > 0).all():
        raise ValueError("edges must be finite and strictly increasing")
    values = daily.to_numpy()
    if len(values) and (values.min() < limits[0] or values.max() > limits[-1]):
        raise ValueError("returns outside histogram edges")
    counts, _ = np.histogram(values, bins=limits)
    bins = tuple(
        HistogramBin(float(limits[i]), float(limits[i + 1]), int(counts[i]))
        for i in range(len(counts))
    )
    if not len(values):
        return ReturnDistribution(0, None, None, None, None, bins)
    return ReturnDistribution(
        len(values),
        float(values.mean()),
        float(np.median(values)),
        float(np.quantile(values, 0.05)),
        float(np.quantile(values, 0.95)),
        bins,
    )


def streaks(returns: pd.Series) -> StreakSummary:
    """Consecutive positive/negative observed days; zero returns break both streaks."""
    daily = _validate_daily(returns, name="returns", minimum=-1)
    current_win = current_loss = longest_win = longest_loss = 0
    for value in daily:
        current_win = current_win + 1 if value > 0 else 0
        current_loss = current_loss + 1 if value < 0 else 0
        longest_win = max(longest_win, current_win)
        longest_loss = max(longest_loss, current_loss)
    return StreakSummary(longest_win, longest_loss, current_win, current_loss)
