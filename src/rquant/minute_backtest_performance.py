"""Adapt original PIT daily accounts and known fills to the existing C7 metrics."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal, Self
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, model_validator

from rquant.minute_backtest_contracts import FrozenMinuteRuntimeInput, MinuteReplayModel, Sha256
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
from rquant.paper_contracts import PaperCostProvenanceState, PaperSide
from rquant.perf import annualized_turnover, equity_curve, monthly_returns, performance_summary, return_distribution, rolling_metrics, streaks
from rquant.perf.trades import Fill, build_round_trips, summarize_round_trips
from rquant.portfolio_backtest_models import PortfolioPerformance, PortfolioRollingMetric


class MinutePerformanceDay(MinuteReplayModel):
    trade_date: date
    status: Literal["complete", "unavailable"]
    nav: Decimal | None = Field(allow_inf_nan=False)
    daily_return: Decimal | None = Field(allow_inf_nan=False)
    normalized_nav: float | None = Field(allow_inf_nan=False)
    drawdown: float | None = Field(allow_inf_nan=False)


class MinuteMonthlyReturn(MinuteReplayModel):
    year: int = Field(ge=1, le=9999)
    month: int = Field(ge=1, le=12)
    daily_observations: int = Field(ge=0)
    return_value: float | None = Field(allow_inf_nan=False)


class MinuteReplayPerformance(MinuteReplayModel):
    contract: Literal["minute-replay-performance/v1"] = "minute-replay-performance/v1"
    input_hash: Sha256
    profile_hash: Sha256
    basis: Literal["pit_asof_15:00"] = "pit_asof_15:00"
    status: Literal["complete", "unavailable"]
    daily: tuple[MinutePerformanceDay, ...]
    metrics: PortfolioPerformance | None
    monthly: tuple[MinuteMonthlyReturn, ...] = ()
    open_quantity: dict[str, int] = Field(default_factory=dict)
    benchmark_unavailable: Literal["minute_source_has_no_benchmark_series"] = "minute_source_has_no_benchmark_series"
    unavailable_reasons: tuple[str, ...] = ()

    @model_validator(mode="after")
    def complete_metrics_need_every_day(self) -> Self:
        if self.status == "complete":
            if self.metrics is None or self.unavailable_reasons or not self.daily or any(
                day.status != "complete" or day.nav is None or day.daily_return is None for day in self.daily
            ):
                raise ValueError("complete minute performance requires every original daily NAV")
        elif self.metrics is not None or self.monthly or not self.unavailable_reasons or any(
            day.daily_return is not None for day in self.daily
        ):
            raise ValueError("unavailable minute performance cannot invent a complete return series")
        return self


def _series(dates: tuple[date, ...], values: list[float]) -> pd.Series:
    return pd.Series(values, index=pd.DatetimeIndex(dates), dtype="float64")


def build_minute_performance(result: MinuteRuntimeReplayResult, *, runtime: FrozenMinuteRuntimeInput) -> MinuteReplayPerformance:
    """This calculation does not attest a source; callers use the full sealed reader."""

    if (result.input_hash, result.profile_hash, result.execution_profile, result.strategy_id, result.strategy_version,
        tuple(day.trade_date for day in result.daily_valuations)) != (
        runtime.input_hash, runtime.execution_profile.profile_hash, runtime.execution_profile,
        runtime.strategy.strategy_id, runtime.strategy.strategy_version, runtime.daily_trade_dates
    ):
        raise ValueError("minute performance differs from its exact original source/profile")
    reasons: list[str] = []
    if result.status != "complete":
        reasons.append("execution_incomplete")
    if result.daily_status != "complete":
        reasons.append("daily_nav_unavailable")
    # A recovered seed does not supply a proven NAV at this research window's start.
    if any(material.relative_path.startswith("seed/") for material in runtime.materials):
        reasons.append("seeded_window_initial_nav_unavailable")
    dates = tuple(day.trade_date for day in result.daily_valuations)
    if any(fill.cost_provenance_state is not PaperCostProvenanceState.KNOWN_V3 or fill.total_fees is None for fill in result.fills):
        reasons.append("fill_cost_provenance_unavailable")
    if any(fill.executed_at.astimezone(ZoneInfo("Asia/Shanghai")).date() not in dates for fill in result.fills):
        reasons.append("fill_outside_requested_trade_days")
    if any(day.account is not None and day.account.nav == 0 for day in result.daily_valuations[:-1]):
        reasons.append("zero_previous_nav")
    if reasons:
        return MinuteReplayPerformance(input_hash=result.input_hash, profile_hash=result.profile_hash,
            status="unavailable", metrics=None, unavailable_reasons=tuple(reasons),
            daily=tuple(MinutePerformanceDay(trade_date=day.trade_date, status=day.status,
                nav=None if day.account is None else day.account.nav, daily_return=None,
                normalized_nav=None, drawdown=None) for day in result.daily_valuations))
    previous = runtime.execution_profile.initial_cash
    daily_returns: list[Decimal] = []
    prior_equity: list[float] = []
    for day in result.daily_valuations:
        assert day.account is not None
        prior_equity.append(float(previous))
        daily_returns.append(day.account.nav / previous - 1)
        previous = day.account.nav
    returns = _series(dates, [float(value) for value in daily_returns])
    positions = {trade_date: index for index, trade_date in enumerate(dates)}
    buy, sell = [0.0] * len(dates), [0.0] * len(dates)
    orders = {order.order_id: order for order in result.orders}
    fills: list[Fill] = []
    for fill in sorted(result.fills, key=lambda item: (item.executed_at, item.sequence, item.fill_id)):
        order = orders.get(fill.order_id)
        if order is None:
            raise ValueError("minute performance fill has no original order")
        trade_date = fill.executed_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
        side = "buy" if order.side is PaperSide.BUY else "sell"
        (buy if side == "buy" else sell)[positions[trade_date]] += float(fill.notional)
        assert fill.total_fees is not None
        fills.append(Fill(trade_date=trade_date, ts_code=order.ts_code, industry="未分类", side=side,
            quantity=fill.quantity, price=float(fill.price), fee=float(fill.total_fees)))
    trips = build_round_trips(fills)
    rolling = rolling_metrics(returns, window=20)
    curve = equity_curve(returns)
    months = monthly_returns(returns)
    metrics = PortfolioPerformance(summary=performance_summary(returns), benchmark_summary=None, relative=None,
        annualized_turnover=annualized_turnover(_series(dates, buy), _series(dates, sell), _series(dates, prior_equity)),
        rolling=tuple(PortfolioRollingMetric(trade_date=stamp.date(),
            volatility=None if pd.isna(row.volatility) else float(row.volatility),
            sharpe=None if pd.isna(row.sharpe) else float(row.sharpe)) for stamp, row in rolling.iterrows()),
        round_trips=trips.closed, round_trip_analysis=summarize_round_trips(trips.closed),
        distribution=return_distribution(returns, edges=(min(-1.0, float(returns.min())), 0.0, max(1.0, float(returns.max())))),
        streaks=streaks(returns))
    return MinuteReplayPerformance(input_hash=result.input_hash, profile_hash=result.profile_hash,
        status="complete", metrics=metrics, open_quantity=trips.open_quantity,
        daily=tuple(MinutePerformanceDay(trade_date=day.trade_date, status=day.status, nav=day.account.nav,
            daily_return=value, normalized_nav=float(curve.nav.iloc[index]), drawdown=float(curve.drawdown.iloc[index]))
            for index, (day, value) in enumerate(zip(result.daily_valuations, daily_returns, strict=True))),
        monthly=tuple(MinuteMonthlyReturn(year=int(year), month=int(month),
            daily_observations=sum(day.year == year and day.month == month for day in dates),
            return_value=None if pd.isna(value) else float(value))
            for year, row in months.iterrows() for month, value in row.items()))
