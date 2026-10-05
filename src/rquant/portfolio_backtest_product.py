"""Execute one frozen portfolio input and adapt its complete ledger to Lab tables."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from rquant.backtest.benchmark import BenchmarkDay, BenchmarkSeries, _source_identity
from rquant.backtest.contracts import BacktestResult
from rquant.backtest.report import render_backtest_html
from rquant.backtest.runner import run_portfolio_backtest
from rquant.perf import (
    annualized_turnover,
    equity_curve,
    monthly_returns,
    performance_summary,
    relative_metrics,
    return_distribution,
    rolling_metrics,
    streaks,
)
from rquant.perf.trades import Fill, build_round_trips, summarize_round_trips
from rquant.portfolio_backtest_models import (
    FrozenPortfolioInput,
    PortfolioBundle,
    PortfolioPerformance,
    PortfolioRollingMetric,
)
from rquant.strict_json import canonical_json_bytes


def _benchmark(value: FrozenPortfolioInput, result: BacktestResult) -> BenchmarkSeries | None:
    if value.benchmark_closes is None:
        return None
    baseline_date, baseline_close = value.benchmark_closes[0]
    previous = baseline_close
    days: list[BenchmarkDay] = []
    for trade_date, close in value.benchmark_closes[1:]:
        days.append(
            BenchmarkDay(
                trade_date=trade_date,
                close=close,
                daily_return=close / previous - 1,
                normalized_nav=close / baseline_close,
            )
        )
        previous = close
    return BenchmarkSeries(
        source_identity=_source_identity(
            value.config.benchmark_code,
            value.request.calendar.source_identity,
            value.benchmark_closes,
        ),
        ts_code=value.config.benchmark_code,
        calendar_source_identity=value.request.calendar.source_identity,
        backtest_content_hash=result.content_hash,
        baseline_trade_date=baseline_date,
        baseline_close=baseline_close,
        days=tuple(days),
    )


def _series(dates: tuple[date, ...], values: object) -> pd.Series:
    return pd.Series(values, index=pd.DatetimeIndex(dates), dtype="float64")


def execute_portfolio_input(value: FrozenPortfolioInput, *, research_root: Path) -> PortfolioBundle:
    value = FrozenPortfolioInput.model_validate(value.model_dump(mode="python"))
    result = run_portfolio_backtest(value.request, research_root=research_root)
    if result.status == "incomplete":
        return PortfolioBundle(
            frozen=value,
            result=result,
            benchmark=None,
            performance=None,
            html=None,
            html_sha256=None,
        )
    benchmark = _benchmark(value, result)
    dates = tuple(day.trade_date for day in result.days)
    returns = _series(dates, [float(day.daily_return) for day in result.days])
    buy: list[float] = []
    sell: list[float] = []
    equity: list[float] = []
    fills: list[Fill] = []
    for index, day in enumerate(result.days):
        equity.append(
            float(value.request.initial_cash if index == 0 else result.days[index - 1].account.nav)
        )
        buy.append(0.0)
        sell.append(0.0)
        industry = {
            item.ts_code: item.industry_l1 or "未分类"
            for item in value.request.days[index].ranking.candidates
        }
        for order in day.orders:
            fill = order.receipt.fill
            if fill is None:
                continue
            amount = float(fill.notional)
            if order.intent.side.value == "BUY":
                buy[-1] += amount
                side = "buy"
            else:
                sell[-1] += amount
                side = "sell"
            fills.append(
                Fill(
                    trade_date=day.trade_date,
                    ts_code=order.intent.ts_code,
                    industry=industry.get(order.intent.ts_code, "未分类"),
                    side=side,
                    quantity=fill.quantity,
                    price=float(fill.price),
                    fee=float(fill.total_fees),
                )
            )
    trips = build_round_trips(fills)
    rolling = rolling_metrics(returns, window=20)
    benchmark_returns = (
        None if benchmark is None else _series(dates, [day.daily_return for day in benchmark.days])
    )
    low, high = min(-1.0, float(returns.min())), max(1.0, float(returns.max()))
    performance = PortfolioPerformance(
        summary=performance_summary(returns),
        benchmark_summary=None
        if benchmark_returns is None
        else performance_summary(benchmark_returns),
        relative=None
        if benchmark_returns is None
        else relative_metrics(returns, benchmark_returns),
        annualized_turnover=annualized_turnover(
            _series(dates, buy), _series(dates, sell), _series(dates, equity)
        ),
        rolling=tuple(
            PortfolioRollingMetric(
                trade_date=stamp.date(),
                volatility=None if pd.isna(row.volatility) else float(row.volatility),
                sharpe=None if pd.isna(row.sharpe) else float(row.sharpe),
            )
            for stamp, row in rolling.iterrows()
        ),
        round_trips=trips.closed,
        round_trip_analysis=summarize_round_trips(trips.closed),
        distribution=return_distribution(returns, edges=(low, 0.0, high)),
        streaks=streaks(returns),
    )
    report = render_backtest_html(result, benchmark)
    return PortfolioBundle(
        frozen=value,
        result=result,
        benchmark=benchmark,
        performance=performance,
        html=report.html_bytes.decode(),
        html_sha256=report.sha256,
    )


def bundle_views(bundle: PortfolioBundle) -> dict[str, list[dict[str, object]]]:
    """Flatten reconciled facts; these views never fabricate an account or a return."""
    nav: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []
    holdings: list[dict[str, object]] = []
    daily: list[dict[str, object]] = []
    logs: list[dict[str, object]] = []
    complete_days = [day for day in bundle.result.days if day.account is not None]
    dates = tuple(day.trade_date for day in complete_days)
    returns = _series(dates, [float(day.daily_return) for day in complete_days])
    curve = equity_curve(returns)
    benchmarks = (
        {} if bundle.benchmark is None else {day.trade_date: day for day in bundle.benchmark.days}
    )
    risk_was_active = False
    for day in bundle.result.days:
        benchmark = benchmarks.get(day.trade_date)
        record = {
            "trade_date": day.trade_date.isoformat(),
            "nav": None if day.account is None else str(day.account.nav),
            "normalized_nav": None if day.normalized_nav is None else str(day.normalized_nav),
            "daily_return": None if day.daily_return is None else str(day.daily_return),
            "cash": None if day.account is None else str(day.account.cash),
            "market_value": None if day.market_value is None else str(day.market_value),
            "fees": str(day.fees),
            "benchmark_nav": None if benchmark is None else benchmark.normalized_nav,
            "benchmark_return": None if benchmark is None else benchmark.daily_return,
            "drawdown": None
            if day.account is None
            else float(curve.drawdown.loc[pd.Timestamp(day.trade_date)]),
            "rebalanced": day.rebalanced,
            "incomplete_reason": day.incomplete_reason,
        }
        daily.append(record)
        nav.append(record)
        if day.account is not None:
            for holding in day.account.holdings:
                holdings.append(
                    {"trade_date": day.trade_date.isoformat(), **holding.model_dump(mode="json")}
                )
        for order in day.orders:
            trades.append(
                {"trade_date": day.trade_date.isoformat(), **order.model_dump(mode="json")}
            )
            logs.append(
                {
                    "trade_date": day.trade_date.isoformat(),
                    "ts_code": order.intent.ts_code,
                    "status": order.receipt.order.status.value,
                    "reason": None
                    if order.receipt.order.reject_reason is None
                    else order.receipt.order.reject_reason.value,
                }
            )
        for skipped in day.skipped:
            logs.append(
                {
                    "trade_date": day.trade_date.isoformat(),
                    "ts_code": skipped.ts_code,
                    "status": "skipped",
                    "reason": skipped.reason,
                }
            )
        if day.incomplete_reason is not None:
            logs.append(
                {
                    "trade_date": day.trade_date.isoformat(),
                    "ts_code": None,
                    "status": "incomplete",
                    "reason": day.incomplete_reason,
                }
            )
        if day.risk is not None:
            active = day.risk.state.active
            if active or risk_was_active:
                logs.append(
                    {
                        "trade_date": day.trade_date.isoformat(),
                        "ts_code": None,
                        "status": "risk",
                        "reason": (
                            "drawdown_blocked"
                            if day.risk.state.rule.action == "block_new_positions"
                            else "drawdown_capped"
                        )
                        if active
                        else "drawdown_released",
                    }
                )
            risk_was_active = active
    monthly = monthly_returns(returns)
    monthly_rows = [
        {
            "year": int(year),
            "month": int(month),
            "return_rate": None
            if pd.isna(monthly.loc[year, month])
            else float(monthly.loc[year, month]),
        }
        for year in monthly.index
        for month in monthly.columns
    ]
    return {
        "portfolio_nav": nav,
        "portfolio_trades": trades,
        "portfolio_holdings": holdings,
        "portfolio_daily": daily,
        "portfolio_monthly": monthly_rows,
        "portfolio_log": logs,
    }


def bundle_tables(bundle: PortfolioBundle) -> dict[str, pd.DataFrame]:
    checked = PortfolioBundle.model_validate_json(bundle.json_bytes())
    tables = {
        "portfolio_bundle": pd.DataFrame(
            {"payload": [checked.json_bytes().decode()]}, dtype="string"
        )
    }
    for name, rows in bundle_views(checked).items():
        tables[name] = pd.DataFrame(
            {"payload": [canonical_json_bytes(row).decode() for row in rows]}, dtype="string"
        )
    return tables
