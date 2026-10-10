"""Project full original template ledgers with the existing performance functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from rquant.backtest.benchmark import BenchmarkDay, BenchmarkSeries, _source_identity
from rquant.experiment_platform_template_models import PreparedExperimentTemplate
from rquant.perf import (
    annualized_turnover,
    performance_summary,
    relative_metrics,
    return_distribution,
    rolling_metrics,
    streaks,
)
from rquant.perf.trades import Fill, build_round_trips, summarize_round_trips
from rquant.portfolio_backtest_models import PortfolioPerformance, PortfolioRollingMetric
from rquant.strategy_template_artifact import TemplateReadResult, bind_complete_template_result
from rquant.web.experiment_platform_models import (
    ExperimentResultData,
    ExperimentTemplateResultIdentity,
)

if TYPE_CHECKING:
    from rquant.experiment_platform_projection import ExperimentAttemptFact, ExperimentFamilyFact


def result_from_template(
    fact: ExperimentAttemptFact,
    family: ExperimentFamilyFact,
    sealed: TemplateReadResult,
    prepared: PreparedExperimentTemplate,
) -> ExperimentResultData:
    from rquant.experiment_platform_evidence import _project_complete_result

    bind_complete_template_result(prepared, sealed.result)
    if (
        sealed.job_id,
        sealed.spec_hash,
        sealed.manifest_hash,
        sealed.result_hash,
        prepared.frozen.input_hash,
        prepared.configuration,
    ) != (
        fact.child.job_id,
        fact.spec_hash,
        fact.manifest_hash,
        fact.result_hash,
        fact.input_hash,
        fact.configuration,
    ):
        raise ValueError("full template result differs from its registered child")
    result, request = sealed.result, prepared.frozen.request
    dates = tuple(d.trade_date for d in result.days)
    index = pd.DatetimeIndex(dates)
    returns = pd.Series([float(d.daily_return) for d in result.days], index=index, dtype=float)
    benchmark = None
    if prepared.benchmark_closes is not None:
        rows = prepared.benchmark_closes
        previous = rows[0][1]
        days = []
        for trade_date, close in rows[1:]:
            days.append(
                BenchmarkDay(
                    trade_date=trade_date,
                    close=close,
                    daily_return=close / previous - 1,
                    normalized_nav=close / rows[0][1],
                )
            )
            previous = close
        benchmark = BenchmarkSeries(
            source_identity=_source_identity(
                prepared.configuration.benchmark_code, request.calendar.source_identity, rows
            ),
            ts_code=prepared.configuration.benchmark_code,
            calendar_source_identity=request.calendar.source_identity,
            backtest_content_hash=result.content_hash,
            baseline_trade_date=rows[0][0],
            baseline_close=rows[0][1],
            days=tuple(days),
        )
        if tuple(d.trade_date for d in benchmark.days) != dates:
            raise ValueError("template benchmark differs from its complete result interval")
    buys, sells, equity, fills = [], [], [], []
    for i, day in enumerate(result.days):
        equity.append(float(request.initial_cash if i == 0 else result.days[i - 1].account.nav))
        buys.append(0.0)
        sells.append(0.0)
        industry = {
            c.ts_code: c.industry_l1 or "未分类" for c in request.days[i].ranking.candidates
        }
        for order in day.orders:
            fill = order.receipt.fill
            if fill is None:
                continue
            side = "buy" if order.intent.side.value == "BUY" else "sell"
            (buys if side == "buy" else sells)[-1] += float(fill.notional)
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
        None
        if benchmark is None
        else pd.Series([d.daily_return for d in benchmark.days], index=index, dtype=float)
    )
    performance = PortfolioPerformance(
        summary=performance_summary(returns),
        benchmark_summary=None
        if benchmark_returns is None
        else performance_summary(benchmark_returns),
        relative=None
        if benchmark_returns is None
        else relative_metrics(returns, benchmark_returns),
        annualized_turnover=annualized_turnover(
            *(pd.Series(v, index=index, dtype=float) for v in (buys, sells, equity))
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
        distribution=return_distribution(
            returns, edges=(min(-1.0, float(returns.min())), 0.0, max(1.0, float(returns.max())))
        ),
        streaks=streaks(returns),
    )
    return _project_complete_result(
        fact,
        family,
        config=prepared.configuration,
        request=request,
        days=result.days,
        performance=performance,
        benchmark_series=benchmark,
        sources=prepared.frozen.sources,
        sealed=sealed,
        execution="strategy-template@1",
        template=ExperimentTemplateResultIdentity(
            strategy_id=result.strategy_id,
            head=prepared.catalog.versions[0].head,
            rules=prepared.frozen.rules,
            content_hash=result.content_hash,
        ),
    )
