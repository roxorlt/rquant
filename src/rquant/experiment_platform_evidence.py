"""Bind complete original artifacts and reuse original performance/overfit functions."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from statistics import mean, stdev
from typing import TYPE_CHECKING
from uuid import UUID
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field

from rquant.backtest.benchmark import BenchmarkSeries
from rquant.backtest.contracts import BacktestDayResult, BacktestRequest
from rquant.experiment_platform import ExperimentPlatformStore
from rquant.experiment_platform_projection import (
    ExperimentAttemptFact,
    ExperimentFamilyFact,
    ExperimentPrivateProjectionReader,
    ExperimentPrivateResultAuthority,
)
from rquant.experiment_registry import ExperimentRegistry
from rquant.overfit import (
    DeflatedSharpeInput,
    SinglePeriodSharpeInput,
    deflated_sharpe_ratio_per_period,
    minimum_track_record_length_per_period,
    probabilistic_sharpe_ratio_per_period,
)
from rquant.overfit_pbo import CSCVInput, CSCVPBOResult, calculate_cscv_pbo
from rquant.perf import annualized_turnover, performance_summary, relative_metrics, return_distribution, rolling_metrics, streaks
from rquant.perf.trades import Fill, build_round_trips, summarize_round_trips
from rquant.portfolio_backtest_artifact import PortfolioReadResult, PortfolioResultReader
from rquant.portfolio_backtest_models import (
    PortfolioBacktestConfig,
    PortfolioPerformance,
    PortfolioRollingMetric,
    PortfolioSourceManifest,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader, TemplateReadResult
from rquant.web.experiment_platform_models import (
    ExperimentComparisonData,
    ExperimentCurvePoint,
    ExperimentHeatmapCell,
    ExperimentHeatmapData,
    ExperimentMetric,
    ExperimentParameterDifference,
    ExperimentPhasePerformance,
    ExperimentResultData,
    ExperimentStatisticsData,
    ExperimentTemplateResultIdentity,
    ExperimentNativeResultIdentity,
    ExperimentNativeParameter,
)
from rquant.web.models.backtests import PortfolioEditableConfig
from rquant.minute_backtest_formal import PreparedMinuteRequest
from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
from rquant.paper_contracts import PaperCostProvenanceState
from rquant.strategy_promotion_contracts import NativeMinuteConfiguration

if TYPE_CHECKING:
    from rquant.minute_backtest_artifact import MinuteSealedReplayReader, MinuteSealedReplayResult
    from rquant.strategy_spec import StrategySpec

_SUMMARY = (
    ("total_return", "净收益", "percent"),
    ("annualized_return", "年化收益", "percent"),
    ("annualized_volatility", "波动率", "percent"),
    ("sharpe", "夏普", "number"),
    ("sortino", "索提诺", "number"),
    ("calmar", "卡玛", "number"),
    ("max_drawdown", "最大回撤", "percent"),
    ("max_drawdown_duration", "回撤天数", "days"),
    ("win_rate", "胜率", "percent"),
    ("payoff_ratio", "盈亏比", "number"),
)


def verify_native_preparation(
    prepared: PreparedMinuteRequest, configuration: NativeMinuteConfiguration
) -> None:
    """Bind the original full C6 preparation to the native target, never its wrapper."""
    frozen, selection = prepared.frozen, configuration.selection
    target, native, runtime = selection.target, frozen.native_registration, frozen.runtime
    if (
        native.logical_id, native.version, native.fingerprint, native.record_hash,
        native.spec.spec_fingerprint, native.spec.parameter_fingerprint,
        runtime.owner_id, runtime.strategy.strategy_id, runtime.strategy.strategy_version,
        runtime.strategy.registration_fingerprint, runtime.strategy.executable_fingerprint,
        runtime.strategy.candidate_schema_fingerprint,
        runtime.execution_profile.profile_hash, runtime.start_date, runtime.end_date,
    ) != (
        target.strategy_id, target.head.version, target.head.registration_fingerprint,
        target.head.record_hash, target.head.spec_fingerprint, target.parameter_fingerprint,
        target.owner_id, target.strategy_id, target.head.version,
        native.fingerprint, native.executable_fingerprint, native.candidate_schema_fingerprint,
        selection.profile_hash, configuration.start_date, configuration.end_date,
    ):
        raise PermissionError("native preparation differs from its exact owner/definition/profile/interval")
    if (
        frozen != prepared.published.receipt.frozen
        or prepared.registration != frozen.wrapper_registration
        or prepared.registration.logical_id != "minute_runtime_replay"
        or prepared.formal_plan.preregistered_at < frozen.provenance.published_at
        or prepared.formal_plan.spec.cost_model_fingerprint != target.cost_fingerprint
    ):
        raise PermissionError("native original wrapper/publication/cost binding differs")


def unavailable_experiment_metrics() -> tuple[ExperimentMetric, ...]:
    """Keep the public metric columns even when a planned result has no values."""
    fields = tuple(f for f in _SUMMARY if f[0] not in ("win_rate", "payoff_ratio")) + (
        ("trade_count", "闭合交易", "count"),
        ("win_rate", "胜率", "percent"),
        ("payoff_ratio", "盈亏比", "number"),
        ("annualized_turnover", "年化换手", "percent"),
        ("excess_total_return", "超额收益", "percent"),
        ("excess_annualized_return", "年化超额", "percent"),
        ("alpha", "阿尔法", "number"),
        ("beta", "贝塔", "number"),
        ("tracking_error", "跟踪误差", "percent"),
        ("information_ratio", "信息比率", "number"),
    )
    return tuple(
        ExperimentMetric(key=k, label=label, unit=unit, value=None) for k, label, unit in fields
    )


def _metrics(
    summary: object,
    *,
    trade_count: int | None = None,
    turnover: float | None = None,
    win_rate: float | None = None,
    payoff_ratio: float | None = None,
) -> tuple[ExperimentMetric, ...]:
    result = [
        ExperimentMetric(key=key, label=label, value=getattr(summary, key), unit=unit)
        for key, label, unit in _SUMMARY
    ]
    if trade_count is not None:
        result = [m for m in result if m.key not in ("win_rate", "payoff_ratio")]
        result.extend(
            (
                ExperimentMetric(
                    key="trade_count", label="闭合交易", value=float(trade_count), unit="count"
                ),
                ExperimentMetric(key="win_rate", label="胜率", value=win_rate, unit="percent"),
                ExperimentMetric(
                    key="payoff_ratio", label="盈亏比", value=payoff_ratio, unit="number"
                ),
            )
        )
    result.append(
        ExperimentMetric(
            key="annualized_turnover", label="年化换手", value=turnover, unit="percent"
        )
    )
    return tuple(result)


def _series(points: tuple[ExperimentCurvePoint, ...]) -> pd.Series:
    return pd.Series(
        [p.daily_return for p in points],
        index=pd.to_datetime([p.trade_date for p in points]),
        dtype=float,
    )


def result_from_sealed(
    fact: ExperimentAttemptFact, family: ExperimentFamilyFact, sealed: PortfolioReadResult
) -> ExperimentResultData:
    bundle = sealed.bundle
    if (
        (sealed.job_id, sealed.spec_hash, sealed.manifest_hash, sealed.result_hash)
        != (fact.child.job_id, fact.spec_hash, fact.manifest_hash, fact.result_hash)
        or bundle.frozen.input_hash != fact.input_hash
        or bundle.frozen.config != fact.configuration
        or bundle.result.status != "complete"
        or bundle.performance is None
    ):
        raise ValueError("full sealed result differs from its registered binding")
    return _project_complete_result(
        fact,
        family,
        config=bundle.frozen.config,
        request=bundle.frozen.request,
        days=bundle.result.days,
        performance=bundle.performance,
        benchmark_series=bundle.benchmark,
        sources=bundle.frozen.sources,
        sealed=sealed,
        execution="portfolio-backtest@1",
    )


def native_curve_and_performance(
    replay: MinuteRuntimeReplayResult, dates: tuple[date, ...],
) -> tuple[tuple[ExperimentCurvePoint, ...], PortfolioPerformance, dict[date, tuple[float, float, float]]]:
    """Adapt actual close NAV and broker fills using the original statistics."""
    if replay.status != "complete" or replay.daily_status != "complete":
        raise ValueError("native original result is incomplete")
    if not dates or tuple(day.trade_date for day in replay.daily_valuations) != dates:
        raise ValueError("native complete original calendar differs")
    profile = replay.execution_profile
    orders = {order.order_id: order for order in replay.orders}
    fills = []
    notional = {day: [0.0, 0.0] for day in dates}
    for original in replay.fills:
        order = orders.get(original.order_id)
        day = original.executed_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
        if (
            order is None or day not in notional
            or original.cost_provenance_state is PaperCostProvenanceState.LEGACY_UNKNOWN
            or original.total_fees is None
            or original.cost_spec_id != profile.execution_costs.cost_spec_id
        ):
            raise ValueError("native fill lost its original order/date/full cost provenance")
        side = order.side.value.lower()
        fills.append(Fill(day, order.ts_code, "未分类", side,
            original.quantity, float(original.price), float(original.total_fees)))
        notional[day][0 if side == "buy" else 1] += float(original.quantity * original.price)
    previous = profile.initial_cash
    curves, turnover_rows = [], {}
    for day in replay.daily_valuations:
        if day.status != "complete" or day.account is None or previous <= 0:
            raise ValueError("native complete original NAV is unavailable")
        curves.append(ExperimentCurvePoint(trade_date=day.trade_date,
            nav=float(day.account.nav / profile.initial_cash), daily_return=float(day.account.nav / previous - 1)))
        turnover_rows[day.trade_date] = (*notional[day.trade_date], float(previous))
        previous = day.account.nav
    points = tuple(curves)
    daily = _series(points)
    ledger = build_round_trips(fills)
    rolling = rolling_metrics(daily, window=20)
    turnover = annualized_turnover(*(
        pd.Series([turnover_rows[day][i] for day in dates], index=pd.to_datetime(dates), dtype=float)
        for i in range(3)))
    performance = PortfolioPerformance(summary=performance_summary(daily), benchmark_summary=None,
        relative=None, annualized_turnover=turnover,
        rolling=tuple(PortfolioRollingMetric(trade_date=day,
            volatility=None if pd.isna(rolling.iloc[i].volatility) else float(rolling.iloc[i].volatility),
            sharpe=None if pd.isna(rolling.iloc[i].sharpe) else float(rolling.iloc[i].sharpe)) for i, day in enumerate(dates)),
        round_trips=ledger.closed, round_trip_analysis=summarize_round_trips(ledger.closed),
        distribution=return_distribution(daily, edges=(min(-1.0, float(daily.min())), 0.0, max(1.0, float(daily.max())))),
        streaks=streaks(daily))
    return points, performance, turnover_rows


def native_parameter_projection(spec: StrategySpec) -> tuple[ExperimentNativeParameter, ...]:
    from rquant.strategy_catalog_source import _PARAMETER_LABELS, _display_parameter

    parameters = spec.model_dump(mode="json")["parameters"]
    return tuple(ExperimentNativeParameter(name=name, value=value, label=_PARAMETER_LABELS[name],
        display_value=_display_parameter(name, tuple(value) if name == "allowed_boards" and isinstance(value, list) else value))
        for name, value in sorted(parameters.items()))


def result_from_native(
    fact: ExperimentAttemptFact, family: ExperimentFamilyFact,
    sealed: MinuteSealedReplayResult, prepared: PreparedMinuteRequest,
) -> ExperimentResultData:
    config = fact.configuration
    if not isinstance(config, NativeMinuteConfiguration):
        raise ValueError("native sealed result needs its native configuration")
    verify_native_preparation(prepared, config)
    if (
        (sealed.job_id, sealed.spec_hash, sealed.manifest_hash, sealed.complete_result_hash)
        != (fact.child.job_id, fact.spec_hash, fact.manifest_hash, fact.result_hash)
        or sealed.formal_plan != prepared.formal_plan
        or sealed.result.publication != prepared.published.receipt
        or sealed.full_input_hash != fact.input_hash
        or (fact.owner, fact.family_id, fact.child.owner, fact.child.family_id,
            fact.attempt.spec.hypothesis_family)
        != (family.owner, family.family_id, family.owner, family.family_id, family.family_id)
    ):
        raise PermissionError("native full seal differs from its original owner/family/spec/source")
    protocol = family.request.protocol
    window = protocol.frozen_outer_test_range if family.phase == "outer" else None
    expected_window = window or type(protocol.train_range)(start_date=protocol.train_range.start_date,
        end_date=protocol.validation_range.end_date)
    if (config.start_date, config.end_date) != (expected_window.start_date, expected_window.end_date):
        raise ValueError("native full result includes a forbidden phase")
    runtime = prepared.frozen.runtime
    dates = tuple(day for day in runtime.market_calendar.open_dates if config.start_date <= day <= config.end_date)
    points, performance, turnover_rows = native_curve_and_performance(sealed.result.replay, dates)
    phases = (("outer", window),) if window else (("training", protocol.train_range), ("validation", protocol.validation_range))
    projected = []
    for name, interval in phases:
        selected = tuple(point for point in points if interval.start_date <= point.trade_date <= interval.end_date)
        expected = tuple(day for day in dates if interval.start_date <= day <= interval.end_date)
        if not expected or tuple(point.trade_date for point in selected) != expected:
            raise ValueError("native phase lacks its complete original calendar")
        summary = performance_summary(_series(selected))
        trips = tuple(trip for trip in performance.round_trips
            if interval.start_date <= trip.entry_date <= trip.exit_date <= interval.end_date)
        stats = summarize_round_trips(trips).overall
        turnover = annualized_turnover(*(
            pd.Series([turnover_rows[day][i] for day in expected], index=pd.to_datetime(expected), dtype=float)
            for i in range(3)))
        projected.append(ExperimentPhasePerformance(phase=name, window=interval, summary=summary,
            metrics=_metrics(summary, trade_count=stats.count, turnover=turnover,
                win_rate=stats.win_rate, payoff_ratio=stats.payoff_ratio), curves=selected,
            message="验证承接训练末账户。" if name == "validation" else None))
    stats = performance.round_trip_analysis.overall
    return ExperimentResultData(experiment_id=fact.attempt.spec.experiment_id, family_id=family.family_id,
        job_id=sealed.job_id, phase=family.phase, configuration=config,
        result_hash=sealed.complete_result_hash, input_hash=sealed.full_input_hash,
        spec_hash=sealed.spec_hash, manifest_hash=sealed.manifest_hash,
        basis_hash=canonical_sha256({"window": expected_window, "phase": family.phase,
            "execution": "minute_runtime_replay@2", "profile": runtime.execution_profile.model_dump(mode="json"),
            "source": (config.source_key, config.source_version), "calendar": runtime.market_calendar.content_sha256,
            "code": runtime.producer_commit}), performance=performance, phases=tuple(projected),
        curves=points, metrics=_metrics(performance.summary, trade_count=stats.count,
            turnover=performance.annualized_turnover, win_rate=stats.win_rate, payoff_ratio=stats.payoff_ratio),
        native=ExperimentNativeResultIdentity(target=config.selection.target, profile_hash=config.selection.profile_hash,
            core_input_hash=sealed.core_input_hash, seed_hash=sealed.seed_hash, content_hash=sealed.result_hash,
            source_kind=sealed.source_kind, execution_costs=runtime.execution_profile.execution_costs,
            parameters=native_parameter_projection(prepared.frozen.native_registration.spec),
            execution_profile=runtime.execution_profile))


def read_native_preparation_result(
    reader: MinuteSealedReplayReader, prepared: PreparedMinuteRequest, *,
    job_id: UUID, configuration: NativeMinuteConfiguration, as_of: datetime,
) -> MinuteSealedReplayResult | None:
    from rquant.minute_backtest_artifact import MinuteSealedReplayReader
    from rquant.minute_backtest_producer import MinuteReplayCatalog

    if type(reader) is not MinuteSealedReplayReader:
        raise TypeError("native result needs the original complete C6 reader")
    verify_native_preparation(prepared, configuration)
    # The original private preparation supplies the full physical reference. Policy
    # permission comes from the installed reader, never from that preparation.
    catalog = MinuteReplayCatalog(entries=(prepared.published.reference,),
        installed_policies=reader.catalog.installed_policies)
    current = MinuteSealedReplayReader(reader=reader.reader, artifact_reader=reader.artifact_reader,
        submission_facade=reader.submission_facade, catalog=catalog)
    target = configuration.selection.target
    return current.read(job_id, owner_id=target.owner_id, native_id=target.strategy_id,
        native_version=target.head.version, as_of=as_of)


def _project_complete_result(
    fact: ExperimentAttemptFact,
    family: ExperimentFamilyFact,
    *,
    config: PortfolioBacktestConfig,
    request: BacktestRequest,
    days: tuple[BacktestDayResult, ...],
    performance: PortfolioPerformance,
    benchmark_series: BenchmarkSeries | None,
    sources: PortfolioSourceManifest,
    sealed: PortfolioReadResult | TemplateReadResult,
    execution: str,
    template: ExperimentTemplateResultIdentity | None = None,
) -> ExperimentResultData:
    if (
        fact.owner,
        fact.family_id,
        fact.child.owner,
        fact.child.family_id,
        fact.attempt.spec.hypothesis_family,
    ) != (family.owner, family.family_id, family.owner, family.family_id, family.family_id):
        raise PermissionError("full sealed result belongs to another owner or family")
    window = family.request.protocol.frozen_outer_test_range if family.phase == "outer" else None
    if window is not None and (config.start_date, config.end_date) != (
        window.start_date,
        window.end_date,
    ):
        raise ValueError("outer result exceeds its admitted interval")
    if family.phase == "search" and (config.start_date, config.end_date) != (
        family.request.protocol.train_range.start_date,
        family.request.protocol.validation_range.end_date,
    ):
        raise ValueError("search result includes a forbidden phase")
    benchmark = (
        {}
        if benchmark_series is None
        else {d.trade_date: d.normalized_nav for d in benchmark_series.days}
    )
    if any(d.normalized_nav is None or not d.normalized_nav.is_finite() for d in days):
        raise ValueError("original normalized NAV is unavailable")
    points = tuple(
        ExperimentCurvePoint(
            trade_date=d.trade_date,
            nav=float(d.normalized_nav),
            daily_return=float(d.daily_return),
            benchmark_nav=benchmark.get(d.trade_date),
        )
        for d in days
    )
    phases = (
        (("outer", window),)
        if window is not None
        else (
            ("training", family.request.protocol.train_range),
            ("validation", family.request.protocol.validation_range),
        )
    )
    summaries = []
    benchmark_returns = (
        {}
        if benchmark_series is None
        else {d.trade_date: d.daily_return for d in benchmark_series.days}
    )
    previous_equity = float(request.initial_cash)
    turnover_rows = {}
    for day in days:
        buy = sum(
            float(order.receipt.fill.notional)
            for order in day.orders
            if order.receipt.fill is not None and order.intent.side.value == "BUY"
        )
        sell = sum(
            float(order.receipt.fill.notional)
            for order in day.orders
            if order.receipt.fill is not None and order.intent.side.value == "SELL"
        )
        turnover_rows[day.trade_date] = (buy, sell, previous_equity)
        previous_equity = float(day.account.nav)
    for phase, interval in phases:
        selected = tuple(
            p for p in points if interval.start_date <= p.trade_date <= interval.end_date
        )
        expected = tuple(
            d for d in request.calendar.dates if interval.start_date <= d <= interval.end_date
        )
        if tuple(p.trade_date for p in selected) != expected:
            raise ValueError("phase result does not cover its complete SSE interval")
        summary = performance_summary(_series(selected))
        # Only actually closed trips wholly inside the phase count as phase trades.
        trips = tuple(
            t
            for t in performance.round_trips
            if interval.start_date <= t.entry_date <= t.exit_date <= interval.end_date
        )
        analysis = summarize_round_trips(trips).overall
        dates = tuple(p.trade_date for p in selected)
        turnover = annualized_turnover(
            *(
                pd.Series(
                    [turnover_rows[d][i] for d in dates], index=pd.to_datetime(dates), dtype=float
                )
                for i in range(3)
            )
        )
        metrics = _metrics(
            summary,
            trade_count=analysis.count,
            turnover=turnover,
            win_rate=analysis.win_rate,
            payoff_ratio=analysis.payoff_ratio,
        )
        if benchmark_returns:
            relative = relative_metrics(
                _series(selected),
                pd.Series(
                    [benchmark_returns[d] for d in dates], index=pd.to_datetime(dates), dtype=float
                ),
            )
            metrics = (*metrics, *_relative_metrics(relative))
        summaries.append(
            ExperimentPhasePerformance(
                phase=phase,
                window=interval,
                summary=summary,
                metrics=metrics,
                curves=selected,
                message="验证承接训练末账户。" if phase == "validation" else None,
            )
        )
    whole = performance
    metrics = list(
        _metrics(
            whole.summary,
            trade_count=whole.round_trip_analysis.overall.count,
            turnover=whole.annualized_turnover,
            win_rate=whole.round_trip_analysis.overall.win_rate,
            payoff_ratio=whole.round_trip_analysis.overall.payoff_ratio,
        )
    )
    if whole.relative is not None:
        metrics.extend(_relative_metrics(whole.relative))
    basis = {
        "window": (config.start_date, config.end_date),
        "phase": family.phase,
        "frequency": "daily",
        "initial_cash": config.initial_cash,
        "benchmark": config.benchmark_code,
        "code": request.producer_commit,
        "execution": execution,
        "cost": config.execution_cost_spec,
        "sources": sources,
    }
    return ExperimentResultData(
        experiment_id=fact.attempt.spec.experiment_id,
        family_id=family.family_id,
        job_id=sealed.job_id,
        phase=family.phase,
        configuration=PortfolioEditableConfig.from_domain(config),
        result_hash=sealed.result_hash,
        input_hash=fact.input_hash,
        spec_hash=sealed.spec_hash,
        manifest_hash=sealed.manifest_hash,
        basis_hash=canonical_sha256(basis),
        performance=whole,
        phases=tuple(summaries),
        metrics=tuple(metrics),
        curves=points,
        template=template,
    )


def _relative_metrics(relative: object) -> tuple[ExperimentMetric, ...]:
    return tuple(
        ExperimentMetric(key=key, label=label, unit=unit, value=getattr(relative, key))
        for key, label, unit in (
            ("excess_total_return", "超额收益", "percent"),
            ("excess_annualized_return", "年化超额", "percent"),
            ("alpha", "阿尔法", "number"),
            ("beta", "贝塔", "number"),
            ("tracking_error", "跟踪误差", "percent"),
            ("information_ratio", "信息比率", "number"),
        )
    )


def read_experiment_result(
    fact: ExperimentAttemptFact,
    family: ExperimentFamilyFact,
    *,
    results: PortfolioResultReader | None,
    authority: ExperimentPrivateResultAuthority,
    template_results: StrategyTemplateSealedResultReader | None = None,
    native_results: MinuteSealedReplayReader | None = None,
) -> ExperimentResultData:
    if fact.result_hash is None or fact.attempt.status.value not in ("executed", "succeeded"):
        raise ValueError("selected experiment has no completed sealed result")
    if isinstance(fact.configuration, NativeMinuteConfiguration):
        from rquant.minute_backtest_artifact import MinuteSealedReplayReader

        if type(native_results) is not MinuteSealedReplayReader:
            raise ValueError("original complete native result reader is unavailable")
        job = native_results.reader.get_job(fact.child.job_id)
        if job is None:
            raise ValueError("native original job is unavailable")
        original = authority.authorize(job, fact.owner).prepared
        if not isinstance(original, PreparedMinuteRequest):
            raise PermissionError("native result lost its original prepared request")
        sealed_native = read_native_preparation_result(native_results, original,
            job_id=fact.child.job_id, configuration=fact.configuration,
            as_of=max(job.updated_at, fact.attempt.completed_at or job.updated_at))
        if sealed_native is None:
            raise ValueError("native original complete seal is unavailable")
        return result_from_native(fact, family, sealed_native, original)
    if family.request.template is not None:
        if template_results is None:
            raise ValueError("original private template result reader is unavailable")
        from rquant.experiment_platform_template_evidence import result_from_template

        sealed_template = template_results.read_private(
            fact.child.job_id,
            expected_result_hash=fact.result_hash,
            private_owner=fact.owner,
            private_authority=authority,
        )
        job = template_results.reader.get_job(fact.child.job_id)
        prepared = authority.authorize(job, fact.owner).prepared
        return result_from_template(fact, family, sealed_template, prepared)
    if results is None:
        raise ValueError("original portfolio result reader is unavailable")
    sealed = results.read(
        fact.child.job_id,
        expected_result_hash=fact.result_hash,
        private_owner=fact.owner,
        private_authority=authority,
    )
    return result_from_sealed(fact, family, sealed)


def _flatten(value: object, prefix: str = "") -> dict[str, str | None]:
    if isinstance(value, dict):
        result = {}
        for key, item in sorted(value.items()):
            result.update(_flatten(item, f"{prefix}.{key}" if prefix else str(key)))
        return result
    if isinstance(value, (tuple, list)):
        result = {}
        for index, item in enumerate(value):
            result.update(_flatten(item, f"{prefix}[{index}]"))
        return result
    return {prefix: None if value is None else str(value)}


def compare_experiment_results(
    a: ExperimentResultData, b: ExperimentResultData
) -> ExperimentComparisonData:
    if a.experiment_id == b.experiment_id:
        raise ValueError("comparison requires two distinct complete results")
    left, right = (
        _flatten(a.configuration.model_dump(mode="json")),
        _flatten(b.configuration.model_dump(mode="json")),
    )
    for result, parameters in ((a, left), (b, right)):
        if result.template is not None:
            parameters.update(
                _flatten(
                    result.template.rules.model_dump(
                        mode="json", include={"entry", "exit", "index_filter"}
                    ),
                    "template",
                )
            )
    differences = tuple(
        ExperimentParameterDifference(path=key, a=left.get(key), b=right.get(key))
        for key in sorted(left.keys() | right.keys())
        if left.get(key) != right.get(key)
    )
    comparable = a.basis_hash == b.basis_hash
    values = {m.key: m for m in a.metrics}
    delta = (
        tuple(
            ExperimentMetric(
                key=m.key,
                label=m.label,
                unit=m.unit,
                value=None
                if m.value is None or values[m.key].value is None
                else m.value - values[m.key].value,
            )
            for m in b.metrics
            if m.key in values
        )
        if comparable
        else ()
    )
    return ExperimentComparisonData(
        a=a,
        b=b,
        differences=differences,
        comparable=comparable,
        message=None if comparable else "口径不同，暂不计算差值。",
        metric_differences=delta,
    )


def _parameter(config: object, key: str) -> str:
    result = config
    for field in key.split("."):
        result = getattr(result, field)
    return str(result)


def experiment_heatmap(
    family: ExperimentFamilyFact,
    facts: tuple[ExperimentAttemptFact, ...],
    *,
    selected: str,
    x: str,
    y: str,
    phase: str,
    metric: str,
    read: Callable[[ExperimentAttemptFact], ExperimentResultData],
) -> ExperimentHeatmapData:
    dimensions = {d.parameter: d for d in family.request.dimensions}
    allowed_phases = {"outer"} if family.phase == "outer" else {"training", "validation"}
    if phase not in allowed_phases or metric not in {key for key, _, _ in _SUMMARY} | {
        "trade_count",
        "annualized_turnover",
        "excess_total_return",
        "excess_annualized_return",
        "alpha",
        "beta",
        "tracking_error",
        "information_ratio",
    }:
        raise ValueError("heatmap phase or metric is not supported")
    if x == y or x not in dimensions or y not in dimensions:
        raise ValueError("heatmap requires two distinct registered dimensions")
    chosen = next((f for f in facts if f.attempt.spec.experiment_id == selected), None)
    if chosen is None:
        raise ValueError("heatmap selected configuration is not registered")
    fixed = {
        name: _parameter(chosen.configuration, name) for name in dimensions if name not in (x, y)
    }
    axes = tuple(tuple(str(v) for v in dimensions[key].values) for key in (x, y))

    # Decimal representations are numeric: 1, 1.0 and 1.00 occupy the same registered cell.
    def coordinates(fact: ExperimentAttemptFact) -> tuple[Decimal, Decimal]:
        return Decimal(_parameter(fact.configuration, x)), Decimal(
            _parameter(fact.configuration, y)
        )

    candidates = tuple(
        f
        for f in facts
        if all(Decimal(_parameter(f.configuration, k)) == Decimal(v) for k, v in fixed.items())
    )
    seen = {}
    for f in candidates:
        coordinate = coordinates(f)
        if coordinate in seen:
            raise ValueError("heatmap cannot merge other configurations into one cell")
        seen[coordinate] = f
    cells = []
    values = {}
    for i, xi in enumerate(axes[0]):
        for j, yj in enumerate(axes[1]):
            f = seen.get((Decimal(xi), Decimal(yj)))
            value = None
            if (
                f is not None
                and f.result_hash is not None
                and f.attempt.status.value in ("executed", "succeeded")
            ):
                result = read(f)
                stage = next((s for s in result.phases if s.phase == phase), None)
                if stage is None:
                    raise ValueError("heatmap phase is not part of this complete result")
                measured = next((m for m in stage.metrics if m.key == metric), None)
                if measured is None:
                    raise ValueError("heatmap metric is not supported")
                value = measured.value
            values[(i, j)] = value
            cells.append(
                ExperimentHeatmapCell(
                    x=xi,
                    y=yj,
                    experiment_id=None if f is None else f.attempt.spec.experiment_id,
                    status="not_run" if f is None else f.attempt.status.value,
                    value=value,
                )
            )
    selected_xy = coordinates(chosen)
    center = tuple(
        next(i for i, value in enumerate(axis) if Decimal(value) == wanted)
        for axis, wanted in zip(axes, selected_xy, strict=True)
    )
    neighbors = tuple(
        (i, j)
        for i in range(max(0, center[0] - 1), min(len(axes[0]), center[0] + 2))
        for j in range(max(0, center[1] - 1), min(len(axes[1]), center[1] + 2))
        if (i, j) != center
    )
    available = tuple(values[position] for position in neighbors if values[position] is not None)
    return ExperimentHeatmapData(
        family_id=family.family_id,
        selected_experiment_id=selected,
        x_parameter=x,
        y_parameter=y,
        x_values=axes[0],
        y_values=axes[1],
        phase=phase,
        metric=metric,
        fixed_parameters=tuple(
            ExperimentParameterDifference(path=k, a=v, b=v) for k, v in sorted(fixed.items())
        ),
        cells=tuple(cells),
        neighbor_count=len(neighbors),
        available_neighbors=len(available),
        neighbor_minimum=min(available) if available else None,
        complete_neighborhood=len(available) == len(neighbors),
    )


class ExperimentIndependenceEvidence(RuntimeContractModel):
    evidence_id: str
    body_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    family_id: str
    period_end_dates: tuple[date, ...]
    result_hashes: tuple[str, ...]
    independent_observations: int = Field(strict=True, ge=30)
    independent_trial_count: int = Field(strict=True, ge=1)
    assumptions: tuple[str, ...]


class ExperimentOverfitEvidence(RuntimeContractModel):
    contract: str = "experiment-overfit/v1"
    owner: str
    family_id: str
    search_count: int
    attempt_digest: str
    result_hashes: tuple[tuple[str, str | None], ...]
    frequency: str = "daily"
    estimator: str = "mean/sample-std;population-central-moments;zero-risk-free/v1"
    independence: ExperimentIndependenceEvidence | None
    period_end_dates: tuple[date, ...] = ()
    return_vector_digests: tuple[tuple[str, str], ...] = ()
    sharpe_inputs: tuple[tuple[str, SinglePeriodSharpeInput], ...] = ()
    statistics: tuple[ExperimentStatisticsData, ...]
    pbo: CSCVPBOResult | None = None
    evidence_id: str | None = None

    def seal(self) -> ExperimentOverfitEvidence:
        identifier = canonical_sha256(
            self.model_dump(
                mode="python",
                exclude={"evidence_id": True, "statistics": {"__all__": {"evidence_id"}}},
            )
        )
        if self.evidence_id is not None and self.evidence_id != identifier:
            raise ValueError("overfit evidence canonical identity differs")
        return self.model_copy(
            update={
                "evidence_id": identifier,
                "statistics": tuple(
                    s.model_copy(update={"evidence_id": identifier}) for s in self.statistics
                ),
            }
        )


def _period_input(values: tuple[float, ...], target: float) -> SinglePeriodSharpeInput:
    if len(values) < 30 or any(not math.isfinite(v) for v in values):
        raise ValueError("period_returns_insufficient")
    deviation = stdev(values)
    center = mean(values)
    variance = mean((v - center) ** 2 for v in values)
    if deviation <= 0 or variance <= 0:
        raise ValueError("period_returns_zero_variance")
    return SinglePeriodSharpeInput(
        observed_sharpe_per_period=center / deviation,
        benchmark_sharpe_per_period=target,
        skewness=mean((v - center) ** 3 for v in values) / variance**1.5,
        pearson_kurtosis=mean((v - center) ** 4 for v in values) / variance**2,
        independent_observations=len(values),
    )


def _pbo_unavailable(error: ValueError) -> str:
    message = str(error)
    for marker, reason in (
        ("at least 30", "每半段至少需要 30 个收益样本。"),
        ("divide evenly", "收益样本数不能等分为所选切片。"),
        ("zero-variance", "候选收益未变化，不能计算过拟合概率。"),
        ("tied IS/OOS", "候选夏普并列，不能计算过拟合概率。"),
        ("at least 2", "至少需要两个完整候选。"),
        ("budget", "收益矩阵超过容量。"),
        ("finite", "收益矩阵包含无效值。"),
    ):
        if marker in message:
            return reason
    return "收益矩阵不满足完整日期和切片要求。"


def build_overfit_evidence(
    family: ExperimentFamilyFact,
    facts: tuple[ExperimentAttemptFact, ...],
    *,
    read: Callable[[ExperimentAttemptFact], ExperimentResultData],
    independence: ExperimentIndependenceEvidence | None = None,
) -> ExperimentOverfitEvidence:
    if len(facts) != family.planned_count or family.phase != "search":
        raise ValueError("overfit requires the original complete registered search ledger")
    results = {}
    for fact in facts:
        if fact.result_hash is not None and fact.attempt.status.value in ("executed", "succeeded"):
            results[fact.attempt.spec.experiment_id] = read(fact)
    dates = next((tuple(p.trade_date for p in r.curves) for r in results.values()), ())
    completed_aligned = bool(results) and all(
        tuple(p.trade_date for p in r.curves) == dates for r in results.values()
    )
    aligned = len(results) == family.planned_count and completed_aligned
    pbo = None
    pbo_reason = None
    if not aligned:
        pbo_reason = "存在未完成或日期不一致的候选，不能计算过拟合概率。"
    else:
        ordered = tuple(f.attempt.spec.experiment_id for f in facts)
        try:
            pbo = calculate_cscv_pbo(
                CSCVInput(
                    candidate_ids=ordered,
                    period_end_dates=dates,
                    returns_by_observation=tuple(
                        tuple(results[i].curves[d].daily_return for i in ordered)
                        for d in range(len(dates))
                    ),
                    slice_count=family.request.pbo_slices,
                )
            )
        except ValueError as error:
            pbo_reason = _pbo_unavailable(error)
    if independence is not None:
        hashes = (
            tuple(
                results[f.attempt.spec.experiment_id].result_hash
                for f in facts
                if f.attempt.spec.experiment_id in results
            )
            if completed_aligned
            else ()
        )
        if (
            (
                independence.family_id,
                independence.period_end_dates,
                independence.result_hashes,
                independence.independent_observations,
            )
            != (family.family_id, dates, hashes, len(dates))
            or not independence.assumptions
            or independence.independent_trial_count > family.search_count
        ):
            raise ValueError("independence evidence does not bind the complete actual search")
    psrs, dsrs, lengths, p_values = {}, {}, {}, {}
    sharpe_inputs = {}
    if independence is not None:
        for identifier, result in results.items():
            try:
                inputs = _period_input(
                    tuple(p.daily_return for p in result.curves),
                    float(family.request.target_period_sharpe),
                )
                sharpe_inputs[identifier] = inputs
                psrs[identifier] = probabilistic_sharpe_ratio_per_period(inputs)
                lengths[identifier] = minimum_track_record_length_per_period(
                    inputs, confidence=float(family.request.confidence)
                )
                p_values[identifier] = Decimal(str(1 - psrs[identifier].probability))
            except ValueError:
                continue
        if len(sharpe_inputs) == family.planned_count and all(
            s.benchmark_sharpe_per_period == 0 for s in sharpe_inputs.values()
        ):
            values = tuple(s.observed_sharpe_per_period for s in sharpe_inputs.values())
            spread = stdev(values) if len(values) > 1 else 0.0
            for identifier, inputs in sharpe_inputs.items():
                dsrs[identifier] = deflated_sharpe_ratio_per_period(
                    DeflatedSharpeInput(
                        selected_strategy=inputs,
                        independent_trial_count=independence.independent_trial_count,
                        family_sharpe_std_per_period=spread,
                    )
                )
    adjusted = ExperimentRegistry._benjamini_hochberg(
        tuple(p_values.items()), hypothesis_count=family.search_count
    )
    raw_id = canonical_sha256(
        {
            "family": family.family_id,
            "ledger": facts,
            "hashes": tuple((k, r.result_hash) for k, r in results.items()),
            "independence": independence,
        }
    )
    statistics = []
    for fact in facts:
        identifier = fact.attempt.spec.experiment_id
        reasons = []
        if independence is None:
            reasons.append("缺少独立性证据，暂不能计算夏普显著性和所需样本。")
        elif identifier not in psrs:
            reasons.append("实际收益样本不足或方差无效，暂不能计算夏普显著性。")
        if pbo_reason is not None:
            reasons.append(pbo_reason)
        if independence is not None and identifier not in dsrs:
            reasons.append("修正夏普需要零目标和完整独立试验输入。")
        if identifier in lengths and lengths[identifier].status == "unreachable":
            reasons.append("当前收益水平无法达到目标。")
        statistics.append(
            ExperimentStatisticsData(
                family_id=family.family_id,
                experiment_id=identifier,
                search_count=family.search_count,
                failed_count=sum(f.attempt.status.value == "failed" for f in facts),
                cancelled_count=sum(f.attempt.status.value == "cancelled" for f in facts),
                evidence_id=raw_id,
                psr=psrs.get(identifier),
                mintrl=lengths.get(identifier),
                dsr=dsrs.get(identifier),
                pbo=None,
                bh_adjusted_p=None if identifier not in adjusted else float(adjusted[identifier]),
                reasons=tuple(reasons),
            )
        )
    return ExperimentOverfitEvidence(
        owner=family.owner,
        family_id=family.family_id,
        search_count=family.search_count,
        attempt_digest=canonical_sha256(
            tuple(f.attempt for f in sorted(facts, key=lambda f: f.attempt.spec.experiment_id))
        ),
        result_hashes=tuple(
            (f.attempt.spec.experiment_id, f.result_hash)
            for f in sorted(facts, key=lambda f: f.attempt.spec.experiment_id)
        ),
        period_end_dates=dates,
        return_vector_digests=tuple(
            (identifier, canonical_sha256(tuple(p.daily_return for p in r.curves)))
            for identifier, r in sorted(results.items())
        ),
        sharpe_inputs=tuple(sorted(sharpe_inputs.items())),
        independence=independence,
        statistics=tuple(statistics),
        pbo=pbo,
    ).seal()


def build_experiment_statistics(
    family: ExperimentFamilyFact,
    facts: tuple[ExperimentAttemptFact, ...],
    *,
    selected: str,
    read: Callable[[ExperimentAttemptFact], ExperimentResultData],
) -> ExperimentStatisticsData:
    evidence = build_overfit_evidence(family, facts, read=read)
    result = next((s for s in evidence.statistics if s.experiment_id == selected), None)
    if result is None:
        raise ValueError("selected statistics must retain its original search candidate")
    return result.model_copy(update={"pbo": evidence.pbo})


class ExperimentEvidencePublisher:
    """Installed beside original lifecycle recovery; it never grants an outcome or approval."""

    def __init__(
        self,
        *,
        store: ExperimentPlatformStore,
        projection: ExperimentPrivateProjectionReader,
        results: PortfolioResultReader,
        template_results: StrategyTemplateSealedResultReader | None = None,
        independence_resolver: Callable[
            [ExperimentFamilyFact, tuple[ExperimentAttemptFact, ...]],
            ExperimentIndependenceEvidence | None,
        ]
        | None = None,
    ) -> None:
        self.store, self.projection, self.results = store, projection, results
        self.independence_resolver = independence_resolver
        self.template_results = template_results

    def __call__(self, observed_at: datetime) -> tuple[str, ...]:
        snapshot = self.projection.snapshot(observed_at)
        if snapshot is None:
            return ()
        saved = []
        for family in snapshot.families:
            facts = tuple(
                sorted(
                    (f for f in snapshot.attempts if f.family_id == family.family_id),
                    key=lambda f: f.index,
                )
            )
            if (
                family.phase != "search"
                or len(facts) != family.planned_count
                or any(f.attempt.status.value in ("registered", "running") for f in facts)
            ):
                continue
            if any(
                f.attempt.status.value in ("executed", "succeeded") and f.result_hash is None
                for f in facts
            ):
                continue
            independence = (
                None
                if self.independence_resolver is None
                else self.independence_resolver(family, facts)
            )

            def read(
                fact: ExperimentAttemptFact, bound_family: ExperimentFamilyFact = family
            ) -> ExperimentResultData:
                return read_experiment_result(
                    fact,
                    bound_family,
                    results=self.results,
                    authority=self.projection.authority,
                    template_results=self.template_results,
                )

            evidence = build_overfit_evidence(
                family,
                facts,
                independence=independence,
                read=read,
            )
            repeated = self.projection.snapshot(observed_at)
            repeated_facts = tuple(f for f in repeated.attempts if f.family_id == family.family_id)
            if sorted(repeated_facts, key=lambda f: f.index) != list(facts):
                raise ValueError("experiment authority changed while sealing evidence")
            saved.append(self.store.save_evidence(evidence))
        return tuple(saved)
