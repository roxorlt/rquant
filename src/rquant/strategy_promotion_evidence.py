"""Read full original artifacts, then adapt one explicit validation domain."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pandas as pd

from rquant.backtest.contracts import SSECalendar
from rquant.definition_registry import StrategySpecRegistration
from rquant.experiment_platform import ExperimentPlatformStore
from rquant.experiment_platform_evidence import (
    ExperimentIndependenceEvidence,
    build_overfit_evidence,
    read_experiment_result,
)
from rquant.experiment_platform_projection import (
    ExperimentAttemptFact,
    ExperimentFamilyFact,
    ExperimentPrivateContextRead,
    ExperimentPrivateProjectionReader,
)
from rquant.experiment_platform_templates import ExperimentTemplateBinding
from rquant.experiment_registry import (
    DateRange,
    ExperimentOutcome,
    ExperimentRegistry,
    ExperimentStatus,
    ForwardArtifactEvidence,
)
from rquant.paper_backtest_source import PaperBacktestSourceReader
from rquant.paper_broker import PaperBrokerReconciliationError
from rquant.paper_portfolio_band import (
    NativePaperBacktestBandInput, PaperBacktestBandInput, bootstrap_daily_band,
)
from rquant.paper_portfolio_ledger import PaperPortfolioLedgerFrame
from rquant.paper_portfolio_models import PaperPortfolioConfiguration
from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
from rquant.paper_portfolio_views import PaperDailyNav
from rquant.paper_research_artifact import PaperResearchSealedAnalysis
from rquant.paper_research_commands import OwnedRunPaperPortfolioResearch
from rquant.perf import performance_summary
from rquant.perf.trades import summarize_round_trips
from rquant.portfolio_backtest_artifact import PortfolioResultReader
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.strategy_promotion_contracts import (
    BoundForwardPromotionEvidence,
    BoundOuterPromotionEvidence,
    BoundValidationPromotionEvidence,
    PromotionEvidenceBundle,
    PromotionEvidenceSelection,
    SealedFamilyAttemptOutcome,
    SealedFamilyOutcomeReceipt,
    SealedPromotionResult,
    NativeMinuteConfiguration,
    NativeMinuteForwardConfiguration,
    StrategyPromotionState,
    StrategyPromotionTarget,
)
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
from rquant.web.experiment_platform_models import ExperimentResultData

if TYPE_CHECKING:
    from rquant.minute_backtest_artifact import MinuteSealedReplayReader
    from rquant.paper_research_runtime import NativeMinuteForwardViewSource
    from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
    from rquant.strategy_promotion_commands import RunStrategyWalkForward
    from rquant.strategy_promotion_walk_forward import (
        StrategyPromotionWalkForwardBackend,
        StrategyPromotionWalkForwardPlan,
        PromotionWalkForwardPlan,
    )


_FullValidation = tuple[
    str, BoundValidationPromotionEvidence, ExperimentResultData, tuple[Decimal, ...],
]


class PromotionStatisticsUnavailableError(ValueError):
    """Missing original p/interval prevents an Outcome, not validation reading."""


def validation_view(
    result: ExperimentResultData, window: DateRange, dates: tuple[date, ...]
) -> ExperimentResultData:
    """Hash binding stays to the full seal; only curves passed to PSR are sliced."""
    selected = tuple(
        p for p in result.curves if window.start_date <= p.trade_date <= window.end_date
    )
    if (
        not dates
        or dates != tuple(sorted(set(dates)))
        or tuple(p.trade_date for p in selected) != dates
    ):
        raise ValueError("complete original validation calendar differs")
    if dates[0] < window.start_date or dates[-1] > window.end_date:
        raise ValueError("validation dates exceed original fixed interval")
    return result.model_copy(update={"curves": selected})


def validation_outcome(
    *,
    experiment_id: str,
    reference_hash: str,
    returns: tuple[Decimal, ...],
    dates: tuple[date, ...],
    trade_count: int,
    max_drawdown: Decimal,
    win_rate: Decimal,
    parent_n: int,
    rank: int,
    raw_p: Decimal | None,
) -> ExperimentOutcome:
    if raw_p is None:
        raise PromotionStatisticsUnavailableError("缺少原独立性或有效收益样本，p 暂不可用。")
    if len(dates) != len(returns) or dates != tuple(sorted(set(dates))):
        raise ValueError("complete validation dates differ from returns")
    series = pd.Series([float(v) for v in returns], index=pd.to_datetime(dates), dtype=float)
    summary = performance_summary(series)
    if summary.total_return is None:
        raise ValueError("full validation daily returns are absent")
    band = bootstrap_daily_band(returns, days=len(returns))[-1]
    point = Decimal(str(summary.total_return))
    if not band.lower - 1 <= point <= band.upper - 1:
        raise PromotionStatisticsUnavailableError("原净收益不在完整90%区间内，统计结果暂不可用。")
    return ExperimentOutcome(
        experiment_id=experiment_id,
        trade_count=trade_count,
        net_return=point,
        max_drawdown=max_drawdown,
        win_rate=win_rate,
        confidence_lower=band.lower - 1,
        confidence_upper=band.upper - 1,
        attempted_configuration_count=parent_n,
        selected_rank=rank,
        raw_p_value=raw_p,
        artifact_hash=reference_hash,
        outer_test_completed=False,
        outer_evidence=None,
    )


def bind_forward_evidence(
    target: StrategyPromotionTarget,
    *,
    state: StrategyPromotionState,
    configuration: PaperPortfolioConfiguration | NativeMinuteForwardConfiguration,
    frame: PaperPortfolioLedgerFrame,
    nav: tuple[PaperDailyNav, ...],
    calendar: SSECalendar,
    band: PaperResearchSealedAnalysis,
    band_input: PaperBacktestBandInput | NativePaperBacktestBandInput,
    as_of: datetime,
) -> BoundForwardPromotionEvidence:
    """Adapt complete original reads; the caller never supplies browser metrics."""
    observed = normalize_aware_utc(as_of)
    local = observed.astimezone(ZoneInfo("Asia/Shanghai"))
    binding = configuration.binding
    if (
        state.target != target
        or state.paper_approval_hash is None
        or state.paper_approved_at is None
    ):
        raise ValueError("forward lacks its bound human paper approval")
    native = type(configuration) is NativeMinuteForwardConfiguration
    if native:
        if type(band_input) is not NativePaperBacktestBandInput or (
            configuration.target, configuration.paper_approval_hash, configuration.paper_approved_at
        ) != (target, state.paper_approval_hash, state.paper_approved_at):
            raise ValueError("native forward differs from its exact human approval or source kind")
        NativePaperBacktestBandInput.model_validate_json(band_input.model_dump_json())
    if (
        binding.owner_id,
        binding.strategy_id,
        binding.strategy_version,
        binding.parameter_fingerprint,
        canonical_sha256(configuration.execution_cost_spec),
    ) != (
        target.owner_id,
        target.strategy_id,
        str(target.head.version),
        target.parameter_fingerprint,
        target.cost_fingerprint,
    ):
        raise ValueError("forward original account has another fixed version/parameters/cost")
    if (frame.configuration_fingerprint, frame.account_id, frame.as_of) != (
        configuration.fingerprint,
        binding.account_id,
        observed,
    ) or frame.account is None:
        raise ValueError("forward original full ledger or valuation is unavailable")
    first_after = state.paper_approved_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
    last_closed = local.date() if local.time() >= time(15) else local.date() - timedelta(days=1)
    if calendar.coverage_start > first_after or calendar.coverage_end < last_closed:
        raise ValueError("forward original calendar coverage is incomplete")
    expected = tuple(day for day in calendar.dates if first_after < day <= last_closed)
    records = tuple(point for point in nav if first_after < point.trade_date <= last_closed)
    if tuple(point.trade_date for point in nav) != tuple(
        sorted({point.trade_date for point in nav})
    ) or any(
        point.published_at > observed
        or point.close_at > observed
        or point.ledger_revision > frame.ledger_revision
        or point.configuration_fingerprint != configuration.fingerprint
        or point.account_id != binding.account_id
        or point.calendar_source_identity != calendar.source_identity
        for point in nav
    ):
        raise ValueError("forward original full NAV contains a future or changed source")
    if not expected or tuple(point.trade_date for point in records) != expected:
        raise ValueError("forward original calendar has missing or reordered NAV days")
    if any(
        point.status != "complete"
        or point.daily_return is None
        or point.normalized_nav is None
        or point.configuration_fingerprint != configuration.fingerprint
        or point.account_id != binding.account_id
        or point.calendar_source_identity != calendar.source_identity
        or point.published_at > observed
        or point.close_at > observed
        or point.ledger_revision > frame.ledger_revision
        for point in records
    ):
        raise ValueError("forward original NAV contains a gap, future fact or changed source")
    source = band_input.backtest
    result = band.band
    if (
        source.owner_id,
        source.strategy_id,
        source.strategy_version,
        source.parameter_fingerprint,
        source.definition_fingerprint,
        source.definition_record_hash,
        source.cost_spec_id,
    ) != (
        target.owner_id,
        target.strategy_id,
        str(target.head.version),
        target.parameter_fingerprint,
        target.head.registration_fingerprint,
        target.head.record_hash,
        binding.cost_spec_id,
    ):
        raise ValueError("forward band backtest differs from its exact immutable definition")
    if (
        result is None
        or band.task_name != "paper_backtest_band"
        or band.completed_at > observed
        or (band.account_id, band.configuration_fingerprint, band.configuration_version)
        != (binding.account_id, configuration.fingerprint, configuration.version)
        or band_input.configuration != configuration
        or band_input.calendar != calendar
        or band_input.comparison_dates != tuple(point.trade_date for point in nav)
        or result.dates != band_input.comparison_dates
        or (result.input_hash, result.backtest_source_hash)
        != (band_input.fingerprint, source.fingerprint)
    ):
        raise ValueError("forward original sealed band differs from its full bound input/calendar")
    series = pd.Series(
        [float(point.daily_return) for point in records],
        index=pd.to_datetime(expected),
        dtype=float,
    )
    summary = performance_summary(series)
    if summary.total_return is None or summary.max_drawdown is None:
        raise ValueError("forward original complete performance is unavailable")
    body = {
        "contract": "strategy-forward-original-evidence/v1",
        "target": target,
        "paper_approval": state.paper_approval_hash,
        "configuration": configuration.model_dump(mode="json") if native else configuration,
        "frame": frame,
        "nav": records,
        "calendar": calendar,
        "band": band,
        "band_input_hash": band_input.fingerprint,
    }
    original = ForwardArtifactEvidence(
        artifact_hash=canonical_sha256(body),
        metric_definition_fingerprint=canonical_sha256(
            {"contract": "strategy-forward-validation/v1", "band_algorithm": result.algorithm}
        ),
        observation_range=DateRange(start_date=expected[0], end_date=expected[-1]),
        available_at=observed,
        trading_days=len(expected),
        fill_count=sum(len(item.fills) for item in frame.history),
        net_return=Decimal(str(summary.total_return)),
        max_drawdown=Decimal(str(abs(summary.max_drawdown))),
    )
    bands = dict(zip(result.dates, result.points, strict=True))
    return BoundForwardPromotionEvidence(
        target=target,
        paper_approval_hash=state.paper_approval_hash,
        configuration_fingerprint=configuration.fingerprint,
        ledger_generation=frame.ledger_generation,
        ledger_head=frame.head_fingerprint,
        ledger_revision=frame.ledger_revision,
        nav_source_hash=canonical_sha256(records),
        calendar_source_hash=calendar.source_identity,
        band_source_hash=band.result_hash,
        band_job=band.job_id,
        full_open_days=len(expected),
        first_date=expected[0],
        last_date=expected[-1],
        reconciliation_hash=canonical_sha256(frame.reconciliation),
        all_inside_original_band=all(
            bands[record.trade_date].lower
            <= record.normalized_nav
            <= bands[record.trade_date].upper
            for record in records
        ),
        original=original,
    )


class StrategyPromotionEvidenceSource:
    """Concrete original private experiment chain, shared by review and apply."""

    def __init__(
        self,
        *,
        registry: ExperimentRegistry,
        platform: ExperimentPlatformStore,
        projection: ExperimentPrivateProjectionReader,
        results: PortfolioResultReader | None,
        template_results: StrategyTemplateSealedResultReader | None,
        independence_resolver: Callable[
            [
                ExperimentFamilyFact,
                tuple[ExperimentAttemptFact, ...],
                tuple[ExperimentResultData, ...],
            ],
            ExperimentIndependenceEvidence | None,
        ]
        | None = None,
        walk_forward: StrategyPromotionWalkForwardBackend | None = None,
        paper_sources: tuple[PaperPortfolioViewSource | NativeMinuteForwardViewSource, ...] = (),
        template_binding: ExperimentTemplateBinding | None = None,
        native_results: MinuteSealedReplayReader | None = None,
    ) -> None:
        from rquant.strategy_promotion_walk_forward import StrategyPromotionWalkForwardBackend
        from rquant.paper_research_runtime import NativeMinuteForwardViewSource

        if (
            type(registry) is not ExperimentRegistry
            or type(platform) is not ExperimentPlatformStore
            or type(projection) is not ExperimentPrivateProjectionReader
        ):
            raise TypeError("promotion requires its concrete original experiment authorities")
        if projection.registry.path != registry.path:
            raise ValueError("private projection belongs to another original Registry")
        if (
            platform.registry is not registry
            or results is not None
            and type(results) is not PortfolioResultReader
            or template_results is not None
            and type(template_results) is not StrategyTemplateSealedResultReader
        ):
            raise TypeError("promotion result authorities differ from original installed chain")
        if walk_forward is not None and (
            type(walk_forward) is not StrategyPromotionWalkForwardBackend
            or walk_forward.registry is not registry
        ):
            raise TypeError("WF source must use the same original Registry")
        if (
            type(paper_sources) is not tuple
            or len(paper_sources) > 64
            or any(type(source) not in {PaperPortfolioViewSource, NativeMinuteForwardViewSource}
                   for source in paper_sources)
        ):
            raise TypeError("forward requires bounded concrete original paper sources")
        if len(
            {source.runtime.state.configuration.binding.account_id for source in paper_sources}
        ) != len(paper_sources):
            raise ValueError("forward paper account installation contains duplicates")
        self.registry, self.platform, self.projection = registry, platform, projection
        self.results, self.template_results = results, template_results
        self.independence_resolver = independence_resolver
        self.walk_forward, self.paper_sources = walk_forward, paper_sources
        if template_binding is not None and (
            type(template_binding) is not ExperimentTemplateBinding
            or template_binding.private.identity() != template_binding.expected_private_identity
            or template_binding.original.identity() != template_binding.expected_original_identity
            or walk_forward is not None
            and walk_forward.runs.store is not template_binding.private
        ):
            raise TypeError(
                "promotion template references require the exact original private binding"
            )
        self.template_binding = template_binding
        if native_results is not None:
            from rquant.minute_backtest_artifact import MinuteSealedReplayReader

            if type(native_results) is not MinuteSealedReplayReader or (
                native_results.reader.path != projection.jobs.path
                or native_results.submission_facade.experiment_registry is not registry
            ):
                raise TypeError("native result source differs from its original private Lab/Registry")
        self.native_results = native_results

    def _read(
        self, fact: ExperimentAttemptFact, family: ExperimentFamilyFact
    ) -> ExperimentResultData:
        return read_experiment_result(
            fact,
            family,
            results=self.results,
            authority=self.projection.authority,
            template_results=self.template_results,
            native_results=self.native_results,
        )

    def _registration(self, fact: ExperimentAttemptFact) -> StrategySpecRegistration:
        job = self.projection.jobs.get_job(fact.child.job_id)
        if job is None:
            raise ValueError("original experiment job is unavailable")
        from rquant.minute_backtest_formal import PreparedMinuteRequest

        prepared = self.projection.authority.authorize(job, fact.owner).prepared
        return prepared.frozen.native_registration if isinstance(prepared, PreparedMinuteRequest) else prepared.registration

    def context_read(self, as_of: datetime) -> AbstractContextManager[ExperimentPrivateContextRead]:
        return self.projection.context_read(normalize_aware_utc(as_of))

    def _reference(
        self, fact: ExperimentAttemptFact, result: ExperimentResultData
    ) -> SealedPromotionResult:
        completed = fact.attempt.completed_at
        if completed is None:
            raise ValueError("original execution completion absent")
        job = self.projection.jobs.get_job(result.job_id)
        if job is None:
            raise ValueError("full seal lost its original job")
        return SealedPromotionResult(
            job_id=result.job_id,
            spec_hash=result.spec_hash,
            manifest_hash=result.manifest_hash,
            result_hash=result.result_hash,
            input_hash=result.input_hash,
            content_hash=(result.template.content_hash if result.template else
                result.native.content_hash if result.native else result.basis_hash),
            available_at=max(completed, job.updated_at),
        )

    def validation(
        self,
        target: StrategyPromotionTarget,
        family: ExperimentFamilyFact,
        fact: ExperimentAttemptFact,
    ) -> tuple[BoundValidationPromotionEvidence, ExperimentResultData, tuple[Decimal, ...]]:
        result = self._read(fact, family)
        registration = self._registration(fact)
        if (
            fact.owner != target.owner_id
            or registration.spec.parameter_fingerprint != target.parameter_fingerprint
            or fact.attempt.spec.cost_model_fingerprint != target.cost_fingerprint
            or (
                registration.logical_id,
                registration.version,
                registration.fingerprint,
                registration.record_hash,
                registration.spec.spec_fingerprint,
            )
            != (
                target.strategy_id,
                target.head.version,
                target.head.registration_fingerprint,
                target.head.record_hash,
                target.head.spec_fingerprint,
            )
            or not isinstance(fact.configuration, NativeMinuteConfiguration)
            and fact.attempt.spec.strategy_spec_fingerprint != target.head.spec_fingerprint
        ):
            raise ValueError("original candidate differs from target version/parameters/cost")
        if isinstance(fact.configuration, NativeMinuteConfiguration) and (
            result.native is None or result.native.target != target
            or canonical_sha256(result.native.execution_costs) != target.cost_fingerprint
        ):
            raise ValueError("native complete result differs from its exact target/cost")
        if result.template is not None and (result.template.strategy_id, result.template.head) != (
            target.strategy_id,
            target.head,
        ):
            raise ValueError("derived candidate does not own this exact template version")
        phase = next(
            (
                p
                for p in result.phases
                if p.phase == ("outer" if family.phase == "outer" else "validation")
            ),
            None,
        )
        if phase is None or not phase.curves:
            raise ValueError("complete fixed validation phase unavailable")
        window = (
            fact.attempt.spec.validation_range
            if family.phase == "search"
            else fact.attempt.spec.frozen_outer_test_range
        )
        if phase.window != window:
            raise ValueError("original phase interval differs")
        dates = tuple(p.trade_date for p in phase.curves)
        view = validation_view(result, window, dates)
        returns = tuple(Decimal(str(p.daily_return)) for p in view.curves)
        summary = performance_summary(
            pd.Series([float(v) for v in returns], index=pd.to_datetime(dates), dtype=float)
        )
        trades = tuple(
            t
            for t in result.performance.round_trips
            if window.start_date <= t.entry_date <= t.exit_date <= window.end_date
        )
        stats = summarize_round_trips(trades).overall
        manifest = self.registry.get_hypothesis_family(fact.attempt.spec.hypothesis_family)
        if summary.total_return is None or summary.max_drawdown is None:
            raise ValueError("original full validation valuation is incomplete")
        bound = BoundValidationPromotionEvidence(
            target=target,
            parent_family=manifest.hypothesis_family,
            parent_manifest_hash=manifest.manifest_id,
            parent_count=manifest.hypothesis_count,
            experiment_id=fact.attempt.spec.experiment_id,
            reference=self._reference(fact, result),
            train_window=fact.attempt.spec.train_range,
            window=window,
            full_dates_hash=canonical_sha256(dates),
            returns_hash=canonical_sha256(returns),
            trades_hash=canonical_sha256(tuple(t.__dict__ for t in trades)),
            closed_trades=stats.count,
            net_return=Decimal(str(summary.total_return)),
            max_drawdown=Decimal(str(abs(summary.max_drawdown))),
            win_rate=Decimal(str(stats.win_rate or 0)),
            sharpe=None if summary.sharpe is None else Decimal(str(summary.sharpe)),
            full_costs=(result.native.execution_costs.is_alignment_eligible if result.native is not None
                else fact.configuration.execution_cost_spec.is_alignment_eligible),
        )
        return bound, view, returns

    def read(
        self,
        target: StrategyPromotionTarget,
        *,
        family_id: str,
        experiment_id: str,
        as_of: datetime,
        register_statistics: bool = False,
    ) -> PromotionEvidenceBundle:
        observed = normalize_aware_utc(as_of)
        with self.context_read(observed) as current:
            snapshot = current.snapshot
            if snapshot is None or target.owner_id in snapshot.truncated_owners:
                raise ValueError("complete original private family is unavailable")
            family = next(
                (
                    f
                    for f in snapshot.families
                    if (f.owner, f.family_id, f.phase) == (target.owner_id, family_id, "search")
                ),
                None,
            )
            if family is None:
                raise PermissionError("original family is not owned by target")
            facts = tuple(
                f for f in snapshot.attempts if (f.owner, f.family_id) == (target.owner_id, family_id)
            )
            selected = next((f for f in facts if f.attempt.spec.experiment_id == experiment_id), None)
            if selected is None or len(facts) != family.planned_count:
                raise ValueError("complete original parent search is absent")
            if selected.result_hash is None:
                return PromotionEvidenceBundle(
                    target=target, observed_at=observed, missing=("原验证结果尚未完整封存。",)
                )
            validation, selected_view, selected_returns = self.validation(target, family, selected)
            missing = []
            receipt = None
            adjusted = None
            adjustment_hash = None
            outer = None
            prior = self.registry.sealed_family_outcome_receipt(selected.attempt.spec.hypothesis_family)
            full_results = None
            if register_statistics or prior is not None:
                try:
                    full_results = self._statistics_inputs(
                        target, family, facts,
                        selected_result=(
                            selected.attempt.spec.experiment_id, validation,
                            selected_view, selected_returns,
                        ),
                    )
                except PromotionStatisticsUnavailableError as exc:
                    if prior is not None:
                        raise ValueError(
                            "recorded original statistical evidence became unavailable"
                        ) from exc
                    missing.append(str(exc))
            grants = tuple(
                g
                for g in self.platform.list_outer_grants(target.owner_id)
                if g.family_id == family_id and g.experiment_id == experiment_id
            )
            if len(grants) > 1:
                raise ValueError("original unique outer grant duplicated")
            if grants:
                grant = grants[0]
                outer_family = next(
                    (
                        f
                        for f in snapshot.families
                        if f.parent_family_id == family_id
                        and f.selected_search_experiment_id == experiment_id
                        and f.phase == "outer"
                    ),
                    None,
                )
                outer_fact = (
                    None
                    if outer_family is None
                    else next(
                        (
                            f
                            for f in snapshot.attempts
                            if f.family_id == outer_family.family_id and f.owner == target.owner_id
                        ),
                        None,
                    )
                )
                if outer_fact is not None and outer_fact.result_hash is not None:
                    b, _, _ = self.validation(target, outer_family, outer_fact)
                    if (
                        b.window != grant.outer_range
                        or grant.config != selected.configuration
                        or outer_fact.configuration.model_dump(exclude={"start_date", "end_date"})
                        != grant.config.model_dump(exclude={"start_date", "end_date"})
                    ):
                        raise ValueError("outer differs from unique original fixed parameters or range")
                    outer = BoundOuterPromotionEvidence(
                        target=target,
                        parent_experiment_id=experiment_id,
                        outer_experiment_id=outer_fact.attempt.spec.experiment_id,
                        grant_hash=canonical_sha256(grant),
                        reference=b.reference,
                        window=b.window,
                        net_return=b.net_return,
                    )
        # Original Registry writes happen only after both readonly scopes finish.
        if full_results is not None:
            try:
                receipt, adjusted, adjustment_hash = self._statistics(
                    target, family, facts, selected, observed, prior, full_results=full_results,
                )
            except PromotionStatisticsUnavailableError as exc:
                if prior is not None:
                    raise ValueError(
                        "recorded original statistical evidence became unavailable"
                    ) from exc
                missing.append(str(exc))
        return PromotionEvidenceBundle(
            target=target,
            validation=validation,
            family_receipt=receipt,
            adjusted_p=adjusted,
            adjustment_hash=adjustment_hash,
            outer=outer,
            observed_at=observed,
            missing=tuple(missing),
        )

    def _statistics_inputs(
        self, target: StrategyPromotionTarget, family: ExperimentFamilyFact,
        facts: tuple[ExperimentAttemptFact, ...], *,
        selected_result: _FullValidation | None = None,
    ) -> tuple[_FullValidation, ...]:
        if any(
            f.attempt.status in (ExperimentStatus.REGISTERED, ExperimentStatus.RUNNING)
            or f.attempt.status in (ExperimentStatus.EXECUTED, ExperimentStatus.SUCCEEDED)
            and f.result_hash is None
            for f in facts
        ):
            raise PromotionStatisticsUnavailableError("父搜索尚未全部完成，不能登记统计结果。")
        full_results = []
        for fact in facts:
            if fact.attempt.status in (ExperimentStatus.EXECUTED, ExperimentStatus.SUCCEEDED):
                registration = self._registration(fact)
                if isinstance(fact.configuration, NativeMinuteConfiguration):
                    candidate = fact.configuration.selection.target
                else:
                    candidate = target.model_copy(
                        update={
                            "strategy_id": registration.logical_id,
                            "head": target.head.model_copy(
                                update={
                                    "version": registration.version,
                                    "registration_fingerprint": registration.fingerprint,
                                    "record_hash": registration.record_hash,
                                    "spec_fingerprint": registration.spec.spec_fingerprint,
                                }
                            ),
                            "parameter_fingerprint": registration.spec.parameter_fingerprint,
                            "cost_fingerprint": fact.attempt.spec.cost_model_fingerprint,
                        }
                    )
                identifier = fact.attempt.spec.experiment_id
                if (
                    selected_result is not None
                    and selected_result[0] == identifier
                    and selected_result[1].target == candidate
                    and selected_result[1].reference.job_id == fact.child.job_id
                    and selected_result[1].reference.result_hash == fact.result_hash
                ):
                    _, bound, view, complete_returns = selected_result
                else:
                    bound, view, complete_returns = self.validation(candidate, family, fact)
                full_results.append((identifier, bound, view, complete_returns))
        return tuple(full_results)

    def _statistics(
        self,
        target: StrategyPromotionTarget,
        family: ExperimentFamilyFact,
        facts: tuple[ExperimentAttemptFact, ...],
        selected: ExperimentAttemptFact,
        observed: datetime,
        prior: SealedFamilyOutcomeReceipt | None,
        *, full_results: tuple[_FullValidation, ...] | None = None,
    ) -> tuple[SealedFamilyOutcomeReceipt, Decimal, str]:
        if full_results is None:
            full_results = self._statistics_inputs(target, family, facts)
        values = {identifier: view for identifier, _, view, _ in full_results}
        bounds = {identifier: bound for identifier, bound, _, _ in full_results}
        returns = {identifier: daily for identifier, _, _, daily in full_results}
        statistical_facts = facts
        independent = None
        if prior is not None:
            original = {a.spec.experiment_id: a for a in prior.attempts}
            if set(original) != {f.attempt.spec.experiment_id for f in facts}:
                raise ValueError("recorded parent attempt set differs")
            statistical_facts = tuple(
                f.model_copy(
                    update={
                        "attempt": f.attempt.model_copy(
                            update={
                                "status": original[f.attempt.spec.experiment_id].original_status,
                                "outcome": None
                                if original[f.attempt.spec.experiment_id].original_status
                                is ExperimentStatus.EXECUTED
                                else original[f.attempt.spec.experiment_id].outcome,
                            }
                        )
                    }
                )
                for f in facts
            )
            independent = self.projection.authority.evidence(
                target.owner_id, family.family_id, prior.overfit_evidence_hash
            ).independence
        elif self.independence_resolver is not None:
            independent = self.independence_resolver(
                family, statistical_facts, tuple(values.values())
            )
        elif family.evidence_id is not None:
            independent = self.projection.authority.evidence(
                target.owner_id, family.family_id, family.evidence_id
            ).independence
            dates = next(
                (tuple(point.trade_date for point in value.curves) for value in values.values()), ()
            )
            if independent is not None and independent.period_end_dates != dates:
                raise PromotionStatisticsUnavailableError("原独立性证据未绑定完整验证区间。")
        overfit = build_overfit_evidence(
            family,
            statistical_facts,
            read=lambda fact: values[fact.attempt.spec.experiment_id],
            independence=independent,
        )
        stats = {value.experiment_id: value for value in overfit.statistics}
        ranked = sorted(values, key=lambda identifier: (-bounds[identifier].net_return, identifier))
        attached = []
        for fact in facts:
            identifier = fact.attempt.spec.experiment_id
            outcome = None
            reference = None
            if identifier in values:
                bound = bounds[identifier]
                reference = bound.reference
                psr = stats[identifier].psr
                outcome = validation_outcome(
                    experiment_id=identifier,
                    reference_hash=reference.result_hash,
                    returns=returns[identifier],
                    dates=tuple(point.trade_date for point in values[identifier].curves),
                    trade_count=bound.closed_trades,
                    max_drawdown=bound.max_drawdown,
                    win_rate=bound.win_rate,
                    parent_n=family.search_count,
                    rank=ranked.index(identifier) + 1,
                    raw_p=None if psr is None else Decimal(str(1 - psr.probability)),
                )
            original_status = (
                fact.attempt.status
                if prior is None
                else next(
                    a.original_status for a in prior.attempts if a.spec.experiment_id == identifier
                )
            )
            attached.append(
                SealedFamilyAttemptOutcome(
                    spec=fact.attempt.spec,
                    original_status=original_status,
                    execution_completed_at=fact.attempt.completed_at,
                    reference=reference,
                    outcome=outcome,
                )
            )
        receipt = SealedFamilyOutcomeReceipt(
            manifest=self.registry.get_hypothesis_family(selected.attempt.spec.hypothesis_family),
            attempts=tuple(attached),
            overfit_evidence_hash=overfit.evidence_id,
            recorded_at=observed if prior is None else prior.recorded_at,
        )
        if prior is None:
            self.platform.save_evidence(overfit)
        elif overfit.evidence_id != prior.overfit_evidence_hash:
            raise ValueError("original persisted statistical proof differs")
        self.registry.record_sealed_family_outcomes(receipt, recorded_at=receipt.recorded_at)
        outcomes = self.registry.adjust_hypothesis_family(
            receipt.manifest.hypothesis_family, adjusted_at=receipt.recorded_at
        )
        chosen = next(
            value
            for value in outcomes
            if value.experiment_id == selected.attempt.spec.experiment_id
        )
        if chosen.adjusted_p_value is None:
            raise ValueError("original full-parent adjustment is absent")
        return receipt, chosen.adjusted_p_value, canonical_sha256(outcomes)

    def plan_walk_forward(
        self,
        request: RunStrategyWalkForward,
        *,
        metadata_identity: StrategyAuthoringIdentity,
        as_of: datetime,
    ) -> PromotionWalkForwardPlan:
        from rquant.strategy_promotion_walk_forward import build_walk_forward_plan, build_native_walk_forward_plan
        self.projection.authority.refresh_live_identity()
        snapshot = self.projection.snapshot(normalize_aware_utc(as_of))
        if snapshot is None or request.target.owner_id in snapshot.truncated_owners:
            raise ValueError("complete original private family is unavailable")
        family = next(
            (
                f
                for f in snapshot.families
                if (f.owner, f.family_id, f.phase)
                == (request.target.owner_id, request.selection.family_id, "search")
            ),
            None,
        )
        if family is None:
            raise PermissionError("WF parent is not owned by target")
        fact = next(
            (
                f
                for f in snapshot.attempts
                if (f.owner, f.family_id, f.attempt.spec.experiment_id)
                == (request.target.owner_id, family.family_id, request.selection.experiment_id)
            ),
            None,
        )
        if fact is None or fact.result_hash is None:
            raise ValueError("WF original parent is not completely sealed")
        parent, _, _ = self.validation(request.target, family, fact)
        if parent.reference.available_at > normalize_aware_utc(as_of):
            raise ValueError("WF original parent is not yet visible")
        result = self._read(fact, family)
        if isinstance(fact.configuration, NativeMinuteConfiguration):
            if self.native_results is None or self.walk_forward is None or self.walk_forward.native is None:
                raise ValueError("native original WF owner/complete reader is not installed")
            job = self.projection.jobs.get_job(fact.child.job_id)
            if job is None:
                raise ValueError("native original parent job disappeared")
            prepared = self.projection.authority.authorize(job, fact.owner).prepared
            dates = tuple(day for day in prepared.frozen.runtime.market_calendar.open_dates
                if parent.train_window.start_date <= day <= parent.window.end_date
                and not parent.train_window.end_date < day < parent.window.start_date)
            phase_dates = tuple(point.trade_date for phase in result.phases
                if phase.phase in ("training", "validation") for point in phase.curves)
            if phase_dates != dates:
                raise ValueError("native WF full original train/validation calendar differs")
            return build_native_walk_forward_plan(request, metadata_identity=metadata_identity,
                parent=parent, selection=fact.configuration.selection, dates=dates,
                calendar_source_identity=prepared.frozen.runtime.market_calendar.content_sha256,
                protocol=family.request.protocol)
        if result.template is None:
            raise ValueError("WF original parent is not an authored template")
        job = self.projection.jobs.get_job(fact.child.job_id)
        if job is None:
            raise ValueError("WF original parent job disappeared")
        original = self.projection.authority.authorize(job, fact.owner).prepared.frozen.request
        dates = tuple(
            day
            for day in original.calendar.dates
            if parent.train_window.start_date <= day <= parent.window.end_date
            and not parent.train_window.end_date < day < parent.window.start_date
        )
        phase_dates = tuple(
            point.trade_date
            for phase in result.phases
            if phase.phase in ("training", "validation")
            for point in phase.curves
        )
        if phase_dates != dates:
            raise ValueError("WF full original train/validation calendar differs")
        return build_walk_forward_plan(
            request,
            metadata_identity=metadata_identity,
            parent=parent,
            dates=dates,
            calendar_source_identity=original.calendar.source_identity,
            initial_cash=original.initial_cash,
        )

    def read_forward(
        self,
        target: StrategyPromotionTarget,
        selection: PromotionEvidenceSelection,
        *,
        state: StrategyPromotionState,
        as_of: datetime,
    ) -> BoundForwardPromotionEvidence:
        matches = tuple(
            source
            for source in self.paper_sources
            if source.runtime.state.configuration.binding.account_id == selection.paper_account_id
            and source.runtime.state.configuration.binding.owner_id == target.owner_id
        )
        if len(matches) != 1 or selection.band_job_id is None:
            raise ValueError("同版本模拟账户或原回测区间尚未配置")
        source = matches[0]
        runtime = source.runtime
        configuration = runtime.state.refresh_configuration()
        calendar = runtime.calendar
        reader = source.research_results
        if calendar is None or reader is None:
            raise ValueError("原完整日历或封存区间读取尚不可用")
        analysis = reader.read(
            account_id=configuration.binding.account_id,
            job_id=selection.band_job_id,
            owner_id=target.owner_id,
            as_of=as_of,
        )
        if analysis is None or analysis.band is None:
            raise ValueError("原回测区间尚未完整封存")
        with runtime.state._connection() as connection:
            row = connection.execute(
                "SELECT owned_body FROM paper_research_admissions WHERE command_id=?",
                (str(selection.band_job_id),),
            ).fetchone()
        if row is None:
            raise ValueError("原区间请求尚未登记")
        command = OwnedRunPaperPortfolioResearch.model_validate_json(row[0])
        reader.backend.validate(command)
        if command.backtest_job_id is None:
            raise ValueError("原同版本完整回测来源尚未安装")
        from rquant.paper_research_runtime import NativeMinuteForwardViewSource
        if type(source) is NativeMinuteForwardViewSource:
            if type(configuration) is not NativeMinuteForwardConfiguration:
                raise TypeError("原生前向账户配置来源不一致")
            original = runtime.band_input(job_id=command.backtest_job_id,
                comparison_dates=analysis.band.dates, as_of=as_of)
            view = source.read(as_of=as_of)
            if view.status != "complete" or view.frame is None:
                raise ValueError("原生前向完整账本或净值尚不可用")
            frame = view.frame
        else:
            backtests = reader.backend.preparer.backtest_reader
            if type(backtests) is not PaperBacktestSourceReader:
                raise ValueError("原同版本完整回测来源尚未安装")
            original = backtests.band_input(
                configuration=configuration,
                job_id=command.backtest_job_id,
                calendar=calendar,
                comparison_dates=analysis.band.dates,
                as_of=as_of,
            )
            material = runtime.materials.latest(decision_at=as_of)
            prices = {
                fact.ts_code: fact.valuation_price
                for fact in material.facts
                if fact.valuation_price is not None and fact.trading_status in ("normal", "suspended")
            }
            frame = runtime.ledger_source_for(source.broker).read(
                configuration=configuration, as_of=as_of, prices=prices
            )
        value = bind_forward_evidence(
            target,
            state=state,
            configuration=configuration,
            frame=frame,
            nav=source.views.nav_series(),
            calendar=calendar,
            band=analysis,
            band_input=original,
            as_of=as_of,
        )
        if runtime.state.refresh_configuration() != configuration:
            raise ValueError("原模拟配置在读取期间发生变化")
        return value

    def read_selection(
        self,
        target: StrategyPromotionTarget,
        selection: PromotionEvidenceSelection,
        *,
        state: StrategyPromotionState,
        as_of: datetime,
        register_statistics: bool = False,
    ) -> PromotionEvidenceBundle:
        bundle = self.read(
            target,
            family_id=selection.family_id,
            experiment_id=selection.experiment_id,
            as_of=as_of,
            register_statistics=register_statistics,
        )
        missing = list(bundle.missing)
        folds = ()
        forward = None
        if selection.walk_forward_id is not None:
            if self.walk_forward is None:
                missing.append("固定参数的验证折尚未配置")
            else:
                folds = self.walk_forward.read_folds(
                    selection.walk_forward_id, target=target, as_of=as_of
                )
        if state.paper_approval_hash is not None:
            try:
                forward = self.read_forward(target, selection, state=state, as_of=as_of)
            except (ValueError, PaperBrokerReconciliationError):
                missing.append("原完整模拟记录或回测区间尚不可用。")
        return PromotionEvidenceBundle.model_validate(
            bundle.model_copy(
                update={"folds": folds, "forward": forward, "missing": tuple(missing)}
            ).model_dump(mode="python")
        )
