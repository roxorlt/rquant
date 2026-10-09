from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import json
import re
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from rquant.experiment_registry import (
    DateRange,
    ExperimentOutcome,
    ExperimentRegistry,
    ExperimentRegistryError,
    ExperimentSpec,
    ExperimentStatus,
    HypothesisFamilyManifest,
)
from rquant.strategy_promotion_contracts import (
    SealedFamilyAttemptOutcome,
    SealedFamilyOutcomeReceipt,
    SealedPromotionResult,
)
from rquant.strategy_promotion_evidence import validation_outcome

if TYPE_CHECKING:
    from rquant.backtest.contracts import SSECalendar
    from rquant.experiment_platform import ExperimentSearchRequest
    from rquant.experiment_platform_projection import ExperimentAttemptFact, ExperimentFamilyFact
    from rquant.paper_portfolio_band import PaperBacktestBandInput
    from rquant.paper_portfolio_ledger import PaperPortfolioLedgerFrame
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration
    from rquant.paper_portfolio_views import PaperDailyNav
    from rquant.paper_research_artifact import PaperResearchSealedAnalysis
    from rquant.strategy_promotion_contracts import StrategyPromotionState, StrategyPromotionTarget
    from rquant.web.experiment_platform_models import ExperimentResultData

NOW = datetime(2026, 10, 6, 8, tzinfo=UTC)


def test_native_parameter_projection_preserves_all_original_definition_values() -> None:
    from rquant.experiment_platform_evidence import native_parameter_projection
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
    from rquant.web.experiment_platform_models import ExperimentNativeResultIdentity
    from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile
    from rquant.strategy_catalog_source import _PARAMETER_LABELS, _display_parameter
    from tests.unit.test_minute_backtest_producer import original_fixture

    profile = MinuteReplayExecutionProfile.model_validate_json(json.dumps(
        original_fixture()["minute_replay"]["execution_profile"]))
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=profile.paper_policy.producer_commit)
    for native_id in ("n_shape", "auction_gap", "growth_board_surge"):
        definition = registry.load_definition(native_id, 1)
        original = definition.spec.model_dump(mode="json")["parameters"]
        projected = native_parameter_projection(definition.spec)
        assert {item.name: item.value for item in projected} == original
        assert tuple(item.name for item in projected) == tuple(sorted(original))
        assert {item.name: (item.label, item.display_value) for item in projected} == {
            name: (_PARAMETER_LABELS[name], _display_parameter(name, value))
            for name, value in definition.spec.parameters.items()}
    fields = ExperimentNativeResultIdentity.model_fields
    assert "parameters" in fields and "execution_profile" in fields


def test_native_full_daily_curve_and_actual_fill_costs() -> None:
    from rquant.experiment_platform_evidence import native_curve_and_performance
    from rquant.minute_backtest_runner import MinuteRuntimeReplayResult
    from tests.unit.test_minute_backtest_producer import original_fixture

    # This is the retained original runner output, not a new sealed-result proof.
    replay = MinuteRuntimeReplayResult.model_validate_json(json.dumps(original_fixture()["minute_replay"]))
    dates = tuple(day.trade_date for day in replay.daily_valuations)
    curves, performance, _ = native_curve_and_performance(replay, dates)
    previous = replay.execution_profile.initial_cash
    for point, day in zip(curves, replay.daily_valuations, strict=True):
        assert point.nav == float(day.account.nav / replay.execution_profile.initial_cash)
        assert point.daily_return == float(day.account.nav / previous - 1)
        previous = day.account.nav
    assert performance.round_trip_analysis.overall.count == 1
    assert performance.round_trips[0].entry_fee == float(replay.fills[0].total_fees)
    assert performance.round_trips[0].exit_fee == float(replay.fills[1].total_fees)
    with pytest.raises(ValueError, match="complete.*calendar"):
        native_curve_and_performance(replay, dates[:-1])
    gap = replay.model_copy(update={"daily_status": "unavailable"})
    with pytest.raises(ValueError, match="incomplete"):
        native_curve_and_performance(gap, dates)


@pytest.fixture
def original_promotion_source(tmp_path: Path) -> Iterator[tuple]:
    from rquant.strategy_authoring_commands import StrategyTemplateHead
    from rquant.strategy_promotion_contracts import (
        PromotionEvidenceSelection,
        StrategyPromotionTarget,
    )
    from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource
    from tests.unit.test_experiment_platform import NOW as ORIGINAL_NOW
    from tests.unit.test_experiment_platform_results import complete_family

    context = complete_family.__wrapped__(tmp_path)
    store, projection, results, authorities = next(context)
    try:
        # The original isolated statistics fixture has a fixed synthetic ledger,
        # not a physical Lab DB. Adapt only its read scope; real worker proofs are separate.
        @contextmanager
        def fixed_fixture_read(*, label: str) -> Iterator[None]:
            del label
            yield None

        from rquant.runtime_contracts import canonical_sha256
        projection.jobs._read_snapshot = fixed_fixture_read
        projection.jobs._storage_revision = lambda: canonical_sha256(tuple(
            (str(key), value.job) for key, value in sorted(authorities.items(), key=lambda item: str(item[0]))))
        observed = ORIGINAL_NOW + timedelta(seconds=3)
        snapshot = projection.snapshot(observed)
        family = snapshot.families[0]
        fact = next(f for f in snapshot.attempts if f.index == 0)
        source = StrategyPromotionEvidenceSource(
            registry=store.registry,
            platform=store,
            projection=projection,
            results=results,
            template_results=None,
        )
        registration = source._registration(fact)
        target = StrategyPromotionTarget(
            source_kind="builtin",
            owner_id="alice",
            strategy_id=registration.logical_id,
            name="原组合",
            head=StrategyTemplateHead(
                version=registration.version,
                registration_fingerprint=registration.fingerprint,
                record_hash=registration.record_hash,
                spec_fingerprint=registration.spec.spec_fingerprint,
            ),
            parameter_fingerprint=registration.spec.parameter_fingerprint,
            cost_fingerprint=fact.attempt.spec.cost_model_fingerprint,
        )
        selection = PromotionEvidenceSelection(
            family_id=family.family_id, experiment_id=fact.attempt.spec.experiment_id
        )
        # Original full payload and physical seal. Only Lab status transport is synthetic.
        yield source, target, selection, observed, authorities
    finally:
        context.close()


def test_original_full_reader_keeps_validation_when_formal_p_is_missing(
    original_promotion_source: tuple,
) -> None:
    source, target, selection, observed, _ = original_promotion_source
    bundle = source.read(
        target,
        family_id=selection.family_id,
        experiment_id=selection.experiment_id,
        as_of=observed,
        register_statistics=True,
    )
    assert bundle.validation is not None and bundle.validation.reference.result_hash
    assert bundle.family_receipt is None and bundle.adjusted_p is None and bundle.missing
    assert source.registry.get_attempt(selection.experiment_id).status is ExperimentStatus.EXECUTED
    assert source.registry.sealed_family_outcome_receipt(selection.family_id) is None


def test_original_full_candidate_cannot_be_rebound_to_another_owner_or_version(
    original_promotion_source: tuple,
) -> None:
    source, target, selection, observed, _ = original_promotion_source
    for changed in (
        target.model_copy(update={"owner_id": "bob"}),
        target.model_copy(update={"parameter_fingerprint": "f" * 64}),
        target.model_copy(
            update={"head": target.head.model_copy(update={"record_hash": "f" * 64})}
        ),
    ):
        with pytest.raises((PermissionError, ValueError, ExperimentRegistryError)) as refused:
            source.read(
                changed,
                family_id=selection.family_id,
                experiment_id=selection.experiment_id,
                as_of=observed,
            )
        reason = refused.value
        if isinstance(reason, ExperimentRegistryError):
            # The original read transaction wraps its body's ValueError. The
            # cause must remain the exact owner/version refusal, not an I/O gap.
            assert isinstance(reason.__cause__, ValueError)
            reason = reason.__cause__
        assert re.search("owner|owned|target|version|parameter", str(reason))


def test_real_full_validation_stats_persist_original_proof_and_restart_without_resolver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tests.unit.test_experiment_platform as platform_fixture
    import tests.unit.test_portfolio_backtest as market_fixture
    from rquant.backtest.contracts import BacktestRequest, SSECalendar
    from rquant.experiment_platform_evidence import ExperimentIndependenceEvidence
    from rquant.portfolio_backtest_source import PortfolioExperimentProtocol
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource

    raw_request = market_fixture._request
    original_search = platform_fixture.search
    days = tuple(
        date(2026, 4, 1) + timedelta(days=i)
        for i in range(96)
        if (date(2026, 4, 1) + timedelta(days=i)).weekday() < 5
    )
    previous = date(2026, 3, 31)
    following = days[-1] + timedelta(days=3)
    calendar = SSECalendar(
        source_identity="9" * 64,
        coverage_start=previous,
        coverage_end=following,
        dates=(previous,) + days + (following,),
    )
    protocol = PortfolioExperimentProtocol(
        train_range=DateRange(start_date=days[0], end_date=days[5]),
        validation_range=DateRange(start_date=days[6], end_date=days[-1]),
        frozen_outer_test_range=DateRange(
            start_date=days[-1] + timedelta(days=1), end_date=days[-1] + timedelta(days=20)
        ),
    )

    def long_search(**changes: object) -> ExperimentSearchRequest:
        value = original_search(**changes)
        return value.model_copy(
            update={
                "protocol": protocol,
                "base_config": value.base_config.model_copy(
                    update={"start_date": days[0], "end_date": days[-1]}
                ),
            }
        )

    def long_request(selections: tuple[str | None, ...], **kwargs: object) -> BacktestRequest:
        value = raw_request(selections, **kwargs)
        items = []
        price = Decimal("1.00")
        for index, day in enumerate(days):
            prev = previous if index == 0 else days[index - 1]
            close = (price * (Decimal("1.03") if index % 3 else Decimal("1.02"))).quantize(
                Decimal(".01")
            )
            original = market_fixture._day(day, prev, market_fixture._CODES[index % 2])
            instruments = tuple(
                item.model_copy(
                    update={"decision_price": price, "open_price": price, "close_price": close}
                )
                for item in original.instruments
            )
            items.append(original.model_copy(update={"instruments": instruments}))
            price = close
        return BacktestRequest.model_validate(
            value.model_copy(update={"calendar": calendar, "days": tuple(items)}).model_dump(
                mode="python"
            )
        )

    monkeypatch.setattr(platform_fixture, "search", long_search)
    monkeypatch.setattr(market_fixture, "_request", long_request)
    context = original_promotion_source.__wrapped__(tmp_path)
    source, target, selection, observed, _ = next(context)
    try:
        calls = []

        def explicit_synthetic_proof(
            family: ExperimentFamilyFact,
            facts: tuple[ExperimentAttemptFact, ...],
            values: tuple[ExperimentResultData, ...],
        ) -> ExperimentIndependenceEvidence:
            calls.append(family.family_id)
            dates = tuple(point.trade_date for point in values[0].curves)
            hashes = tuple(value.result_hash for value in values)
            return ExperimentIndependenceEvidence(
                evidence_id="explicit-synthetic-validation-independence",
                body_hash=canonical_sha256({"dates": dates, "hashes": hashes}),
                family_id=family.family_id,
                period_end_dates=dates,
                result_hashes=hashes,
                independent_observations=len(dates),
                independent_trial_count=4,
                assumptions=("合成市场用于原统计函数的边界验证；此证明不用于真实研究。",),
            )

        source.independence_resolver = explicit_synthetic_proof
        first = source.read(
            target,
            family_id=selection.family_id,
            experiment_id=selection.experiment_id,
            as_of=observed,
            register_statistics=True,
        )
        assert (
            first.family_receipt is not None and first.adjusted_p is not None and not first.missing
        )
        assert first.family_receipt.manifest.hypothesis_count == 4
        assert all(
            item.outcome.outer_test_completed is False for item in first.family_receipt.attempts
        )
        proof = source.projection.authority.evidence(
            target.owner_id, selection.family_id, first.family_receipt.overfit_evidence_hash
        )
        assert proof.independence.period_end_dates == days[6:]
        restored = StrategyPromotionEvidenceSource(
            registry=source.registry,
            platform=source.platform,
            projection=source.projection,
            results=source.results,
            template_results=None,
        )
        again = restored.read(
            target,
            family_id=selection.family_id,
            experiment_id=selection.experiment_id,
            as_of=observed,
            register_statistics=True,
        )
        assert again == first and calls == [selection.family_id]
        assert all(
            source.registry.get_attempt(item.spec.experiment_id).completed_at
            == item.execution_completed_at
            for item in first.family_receipt.attempts
        )
    finally:
        context.close()


def spec(seed: int = 1) -> ExperimentSpec:
    return ExperimentSpec(
        strategy_spec_fingerprint="a" * 64,
        strategy_executable_fingerprint="b" * 64,
        candidate_schema_fingerprint="c" * 64,
        dataset_snapshot_id="d" * 64,
        code_commit="e" * 40,
        parameter_fingerprint=str(seed) * 64,
        hypothesis_family="promotion-family",
        metric_definition_fingerprint="f" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 2, 28)),
        validation_range=DateRange(start_date=date(2025, 3, 1), end_date=date(2025, 6, 30)),
        frozen_outer_test_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 8, 31)),
        cost_model_fingerprint="1" * 64,
        execution_model_fingerprint="2" * 64,
        seed=seed,
    )


def original_family(
    tmp_path: Path,
) -> tuple[ExperimentRegistry, tuple[ExperimentSpec, ...], SealedFamilyOutcomeReceipt]:
    registry = ExperimentRegistry(tmp_path / "experiments.sqlite", managed_trust_root=tmp_path)
    specs = (spec(3), spec(4))
    manifest = HypothesisFamilyManifest(
        hypothesis_family="promotion-family",
        experiment_ids=tuple(s.experiment_id for s in specs),
        search_space_fingerprint="5" * 64,
        metric_definition_fingerprint="f" * 64,
        preregistered_at=NOW - timedelta(days=2),
    )
    registry.register_hypothesis_family(manifest)
    for s in specs:
        registry.register_attempt(s, registered_at=NOW - timedelta(days=1))
        registry.start_attempt(s.experiment_id, started_at=NOW - timedelta(hours=2))
    registry.record_execution_completed(
        specs[0].experiment_id, completed_at=NOW - timedelta(hours=1)
    )
    registry.record_failure(
        specs[1].experiment_id,
        first_error="original failure",
        completed_at=NOW - timedelta(hours=1),
    )
    reference = SealedPromotionResult(
        job_id="00000000-0000-0000-0000-000000000001",
        spec_hash="6" * 64,
        manifest_hash="7" * 64,
        result_hash="8" * 64,
        input_hash="9" * 64,
        content_hash="0" * 64,
        available_at=NOW,
    )
    outcome = ExperimentOutcome(
        experiment_id=specs[0].experiment_id,
        trade_count=30,
        net_return=Decimal(".2"),
        max_drawdown=Decimal(".1"),
        win_rate=Decimal(".6"),
        confidence_lower=Decimal(".1"),
        confidence_upper=Decimal(".3"),
        attempted_configuration_count=2,
        selected_rank=1,
        raw_p_value=Decimal(".01"),
        artifact_hash=reference.result_hash,
        outer_test_completed=False,
    )
    receipt = SealedFamilyOutcomeReceipt(
        manifest=manifest,
        recorded_at=NOW,
        overfit_evidence_hash="a" * 64,
        attempts=(
            SealedFamilyAttemptOutcome(
                spec=specs[0],
                original_status=ExperimentStatus.EXECUTED,
                execution_completed_at=NOW - timedelta(hours=1),
                reference=reference,
                outcome=outcome,
            ),
            SealedFamilyAttemptOutcome(
                spec=specs[1],
                original_status=ExperimentStatus.FAILED,
                execution_completed_at=NOW - timedelta(hours=1),
            ),
        ),
    )
    return registry, specs, receipt


def test_sealed_family_attach_is_legal_complete_immutable_and_keeps_parent_n(
    tmp_path: Path,
) -> None:
    registry, specs, receipt = original_family(tmp_path)
    original_time = registry.get_attempt(specs[0].experiment_id).completed_at
    assert registry.record_sealed_family_outcomes(receipt, recorded_at=NOW) == receipt
    assert registry.get_attempt(specs[0].experiment_id).status is ExperimentStatus.SUCCEEDED
    assert registry.get_attempt(specs[0].experiment_id).completed_at == original_time
    assert registry.get_attempt(specs[1].experiment_id).status is ExperimentStatus.FAILED
    assert registry.record_sealed_family_outcomes(receipt, recorded_at=NOW) == receipt
    outcomes = registry.adjust_hypothesis_family("promotion-family", adjusted_at=NOW)
    assert outcomes[0].adjusted_p_value == Decimal(".02")
    assert (
        registry.policy.minimum_forward_days == 10 and registry.policy.minimum_forward_fills == 20
    )


def test_new_attach_cannot_replace_timing_or_old_running_contract(tmp_path: Path) -> None:
    registry, specs, receipt = original_family(tmp_path)
    with pytest.raises(Exception, match="running"):
        registry.record_success(receipt.attempts[0].outcome, completed_at=NOW)
    altered = receipt.model_copy(
        update={
            "attempts": (
                receipt.attempts[0].model_copy(
                    update={"execution_completed_at": NOW - timedelta(minutes=1)}
                ),
                receipt.attempts[1],
            )
        }
    )
    with pytest.raises(Exception, match="completion"):
        registry.record_sealed_family_outcomes(altered, recorded_at=NOW)
    assert registry.get_attempt(specs[0].experiment_id).status is ExperimentStatus.EXECUTED


def test_receipt_replay_does_not_replace_the_registered_outcome(tmp_path: Path) -> None:
    registry, specs, receipt = original_family(tmp_path)
    registry.record_sealed_family_outcomes(receipt, recorded_at=NOW)
    changed_outcome = receipt.attempts[0].outcome.model_copy(update={"raw_p_value": Decimal(".02")})
    changed = receipt.model_copy(
        update={
            "attempts": (
                receipt.attempts[0].model_copy(update={"outcome": changed_outcome}),
                receipt.attempts[1],
            )
        }
    )
    with pytest.raises(Exception, match="immutable|conflict"):
        registry.record_sealed_family_outcomes(changed, recorded_at=NOW)
    assert registry.get_attempt(specs[0].experiment_id).outcome.raw_p_value == Decimal(".01")


def test_statistical_outcome_uses_original_full_range_and_does_not_clip_interval() -> None:
    values = tuple(Decimal(".002") if i % 3 else Decimal("-.001") for i in range(90))
    dates = tuple(date(2025, 3, 1) + timedelta(days=i) for i in range(90))
    value = validation_outcome(
        experiment_id="1" * 64,
        reference_hash="2" * 64,
        returns=values,
        dates=dates,
        trade_count=30,
        max_drawdown=Decimal(".1"),
        win_rate=Decimal(".6"),
        parent_n=2,
        rank=1,
        raw_p=Decimal(".01"),
    )
    assert not value.outer_test_completed and value.attempted_configuration_count == 2
    assert value.confidence_lower <= value.net_return <= value.confidence_upper
    with pytest.raises(ValueError, match="p"):
        validation_outcome(
            experiment_id="1" * 64,
            reference_hash="2" * 64,
            returns=values,
            dates=dates,
            trade_count=30,
            max_drawdown=Decimal(".1"),
            win_rate=Decimal(".6"),
            parent_n=2,
            rank=1,
            raw_p=None,
        )


def forward_material(
    tmp_path: Path,
) -> tuple[
    StrategyPromotionTarget,
    StrategyPromotionState,
    PaperPortfolioConfiguration,
    PaperPortfolioLedgerFrame,
    tuple[PaperDailyNav, ...],
    SSECalendar,
    PaperResearchSealedAnalysis,
    PaperBacktestBandInput,
    datetime,
]:
    from rquant.backtest.contracts import SSECalendar
    from rquant.experiment_registry import PromotionStage
    from rquant.paper_portfolio_band import (
        PaperBacktestBandInput,
        SealedPaperBacktestReturns,
        SealedPaperDailyReturn,
        execute_paper_backtest_band,
    )
    from rquant.paper_portfolio_views import PaperPortfolioViewStore
    from rquant.paper_research_artifact import PaperResearchSealedAnalysis
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strategy_authoring_commands import StrategyTemplateHead
    from rquant.strategy_promotion_contracts import StrategyPromotionState, StrategyPromotionTarget
    from tests.unit.test_paper_portfolio_ledger_views import close_material, filled, ledger_source
    from tests.unit.test_paper_signal_worker import TRADE_DATE

    broker, basis, _, runtime = filled(tmp_path)
    config = basis.configuration
    dates = tuple(
        TRADE_DATE + timedelta(days=i)
        for i in range(28)
        if (TRADE_DATE + timedelta(days=i)).weekday() < 5
    )
    calendar = SSECalendar(
        source_identity="c" * 64,
        coverage_start=dates[0] - timedelta(days=1),
        coverage_end=dates[-1],
        dates=(dates[0] - timedelta(days=1),) + dates,
    )
    views = PaperPortfolioViewStore(runtime.state)
    at = datetime.combine(dates[-1], datetime.min.time(), tzinfo=UTC) + timedelta(hours=7)
    keeper = broker._connect()
    try:
        for day in dates:
            close = close_material(config, day=day, price="1.00625").model_copy(
                update={"calendar": calendar}
            )
            views.record_close(ledger_source(broker), close, published_at=close.available_at)
        frame = ledger_source(broker).read(
            configuration=config, as_of=at, prices={"600000.SH": Decimal("1.00625")}
        )
    finally:
        keeper.close()
    target = StrategyPromotionTarget(
        source_kind="builtin",
        owner_id="alice",
        strategy_id=config.binding.strategy_id,
        name="原策略",
        head=StrategyTemplateHead(
            version=1,
            registration_fingerprint="d" * 64,
            record_hash="e" * 64,
            spec_fingerprint="f" * 64,
        ),
        parameter_fingerprint=config.binding.parameter_fingerprint,
        cost_fingerprint=canonical_sha256(config.execution_cost_spec),
    )
    state = StrategyPromotionState(
        target=target,
        stage=PromotionStage.PAPER_CANDIDATE,
        revision=2,
        latest_approval_hash="1" * 64,
        paper_approval_hash="1" * 64,
        paper_approved_at=at - timedelta(days=28),
    )
    sealed_returns = SealedPaperBacktestReturns(
        job_id=uuid4(),
        owner_id="alice",
        strategy_id=target.strategy_id,
        strategy_version="1",
        parameter_fingerprint=target.parameter_fingerprint,
        cost_spec_id=config.binding.cost_spec_id,
        calendar_source_identity=calendar.source_identity,
        definition_fingerprint=target.head.registration_fingerprint,
        definition_record_hash=target.head.record_hash,
        spec_hash="2" * 64,
        manifest_hash="3" * 64,
        complete_result_hash="4" * 64,
        backtest_content_hash="5" * 64,
        returns=tuple(
            SealedPaperDailyReturn(trade_date=day, daily_return=Decimal(0)) for day in dates
        ),
    )
    original = PaperBacktestBandInput(
        configuration=config, backtest=sealed_returns, calendar=calendar, comparison_dates=dates
    )
    # Real original sampler/ledger/NAV. Only sealed transport is synthetic in this unit adapter.
    band = execute_paper_backtest_band(original)
    analysis = PaperResearchSealedAnalysis(
        task_name="paper_backtest_band",
        job_id=uuid4(),
        account_id=config.binding.account_id,
        configuration_fingerprint=config.fingerprint,
        configuration_version=config.version,
        spec_hash="6" * 64,
        manifest_hash="7" * 64,
        complete_result_hash="8" * 64,
        result_hash=band.fingerprint,
        completed_at=at,
        band=band,
    )
    return target, state, config, frame, views.nav_series(), calendar, analysis, original, at


def test_native_forward_binds_original_ledger_full_nav_and_native_band(tmp_path: Path) -> None:
    from rquant.experiment_registry import PromotionStage
    from rquant.paper_broker import BrokerCostPolicy, PaperBrokerStore
    from rquant.paper_portfolio_band import NativePaperBacktestBandInput, execute_paper_backtest_band
    from rquant.paper_portfolio_ledger import PaperPortfolioLedgerSource
    from rquant.paper_portfolio_views import PaperDailyNav
    from rquant.paper_research_artifact import PaperResearchSealedAnalysis
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strategy_promotion_evidence import bind_forward_evidence
    from rquant.strategy_promotion_contracts import StrategyPromotionState
    from tests.unit.test_strategy_promotion_contracts import native_band_input

    original = native_band_input()
    configuration = original.configuration
    profile = configuration.execution_profile
    broker = PaperBrokerStore(tmp_path / "native.sqlite", account_id=configuration.binding.account_id,
        initial_cash=profile.initial_cash, cost_policy=BrokerCostPolicy.from_execution_cost_spec(profile.execution_costs))
    keeper = broker._connect()
    try:
        _, head = broker._attestation_head(keeper)
        configuration = configuration.model_copy(update={"binding": configuration.binding.model_copy(
            update={"ledger_id": head["ledger_generation"]})})
        original = NativePaperBacktestBandInput.model_validate({**original.model_dump(mode="python"),
            "configuration": configuration})
        ledger = PaperPortfolioLedgerSource(path=broker.path, account_id=configuration.binding.account_id,
            initial_cash=profile.initial_cash, cost_policy=broker.cost_policy)
        at = datetime(2026, 10, 8, 8, tzinfo=UTC)
        frame = ledger.read(configuration=configuration, as_of=at, prices={})
        nav = []
        for day in original.comparison_dates:
            close_at = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=7)
            close = ledger.read(configuration=configuration, as_of=close_at, prices={})
            nav.append(PaperDailyNav(configuration_fingerprint=configuration.fingerprint,
                account_id=configuration.binding.account_id, trade_date=day,
                calendar_source_identity=original.calendar.source_identity,
                material_fingerprint=canonical_sha256({"synthetic_domain_close": close.fingerprint}),
                ledger_revision=close.ledger_revision, ledger_head_fingerprint=close.head_fingerprint,
                close_at=close_at, published_at=close_at, status="complete", account=close.account,
                normalized_nav=close.account.nav / profile.initial_cash, daily_return=Decimal(0)))
        state = StrategyPromotionState(target=configuration.target, stage=PromotionStage.PAPER_CANDIDATE,
            revision=2, latest_approval_hash=configuration.paper_approval_hash,
            paper_approval_hash=configuration.paper_approval_hash, paper_approved_at=configuration.paper_approved_at)
        # Original complete ledger and band mathematics; only sealed transport/material are synthetic.
        result = execute_paper_backtest_band(original)
        sealed = PaperResearchSealedAnalysis(task_name="paper_backtest_band", job_id=uuid4(),
            account_id=configuration.binding.account_id, configuration_fingerprint=configuration.fingerprint,
            configuration_version=configuration.version, spec_hash="6" * 64, manifest_hash="7" * 64,
            complete_result_hash="8" * 64, result_hash=result.fingerprint, completed_at=at, band=result)
        kwargs = dict(state=state, configuration=configuration, frame=frame, nav=tuple(nav),
            calendar=original.calendar, band=sealed, band_input=original, as_of=at)
        value = bind_forward_evidence(configuration.target, **kwargs)
        assert value.full_open_days == 2 and value.original.fill_count == 0
        assert value.original.net_return == 0 and value.reconciliation_hash == canonical_sha256(frame.reconciliation)
        assert value.band_source_hash == sealed.result_hash
        for changes in (
            {"state": state.model_copy(update={"paper_approval_hash": "f" * 64})},
            {"state": state.model_copy(update={"paper_approved_at": state.paper_approved_at - timedelta(seconds=1)})},
            {"band_input": original.model_copy(update={"backtest": original.backtest.model_copy(
                update={"native_spec_fingerprint": "f" * 64})})},
            {"band_input": original.model_copy(update={"backtest": original.backtest.model_copy(
                update={"profile_hash": "f" * 64})})},
        ):
            with pytest.raises(ValueError):
                bind_forward_evidence(configuration.target, **{**kwargs, **changes})
    finally:
        keeper.close()


def test_forward_uses_full_original_ledger_calendar_nav_and_closed_band(tmp_path: Path) -> None:
    from rquant.strategy_promotion_evidence import bind_forward_evidence

    target, state, config, frame, nav, calendar, analysis, original, at = forward_material(tmp_path)
    value = bind_forward_evidence(
        target,
        state=state,
        configuration=config,
        frame=frame,
        nav=nav,
        calendar=calendar,
        band=analysis,
        band_input=original,
        as_of=at,
    )
    assert value.full_open_days == 20 and value.all_inside_original_band
    assert value.ledger_head == frame.head_fingerprint and value.original.fill_count == 1
    assert value.original.net_return == 0 and value.original.max_drawdown == 0
    assert value.paper_approval_hash == state.paper_approval_hash


def test_future_forward_evidence_cannot_enter_a_review_bundle(tmp_path: Path) -> None:
    from rquant.strategy_promotion_contracts import PromotionEvidenceBundle
    from rquant.strategy_promotion_evidence import bind_forward_evidence

    target, state, config, frame, nav, calendar, analysis, original, at = forward_material(tmp_path)
    forward = bind_forward_evidence(
        target,
        state=state,
        configuration=config,
        frame=frame,
        nav=nav,
        calendar=calendar,
        band=analysis,
        band_input=original,
        as_of=at,
    )
    with pytest.raises(ValueError, match="visible|future"):
        PromotionEvidenceBundle(
            target=target, forward=forward, observed_at=at - timedelta(seconds=1)
        )


def test_monitor_gate_uses_twenty_complete_days_and_keeps_old_fill_profit_gates_out(
    tmp_path: Path,
) -> None:
    from rquant.strategy_promotion import evaluate_review
    from rquant.strategy_promotion_commands import RequestPromotionReview
    from rquant.strategy_promotion_contracts import (
        PromotionEvidenceBundle,
        PromotionEvidenceSelection,
    )
    from rquant.strategy_promotion_evidence import bind_forward_evidence
    from tests.unit.test_strategy_promotion import saved_target

    target, state, config, frame, nav, calendar, analysis, original, at = forward_material(tmp_path)
    store, _ = saved_target(tmp_path)
    value = bind_forward_evidence(
        target,
        state=state,
        configuration=config,
        frame=frame,
        nav=nav,
        calendar=calendar,
        band=analysis,
        band_input=original,
        as_of=at,
    )
    request = RequestPromotionReview(
        command_id=str(uuid4()),
        requested_at=at,
        generation_id="test-gen",
        target=target,
        expected_revision=2,
        selection=PromotionEvidenceSelection(family_id="original-parent", experiment_id="a" * 64),
    )
    bundle = PromotionEvidenceBundle(target=target, forward=value, observed_at=at)
    review = evaluate_review(
        request, state=state, bundle=bundle, actor_id="alice", metadata_identity=store.identity()
    )
    assert review.eligible and value.original.fill_count == 1 and value.original.net_return == 0
    short = value.model_copy(
        update={
            "full_open_days": 19,
            "first_date": nav[1].trade_date,
            "original": value.original.model_copy(
                update={
                    "trading_days": 19,
                    "observation_range": DateRange(
                        start_date=nav[1].trade_date, end_date=value.last_date
                    ),
                }
            ),
        }
    )
    short_bundle = PromotionEvidenceBundle(target=target, forward=short, observed_at=at)
    assert not evaluate_review(
        request,
        state=state,
        bundle=short_bundle,
        actor_id="alice",
        metadata_identity=store.identity(),
    ).eligible


@pytest.mark.parametrize("fault", ["gap", "future", "definition", "band_dates", "out_of_band"])
def test_forward_missing_middle_future_wrong_version_and_band_are_not_substituted(
    tmp_path: Path, fault: str
) -> None:
    from rquant.strategy_promotion_evidence import bind_forward_evidence

    target, state, config, frame, nav, calendar, analysis, original, at = forward_material(tmp_path)
    if fault == "gap":
        nav = nav[:5] + nav[6:]
    if fault == "future":
        nav = nav[:-1] + (nav[-1].model_copy(update={"published_at": at + timedelta(seconds=1)}),)
    if fault == "definition":
        target = target.model_copy(
            update={"head": target.head.model_copy(update={"record_hash": "9" * 64})}
        )
    if fault == "band_dates":
        original = original.model_copy(update={"comparison_dates": original.comparison_dates[:-1]})
    if fault == "out_of_band":
        nav = nav[:-1] + (nav[-1].model_copy(update={"normalized_nav": Decimal("1.01")}),)
    if fault == "out_of_band":
        value = bind_forward_evidence(
            target,
            state=state,
            configuration=config,
            frame=frame,
            nav=nav,
            calendar=calendar,
            band=analysis,
            band_input=original,
            as_of=at,
        )
        assert not value.all_inside_original_band
    else:
        with pytest.raises(ValueError, match="original|forward|calendar|bound|version|future"):
            bind_forward_evidence(
                target,
                state=state,
                configuration=config,
                frame=frame,
                nav=nav,
                calendar=calendar,
                band=analysis,
                band_input=original,
                as_of=at,
            )
