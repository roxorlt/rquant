"""Owner facts use actual batches and original retained/portfolio materials."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from rquant.feature_spool import FeatureBatchSpool
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.market_minute_source_service import capture_market_minute_step
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_health_details import RuntimeHealthAsOfValidity
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeStepResult,
    project_heartbeat,
)
from rquant.strategy_live_service import run_strategy_live_batch
from tests.paper_cost_fixtures import paper_cost_policy
from tests.unit.test_market_minute_source_service import NOW, _frame
from tests.unit.test_paper_broker import BUY_DATE, BUY_TIME, _intent, _quote, _store
from tests.unit.test_runtime_service_control import _spec
from tests.unit.test_strategy_live_service import _candidate_loader, _evaluator, _publish, _runner

if TYPE_CHECKING:
    from rquant.paper_portfolio_exposure import PaperExposureView
    from rquant.paper_portfolio_exposure_source import PaperBenchmarkSnapshot
    from rquant.paper_portfolio_projection import (
        PaperPortfolioPublishedAccount,
        PaperPortfolioSnapshot,
    )
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
    from rquant.runtime_serving_snapshot import SourceReadResult


def gateway(
    root: Path, *, fetcher: Callable[[], pd.DataFrame] = _frame, completion: datetime = NOW
) -> MarketMinuteGateway:
    return MarketMinuteGateway(
        spool=LiveBatchSpool(root),
        fetcher=fetcher,
        config=MarketMinuteGatewayConfig(producer_version="health-test", producer_commit="a" * 40),
        completion_clock=lambda: completion,
    )


def minute(root: Path, **kwargs: object) -> RuntimeStepResult:
    return capture_market_minute_step(
        gateway(root), received_at=NOW, health_metrics_enabled=True, **kwargs
    )


def test_minute_actual_distinct_codes_and_complete_same_request_scope(tmp_path: Path) -> None:
    frame = _frame()
    earlier = frame.copy()
    earlier["trade_time"] = "2026-07-31 09:39:00"
    owner = gateway(tmp_path, fetcher=lambda: pd.concat([earlier, frame]))
    result = capture_market_minute_step(
        owner,
        received_at=NOW,
        health_metrics_enabled=True,
        expected_codes=("600000.SH", "600001.SH"),
    )
    delay, missing = result.health_metrics
    assert delay.value == Decimal(2) and missing.value == 1
    assert missing.scope.expected_universe_identity == canonical_sha256(("600000.SH", "600001.SH"))
    assert missing.scope.scope_complete and delay.scope.batch_id == missing.scope.batch_id
    assert delay.source_generation_id == result.source_generations["market_minute"]
    assert isinstance(delay.validity, RuntimeHealthAsOfValidity)
    assert delay.observed_at == delay.available_at == NOW


@pytest.mark.parametrize("scope", [None, ("600001.SH",)])
def test_minute_unknown_scope_never_claims_zero_missing(
    tmp_path: Path, scope: tuple[str, ...] | None
) -> None:
    missing = minute(tmp_path, expected_codes=scope).health_metrics[1]
    assert missing.value is None and missing.completeness != "complete"
    assert missing.verdict == "unavailable"


def test_minute_default_off_preserves_old_result_material(tmp_path: Path) -> None:
    result = capture_market_minute_step(gateway(tmp_path), received_at=NOW)
    assert result.health_metrics is None
    assert "health_metrics" not in result.model_dump(mode="json")


def test_minute_duplicate_keeps_original_batch_times_and_receipt(tmp_path: Path) -> None:
    owner = gateway(tmp_path)
    first = capture_market_minute_step(
        owner, received_at=NOW, health_metrics_enabled=True, expected_codes=("600000.SH",)
    )
    owner._completion_clock = lambda: NOW + timedelta(seconds=12)
    retry = capture_market_minute_step(
        owner,
        received_at=NOW + timedelta(seconds=10),
        health_metrics_enabled=True,
        expected_codes=("600000.SH",),
    )
    assert retry.processed_count == 0
    assert retry.health_metrics == first.health_metrics


def test_minute_source_error_has_no_fake_zero_delay_or_missing(tmp_path: Path) -> None:
    owner = gateway(tmp_path, fetcher=lambda: (_ for _ in ()).throw(TimeoutError("offline")))
    result = capture_market_minute_step(
        owner, received_at=NOW, health_metrics_enabled=True, expected_codes=("600000.SH",)
    )
    assert all(
        item.value is None and item.verdict == "unavailable" for item in result.health_metrics
    )


def test_file_metric_receipt_survives_failure_stop_and_idle_without_wire_changes(
    tmp_path: Path,
) -> None:
    result = minute(tmp_path / "spool", expected_codes=("600000.SH",))
    control = RuntimeServiceControl(tmp_path / "control", spec=_spec(), clock=lambda: NOW)
    original = control.start()
    try:
        running = control.record_success(result)
        legacy = project_heartbeat(running).model_dump(mode="json")
        assert "health_metrics" not in legacy
        assert control.record_failure(ValueError("offline")).health_metrics == result.health_metrics
        idle = control.record_success(
            RuntimeStepResult(
                output_sequence=result.output_sequence, source_generations=result.source_generations
            )
        )
        assert idle.health_metrics == result.health_metrics
    finally:
        stopped = control.stop(reason="test done")
    assert stopped.health_metrics == result.health_metrics
    assert original.health_metrics is None
    assert RuntimeServiceControl.read_heartbeat(tmp_path / "control", _spec()) == stopped


def test_file_metric_duplicate_scope_and_detached_generation_reject(tmp_path: Path) -> None:
    result = minute(tmp_path, expected_codes=("600000.SH",))
    with pytest.raises(ValueError):
        RuntimeStepResult(
            source_generations=result.source_generations,
            health_metrics=(result.health_metrics[0], result.health_metrics[0]),
        )
    with pytest.raises(ValueError):
        RuntimeStepResult(
            source_generations={"market_minute": "f" * 64}, health_metrics=result.health_metrics
        )


def test_strategy_real_count_duration_and_idle_batch_semantics(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path / "features")
    _publish(spool)
    runner = _runner(tmp_path / "runner.sqlite3")
    samples = iter((10.0, 12.0))
    times = iter((NOW, NOW + timedelta(seconds=2)))
    kwargs = dict(
        feature_spool=spool,
        runner=runner,
        candidate_universe_loader=_candidate_loader(tmp_path),
        evaluator=_evaluator,
        observed_at=NOW,
        limit=10,
        health_metrics_enabled=True,
        completion_clock=lambda: next(times),
        monotonic_clock=lambda: next(samples),
    )
    first = run_strategy_live_batch(**kwargs)
    assert first.processed_count == 1
    fact = first.last_batch_health
    assert fact.processed_candidates == 1 and fact.duration_seconds == Decimal(2)
    assert fact.original_batch_id == "feature-0"
    assert fact.processing_started_at == NOW and fact.processing_finished_at == NOW + timedelta(
        seconds=2
    )
    idle = run_strategy_live_batch(**kwargs)
    assert idle.processed_count == 0 and idle.last_batch_health is None


def test_strategy_replay_recovers_count_without_manufacturing_duration(tmp_path: Path) -> None:
    spool = FeatureBatchSpool(tmp_path / "features")
    _publish(spool)
    runner = _runner(tmp_path / "runner.sqlite3")
    samples = iter((0.0, 2.0))
    times = iter((NOW, NOW + timedelta(seconds=2)))
    kwargs = dict(
        feature_spool=spool,
        runner=runner,
        candidate_universe_loader=_candidate_loader(tmp_path),
        evaluator=_evaluator,
        observed_at=NOW,
        limit=10,
        health_metrics_enabled=True,
        completion_clock=lambda: next(times),
        monotonic_clock=lambda: next(samples),
    )
    with pytest.raises(RuntimeError, match="after commit"):
        run_strategy_live_batch(
            **kwargs, fault_hook=lambda _: (_ for _ in ()).throw(RuntimeError("after commit"))
        )
    replay = run_strategy_live_batch(**kwargs)
    assert replay.replayed_count == 1 and replay.last_batch_health.processed_candidates == 1
    assert replay.last_batch_health.duration_seconds is None


def test_strategy_fact_rejects_unbound_receipt_or_incomplete_processing_interval(
    tmp_path: Path,
) -> None:
    from rquant.strategy_live_service import StrategyHealthBatchFacts
    from rquant.strategy_runner import StrategySourceBatchReceipt

    receipt = StrategySourceBatchReceipt(
        source_generation_id="a" * 64,
        source_sequence=0,
        source_batch_id="actual-batch",
        source_content_hash="b" * 64,
    )
    values = dict(
        original_batch_id="different",
        source_receipt=receipt,
        runner_generation_id="c" * 64,
        result_identity="d" * 64,
        event_time=NOW,
        available_at=NOW,
        processed_candidates=1,
        duration_seconds=None,
    )
    with pytest.raises(ValueError, match="receipt"):
        StrategyHealthBatchFacts(**values)
    with pytest.raises(ValueError, match="interval"):
        StrategyHealthBatchFacts(
            **(values | {"original_batch_id": "actual-batch", "duration_seconds": Decimal(2)})
        )


def test_strategy_builder_retains_actual_facts_through_idle(tmp_path: Path) -> None:
    from rquant.runtime_builder_strategy import strategy_live_builder
    from tests.unit.test_runtime_builder_strategy import NOW as STRATEGY_NOW
    from tests.unit.test_runtime_builder_strategy import _manifest, _publish_candidates
    from tests.unit.test_runtime_builder_strategy import _publish as publish

    manifest = _manifest(tmp_path)
    manifest = type(manifest).model_validate(
        manifest.model_copy(
            update={"settings": dict(manifest.settings) | {"health_metrics_enabled": True}}
        ).model_dump(mode="python")
    )
    publish(FeatureBatchSpool(tmp_path / "features"), sequence=0)
    _publish_candidates(
        tmp_path / "candidates",
        definition_fingerprint=manifest.settings["strategy_registration_fingerprint"],
        executable_fingerprint=manifest.settings["strategy_executable_fingerprint"],
        candidate_schema_fingerprint=manifest.settings["candidate_schema_fingerprint"],
    )
    step = strategy_live_builder(clock=lambda: STRATEGY_NOW + timedelta(minutes=1))(manifest)
    first, idle = step(), step()
    assert first.processed_count == 1 and idle.processed_count == 0
    assert first.health_metrics == idle.health_metrics
    assert first.health_metrics[1].value == 1


def test_retained_ratio_uses_two_of_five_even_when_total_is_twenty(tmp_path: Path) -> None:
    from rquant.paper_history_serving import paper_history_summary

    broker = _store(tmp_path / "broker.sqlite3", paper_cost_policy())
    for index in range(5):
        broker.submit_intent(
            _intent(signal_seed=str(index)),
            decision_time=BUY_TIME,
            trade_date=BUY_DATE,
            quote=_quote("10", risk_rejected=index < 2),
        )
    actual = broker.recent_order_history(as_of=BUY_TIME)
    window = type(actual).model_validate(
        actual.model_copy(update={"total_orders": 20, "has_more": True}).model_dump(mode="python")
    )
    summary = paper_history_summary(window)
    assert summary.rejected_count == 2 and summary.retained_count == 5
    assert summary.rejection_ratio == Decimal(".4") and summary.total_orders == 20
    assert summary.window_identity == canonical_sha256(window)


def test_empty_retained_window_has_no_zero_percent(tmp_path: Path) -> None:
    from rquant.paper_history_serving import paper_history_summary

    summary = paper_history_summary(
        _store(tmp_path / "broker.sqlite3", paper_cost_policy()).recent_order_history(
            as_of=BUY_TIME
        )
    )
    assert summary.retained_count == summary.rejected_count == 0
    assert summary.rejection_ratio is None


def test_paper_builder_metric_binds_selected_old_generation_and_cutoff(tmp_path: Path) -> None:
    from rquant.runtime_builder_paper import paper_broker_builder
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
    from tests.unit.test_runtime_builder_paper import NOW as PAPER_NOW
    from tests.unit.test_runtime_builder_paper import _manifest, _publish_signal

    _publish_signal(tmp_path)
    manifest = _manifest(tmp_path, RuntimeServiceKind.PAPER_BROKER)
    manifest = RuntimeServiceManifest.model_validate(
        manifest.model_copy(
            update={
                "settings": dict(manifest.settings)
                | {
                    "health_metrics_enabled": True,
                    "paused": True,
                    "serving_authority_root": str(tmp_path / "authority"),
                }
            }
        ).model_dump(mode="python")
    )
    current = [PAPER_NOW]
    step = paper_broker_builder(
        clock=lambda: current[0],
        quote_resolver=lambda *_: None,
        trade_date_resolver=lambda _: PAPER_NOW.date(),
    )(manifest)
    first = step()
    current[0] += timedelta(seconds=20)
    retry = step()
    assert first.source_generations["paper_accounts"] == retry.source_generations["paper_accounts"]
    assert retry.health_metrics == first.health_metrics
    assert retry.health_metrics[0].observed_at == PAPER_NOW
    assert retry.health_metrics[0].value is None


def _paper_source_fixture(
    tmp_path: Path,
) -> tuple[PaperPortfolioViewSource, PaperPortfolioSnapshot]:
    import sqlite3
    from contextlib import closing

    from rquant.paper_portfolio_projection import PaperPortfolioSnapshot
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
    from rquant.paper_signal_worker import PaperSignalQueueStore
    from tests.unit.test_paper_portfolio_ledger_views import filled
    from tests.unit.test_paper_portfolio_view_source import market
    from tests.unit.test_paper_signal_worker import EXECUTION_TIME, _policy

    broker, _, _, runtime = filled(tmp_path)
    at = EXECUTION_TIME + timedelta(seconds=1)
    market(runtime, at=at)
    source = PaperPortfolioViewSource(
        runtime,
        broker=broker,
        queue=PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy()),
    )
    with closing(sqlite3.connect(broker.path)) as pinned:
        pinned.execute("SELECT count(*) FROM paper_order").fetchone()
        value = PaperPortfolioSnapshot(available_at=at, accounts=(source.read(as_of=at),))
    return source, value


def _selected_paper(
    source: PaperPortfolioViewSource, account: PaperPortfolioPublishedAccount, at: datetime
) -> SourceReadResult:
    from rquant.paper_history_serving import paper_history_projections
    from rquant.paper_portfolio_projection import (
        PaperPortfolioSnapshot,
        paper_portfolio_projections,
    )
    from rquant.runtime_serving_snapshot import PaperAccountsPayload, SourceReadResult
    from rquant.serving_contracts import FreshnessStatus

    projections = paper_portfolio_projections(
        PaperPortfolioSnapshot(available_at=at, accounts=(account,))
    )
    history = source.broker.recent_order_history(as_of=at)
    values = dict(
        dataset_id="paper_accounts",
        sequence=history.ledger_revision,
        event_time=at,
        published_at=at,
        status=FreshnessStatus.FRESH,
        reason=None,
        payload=PaperAccountsPayload(
            paper_accounts=(account.frame.account,),
            projections=paper_history_projections(history) + projections,
        ),
    )
    return SourceReadResult(generation_id=canonical_sha256(values), **values)


def _closed_comparison(
    tmp_path: Path, *, lower: str = "1", upper: str = "2"
) -> tuple[PaperPortfolioViewSource, PaperPortfolioPublishedAccount, datetime]:
    import sqlite3
    from contextlib import closing
    from uuid import UUID

    from rquant.lab_jobs import JobStatus
    from rquant.paper_portfolio_band import BootstrapPoint, PaperBacktestBandResult
    from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
    from rquant.paper_research_artifact import PaperResearchSealedAnalysis, PaperResearchSummary
    from rquant.paper_signal_worker import PaperSignalQueueStore
    from tests.unit.test_paper_portfolio_ledger_views import close_material, filled
    from tests.unit.test_paper_portfolio_view_source import market
    from tests.unit.test_paper_signal_worker import _policy

    broker, basis, _, runtime = filled(tmp_path)
    source = PaperPortfolioViewSource(
        runtime,
        broker=broker,
        queue=PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy()),
    )
    close = close_material(basis.configuration)
    source.runtime.calendar = close.calendar
    market(source.runtime, at=close.close_at)
    with closing(sqlite3.connect(broker.path)) as pinned:
        pinned.execute("SELECT count(*) FROM paper_order").fetchone()
        account = source.read(as_of=close.close_at)
    band = PaperBacktestBandResult(
        input_hash="a" * 64,
        configuration_fingerprint=account.configuration.fingerprint,
        backtest_source_hash="b" * 64,
        dates=(close.trade_date,),
        points=(BootstrapPoint(day_index=1, lower=lower, upper=upper),),
    )
    job_id = UUID("11111111-1111-4111-8111-111111111111")
    sealed = PaperResearchSealedAnalysis(
        task_name="paper_backtest_band",
        job_id=job_id,
        account_id=account.configuration.binding.account_id,
        configuration_fingerprint=account.configuration.fingerprint,
        configuration_version=account.configuration.version,
        spec_hash="c" * 64,
        manifest_hash="d" * 64,
        complete_result_hash="e" * 64,
        result_hash=band.fingerprint,
        completed_at=close.close_at,
        band=band,
    )
    summary = PaperResearchSummary(
        task_name=sealed.task_name,
        job_id=job_id,
        account_id=sealed.account_id,
        configuration_fingerprint=sealed.configuration_fingerprint,
        configuration_version=sealed.configuration_version,
        status=JobStatus.SUCCEEDED,
        accepted_at=close.close_at,
        sealed=sealed,
    )
    return (
        source,
        type(account).model_validate(
            account.model_copy(update={"band": band, "recent_research": (summary,)}).model_dump(
                mode="python"
            )
        ),
        close.close_at,
    )


@pytest.mark.parametrize(
    "lower,upper,expected",
    [("1", "2", "inside"), ("1", "1.5", "outside"), ("1.795", "1.795", "inside")],
)
def test_original_owner_publishes_bound_band_position_and_sealed_basis(
    tmp_path: Path, lower: str, upper: str, expected: str
) -> None:
    from rquant.paper_portfolio_view_source import publish_paper_band_position
    from rquant.runtime_builder_paper import paper_health_metrics_for_publication

    source, original, at = _closed_comparison(tmp_path, lower=lower, upper=upper)
    assert original.band_position is None and "band_position" not in original.model_dump(
        mode="json"
    )
    account = publish_paper_band_position(original)
    assert account.band_position == expected
    read = _selected_paper(source, account, at)
    metrics = paper_health_metrics_for_publication(
        read, account_id=source.broker.account_id, fallback_configuration_identity="f" * 64
    )
    metric = next(item for item in metrics if item.metric_id == "return_comparison")
    assert metric.value == expected and metric.scope.comparison_dates == metric.scope.baseline_dates
    assert metric.validity.basis == "sealed_comparison"
    assert metric.validity.basis_identity == original.band.backtest_source_hash
    assert metric.source_identity == canonical_sha256(read)


def test_unsealed_or_wrong_dates_cannot_publish_comparison(tmp_path: Path) -> None:
    from rquant.paper_portfolio_view_source import publish_paper_band_position

    _, account, _ = _closed_comparison(tmp_path)
    unsealed = type(account).model_validate(
        account.model_copy(update={"recent_research": ()}).model_dump(mode="python")
    )
    assert publish_paper_band_position(unsealed).band_position is None
    with pytest.raises(ValueError, match="sealed comparison"):
        type(account).model_validate(
            unsealed.model_copy(update={"band_position": "inside"}).model_dump(mode="python")
        )
    shifted = account.band.model_copy(
        update={"dates": (account.band.dates[0] + timedelta(days=1),)}
    )
    other_dates = type(account).model_validate(
        account.model_copy(update={"band": shifted}).model_dump(mode="python")
    )
    assert publish_paper_band_position(other_dates).band_position is None


def test_original_past_risk_decision_transfers_policy_without_fake_current_freshness(
    tmp_path: Path,
) -> None:
    from rquant.portfolio.drawdown import DrawdownRule
    from rquant.runtime_builder_paper import paper_health_metrics_for_publication
    from tests.unit.test_paper_portfolio_view_source import market

    source, original = _paper_source_fixture(tmp_path)
    configuration = source.runtime.state.configuration
    configuration = type(configuration).model_validate(
        configuration.model_copy(
            update={
                "version": 2,
                "drawdown_rule": DrawdownRule(
                    trigger_drawdown=".20", release_drawdown=".05", action="block_new_positions"
                ),
            }
        ).model_dump(mode="python")
    )
    source.runtime.state.start_configuration(configuration)
    at = original.available_at + timedelta(seconds=1)
    revision = original.accounts[0].frame.ledger_revision
    source.runtime.state.observe_nav(
        Decimal(1000), observed_at=at, ledger_revision=revision, source_fingerprint="a" * 64
    )
    at += timedelta(seconds=1)
    risk = source.runtime.state.observe_nav(
        Decimal(700), observed_at=at, ledger_revision=revision, source_fingerprint="b" * 64
    )
    market(source.runtime, at=at)
    account = source.read(as_of=at)
    read = _selected_paper(source, account, at)
    metric = next(
        item
        for item in paper_health_metrics_for_publication(
            read, account_id=source.broker.account_id, fallback_configuration_identity="f" * 64
        )
        if item.metric_id == "portfolio_risk"
    )
    assert risk.decision.state.active and metric.value == "breached"
    assert metric.policy_identity == canonical_sha256(configuration.drawdown_rule)
    assert metric.validity.basis == "past_risk_decision" and metric.fresh_until is None
    assert metric.event_time_end == risk.observed_at


def _exposure_fixture(
    tmp_path: Path,
) -> tuple[PaperPortfolioViewSource, PaperPortfolioSnapshot, PaperBenchmarkSnapshot]:
    from rquant.paper_portfolio_exposure_source import (
        PaperBenchmarkSnapshot,
        PaperPortfolioExposureStore,
    )

    source, value = _paper_source_fixture(tmp_path)
    source.runtime.exposure_store = PaperPortfolioExposureStore(source.runtime.state)
    at = value.available_at
    benchmark = PaperBenchmarkSnapshot(
        configuration_fingerprint=value.accounts[0].configuration.fingerprint,
        observed_at=at,
        available_at=at,
        valid_through=at + timedelta(seconds=5),
        source_identity="b" * 64,
        weights=({"industry_l1": "银行", "weight": 1},),
        cash_weight=0,
    )
    source.runtime.exposure_store.publish_benchmark(benchmark)
    return source, value, benchmark


def test_exposure_receipt_is_single_original_read_calculation_and_default_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from contextlib import contextmanager

    import rquant.paper_portfolio_exposure_source as product

    source, value, benchmark = _exposure_fixture(tmp_path)
    account = value.accounts[0]
    queries, calculations = [], []
    original_connection = source.runtime.state._connection
    original_calculate = product.calculate_paper_exposure

    @contextmanager
    def traced_connection(*args: object, **kwargs: object) -> Iterator[sqlite3.Connection]:
        with original_connection(*args, **kwargs) as connection:
            connection.set_trace_callback(queries.append)
            try:
                yield connection
            finally:
                connection.set_trace_callback(None)

    def calculate(*args: object, **kwargs: object) -> PaperExposureView:
        calculations.append(1)
        return original_calculate(*args, **kwargs)

    monkeypatch.setattr(source.runtime.state, "_connection", traced_connection)
    monkeypatch.setattr(product, "calculate_paper_exposure", calculate)
    kwargs = dict(facts=account.market_material.facts, as_of=value.available_at)
    receipt = source.runtime.exposure_store.exposure(
        account.frame, **kwargs, include_health_receipt=True
    )
    assert (
        receipt.benchmark == benchmark and receipt.material.facts == account.market_material.facts
    )
    assert len([sql for sql in queries if "SELECT body FROM paper_benchmarks" in sql]) == 1
    assert len(calculations) == 1
    old = source.runtime.exposure_store.exposure(account.frame, **kwargs)
    assert old == receipt.result
    assert len(calculations) == 2


def test_cash_weight_uses_actual_earlier_benchmark_expiry_and_preserves_all_original_rows(
    tmp_path: Path,
) -> None:
    import sqlite3
    from contextlib import closing

    from rquant.runtime_builder_paper import paper_health_metrics_for_publication

    source, value, benchmark = _exposure_fixture(tmp_path)
    source.health_metrics_enabled = True
    with closing(sqlite3.connect(source.broker.path)) as pinned:
        pinned.execute("SELECT count(*) FROM paper_order").fetchone()
        account = source.read(as_of=value.available_at)
    assert account.exposure_receipt.benchmark == benchmark
    assert account.exposure_receipt.result == account.exposure
    assert {item.kind for item in account.exposure_receipt.result.exposure.rows} == {
        "industry",
        "cash",
    }
    read = _selected_paper(source, account, value.available_at)
    metric = next(
        item
        for item in paper_health_metrics_for_publication(
            read, account_id=source.broker.account_id, fallback_configuration_identity="f" * 64
        )
        if item.metric_id == "portfolio_exposure"
    )
    cash = next(item for item in account.exposure.exposure.rows if item.kind == "cash")
    assert metric.reason_code == "portfolio_cash_weight" and metric.value == cash.portfolio_weight
    assert metric.validity.valid_until == benchmark.valid_through
    assert not metric.validity.expired_at(benchmark.valid_through)
    assert metric.validity.expired_at(benchmark.valid_through + timedelta(microseconds=1))
    assert metric.source_identity == canonical_sha256(read)


def test_old_optional_exposure_absence_keeps_canonical_graph_and_new_bad_receipt_rejects(
    tmp_path: Path,
) -> None:
    import sqlite3
    from contextlib import closing

    from rquant.paper_portfolio_projection import (
        PaperPortfolioSnapshot,
        paper_portfolio_projections,
        validate_paper_portfolio_projections,
    )

    source, value, _ = _exposure_fixture(tmp_path)
    with closing(sqlite3.connect(source.broker.path)) as pinned:
        pinned.execute("SELECT count(*) FROM paper_order").fetchone()
        old = source.read(as_of=value.available_at)
        source.health_metrics_enabled = True
        new = source.read(as_of=value.available_at)
    assert old.exposure_receipt is None and "exposure_receipt" not in old.model_dump(mode="json")
    snapshot = PaperPortfolioSnapshot(available_at=value.available_at, accounts=(old,))
    projections = paper_portfolio_projections(snapshot)
    assert (
        validate_paper_portfolio_projections({item.table_name: item for item in projections})
        == snapshot
    )
    assert old.model_dump(mode="json") == new.model_dump(mode="json", exclude={"exposure_receipt"})
    detached = new.exposure_receipt.model_copy(
        update={
            "material": new.exposure_receipt.material.model_copy(
                update={"benchmark_source_identity": "f" * 64}
            )
        }
    )
    with pytest.raises(ValueError):
        type(new).model_validate(
            new.model_copy(update={"exposure_receipt": detached}).model_dump(mode="python")
        )
