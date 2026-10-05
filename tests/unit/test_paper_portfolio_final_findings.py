"""Original batch and calendar counterexamples from the concentrated final review."""

from contextlib import closing
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from rquant.paper_broker import PaperBrokerStore
from rquant.backtest.contracts import SSECalendar
from rquant.paper_portfolio_band import SealedPaperBacktestReturns
from rquant.paper_operator import PaperOperatorControlStore
from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperTargetMaterials
from rquant.paper_portfolio_runtime import PaperPortfolioRuntime
from rquant.paper_portfolio_source import PaperPortfolioMaterialStore, PaperPortfolioMarketSnapshot, PaperPortfolioRawFact
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
from rquant.paper_portfolio_target import prepare_paper_target_quantity
from rquant.paper_signal_worker import PaperQuoteSnapshot, PaperSignalQueueStore, run_paper_signal_batch
from rquant.signal_contracts import SignalEnvelope
from tests.paper_cost_fixtures import paper_instrument_context
from tests.unit.test_paper_portfolio_admission import confirm, request
from tests.unit.test_paper_portfolio_core import config_data, materials
from tests.unit.test_paper_signal_worker import EXECUTION_TIME, TRADE_DATE, _policy, _quote, _signal


def _two_buy_runtime(tmp_path: Path) -> tuple[PaperBrokerStore, PaperPortfolioRuntime, PaperSignalQueueStore, tuple[SignalEnvelope, SignalEnvelope]]:
    broker, _ = materials(tmp_path)
    configuration = PaperPortfolioConfiguration(**config_data(
        weight_rule={"method": "equal", "max_positions": 2, "cash_reserve": ".1"},
        drawdown_rule={"action": "block_new_positions", "trigger_drawdown": ".004", "release_drawdown": ".002"},
    ))
    state = PaperPortfolioStateStore(tmp_path / "state.sqlite", configuration=configuration)
    operator = PaperOperatorControlStore(state, root=tmp_path / "control" / "operator", clock=lambda: EXECUTION_TIME)
    source = PaperPortfolioMaterialStore(state)
    source.publish(PaperPortfolioMarketSnapshot(
        binding=configuration.binding, configuration_fingerprint=configuration.fingerprint,
        dataset_snapshot_id="c" * 64, feature_snapshot_id="d" * 64,
        observed_at=EXECUTION_TIME, available_at=EXECUTION_TIME,
        facts=tuple(PaperPortfolioRawFact(ts_code=code, rank_score="1", industry_l1="银行", valuation_price="1",
                    trading_status="normal", observed_at=EXECUTION_TIME, available_at=EXECUTION_TIME,
                    source_snapshot_id="f" * 64) for code in ("600000.SH", "600001.SH")),
    ))
    control = confirm(operator, request(operator))
    operator.publish(control)
    operator.apply(observed_at=EXECUTION_TIME)
    runtime = PaperPortfolioRuntime(state, operator=operator, materials=source, producer_commit="a" * 40)
    queue = PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy())
    first = _signal()
    second = SignalEnvelope(**{**first.model_dump(mode="python", exclude={"signal_id"}), "candidate_id": "600001.SH"})
    return broker, runtime, queue, (first, second)


def _same_cutoff_quote(signal: SignalEnvelope, cutoff: datetime) -> PaperQuoteSnapshot:
    original = _quote(price="1", available_at=cutoff)
    context = original.context.model_copy(update={"instrument_context": paper_instrument_context(signal.candidate_id)})
    return PaperQuoteSnapshot(**{**original.model_dump(mode="python", exclude={"snapshot_id"}), "ts_code": signal.candidate_id, "context": context})


def test_same_cutoff_second_new_buy_cannot_reuse_risk_before_first_fee(tmp_path: Path) -> None:
    broker, runtime, queue, signals = _two_buy_runtime(tmp_path)
    first = runtime.prepare(broker, signals[0], _same_cutoff_quote(signals[0], EXECUTION_TIME), decision_at=EXECUTION_TIME)
    assert runtime.prepare(broker, signals[0], _same_cutoff_quote(signals[0], EXECUTION_TIME), decision_at=EXECUTION_TIME) == first
    for signal in signals:
        queue.ingest(signal, received_at=signal.available_at)
    ordered = queue.due_records(now=EXECUTION_TIME, limit=10)
    result = run_paper_signal_batch(queue, broker, now=EXECUTION_TIME, trade_date=TRADE_DATE,
                                   quote_resolver=_same_cutoff_quote, limit=10, portfolio_runtime=runtime)
    records = tuple(queue.record(record.signal.signal_id) for record in ordered)
    assert records[0].order is not None and records[0].order.filled_quantity == 400
    assert records[1].order is None
    assert broker.reconcile().order_count == 1
    assert result.completed_count == 1
    account = runtime.account_authority(broker, cutoff=EXECUTION_TIME, prices={"600000.SH": Decimal(1), "600001.SH": Decimal(1)})
    assert account.snapshot.nav == Decimal("995")
    with pytest.raises(ValueError, match="original paper NAV observation differs"):
        runtime.state.observe_nav(account.snapshot.nav, observed_at=EXECUTION_TIME, ledger_revision=account.revision,
                                  source_fingerprint=account.state_fingerprint)
    print("PP_FINAL_01_ORIGINAL_BATCH: first400/fee5/NAV995; second_new_BUY_no_order; exact_before_submit_repeat=True")


def test_fresh_reduction_observer_checks_changed_account_at_same_cutoff(tmp_path: Path) -> None:
    broker, runtime, queue, signals = _two_buy_runtime(tmp_path)
    first = runtime.prepare(broker, signals[0], _same_cutoff_quote(signals[0], EXECUTION_TIME), decision_at=EXECUTION_TIME)
    queue.ingest(signals[0], received_at=signals[0].available_at)
    prepared = queue.prepare(signals[0].signal_id, quote=_same_cutoff_quote(signals[0], EXECUTION_TIME),
                             prepared_at=EXECUTION_TIME, target_quantity_authority=first)
    broker.submit_intent(prepared.intent, execution_id=prepared.execution_id, decision_time=EXECUTION_TIME,
                         trade_date=TRADE_DATE, quote=prepared.quote.context)
    with pytest.raises(ValueError, match="original paper NAV observation differs"):
        runtime.plan_risk_reductions(broker, decision_at=EXECUTION_TIME, trade_date=TRADE_DATE,
                                     quote_resolver=_same_cutoff_quote)
    assert broker.reconcile().order_count == 1


@pytest.mark.parametrize("changed", ["nav", "revision", "source", "cutoff"])
def test_target_requires_risk_from_full_current_account_observation(tmp_path: Path, changed: str) -> None:
    broker, runtime, _, signals = _two_buy_runtime(tmp_path)
    first = runtime.prepare(broker, signals[0], _same_cutoff_quote(signals[0], EXECUTION_TIME), decision_at=EXECUTION_TIME)
    risk = first.basis.risk_observation
    assert risk is not None
    changes = {
        "nav": {"nav": Decimal("999")},
        "revision": {"ledger_revision": risk.ledger_revision + 1},
        "source": {"source_fingerprint": "e" * 64},
        "cutoff": {"observed_at": risk.observed_at - timedelta(seconds=1)},
    }
    substituted = first.basis.model_copy(update={"risk_observation": risk.model_copy(update=changes[changed])})
    with pytest.raises(ValueError, match="drawdown observation"):
        prepare_paper_target_quantity(substituted)
    assert broker.reconcile().order_count == 0


@pytest.mark.parametrize("changed", ["nav", "revision", "source", "cutoff"])
def test_reduction_plan_requires_risk_from_full_original_account(tmp_path: Path, changed: str) -> None:
    from rquant.paper_portfolio_reductions import PaperRiskReductionPlan
    from tests.unit.test_paper_portfolio_reductions import fixture, material

    broker, runtime, at = fixture(tmp_path)
    at += timedelta(minutes=1)
    material(runtime, at, ".75")
    plan = runtime.plan_risk_reductions(broker, decision_at=at, trade_date=TRADE_DATE,
                                       quote_resolver=lambda _signal, cutoff: _quote(price=".75", available_at=cutoff))
    assert plan is not None
    changes = {
        "nav": {"nav": plan.risk.nav - 1},
        "revision": {"ledger_revision": plan.risk.ledger_revision + 1},
        "source": {"source_fingerprint": "e" * 64},
        "cutoff": {"observed_at": plan.risk.observed_at - timedelta(seconds=1)},
    }
    substituted = plan.model_copy(update={"risk": plan.risk.model_copy(update=changes[changed])})
    with pytest.raises(ValueError, match="configured sources"):
        PaperRiskReductionPlan.model_validate(substituted.model_dump(mode="python"))


def _calendar() -> SSECalendar:
    days = (date(2026, 7, 31), date(2026, 8, 3), date(2026, 8, 4))
    return SSECalendar(source_identity="c" * 64, coverage_start=days[0], coverage_end=days[-1], dates=days)


def _returns(configuration: PaperPortfolioConfiguration, dates: tuple[date, ...]) -> SealedPaperBacktestReturns:
    binding = configuration.binding
    return SealedPaperBacktestReturns(job_id=UUID("11111111-1111-4111-8111-111111111111"),
        owner_id=binding.owner_id, strategy_id=binding.strategy_id, strategy_version=binding.strategy_version,
        parameter_fingerprint=binding.parameter_fingerprint, cost_spec_id=binding.cost_spec_id,
        calendar_source_identity=_calendar().source_identity, definition_fingerprint="d" * 64,
        definition_record_hash="e" * 64, spec_hash="f" * 64, manifest_hash="1" * 64,
        complete_result_hash="2" * 64, backtest_content_hash="3" * 64,
        returns=tuple({"trade_date": day, "daily_return": ".02"} for day in dates))


@pytest.mark.parametrize("missing", ["comparison", "returns"])
def test_band_refuses_absent_original_open_day_before_sampling(missing: str) -> None:
    from rquant.paper_portfolio_band import PaperBacktestBandInput

    configuration = PaperPortfolioConfiguration(**config_data())
    calendar = _calendar()
    partial = (calendar.dates[0], calendar.dates[-1])
    with pytest.raises(ValueError, match="calendar"):
        PaperBacktestBandInput(configuration=configuration, calendar=calendar,
            backtest=_returns(configuration, partial if missing == "returns" else calendar.dates),
            comparison_dates=partial if missing == "comparison" else calendar.dates)


def test_complete_original_calendar_keeps_third_day_constant_return_band() -> None:
    from rquant.paper_portfolio_band import PaperBacktestBandInput, execute_paper_backtest_band

    configuration = PaperPortfolioConfiguration(**config_data())
    calendar = _calendar()
    value = PaperBacktestBandInput(configuration=configuration, calendar=calendar,
        backtest=_returns(configuration, calendar.dates), comparison_dates=calendar.dates)
    result = execute_paper_backtest_band(value)
    assert result.dates == calendar.dates
    assert result.points[-1].day_index == 3
    assert result.points[-1].lower == result.points[-1].upper == Decimal("1.061208")


def _publish_missing_middle_close(source: PaperPortfolioViewSource, *, missing: str = "day") -> datetime:
    from tests.unit.test_paper_portfolio_ledger_views import close_material
    from tests.unit.test_paper_portfolio_view_source import market

    source.runtime.calendar = _calendar()
    days = _calendar().dates if missing in ("return", "complete") else (_calendar().dates[0], _calendar().dates[-1])
    for day in days:
        original = close_material(source.runtime.state.configuration, day=day)
        material = type(original).model_validate(original.model_copy(update={"calendar": _calendar()}).model_dump(mode="python"))
        source.views.record_close(source.runtime.ledger_source_for(source.broker), material, published_at=material.available_at)
    if missing == "return":
        # A typed unavailable derived return keeps the original marked account.
        last = source.views.nav_series()[-1].model_copy(update={"daily_return": None})
        with source.runtime.state._connection(write=True) as connection:
            connection.execute("UPDATE portfolio_daily_nav SET body=? WHERE configuration=? AND trade_date=?",
                               (last.model_dump_json(), last.configuration_fingerprint, last.trade_date.isoformat()))
    market(source.runtime, at=material.close_at)
    return material.close_at


def test_original_nav_store_reports_missing_middle_date_without_substitute_numbers(tmp_path: Path) -> None:
    from tests.unit.test_paper_portfolio_ledger_views import filled

    broker, _, _, runtime = filled(tmp_path)
    with closing(broker._connect()):
        source = PaperPortfolioViewSource(runtime, broker=broker, queue=PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy()))
        cutoff = _publish_missing_middle_close(source)
        series = source.views.nav_series()
        assert tuple(point.trade_date for point in series) == _calendar().dates
        gap = series[1]
        assert gap.status == "unavailable" and gap.reason
        assert gap.account is None and gap.normalized_nav is None and gap.daily_return is None
        assert gap.published_at == series[-1].published_at
        assert gap.ledger_revision == series[-1].ledger_revision
        assert gap.ledger_head_fingerprint == series[-1].ledger_head_fingerprint
        with runtime.state._connection() as connection:
            original_rows = connection.execute("SELECT * FROM portfolio_daily_nav ORDER BY trade_date").fetchall()
        assert source.views.nav_series() == series
        with runtime.state._connection() as connection:
            assert connection.execute("SELECT * FROM portfolio_daily_nav ORDER BY trade_date").fetchall() == original_rows
        view = source.read(as_of=cutoff)
        assert tuple(point.trade_date for point in view.nav) == _calendar().dates
        assert view.nav[1].nav is None and view.nav[1].cash is None


def test_original_source_hides_prior_band_when_actual_day_return_becomes_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.lab_jobs import JobStatus
    from rquant.paper_backtest_source import PaperBacktestSourceReader
    from rquant.paper_portfolio_band import PaperBacktestBandInput, execute_paper_backtest_band
    from rquant.paper_research_artifact import PaperResearchResultReader, PaperResearchSealedAnalysis, PaperResearchSummary
    from rquant.paper_research_commands import RunPaperPortfolioResearch
    from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
    from tests.unit.test_paper_research_submission import fixture
    from tests.unit.test_strategy_authoring import store

    _, backend, runtime, request, _ = fixture(tmp_path)
    runs = backend.research_backend
    source = runs.preparer.source_for(request.account_id, "alice")
    with closing(source.broker._connect()):
        cutoff = _publish_missing_middle_close(source, missing="complete")
        authoring = tmp_path / "authoring"
        authoring.mkdir(mode=0o700)
        target = store(authoring)
        reader = runs.facade.reader
        artifacts = ArtifactPreviewReader(reader=reader, artifact_root=tmp_path / "artifacts")
        backtest = PaperBacktestSourceReader(store=target, expected_identity=target.identity(),
            sealed_reader=StrategyTemplateSealedResultReader(reader=reader, artifact_reader=artifacts))
        band_input = PaperBacktestBandInput(configuration=runtime.state.configuration, calendar=_calendar(),
            backtest=_returns(runtime.state.configuration, _calendar().dates), comparison_dates=_calendar().dates)
        # Synthetic return/result DTOs isolate the publication choice. They are
        # not evidence of a new trusted C5 result or isolated sealed worker.
        monkeypatch.setattr(backtest, "band_input", lambda **_kwargs: band_input)
        runs.preparer.backtest_reader = backtest
        runs.preparer.clock = lambda: cutoff
        run = RunPaperPortfolioResearch(**{**request.model_dump(mode="python"), "command_id": str(uuid4()),
            "requested_at": cutoff, "task_name": "paper_backtest_band", "backtest_job_id": uuid4()})
        owned = runs.compile(run, owner_id="alice", expected_identity=runtime.state.identity())
        band = execute_paper_backtest_band(band_input)
        result = PaperResearchSealedAnalysis(task_name=owned.task_name, job_id=UUID(owned.command_id), account_id=owned.account_id,
            configuration_fingerprint=owned.configuration_fingerprint, configuration_version=owned.catalog.configuration.version,
            spec_hash=owned.spec.spec_hash, manifest_hash="a" * 64, complete_result_hash="b" * 64,
            result_hash=band.fingerprint, completed_at=cutoff, band=band)
        summary = PaperResearchSummary(task_name=owned.task_name, job_id=result.job_id, account_id=owned.account_id,
            configuration_fingerprint=owned.configuration_fingerprint, configuration_version=owned.catalog.configuration.version,
            status=JobStatus.SUCCEEDED, accepted_at=owned.accepted_at, sealed=result)
        results = PaperResearchResultReader(backend=runs, reader=reader, artifact_reader=artifacts)
        monkeypatch.setattr(results, "summary", lambda **_kwargs: summary)
        source.research_results = results
        assert source.read(as_of=cutoff).band == band
        last = source.views.nav_series()[-1].model_copy(update={"daily_return": None})
        with runtime.state._connection(write=True) as connection:
            connection.execute("UPDATE portfolio_daily_nav SET body=? WHERE configuration=? AND trade_date=?",
                               (last.model_dump_json(), last.configuration_fingerprint, last.trade_date.isoformat()))
        changed = source.read(as_of=cutoff)
        assert changed.nav[-1].status == "complete" and changed.nav[-1].daily_return is None
        assert changed.band is None


@pytest.mark.parametrize("missing", ["day", "return"])
def test_original_journal_refuses_gap_before_input_acceptance_or_lab_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.lab_jobs import LabJobReader
    from rquant.paper_backtest_source import PaperBacktestSourceReader
    from rquant.paper_research_commands import RunPaperPortfolioResearch
    from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
    from tests.unit.test_paper_research_submission import fixture
    from tests.unit.test_strategy_authoring import store

    service, backend, runtime, request, jobs = fixture(tmp_path)
    source = backend.research_backend.preparer.source_for(request.account_id, "alice")
    with closing(source.broker._connect()):
        cutoff = _publish_missing_middle_close(source, missing=missing)
        authoring = tmp_path / "authoring"
        authoring.mkdir(mode=0o700)
        target = store(authoring)
        reader = LabJobReader(jobs.path)
        artifact = ArtifactPreviewReader(reader=reader, artifact_root=tmp_path / "artifacts")
        backtest = PaperBacktestSourceReader(store=target, expected_identity=target.identity(),
            sealed_reader=StrategyTemplateSealedResultReader(reader=reader, artifact_reader=artifact))
        preparer = backend.research_backend.preparer
        preparer.backtest_reader = backtest
        preparer.clock = lambda: cutoff
        monkeypatch.setattr(backtest, "band_input", lambda **_kwargs: pytest.fail("missing NAV reached sealed-source work"))
        run = RunPaperPortfolioResearch(**{**request.model_dump(mode="python"), "command_id": str(uuid4()),
            "requested_at": cutoff, "task_name": "paper_backtest_band", "backtest_job_id": uuid4()})
        with pytest.raises(ValueError, match="净值|收益|calendar"):
            service._submit_trusted_paper_portfolio(run, authenticated_actor_id="alice", verified_metadata_identity=runtime.state.identity())
        assert service.outbox.lookup_paper_portfolio_command(run, authenticated_actor_id="alice") is None
        with runtime.state._connection() as connection:
            assert connection.execute("SELECT count(*) FROM paper_research_admissions").fetchone()[0] == 0
        assert tuple(preparer.input_root.iterdir()) == ()
        assert backend.research_backend.facade.spool.pending() == ()


@pytest.mark.parametrize("missing", ["day", "return"])
def test_original_projection_and_private_reader_hide_comparison_on_nav_gap(tmp_path: Path, missing: str) -> None:
    from rquant.paper_portfolio_band import PaperBacktestBandResult
    from rquant.paper_portfolio_projection import PaperPortfolioSnapshot, paper_portfolio_projections
    from rquant.serving_publisher import ServingPublisher
    from rquant.serving_read_models import SERVING_TABLE_SPECS, ServingProjectionInput, ServingReadModelInput, build_serving_read_models
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import ProofTestClient, with_test_proxy_identity
    from tests.support.web_serving_fixture import _generation_ids, _watermarks
    from tests.unit.test_paper_portfolio_ledger_views import filled

    broker, _, _, runtime = filled(tmp_path)
    with closing(broker._connect()):
        source = PaperPortfolioViewSource(runtime, broker=broker, queue=PaperSignalQueueStore(tmp_path / "queue.sqlite", policy=_policy()))
        cutoff = _publish_missing_middle_close(source, missing=missing)
        view = source.read(as_of=cutoff)
        # A synthetic existing result DTO tests current comparison visibility;
        # it is not claimed as a newly sealed C5 or native research result.
        dates = tuple(point.trade_date for point in view.nav)
        band = PaperBacktestBandResult(input_hash="a" * 64, configuration_fingerprint=runtime.state.configuration.fingerprint,
            backtest_source_hash="b" * 64, dates=dates,
            points=tuple({"day_index": index + 1, "lower": Decimal("1.02") ** (index + 1), "upper": Decimal("1.02") ** (index + 1)} for index in range(len(dates))))
        snapshot = PaperPortfolioSnapshot(available_at=cutoff, accounts=(view.model_copy(update={"band": band}),))
        generations = _generation_ids("baseline", 0)
        tables = build_serving_read_models(ServingReadModelInput(observed_at=cutoff, paper_accounts=(view.frame.account,),
            projections=tuple(ServingProjectionInput.bind(part, owner_dataset_id="paper_accounts", owner_generation_id=generations["paper_accounts"])
                              for part in paper_portfolio_projections(snapshot))))
        root = tmp_path / "serving"
        ServingPublisher(root, producer_commit="0" * 40, schema_version=3, table_specs=SERVING_TABLE_SPECS).publish(
            tables, watermarks=tuple(mark.model_copy(update={"event_time": min(mark.event_time, cutoff), "published_at": min(mark.published_at, cutoff)})
                                     for mark in _watermarks("baseline", built_at=cutoff, generations=generations, sequence=0)),
            source_generations=generations, built_at=cutoff)
        app = create_app(with_test_proxy_identity(WebSettings(serving_root=root)), clock=lambda: cutoff, background=False)
        with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
            response = client.get(f"/api/v1/paper-portfolios/{runtime.state.configuration.binding.account_id}")
            assert response.status_code == 200
            data = response.json()["data"]
            assert data["metrics"]["status"] == "unavailable" and data["metrics"]["running_days"] == 3
            assert data["can_band"] is False and data["band_position"] == "unavailable" and data["band"] is None
            assert tuple(point["trade_date"] for point in data["nav"]) == tuple(day.isoformat() for day in _calendar().dates)
            if missing == "day":
                gap = data["nav"][1]
                assert gap["status"] == "unavailable" and gap["reason"]
                assert gap["nav"] is None and gap["normalized_nav"] is None and gap["daily_return"] is None
