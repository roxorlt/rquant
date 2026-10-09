import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

if TYPE_CHECKING:
    from rquant.paper_portfolio_band import NativePaperBacktestBandInput

from rquant.experiment_registry import DateRange, PromotionStage
from rquant.strategy_authoring_commands import StrategyTemplateHead
from rquant.strategy_promotion_contracts import (
    BoundOuterPromotionEvidence,
    ManualPromotionPolicy,
    SealedPromotionResult,
    StrategyPromotionTarget,
    next_stage,
)

NOW = datetime(2026, 10, 6, 8, tzinfo=UTC)


def target() -> StrategyPromotionTarget:
    return StrategyPromotionTarget(
        source_kind="template",
        owner_id="alice",
        strategy_id="template_example",
        name="研究策略",
        head=StrategyTemplateHead(
            version=1,
            registration_fingerprint="a" * 64,
            record_hash="b" * 64,
            spec_fingerprint="c" * 64,
        ),
        parameter_fingerprint="d" * 64,
        cost_fingerprint="e" * 64,
    )


def reference() -> SealedPromotionResult:
    return SealedPromotionResult(
        job_id="00000000-0000-0000-0000-000000000001",
        spec_hash="1" * 64,
        manifest_hash="2" * 64,
        result_hash="3" * 64,
        input_hash="4" * 64,
        content_hash="5" * 64,
        available_at=NOW,
    )


def native_forward_configuration_data() -> dict[str, object]:
    from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile
    from rquant.runtime_contracts import canonical_sha256
    from tests.unit.test_minute_backtest_producer import original_fixture

    profile = MinuteReplayExecutionProfile.model_validate_json(
        json.dumps(original_fixture()["minute_replay"]["execution_profile"])
    )
    native = target().model_copy(update={
        "source_kind": "builtin", "strategy_id": "n_shape",
        "cost_fingerprint": canonical_sha256(profile.execution_costs),
    })
    return {
        "target": native,
        "binding": {
            "role_id": "native:n_shape", "account_id": profile.paper_policy.account_id,
            "owner_id": native.owner_id, "strategy_id": native.strategy_id,
            "strategy_version": "1", "parameter_fingerprint": native.parameter_fingerprint,
            "cost_spec_id": profile.execution_costs.cost_spec_id,
            "ledger_id": "synthetic-native-ledger", "manifest_fingerprint": "1" * 64,
        },
        "metadata_identity": {"path": "/synthetic/native-metadata", "instance_id": "2" * 32,
            "st_dev": 1, "st_ino": 2},
        "source_key": original_fixture()["frozen_input"]["source_key"],
        "source_version": original_fixture()["frozen_input"]["source_version"],
        "execution_profile": profile, "version": 1, "configured_at": NOW,
        "paper_approval_hash": "3" * 64, "paper_approved_at": NOW,
    }


def test_native_forward_contract_never_casts_daily_portfolio_owner() -> None:
    from rquant.strategy_promotion_contracts import NativeMinuteForwardConfiguration
    from rquant.strategy_authoring_commands import StrategyAuthoringIdentity

    data = native_forward_configuration_data()
    value = NativeMinuteForwardConfiguration.model_validate(data)
    assert type(value.metadata_identity) is StrategyAuthoringIdentity
    assert value.execution_cost_spec == value.execution_profile.execution_costs
    assert len(value.fingerprint) == 64
    assert NativeMinuteForwardConfiguration.model_validate_json(value.model_dump_json()) == value
    for change in (
        {"target": value.target.model_copy(update={"strategy_id": "portfolio_backtest"})},
        {"binding": value.binding.model_copy(update={"owner_id": "other"})},
        {"binding": value.binding.model_copy(update={"account_id": "daily-portfolio"})},
        {"target": value.target.model_copy(update={"cost_fingerprint": "f" * 64})},
        {"configured_at": datetime(2026, 10, 5, tzinfo=UTC)},
    ):
        with pytest.raises(ValueError):
            NativeMinuteForwardConfiguration.model_validate({**value.model_dump(mode="python"), **change})


def test_native_daily_producer_refuses_an_uninstalled_forward_source() -> None:
    from rquant.strategy_live_service import publish_native_forward_close

    with pytest.raises(TypeError, match="original native"):
        publish_native_forward_close(object(), observed_at=NOW, completion_receipt_id="a" * 64)


def native_band_input() -> "NativePaperBacktestBandInput":
    from rquant.backtest.contracts import SSECalendar
    from rquant.paper_portfolio_band import (
        NativePaperBacktestBandInput, NativeSealedPaperBacktestReturns,
        SealedPaperDailyReturn,
    )
    from rquant.strategy_promotion_contracts import NativeMinuteForwardConfiguration

    configuration = NativeMinuteForwardConfiguration.model_validate(native_forward_configuration_data())
    dates = (date(2026, 10, 7), date(2026, 10, 8))
    old_dates = (date(2026, 1, 5), date(2026, 1, 6))
    calendar = SSECalendar(source_identity="6" * 64, coverage_start=old_dates[0],
        coverage_end=dates[-1], dates=(*old_dates, *dates))
    # Typed model/mathematics fixture only. Real producer reads the sealed C6 result.
    returns = NativeSealedPaperBacktestReturns(
        job_id=reference().job_id, owner_id=configuration.target.owner_id,
        strategy_id=configuration.target.strategy_id, strategy_version="1",
        parameter_fingerprint=configuration.target.parameter_fingerprint,
        cost_spec_id=configuration.execution_cost_spec.cost_spec_id,
        calendar_source_identity=calendar.source_identity,
        definition_fingerprint=configuration.target.head.registration_fingerprint,
        definition_record_hash=configuration.target.head.record_hash,
        native_spec_fingerprint=configuration.target.head.spec_fingerprint,
        profile_hash=configuration.execution_profile.profile_hash,
        full_input_hash="7" * 64, source_kind="reconstructed",
        spec_hash="1" * 64, manifest_hash="2" * 64, complete_result_hash="3" * 64,
        backtest_content_hash="4" * 64,
        returns=(SealedPaperDailyReturn(trade_date=old_dates[0], daily_return=Decimal("0.01")),
            SealedPaperDailyReturn(trade_date=old_dates[1], daily_return=Decimal("-0.02"))),
    )
    band = NativePaperBacktestBandInput(configuration=configuration, backtest=returns,
        calendar=calendar, comparison_dates=dates)
    return band


def test_native_band_uses_original_paths_and_complete_same_version_source() -> None:
    from rquant.paper_portfolio_band import (
        NativePaperBacktestBandInput, bootstrap_daily_band, execute_paper_backtest_band,
    )
    from rquant.paper_research import NativePaperResearchAdapterCatalog, FrozenPaperResearchInput

    band = native_band_input()
    configuration, calendar, dates, returns = (
        band.configuration, band.calendar, band.comparison_dates, band.backtest,
    )
    catalog = NativePaperResearchAdapterCatalog(metadata_identity=configuration.metadata_identity,
        configuration=configuration, source_code_identity="8" * 64)
    value = FrozenPaperResearchInput(task_name="paper_backtest_band", catalog=catalog,
        code_sha="9" * 40, available_at=datetime(2026, 10, 8, 8, tzinfo=UTC), band=band)
    assert FrozenPaperResearchInput.model_validate_json(value.model_dump_json()) == value
    assert len(value.fingerprint) == 64
    result = execute_paper_backtest_band(band)
    assert result.paths == 2048 and result.seed == 20261005
    assert result.points == bootstrap_daily_band((Decimal("0.01"), Decimal("-0.02")), days=2)
    for changed in (
        returns.model_copy(update={"profile_hash": "f" * 64}),
        returns.model_copy(update={"native_spec_fingerprint": "f" * 64}),
        returns.model_copy(update={"owner_id": "other"}),
    ):
        with pytest.raises(ValueError):
            NativePaperBacktestBandInput.model_validate({**band.model_dump(mode="python"), "backtest": changed})
    with pytest.raises(ValueError):
        NativePaperBacktestBandInput.model_validate({**band.model_dump(mode="python"),
            "comparison_dates": tuple(point.trade_date for point in returns.returns)})


def test_native_worker_catalog_keeps_original_codec_and_installed_mode_refusal() -> None:
    import json
    from rquant.lab_worker_registry import BuiltinLabShardRuntimeConfig
    from rquant.paper_research import NativePaperResearchAdapterCatalog
    from rquant.strategy_promotion_contracts import NativeMinuteForwardConfiguration

    configuration = NativeMinuteForwardConfiguration.model_validate(native_forward_configuration_data())
    catalog = NativePaperResearchAdapterCatalog(metadata_identity=configuration.metadata_identity,
        configuration=configuration, source_code_identity="8" * 64)
    base = {"schema_version": 1, "configured": False, "adapter_manifest_hash": "a" * 64}
    config = BuiltinLabShardRuntimeConfig(**base, paper_catalog=catalog)
    assert BuiltinLabShardRuntimeConfig.model_validate_json(config.model_dump_json()) == config
    assert config.paper_catalog.metadata_identity == configuration.metadata_identity
    from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperPortfolioStateIdentity
    from rquant.paper_research import PaperResearchAdapterCatalog
    from tests.unit.test_paper_portfolio_core import config_data
    old = PaperResearchAdapterCatalog(
        metadata_identity=PaperPortfolioStateIdentity(path="/synthetic/original-paper", instance_id="original", st_dev=1, st_ino=2),
        configuration=PaperPortfolioConfiguration(**config_data()), source_code_identity="8" * 64,
    )
    old_config = BuiltinLabShardRuntimeConfig(**base, paper_catalog=old)
    old_decoded = BuiltinLabShardRuntimeConfig.model_validate_json(old_config.model_dump_json())
    assert old_decoded == old_config and type(old_decoded.paper_catalog) is PaperResearchAdapterCatalog
    raw = json.loads(config.model_dump_json())
    assert "minute_registry_mode" not in raw and "minute_catalog" not in raw
    raw["paper_catalog"]["contract"] = "invented-native-catalog"
    with pytest.raises(ValueError):
        BuiltinLabShardRuntimeConfig.model_validate_json(json.dumps(raw))
    with pytest.raises(ValueError, match="installed.*catalog"):
        BuiltinLabShardRuntimeConfig(**base, minute_registry_mode="installed")


def test_original_command_reexports_preserve_one_type_and_union() -> None:
    from typing import get_args
    import rquant.paper_operator_commands as operator
    import rquant.paper_research_commands as research
    from rquant.experiment_platform_template_models import ExperimentTemplateSelection as old_selection
    from rquant.strategy_authoring_commands import ExperimentTemplateSelection

    assert old_selection is ExperimentTemplateSelection
    assert operator.RunPaperPortfolioResearch is research.RunPaperPortfolioResearch
    assert operator.OwnedRunPaperPortfolioResearch is research.OwnedRunPaperPortfolioResearch
    assert get_args(operator.PaperPortfolioCommand) == (
        operator.SetPaperAccountPaused, operator.SavePaperPortfolioConfiguration,
        research.RunPaperPortfolioResearch,
    )
    assert get_args(operator.OwnedPaperPortfolioCommand) == (
        operator.OwnedSetPaperAccountPaused, operator.OwnedSavePaperPortfolioConfiguration,
        research.OwnedRunPaperPortfolioResearch,
    )
    original = research.RunPaperPortfolioResearch(command_id=str(reference().job_id),
        requested_at=NOW, generation_id="a" * 64, account_id="original-paper",
        configuration_fingerprint="b" * 64, task_name="paper_backtest_band",
        backtest_job_id=reference().job_id)
    assert operator.RunPaperPortfolioResearch.model_validate_json(original.model_dump_json()) == original


def test_native_forward_state_requires_original_owner_and_human_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.paper_research_runtime import NativeMinuteForwardState
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
    from tests.support.strategy_promotion_native_fixture import native_forward_owner_fixture

    owner, configuration, _, roles_path = native_forward_owner_fixture(tmp_path, monkeypatch)
    with pytest.raises((ValueError, PermissionError), match="approval|stage"):
        NativeMinuteForwardState(owner, configuration.model_copy(update={"paper_approval_hash": "f" * 64}))
    state = NativeMinuteForwardState(owner, configuration)
    assert type(state.identity()) is StrategyAuthoringIdentity
    assert state.store is owner.domain.store and state.refresh_configuration() == configuration
    assert state.configuration_at(configuration.fingerprint, version=1) == configuration
    assert NativeMinuteForwardState(owner, configuration).identity() == configuration.metadata_identity
    changed = RoleState.create(revision=3, users=(RoleEntry(username=configuration.target.owner_id, role="viewer"),
        RoleEntry(username="root", role="admin")))
    roles_path.write_text(changed.model_dump_json())
    assert state.refresh_configuration() == configuration
    with pytest.raises(PermissionError):
        with state._connection(write=True):
            pytest.fail("withdrawn writer entered the original metadata transaction")
    with pytest.raises(PermissionError):
        state.authorize("another-owner", read=True)
    with pytest.raises(ValueError, match="configuration|version"):
        state.configuration_at("f" * 64, version=1)


def test_native_close_uses_original_financial_ledger_nav_and_honest_gaps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.paper_portfolio_views import NativePaperCloseMaterials, PaperPortfolioViewStore
    from rquant.paper_research_runtime import NativeMinuteForwardState
    from rquant.paper_portfolio_ledger import PaperPortfolioLedgerSource
    from rquant.paper_broker import BrokerCostPolicy, PaperBrokerStore
    from rquant.backtest.contracts import SSECalendar
    from rquant.live_contracts import CurrentPointer, BatchEnvelope, BatchQualityStatus, LiveChannel
    from rquant.strategy_promotion_contracts import NativeMinuteForwardValuation
    from tests.support.strategy_promotion_native_fixture import native_forward_owner_fixture

    owner, original, clock, _ = native_forward_owner_fixture(tmp_path, monkeypatch)
    profile = original.execution_profile
    broker = PaperBrokerStore(tmp_path / "native-ledger.sqlite", account_id=profile.paper_policy.account_id,
        initial_cash=profile.initial_cash, cost_policy=BrokerCostPolicy.from_execution_cost_spec(profile.execution_costs))
    with broker._connect() as connection:
        _, head = broker._attestation_head(connection)
    configuration = original.model_copy(update={"binding": original.binding.model_copy(update={
        "ledger_id": head["ledger_generation"]})})
    state = NativeMinuteForwardState(owner, configuration)
    source = PaperPortfolioLedgerSource(path=broker.path, account_id=broker.account_id,
        initial_cash=broker.initial_cash, cost_policy=broker.cost_policy)
    dates = (date(2026, 10, 7), date(2026, 10, 8), date(2026, 10, 9))
    calendar = SSECalendar(source_identity="4" * 64, coverage_start=dates[0],
        coverage_end=dates[-1], dates=dates)
    close = datetime(2026, 10, 7, 7, tzinfo=UTC)
    clock[0] = close
    # This financial-reader test uses a synthetic typed market pointer only.
    # The installed producer must obtain its real pointer and PIT quotes itself.
    pointer = CurrentPointer(channel=LiveChannel.MARKET_MINUTE, source_generation_id="5" * 64,
        batch_id="synthetic-native-market", sequence=0, revision=1, content_sha256="6" * 64,
        quality_status=BatchQualityStatus.PUBLISHED, published_at=close)
    account = broker.account_snapshot(as_of=close, market_prices={})
    envelope = BatchEnvelope(schema_version=1, channel=pointer.channel, dataset_id="market_minute",
        source="synthetic-contract", source_request_id="synthetic-close", batch_id=pointer.batch_id,
        sequence=pointer.sequence, revision=pointer.revision, event_time_start=close, event_time_end=close,
        source_time=close, received_at=close, available_at=close, row_count=0, content_sha256=pointer.content_sha256,
        quality_status=pointer.quality_status, producer_version="synthetic", producer_commit=profile.paper_policy.producer_commit)
    valuation = NativeMinuteForwardValuation(input_hash=configuration.fingerprint, trade_date=dates[0],
        as_of=close, observed_at=close, profile_hash=profile.profile_hash, calendar_sha256="7" * 64,
        status="complete", market_pointer=pointer, market_envelope=envelope, account=account)
    material = NativePaperCloseMaterials(configuration=configuration, calendar=calendar,
        trade_date=dates[0], close_at=close, available_at=close, prices=(), valuation=valuation,
        trade_calendar_sha256="7" * 64)
    store = PaperPortfolioViewStore(state)
    nav = store.record_close(source, material, published_at=close)
    assert nav.normalized_nav == Decimal(1) and nav.daily_return == Decimal(0)
    assert nav.ledger_revision == source.read(configuration=configuration, as_of=close, prices={}).ledger_revision
    assert nav.account == account
    assert store.record_close(source, material, published_at=close) == nav
    assert source.read(configuration=configuration, as_of=close, prices={}).reconciliation.is_consistent
    with pytest.raises(ValueError, match="ledger"):
        source.read(configuration=configuration.model_copy(update={"binding": configuration.binding.model_copy(
            update={"ledger_id": "unrelated-ledger"})}), as_of=close, prices={})
    gap_at = datetime(2026, 10, 9, 7, tzinfo=UTC)
    clock[0] = gap_at
    gap = NativeMinuteForwardValuation(input_hash=configuration.fingerprint, trade_date=dates[-1], as_of=gap_at,
        observed_at=gap_at, profile_hash=profile.profile_hash, calendar_sha256="7" * 64,
        status="unavailable", unavailable_reasons=("original_market_close_missing",))
    gap_material = NativePaperCloseMaterials(configuration=configuration, calendar=calendar,
        trade_date=dates[-1], close_at=gap_at, available_at=gap_at, prices=(), valuation=gap,
        trade_calendar_sha256="7" * 64)
    unavailable = store.record_close(source, gap_material, published_at=gap_at)
    assert unavailable.status == "unavailable" and unavailable.normalized_nav is None
    assert unavailable.daily_return is None and unavailable.account is None
    series = store.nav_series()
    assert tuple(point.trade_date for point in series) == dates
    assert series[1].status == "unavailable" and series[1].daily_return is None
    previous_at = datetime(2026, 10, 8, 7, tzinfo=UTC)
    previous = NativePaperCloseMaterials(configuration=configuration, calendar=calendar, trade_date=dates[1],
        close_at=previous_at, available_at=previous_at, prices=(), trade_calendar_sha256="7" * 64,
        valuation=valuation.model_copy(update={"trade_date": dates[1], "as_of": previous_at,
            "observed_at": previous_at, "market_pointer": pointer.model_copy(update={"published_at": previous_at}),
            "market_envelope": envelope.model_copy(update={"event_time_start": previous_at,
                "event_time_end": previous_at, "source_time": previous_at, "received_at": previous_at,
                "available_at": previous_at}),
            "account": broker.account_snapshot(as_of=previous_at, market_prices={})}))
    with pytest.raises(PermissionError) as error:
        store.record_close(source, previous, published_at=gap_at)
    assert isinstance(error.value.__cause__, ValueError)
    assert "contemporaneous" in str(error.value.__cause__)


def test_native_forward_runtime_requires_installed_original_peers() -> None:
    from rquant.paper_research_runtime import NativeMinuteForwardRuntime, NativeMinuteForwardViewSource
    from types import SimpleNamespace

    with pytest.raises(TypeError, match="original|concrete"):
        NativeMinuteForwardRuntime(state=SimpleNamespace(), broker=SimpleNamespace(),
            queue=SimpleNamespace(), runner=SimpleNamespace(), quote=SimpleNamespace(),
            calendar=SimpleNamespace(), manifest=SimpleNamespace())
    with pytest.raises(TypeError, match="original|concrete"):
        NativeMinuteForwardViewSource(SimpleNamespace())


def test_native_original_broker_owner_keeps_wal_for_readonly_peers_and_closes(tmp_path: Path) -> None:
    import gc
    from rquant.paper_broker import BrokerCostPolicy, PaperBrokerStore, PaperBrokerReconciliationError
    from rquant.runtime_builder_paper import PaperBrokerSettings, paper_broker_builder
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
    from rquant.runtime_service_control import RuntimeServicePlane
    from rquant.signal_contracts import SignalAction
    from rquant.signal_bus import SignalBusStore
    from rquant.signal_route_spool import SignalRouteSpool

    profile = native_forward_configuration_data()["execution_profile"]
    policy = profile.paper_policy
    bus = SignalBusStore(tmp_path / "original-bus.sqlite")
    route = SignalRouteSpool(tmp_path / "original-route")
    route.publish(source=bus.source_descriptor(), records=())
    settings = PaperBrokerSettings(account_id=policy.account_id,
        execution_lag_seconds=int(policy.execution_lag.total_seconds()),
        buy_quantity=policy.action_quantities[SignalAction.B_INTENT],
        reduce_quantity=policy.action_quantities[SignalAction.REDUCE],
        sell_quantity=policy.action_quantities[SignalAction.S_INTENT],
        signal_spool_root=route.paths.root, queue_path=tmp_path / "original-queue.sqlite",
        consumer_state_path=tmp_path / "original-consumer.sqlite", broker_path=tmp_path / "original-broker.sqlite",
        initial_cash=profile.initial_cash, execution_cost_spec=profile.execution_costs, limit=1, paused=True)
    manifest = RuntimeServiceManifest(service_id="paper:native-lifecycle", service_kind=RuntimeServiceKind.PAPER_BROKER,
        plane=RuntimeServicePlane.LIVE, interval_seconds=2, stale_after_seconds=20,
        producer_commit=policy.producer_commit, settings=settings.model_dump(mode="json"))

    def no_trade(*args: object, **kwargs: object) -> None:
        raise AssertionError("paused empty owner cannot request an execution quote")

    step = paper_broker_builder(clock=lambda: NOW, quote_resolver=no_trade,
        trade_date_resolver=lambda _: NOW.date())(manifest)
    cost = BrokerCostPolicy.from_execution_cost_spec(profile.execution_costs)
    try:
        gc.collect()
        for _ in range(2):
            with PaperBrokerStore.open_readonly(settings.broker_path, account_id=policy.account_id,
                initial_cash=profile.initial_cash, cost_policy=cost) as peer:
                assert peer.account_snapshot(as_of=NOW, market_prices={}).cash == profile.initial_cash
                assert peer.reconcile().is_consistent
            gc.collect()
        assert step().processed_count == 0
        # SQLite itself rejects a close from the wrong thread. The owner must
        # retain the session so its original lifecycle can still close it.
        from threading import Thread
        import sqlite3
        errors: list[BaseException] = []

        def wrong_thread_close() -> None:
            try:
                step.close()
            except BaseException as error:
                errors.append(error)

        thread = Thread(target=wrong_thread_close)
        thread.start()
        thread.join(timeout=2)
        assert not thread.is_alive() and len(errors) == 1 and isinstance(errors[0], sqlite3.ProgrammingError)
        with PaperBrokerStore.open_readonly(settings.broker_path, account_id=policy.account_id,
            initial_cash=profile.initial_cash, cost_policy=cost) as peer:
            assert peer.reconcile().is_consistent
        step.close()
        step.close()
        gc.collect()
        assert not settings.broker_path.with_name(settings.broker_path.name + "-wal").exists()
        with pytest.raises(PaperBrokerReconciliationError, match="safe side file"):
            with PaperBrokerStore.open_readonly(settings.broker_path, account_id=policy.account_id,
                initial_cash=profile.initial_cash, cost_policy=cost):
                raise AssertionError("closed owner must not invent a safe WAL source")
    finally:
        closer = getattr(step, "close", None)
        if closer is not None:
            closer()


def test_native_band_adapter_and_original_owned_identity_decoder() -> None:
    from rquant.paper_research import NativePaperResearchAdapterCatalog
    from rquant.paper_research_adapter import paper_research_adapter_registry
    from rquant.paper_research_commands import OwnedRunPaperPortfolioResearch
    from rquant.strategy_promotion_contracts import NativeMinuteForwardConfiguration
    from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
    from rquant.paper_portfolio_models import PaperPortfolioStateIdentity
    from typing import get_args

    configuration = NativeMinuteForwardConfiguration.model_validate(native_forward_configuration_data())
    catalog = NativePaperResearchAdapterCatalog(metadata_identity=configuration.metadata_identity,
        configuration=configuration, source_code_identity="8" * 64)
    adapter = paper_research_adapter_registry(catalog).get("paper-backtest-band", "1")
    assert adapter.catalog == catalog
    assert adapter.source_usage().expected_calls == 0 and not adapter.source_usage().external
    assert set(get_args(OwnedRunPaperPortfolioResearch.model_fields["metadata_identity"].annotation)) == {
        StrategyAuthoringIdentity, PaperPortfolioStateIdentity}


def test_native_research_source_identity_binds_its_complete_consumer_code() -> None:
    import hashlib
    import rquant.paper_research_source as source
    from rquant.runtime_contracts import canonical_sha256

    native = source.paper_research_code_identity(native=True)
    root = Path(source.__file__).parent
    original = canonical_sha256({name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in source._CODE_FILES})
    assert source.paper_research_code_identity() == original
    assert native != original
    assert "paper_research_runtime.py" in source._NATIVE_CODE_FILES
    assert "minute_backtest_artifact.py" in source._NATIVE_CODE_FILES
    assert native == canonical_sha256({name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in source._NATIVE_CODE_FILES})


def test_native_close_keeps_actual_late_clock_and_original_publication(tmp_path: Path) -> None:
    import hashlib
    from datetime import timedelta
    from rquant.live_spool import LiveBatchSpool
    from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
    from rquant.paper_broker import PaperBrokerStore, BrokerCostPolicy
    from rquant.minute_backtest_contracts import MinuteRuntimeDailyValuation
    from rquant.strategy_promotion_contracts import NativeMinuteForwardValuation

    close = datetime(2026, 10, 7, 7, tzinfo=UTC)
    raw = LiveBatchSpool(tmp_path / "original-market")
    payload = b"synthetic-contract-only-empty-market"
    envelope = BatchEnvelope(schema_version=1, channel=LiveChannel.MARKET_MINUTE,
        dataset_id="market_minute", source="synthetic", source_request_id="synthetic-close",
        batch_id="synthetic-native-close", sequence=0, revision=1, event_time_start=close,
        event_time_end=close, source_time=close, received_at=close, available_at=close,
        row_count=0, content_sha256=hashlib.sha256(payload).hexdigest(), quality_status=BatchQualityStatus.PUBLISHED,
        producer_version="synthetic-contract", producer_commit="a" * 40)
    raw.publish(envelope, payload)
    pointer = raw.current(LiveChannel.MARKET_MINUTE)
    profile = native_forward_configuration_data()["execution_profile"]
    broker = PaperBrokerStore(tmp_path / "original-account.sqlite", account_id="native-test",
        initial_cash=profile.initial_cash, cost_policy=BrokerCostPolicy.from_execution_cost_spec(profile.execution_costs))
    values = dict(input_hash="1" * 64, trade_date=close.date(), as_of=close,
        observed_at=close + timedelta(seconds=5), profile_hash="2" * 64, calendar_sha256="3" * 64,
        status="complete", market_pointer=pointer, market_envelope=envelope,
        account=broker.account_snapshot(as_of=close, market_prices={}))
    value = NativeMinuteForwardValuation(**values)
    assert value.as_of == close and value.observed_at == close + timedelta(seconds=5)
    assert NativeMinuteForwardValuation.model_validate_json(value.model_dump_json()) == value
    for delta in (timedelta(seconds=-1), timedelta(hours=6, seconds=1), timedelta(days=1)):
        with pytest.raises(ValueError, match="observation|close"):
            NativeMinuteForwardValuation(**{**values, "observed_at": close + delta})
    for change in (
        {"market_envelope": envelope.model_copy(update={"content_sha256": "f" * 64})},
        {"market_envelope": None},
        {"market_pointer": pointer.model_copy(update={"published_at": close + timedelta(seconds=1)})},
        {"account": broker.account_snapshot(as_of=close + timedelta(seconds=5), market_prices={})},
    ):
        with pytest.raises(ValueError):
            NativeMinuteForwardValuation(**{**values, **change})
    with pytest.raises(ValueError, match="observation"):
        MinuteRuntimeDailyValuation.model_validate({key: item for key, item in values.items()
            if key != "market_envelope"})


def test_native_family_context_keeps_native_configuration_and_phase() -> None:
    from rquant.experiment_platform import NativeMinuteExperimentRequest
    from rquant.experiment_platform_projection import ExperimentSearchContext
    from rquant.minute_backtest_formal import MinuteExperimentProtocol
    from rquant.strategy_promotion_contracts import NativeMinuteConfiguration, NativeMinuteSelection

    native = target().model_copy(update={"source_kind": "builtin", "strategy_id": "n_shape"})
    selection = NativeMinuteSelection(target=native, source_key="native.research", source_version=1, profile_hash="f" * 64)
    configuration = NativeMinuteConfiguration(selection=selection, start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))
    request = NativeMinuteExperimentRequest(name="原生策略", configurations=(configuration,), protocol=MinuteExperimentProtocol(
        train_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 1, 1)),
        validation_range=DateRange(start_date=date(2026, 1, 2), end_date=date(2026, 1, 2)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 1, 3), end_date=date(2026, 1, 3))))
    context = ExperimentSearchContext.from_request(request)
    assert context.kind == "native_minute" and context.configuration_count == 1
    assert context.protocol == request.protocol and context.profile_hash == selection.profile_hash
    assert context.template is None and context.walk_forward_plan_hash is None
    with pytest.raises(ValueError, match="complete|interval"):
        NativeMinuteExperimentRequest.model_validate({**request.model_dump(mode="python"),
            "configurations": (configuration.model_copy(update={"end_date": configuration.start_date}),)})


def test_native_outer_grant_retains_the_selected_native_source() -> None:
    from uuid import uuid4
    from rquant.experiment_platform import ExperimentOuterGrant, HoldoutPolicy
    from rquant.strategy_promotion_contracts import NativeMinuteConfiguration, NativeMinuteSelection

    native = target().model_copy(update={"source_kind": "builtin", "strategy_id": "auction_gap"})
    selection = NativeMinuteSelection(target=native, source_key="native.auction-gap", source_version=2,
        profile_hash="f" * 64)
    configuration = NativeMinuteConfiguration(selection=selection,
        start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))
    values = dict(grant_id="1" * 64, owner=native.owner_id, request_id=uuid4(),
        family_id="original-parent", experiment_id="2" * 64, config=configuration,
        outer_range=DateRange(start_date=date(2026, 1, 3), end_date=date(2026, 1, 3)),
        policy=HoldoutPolicy(version=1, months=0, updated_at=NOW), admitted_at=NOW,
        source_key=selection.source_key, source_version=selection.source_version,
        body_hash="3" * 64, result_hash="4" * 64, source_identity="5" * 64)
    grant = ExperimentOuterGrant(**values)
    assert grant.config == configuration and (grant.source_key, grant.source_version) == (
        selection.source_key, selection.source_version)
    for changed in ({"source_key": "native.n-shape"}, {"source_version": 1}, {"owner": "other"}):
        with pytest.raises(ValueError, match="native|selected|owner|source"):
            ExperimentOuterGrant(**(values | changed))


def test_manual_policy_is_exact_version_and_differs_from_old_thresholds() -> None:
    policy = ManualPromotionPolicy()
    assert policy.validation_trades == 30 and policy.validation_sharpe == Decimal("0.8")
    assert policy.adjusted_p_limit == Decimal("0.05") and policy.p_is_strict
    assert policy.fold_count == 6 and policy.positive_folds == 4
    assert policy.forward_open_days == 20
    assert len(policy.fingerprint) == 64
    for name, value in (("validation_trades", 29), ("fold_count", 5), ("forward_open_days", 10)):
        with pytest.raises(ValidationError):
            ManualPromotionPolicy.model_validate({name: value})


def test_target_fingerprint_cannot_cross_owner_version_or_cost() -> None:
    original = target()
    assert original.fingerprint != original.model_copy(update={"owner_id": "bob"}).fingerprint
    assert (
        original.fingerprint
        != original.model_copy(update={"cost_fingerprint": "f" * 64}).fingerprint
    )
    assert (
        original.fingerprint
        != original.model_copy(
            update={"head": original.head.model_copy(update={"version": 2})}
        ).fingerprint
    )


@pytest.mark.parametrize(
    "stage,expected",
    [
        (PromotionStage.EXPLORATORY, PromotionStage.COMPARABLE),
        (PromotionStage.COMPARABLE, PromotionStage.PAPER_CANDIDATE),
        (PromotionStage.PAPER_CANDIDATE, PromotionStage.MONITOR_APPROVED),
    ],
)
def test_stage_has_only_one_next_transition(
    stage: PromotionStage, expected: PromotionStage
) -> None:
    assert next_stage(stage) is expected


def test_terminal_stage_cannot_be_advanced() -> None:
    with pytest.raises(ValueError):
        next_stage(PromotionStage.MONITOR_APPROVED)


@pytest.mark.parametrize(
    "net,passed", [(Decimal("-0.01"), False), (Decimal(0), False), (Decimal("0.01"), True)]
)
def test_outer_requires_strict_positive_original_net_return(net: Decimal, passed: bool) -> None:
    evidence = BoundOuterPromotionEvidence(
        target=target(),
        parent_experiment_id="6" * 64,
        outer_experiment_id="7" * 64,
        grant_hash="8" * 64,
        reference=reference(),
        window=DateRange(start_date=date(2026, 9, 1), end_date=date(2026, 9, 30)),
        net_return=net,
    )
    assert evidence.passed is passed


def test_result_requires_all_original_digests_and_aware_time() -> None:
    value = reference().model_dump(mode="python")
    value["manifest_hash"] = "bad"
    with pytest.raises(ValidationError):
        SealedPromotionResult.model_validate(value)
    value = reference().model_dump(mode="python")
    value["available_at"] = datetime(2026, 10, 6)
    with pytest.raises(ValidationError):
        SealedPromotionResult.model_validate(value)
