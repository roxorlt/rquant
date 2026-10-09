"""Each historical day uses the actual original screening and portfolio contracts."""

from __future__ import annotations

import importlib
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.portfolio_backtest_source import PortfolioSourceData
from rquant.screen.query_admission import ScreenQueryExecutor, ScreenQueryPrivateConfig
from rquant.screen.query_contracts import ScreenQueryDefinition
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_web_screen_replica import _replica_world, _publish
from tests.unit.test_backtest_preparation import source_data
from tests.paper_cost_fixtures import paper_instrument_context


def historical_source_fixture(tmp_path: Path):
    primary, replica, latest = _replica_world(tmp_path, days=3)
    with DuckDBStore(primary) as store:
        store._conn.execute("UPDATE daily_basic SET circ_mv=CASE ts_code WHEN '600002.SH' THEN 50000 ELSE 200000 END WHERE trade_date=?", [latest - timedelta(days=1)])
    _publish(primary, replica)
    executor = ScreenQueryExecutor(ScreenQueryPrivateConfig(socket_path=Path("/private/tmp/offline-unused-historical.sock"), trusted_web_uid=os.geteuid()+1, shared_gid=os.getegid(), allowed_users=frozenset({"researcher"}), serving_root=tmp_path / "absent-serving", primary_path=primary, replica_path=replica))
    base = source_data().model_dump(mode="python")
    dates = [latest - timedelta(days=2), latest - timedelta(days=1), latest, latest + timedelta(days=1)]
    old_dates = base["template"]["calendar"]["dates"]
    convert = dict(zip(old_dates, dates, strict=True))
    template = base["template"]
    template["calendar"].update(dates=tuple(dates), coverage_start=dates[0], coverage_end=dates[-1])
    def converted_time(value: datetime | None):
        return None if value is None else datetime.combine(convert[value.date()], value.timetz())
    codes = ("600001.SH", "600002.SH", "600003.SH")
    for day in template["days"]:
        day["trade_date"] = convert[day["trade_date"]]
        day["ranking"]["source_trade_date"] = convert[day["ranking"]["source_trade_date"]]
        day["ranking"]["observed_at"] = converted_time(day["ranking"]["observed_at"])
        day["ranking"]["candidates"] = tuple({"ts_code": code, "rank_score": "1", "industry_l1": "样本行业"} for code in codes)
        for code, quote in zip(codes, day["instruments"], strict=True):
            quote["ts_code"] = code
            quote["instrument_context"] = paper_instrument_context(code).model_dump(mode="python")
            for name in ("classification_observed_at", "decision_price_observed_at", "open_observed_at", "close_observed_at"):
                quote[name] = converted_time(quote[name])
            quote["conditions"]["observed_at"] = converted_time(quote["conditions"]["observed_at"])
    base["benchmarks"] = {key: tuple((convert[day], price) for day, price in rows) for key, rows in base["benchmarks"].items()}
    base["material_hash"] = None
    material = PortfolioSourceData.model_validate(base)
    config = PortfolioBacktestConfig.from_request(material.template)
    available = executor.service.replica.available_dates()
    definition = ScreenQueryDefinition(trade_date=latest, source_kind="replica", source_identity=available.identity, conditions=({"name": "not_st", "args": {}},), ranking={"conditions": [{"metric": "CIRC_MV[0]", "ascending": True, "weight": 1}], "top_n": 1})
    return executor, material, config, definition


def test_complete_interval_uses_distinct_original_previous_day_candidates(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_screen_backtest_source")
    executor, base, config, definition = historical_source_fixture(tmp_path)
    producer = module.AIHistoricalScreenSource(screen=executor, base=base, default_config=config)
    result = producer.build(definition, start_date=config.start_date, end_date=config.end_date)
    assert [day.ranking.candidates[0].ts_code for day in result.source.template.days] == ["600001.SH", "600002.SH"]
    assert tuple(day.source_trade_date for day in result.proof.days) == tuple(day.ranking.source_trade_date for day in base.template.days)
    assert result.config.weight_rule == config.weight_rule
    assert result.config.rebalance_rule == config.rebalance_rule
    assert result.config.execution_cost_spec == config.execution_cost_spec
    assert result.config.initial_cash == config.initial_cash
    assert result.proof.complete and result.proof.candidate_count == 2
    from rquant.portfolio_backtest_source import freeze_portfolio_config
    frozen = freeze_portfolio_config(result.source, result.config)
    assert frozen.benchmark_unavailable is None
    assert frozen.sources.market_hash == base.sources.market_hash


@pytest.mark.parametrize("defect", ["missing_price", "missing_benchmark", "missing_industry", "missing_date", "wrong_generation"])
def test_incomplete_material_stops_confirmation_without_daily_clone(tmp_path: Path, defect: str) -> None:
    module = importlib.import_module("rquant.ai_screen_backtest_source")
    executor, base, config, definition = historical_source_fixture(tmp_path)
    raw = base.model_dump(mode="python")
    raw["material_hash"] = None
    if defect == "missing_price":
        raw["template"]["days"][0]["instruments"][0]["open_price"] = None
        raw["template"]["days"][0]["instruments"][0]["open_observed_at"] = None
    elif defect == "missing_benchmark":
        raw["benchmarks"] = {}
    elif defect == "missing_industry":
        raw["template"]["days"][0]["ranking"]["candidates"][0]["industry_l1"] = None
        config = config.model_copy(update={"weight_rule": config.weight_rule.model_copy(update={"max_industry_weight": Decimal("0.50")})})
    elif defect == "missing_date":
        config = config.model_copy(update={"start_date": config.start_date - timedelta(days=1)})
    else:
        definition = definition.model_copy(update={"source_identity": "0"*64})
    producer = module.AIHistoricalScreenSource(screen=executor, base=PortfolioSourceData.model_validate(raw), default_config=config)
    with pytest.raises((ValueError, PermissionError)):
        producer.build(definition, start_date=config.start_date, end_date=config.end_date)


def prepared_history_fixture(tmp_path: Path):
    from datetime import UTC
    from uuid import uuid4
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from rquant.screen.query_contracts import ExecuteScreenQuery
    from rquant.screen.query_history import ScreenQueryHistory, prepare_private_screen_outbox
    screen, base, config, definition = historical_source_fixture(tmp_path)
    now = datetime.now(UTC) + timedelta(seconds=1)
    path = tmp_path / "private" / "original.sqlite3"
    prepare_private_screen_outbox(path)
    outbox = PageControlOutbox(path)
    history = ScreenQueryHistory(outbox, cursor_key=screen.cursor_key)
    service = PageControlService(outbox=outbox, consumer=PageControlConsumer(outbox=outbox,
        data_dir=tmp_path / "data", log_dir=tmp_path / "logs", clock=lambda: now,
        screen_query_history=history, screen_query_executor=screen))
    command = ExecuteScreenQuery(command_id=str(uuid4()), requested_at=now, definition=definition)
    result = service._submit_trusted_screen_query(command, authenticated_actor_id="researcher")
    assert result.status.value == "succeeded", result
    return screen, base, config, service, history, command, now


def test_prepare_is_private_immutable_original_history_and_lookup_first(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_screen_backtest_source")
    from uuid import uuid4
    from rquant.web.models.ai_assistance import AIBacktestPrepareRequest
    from rquant.ai_usage import AIRequestConflict, AIRequestNotFound
    screen, base, config, service, history, command, now = prepared_history_fixture(tmp_path)
    original = AIBacktestPrepareRequest(request_id=uuid4(), execution_id=command.command_id,
        start_date=config.start_date, end_date=config.end_date)
    pipeline = module.AIScreenBacktestPipeline(history=history,
        source=module.AIHistoricalScreenSource(screen=screen, base=base, default_config=config),
        artifacts=module.AIScreenBacktestArtifacts(tmp_path / "private" / "prepared"), clock=lambda: now)
    view = pipeline.prepare("researcher", original)
    assert view.candidate_count == 2 and view.complete
    assert view.config.initial_cash == config.initial_cash
    assert service.outbox.ai_usage_account_calls("shared", config.start_date) == 0
    screen.service.replica = None
    assert pipeline.prepare("researcher", original) == view
    assert pipeline.provider(view.config.source_key, view.config.source_version).material_hash == view.material_sha256
    with pytest.raises(AIRequestNotFound):
        pipeline.prepare("other", original)
    with pytest.raises(AIRequestConflict):
        pipeline.prepare("researcher", original.model_copy(update={"end_date": config.start_date}))
    assert len(tuple((tmp_path / "private" / "prepared").iterdir())) == 1


def test_prepared_source_file_change_is_rejected_before_original_preparer(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_screen_backtest_source")
    from uuid import uuid4
    from rquant.web.models.ai_assistance import AIBacktestPrepareRequest
    screen, base, config, service, history, command, now = prepared_history_fixture(tmp_path)
    root = tmp_path / "private" / "prepared"
    pipeline = module.AIScreenBacktestPipeline(history=history,
        source=module.AIHistoricalScreenSource(screen=screen, base=base, default_config=config),
        artifacts=module.AIScreenBacktestArtifacts(root), clock=lambda: now)
    view = pipeline.prepare("researcher", AIBacktestPrepareRequest(request_id=uuid4(), execution_id=command.command_id,
        start_date=config.start_date, end_date=config.end_date))
    file = next(root.iterdir())
    file.write_bytes(file.read_bytes().replace(b'600001.SH', b'600004.SH'))
    with pytest.raises(ValueError):
        pipeline.provider(view.config.source_key, view.config.source_version)


def test_confirm_uses_actual_original_preparer_spool_and_idempotent_receipt(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_screen_backtest_source")
    from uuid import uuid4
    from rquant.web.models.ai_assistance import AIBacktestPrepareRequest, AIBacktestConfirmRequest
    from tests.support.ai_assistance_fixture import build_portfolio_foundation
    screen, base, config, service, history, command, now = prepared_history_fixture(tmp_path)
    foundation = build_portfolio_foundation(tmp_path / "lab", base, clock=lambda: now)
    pipeline = module.AIScreenBacktestPipeline(history=history,
        source=module.AIHistoricalScreenSource(screen=screen, base=base, default_config=config),
        artifacts=module.AIScreenBacktestArtifacts(tmp_path / "private" / "prepared"), clock=lambda: now)
    writer = module.build_original_portfolio_writer(pipeline=pipeline, commands=foundation.commands,
        metadata_path=foundation.metadata_path, catalog_path=foundation.catalog_path,
        lake_root=foundation.lake_root, input_root=foundation.input_root,
        protocol=foundation.protocol, code_commit=foundation.code_commit, clock=lambda: now)
    service.consumer.portfolio_backend = writer
    view = pipeline.prepare("researcher", AIBacktestPrepareRequest(request_id=uuid4(), execution_id=command.command_id,
        start_date=config.start_date, end_date=config.end_date))
    request = AIBacktestConfirmRequest(command_id=uuid4(), requested_at=now,
        prepared_request_id=view.request_id, config_sha256=view.config_sha256, proof_sha256=view.proof_sha256)
    result = pipeline.confirm("researcher", request, control=service)
    assert result.receipt.status.value == "succeeded", result
    assert service.outbox.authorize_ai_portfolio_result('researcher',result.job_id).config==view.config
    with pytest.raises(PermissionError):
        service.outbox.authorize_ai_portfolio_result('other',result.job_id)
    assert pipeline.confirm("researcher", request, control=service) == result
    assert len(foundation.commands.spool.pending()) == 1
    assert len(tuple(foundation.input_root.iterdir())) == 1
    with pytest.raises((ValueError, PermissionError)):
        pipeline.confirm("researcher", request.model_copy(update={"proof_sha256": "0"*64}), control=service)


def test_private_profile_installs_original_default_producer_without_enabling_model(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ai_screen_backtest_source")
    import hashlib
    from rquant.ai_assistance_admission import AIPrivateConfig
    from rquant.lab_artifact_export import LabJobZipExportFacade
    from rquant.lab_page_control import LabPageControlWriter
    from rquant.page_control_service import build_page_control_service_with_dependencies
    from tests.support.ai_assistance_fixture import build_portfolio_foundation
    screen, base, config, _, _, _, now = prepared_history_fixture(tmp_path)
    foundation = build_portfolio_foundation(tmp_path / "lab", base, clock=lambda: now)
    base_file = tmp_path / "private" / "base.json"
    base_file.write_bytes(base.model_dump_json().encode())
    base_file.chmod(0o600)
    profile = module.AIHistoricalProfile(base_source_file=base_file,
        base_source_sha256=hashlib.sha256(base_file.read_bytes()).hexdigest(), default_config=config,
        artifact_root=tmp_path / "private" / "prepared-profile", metadata_path=foundation.metadata_path,
        catalog_path=foundation.catalog_path, lake_root=foundation.lake_root, input_root=foundation.input_root,
        protocol=foundation.protocol, code_commit=foundation.code_commit,
        lab_jobs_path=foundation.reader.path, lab_command_spool_path=foundation.commands.spool.root,
        lab_final_artifact_root=foundation.artifacts.root)
    profile_file = tmp_path / "private" / "profile.json"
    profile_file.write_text(profile.model_dump_json())
    profile_file.chmod(0o600)
    cfg = AIPrivateConfig(account_id="shared", model_id="gpt-test",
        socket_path=Path("/private/tmp/unused-ai-profile.sock"), trusted_web_uid=os.geteuid()+1,
        shared_gid=os.getegid(), allowed_users=frozenset({"researcher"}), historical_profile_file=profile_file)
    lab = LabPageControlWriter(commands=foundation.commands, zip_exports=LabJobZipExportFacade(
        reader=foundation.reader, artifact_store=foundation.artifacts, export_root=tmp_path / "exports"))
    control = build_page_control_service_with_dependencies(outbox_path=tmp_path / "profile-control" / "original.sqlite3",
        data_dir=tmp_path / "data", log_dir=tmp_path / "logs", allowed_lab_export_roots=(),
        lab_backend=lab, load_default_lab_backend=False, screen_query_executor=screen,
        screen_query_cursor_key=screen.cursor_key, ai_config=cfg, clock=lambda: now)
    assert control.ai_assistance.provider is None
    assert control.ai_assistance.backtests.history is control.consumer.screen_query_history
    assert control.consumer.portfolio_backend.commands is foundation.commands
    assert control.ai_assistance.control is control
    assert control.ai_assistance.capabilities("researcher").can_prepare_backtest
    assert not control.ai_assistance.capabilities("researcher").can_generate
