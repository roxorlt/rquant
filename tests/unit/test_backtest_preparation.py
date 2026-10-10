from __future__ import annotations

import importlib
import importlib.util
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from pydantic import ValidationError

from tests.unit.test_backtest_platform import config, frozen


def source_module() -> ModuleType:
    name = "rquant.portfolio_backtest_source"
    assert importlib.util.find_spec(name), "trusted portfolio preparation is missing"
    return importlib.import_module(name)


def source_data() -> Any:
    value = frozen()
    return source_module().PortfolioSourceData(
        source_key=value.config.source_key,
        source_version=value.config.source_version,
        template=value.request,
        sources=value.sources,
        benchmarks={value.config.benchmark_code: value.benchmark_closes},
    )


def test_pb03_trusted_preparer_handles_new_user_config_without_manual_plan_list() -> None:
    module = source_module()
    data = source_data()
    first = module.freeze_portfolio_config(data, config())
    settings = config().model_dump(mode="python") | {"initial_cash": Decimal("250000")}
    changed = type(config()).model_validate(settings)
    second = module.freeze_portfolio_config(data, changed)
    assert first.config.config_hash != second.config.config_hash
    assert first.request.request_id != second.request.request_id
    assert first.input_hash != second.input_hash
    assert first.sources == second.sources
    assert first.request.days == second.request.days
    assert first.request.input_generation_id == second.request.input_generation_id
    assert second.request.initial_cash == Decimal("250000")
    with pytest.raises(ValueError, match="source"):
        module.freeze_portfolio_config(data, changed.model_copy(update={"source_version": 2}))


def test_pb06_benchmark_boolean_and_unproved_top_n_are_rejected() -> None:
    value = frozen().model_dump(mode="python")
    value["input_hash"] = None
    value["benchmark_closes"] = ((date(2026, 8, 7), True), *value["benchmark_closes"][1:])
    with pytest.raises(ValidationError, match="benchmark"):
        type(frozen()).model_validate(value)
    raw = source_data().model_dump(mode="python")
    day = raw["template"]["days"][0]
    candidate = day["ranking"]["candidates"][0]
    day["ranking"]["candidates"] = (candidate, dict(candidate, ts_code="000001.SZ"))
    raw["material_hash"] = None
    data = source_module().PortfolioSourceData.model_validate(raw)
    with pytest.raises(ValueError, match="ranking|top-N"):
        source_module().freeze_portfolio_config(data, config())


def test_pb06_trusted_material_does_not_coerce_boolean_benchmark() -> None:
    raw = source_data().model_dump(mode="python")
    raw["benchmarks"]["000300.SH"] = ((date(2026, 8, 7), True), *raw["benchmarks"]["000300.SH"][1:])
    raw["material_hash"] = None
    with pytest.raises(ValidationError, match="benchmark"):
        source_module().PortfolioSourceData.model_validate(raw)


def test_pb03_exact_daily_gate_rejects_intraday_audit_and_drift(tmp_path: Path) -> None:
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_gate import ResearchGateRequest, evaluate_store_research_gate
    from rquant.storage.duckdb import DuckDBStore

    module = source_module()
    value = module.freeze_portfolio_config(source_data(), config())
    now = datetime(2026, 10, 5, tzinfo=UTC)
    with DuckDBStore(tmp_path / "metadata.duckdb") as store:
        prepared = module.publish_portfolio_input(
            value,
            metadata_store=store,
            source_path=tmp_path / "input.duckdb",
            catalog=ResearchCatalog(tmp_path / "lake-catalog.duckdb"),
            lake_root=tmp_path / "lake",
            now=now,
        )
        request = ResearchGateRequest(
            mode="formal",
            strategy_name="portfolio_backtest",
            start_date=value.config.start_date,
            end_date=value.config.end_date,
            code_commit=value.request.producer_commit,
            audit_run_id=prepared.identity.audit_run_id,
            dataset_snapshot_id=prepared.identity.snapshot_id,
            dataset_binding_hash=prepared.identity.binding_hash,
        )
        old = evaluate_store_research_gate(store, request, binding_verified=True)
        assert not old.allowed
        assert any(f.code == "snapshot_eligibility_resolution_missing" for f in old.failures)
        good = module.evaluate_portfolio_gate(store, request, binding_verified=True)
        assert good.allowed and good.research_status == "comparable"
        with pytest.raises(PermissionError):
            module.require_portfolio_gate(
                store, request.model_copy(update={"code_commit": "0" * 40}), binding_verified=True
            )
        with pytest.raises(PermissionError):
            module.require_portfolio_gate(
                store,
                request.model_copy(update={"dataset_binding_hash": "0" * 64}),
                binding_verified=True,
            )


def test_pb03_replay_definition_has_distinct_trusted_identity_and_no_live_entry() -> None:
    name = "rquant.portfolio_backtest_definition"
    assert importlib.util.find_spec(name), "trusted portfolio definition is missing"
    definition = importlib.import_module(name).build_portfolio_definition("1" * 40)
    assert definition.strategy_id == "portfolio_backtest"
    assert definition.spec.parameters["replay_only"] is True
    with pytest.raises(PermissionError, match="replay"):
        definition.entry_evaluator(None, None)


def test_pb03_closed_child_json_restores_path_tuple_and_retains_strict_scalars(
    tmp_path: Path,
) -> None:
    import rquant.lab_worker_registry as registry

    assert hasattr(registry, "_read_builtin_runtime_configuration"), (
        "closed JSON runtime decoder is missing"
    )
    config = registry.builtin_lab_shard_configuration(
        catalog_path=tmp_path / "metadata.duckdb",
        forbidden_paths=(tmp_path / "private.duckdb",),
        snapshot_root=tmp_path / "copies",
        research_lake_root=tmp_path / "lake",
    )
    decoded = registry._read_builtin_runtime_configuration(config.model_dump(mode="json"))
    assert decoded == config
    assert type(decoded.catalog_path) is type(config.catalog_path)
    assert isinstance(decoded.forbidden_paths, tuple)
    assert registry._read_builtin_runtime_configuration(config) == config
    for changed in (
        {"configured": "true"},
        {"configured": 1},
        {"catalog_path": 1},
        {"forbidden_paths": [1]},
        {"forbidden_paths": "/private"},
        {"schema_version": 2},
        {"extra": "ignored"},
    ):
        with pytest.raises((ValidationError, TypeError)):
            registry._read_builtin_runtime_configuration(config.model_dump(mode="json") | changed)
    assert registry.BuiltinLabShardRuntimeConfig.model_config["strict"] is True
    assert registry.BuiltinLabShardRuntimeConfig.model_config["extra"] == "forbid"


def test_pb03_actual_builtin_worker_uses_registered_v3_prepared_identity(tmp_path: Path) -> None:
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.experiment_registry import DateRange, ExperimentRegistry
    from rquant.lab_worker_registry import (
        builtin_lab_shard_configuration,
        execute_builtin_lab_shard,
    )
    from rquant.research_catalog import ResearchCatalog
    from rquant.runtime_definition_bootstrap import (
        bootstrap_builtin_definitions,
        plan_builtin_definitions,
    )
    from rquant.storage.duckdb import DuckDBStore
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from tests.unit.test_backtest_platform import portfolio_claim

    module = source_module()
    value = module.freeze_portfolio_config(source_data(), config())
    now = datetime(2026, 10, 5, tzinfo=UTC)
    code = value.request.producer_commit
    builtins = BuiltinStrategyEvaluatorRegistry(producer_commit=code)
    assert builtins.load_definition("portfolio_backtest", 1).strategy_id == "portfolio_backtest"
    definitions_root = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=code)
    bootstrap_builtin_definitions(
        definitions_root,
        producer_commit=code,
        registered_at=now,
        available_at=now,
        expected_plan_id=plan.plan_id,
    )
    importlib.import_module("rquant.portfolio_backtest_definition").bootstrap_portfolio_definition(
        definitions_root, producer_commit=code, now=now
    )
    definitions = ImmutableDefinitionRegistry(
        definitions_root, execution_registry=builtins.trusted_executable_registry()
    )
    experiments_root = tmp_path / "experiments"
    experiments_root.mkdir(mode=0o700)
    experiments = ExperimentRegistry(
        experiments_root / "experiments.sqlite3", managed_trust_root=experiments_root
    )
    metadata_path = tmp_path / "metadata.duckdb"
    with DuckDBStore(metadata_path) as metadata:
        published = module.publish_portfolio_input(
            value,
            metadata_store=metadata,
            source_path=tmp_path / "input.duckdb",
            catalog=ResearchCatalog(tmp_path / "lake-catalog.duckdb"),
            lake_root=tmp_path / "lake",
            now=now,
        )
    protocol = module.PortfolioExperimentProtocol(
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 6, 30)),
        validation_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 12, 31)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 6, 30)),
    )
    prepared = module.register_portfolio_plan(
        value,
        published,
        definitions=definitions,
        experiments=experiments,
        protocol=protocol,
        now=now,
        deadline=datetime(2026, 12, 1, tzinfo=UTC),
    )
    submission = prepared.submission(job_id=__import__("uuid").uuid4())
    assert submission.spec.schema_version == 3
    registry = default_strategy_job_adapter_registry()
    validated = registry.validate_claim(portfolio_claim(registry, submission.spec))
    runtime = builtin_lab_shard_configuration(
        catalog_path=metadata_path,
        forbidden_paths=(),
        snapshot_root=tmp_path / "catalog-copies",
        research_lake_root=tmp_path / "lake",
    )
    result = execute_builtin_lab_shard(runtime, validated, runtime_code_sha=code)
    bundle = importlib.import_module(
        "rquant.portfolio_backtest_models"
    ).PortfolioBundle.model_validate_json(result.tables[0].frame.iloc[0, 0])
    assert bundle.frozen == value
    assert bundle.result.status == "complete"
    assert not list((tmp_path / "catalog-copies").iterdir())
    assert not list((tmp_path / "lake" / ".execution_sessions").iterdir())
    with pytest.raises(PermissionError):
        execute_builtin_lab_shard(runtime, validated, runtime_code_sha="0" * 40)
