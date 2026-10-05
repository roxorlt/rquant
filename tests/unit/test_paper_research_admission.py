"""Original Lab admission must bind two fixed paper analyses to exact owned facts."""

from datetime import timedelta
from pathlib import Path

import pytest

from rquant.research_run_spec import DatasetSnapshotIdentity, ResearchJobType, ResearchParameter, ResearchRunParameters, ResearchRunSpec, ResourceClass
from rquant.strategy_job_adapters import build_adapter_execution_contract
from tests.unit.test_paper_portfolio_ledger_views import filled
from tests.unit.test_paper_signal_worker import EXECUTION_TIME


def catalog(tmp_path: Path):
    from rquant.paper_research import PaperResearchAdapterCatalog

    _, _, _, runtime = filled(tmp_path)
    return PaperResearchAdapterCatalog(metadata_identity=runtime.state.identity(), configuration=runtime.state.configuration,
                                       source_code_identity="e"*64)


def spec_for(value, *, name="paper_reconcile", work_units=1):
    configuration = value.configuration
    arguments = dict(owner_id="alice", account_id=configuration.binding.account_id,
                     configuration_fingerprint=configuration.fingerprint, configuration_version=configuration.version,
                     strategy_id=configuration.binding.strategy_id, strategy_version=configuration.binding.strategy_version,
                     parameter_fingerprint=configuration.binding.parameter_fingerprint, cost_spec_id=configuration.binding.cost_spec_id,
                     source_code_identity=value.source_code_identity, input_hash="f"*64, request_id="11111111-1111-4111-8111-111111111111", work_units=work_units)
    return ResearchRunSpec(schema_version=2, job_type=ResearchJobType.STRATEGY_REPLAY,
                           parameters=ResearchRunParameters(strategy_name=name, start_date=EXECUTION_TIME.date(), end_date=EXECUTION_TIME.date(),
                                                            arguments=tuple(ResearchParameter(name=key, kind="integer" if type(val) is int else "text", value=val) for key, val in arguments.items())),
                           code_sha="a"*40, dataset_snapshot=DatasetSnapshotIdentity(snapshot_id="a"*64, binding_hash="b"*64, audit_run_id="c"*64),
                           feature_contract=build_adapter_execution_contract("paper-reconcile" if name=="paper_reconcile" else "paper-backtest-band", "1", "a"*40),
                           execution_costs=configuration.execution_cost_spec, random_seed=20261005, resource_class=ResourceClass.STANDARD,
                           deadline=EXECUTION_TIME+timedelta(hours=2), research_status="exploratory")


def test_original_child_config_binds_exact_paper_registry_and_keeps_default_none(tmp_path: Path) -> None:
    from rquant.lab_worker_registry import builtin_lab_shard_configuration, resolve_builtin_adapter_registry
    from rquant.lab_daemon import LabDaemonConfigurationError

    value = catalog(tmp_path)
    arguments = dict(catalog_path=tmp_path/"metadata.duckdb", forbidden_paths=(), snapshot_root=tmp_path/"snapshots", research_lake_root=tmp_path/"lake")
    config = builtin_lab_shard_configuration(**arguments, paper_catalog=value)
    registry = resolve_builtin_adapter_registry(config)
    assert registry.for_spec(spec_for(value)).adapter_id == "paper-reconcile"
    assert registry.for_spec(spec_for(value, name="paper_backtest_band", work_units=2520)).adapter_id == "paper-backtest-band"
    old = resolve_builtin_adapter_registry(builtin_lab_shard_configuration(**arguments))
    assert old.closed_descriptor().adapters == tuple(item for item in registry.closed_descriptor().adapters if item.adapter_id not in ("paper-reconcile", "paper-backtest-band"))
    broken = config.model_copy(update={"adapter_manifest_hash": "0"*64})
    with pytest.raises(LabDaemonConfigurationError, match="registry hash"):
        resolve_builtin_adapter_registry(broken)


def test_original_preflight_uses_paper_catalog_and_rejects_wrong_owner_or_version(tmp_path: Path) -> None:
    from rquant.lab_job_center import _preflight_research_plan

    value = catalog(tmp_path)
    exact = spec_for(value)
    _preflight_research_plan(exact, paper_catalog=value)
    for key, changed in (("owner_id", "bob"), ("configuration_version", 2), ("strategy_id", "unknown")):
        body = exact.model_dump(mode="python")
        body["parameters"]["arguments"] = tuple({"name": item.name, "kind": item.kind, "value": changed if item.name == key else item.value} for item in exact.parameters.arguments)
        with pytest.raises(ValueError):
            _preflight_research_plan(ResearchRunSpec.model_validate(body), paper_catalog=value)


def test_original_immutable_contract_allows_only_two_exact_paper_inputs() -> None:
    from rquant.strategy_dependencies import StrategyExecutionDependencies, StrategyTableDependency, strategy_execution_dependencies

    for name in ("paper_reconcile", "paper_backtest_band"):
        dependency = strategy_execution_dependencies(name)
        assert dependency.lake_datasets == ()
        assert dependency.materialized_tables == (StrategyTableDependency(dataset_id="paper_research_input", table_name="paper_research_input"),)
    with pytest.raises(ValueError):
        StrategyExecutionDependencies(strategy_id="paper_arbitrary", contract_version="paper-research-input/v1", lake_datasets=(),
                                      materialized_tables=(StrategyTableDependency(dataset_id="paper_research_input", table_name="paper_research_input"),))


def test_original_runtime_directory_preserves_accepted_configuration_after_head_change(tmp_path: Path) -> None:
    from rquant.paper_research_runtime import PaperResearchRuntimeDirectory
    from rquant.paper_research_source import paper_research_code_identity
    from rquant.paper_research import PaperResearchAdapterCatalog
    from rquant.lab_worker import build_builtin_shard_runtime_manifest
    from rquant.lab_worker_registry import _read_builtin_runtime_configuration
    from rquant.strict_json import strict_canonical_json_loads

    _, _, _, runtime = filled(tmp_path)
    state = runtime.state
    original = state.configuration
    exact = PaperResearchAdapterCatalog(metadata_identity=state.identity(), configuration=original,
                                        source_code_identity=paper_research_code_identity())
    directory = PaperResearchRuntimeDirectory(states=(state,), expected_identities=(state.identity(),))
    accepted = spec_for(exact)
    manifest = build_builtin_shard_runtime_manifest(catalog_path=tmp_path/"catalog.duckdb", forbidden_paths=(),
                                                    snapshot_root=tmp_path/"snapshots", research_lake_root=tmp_path/"lake")
    state.start_configuration(original.model_copy(update={"version": 2, "configured_at": EXECUTION_TIME+timedelta(minutes=1)}))
    assert directory.catalog_for_spec(accepted) == exact
    frozen = directory.manifest_for_spec(accepted, manifest)
    parsed = _read_builtin_runtime_configuration(strict_canonical_json_loads(frozen.registry.configuration_json))
    assert parsed.paper_catalog == exact
    bad = accepted.model_dump(mode="python")
    bad["parameters"]["arguments"] = tuple({"name": item.name, "kind": item.kind, "value": "unknown" if item.name=="account_id" else item.value}
                                             for item in accepted.parameters.arguments)
    with pytest.raises(ValueError):
        directory.catalog_for_spec(ResearchRunSpec.model_validate(bad))
    replacement = state.path.with_suffix(".replacement")
    state.path.rename(replacement)
    state.path.write_bytes(replacement.read_bytes())
    state.path.chmod(0o600)
    with pytest.raises(ValueError, match="replace"):
        directory.catalog_for_spec(accepted)
