"""Exact saved identities execute through the original adapter protocol."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import duckdb
import pytest

from rquant.experiment_registry import DateRange, ExperimentSpec, FormalExperimentPlan
from rquant.lab_job_center import _research_parameter
from rquant.lab_shard_protocol import LabShardClaim
from rquant.research_run_spec import DatasetSnapshotIdentity, ResearchExperimentIdentity, ResearchJobType, ResearchRunParameters, ResearchRunSpec, ResourceClass, StrategyExecutionIdentity
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import ArchiveStrategyTemplate, StrategyTemplateHead
from rquant.strategy_template import compile_strategy_template
from rquant.strategy_template_adapter import StrategyTemplateRunParameters, build_strategy_template_adapter_catalog, strategy_template_adapter_registry, template_adapter_id, write_strategy_template_input
from rquant.strategy_template_definition import strategy_template_feature_contract
from rquant.strategy_job_adapters import build_adapter_execution_contract, default_strategy_job_adapter_registry
from tests.unit.test_strategy_authoring import NOW, catalog as source_catalog, draft
from tests.unit.test_strategy_template_run import frozen


def adapter_fixture(tmp_path):
    value = frozen(tmp_path, exit_rules={"stop_loss": "0.1"})
    target = StrategyAuthoringStore(tmp_path / "metadata.sqlite", definition_root=tmp_path / "definitions", producer_commit=value.request.producer_commit)
    catalog = build_strategy_template_adapter_catalog(target, expected_identity=target.identity())
    registry = strategy_template_adapter_registry(catalog)
    return target, value, catalog, registry


def template_spec(value, *, request_id: str | None = None) -> ResearchRunSpec:
    parameters = StrategyTemplateRunParameters.from_input(value, request_id=request_id or str(uuid4()))
    run_parameters = ResearchRunParameters(strategy_name=value.definition.logical_id, start_date=value.request.days[0].trade_date, end_date=value.request.days[-1].trade_date, arguments=tuple(_research_parameter(name, getattr(parameters, name)) for name in type(parameters).model_fields))
    definition = value.definition
    execution = StrategyExecutionIdentity(strategy_id=definition.logical_id, strategy_version=definition.version, adapter_id=template_adapter_id(definition.logical_id), adapter_version="1", strategy_spec_fingerprint=definition.spec.spec_fingerprint, strategy_definition_fingerprint=definition.fingerprint, strategy_executable_fingerprint=definition.executable_fingerprint, candidate_schema_fingerprint=definition.candidate_schema_fingerprint, definition_registration_record_hash=definition.record_hash, definition_registered_at=definition.registered_at, definition_available_at=definition.available_at, producer_code_commit=definition.producer_commit)
    feature = build_adapter_execution_contract(execution.adapter_id, "1", definition.producer_commit)
    experiment = ExperimentSpec(strategy_spec_fingerprint=execution.strategy_spec_fingerprint, strategy_executable_fingerprint=execution.strategy_executable_fingerprint, candidate_schema_fingerprint=execution.candidate_schema_fingerprint, dataset_snapshot_id="a" * 64, code_commit=definition.producer_commit, parameter_fingerprint=canonical_sha256(run_parameters), hypothesis_family="template-private-synthetic", metric_definition_fingerprint="b" * 64, train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31)), validation_range=DateRange(start_date=date(2025, 2, 1), end_date=date(2025, 2, 28)), frozen_outer_test_range=DateRange(start_date=date(2025, 3, 1), end_date=date(2025, 3, 31)), cost_model_fingerprint=canonical_sha256(value.request.execution_cost_spec), execution_model_fingerprint=canonical_sha256({"contract": "lab-adapter-execution/v1", "adapter_id": execution.adapter_id, "adapter_version": "1", "feature_contract": feature}), seed=1)
    plan = FormalExperimentPlan(schema_version=2, spec=experiment, hypothesis_variant="synthetic", strategy_definition_fingerprint=definition.fingerprint, definition_registration_record_hash=definition.record_hash, preregistered_at=definition.available_at)
    return ResearchRunSpec(schema_version=3, job_type=ResearchJobType.STRATEGY_REPLAY, parameters=run_parameters, code_sha=definition.producer_commit, dataset_snapshot=DatasetSnapshotIdentity(snapshot_id="a" * 64, binding_hash="c" * 64, audit_run_id="d" * 64), feature_contract=feature, execution_costs=value.request.execution_cost_spec, random_seed=1, resource_class=ResourceClass.STANDARD, deadline=definition.available_at + timedelta(days=1), research_status="exploratory", strategy_execution=execution, experiment=ResearchExperimentIdentity(schema_version=2, spec=experiment, experiment_id=experiment.experiment_id, hypothesis_family=experiment.hypothesis_family, hypothesis_variant=plan.hypothesis_variant, formal_plan_id=plan.plan_id))


def claimed(registry, spec):
    definition = registry.plan(spec)[0]
    claim = LabShardClaim(job_id=uuid4(), spec_hash=spec.spec_hash, definition=definition, worker_id="template-private-test", claim_token=uuid4(), claim_generation=1, scheduler_fencing_token=1, claimed_at=NOW, lease_expires_at=NOW + timedelta(seconds=120))
    return registry.validate_claim(claim)


def test_only_committed_metadata_produces_exact_adapters_and_preserves_original_registry(tmp_path) -> None:
    target, value, catalog, registry = adapter_fixture(tmp_path)
    orphan_id = "template_" + "9" * 32
    orphan = target.definition_registry(orphan_id)
    contract = strategy_template_feature_contract(producer_commit=target.producer_commit)
    available_at = value.definition.available_at
    feature = orphan.register_feature_contract(contract, registered_at=available_at, available_at=available_at, producer_commit=target.producer_commit, expected_fingerprint=contract.contract_fingerprint)
    spec = compile_strategy_template(value.rules, strategy_id=orphan_id, version=1, producer_commit=target.producer_commit)
    orphan.register_strategy_spec(spec, feature_contract_fingerprint=feature.fingerprint, registered_at=available_at, available_at=available_at, producer_commit=target.producer_commit, expected_fingerprint=spec.spec_fingerprint)
    rebuilt = build_strategy_template_adapter_catalog(target, expected_identity=target.identity())
    assert rebuilt == catalog and tuple(item.strategy_id for item in catalog.versions) == (value.definition.logical_id,)
    for original in default_strategy_job_adapter_registry().closed_descriptor().adapters:
        assert registry.get(original.adapter_id, original.adapter_version) is default_strategy_job_adapter_registry().get(original.adapter_id, original.adapter_version)
    with pytest.raises(ValueError, match="unknown adapter"):
        registry.get(template_adapter_id(orphan_id), "1")
    assert registry.for_spec(template_spec(value)).strategy_name == value.definition.logical_id


def test_exact_adapter_consumes_full_input_original_broker_and_perf(tmp_path) -> None:
    _, value, _, registry = adapter_fixture(tmp_path)
    spec = template_spec(value)
    with duckdb.connect(":memory:") as connection:
        write_strategy_template_input(connection, value)
        result = registry.execute_shard(claimed(registry, spec), SimpleNamespace(_conn=connection))
    frames = {item.name: item.frame for item in result.tables}
    assert Decimal(frames["equity"].iloc[1]["cash"]) == Decimal("2789.20")
    assert frames["orders"]["side"].tolist() == ["BUY", "SELL", "BUY"]
    assert frames["summary"].iloc[0]["observations"] == 3
    assert frames["template_reference"].iloc[0]["input_hash"] == value.input_hash
    aggregate = registry.aggregate_results(spec, (result,))
    assert aggregate.adapter_id == template_adapter_id(value.definition.logical_id)


@pytest.mark.parametrize("field", ["owner_id", "record_hash", "rules_hash", "input_hash"])
def test_parameter_swap_rejected_before_original_broker(tmp_path, monkeypatch, field) -> None:
    import rquant.strategy_template_adapter as adapter_module

    _, value, _, registry = adapter_fixture(tmp_path)
    spec = template_spec(value)
    values = {item.name: item.value for item in spec.parameters.arguments}
    values[field] = "bob" if field == "owner_id" else "e" * 64
    altered = spec.parameters.model_copy(update={"arguments": tuple(_research_parameter(key, item) for key, item in values.items())})
    # Rebind the experiment parameter hash so failure comes from the trusted definition/input.
    experiment = spec.experiment.spec.model_copy(update={"parameter_fingerprint": canonical_sha256(altered), "experiment_id": None})
    experiment = ExperimentSpec.model_validate(experiment.model_dump(mode="python"))
    identity = spec.experiment.model_copy(update={"spec": experiment, "experiment_id": experiment.experiment_id, "attempt_identity": None})
    tampered = ResearchRunSpec.model_validate({**spec.model_dump(mode="python"), "parameters": altered, "experiment": identity})
    def no_broker(*args, **kwargs):
        raise AssertionError("tampered input reached the broker")
    monkeypatch.setattr(adapter_module, "execute_strategy_template_input", no_broker)
    with duckdb.connect(":memory:") as connection:
        write_strategy_template_input(connection, value)
        with pytest.raises(ValueError, match="binding|input|definition"):
            registry.execute_shard(claimed(registry, tampered), SimpleNamespace(_conn=connection))


def test_catalog_rejects_replaced_private_metadata_instance(tmp_path) -> None:
    target, _, _, _ = adapter_fixture(tmp_path)
    original = target.identity()
    target.path.rename(target.path.with_suffix(".old"))
    target.initialize()
    with pytest.raises(RuntimeError, match="identity"):
        build_strategy_template_adapter_catalog(target, expected_identity=original)


def test_frozen_original_run_survives_new_version_archive_and_exact_retry(tmp_path) -> None:
    target, value, catalog, registry = adapter_fixture(tmp_path)
    spec = template_spec(value)
    parameters = StrategyTemplateRunParameters.model_validate({item.name: item.value for item in spec.parameters.arguments})
    head = catalog.versions[0].head
    target.admit_run(value.definition.logical_id, head, owner_id="alice", command_id=parameters.request_id, request_hash=spec.spec_hash)
    next_version = target.save(draft(strategy_id=value.definition.logical_id, expected_head=head).model_copy(update={"rules": value.rules}), owner_id="alice", catalog=source_catalog())
    target.archive(ArchiveStrategyTemplate(command_id=str(uuid4()), requested_at=NOW, generation_id="generation-a", strategy_id=value.definition.logical_id, expected_head=next_version.head), owner_id="alice")
    assert target.admit_run(value.definition.logical_id, head, owner_id="alice", command_id=parameters.request_id, request_hash=spec.spec_hash).head == head
    current = strategy_template_adapter_registry(build_strategy_template_adapter_catalog(target, expected_identity=target.identity()))
    assert current.plan(spec) == registry.plan(spec)
    with duckdb.connect(":memory:") as connection:
        write_strategy_template_input(connection, value)
        result = current.execute_shard(claimed(current, spec), SimpleNamespace(_conn=connection))
    assert result.spec_hash == spec.spec_hash and result.tables[0].frame.iloc[0]["version"] == 1


def test_bundle_cannot_attach_another_result_to_the_exact_template(tmp_path) -> None:
    from rquant.strategy_template_adapter import template_result_tables
    from rquant.strategy_template_run import StrategyTemplateResult, execute_strategy_template_input

    _, value, _, _ = adapter_fixture(tmp_path)
    original = execute_strategy_template_input(value, research_root=tmp_path)
    other = StrategyTemplateResult.model_validate({**original.model_dump(mode="python"), "owner_id": "bob", "content_hash": None})
    with pytest.raises(ValueError, match="result binding"):
        template_result_tables(value, StrategyTemplateRunParameters.from_input(value, request_id=str(uuid4())), other)
