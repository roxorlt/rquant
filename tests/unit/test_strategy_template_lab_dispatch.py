"""Closed template catalog augments the original Lab admission and runtime."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from rquant.experiment_registry import FormalExperimentPlan
from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.lab_job_center import ResearchJobSubmissionError, build_research_job_submission
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
from rquant.lab_worker import (
    LabShardRuntimeManifest,
    LabWorker,
    build_builtin_shard_runtime_manifest,
)
from rquant.lab_worker_registry import builtin_lab_shard_configuration
from rquant.research_gate import ResearchGateDecision
from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
from rquant.strategy_template_adapter import StrategyTemplateRunInput, StrategyTemplateRunParameters
from tests.unit.test_strategy_template_adapter import adapter_fixture, template_spec


def builder_values(value, spec):
    experiment = spec.experiment
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=experiment.spec,
        hypothesis_variant=experiment.hypothesis_variant,
        strategy_definition_fingerprint=value.definition.fingerprint,
        definition_registration_record_hash=value.definition.record_hash,
        preregistered_at=value.definition.available_at,
    )
    return dict(
        gate_decision=ResearchGateDecision(
            allowed=True,
            research_status="exploratory",
            audit_run_id=spec.dataset_snapshot.audit_run_id,
            dataset_snapshot_id=spec.dataset_snapshot.snapshot_id,
            dataset_binding_hash=spec.dataset_snapshot.binding_hash,
            coverage_ratios={},
            coverage_counts={},
            failures=(),
        ),
        code_sha=spec.code_sha,
        dataset_snapshot=spec.dataset_snapshot,
        feature_contract=spec.feature_contract,
        execution_costs=spec.execution_costs,
        random_seed=spec.random_seed,
        resource_class=spec.resource_class,
        deadline=spec.deadline,
        job_id=uuid4(),
        trusted_strategy_registration=value.definition,
        formal_experiment_plan=plan,
    )


def test_template_submission_uses_exact_saved_catalog_original_v3_and_command(tmp_path) -> None:
    _, value, catalog, _ = adapter_fixture(tmp_path)
    spec = template_spec(value)
    run = StrategyTemplateRunInput(
        start_date=spec.parameters.start_date,
        end_date=spec.parameters.end_date,
        parameters=StrategyTemplateRunParameters.model_validate(
            {item.name: item.value for item in spec.parameters.arguments}
        ),
    )
    result = build_research_job_submission(
        run, template_catalog=catalog, **builder_values(value, spec)
    )
    assert (
        result.spec.schema_version == 3
        and result.spec.strategy_execution.strategy_id == value.definition.logical_id
    )
    assert (
        result.command.spec == result.spec
        and result.spec.strategy_execution.definition_registration_record_hash
        == value.definition.record_hash
    )
    with pytest.raises(ResearchJobSubmissionError, match="adapter_plan"):
        build_research_job_submission(run, **builder_values(value, spec))


def test_builtin_runtime_descriptor_binds_frozen_template_catalog_and_manifest(tmp_path) -> None:
    _, _, catalog, registry = adapter_fixture(tmp_path)
    kwargs = dict(
        catalog_path=tmp_path / "catalog.duckdb",
        forbidden_paths=(),
        snapshot_root=tmp_path / "snapshots",
        research_lake_root=tmp_path / "lake",
    )
    config = builtin_lab_shard_configuration(**kwargs, template_catalog=catalog)
    assert config.template_catalog == catalog
    assert config.adapter_manifest_hash == registry.closed_descriptor().manifest_hash
    manifest = build_builtin_shard_runtime_manifest(**kwargs, template_catalog=catalog)
    assert catalog.catalog_hash in manifest.registry.configuration_json


def worker_manifest(tmp_path, catalog, registry, *, adapter_manifest_hash=None):
    original = build_builtin_shard_runtime_manifest(
        catalog_path=tmp_path / "catalog.duckdb",
        forbidden_paths=(),
        snapshot_root=tmp_path / "snapshots",
        research_lake_root=tmp_path / "lake",
    )
    config = json.loads(original.registry.configuration_json)
    config.update(
        template_catalog=catalog.model_dump(mode="json"),
        adapter_manifest_hash=adapter_manifest_hash or registry.closed_descriptor().manifest_hash,
    )
    return LabShardRuntimeManifest(
        registry=original.registry.model_copy(
            update={"configuration_json": json.dumps(config, sort_keys=True, separators=(",", ":"))}
        )
    )


def worker(tmp_path, manifest, **kwargs):
    code = json.loads(manifest.registry.configuration_json)["template_catalog"]["versions"][0][
        "definition"
    ]["producer_commit"]
    return LabWorker(
        worker_id="template-private-test",
        claim_spool=LabClaimSpool(tmp_path / "claims"),
        report_spool=LabReportSpool(tmp_path / "reports"),
        artifact_root=tmp_path / "artifacts",
        shard_runtime_manifest=manifest,
        verified_code_sha_provider=lambda: code,
        **kwargs,
    )


def test_original_worker_builds_its_exact_registry_from_the_frozen_builtin_manifest(
    tmp_path,
) -> None:
    _, value, catalog, registry = adapter_fixture(tmp_path)
    actual = worker(tmp_path, worker_manifest(tmp_path, catalog, registry))
    assert (
        actual.adapter_registry.for_spec(template_spec(value)).strategy_name
        == value.definition.logical_id
    )


def test_original_worker_rejects_catalog_manifest_and_supplied_registry_mismatch(tmp_path) -> None:
    _, _, catalog, registry = adapter_fixture(tmp_path)
    with pytest.raises(LabDaemonConfigurationError, match="registry hash"):
        worker(
            tmp_path, worker_manifest(tmp_path, catalog, registry, adapter_manifest_hash="f" * 64)
        )
    with pytest.raises(LabDaemonConfigurationError, match="adapter registry"):
        worker(
            tmp_path,
            worker_manifest(tmp_path, catalog, registry),
            adapter_registry=default_strategy_job_adapter_registry(),
        )
