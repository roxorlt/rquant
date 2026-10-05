"""A first saved ID must enter the original scheduler, not another queue."""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from rquant.experiment_registry import (
    ExperimentRegistry,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
)
from rquant.lab_job_center import ExperimentLifecycleCoordinator, LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool, SubmitJobCommand
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import ArchiveStrategyTemplate
from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
from rquant.strategy_template_runtime import StrategyTemplateRuntimeDirectory
from tests.unit.test_portfolio_backtest import _CODES, _request
from tests.unit.test_strategy_authoring import NOW, draft
from tests.unit.test_strategy_authoring import catalog as source_catalog
from tests.unit.test_strategy_template_adapter import adapter_fixture, template_spec


def test_first_saved_id_enters_the_already_started_original_scheduler(tmp_path) -> None:
    research = tmp_path / "research"
    research.mkdir(mode=0o700)
    jobs = LabJobStore(research / "jobs.sqlite")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    spool = LabCommandSpool(research / "commands")
    claims = LabClaimSpool(research / "claims")
    reports = LabReportSpool(research / "reports")
    target = StrategyAuthoringStore(
        tmp_path / "metadata.sqlite",
        definition_root=tmp_path / "definitions",
        producer_commit=_request((_CODES[0],)).producer_commit,
    )
    target.initialize()
    directory = StrategyTemplateRuntimeDirectory(target, expected_identity=target.identity())
    experiments = ExperimentRegistry(research / "experiments.sqlite", managed_trust_root=research)
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=spool,
        experiment_registry=experiments,
        template_directory=directory,
        clock=lambda: target._now(),
    )
    # The original host exists before the first logical ID is saved.
    scheduler = LabScheduler(
        store=jobs,
        spool=spool,
        owner_id="template-private-scheduler",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("template-private-worker",),
        adapter_registry=default_strategy_job_adapter_registry(),
        template_directory=directory,
    )
    target, value, _, _ = adapter_fixture(tmp_path, existing_store=target)
    spec = template_spec(value)
    now = value.definition.available_at + timedelta(seconds=1)
    scheduler.clock = lambda: now
    experiment = spec.experiment
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=experiment.spec,
        hypothesis_variant=experiment.hypothesis_variant,
        strategy_definition_fingerprint=value.definition.fingerprint,
        definition_registration_record_hash=value.definition.record_hash,
        preregistered_at=value.definition.available_at,
    )
    experiments.register_formal_plan(
        plan,
        family_manifest=HypothesisFamilyManifest(
            hypothesis_family=plan.spec.hypothesis_family,
            experiment_ids=(plan.spec.experiment_id,),
            search_space_fingerprint="f" * 64,
            metric_definition_fingerprint=plan.spec.metric_definition_fingerprint,
            preregistered_at=plan.preregistered_at,
        ),
    )
    parameters = {item.name: item.value for item in spec.parameters.arguments}
    job_id = UUID(parameters["request_id"])
    facade.clock = lambda: now
    scheduler.lifecycle_synchronizer = ExperimentLifecycleCoordinator(facade)
    facade.submit_create(
        SubmitJobCommand(job_id=job_id, spec=spec), interaction_key=parameters["request_id"]
    )
    tick = scheduler.run_once()
    assert tick.plans_created == 1 and tick.plans_failed == 0, (
        tick.plans_created,
        tick.plans_failed,
        reader.get_job(job_id).status.value,
    )
    assert reader.get_job(job_id).status is JobStatus.RUNNING
    assert claims.pending()[0].claim.definition.adapter_id == spec.strategy_execution.adapter_id


def test_old_claim_manifest_is_identical_after_new_head_and_archive(tmp_path) -> None:
    from tests.unit.test_strategy_template_adapter import claimed

    target, value, _, registry = adapter_fixture(tmp_path)
    directory = StrategyTemplateRuntimeDirectory(target, expected_identity=target.identity())
    spec = template_spec(value)
    claim = claimed(registry, spec).claim
    base = build_builtin_shard_runtime_manifest(
        catalog_path=tmp_path / "catalog.duckdb",
        forbidden_paths=(),
        snapshot_root=tmp_path / "snapshots",
        research_lake_root=tmp_path / "lake",
    )
    before = directory.manifest_for_spec(spec, base)
    worker = LabWorker(
        worker_id=claim.worker_id,
        claim_spool=LabClaimSpool(tmp_path / "claims"),
        report_spool=LabReportSpool(tmp_path / "reports"),
        artifact_root=tmp_path / "shards",
        shard_runtime_manifest=base,
        template_directory=directory,
        verified_code_sha_provider=lambda: spec.code_sha,
    )
    original = worker._validate_closed_claim(claim)
    next_version = target.save(
        draft(
            strategy_id=value.definition.logical_id,
            expected_head=directory.catalog_for_spec(spec).versions[0].head,
        ).model_copy(update={"rules": value.rules}),
        owner_id="alice",
        catalog=source_catalog(),
    )
    target.archive(
        ArchiveStrategyTemplate(
            command_id=str(uuid4()),
            requested_at=NOW,
            generation_id="generation-a",
            strategy_id=value.definition.logical_id,
            expected_head=next_version.head,
        ),
        owner_id="alice",
    )
    assert directory.manifest_for_spec(spec, base) == before
    assert worker._validate_closed_claim(claim) == original
    assert directory.registry_for_spec(spec).plan(spec) == registry.plan(spec)


def test_runtime_directory_rejects_replaced_private_metadata(tmp_path) -> None:
    target, value, _, _ = adapter_fixture(tmp_path)
    directory = StrategyTemplateRuntimeDirectory(target, expected_identity=target.identity())
    target.path.rename(target.path.with_suffix(".old"))
    target.initialize()
    with pytest.raises(RuntimeError, match="identity"):
        directory.registry_for_spec(template_spec(value))


def test_runtime_directory_is_concrete_not_an_arbitrary_callable(tmp_path) -> None:
    with pytest.raises(TypeError, match="concrete"):
        LabScheduler(
            store=LabJobStore(tmp_path / "jobs.sqlite"),
            spool=LabCommandSpool(tmp_path / "commands"),
            owner_id="private-test",
            lease_seconds=60,
            heartbeat_seconds=10,
            poll_interval_ms=5,
            template_directory=lambda: default_strategy_job_adapter_registry(),
        )
