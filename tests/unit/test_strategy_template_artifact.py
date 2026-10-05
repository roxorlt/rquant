"""Original v3 submission, scheduler, worker seal and finalizer authority."""

from __future__ import annotations

from datetime import timedelta
from time import monotonic
from types import SimpleNamespace
from uuid import UUID, uuid4

import duckdb
import pytest

from rquant.experiment_registry import (
    ExperimentRegistry,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
)
from rquant.lab_artifact_preview import ArtifactPreviewIntegrityError, ArtifactPreviewReader
from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_job_center import ExperimentLifecycleCoordinator, LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool, SubmitJobCommand
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, LabResultState
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportSpool,
    LabShardSucceeded,
    LabShardTelemetry,
    LabWorkerReport,
)
from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest
from rquant.strategy_authoring_projection import build_strategy_authoring_snapshot
from rquant.strategy_template_adapter import (
    StrategyTemplateRunParameters,
    write_strategy_template_input,
)
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
from tests.unit.test_strategy_authoring import catalog as source_catalog
from tests.unit.test_strategy_template_adapter import adapter_fixture, template_spec


def sealed_template(tmp_path, *, seal: bool = True):
    target, value, catalog, registry = adapter_fixture(tmp_path)
    spec = template_spec(value)
    parameters = StrategyTemplateRunParameters.model_validate(
        {item.name: item.value for item in spec.parameters.arguments}
    )
    job_id = UUID(parameters.request_id)
    target.admit_run(
        value.definition.logical_id,
        catalog.versions[0].head,
        owner_id="alice",
        command_id=parameters.request_id,
        request_hash=spec.spec_hash,
    )
    now = value.definition.available_at + timedelta(seconds=1)
    clock = lambda: now
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=spec.experiment.spec,
        hypothesis_variant=spec.experiment.hypothesis_variant,
        strategy_definition_fingerprint=value.definition.fingerprint,
        definition_registration_record_hash=value.definition.record_hash,
        preregistered_at=value.definition.available_at,
    )
    research = tmp_path / "research"
    research.mkdir(mode=0o700)
    experiments = ExperimentRegistry(research / "experiments.sqlite", managed_trust_root=research)
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
    jobs = LabJobStore(research / "jobs.sqlite")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    commands, claims, reports = (
        LabCommandSpool(research / "commands"),
        LabClaimSpool(research / "claims"),
        LabReportSpool(research / "reports"),
    )
    commits, artifacts = (
        LabArtifactCommitSpool(research / "commits"),
        LabJobArtifactStore(research / "results"),
    )
    key = LabFinalizerAuthorityKey(
        key_id="template-synthetic-finalizer", secret=b"template-private-reference-key-0000000000"
    )
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=commands,
        experiment_registry=experiments,
        definition_registry=target.definition_registry(value.definition.logical_id),
        clock=clock,
    )
    assert (
        facade.submit_create(
            SubmitJobCommand(job_id=job_id, spec=spec), interaction_key=parameters.request_id
        ).job_id
        == job_id
    )
    scheduler = LabScheduler(
        store=jobs,
        spool=commands,
        owner_id="template-synthetic-scheduler",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("template-synthetic-worker",),
        shard_lease_seconds=120,
        adapter_registry=registry,
        artifact_commit_spool=commits,
        artifact_store=artifacts,
        finalizer_authority_key_provider=lambda requested: key if requested == key.key_id else None,
        lifecycle_synchronizer=ExperimentLifecycleCoordinator(facade),
        runtime_guard=lambda: spec.code_sha,
        clock=clock,
    )
    assert scheduler.run_once().applied == 1
    if not seal:
        return target, value, reader, artifacts.root, job_id, now
    claim = claims.pending()[0].claim
    claims.consume(claims.pending()[0])
    started = monotonic()
    with duckdb.connect(":memory:") as connection:
        write_strategy_template_input(connection, value)
        execution = registry.execute_shard(
            registry.validate_claim(claim), SimpleNamespace(_conn=connection)
        )
    finished = monotonic()
    runtime_manifest = build_builtin_shard_runtime_manifest(
        catalog_path=research / "unused-catalog.duckdb",
        forbidden_paths=(),
        snapshot_root=research / "snapshots",
        research_lake_root=research / "lake",
        template_catalog=catalog,
    )
    worker = LabWorker(
        worker_id=claim.worker_id,
        claim_spool=claims,
        report_spool=reports,
        artifact_root=research / "shards",
        shard_runtime_manifest=runtime_manifest,
        verified_code_sha_provider=lambda: spec.code_sha,
        clock=clock,
    )
    manifest = worker._seal_result(claim, execution, deadline=spec.deadline)
    reports.publish(
        LabWorkerReport.from_claim(
            claim,
            report_id=uuid4(),
            reported_at=now,
            body=LabShardSucceeded.current(
                result_manifest_hash=manifest.manifest_hash,
                worker_code_sha=spec.code_sha,
                telemetry=LabShardTelemetry.from_work_plan(
                    claim.definition.work_plan,
                    monotonic_started=started,
                    monotonic_finished=finished,
                ),
            ),
        )
    )
    assert scheduler.run_once().reports_accepted == 1
    assert reader.get_job(job_id).result_state is LabResultState.READY
    finalizer = LabFinalizer(
        reader=reader,
        shard_artifact_root=research / "shards",
        artifact_store=artifacts,
        commit_spool=commits,
        adapter_registry=registry,
        verified_code_sha_provider=lambda: spec.code_sha,
        finalizer_authority_key_provider=lambda: key,
    )
    assert finalizer.finalize(job_id).status == "published"
    assert scheduler.run_once().artifact_commits_accepted == 1
    job = reader.get_job(job_id)
    assert job.status is JobStatus.SUCCEEDED and job.result_state is LabResultState.SEALED
    return target, value, reader, artifacts.root, job_id, now


def test_recent_run_requires_original_sealed_graph_and_exact_version_owner_input(tmp_path) -> None:
    target, value, jobs, artifact_root, job_id, now = sealed_template(tmp_path)
    sealed = StrategyTemplateSealedResultReader(
        reader=jobs, artifact_reader=ArtifactPreviewReader(reader=jobs, artifact_root=artifact_root)
    )
    snapshot = build_strategy_authoring_snapshot(
        target, available_at=now, source_catalogs=(source_catalog(),), sealed_result_reader=sealed
    )
    run = snapshot.versions[0].latest_run
    assert run.job_id == job_id and run.owner_id == "alice" and run.input_hash == value.input_hash
    assert run.head == snapshot.versions[0].metadata.head and run.complete_result_hash
    assert (
        sealed.recent_runs(
            target, expected_identity=target.identity(), as_of=now - timedelta(microseconds=1)
        )
        == ()
    )


def test_pending_or_unsealed_original_job_has_no_recent_result_and_never_reads_artifacts(
    tmp_path, monkeypatch
) -> None:
    target, _, jobs, artifact_root, _, now = sealed_template(tmp_path, seal=False)
    preview = ArtifactPreviewReader(reader=jobs, artifact_root=artifact_root)

    def forbidden(*args, **kwargs):
        raise AssertionError("unsealed result read files")

    monkeypatch.setattr(preview, "preview", forbidden)
    sealed = StrategyTemplateSealedResultReader(reader=jobs, artifact_reader=preview)
    assert sealed.recent_runs(target, expected_identity=target.identity(), as_of=now) == ()


def test_sealed_result_reference_corruption_is_not_published_as_recent_success(tmp_path) -> None:
    target, _, jobs, artifact_root, _, now = sealed_template(tmp_path)
    preview = ArtifactPreviewReader(reader=jobs, artifact_root=artifact_root)
    sealed = StrategyTemplateSealedResultReader(reader=jobs, artifact_reader=preview)
    table = next(artifact_root.rglob("template_reference.parquet"))
    table.chmod(0o600)
    table.write_bytes(table.read_bytes() + b"changed")
    with pytest.raises(ArtifactPreviewIntegrityError):
        sealed.recent_runs(target, expected_identity=target.identity(), as_of=now)


@pytest.mark.parametrize(
    "field,value",
    [("owner_id", "bob"), ("version", 2), ("rules_hash", "e" * 64), ("input_hash", "e" * 64)],
)
def test_reference_cannot_be_rebound_to_another_plan_even_with_valid_scalar_hashes(
    tmp_path, field, value
) -> None:
    from rquant.strategy_template_artifact import TemplateSealedResultReference

    target, _, jobs, artifact_root, job_id, _ = sealed_template(tmp_path)
    parameters = StrategyTemplateRunParameters.model_validate(
        {item.name: item.value for item in jobs.get_job(job_id).spec.parameters.arguments}
    )
    preview = ArtifactPreviewReader(reader=jobs, artifact_root=artifact_root).preview(
        job_id, table_name="template_reference", row_limit=1, column_limit=12
    )
    data = dict(zip(preview.table.columns, preview.table.rows[0], strict=True))
    changed = TemplateSealedResultReference.model_validate({**data, field: value})
    with pytest.raises(ValueError, match="original plan"):
        changed.bind_parameters(parameters)


def test_sealed_reference_requires_the_original_admission_spec_hash(tmp_path) -> None:
    target, _, jobs, artifact_root, job_id, now = sealed_template(tmp_path)
    with target._connection(write=True) as connection:
        connection.execute(
            "UPDATE run_admissions SET spec_hash=? WHERE command_id=?", ("e" * 64, str(job_id))
        )
    sealed = StrategyTemplateSealedResultReader(
        reader=jobs, artifact_reader=ArtifactPreviewReader(reader=jobs, artifact_root=artifact_root)
    )
    with pytest.raises(ValueError, match="original accepted run"):
        sealed.recent_runs(target, expected_identity=target.identity(), as_of=now)
