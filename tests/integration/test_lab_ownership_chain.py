from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pandas as pd
import pytest

from rquant.artifact_retention import ArtifactReferenceStore
from rquant.artifact_retention_catalog_authority import (
    bootstrap_retention_catalog_authority,
)
from rquant.dashboard.lab.job_center import (
    RegistryBackedFormalExperimentResolver,
    StrategyLabJobCenterController,
    StrategyLabSubmissionContext,
)
from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import (
    DateRange,
    ExperimentRegistry,
    ExperimentRegistryReadonlyReader,
    ExperimentSpec,
    ExperimentStatus,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
)
from rquant.job_center_authority import (
    publish_install_current_job_center_authority,
)
from rquant.lab_artifact_catalog import LabArtifactCatalogRegistrar
from rquant.lab_artifact_catalog_readers import (
    build_lab_artifact_owner_reader_composition,
)
from rquant.lab_artifact_catalog_runtime import (
    LabArtifactCatalogRuntime,
    LabArtifactDiscoveryQueue,
)
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_artifact_protocol import LabArtifactCommitSpool
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_job_center import (
    NShapeComparisonRunInput,
    build_research_job_submission,
)
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, LabResultState, ResourceClass
from rquant.lab_jobs_serving_authority import (
    LabArtifactTerminalReleaseCoordinator,
    LabJobsServingSourceReader,
    TrustedLabStrategyProjectionReader,
)
from rquant.lab_page_control import build_lab_page_control_writer
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
from rquant.lab_worker import LabShardResultManifest
from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SubmitLabCommand,
)
from rquant.research_gate import ResearchGateDecision
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    ResearchRunParameters,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_job_adapters import (
    LabShardExecutionResult,
    LabShardTable,
    NShapeCompareParameters,
    build_adapter_execution_contract,
    default_strategy_job_adapter_registry,
)
from tests.unit.test_lab_artifact_catalog_readers import _create_dataset_authority
from tests.unit.test_lab_finalizer import (
    _authority_key_provider,
    _authority_verification_key_provider,
)
from tests.unit.test_lab_job_center import _v3_strategy_registration
from tests.unit.test_lab_worker import (
    NOW,
    MetadataStoreFactory,
    RecordingRegistry,
    RecordingResearchStoreOpener,
    _worker,
)

CODE_SHA = "1" * 40


class _ProjectionRecordingRegistry(RecordingRegistry):
    def execute_shard(self, validated, store):  # type: ignore[no-untyped-def]
        del store
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(
                    name="summary",
                    frame=pd.DataFrame(
                        [
                            {
                                "entry_mode": "first_break",
                                "profile_variant": "baseline",
                                "candidates": 1,
                                "trades": 1,
                                "trigger_rate_pct": 100.0,
                                "mean_ret_pct": 2.0,
                                "median_ret_pct": 2.0,
                                "win_rate_pct": 100.0,
                                "best_ret_pct": 2.0,
                                "worst_ret_pct": 2.0,
                                "gap_stop_rate_pct": 0.0,
                            }
                        ]
                    ),
                ),
                LabShardTable(
                    name="trades",
                    frame=pd.DataFrame(
                        [
                            {
                                "entry_mode": "first_break",
                                "profile_variant": "baseline",
                                "signal_date": date(2026, 6, 30),
                                "ts_code": "600000.SH",
                                "name": "PF Bank",
                                "entry_time": datetime(2026, 6, 30, 1, 31, tzinfo=UTC),
                                "entry_price_raw": 10.0,
                                "entry_price": 10.0,
                                "stop_loss_basis": 9.5,
                                "take_profit_basis": 11.0,
                                "volume_profile_lookbacks": "90",
                                "volume_profile_rr": 2.0,
                                "exit_time": datetime(2026, 6, 30, 7, 0, tzinfo=UTC),
                                "exit_price": 10.2,
                                "exit_reason": "close",
                                "ret_pct": 2.0,
                            }
                        ]
                    ),
                ),
            ),
        )


@pytest.mark.parametrize("auto_release", [False, True], ids=["crash-recovery", "auto-compose"])
def test_real_job_completion_is_discovered_with_exact_experiment_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    auto_release: bool,
) -> None:
    registration = _v3_strategy_registration(tmp_path)
    research_root = tmp_path / "research"
    research_root.mkdir(mode=0o700)
    dataset_path = research_root / "research_ro.duckdb"
    snapshot_id, snapshot_binding_hash, audit_run_id = _create_dataset_authority(dataset_path)
    run_input = NShapeComparisonRunInput(
        start_date=date(2026, 4, 1),
        end_date=date(2026, 6, 30),
        parameters=NShapeCompareParameters(hold_days=(1,), entry_modes=("first_break",)),
    )
    gate = ResearchGateDecision(
        allowed=True,
        research_status="comparable",
        audit_run_id=audit_run_id,
        dataset_snapshot_id=snapshot_id,
        dataset_binding_hash=snapshot_binding_hash,
        coverage_ratios={},
        coverage_counts={},
        failures=(),
    )
    snapshot = DatasetSnapshotIdentity(
        snapshot_id=snapshot_id,
        binding_hash=snapshot_binding_hash,
        audit_run_id=audit_run_id,
    )
    feature_contract = build_adapter_execution_contract("nshape-compare", "1", CODE_SHA)
    costs = ExecutionCostSpec(
        commission_bps=Decimal("2.5"),
        stamp_duty_bps=Decimal("5"),
        transfer_fee_bps=Decimal("0.1"),
        slippage_bps=Decimal("3"),
    )
    common = dict(
        gate_decision=gate,
        code_sha=CODE_SHA,
        dataset_snapshot=snapshot,
        feature_contract=feature_contract,
        execution_costs=costs,
        random_seed=7,
        resource_class=ResourceClass.STANDARD,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        job_id=UUID("11111111-1111-4111-8111-111111111111"),
    )
    exploratory = build_research_job_submission(
        run_input,
        **{
            **common,
            "gate_decision": gate.model_copy(update={"research_status": "exploratory"}),
        },
    )
    parameters = ResearchRunParameters(
        strategy_name="n_shape",
        start_date=run_input.start_date,
        end_date=run_input.end_date,
        arguments=exploratory.spec.parameters.arguments,
    )
    experiment = ExperimentSpec(
        strategy_spec_fingerprint=registration.spec.spec_fingerprint,
        strategy_executable_fingerprint=registration.executable_fingerprint,
        candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
        dataset_snapshot_id=snapshot_id,
        code_commit=CODE_SHA,
        parameter_fingerprint=canonical_sha256(parameters),
        hypothesis_family="ownership-chain",
        metric_definition_fingerprint="b" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 6, 30)),
        validation_range=DateRange(
            start_date=date(2025, 7, 1),
            end_date=date(2025, 12, 31),
        ),
        frozen_outer_test_range=DateRange(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 3, 31),
        ),
        cost_model_fingerprint=canonical_sha256(costs),
        execution_model_fingerprint=canonical_sha256(
            {
                "contract": "lab-adapter-execution/v1",
                "adapter_id": "nshape-compare",
                "adapter_version": "1",
                "feature_contract": feature_contract,
            }
        ),
        seed=7,
    )
    assert experiment.experiment_id is not None

    jobs_path = research_root / "lab_jobs.sqlite3"
    store = LabJobStore(jobs_path)
    store.initialize()
    jobs_path.chmod(0o600)
    command_path = research_root / "commands"
    commands = LabCommandSpool(command_path)
    final_artifact_path = research_root / "job-artifacts"
    LabJobArtifactStore(final_artifact_path)
    catalog_root = research_root / "artifact-catalog"
    catalog_root.mkdir(mode=0o700)
    reference_store = ArtifactReferenceStore(
        catalog_root / "references.sqlite3",
        managed_trust_root=catalog_root,
        clock=lambda: datetime(2026, 8, 2, 6, tzinfo=UTC),
    )
    retention_root = research_root / "artifact-retention"
    retention_root.mkdir(mode=0o700)
    retention_references = retention_root / "references.sqlite3"
    ArtifactReferenceStore(retention_references, managed_trust_root=retention_root)
    retention_references.chmod(0o600)
    catalog_authority = bootstrap_retention_catalog_authority(
        state_root=retention_root,
        reference_store_path=retention_references,
        producer_commit=CODE_SHA,
    )
    experiment_path = research_root / "experiments.sqlite3"
    registry = ExperimentRegistry(
        experiment_path,
        managed_trust_root=research_root,
    )
    family = HypothesisFamilyManifest(
        hypothesis_family=experiment.hypothesis_family,
        experiment_ids=(experiment.experiment_id,),
        search_space_fingerprint="d" * 64,
        metric_definition_fingerprint=experiment.metric_definition_fingerprint,
        preregistered_at=NOW - timedelta(days=1),
    )
    formal_plan = FormalExperimentPlan(
        schema_version=2,
        spec=experiment,
        hypothesis_variant="hold-1",
        strategy_definition_fingerprint=registration.fingerprint,
        definition_registration_record_hash=registration.record_hash,
        preregistered_at=NOW - timedelta(days=1),
    )
    registry.register_formal_plan(
        formal_plan,
        family_manifest=family,
    )
    manifest = publish_install_current_job_center_authority(
        code_sha=CODE_SHA,
        deployment_profile_id="2" * 64,
        deployment_generation_hash="3" * 64,
        runtime_deployment_root=tmp_path,
        current_code_sha=lambda: CODE_SHA,
        runtime_root=research_root,
        lab_jobs_path=jobs_path,
        command_spool_path=command_path,
        final_artifact_root=final_artifact_path,
        definition_registry_root=tmp_path / "definitions",
        experiment_registry_path=experiment_path,
        dataset_authority_path=dataset_path,
        catalog_authority_root=catalog_authority.root,
        catalog_authority_receipt_path=catalog_authority.current_receipt_path,
    )
    export_root = research_root / "exports"
    page_outbox = PageControlOutbox(tmp_path / "page-control" / "outbox.sqlite3")
    page_writer = build_lab_page_control_writer(
        manifest,
        clock=lambda: NOW - timedelta(minutes=1),
    )
    page_service = PageControlService(
        outbox=page_outbox,
        consumer=PageControlConsumer(
            outbox=page_outbox,
            data_dir=tmp_path / "page-data",
            log_dir=tmp_path / "page-logs",
            allowed_lab_export_roots=(export_root,),
            lab_backend=page_writer,
        ),
    )
    definitions = ImmutableDefinitionRegistry(
        manifest.definition_registry_root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=manifest.code_sha
        ).trusted_executable_registry(),
    )
    experiment_reader = ExperimentRegistryReadonlyReader(
        manifest.experiment_registry_path,
        managed_trust_root=manifest.research_root,
    )
    page_controller = StrategyLabJobCenterController(
        reader=LabJobReader(manifest.lab_jobs_path),
        preview_reader=ArtifactPreviewReader(
            reader=LabJobReader(manifest.lab_jobs_path),
            artifact_root=manifest.final_artifact_root,
        ),
        definition_registry=definitions,
        formal_experiment_resolver=RegistryBackedFormalExperimentResolver(experiment_reader),
        clock=lambda: NOW - timedelta(minutes=1),
    )
    assert page_controller._commands is None
    command = page_controller.build_submission_command(
        run_input,
        context=StrategyLabSubmissionContext(
            gate_decision=gate,
            code_sha=CODE_SHA,
            dataset_snapshot=snapshot,
            execution_costs=costs,
            random_seed=7,
            resource_class=ResourceClass.STANDARD,
            deadline=datetime(2026, 8, 1, tzinfo=UTC),
            max_attempts=1,
        ),
        job_id=common["job_id"],
        as_of=NOW - timedelta(minutes=1),
    )
    submitted = page_service.submit(
        SubmitLabCommand(
            command_id="ownership-chain-submit",
            requested_at=NOW - timedelta(minutes=1),
            command=command,
            interaction_key="ownership-chain",
        )
    )
    assert submitted.status is PageControlStatus.SUCCEEDED
    assert submitted.result is not None
    assert submitted.result["result"] == "submitted"
    assert submitted.result["job_id"] == str(common["job_id"])
    job_id = common["job_id"]

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commits = LabArtifactCommitSpool(tmp_path / "artifact-commits")
    final_artifacts = LabJobArtifactStore(final_artifact_path)
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=(),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        artifact_commit_spool=commits,
        artifact_store=final_artifacts,
        finalizer_authority_key_provider=_authority_verification_key_provider,
        runtime_guard=lambda: CODE_SHA,
        require_authority_manifest=True,
        clock=lambda: NOW,
    )
    scheduler.run_once()
    scheduler.release()
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        artifact_commit_spool=commits,
        artifact_store=final_artifacts,
        finalizer_authority_key_provider=_authority_verification_key_provider,
        runtime_guard=lambda: CODE_SHA,
        require_authority_manifest=True,
        clock=lambda: NOW,
    )
    scheduler.run_once()
    claim = claims.pending()[0].claim
    research_store_opener = RecordingResearchStoreOpener()
    worker = _worker(
        tmp_path,
        registry=_ProjectionRecordingRegistry(),
        claims=claims,
        reports=reports,
        exploratory_store_factory=None,
        metadata_store_factory=MetadataStoreFactory(object()),
        lake_root=tmp_path / "research-lake",
        research_store_opener=research_store_opener,
        verified_code_sha_provider=lambda: CODE_SHA,
    )

    try:
        assert worker.run_once().status == "succeeded"
    finally:
        research_store_opener.close()
    scheduler.run_once()
    finalizer = LabFinalizer(
        reader=LabJobReader(store.path),
        shard_artifact_root=tmp_path / "artifacts",
        artifact_store=final_artifacts,
        commit_spool=commits,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: CODE_SHA,
        finalizer_authority_key_provider=_authority_key_provider,
    )
    finalizer.finalize(job_id)
    scheduler.run_once()
    scheduler.release()

    reader = LabJobReader(store.path)
    job = reader.get_job(job_id)
    assert job is not None
    assert (job.status, job.result_state) == (JobStatus.SUCCEEDED, LabResultState.SEALED)
    attempt = registry.get_attempt(experiment.experiment_id)
    assert attempt.spec.experiment_id == experiment.experiment_id
    assert attempt.status is ExperimentStatus.EXECUTED

    catalog_now = datetime(2026, 8, 2, 6, tzinfo=UTC)
    discovery_queue = LabArtifactDiscoveryQueue(
        tmp_path / "artifact-discovery.sqlite3",
        managed_trust_root=tmp_path,
    )
    catalog_lock = tmp_path / "artifact-catalog.lock"
    catalog_lock.touch(mode=0o600)
    (tmp_path / "artifacts").chmod(0o700)
    owner_readers = build_lab_artifact_owner_reader_composition(
        lab_jobs_path=manifest.lab_jobs_path,
        lab_jobs_managed_trust_root=manifest.research_root,
        dataset_authority_path=manifest.dataset_authority_path,
        dataset_authority_managed_trust_root=manifest.research_root,
        experiment_registry_path=manifest.experiment_registry_path,
        experiment_registry_managed_trust_root=manifest.research_root,
        clock=lambda: catalog_now,
    )
    terminal_releases = LabArtifactTerminalReleaseCoordinator(
        reader=reader,
        experiment_registry=registry,
        definition_registry=definitions,
        reference_store=reference_store,
    )
    resolver = owner_readers.owner_resolver
    registrar = LabArtifactCatalogRegistrar(
        artifact_root=tmp_path / "artifacts",
        reference_store=reference_store,
        owner_resolver=resolver,
        terminal_owner_releaser=None if auto_release else terminal_releases,
        location_id="integration-local",
        failure_domain="integration-disk",
        clock=lambda: catalog_now,
    )
    runtime = LabArtifactCatalogRuntime(
        registrar=registrar,
        discovery_queue=discovery_queue,
        max_bundles=8,
        max_discovery_entries=256,
        max_directories_per_step=64,
        max_discovery_seconds=5,
        lock_path=catalog_lock,
        clock=lambda: catalog_now,
    )

    if not auto_release:

        def crash_after_receipt_prepare(_receipt: object) -> None:
            raise RuntimeError("injected daemon crash after terminal receipt prepare")

        with monkeypatch.context() as crash:
            crash.setattr(
                terminal_releases,
                "_after_receipt_prepared",
                crash_after_receipt_prepare,
            )
            with pytest.raises(RuntimeError, match="injected daemon crash"):
                runtime.run_step()

        pending_manifest_hash = LabShardResultManifest.model_validate_json(
            (worker.sealed_bundle_path(claim) / "manifest.json").read_text(encoding="utf-8")
        ).manifest_hash
        assert {
            reference.owner_type
            for reference in reference_store.list_active_references(pending_manifest_hash)
        } == {"audit", "experiment", "job", "snapshot"}

    catalog_result = runtime.run_step()

    assert catalog_result.processed_paths == (
        worker.sealed_bundle_path(claim).relative_to(tmp_path / "artifacts").as_posix(),
    )
    manifest_hash = catalog_result.batch.content_hashes[0]
    assert {
        (reference.owner_type, reference.owner_id)
        for reference in reference_store.list_active_references(manifest_hash)
    } == {
        ("snapshot", snapshot_id),
        ("experiment", experiment.experiment_id),
        ("audit", audit_run_id),
    }

    sealed_manifest = LabShardResultManifest.model_validate_json(
        (worker.sealed_bundle_path(claim) / "manifest.json").read_text(encoding="utf-8")
    )
    owners = resolver(sealed_manifest)
    first_receipt = terminal_releases(sealed_manifest, owners, catalog_now)
    repeated_receipt = LabArtifactTerminalReleaseCoordinator(
        reader=reader,
        experiment_registry=registry,
        definition_registry=definitions,
        reference_store=reference_store,
    )(sealed_manifest, owners, catalog_now + timedelta(minutes=1))
    assert repeated_receipt == first_receipt

    projections = TrustedLabStrategyProjectionReader(
        reader=reader,
        artifact_store=final_artifacts,
        experiment_registry=registry,
        definition_registry=definitions,
    )
    serving = LabJobsServingSourceReader(
        reader=reader,
        strategy_projection_reader=projections,
    )(catalog_now)
    assert serving.payload.projections[0].table_name == "strategy_summary"
    assert serving.payload.projections[0].rows[0]["mean_ret_pct"] == 2.0
    assert serving.payload.projections[1].table_name == "strategy_trade"
    assert serving.payload.projections[1].rows[0]["ret_pct"] == 2.0
