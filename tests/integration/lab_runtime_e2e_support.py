"""Real Strategy Lab command-to-sealed-artifact fixture for runtime E2E tests."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

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
    FormalExperimentPlan,
    HypothesisFamilyManifest,
)
from rquant.job_center_authority import publish_install_current_job_center_authority
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_artifact_protocol import LabArtifactCommitSpool
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_job_center import NShapeComparisonRunInput, build_research_job_submission
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, LabResultState, ResourceClass
from rquant.lab_page_control import build_lab_page_control_writer
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportReceipt,
    LabReportSpool,
    LabWorkerReport,
)
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
    NShapeCompareParameters,
    build_adapter_execution_contract,
    default_strategy_job_adapter_registry,
)
from tests.unit.test_lab_finalizer import (
    _authority_key_provider,
    _authority_verification_key_provider,
)
from tests.unit.test_lab_job_center import _v3_strategy_registration
from tests.unit.test_lab_worker import (
    MetadataStoreFactory,
    RecordingRegistry,
    RecordingResearchStoreOpener,
    _MetadataStore,
    _worker,
)


def create_real_sealed_lab_job(
    *,
    tmp_path: Path,
    research_root: Path,
    artifact_root: Path,
    snapshot_id: str,
    snapshot_binding_hash: str,
    audit_run_id: str,
    catalog_authority_root: Path,
    catalog_authority_receipt_path: Path,
    code_sha: str,
    now: datetime,
) -> ExperimentSpec:
    """Submit through Page Control and drive scheduler/worker/finalizer to SEALED."""

    registration = _v3_strategy_registration(tmp_path)
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
    costs = ExecutionCostSpec(
        commission_bps=Decimal("2.5"),
        stamp_duty_bps=Decimal("5"),
        transfer_fee_bps=Decimal("0.1"),
        slippage_bps=Decimal("3"),
    )
    exploratory = build_research_job_submission(
        run_input,
        gate_decision=gate.model_copy(update={"research_status": "exploratory"}),
        code_sha=code_sha,
        dataset_snapshot=snapshot,
        feature_contract=build_adapter_execution_contract("nshape-compare", "1", code_sha),
        execution_costs=costs,
        random_seed=7,
        resource_class=ResourceClass.STANDARD,
        deadline=now + timedelta(days=1),
        job_id=UUID("11111111-1111-4111-8111-111111111111"),
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
        code_commit=code_sha,
        parameter_fingerprint=canonical_sha256(parameters),
        hypothesis_family="runtime-terminal-e2e",
        metric_definition_fingerprint="b" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 6, 30)),
        validation_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 12, 31)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 3, 31)),
        cost_model_fingerprint=canonical_sha256(costs),
        execution_model_fingerprint=canonical_sha256(
            {
                "contract": "lab-adapter-execution/v1",
                "adapter_id": "nshape-compare",
                "adapter_version": "1",
                "feature_contract": build_adapter_execution_contract(
                    "nshape-compare", "1", code_sha
                ),
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
    final_artifact_root = research_root / "job-artifacts"
    LabJobArtifactStore(final_artifact_root)
    registry_path = research_root / "experiment_registry.sqlite3"
    registry = ExperimentRegistry(registry_path, managed_trust_root=research_root)
    family = HypothesisFamilyManifest(
        hypothesis_family=experiment.hypothesis_family,
        experiment_ids=(experiment.experiment_id,),
        search_space_fingerprint="d" * 64,
        metric_definition_fingerprint=experiment.metric_definition_fingerprint,
        preregistered_at=now - timedelta(days=1),
    )
    registry.register_formal_plan(
        FormalExperimentPlan(
            schema_version=2,
            spec=experiment,
            hypothesis_variant="hold-1",
            strategy_definition_fingerprint=registration.fingerprint,
            definition_registration_record_hash=registration.record_hash,
            preregistered_at=now - timedelta(days=1),
        ),
        family_manifest=family,
    )
    authority = publish_install_current_job_center_authority(
        code_sha=code_sha,
        deployment_profile_id="2" * 64,
        deployment_generation_hash="3" * 64,
        runtime_deployment_root=tmp_path,
        current_code_sha=lambda: code_sha,
        runtime_root=research_root,
        lab_jobs_path=jobs_path,
        command_spool_path=command_path,
        final_artifact_root=final_artifact_root,
        definition_registry_root=tmp_path / "definitions",
        experiment_registry_path=registry_path,
        dataset_authority_path=research_root / "research_ro.duckdb",
        catalog_authority_root=catalog_authority_root,
        catalog_authority_receipt_path=catalog_authority_receipt_path,
    )
    page_outbox = PageControlOutbox(tmp_path / "page-control" / "outbox.sqlite3")
    page_service = PageControlService(
        outbox=page_outbox,
        consumer=PageControlConsumer(
            outbox=page_outbox,
            data_dir=tmp_path / "page-data",
            log_dir=tmp_path / "page-logs",
            allowed_lab_export_roots=(research_root / "exports",),
            lab_backend=build_lab_page_control_writer(
                authority,
                clock=lambda: now - timedelta(minutes=1),
            ),
        ),
    )
    definitions = ImmutableDefinitionRegistry(
        authority.definition_registry_root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=authority.code_sha
        ).trusted_executable_registry(),
    )
    controller = StrategyLabJobCenterController(
        reader=LabJobReader(jobs_path),
        preview_reader=ArtifactPreviewReader(
            reader=LabJobReader(jobs_path), artifact_root=final_artifact_root
        ),
        definition_registry=definitions,
        formal_experiment_resolver=RegistryBackedFormalExperimentResolver(
            ExperimentRegistryReadonlyReader(
                registry_path,
                managed_trust_root=research_root,
            )
        ),
        clock=lambda: now - timedelta(minutes=1),
    )
    command = controller.build_submission_command(
        run_input,
        context=StrategyLabSubmissionContext(
            gate_decision=gate,
            code_sha=code_sha,
            dataset_snapshot=snapshot,
            execution_costs=costs,
            random_seed=7,
            resource_class=ResourceClass.STANDARD,
            deadline=now + timedelta(days=1),
            max_attempts=1,
        ),
        job_id=UUID("11111111-1111-4111-8111-111111111111"),
        as_of=now - timedelta(minutes=1),
    )
    submitted = page_service.submit(
        SubmitLabCommand(
            command_id="runtime-terminal-e2e-submit",
            requested_at=now - timedelta(minutes=1),
            command=command,
            interaction_key="runtime-terminal-e2e",
        )
    )
    assert submitted.status is PageControlStatus.SUCCEEDED

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commits = LabArtifactCommitSpool(tmp_path / "artifact-commits")
    artifact_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    artifact_root.chmod(0o700)
    artifact_store = LabJobArtifactStore(final_artifact_root)
    common_scheduler = dict(
        store=store,
        spool=commands,
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        artifact_commit_spool=commits,
        artifact_store=artifact_store,
        finalizer_authority_key_provider=_authority_verification_key_provider,
        runtime_guard=lambda: code_sha,
        require_authority_manifest=True,
        clock=lambda: now,
    )
    first = LabScheduler(owner_id="scheduler-a", claim_worker_ids=(), **common_scheduler)
    first.run_once()
    first.release()
    scheduler = LabScheduler(
        owner_id="scheduler-b",
        claim_worker_ids=("worker-a",),
        **common_scheduler,
    )
    scheduler.run_once()
    opener = RecordingResearchStoreOpener()
    formal_store = _MetadataStore(snapshot)

    def accept_report(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        return LabReportReceipt.from_report(
            report,
            status="accepted",
            reason=f"accepted:{report.body.report_type}",
            accepted_at=now,
        )

    worker = _worker(
        artifact_root.parent,
        registry=RecordingRegistry(),
        claims=claims,
        reports=reports,
        exploratory_store_factory=None,
        metadata_store_factory=MetadataStoreFactory(formal_store),
        lake_root=tmp_path / "research-lake",
        research_store_opener=opener,
        verified_code_sha_provider=lambda: code_sha,
        clock=lambda: now,
        receipt_waiter=accept_report,
    )
    try:
        assert worker.run_once().status == "succeeded"
    finally:
        opener.close()
    scheduler.run_once()
    LabFinalizer(
        reader=LabJobReader(store.path),
        shard_artifact_root=artifact_root,
        artifact_store=artifact_store,
        commit_spool=commits,
        adapter_registry=default_strategy_job_adapter_registry(),
        verified_code_sha_provider=lambda: code_sha,
        finalizer_authority_key_provider=_authority_key_provider,
    ).finalize(UUID("11111111-1111-4111-8111-111111111111"))
    scheduler.run_once()
    scheduler.release()
    job = LabJobReader(jobs_path).get_job(UUID("11111111-1111-4111-8111-111111111111"))
    assert job is not None
    assert (job.status, job.result_state) == (JobStatus.SUCCEEDED, LabResultState.SEALED)
    assert registry.get_attempt(experiment.experiment_id).status.value == "executed"
    return experiment
