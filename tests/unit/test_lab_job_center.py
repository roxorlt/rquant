from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from inspect import signature
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import (
    DateRange,
    ExperimentRegistry,
    ExperimentSpec,
    ExperimentStatus,
    FormalExperimentPlan,
    HypothesisFamilyManifest,
)
from rquant.lab_highwater_authority import (
    LabHighWaterAuthorityClient,
    LabHighWaterAuthorityConfig,
)
from rquant.lab_job_center import (
    CommandSubmissionConflict,
    CommandSubmissionReceipt,
    ExperimentJobLifecycleSynchronizer,
    ExperimentLifecycleCoordinator,
    LabCommandSubmissionFacade,
    NShapeComparisonRunInput,
    build_research_job_submission,
)
from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommandEnvelope,
    LabCommandSpool,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    LAB_JOB_LIST_FILTER_SQL_PARAMETER_MAX,
    LAB_JOB_LIST_QUERY_PARAMETER_MAX,
    MAX_JOB_SHARDS,
    InvalidStoredJobError,
    JobStatus,
    LabGraphIntegrityReceipt,
    LabIncrementalIntegrityReceipt,
    LabJobListFilters,
    LabJobReader,
    LabJobStore,
    ResourceClass,
)
from rquant.lab_shard_protocol import LabShardFailed
from rquant.research_gate import ResearchGateDecision
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    ResearchExperimentIdentity,
    ResearchJobType,
    ResearchRunParameters,
    StrategyExecutionIdentity,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_job_adapters import (
    NShapeCompareParameters,
    build_adapter_execution_contract,
)

from .test_lab_finalizer import _ready_scenario
from .test_lab_jobs import (
    NOW,
    _formal_v2_spec,
    _formal_v3_spec,
    _lease,
    _register_unprivileged_job_functions,
    _spec,
    _submit,
)
from .test_lab_shard_control_plane import _claim, _report, _setup


def _v3_strategy_registration(tmp_path: Path) -> object:
    code_sha = "1" * 40
    registered_at = datetime(2026, 7, 1, tzinfo=UTC)
    root = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=code_sha)
    bootstrap_builtin_definitions(
        root,
        producer_commit=code_sha,
        registered_at=registered_at,
        available_at=registered_at,
        expected_plan_id=plan.plan_id,
    )
    registration = ImmutableDefinitionRegistry(
        root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=code_sha
        ).trusted_executable_registry(),
    ).latest_strategy_spec("n_shape", as_of=registered_at)
    assert registration is not None
    return registration


def _internally_coherent_formal_authorities(
    tmp_path: Path,
    *,
    definition_fingerprint: str | None = None,
    registration_record_hash: str | None = None,
) -> tuple[object, ExperimentRegistry, ImmutableDefinitionRegistry]:
    tmp_path.mkdir(parents=True, exist_ok=True, mode=0o700)
    registration = _v3_strategy_registration(tmp_path)
    base = _formal_v2_spec()
    selected_definition = definition_fingerprint or registration.fingerprint
    selected_record = registration_record_hash or registration.record_hash
    execution = StrategyExecutionIdentity(
        strategy_id=registration.logical_id,
        strategy_version=registration.version,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        strategy_spec_fingerprint=registration.spec.spec_fingerprint,
        strategy_definition_fingerprint=selected_definition,
        strategy_executable_fingerprint=registration.executable_fingerprint,
        candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
        definition_registration_record_hash=selected_record,
        definition_registered_at=registration.registered_at,
        definition_available_at=registration.available_at,
        producer_code_commit=registration.producer_commit,
    )
    assert base.dataset_snapshot is not None
    experiment = ExperimentSpec(
        strategy_spec_fingerprint=execution.strategy_spec_fingerprint,
        strategy_executable_fingerprint=execution.strategy_executable_fingerprint,
        candidate_schema_fingerprint=execution.candidate_schema_fingerprint,
        dataset_snapshot_id=base.dataset_snapshot.snapshot_id,
        code_commit=base.code_sha,
        parameter_fingerprint=canonical_sha256(base.parameters),
        hypothesis_family="synthetic-definition-review",
        metric_definition_fingerprint="7" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 6, 30)),
        validation_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 12, 31)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 3, 31)),
        cost_model_fingerprint=canonical_sha256(base.execution_costs),
        execution_model_fingerprint=canonical_sha256(
            {
                "contract": "lab-adapter-execution/v1",
                "adapter_id": execution.adapter_id,
                "adapter_version": execution.adapter_version,
                "feature_contract": base.feature_contract,
            }
        ),
        seed=base.random_seed,
    )
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=experiment,
        hypothesis_variant="baseline",
        strategy_definition_fingerprint=selected_definition,
        definition_registration_record_hash=selected_record,
        preregistered_at=NOW - timedelta(minutes=2),
    )
    assert experiment.experiment_id is not None
    assert plan.plan_id is not None
    spec = base.model_copy(
        update={
            "schema_version": 3,
            "strategy_execution": execution,
            "experiment": ResearchExperimentIdentity(
                schema_version=2,
                spec=experiment,
                experiment_id=experiment.experiment_id,
                hypothesis_family=experiment.hypothesis_family,
                hypothesis_variant=plan.hypothesis_variant,
                formal_plan_id=plan.plan_id,
            ),
        }
    )
    registry_root = tmp_path / "research"
    registry_root.mkdir(parents=True, mode=0o700)
    registry = ExperimentRegistry(
        registry_root / "experiments.sqlite3",
        managed_trust_root=registry_root,
    )
    registry.register_formal_plan(
        plan,
        family_manifest=HypothesisFamilyManifest(
            hypothesis_family=experiment.hypothesis_family,
            experiment_ids=(experiment.experiment_id,),
            search_space_fingerprint="8" * 64,
            metric_definition_fingerprint=experiment.metric_definition_fingerprint,
            preregistered_at=NOW - timedelta(minutes=3),
        ),
    )
    definitions = ImmutableDefinitionRegistry(
        tmp_path / "definitions",
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=base.code_sha
        ).trusted_executable_registry(),
    )
    return spec, registry, definitions


def test_formal_submission_requires_trusted_definition_and_experiment_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_input = NShapeComparisonRunInput(
        start_date=date(2026, 4, 1),
        end_date=date(2026, 6, 30),
        parameters=NShapeCompareParameters(hold_days=(3,), entry_modes=("first_break",)),
    )
    gate = ResearchGateDecision(
        allowed=True,
        research_status="comparable",
        audit_run_id="e" * 64,
        dataset_snapshot_id="a" * 64,
        dataset_binding_hash="f" * 64,
        coverage_ratios={},
        coverage_counts={},
        failures=(),
    )
    kwargs = dict(
        gate_decision=gate,
        code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="a" * 64,
            binding_hash="f" * 64,
            audit_run_id="e" * 64,
        ),
        feature_contract=build_adapter_execution_contract("nshape-compare", "1", "1" * 40),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal("2.5"),
            stamp_duty_bps=Decimal("5"),
            transfer_fee_bps=Decimal("0.1"),
            slippage_bps=Decimal("3"),
        ),
        random_seed=7,
        resource_class=ResourceClass.STANDARD,
        deadline=datetime(2026, 8, 1, tzinfo=UTC),
        job_id=UUID(int=77),
    )

    with pytest.raises(ValueError, match="trusted strategy registration"):
        build_research_job_submission(run_input, **kwargs)

    registration = _v3_strategy_registration(tmp_path)
    expected_parameters = ResearchRunParameters(
        strategy_name="n_shape",
        start_date=run_input.start_date,
        end_date=run_input.end_date,
        arguments=tuple(
            # The builder owns parameter canonicalization; copy its legacy exploratory shape.
            build_research_job_submission(
                run_input,
                **{
                    **kwargs,
                    "gate_decision": gate.model_copy(update={"research_status": "exploratory"}),
                },
            ).spec.parameters.arguments
        ),
    )
    costs = kwargs["execution_costs"]
    feature_contract = kwargs["feature_contract"]
    experiment = ExperimentSpec(
        strategy_spec_fingerprint=registration.spec.spec_fingerprint,
        strategy_executable_fingerprint=registration.executable_fingerprint,
        candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
        dataset_snapshot_id="a" * 64,
        code_commit="1" * 40,
        parameter_fingerprint=canonical_sha256(expected_parameters),
        hypothesis_family="n-shape-family",
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
                "feature_contract": feature_contract,
            }
        ),
        seed=7,
    )
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=experiment,
        hypothesis_variant="hold-3",
        strategy_definition_fingerprint=registration.fingerprint,
        definition_registration_record_hash=registration.record_hash,
        preregistered_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    built = build_research_job_submission(
        run_input,
        **kwargs,
        trusted_strategy_registration=registration,
        formal_experiment_plan=plan,
    )

    assert built.spec.schema_version == 3
    assert built.spec.catalog_owner_eligible
    assert built.spec.strategy_execution is not None
    assert built.spec.strategy_execution.strategy_definition_fingerprint == registration.fingerprint
    assert built.spec.experiment is not None
    assert built.spec.experiment.experiment_id == experiment.experiment_id

    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    assert experiment.experiment_id is not None
    family = HypothesisFamilyManifest(
        hypothesis_family=experiment.hypothesis_family,
        experiment_ids=(experiment.experiment_id,),
        search_space_fingerprint="e" * 64,
        metric_definition_fingerprint=experiment.metric_definition_fingerprint,
        preregistered_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    registry.register_formal_plan(plan, family_manifest=family)
    definitions = ImmutableDefinitionRegistry(
        tmp_path / "definitions",
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit="1" * 40
        ).trusted_executable_registry(),
    )
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(tmp_path / "commands"),
        experiment_registry=registry,
        definition_registry=definitions,
        clock=lambda: datetime(2026, 7, 2, tzinfo=UTC),
    )

    receipt = facade.submit_create(built.command, interaction_key="formal-v3")

    assert isinstance(receipt, CommandSubmissionReceipt)
    assert registry.get_attempt(experiment.experiment_id).spec == experiment
    assert registry.list_pending_submissions(limit=10) == ()

    recovery_spool = LabCommandSpool(tmp_path / "recovery-commands")
    recovery_facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=recovery_spool,
        experiment_registry=registry,
        definition_registry=definitions,
        clock=lambda: datetime(2026, 7, 2, 0, 1, tzinfo=UTC),
    )
    recovery_command = SubmitJobCommand(
        job_id=UUID(int=78),
        spec=built.spec,
        max_attempts=2,
    )
    original_publish = recovery_spool.publish

    def fail_publish(_envelope: LabCommandEnvelope) -> object:
        raise OSError("injected command spool outage")

    monkeypatch.setattr(recovery_spool, "publish", fail_publish)
    with pytest.raises(OSError, match="spool outage"):
        recovery_facade.submit_create(recovery_command, interaction_key="formal-v3-recovery")
    assert len(registry.list_pending_submissions(limit=10)) == 1
    monkeypatch.setattr(recovery_spool, "publish", original_publish)

    recovered = recovery_facade.recover_pending_experiment_submissions()

    assert len(recovered) == 1
    assert recovered[0].job_id == UUID(int=78)
    assert registry.list_pending_submissions(limit=10) == ()

    class _LifecycleReader:
        job: object

        def get_job(self, _job_id: UUID) -> object:
            return self.job

    lifecycle_reader = _LifecycleReader()
    lifecycle = ExperimentJobLifecycleSynchronizer(
        reader=lifecycle_reader,  # type: ignore[arg-type]
        registry=registry,
    )
    lifecycle_reader.job = SimpleNamespace(
        spec=built.spec,
        status=JobStatus.RUNNING,
        recoverable=True,
        attempt_count=1,
        max_attempts=3,
        updated_at=datetime(2026, 7, 2, 0, 2, tzinfo=UTC),
    )
    running = lifecycle.synchronize(
        built.command.job_id,
        observed_at=datetime(2026, 7, 2, 0, 3, tzinfo=UTC),
    )
    lifecycle_reader.job = SimpleNamespace(
        spec=built.spec,
        status=JobStatus.FAILED,
        recoverable=True,
        attempt_count=1,
        max_attempts=3,
        updated_at=datetime(2026, 7, 2, 0, 4, tzinfo=UTC),
    )
    retryable_failure = lifecycle.synchronize(
        built.command.job_id,
        observed_at=datetime(2026, 7, 2, 0, 5, tzinfo=UTC),
    )

    assert running.spec.experiment_id == retryable_failure.spec.experiment_id
    assert running.started_at == retryable_failure.started_at


def test_submission_facade_rejects_v2_comparable_without_any_durable_side_effect(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    command = SubmitJobCommand(
        job_id=UUID(int=7_001),
        spec=_formal_v2_spec(),
        max_attempts=1,
    )

    with pytest.raises(ValueError, match="v2.*comparable|comparable.*v2"):
        LabCommandSubmissionFacade(
            reader=LabJobReader(store.path),
            spool=spool,
        ).submit_create(command, interaction_key="reject-v2-comparable")

    assert LabJobReader(store.path).get_job(command.job_id) is None
    assert spool.pending() == ()


def test_submission_facade_rejects_v3_without_exact_formal_plan_before_writes(
    tmp_path: Path,
) -> None:
    spec = _formal_v3_spec()
    assert spec.experiment is not None
    experiment = spec.experiment.spec
    assert experiment.experiment_id is not None
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    registry.register_hypothesis_family(
        HypothesisFamilyManifest(
            hypothesis_family=experiment.hypothesis_family,
            experiment_ids=(experiment.experiment_id,),
            search_space_fingerprint="8" * 64,
            metric_definition_fingerprint=experiment.metric_definition_fingerprint,
            preregistered_at=NOW - timedelta(minutes=1),
        )
    )
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    command = SubmitJobCommand(job_id=UUID(int=7_002), spec=spec, max_attempts=1)

    with pytest.raises(ValueError, match="formal plan"):
        LabCommandSubmissionFacade(
            reader=LabJobReader(store.path),
            spool=spool,
            experiment_registry=registry,
            clock=lambda: NOW,
        ).submit_create(command, interaction_key="missing-formal-plan")

    assert registry.list_family_attempts(experiment.hypothesis_family) == ()
    assert registry.list_submission_intents(limit=10) == ()
    assert LabJobReader(store.path).get_job(command.job_id) is None
    assert spool.pending() == ()


def test_submission_facade_rejects_synthetic_definition_identity_before_writes(
    tmp_path: Path,
) -> None:
    spec, registry, definitions = _internally_coherent_formal_authorities(
        tmp_path,
        definition_fingerprint="f" * 64,
        registration_record_hash="e" * 64,
    )
    assert spec.experiment is not None
    experiment = spec.experiment.spec
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    command = SubmitJobCommand(job_id=UUID(int=7_003), spec=spec, max_attempts=1)

    with pytest.raises(ValueError, match="trusted strategy registration"):
        LabCommandSubmissionFacade(
            reader=LabJobReader(store.path),
            spool=spool,
            experiment_registry=registry,
            definition_registry=definitions,
            clock=lambda: NOW,
        ).submit_create(command, interaction_key="synthetic-definition")

    assert registry.list_family_attempts(experiment.hypothesis_family) == ()
    assert registry.list_submission_intents(limit=10) == ()
    assert LabJobReader(store.path).get_job(command.job_id) is None
    assert spool.pending() == ()


class _MutableLifecycleReader:
    def __init__(self, job: object) -> None:
        self.job = job

    def get_job(self, _job_id: UUID) -> object:
        return self.job


def _lifecycle_facade(
    tmp_path: Path,
    *,
    case: str,
) -> tuple[LabCommandSubmissionFacade, _MutableLifecycleReader, str]:
    spec, registry, definitions = _internally_coherent_formal_authorities(tmp_path / case)
    assert spec.experiment is not None
    experiment = spec.experiment.spec
    assert experiment.experiment_id is not None
    registry.register_attempt(experiment, registered_at=NOW - timedelta(minutes=1))
    reader = _MutableLifecycleReader(
        SimpleNamespace(
            spec=spec,
            status=JobStatus.QUEUED,
            recoverable=False,
            attempt_count=0,
            max_attempts=3,
            updated_at=NOW,
        )
    )
    return (
        LabCommandSubmissionFacade(
            reader=reader,  # type: ignore[arg-type]
            spool=LabCommandSpool(tmp_path / case / "commands"),
            experiment_registry=registry,
            definition_registry=definitions,
            clock=lambda: NOW,
        ),
        reader,
        experiment.experiment_id,
    )


def test_facade_lifecycle_recovery_is_idempotent_across_retry_and_success(
    tmp_path: Path,
) -> None:
    facade, reader, experiment_id = _lifecycle_facade(tmp_path, case="retry")
    job_id = UUID(int=501)
    started_at = NOW + timedelta(minutes=1)
    reader.job = SimpleNamespace(
        spec=reader.job.spec,
        status=JobStatus.RUNNING,
        recoverable=False,
        attempt_count=1,
        max_attempts=3,
        updated_at=started_at,
    )
    running = facade.synchronize_experiment_lifecycle(
        job_id,
        observed_at=started_at + timedelta(seconds=1),
    )

    reader.job = SimpleNamespace(
        spec=reader.job.spec,
        status=JobStatus.FAILED,
        recoverable=True,
        attempt_count=1,
        max_attempts=3,
        updated_at=started_at + timedelta(minutes=1),
    )
    recoverable_failure = facade.synchronize_experiment_lifecycle(
        job_id,
        observed_at=started_at + timedelta(minutes=2),
    )
    reader.job = SimpleNamespace(
        spec=reader.job.spec,
        status=JobStatus.QUEUED,
        recoverable=False,
        attempt_count=1,
        max_attempts=3,
        updated_at=started_at + timedelta(minutes=3),
    )
    queued_retry = facade.synchronize_experiment_lifecycle(
        job_id,
        observed_at=started_at + timedelta(minutes=4),
    )
    reader.job = SimpleNamespace(
        spec=reader.job.spec,
        status=JobStatus.SUCCEEDED,
        recoverable=False,
        attempt_count=2,
        max_attempts=3,
        updated_at=started_at + timedelta(minutes=5),
    )
    succeeded_job = facade.synchronize_experiment_lifecycle(
        job_id,
        observed_at=started_at + timedelta(minutes=6),
    )
    repeated = facade.synchronize_experiment_lifecycle(
        job_id,
        observed_at=started_at + timedelta(minutes=7),
    )

    assert {
        attempt.spec.experiment_id
        for attempt in (
            running,
            recoverable_failure,
            queued_retry,
            succeeded_job,
            repeated,
        )
    } == {experiment_id}
    assert all(
        attempt.status is ExperimentStatus.RUNNING
        for attempt in (running, recoverable_failure, queued_retry)
    )
    assert succeeded_job.status is ExperimentStatus.EXECUTED
    assert repeated.status is ExperimentStatus.EXECUTED
    assert succeeded_job.completed_at == started_at + timedelta(minutes=5)
    assert all(
        attempt.started_at == started_at
        for attempt in (
            running,
            recoverable_failure,
            queued_retry,
            succeeded_job,
            repeated,
        )
    )


def test_lifecycle_coordinator_recovers_published_job_after_daemon_restart(
    tmp_path: Path,
) -> None:
    facade, reader, experiment_id = _lifecycle_facade(tmp_path, case="restart")
    job_id = UUID(int=503)
    command = SubmitJobCommand(
        job_id=job_id,
        spec=reader.job.spec,
        max_attempts=3,
    )
    owned_spec = reader.job.spec
    reader.job = None
    facade.submit_create(command, interaction_key="restart-owned-job")
    reader.job = SimpleNamespace(
        spec=owned_spec,
        status=JobStatus.SUCCEEDED,
        recoverable=False,
        attempt_count=1,
        max_attempts=3,
        updated_at=NOW + timedelta(minutes=1),
    )
    restarted = ExperimentLifecycleCoordinator(facade)

    first = restarted.recover(observed_at=NOW + timedelta(minutes=2))
    repeated = ExperimentLifecycleCoordinator(facade).recover(
        observed_at=NOW + timedelta(minutes=3)
    )

    assert first.synchronized_job_ids == (job_id,)
    assert repeated.synchronized_job_ids == ()
    attempt = facade.experiment_registry.get_attempt(experiment_id)
    assert attempt.status is ExperimentStatus.EXECUTED


@pytest.mark.parametrize(
    ("case", "status", "recoverable", "expected_status", "expected_error"),
    (
        (
            "failed",
            JobStatus.FAILED,
            False,
            ExperimentStatus.FAILED,
            "lab job failed after 3/3 attempts",
        ),
        (
            "cancelled",
            JobStatus.CANCELLED,
            False,
            ExperimentStatus.CANCELLED,
            "lab job cancelled",
        ),
    ),
)
def test_facade_lifecycle_terminal_mapping_is_immutable_and_idempotent(
    tmp_path: Path,
    case: str,
    status: JobStatus,
    recoverable: bool,
    expected_status: ExperimentStatus,
    expected_error: str,
) -> None:
    facade, reader, experiment_id = _lifecycle_facade(tmp_path, case=case)
    completed_at = NOW + timedelta(minutes=1)
    reader.job = SimpleNamespace(
        spec=reader.job.spec,
        status=status,
        recoverable=recoverable,
        attempt_count=3,
        max_attempts=3,
        updated_at=completed_at,
    )

    first = facade.synchronize_experiment_lifecycle(
        UUID(int=502),
        observed_at=completed_at + timedelta(seconds=1),
    )
    repeated = facade.synchronize_experiment_lifecycle(
        UUID(int=502),
        observed_at=completed_at + timedelta(seconds=2),
    )

    assert first == repeated
    assert first.spec.experiment_id == experiment_id
    assert first.status is expected_status
    assert first.completed_at == completed_at
    assert first.first_error == expected_error


def test_facade_lifecycle_rejects_legacy_non_owner_job(tmp_path: Path) -> None:
    tmp_path.chmod(0o700)
    reader = _MutableLifecycleReader(
        SimpleNamespace(
            spec=_spec(),
            status=JobStatus.SUCCEEDED,
            recoverable=False,
            attempt_count=1,
            max_attempts=1,
            updated_at=NOW,
        )
    )
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    facade = LabCommandSubmissionFacade(
        reader=reader,  # type: ignore[arg-type]
        spool=LabCommandSpool(tmp_path / "commands"),
        experiment_registry=registry,
    )

    with pytest.raises(ValueError, match="legacy lab jobs"):
        facade.synchronize_experiment_lifecycle(
            UUID(int=503),
            observed_at=NOW + timedelta(seconds=1),
        )


def test_v3_submission_interaction_conflict_does_not_prepare_a_second_outbox(
    tmp_path: Path,
) -> None:
    facade, reader, experiment_id = _lifecycle_facade(tmp_path, case="submit-conflict")
    spec = reader.job.spec
    reader.job = None
    interaction_key = "formal-v3-content-conflict"

    first = facade.submit_create(
        SubmitJobCommand(job_id=UUID(int=601), spec=spec, max_attempts=2),
        interaction_key=interaction_key,
    )
    conflict = facade.submit_create(
        SubmitJobCommand(job_id=UUID(int=602), spec=spec, max_attempts=2),
        interaction_key=interaction_key,
    )

    assert isinstance(first, CommandSubmissionReceipt)
    assert isinstance(conflict, CommandSubmissionConflict)
    assert conflict.reason == "interaction_content_conflict"
    assert facade.experiment_registry is not None
    assert facade.experiment_registry.get_attempt(experiment_id).spec == spec.experiment.spec
    assert facade.experiment_registry.list_pending_submissions(limit=10) == ()


class _CountingReader(LabJobReader):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.statements: list[str] = []

    def _connect(self):  # type: ignore[no-untyped-def]
        connection = super()._connect()
        connection.set_trace_callback(
            lambda statement: self.statements.append(" ".join(statement.split()))
        )
        return connection


class _VmStepReader(LabJobReader):
    """Count SQLite VM opcodes for one reader operation, not SQL text."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.vm_steps = 0

    def _connect(self):  # type: ignore[no-untyped-def]
        connection = super()._connect()

        def count_step() -> int:
            self.vm_steps += 1
            return 0

        connection.set_progress_handler(count_step, 1)
        return connection


def _seed_jobs(tmp_path: Path, count: int) -> tuple[LabJobStore, tuple[UUID, ...]]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    return store, _append_seed_jobs(store, count)


def _append_seed_jobs(store: LabJobStore, count: int, *, start: int = 1) -> tuple[UUID, ...]:
    lease = _lease(store, seconds=10_000)
    job_ids: list[UUID] = []
    for index in range(count):
        job_id = UUID(int=start + index)
        spec = _spec(
            job_type=(
                ResearchJobType.PARAMETER_SEARCH if index % 2 else ResearchJobType.STRATEGY_REPLAY
            ),
            resource_class=(ResourceClass.HEAVY if index % 3 == 0 else ResourceClass.STANDARD),
        )
        spec = spec.model_copy(
            update={
                "parameters": spec.parameters.model_copy(
                    update={"strategy_name": f"strategy-{start + index - 1:03d}"}
                )
            }
        )
        envelope = _submit(job_id=job_id, spec=spec)
        receipt = store.apply_command(
            envelope,
            lease=lease,
            now=NOW + timedelta(seconds=index),
        )
        assert receipt.status == "applied"
        job_ids.append(job_id)
    return tuple(job_ids)


def test_rerun_submits_new_identity_with_authoritative_spec_exactly_once(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    source_id = UUID(int=101)
    source_spec = _spec()
    source_receipt = store.apply_command(
        _submit(job_id=source_id, spec=source_spec),
        lease=lease,
        now=NOW,
    )
    assert source_receipt.status == "applied"
    spool = LabCommandSpool(tmp_path / "commands")
    facade = LabCommandSubmissionFacade(reader=LabJobReader(store.path), spool=spool)
    new_job_id = UUID(int=102)

    first = facade.submit_rerun(
        source_id,
        new_job_id=new_job_id,
        max_attempts=3,
        interaction_key="rerun-101",
    )
    repeated = facade.submit_rerun(
        source_id,
        new_job_id=new_job_id,
        max_attempts=3,
        interaction_key="rerun-101",
    )

    assert isinstance(first, CommandSubmissionReceipt)
    assert repeated == first
    assert "spec" not in signature(facade.submit_rerun).parameters
    assert len(spool.pending()) == 1
    command = spool.pending()[0].envelope.command
    assert isinstance(command, SubmitJobCommand)
    assert command.job_id == new_job_id
    assert command.spec == source_spec
    assert command.max_attempts == 3
    source = LabJobReader(store.path).get_job(source_id)
    assert source is not None
    assert source.job_id == source_id
    assert source.spec == source_spec
    assert source.version == 0


def test_rerun_rejects_missing_same_or_existing_job_identity(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    source_id = UUID(int=201)
    existing_id = UUID(int=202)
    assert (
        store.apply_command(_submit(job_id=source_id, spec=_spec()), lease=lease, now=NOW).status
        == "applied"
    )
    assert (
        store.apply_command(
            _submit(job_id=existing_id, spec=_spec()),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        ).status
        == "applied"
    )
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(tmp_path / "commands"),
    )

    missing = facade.submit_rerun(
        UUID(int=999),
        new_job_id=UUID(int=203),
        max_attempts=1,
        interaction_key="rerun-missing",
    )
    same = facade.submit_rerun(
        source_id,
        new_job_id=source_id,
        max_attempts=1,
        interaction_key="rerun-same",
    )
    existing = facade.submit_rerun(
        source_id,
        new_job_id=existing_id,
        max_attempts=1,
        interaction_key="rerun-existing",
    )

    assert isinstance(missing, CommandSubmissionConflict)
    assert missing.reason == "job_not_found"
    assert isinstance(same, CommandSubmissionConflict)
    assert same.reason == "job_id_exists"
    assert isinstance(existing, CommandSubmissionConflict)
    assert existing.reason == "job_id_exists"
    assert facade.spool.pending() == ()


def test_rerun_stable_interaction_key_rejects_content_conflict(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    source_id = UUID(int=301)
    assert (
        store.apply_command(
            _submit(job_id=source_id, spec=_spec()), lease=_lease(store), now=NOW
        ).status
        == "applied"
    )
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(tmp_path / "commands"),
    )

    first = facade.submit_rerun(
        source_id,
        new_job_id=UUID(int=302),
        max_attempts=2,
        interaction_key="rerun-conflict",
    )
    conflict = facade.submit_rerun(
        source_id,
        new_job_id=UUID(int=302),
        max_attempts=3,
        interaction_key="rerun-conflict",
    )

    assert isinstance(first, CommandSubmissionReceipt)
    assert isinstance(conflict, CommandSubmissionConflict)
    assert conflict.request_id == first.request_id
    assert conflict.reason == "interaction_content_conflict"
    assert len(facade.spool.pending()) == 1


@pytest.mark.parametrize("max_attempts", [0, True, "2"])
def test_rerun_rejects_malformed_attempt_bounds(
    tmp_path: Path,
    max_attempts: object,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    source_id = UUID(int=401)
    assert (
        store.apply_command(
            _submit(job_id=source_id, spec=_spec()), lease=_lease(store), now=NOW
        ).status
        == "applied"
    )
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(tmp_path / "commands"),
    )

    with pytest.raises(ValidationError, match="max_attempts"):
        facade.submit_rerun(
            source_id,
            new_job_id=UUID(int=402),
            max_attempts=max_attempts,  # type: ignore[arg-type]
            interaction_key="rerun-bounds",
        )

    assert facade.spool.pending() == ()


def test_list_jobs_keyset_pagination_is_stable_bounded_and_has_no_n_plus_one(
    tmp_path: Path,
) -> None:
    store, expected_ids = _seed_jobs(tmp_path, 125)
    reader = _CountingReader(store.path)

    cursor: str | None = None
    observed: list[UUID] = []
    while True:
        page = reader.list_jobs(limit=17, cursor=cursor)
        observed.extend(item.job_id for item in page.items)
        assert page.total_count == 125
        if not page.has_more:
            assert page.next_cursor is None
            break
        assert page.next_cursor is not None
        cursor = page.next_cursor

    assert observed == list(reversed(expected_ids))
    assert len(observed) == len(set(observed)) == 125
    selects = [
        statement for statement in reader.statements if statement.startswith(("SELECT", "WITH"))
    ]
    assert reader.graph_validation_runs == 0
    assert reader.graph_validation_peak_batch == 0
    for table in (
        "lab_job",
        "lab_shard",
        "lab_event",
        "lab_lease",
        "lab_artifact",
        "lab_command",
        "lab_worker_report",
        "lab_artifact_commit",
        "lab_job_result_artifact",
        "lab_scheduler_state",
    ):
        assert not any(statement == f"SELECT * FROM {table}" for statement in selects)
    page_selects = [statement for statement in selects if "page_jobs AS MATERIALIZED" in statement]
    assert len(page_selects) == 8
    assert all(
        "WHERE job_id IN (SELECT job_id FROM page_jobs)" in statement for statement in page_selects
    )
    assert reader.statements.count("BEGIN") == 8
    assert reader.statements.count("COMMIT") == 8


@pytest.mark.parametrize("reader_method", ("list_jobs", "list_finalization_candidates"))
def test_first_page_after_new_mutation_never_scans_the_historical_integrity_graph(
    tmp_path: Path,
    reader_method: str,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 257)
    if reader_method == "list_jobs":
        lease = LabJobReader(store.path).list_leases()[-1]
        store.apply_command(
            _submit(job_id=UUID(int=10_000)),
            lease=lease,
            now=NOW + timedelta(seconds=300),
        )
    else:
        lease = LabJobReader(store.path).list_leases()[-1]
        # These synthetic records exercise the historical read graph, not queue recovery.
        for job_id in _job_ids:
            assert store.fail_unplanned_job(
                job_id,
                reason="historical job center graph fixture",
                lease=lease,
                now=NOW + timedelta(seconds=300),
            )
        historical_job = LabJobReader(store.path).get_job(_job_ids[0])
        assert historical_job is not None
        historical_version = historical_job.version
        store.release_scheduler_lease(
            lease,
            now=NOW + timedelta(seconds=300),
        )
        store = _ready_scenario(tmp_path, hold_days=(1,)).store
        historical_job = LabJobReader(store.path).get_job(_job_ids[0])
        assert historical_job is not None
        assert historical_job.version == historical_version

    reader = _CountingReader(store.path)
    page = getattr(reader, reader_method)(limit=1)

    assert page.items
    statements = tuple(reader.statements)
    selects = tuple(
        statement for statement in statements if statement.startswith(("SELECT", "WITH"))
    )
    assert len(selects) <= 8
    assert reader.graph_validation_runs == 0
    assert not any("PRAGMA foreign_key_check" in statement for statement in statements)
    for table in (
        "lab_job",
        "lab_shard",
        "lab_event",
        "lab_lease",
        "lab_artifact",
        "lab_command",
        "lab_worker_report",
        "lab_artifact_commit",
        "lab_job_result_artifact",
        "lab_scheduler_state",
    ):
        assert f"SELECT * FROM {table}" not in statements
    shard_reads = tuple(statement for statement in selects if "FROM lab_shard" in statement)
    assert shard_reads
    assert all(
        "WHERE job_id =" in statement
        or "WHERE job_id IN (SELECT job_id FROM page_jobs)" in statement
        for statement in shard_reads
    )


def test_explicit_integrity_audit_returns_a_generation_bound_verified_receipt(
    tmp_path: Path,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 3)
    reader = LabJobReader(store.path)

    first = reader.audit_integrity()
    repeated = reader.audit_integrity()
    lease = reader.list_leases()[-1]
    store.apply_command(
        _submit(job_id=UUID(int=10_001)),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    after_mutation = reader.audit_integrity()

    assert isinstance(first, LabGraphIntegrityReceipt)
    assert repeated == first
    assert first.table_counts.lab_job == 3
    assert after_mutation.table_counts.lab_job == 4
    assert after_mutation.database_generation == first.database_generation
    assert after_mutation.mutation_epoch > first.mutation_epoch
    assert after_mutation.receipt_hash != first.receipt_hash


def test_integrity_audit_rejects_reused_receipt_when_epoch_is_rolled_back(
    tmp_path: Path,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 2)
    reader = LabJobReader(store.path)
    first = reader.audit_integrity()
    first_epoch = first.mutation_epoch
    lease = reader.list_leases()[-1]

    store.apply_command(
        _submit(job_id=UUID(int=10_002)),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_ledger_epoch SET mutation_epoch = ? WHERE singleton = 1",
            (first_epoch,),
        )

    with pytest.raises(InvalidStoredJobError, match="mutation epoch rolled back"):
        reader.audit_integrity()

    assert reader.graph_validation_runs == 2


def _highwater_key_pair(root: Path, key_id: str) -> tuple[Path, bytes]:
    key_root = root / "key-material"
    key_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_key = key_root / f"{key_id}.private.pem"
    public_key = key_root / f"{key_id}.public.pem"
    if not private_key.exists():
        subprocess.run(
            [
                "/opt/homebrew/bin/openssl",
                "genpkey",
                "-algorithm",
                "ED25519",
                "-out",
                str(private_key),
            ],
            check=True,
            capture_output=True,
        )
        private_key.chmod(0o600)
        subprocess.run(
            [
                "/opt/homebrew/bin/openssl",
                "pkey",
                "-in",
                str(private_key),
                "-pubout",
                "-out",
                str(public_key),
            ],
            check=True,
            capture_output=True,
        )
    return private_key, public_key.read_bytes()


def _highwater_public_keys(root: Path, key_ids: set[str]) -> dict[str, bytes]:
    return {key_id: _highwater_key_pair(root, key_id)[1] for key_id in key_ids}


def _highwater_observer(
    root: Path,
    *,
    key_id: str = "anchor-v1",
    authority_key_ids: set[str] | None = None,
    trusted_keys: dict[str, bytes] | None = None,
    code_identity: str = "1" * 40,
    profile_identity: str = "2" * 64,
    allow_identity_rotation: bool = False,
) -> LabHighWaterAuthorityClient:
    """Build the real external high-water helper used by production authority tests."""

    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    keys_path = root / "keys.json"
    key_ids = authority_key_ids or {key_id}
    previous_key_ids = key_ids - {key_id}
    document = {
        "schema_version": 3,
        "generation": 2 if previous_key_ids else 1,
        "previous_manifest_hash": "1" * 64 if previous_key_ids else "0" * 64,
        "active_key_id": key_id,
        "active_private_key_path": str(_highwater_key_pair(root, key_id)[0]),
        "previous_public_keys": {
            name: _highwater_key_pair(root, name)[1].decode("utf-8")
            for name in previous_key_ids
        },
    }
    keys_path.write_text(json.dumps(document), encoding="utf-8")
    keys_path.chmod(0o600)
    helper = (
        Path(__file__).resolve().parents[2]
        / "deploy"
        / "libexec"
        / "rquant-lab-highwater-authority"
    )
    verification_keys = trusted_keys or _highwater_public_keys(root, key_ids)
    return LabHighWaterAuthorityClient(
        LabHighWaterAuthorityConfig(
            command=(
                sys.executable,
                str(helper),
                "--state-root",
                str(root / "state"),
                "--keys-file",
                str(keys_path),
            ),
            stable_identity="strategy-lab-test-ledger",
            code_identity=code_identity,
            profile_identity=profile_identity,
            trusted_key_provider=verification_keys.get,
            active_key_id=key_id,
            allow_identity_rotation=allow_identity_rotation,
        )
    )


def _highwater_state(root: Path) -> Path:
    states = tuple((root / "state").glob("*/"))
    assert len(states) == 1
    return states[0]


def test_external_highwater_rejects_cross_process_chain_rollback(tmp_path: Path) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 2)
    observer = _highwater_observer(tmp_path / "integrity-anchor")
    first = LabJobReader(
        store.path, highwater_observer=observer, production_mode=True
    ).audit_integrity()
    lease = LabJobReader(store.path).list_leases()[-1]
    store.apply_command(
        _submit(job_id=UUID(int=10_005)),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    advanced = LabJobReader(
        store.path, highwater_observer=observer, production_mode=True
    ).audit_integrity()
    assert advanced.chain_generation > first.chain_generation

    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "DELETE FROM lab_ledger_chain_entry WHERE chain_generation > ?",
            (first.chain_generation,),
        )
        connection.execute(
            "UPDATE lab_ledger_chain SET chain_generation = ?, head_hash = ? WHERE singleton = 1",
            (first.chain_generation, first.chain_head_hash),
        )
        connection.execute(
            "UPDATE lab_ledger_epoch SET mutation_epoch = ? WHERE singleton = 1",
            (first.mutation_epoch,),
        )

    with pytest.raises(Exception, match="high-water.*rolled back"):
        LabJobReader(
            store.path, highwater_observer=observer, production_mode=True
        ).audit_integrity()


def test_external_highwater_rejects_unsigned_joint_database_and_authority_forgery(
    tmp_path: Path,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 1)
    root = tmp_path / "integrity-anchor"
    observer = _highwater_observer(root)
    LabJobReader(store.path, highwater_observer=observer, production_mode=True).audit_integrity()

    state = _highwater_state(root)
    (state / "chain.jsonl").write_text('{"sequence":0,"signature":"0"}\n', encoding="utf-8")
    (state / "current.json").write_text('{"sequence":0,"signature":"0"}', encoding="utf-8")

    with pytest.raises(Exception, match="high-water"):
        LabJobReader(
            store.path, highwater_observer=observer, production_mode=True
        ).audit_integrity()


def test_external_highwater_rejects_rollback_after_process_restart(tmp_path: Path) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 1)
    root = tmp_path / "integrity-anchor"
    observer = _highwater_observer(root)
    first = LabJobReader(
        store.path, highwater_observer=observer, production_mode=True
    ).audit_integrity()
    lease = LabJobReader(store.path).list_leases()[-1]
    store.apply_command(
        _submit(job_id=UUID(int=10_006)),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    LabJobReader(store.path, highwater_observer=observer, production_mode=True).audit_integrity()
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "DELETE FROM lab_ledger_chain_entry WHERE chain_generation > ?",
            (first.chain_generation,),
        )
        connection.execute(
            "UPDATE lab_ledger_chain SET chain_generation = ?, head_hash = ? WHERE singleton = 1",
            (first.chain_generation, first.chain_head_hash),
        )
        connection.execute(
            "UPDATE lab_ledger_epoch SET mutation_epoch = ? WHERE singleton = 1",
            (first.mutation_epoch,),
        )
    script = """
from pathlib import Path
from rquant.lab_highwater_authority import LabHighWaterAuthorityClient, LabHighWaterAuthorityConfig
from rquant.lab_jobs import LabJobReader
import sys
root = Path(sys.argv[2])
public_key = (root / 'key-material' / 'anchor-v1.public.pem').read_bytes()
observer = LabHighWaterAuthorityClient(LabHighWaterAuthorityConfig(
    command=(
        sys.executable, sys.argv[3], '--state-root', str(root / 'state'),
        '--keys-file', str(root / 'keys.json'),
    ),
    stable_identity='strategy-lab-test-ledger', code_identity='1' * 40,
    profile_identity='2' * 64,
    trusted_key_provider=lambda key_id: public_key if key_id == 'anchor-v1' else None,
    active_key_id='anchor-v1',
))
try:
    LabJobReader(
        Path(sys.argv[1]), highwater_observer=observer, production_mode=True,
    ).audit_integrity()
except Exception:
    raise SystemExit(2)
raise SystemExit(0)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(store.path),
            str(root),
            str(
                Path(__file__).resolve().parents[2]
                / "deploy"
                / "libexec"
                / "rquant-lab-highwater-authority"
            ),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )
    assert result.returncode == 2


def test_external_highwater_requires_explicit_credential_in_production(
    tmp_path: Path,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 1)
    with pytest.raises(ValueError, match="external high-water authority"):
        LabJobReader(store.path, production_mode=True)


def test_external_highwater_recovers_current_and_supports_trusted_key_rotation(
    tmp_path: Path,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 1)
    root = tmp_path / "integrity-anchor"
    old_observer = _highwater_observer(root, key_id="anchor-old")
    first = LabJobReader(
        store.path, highwater_observer=old_observer, production_mode=True
    ).audit_integrity()
    (_highwater_state(root) / "current.json").unlink()

    key_ids = {"anchor-old", "anchor-new"}
    keys = _highwater_public_keys(root, key_ids)
    rotated = _highwater_observer(
        root, key_id="anchor-new", authority_key_ids=key_ids, trusted_keys=keys
    )
    assert (
        LabJobReader(store.path, highwater_observer=rotated, production_mode=True).audit_integrity()
        == first
    )
    assert (_highwater_state(root) / "current.json").exists()

    untrusted = _highwater_observer(
        root,
        key_id="anchor-new",
        authority_key_ids=key_ids,
        trusted_keys={"anchor-old": keys["anchor-old"]},
    )
    with pytest.raises(Exception, match="signing key is not trusted"):
        LabJobReader(
            store.path, highwater_observer=untrusted, production_mode=True
        ).audit_integrity()


def test_external_highwater_requires_explicit_profile_identity_rotation(
    tmp_path: Path,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 1)
    root = tmp_path / "integrity-anchor"
    initial = _highwater_observer(root)
    LabJobReader(store.path, highwater_observer=initial, production_mode=True).audit_integrity()
    blocked = _highwater_observer(root, code_identity="3" * 40, profile_identity="4" * 64)
    with pytest.raises(Exception, match="code or profile identity conflicts"):
        LabJobReader(store.path, highwater_observer=blocked, production_mode=True).audit_integrity()

    rotated = _highwater_observer(
        root, code_identity="3" * 40, profile_identity="4" * 64, allow_identity_rotation=True
    )
    LabJobReader(store.path, highwater_observer=rotated, production_mode=True).audit_integrity()


def test_incremental_audit_detects_same_inode_epoch_rollback_from_chain_tail(
    tmp_path: Path,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 2)
    reader = LabJobReader(store.path)
    first = reader.audit_incremental(max_chain_entries=3)
    assert isinstance(first, LabIncrementalIntegrityReceipt)
    assert 1 <= first.checked_chain_entries <= 4
    lease = reader.list_leases()[-1]
    store.apply_command(
        _submit(job_id=UUID(int=10_003)),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE lab_ledger_epoch SET mutation_epoch = ? WHERE singleton = 1",
            (first.mutation_epoch,),
        )

    with pytest.raises(InvalidStoredJobError, match="mutation epoch"):
        reader.audit_incremental(max_chain_entries=3)


def test_incremental_audit_rejects_same_inode_chain_and_epoch_rollback(
    tmp_path: Path,
) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 2)
    reader = LabJobReader(store.path)
    initial = reader.audit_incremental(max_chain_entries=3)
    lease = reader.list_leases()[-1]
    store.apply_command(
        _submit(job_id=UUID(int=10_004)),
        lease=lease,
        now=NOW + timedelta(seconds=10),
    )
    advanced = reader.audit_incremental(max_chain_entries=3)
    assert advanced.chain_generation > initial.chain_generation

    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "DELETE FROM lab_ledger_chain_entry WHERE chain_generation > ?",
            (initial.chain_generation,),
        )
        connection.execute(
            "UPDATE lab_ledger_chain SET chain_generation = ?, head_hash = ? WHERE singleton = 1",
            (initial.chain_generation, initial.chain_head_hash),
        )
        connection.execute(
            "UPDATE lab_ledger_epoch SET mutation_epoch = ? WHERE singleton = 1",
            (initial.mutation_epoch,),
        )

    with pytest.raises(InvalidStoredJobError, match="rolled back"):
        reader.audit_incremental(max_chain_entries=3)


def test_integrity_audit_detects_tampered_historical_chain_entry(tmp_path: Path) -> None:
    store, _job_ids = _seed_jobs(tmp_path, 2)
    reader = LabJobReader(store.path)
    reader.audit_integrity()
    with sqlite3.connect(store.path) as connection:
        generation = connection.execute(
            "SELECT chain_generation FROM lab_ledger_chain WHERE singleton = 1"
        ).fetchone()[0]
        assert generation > 0
        connection.execute(
            "UPDATE lab_ledger_chain_entry SET entry_hash = ? WHERE chain_generation = ?",
            ("0" * 64, generation - 1),
        )

    with pytest.raises(InvalidStoredJobError, match="chain"):
        reader.audit_integrity()


def test_unfiltered_job_page_vm_steps_remain_bounded_as_history_grows(tmp_path: Path) -> None:
    observed: list[int] = []
    for count in (1, 257, 1024):
        history = tmp_path / f"history-{count}"
        store, _job_ids = _seed_jobs(history, count)
        reader = _VmStepReader(store.path)

        page = reader.list_jobs(limit=1)

        assert len(page.items) == 1
        observed.append(reader.vm_steps)

    one, medium, large = observed
    assert medium <= one * 4 + 1_000
    assert large <= one * 4 + 1_000


def test_filtered_job_page_omits_global_total_and_stays_bounded(tmp_path: Path) -> None:
    observed: list[int] = []
    for count in (1, 257, 1024):
        history = tmp_path / f"filtered-history-{count}"
        store, _job_ids = _seed_jobs(history, count)
        reader = _VmStepReader(store.path)

        page = reader.list_jobs(filters=LabJobListFilters(keyword="strategy"), limit=1)

        assert len(page.items) == 1
        assert page.total_count is None
        observed.append(reader.vm_steps)

    one, medium, large = observed
    assert medium <= one * 4 + 1_000
    assert large <= one * 4 + 1_000


def test_finalization_candidate_page_vm_steps_remain_bounded_as_history_grows(
    tmp_path: Path,
) -> None:
    observed: list[int] = []
    for count in (1, 257, 1024):
        root = tmp_path / f"candidate-history-{count}"
        root.mkdir(mode=0o700)
        scenario = _ready_scenario(root, hold_days=(1,))
        _append_seed_jobs(scenario.store, count, start=20_000)
        reader = _VmStepReader(scenario.store.path)

        page = reader.list_finalization_candidates(limit=1)

        assert len(page.items) == 1
        observed.append(reader.vm_steps)

    one, medium, large = observed
    assert medium <= one * 4 + 2_000
    assert large <= one * 4 + 2_000


def test_graph_validation_cache_is_invalidated_by_ledger_epoch(tmp_path: Path) -> None:
    store, job_ids = _seed_jobs(tmp_path, 2)
    reader = LabJobReader(store.path)

    first = reader.audit_integrity()
    assert reader.audit_integrity() == first
    assert reader.graph_validation_runs == 1

    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
            (
                '{"parameters":{"strategy_name":"first","strategy_name":7}}',
                str(job_ids[0]),
            ),
        )

    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.audit_integrity()
    assert reader.graph_validation_runs == 2


def test_graph_validation_cache_is_invalidated_by_database_file_generation(
    tmp_path: Path,
) -> None:
    store, job_ids = _seed_jobs(tmp_path, 2)
    reader = LabJobReader(store.path)

    reader.audit_integrity()
    assert reader.graph_validation_runs == 1
    with sqlite3.connect(store.path) as connection:
        epoch = int(
            connection.execute(
                "SELECT mutation_epoch FROM lab_ledger_epoch WHERE singleton = 1"
            ).fetchone()[0]
        )

    replacement = store.path.with_suffix(".replacement.sqlite3")
    with sqlite3.connect(store.path) as source, sqlite3.connect(replacement) as target:
        source.backup(target)
    with sqlite3.connect(replacement) as connection:
        connection.execute("PRAGMA journal_mode = DELETE")
        _register_unprivileged_job_functions(connection)
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
            (
                '{"parameters":{"strategy_name":"first","strategy_name":7}}',
                str(job_ids[0]),
            ),
        )
        connection.execute(
            "UPDATE lab_ledger_epoch SET mutation_epoch = ? WHERE singleton = 1",
            (epoch,),
        )
    for suffix in ("-wal", "-shm"):
        Path(f"{store.path}{suffix}").unlink(missing_ok=True)
    os.replace(replacement, store.path)

    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.audit_integrity()
    assert reader.graph_validation_runs == 2


def test_list_jobs_immutable_cursor_survives_updates_and_live_insert(
    tmp_path: Path,
) -> None:
    store, initial_ids = _seed_jobs(tmp_path, 25)
    reader = LabJobReader(store.path)
    first = reader.list_jobs(limit=7)
    assert first.next_cursor is not None
    assert first.total_count == 25

    leases = reader.list_leases()
    assert len(leases) == 1
    lease = leases[0]
    unseen_id = initial_ids[0]
    unseen = reader.get_job(unseen_id)
    assert unseen is not None
    cancelled = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=CancelJobCommand(
                job_id=unseen_id,
                expected_version=unseen.version,
                reason="concurrent status update",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=200),
    )
    inserted_id = UUID(int=10_000)
    inserted = store.apply_command(
        _submit(job_id=inserted_id, spec=_spec()),
        lease=lease,
        now=NOW + timedelta(seconds=201),
    )
    assert cancelled.status == inserted.status == "applied"

    observed = [item.job_id for item in first.items]
    cursor = first.next_cursor
    live_totals: list[int] = []
    while cursor is not None:
        page = reader.list_jobs(limit=7, cursor=cursor)
        observed.extend(item.job_id for item in page.items)
        live_totals.append(page.total_count)
        assert page.has_more is (page.next_cursor is not None)
        cursor = page.next_cursor

    assert observed == list(reversed(initial_ids))
    assert len(observed) == len(set(observed)) == len(initial_ids)
    assert inserted_id not in observed
    assert live_totals and set(live_totals) == {26}


def test_list_jobs_cursor_is_versioned_and_bound_to_filter_identity(tmp_path: Path) -> None:
    store, _ = _seed_jobs(tmp_path, 4)
    reader = LabJobReader(store.path)
    queued_filter = LabJobListFilters(statuses=(JobStatus.QUEUED,))
    first = reader.list_jobs(filters=queued_filter, limit=2)
    assert first.next_cursor is not None

    padding = "=" * (-len(first.next_cursor) % 4)
    payload = json.loads(urlsafe_b64decode(f"{first.next_cursor}{padding}"))
    assert payload["cursor_type"] == "lab_job_list"
    assert payload["schema_version"] == 1
    assert payload["filter_identity"]

    with pytest.raises(ValueError, match="cursor.*filter"):
        reader.list_jobs(
            filters=LabJobListFilters(job_types=(ResearchJobType.PARAMETER_SEARCH,)),
            limit=2,
            cursor=first.next_cursor,
        )

    payload["schema_version"] = 2
    unsupported = (
        urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
        )
        .decode("ascii")
        .rstrip("=")
    )
    with pytest.raises(ValueError, match="cursor"):
        reader.list_jobs(filters=queued_filter, limit=2, cursor=unsupported)


def test_list_jobs_combines_status_type_resource_date_and_keyword_filters(
    tmp_path: Path,
) -> None:
    store, _ = _seed_jobs(tmp_path, 12)
    reader = LabJobReader(store.path)

    page = reader.list_jobs(
        filters=LabJobListFilters(
            statuses=(JobStatus.QUEUED,),
            job_types=(ResearchJobType.PARAMETER_SEARCH,),
            resource_classes=(ResourceClass.STANDARD,),
            created_from=NOW + timedelta(seconds=1),
            created_before=NOW + timedelta(seconds=11),
            keyword="strategy-007",
        ),
        limit=10,
    )

    assert page.total_count is None
    assert tuple(item.strategy_name for item in page.items) == ("strategy-007",)
    with pytest.raises(ValueError, match="cursor"):
        reader.list_jobs(limit=10, cursor="not-an-opaque-cursor")
    with pytest.raises(ValueError, match="limit"):
        reader.list_jobs(limit=101)


@pytest.mark.parametrize(
    "corrupt_spec",
    (
        '{"parameters":{"strategy_name":"excluded","strategy_name":"needle"}}',
        '{ "parameters":{"strategy_name":"needle"}}',
        '{"parameters":{"strategy_name":7}}',
    ),
)
def test_list_jobs_keyword_fails_closed_before_filtering_corrupt_specs(
    tmp_path: Path,
    corrupt_spec: str,
) -> None:
    store, job_ids = _seed_jobs(tmp_path, 2)
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
            (corrupt_spec, str(job_ids[0])),
        )

    reader = LabJobReader(store.path)
    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.list_jobs(filters=LabJobListFilters(keyword="needle"), limit=1)


def test_list_jobs_keyword_rejects_corrupt_row_beyond_first_page_and_cursor(
    tmp_path: Path,
) -> None:
    store, job_ids = _seed_jobs(tmp_path, 4)
    reader = LabJobReader(store.path)
    filters = LabJobListFilters(keyword="strategy")
    first = reader.list_jobs(filters=filters, limit=1)
    assert first.next_cursor is not None
    valid = reader.get_job(job_ids[0])
    assert valid is not None
    noncanonical = json.dumps(valid.spec.model_dump(mode="json"), indent=2, default=str)
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
            (noncanonical, str(job_ids[0])),
        )

    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.list_jobs(
            filters=filters,
            limit=1,
            cursor=first.next_cursor,
        )


@pytest.mark.parametrize(
    ("filters", "corrupt_index"),
    (
        (LabJobListFilters(statuses=(JobStatus.QUEUED,)), 0),
        (LabJobListFilters(job_types=(ResearchJobType.PARAMETER_SEARCH,)), 0),
        (LabJobListFilters(resource_classes=(ResourceClass.STANDARD,)), 0),
        (LabJobListFilters(created_from=NOW + timedelta(seconds=1)), 0),
        (LabJobListFilters(created_before=NOW + timedelta(seconds=1)), 1),
    ),
)
def test_list_jobs_bounds_validation_to_visible_rows_and_explicit_audit_finds_hidden_job(
    tmp_path: Path,
    filters: LabJobListFilters,
    corrupt_index: int,
) -> None:
    store, job_ids = _seed_jobs(tmp_path, 2)
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute("PRAGMA ignore_check_constraints = ON")
        if filters.statuses:
            connection.execute(
                "UPDATE lab_job SET status = 'cancelled' WHERE job_id = ?",
                (str(job_ids[corrupt_index]),),
            )
        connection.execute(
            "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
            (
                '{"parameters":{"strategy_name":"hidden","strategy_name":7}}',
                str(job_ids[corrupt_index]),
            ),
        )

    reader = LabJobReader(store.path)
    page = reader.list_jobs(filters=filters, limit=1)

    assert all(item.job_id != job_ids[corrupt_index] for item in page.items)
    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.audit_integrity()


def test_explicit_audit_validates_shard_evidence_hidden_by_job_filter(tmp_path: Path) -> None:
    store, _lease_value, _job_id = _setup(tmp_path)
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_shard SET failure_json = ?",
            ('{"reason":"first","reason":"second"}',),
        )

    reader = LabJobReader(store.path)
    page = reader.list_jobs(
        filters=LabJobListFilters(statuses=(JobStatus.SUCCEEDED,)),
        limit=1,
    )

    assert not page.items
    with pytest.raises(InvalidStoredJobError, match="shard|duplicate"):
        reader.audit_integrity()


def test_job_list_filters_canonicalize_enum_tuples_and_bound_sql_parameters() -> None:
    filters = LabJobListFilters(
        statuses=tuple(reversed(tuple(JobStatus))) + tuple(JobStatus),
        job_types=tuple(reversed(tuple(ResearchJobType))) + tuple(ResearchJobType),
        resource_classes=tuple(reversed(tuple(ResourceClass))) + tuple(ResourceClass),
        created_from=NOW,
        created_before=NOW + timedelta(days=1),
        keyword="strategy",
    )

    assert filters.statuses == tuple(sorted(JobStatus, key=lambda item: item.value))
    assert filters.job_types == tuple(sorted(ResearchJobType, key=lambda item: item.value))
    assert filters.resource_classes == tuple(sorted(ResourceClass, key=lambda item: item.value))
    _, parameters = LabJobReader._job_filters_sql(filters)
    assert len(parameters) == LAB_JOB_LIST_FILTER_SQL_PARAMETER_MAX
    assert len(parameters) + 4 == LAB_JOB_LIST_QUERY_PARAMETER_MAX
    assert LabJobReader._job_filters_sql(LabJobListFilters()) == ([], [])


def test_job_list_filters_reject_pathological_raw_tuple_before_sql() -> None:
    with pytest.raises(ValidationError, match="statuses"):
        LabJobListFilters(statuses=(JobStatus.QUEUED,) * 100_000)


def test_job_detail_is_bounded_and_reports_first_failure_without_paused_eta(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=5, max_attempts=3, with_work_plan=True)
    claim = _claim(store, lease)
    failed = store.apply_worker_report(
        _report(
            claim,
            LabShardFailed(failure_json='{"kind":"first"}'),
            offset=4,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert failed.status == "accepted"
    reader = _CountingReader(store.path)

    detail = reader.get_job_detail(
        job_id,
        as_of=NOW + timedelta(seconds=20),
        shard_limit=2,
        event_limit=2,
        artifact_limit=1,
    )

    assert detail is not None
    assert detail.job.status is JobStatus.FAILED
    assert len(detail.shards) == 2
    assert detail.shard_count == 5
    assert detail.shards_truncated is True
    assert len(detail.events) == 2
    assert detail.events_truncated is True
    assert detail.first_failure is not None
    assert detail.first_failure.failure.failure_json == '{"kind":"first"}'
    assert detail.eta is not None and detail.eta.finish_at is None
    assert detail.command_availability.retry is True
    selects = [
        statement for statement in reader.statements if statement.startswith(("SELECT", "WITH"))
    ]
    assert len(selects) <= 13


def test_job_detail_marks_running_heartbeat_stale_and_truncates_independently(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=3, with_work_plan=True)
    _claim(store, lease, duration=120)
    reader = _CountingReader(store.path)

    detail = reader.get_job_detail(
        job_id,
        as_of=NOW + timedelta(seconds=40),
        heartbeat_stale_after=timedelta(seconds=10),
        shard_limit=1,
        event_limit=10,
    )

    assert detail is not None
    assert detail.heartbeat.active_shards == 1
    assert detail.heartbeat.stale is True
    assert detail.progress.phase == "strategy_replay"
    assert detail.shards_truncated is True
    assert any(f"LIMIT {MAX_JOB_SHARDS + 1}" in statement for statement in reader.statements)


def test_eta_and_detail_fail_closed_on_damaged_oversized_remaining_shard_graph(
    tmp_path: Path,
) -> None:
    store, job_ids = _seed_jobs(tmp_path, 1)
    timestamp = NOW.isoformat(timespec="microseconds")
    with sqlite3.connect(store.path) as connection:
        connection.executemany(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version,
                attempt_count, max_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'queued', 0, 0, 3, ?, ?)
            """,
            (
                (
                    str(UUID(int=10_000 + index)),
                    str(job_ids[0]),
                    index,
                    timestamp,
                    timestamp,
                )
                for index in range(MAX_JOB_SHARDS + 1)
            ),
        )

    eta_reader = _CountingReader(store.path)
    with pytest.raises(InvalidStoredJobError, match="shard limit"):
        eta_reader.get_eta_input(job_ids[0], as_of=NOW)
    eta_selects = [
        statement for statement in eta_reader.statements if statement.startswith("SELECT")
    ]
    assert len(eta_selects) == 2
    assert any(f"LIMIT {MAX_JOB_SHARDS + 1}" in statement for statement in eta_selects)
    assert not any("completion_sequence FROM lab_shard" in statement for statement in eta_selects)
    with pytest.raises(InvalidStoredJobError, match="shard limit"):
        eta_reader.list_shards(job_ids[0])

    detail_reader = _CountingReader(store.path)
    with pytest.raises(InvalidStoredJobError, match="shard limit"):
        detail_reader.get_job_detail(
            job_ids[0],
            as_of=NOW,
            shard_limit=1,
            event_limit=1,
            artifact_limit=1,
        )
    detail_selects = [
        statement
        for statement in detail_reader.statements
        if statement.startswith(("SELECT", "WITH"))
    ]
    assert len(detail_selects) <= 13


def test_list_finalization_candidates_is_typed_readonly_and_bounded(tmp_path: Path) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    reader = _CountingReader(scenario.store.path)

    page = reader.list_finalization_candidates(limit=1)

    assert tuple(item.job_id for item in page.items) == (scenario.job_id,)
    assert page.has_more is False
    assert reader.statements.count("BEGIN") == 1
    assert reader.statements.count("COMMIT") == 1
    reader.execute_for_test("SELECT 1")
    with pytest.raises(Exception, match="readonly|read-only|query_only"):
        reader.execute_for_test("DELETE FROM lab_job")


@pytest.mark.parametrize("reader_method", ("list_jobs", "list_finalization_candidates"))
def test_reader_pages_reject_ready_job_with_incomplete_result_graph(
    tmp_path: Path,
    reader_method: str,
) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    with sqlite3.connect(scenario.store.path) as connection:
        _register_unprivileged_job_functions(connection)
        trigger_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'trg_lab_complete_result_shard_no_update'"
        ).fetchone()
        assert trigger_row is not None and trigger_row[0] is not None
        connection.execute("DROP TRIGGER trg_lab_complete_result_shard_no_update")
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_shard SET status = 'queued' WHERE job_id = ?",
            (str(scenario.job_id),),
        )
        connection.execute(str(trigger_row[0]))

    reader = LabJobReader(scenario.store.path)
    with pytest.raises(InvalidStoredJobError, match="ready|succeeded|shard"):
        getattr(reader, reader_method)(limit=1)
