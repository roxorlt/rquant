from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Event, Thread, get_ident
from uuid import UUID, uuid4

import pandas as pd
import pytest
from pydantic import ValidationError

import rquant.lab_jobs as lab_jobs
from rquant.artifact_retention import ArtifactReferenceStore
from rquant.job_center_authority import (
    install_job_center_authority,
    publish_job_center_authority_candidate,
)
from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommit,
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
    LabFinalizerAuthorityAuthenticationError,
    LabFinalizerAuthorityClaims,
    LabFinalizerAuthorityKey,
    LabFinalizerAuthorityShardEvidence,
    sign_finalizer_authority,
)
from rquant.lab_artifacts import (
    LabArtifactIndexEvidence,
    LabArtifactIntegrityError,
    LabJobArtifactStore,
    LabSealedJobArtifact,
)
from rquant.lab_daemon import (
    LabDaemonConfigurationError,
    prepare_private_sqlite_path,
)
from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabAcknowledgedCommand,
    LabCommandEnvelope,
    LabCommandReceipt,
    LabCommandSpool,
    LabSpoolEntry,
    LabSpoolFileIdentity,
    PauseJobCommand,
    RequestContentConflictError,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    COMPLETE_RESULT_CONTRACT_VERSION,
    ArtifactCommitDeadlineExpiredError,
    InvalidStoredJobError,
    JobStatus,
    LabIntegrityDegradedError,
    LabJobReader,
    LabJobRecord,
    LabJobStore,
    LabResultState,
    SchedulerLeaseFencedError,
    SchedulerLeaseUnavailableError,
)
from rquant.lab_scheduler import LabFullIntegrityAuditStateStore, LabScheduler, SchedulerTickResult
from rquant.lab_shard_protocol import LabShardSucceeded, LabWorkerReport
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchJobType,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)
from rquant.strict_json import canonical_model_json_bytes

from .test_lab_shard_control_plane import PLAN_HASH, _definition

NOW = datetime(2026, 7, 24, 1, 0, tzinfo=UTC)
AUTHORITY_KEY = LabFinalizerAuthorityKey(key_id="scheduler-test-key", secret=b"a" * 32)


def _authority_key_provider() -> LabFinalizerAuthorityKey:
    return AUTHORITY_KEY


def _authority_verification_key_provider(
    key_id: str,
) -> LabFinalizerAuthorityKey | None:
    return AUTHORITY_KEY if key_id == AUTHORITY_KEY.key_id else None


def _signed_artifact_envelope(
    store: LabJobStore,
    commit: LabArtifactCommit,
    *,
    request_id: UUID | None = None,
    key: LabFinalizerAuthorityKey = AUTHORITY_KEY,
) -> LabArtifactCommitEnvelope:
    resolved_request_id = request_id or uuid4()
    snapshot = LabJobReader(store.path).get_finalization_snapshot(commit.job_id)
    assert snapshot is not None
    claims = LabFinalizerAuthorityClaims(
        request_id=resolved_request_id,
        commit_content_hash=hashlib.sha256(commit.canonical_json_bytes()).hexdigest(),
        job_id=commit.job_id,
        ready_event_id=snapshot.ready_epoch.event.event_id,
        ready_job_version=snapshot.ready_epoch.job_version,
        scheduler_fencing_token=snapshot.ready_epoch.event.scheduler_fencing_token or 0,
        spec_hash=snapshot.job.spec_hash,
        finalizer_code_sha=snapshot.job.spec.code_sha,
        shards=tuple(
            LabFinalizerAuthorityShardEvidence(
                shard_index=item.shard.shard_index,
                shard_id=item.shard.shard_id,
                payload_hash=item.shard.payload_hash,
                plan_hash=item.shard.plan_hash,
                result_manifest_hash=item.shard.result_manifest_hash or "",
                accepted_report_content_hash=item.accepted_success.report.content_hash,
                claim_token=item.accepted_success.report.claim_token,
                claim_generation=item.accepted_success.report.claim_generation,
                scheduler_fencing_token=(item.accepted_success.report.scheduler_fencing_token),
            )
            for item in snapshot.shards
        ),
        artifact_manifest_hash=commit.manifest_hash,
        complete_result_hash=commit.complete_result_hash,
    )
    proof = sign_finalizer_authority(claims, key_provider=lambda: key)
    return LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=resolved_request_id,
        commit=commit,
        authority_proof=proof,
    )


def _canonical_json(
    model: LabArtifactCommitEnvelope | LabArtifactCommitReceipt | LabArtifactIndexEvidence,
) -> str:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _spec(
    *,
    deadline: datetime | None = None,
    with_dataset_snapshot: bool = True,
    dataset_audit_run_id: str | None = "d" * 64,
) -> ResearchRunSpec:
    dataset_snapshot = (
        DatasetSnapshotIdentity(
            snapshot_id="a" * 64,
            binding_hash="b" * 64,
            audit_run_id=dataset_audit_run_id,
        )
        if with_dataset_snapshot
        else None
    )
    return ResearchRunSpec(
        job_type=ResearchJobType.STRATEGY_REPLAY,
        parameters=ResearchRunParameters(
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 14),
        ),
        code_sha="1" * 40,
        dataset_snapshot=dataset_snapshot,
        feature_contract=FeatureContractIdentity(
            contract_id="intraday-core",
            contract_version="v1",
            contract_hash="c" * 64,
        ),
        execution_costs=ExecutionCostSpec(
            commission_bps=Decimal("2.5"),
            stamp_duty_bps=Decimal("5"),
            transfer_fee_bps=Decimal("0.1"),
            slippage_bps=Decimal("3"),
        ),
        random_seed=20260724,
        resource_class=ResourceClass.STANDARD,
        deadline=deadline or datetime(2026, 7, 25, 2, tzinfo=UTC),
        research_status="exploratory",
    )


def _envelope(*, spec: ResearchRunSpec | None = None) -> LabCommandEnvelope:
    return LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(job_id=uuid4(), spec=spec or _spec(), max_attempts=3),
    )


def _components(tmp_path: Path) -> tuple[LabJobStore, LabCommandSpool]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    return store, LabCommandSpool(tmp_path / "commands")


def _scheduler(
    store: LabJobStore,
    spool: LabCommandSpool,
    *,
    owner: str = "scheduler-a",
    now: datetime = NOW,
    batch_size: int = 32,
) -> LabScheduler:
    return LabScheduler(
        store=store,
        spool=spool,
        owner_id=owner,
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        max_commands_per_tick=batch_size,
        clock=lambda: now,
    )


def test_scheduler_tick_result_preclaim_blocked_defaults_to_zero() -> None:
    result = SchedulerTickResult(
        lease_acquired=False,
        processed=0,
        applied=0,
        rejected=0,
        quarantined=0,
        recovered=0,
    )

    assert result.preclaim_blocked == 0


def test_scheduler_tick_does_not_run_maintenance_recovery_before_claim_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)

    def reject_hidden_maintenance(*_args: object, **_kwargs: object) -> tuple[UUID, ...]:
        raise AssertionError("maintenance recovery must not precede caller claim routing")

    monkeypatch.setattr(store, "recover_stale_shards", reject_hidden_maintenance)

    result = scheduler.run_once()

    assert result.lease_acquired is True
    assert result.recovered == 0


def test_scheduler_runs_full_integrity_audit_on_persistent_due_cadence(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    observed = tmp_path / "full-audit-pids.log"
    audit_code = (
        "import os\n"
        "from pathlib import Path\n"
        f"Path({str(observed)!r}).open('a').write(str(os.getpid()) + '\\n')\n"
        f'print(\'{{"receipt_hash":"{"a" * 64}"}}\')'
    )
    command = (
        sys.executable,
        "-c",
        audit_code,
    )

    class _Auditor:
        def audit_incremental(self, *, max_chain_entries: int) -> object:
            return object()

    current = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        max_commands_per_tick=1,
        integrity_auditor=_Auditor(),
        full_integrity_command=command,
        full_integrity_state_store=LabFullIntegrityAuditStateStore(tmp_path / "audit-state.json"),
        full_integrity_interval_seconds=60,
        full_integrity_budget_seconds=1,
        clock=lambda: current[0],
    )

    scheduler.run_once()
    current[0] += timedelta(seconds=30)
    scheduler.run_once()
    current[0] += timedelta(seconds=31)
    scheduler.run_once()

    pids = observed.read_text(encoding="ascii").splitlines()
    assert len(pids) == 2
    assert all(pid != str(os.getpid()) for pid in pids)


def test_scheduler_full_integrity_timeout_persists_degraded_health(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)

    class _SlowAuditor:
        def audit_incremental(self, *, max_chain_entries: int) -> object:
            return object()

    state_store = LabFullIntegrityAuditStateStore(tmp_path / "audit-state.json")
    authority_fences: list[str] = []
    timeout_code = f'import time\ntime.sleep(0.05)\nprint(\'{{"receipt_hash":"{"a" * 64}"}}\')'
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        max_commands_per_tick=1,
        integrity_auditor=_SlowAuditor(),
        full_integrity_command=(
            sys.executable,
            "-c",
            timeout_code,
        ),
        full_integrity_state_store=state_store,
        full_integrity_interval_seconds=60,
        full_integrity_budget_seconds=0.01,
        full_integrity_degradation_reporter=authority_fences.append,
        clock=lambda: NOW,
    )

    with pytest.raises(LabIntegrityDegradedError, match="resource budget"):
        scheduler.run_once()
    persisted = state_store.load()
    assert persisted is not None
    assert persisted.degraded_reason == "full ledger audit exceeded its resource budget"
    assert authority_fences == ["full ledger audit exceeded its resource budget"]


def test_scheduler_full_integrity_degraded_state_blocks_until_controlled_remediation(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)

    class _IncrementalAuditor:
        def audit_incremental(self, *, max_chain_entries: int) -> object:
            return object()

    state_store = LabFullIntegrityAuditStateStore(tmp_path / "audit-state.json")
    slow_code = f'import time\ntime.sleep(0.5)\nprint(\'{{"receipt_hash":"{"b" * 64}"}}\')'
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        max_commands_per_tick=1,
        integrity_auditor=_IncrementalAuditor(),
        full_integrity_command=(
            sys.executable,
            "-c",
            slow_code,
        ),
        full_integrity_state_store=state_store,
        full_integrity_interval_seconds=60,
        full_integrity_budget_seconds=0.2,
        full_integrity_remediation_authorizer=lambda: None,
        clock=lambda: NOW,
    )

    with pytest.raises(LabIntegrityDegradedError, match="resource budget"):
        scheduler.run_once()
    with pytest.raises(LabIntegrityDegradedError, match="remains degraded"):
        scheduler.run_once()
    scheduler.full_integrity_command = (
        sys.executable,
        "-c",
        'print(\'{"receipt_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}\')',
    )
    scheduler.full_integrity_budget_seconds = 1
    scheduler.remediate_full_integrity()

    repaired = state_store.load()
    assert repaired is not None
    assert repaired.degraded_reason is None
    assert repaired.receipt_hash == "b" * 64


def test_scheduler_recovers_lifecycle_once_and_synchronizes_each_mutated_job(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    envelope = _envelope()
    spool.publish(envelope)

    class _LifecycleSpy:
        def __init__(self) -> None:
            self.recoveries: list[datetime] = []
            self.synchronized: list[tuple[UUID, datetime]] = []

        def recover(self, *, observed_at: datetime) -> None:
            self.recoveries.append(observed_at)

        def synchronize(self, job_id: UUID, *, observed_at: datetime) -> None:
            self.synchronized.append((job_id, observed_at))

    lifecycle = _LifecycleSpy()
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        lifecycle_synchronizer=lifecycle,
        clock=lambda: NOW,
    )

    first = scheduler.run_once()
    scheduler.run_once()

    assert first.applied == 1
    assert lifecycle.recoveries == [NOW]
    assert lifecycle.synchronized == [(envelope.command.job_id, NOW)]


def test_scheduler_runs_bounded_incremental_audit_before_and_after_mutations(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    spool.publish(_envelope())

    class _Auditor:
        def __init__(self) -> None:
            self.max_chain_entries: list[int] = []

        def audit_incremental(self, *, max_chain_entries: int) -> object:
            self.max_chain_entries.append(max_chain_entries)
            return object()

    auditor = _Auditor()
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        integrity_auditor=auditor,
        max_integrity_chain_entries=7,
        clock=lambda: NOW,
    )

    result = scheduler.run_once()

    assert result.applied == 1
    assert auditor.max_chain_entries == [7, 7]


def test_scheduler_default_incremental_auditor_validates_real_ledger_tail(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    spool.publish(_envelope())
    scheduler = _scheduler(store, spool)

    result = scheduler.run_once()
    receipt = LabJobReader(store.path).audit_incremental(max_chain_entries=8)

    assert result.applied == 1
    assert receipt.chain_generation > 0
    assert receipt.mutation_epoch > 0


def test_scheduler_fails_closed_when_incremental_audit_is_degraded(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    envelope = _envelope()
    spool.publish(envelope)

    class _FailingAuditor:
        def audit_incremental(self, *, max_chain_entries: int) -> object:
            assert max_chain_entries == 8
            raise InvalidStoredJobError("tampered chain tail")

    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        integrity_auditor=_FailingAuditor(),
        max_integrity_chain_entries=8,
        clock=lambda: NOW,
    )

    with pytest.raises(LabIntegrityDegradedError, match="scheduler_pre_tick"):
        scheduler.run_once()

    assert spool.pending()[0].envelope == envelope


def test_scheduler_synchronizes_deadline_terminal_transition(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    envelope = _envelope(spec=_spec(deadline=NOW))
    spool.publish(envelope)

    class _LifecycleSpy:
        def __init__(self) -> None:
            self.job_ids: list[UUID] = []

        def recover(self, *, observed_at: datetime) -> None:
            del observed_at

        def synchronize(self, job_id: UUID, *, observed_at: datetime) -> None:
            del observed_at
            self.job_ids.append(job_id)

    lifecycle = _LifecycleSpy()
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        lifecycle_synchronizer=lifecycle,
        clock=lambda: NOW,
    )

    result = scheduler.run_once()

    assert result.deadlines_expired == 1
    assert lifecycle.job_ids == [envelope.command.job_id, envelope.command.job_id]


def test_scheduler_fails_closed_on_formal_job_without_lifecycle_authority(
    tmp_path: Path,
) -> None:
    from .test_lab_job_center import _formal_v3_spec

    store, spool = _components(tmp_path)
    published = spool.publish(_envelope(spec=_formal_v3_spec()))
    scheduler = _scheduler(store, spool)

    with pytest.raises(RuntimeError, match="formal v3|lifecycle authority"):
        scheduler.run_once()

    assert published.path.exists()
    assert LabJobReader(store.path).get_job(published.envelope.command.job_id) is None


def test_scheduler_rejects_injected_v2_comparable_without_job_creation(
    tmp_path: Path,
) -> None:
    from .test_lab_jobs import _formal_v2_spec

    store, spool = _components(tmp_path)
    envelope = _envelope(spec=_formal_v2_spec())
    injected = spool.pending_dir / f"{1:020d}-{envelope.request_id}.json"
    injected.write_bytes(canonical_model_json_bytes(envelope))
    injected.chmod(0o600)

    result = _scheduler(store, spool).run_once()
    acknowledged = spool.find(envelope.request_id)

    assert result.rejected == 1
    assert result.applied == 0
    assert spool.pending() == ()
    assert LabJobReader(store.path).get_job(envelope.command.job_id) is None
    assert isinstance(acknowledged, LabAcknowledgedCommand)
    assert acknowledged.receipt.reason == "v2_formal_requires_exploratory_migration"


def test_scheduler_quarantines_missing_formal_plan_before_any_authority_write(
    tmp_path: Path,
) -> None:
    from rquant.experiment_registry import ExperimentRegistry

    from .test_job_center_authority import CODE_SHA, _publish_and_install
    from .test_lab_jobs import _formal_v3_spec

    _manifest_path, paths = _publish_and_install(tmp_path)
    store = LabJobStore(paths["lab_jobs_path"])
    spool = LabCommandSpool(paths["command_spool_path"])
    envelope = _envelope(spec=_formal_v3_spec())
    spool.publish(envelope)

    def build_scheduler(owner: str) -> LabScheduler:
        return LabScheduler(
            store=store,
            spool=spool,
            owner_id=owner,
            lease_seconds=60,
            heartbeat_seconds=10,
            poll_interval_ms=10,
            runtime_guard=lambda: CODE_SHA,
            clock=lambda: NOW,
        )

    first = build_scheduler("scheduler-a")
    result = first.run_once()
    first.release()
    restarted = build_scheduler("scheduler-b")
    replay = restarted.run_once()

    registry = ExperimentRegistry(
        paths["experiment_registry_path"],
        managed_trust_root=paths["runtime_root"],
    )
    assert result.quarantined == 1
    assert result.processed == 0
    assert replay.processed == 0
    assert spool.pending() == ()
    assert LabJobReader(store.path).get_job(envelope.command.job_id) is None
    assert registry.list_submission_intents(limit=10) == ()


def test_scheduler_quarantines_synthetic_v3_identity_before_any_authority_write(
    tmp_path: Path,
) -> None:
    from .test_job_center_authority import (
        CODE_SHA,
        DEPLOYMENT_GENERATION_HASH,
        DEPLOYMENT_PROFILE_ID,
        _private_directory,
    )
    from .test_lab_job_center import _internally_coherent_formal_authorities

    spec, registry, _definitions = _internally_coherent_formal_authorities(
        tmp_path,
        definition_fingerprint="f" * 64,
        registration_record_hash="e" * 64,
    )
    runtime_root = tmp_path / "research"
    jobs_path = runtime_root / "lab_jobs.sqlite3"
    store = LabJobStore(jobs_path)
    store.initialize()
    jobs_path.chmod(0o600)
    registry.path.chmod(0o600)
    command_path = _private_directory(runtime_root / "commands")
    artifact_path = _private_directory(runtime_root / "final-artifacts")
    dataset_path = runtime_root / "research_ro.duckdb"
    dataset_path.touch(mode=0o600)
    retention_root = _private_directory(runtime_root / "artifact-retention")
    references_path = retention_root / "references.sqlite3"
    ArtifactReferenceStore(references_path, managed_trust_root=retention_root)
    references_path.chmod(0o600)
    from rquant.artifact_retention_catalog_authority import (
        bootstrap_retention_catalog_authority,
    )

    catalog_authority = bootstrap_retention_catalog_authority(
        state_root=retention_root,
        reference_store_path=references_path,
        producer_commit=CODE_SHA,
    )
    staging = _private_directory(tmp_path / "staging")
    candidate = publish_job_center_authority_candidate(
        staging / "candidate.json",
        code_sha=CODE_SHA,
        deployment_profile_id=DEPLOYMENT_PROFILE_ID,
        deployment_generation_hash=DEPLOYMENT_GENERATION_HASH,
        runtime_deployment_root=tmp_path,
        runtime_root=runtime_root,
        lab_jobs_path=jobs_path,
        command_spool_path=command_path,
        final_artifact_root=artifact_path,
        definition_registry_root=tmp_path / "definitions",
        experiment_registry_path=registry.path,
        dataset_authority_path=dataset_path,
        catalog_authority_root=catalog_authority.root,
        catalog_authority_receipt_path=catalog_authority.current_receipt_path,
    )
    install_job_center_authority(
        candidate,
        target=runtime_root / "job-center-authority.json",
        expected_code_sha=CODE_SHA,
        expected_runtime_root=runtime_root,
        expected_runtime_deployment_root=tmp_path,
        expected_deployment_profile_id=DEPLOYMENT_PROFILE_ID,
        expected_deployment_generation_hash=DEPLOYMENT_GENERATION_HASH,
    )
    spool = LabCommandSpool(command_path)
    envelope = _envelope(spec=spec)
    spool.publish(envelope)
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        runtime_guard=lambda: CODE_SHA,
        clock=lambda: NOW,
    )

    result = scheduler.run_once()

    assert result.quarantined == 1
    assert result.processed == 0
    assert spool.pending() == ()
    assert LabJobReader(store.path).get_job(envelope.command.job_id) is None
    assert registry.list_submission_intents(limit=10) == ()
    assert registry.list_family_attempts(spec.experiment.hypothesis_family) == ()


def test_scheduler_auto_composes_lifecycle_from_private_authority_manifest(
    tmp_path: Path,
) -> None:
    from .test_job_center_authority import CODE_SHA, _publish_and_install

    _manifest_path, paths = _publish_and_install(tmp_path)
    store = LabJobStore(paths["lab_jobs_path"])
    spool = LabCommandSpool(paths["command_spool_path"])
    artifacts = LabJobArtifactStore(paths["final_artifact_root"])

    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=LabArtifactCommitSpool(paths["runtime_root"] / "artifact-commits"),
        artifact_store=artifacts,
        finalizer_authority_key_provider=_authority_verification_key_provider,
        runtime_guard=lambda: CODE_SHA,
        clock=lambda: NOW,
    )

    assert scheduler.lifecycle_synchronizer is not None


def test_production_scheduler_requires_installed_authority_before_start(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)

    with pytest.raises(LabDaemonConfigurationError, match="authority manifest"):
        LabScheduler(
            store=store,
            spool=spool,
            owner_id="scheduler-a",
            lease_seconds=60,
            heartbeat_seconds=10,
            poll_interval_ms=10,
            runtime_guard=lambda: "1" * 40,
            require_authority_manifest=True,
            clock=lambda: NOW,
        )


def test_production_scheduler_reloads_authority_generation_before_tick(
    tmp_path: Path,
) -> None:
    from .test_job_center_authority import CODE_SHA, _publish_and_install

    _manifest_path, paths = _publish_and_install(tmp_path)
    scheduler = LabScheduler(
        store=LabJobStore(paths["lab_jobs_path"]),
        spool=LabCommandSpool(paths["command_spool_path"]),
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        runtime_guard=lambda: CODE_SHA,
        require_authority_manifest=True,
        clock=lambda: NOW,
    )
    registry_path = paths["experiment_registry_path"]
    replacement = registry_path.with_suffix(".replacement")
    replacement.write_bytes(registry_path.read_bytes())
    replacement.chmod(0o600)
    os.replace(replacement, registry_path)

    with pytest.raises(LabDaemonConfigurationError, match="authority manifest"):
        scheduler.run_once()


def test_scheduler_runtime_drift_between_ticks_leaves_command_unacknowledged(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    drifted = False

    def runtime_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("runtime checkout drifted")
        return "1" * 40

    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        runtime_guard=runtime_guard,
        clock=lambda: NOW,
    )
    assert scheduler.run_once().processed == 0
    published = spool.publish(_envelope())
    drifted = True

    with pytest.raises(LabDaemonConfigurationError, match="drifted"):
        scheduler.run_once()

    assert published.path.exists()
    assert not (spool.ack_dir / f"{published.envelope.request_id}.json").exists()


def test_scheduler_runtime_drift_after_invalid_command_load_does_not_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    scheduler.run_once()
    published = spool.publish(_envelope())
    drifted = False
    original_load = spool.load

    def drift_after_load(path: Path) -> object:
        nonlocal drifted
        entry = original_load(path)
        drifted = True
        return entry

    def conflict_after_load(*_args: object, **_kwargs: object) -> object:
        raise RequestContentConflictError("injected content conflict")

    def runtime_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("runtime checkout drifted")
        return "1" * 40

    monkeypatch.setattr(spool, "load", drift_after_load)
    monkeypatch.setattr(store, "apply_command", conflict_after_load)
    scheduler.runtime_guard = runtime_guard

    with pytest.raises(LabDaemonConfigurationError, match="drifted"):
        scheduler.run_once()

    assert published.path.exists()
    assert tuple(spool.quarantine_dir.iterdir()) == ()


def test_scheduler_runtime_drift_during_invalid_command_load_does_not_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    scheduler.run_once()
    pending = spool.pending_dir / f"00000000000000000000-{uuid4()}.json"
    pending.write_text("{broken", encoding="utf-8")
    drifted = False
    original_load = spool.load

    def drift_during_load(path: Path) -> object:
        nonlocal drifted
        try:
            return original_load(path)
        finally:
            drifted = True

    def runtime_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("runtime checkout drifted")
        return "1" * 40

    monkeypatch.setattr(spool, "load", drift_during_load)
    scheduler.runtime_guard = runtime_guard

    with pytest.raises(LabDaemonConfigurationError, match="drifted"):
        scheduler.run_once()

    assert pending.exists()
    assert tuple(spool.quarantine_dir.iterdir()) == ()


def test_scheduler_sqlite_identity_drift_rolls_back_without_command_ack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    store = LabJobStore(path, identity_authority=authority)
    store.initialize()
    spool = LabCommandSpool(tmp_path / "commands")
    scheduler = _scheduler(store, spool)
    scheduler.run_once()
    published = spool.publish(_envelope())
    original = root / "original.sqlite3"
    replacement = root / "replacement.sqlite3"
    with sqlite3.connect(path) as source, sqlite3.connect(replacement) as target:
        source.backup(target)
    replacement.chmod(0o600)
    real_commit = lab_jobs._LabJobStoreConnection.commit
    swapped = False

    def swap_before_commit(connection: lab_jobs._LabJobStoreConnection) -> None:
        nonlocal swapped
        if not swapped:
            swapped = True
            path.rename(original)
            replacement.rename(path)
        real_commit(connection)

    monkeypatch.setattr(lab_jobs._LabJobStoreConnection, "commit", swap_before_commit)
    try:
        with pytest.raises(LabDaemonConfigurationError, match="identity changed"):
            scheduler.run_once()
    finally:
        monkeypatch.setattr(lab_jobs._LabJobStoreConnection, "commit", real_commit)
        if path.exists():
            path.rename(replacement)
        if original.exists():
            original.rename(path)
        authority.close()

    assert published.path.exists()
    assert not (spool.ack_dir / f"{published.envelope.request_id}.json").exists()
    assert LabJobReader(path).get_job(published.envelope.command.job_id) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_commands_per_tick", 257),
        ("max_reports_per_tick", 257),
        ("max_plans_per_tick", 257),
        ("max_claims_per_tick", 129),
        ("max_claim_authority_per_tick", 513),
        ("max_artifact_commits_per_tick", 257),
    ],
)
def test_scheduler_rejects_unbounded_tick_batches(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    store, spool = _components(tmp_path)
    kwargs = {
        "store": store,
        "spool": spool,
        "owner_id": "scheduler-a",
        "lease_seconds": 60,
        "heartbeat_seconds": 10,
        "poll_interval_ms": 10,
        field: value,
    }

    with pytest.raises(ValueError, match="safety limit"):
        LabScheduler(**kwargs)


def test_run_once_consumes_submit_but_keeps_job_queued_without_adapter(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    envelope = _envelope()
    spool.publish(envelope)
    scheduler = _scheduler(store, spool)

    result = scheduler.run_once()
    job = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert isinstance(result, SchedulerTickResult)
    assert result.lease_acquired is True
    assert result.processed == 1
    assert result.applied == 1
    assert result.rejected == 0
    assert result.quarantined == 0
    assert result.recovered == 0
    assert job is not None
    assert job.status is JobStatus.QUEUED
    assert spool.pending() == ()


def test_scheduler_commits_verified_complete_result_before_ack(tmp_path: Path) -> None:
    store, command_spool = _components(tmp_path)
    commit_spool = LabArtifactCommitSpool(tmp_path / "artifact-commits")
    artifact_store = LabJobArtifactStore(tmp_path / "artifacts")
    clock = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=command_spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=commit_spool,
        artifact_store=artifact_store,
        finalizer_authority_key_provider=_authority_verification_key_provider,
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    assert scheduler.lease is not None
    envelope = _envelope()
    assert store.apply_command(envelope, lease=scheduler.lease, now=NOW).status == "applied"
    store.plan_job(
        envelope.command.job_id,
        (_definition(0),),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    report = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW + timedelta(seconds=3),
        body=LabShardSucceeded.current(
            result_manifest_hash="9" * 64,
            worker_code_sha="1" * 40,
        ),
    )
    assert (
        store.apply_worker_report(
            report,
            lease=scheduler.lease,
            now=NOW + timedelta(seconds=3),
        ).status
        == "accepted"
    )
    job = LabJobReader(store.path).get_job(envelope.command.job_id)
    assert job is not None and job.result_state is LabResultState.READY
    sealed = artifact_store.seal_candidate(
        artifact_store.prepare_candidate(
            job_id=job.job_id,
            spec=job.spec,
            plan_hash=PLAN_HASH,
            adapter_id="n-shape-replay",
            adapter_version="v1",
            result_contract_version=COMPLETE_RESULT_CONTRACT_VERSION,
            metrics={"shards": 1},
            report_markdown="# Complete result\n",
            tables={"result": pd.DataFrame({"value": [1]})},
        )
    )
    commit = LabArtifactCommit(
        job_id=job.job_id,
        spec_hash=sealed.manifest.spec_hash,
        plan_hash=sealed.manifest.plan_hash,
        adapter_id=sealed.manifest.adapter_id,
        adapter_version=sealed.manifest.adapter_version,
        result_contract_version=sealed.manifest.result_contract_version,
        code_sha=sealed.manifest.code_sha,
        dataset_snapshot=sealed.manifest.dataset_snapshot,
        manifest_hash=sealed.manifest_hash,
        complete_result_hash=sealed.manifest.complete_result_hash,
        sealed_path=sealed.path,
    )
    published = commit_spool.publish(_signed_artifact_envelope(store, commit))
    clock[0] = NOW + timedelta(seconds=4)

    tick = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    evidence = LabJobReader(store.path).get_result_artifact(job.job_id)
    assert tick.artifact_commits_processed == 1
    assert tick.artifact_commits_accepted == 1
    assert tick.artifact_commits_rejected == 0
    assert tick.artifact_commits_quarantined == 0
    assert completed is not None and completed.status is JobStatus.SUCCEEDED
    assert completed.result_state is LabResultState.SEALED
    assert evidence is not None
    assert evidence.manifest_hash == sealed.manifest_hash
    assert evidence.complete_result_hash == sealed.manifest.complete_result_hash
    assert commit_spool.pending() == ()
    assert (
        commit_spool.load_receipt(
            commit_spool.ack_dir / f"{published.envelope.request_id}.json"
        ).status
        == "accepted"
    )
    with sqlite3.connect(store.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE lab_job_result_artifact SET sealed_path = sealed_path WHERE job_id = ?",
                (str(job.job_id),),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "DELETE FROM lab_job_result_artifact WHERE job_id = ?",
                (str(job.job_id),),
            )


def test_scheduler_quarantines_untrusted_mac_without_poisoning_request_id(
    tmp_path: Path,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            publish=False,
            artifact_frame=pd.DataFrame([{"hold_days": 999, "ret_pct": 999.0}]),
        )
    )
    forged = _signed_artifact_envelope(
        store,
        envelope.commit,
        request_id=envelope.request_id,
        key=LabFinalizerAuthorityKey(key_id="attacker-key", secret=b"x" * 32),
    )
    spool.publish(forged)
    clock[0] = NOW + timedelta(seconds=5)

    tick = scheduler.run_once()

    current = LabJobReader(store.path).get_job(job.job_id)
    receipt = LabJobReader(store.path).get_artifact_commit(forged.request_id)
    assert tick.artifact_commits_accepted == 0
    assert tick.artifact_commits_rejected == 0
    assert tick.artifact_commits_quarantined == 1
    assert current is not None and current.result_state is LabResultState.READY
    assert receipt is None
    assert not (spool.ack_dir / f"{forged.request_id}.json").exists()

    spool.publish(envelope)
    clock[0] = NOW + timedelta(seconds=10)
    accepted = scheduler.run_once()

    committed = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert accepted.artifact_commits_accepted == 1
    assert committed is not None and committed.receipt.status == "accepted"


def test_scheduler_quarantines_bad_mac_for_known_key_without_ledger_or_ack(
    tmp_path: Path,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    proof = envelope.authority_proof
    assert proof is not None
    forged = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=envelope.commit,
        authority_proof=proof.model_copy(update={"mac_sha256": "0" * 64}),
    )
    spool.publish(forged)
    clock[0] = NOW + timedelta(seconds=5)

    tick = scheduler.run_once()

    assert tick.artifact_commits_quarantined == 1
    assert tick.artifact_commits_rejected == 0
    assert LabJobReader(store.path).get_artifact_commit(forged.request_id) is None
    assert not (spool.ack_dir / f"{forged.request_id}.json").exists()
    current = LabJobReader(store.path).get_job(job.job_id)
    assert current is not None and current.result_state is LabResultState.READY


def test_scheduler_runtime_drift_after_artifact_load_does_not_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, _artifacts, _job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    proof = envelope.authority_proof
    assert proof is not None
    forged = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=envelope.commit,
        authority_proof=proof.model_copy(update={"mac_sha256": "0" * 64}),
    )
    published = spool.publish(forged)
    clock[0] = NOW + timedelta(seconds=5)
    drifted = False
    original_load = spool.load

    def drift_after_load(path: Path) -> object:
        nonlocal drifted
        entry = original_load(path)
        drifted = True
        return entry

    def runtime_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("runtime checkout drifted")
        return "1" * 40

    monkeypatch.setattr(spool, "load", drift_after_load)
    scheduler.runtime_guard = runtime_guard

    with pytest.raises(LabDaemonConfigurationError, match="drifted"):
        scheduler.run_once()

    assert published.path.exists()
    assert tuple(spool.quarantine_dir.iterdir()) == ()
    assert not (spool.ack_dir / f"{forged.request_id}.json").exists()


def test_scheduler_rejects_bad_mac_before_artifact_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    proof = envelope.authority_proof
    assert proof is not None
    forged = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=envelope.commit,
        authority_proof=proof.model_copy(update={"mac_sha256": "0" * 64}),
    )
    spool.publish(forged)
    clock[0] = NOW + timedelta(seconds=5)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("unauthenticated commit reached artifact binding")

    monkeypatch.setattr(artifacts, "bind_verified_sealed", forbidden)
    tick = scheduler.run_once()

    assert tick.artifact_commits_quarantined == 1
    assert tick.artifact_commits_rejected == 0
    assert LabJobReader(store.path).get_artifact_commit(forged.request_id) is None
    assert not (spool.ack_dir / f"{forged.request_id}.json").exists()
    current = LabJobReader(store.path).get_job(job.job_id)
    assert current is not None and current.result_state is LabResultState.READY


def test_scheduler_reverifies_authority_inside_sqlite_transaction(
    tmp_path: Path,
) -> None:
    calls = 0

    def rotating_provider(key_id: str) -> LabFinalizerAuthorityKey | None:
        nonlocal calls
        calls += 1
        if calls == 1 and key_id == AUTHORITY_KEY.key_id:
            return AUTHORITY_KEY
        return None

    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            publish=False,
            authority_key_provider=rotating_provider,
        )
    )
    spool.publish(envelope)
    clock[0] = NOW + timedelta(seconds=5)

    tick = scheduler.run_once()

    assert calls == 2
    assert tick.artifact_commits_quarantined == 1
    assert tick.artifact_commits_accepted == 0
    assert tick.artifact_commits_rejected == 0
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert not (spool.ack_dir / f"{envelope.request_id}.json").exists()
    current = LabJobReader(store.path).get_job(job.job_id)
    assert current is not None and current.result_state is LabResultState.READY


def test_scheduler_accepts_transition_key_from_verification_keyring(
    tmp_path: Path,
) -> None:
    transition = LabFinalizerAuthorityKey(key_id="scheduler-old-key", secret=b"o" * 32)
    keys = {AUTHORITY_KEY.key_id: AUTHORITY_KEY, transition.key_id: transition}
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            publish=False,
            authority_key_provider=keys.get,
        )
    )
    old_envelope = _signed_artifact_envelope(
        store,
        envelope.commit,
        request_id=envelope.request_id,
        key=transition,
    )
    spool.publish(old_envelope)
    clock[0] = NOW + timedelta(seconds=5)

    tick = scheduler.run_once()

    record = LabJobReader(store.path).get_artifact_commit(old_envelope.request_id)
    current = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_accepted == 1
    assert record is not None and record.receipt.status == "accepted"
    assert current is not None and current.result_state is LabResultState.SEALED


def test_scheduler_quarantines_legacy_unsigned_artifact_commit(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    unsigned = LabArtifactCommitEnvelope(
        schema_version=1,
        request_id=envelope.request_id,
        commit=envelope.commit,
    )
    spool.publish(unsigned)
    clock[0] = NOW + timedelta(seconds=5)

    tick = scheduler.run_once()

    record = LabJobReader(store.path).get_artifact_commit(unsigned.request_id)
    current = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_rejected == 0
    assert tick.artifact_commits_quarantined == 1
    assert record is None
    assert not (spool.ack_dir / f"{unsigned.request_id}.json").exists()
    assert current is not None and current.result_state is LabResultState.READY


def test_scheduler_reloads_authority_key_and_quarantines_unknown_rotation(
    tmp_path: Path,
) -> None:
    current_key = LabFinalizerAuthorityKey(
        key_id="rotated-key",
        secret=b"r" * 32,
    )
    calls = 0

    def provider(_key_id: str) -> LabFinalizerAuthorityKey | None:
        nonlocal calls
        calls += 1
        return current_key

    store, scheduler, _spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            authority_key_provider=provider,
        )
    )

    tick = scheduler.run_once()

    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    current = LabJobReader(store.path).get_job(job.job_id)
    assert calls == 1
    assert tick.artifact_commits_rejected == 0
    assert tick.artifact_commits_quarantined == 1
    assert record is None
    assert current is not None and current.result_state is LabResultState.READY


def test_scheduler_rejects_signed_proof_that_does_not_match_ready_shard_graph(
    tmp_path: Path,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    proof = envelope.authority_proof
    assert proof is not None
    shard = proof.claims.shards[0]
    changed_claims = proof.claims.model_copy(
        update={"shards": (shard.model_copy(update={"accepted_report_content_hash": "e" * 64}),)}
    )
    changed = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=envelope.commit,
        authority_proof=sign_finalizer_authority(
            changed_claims,
            key_provider=_authority_key_provider,
        ),
    )
    spool.publish(changed)
    clock[0] = NOW + timedelta(seconds=5)

    tick = scheduler.run_once()

    record = LabJobReader(store.path).get_artifact_commit(changed.request_id)
    current = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_rejected == 1
    assert record is not None
    assert record.receipt.reason == "finalizer_authority_graph_mismatch"
    assert current is not None and current.result_state is LabResultState.READY


def _ready_artifact_commit_scenario(
    tmp_path: Path,
    *,
    scheduler_type: type[LabScheduler] = LabScheduler,
    commit_spool_type: type[LabArtifactCommitSpool] = LabArtifactCommitSpool,
    publish: bool = True,
    deadline: datetime | None = None,
    with_dataset_snapshot: bool = True,
    dataset_audit_run_id: str | None = "d" * 64,
    artifact_frame: pd.DataFrame | None = None,
    authority_key_provider: Callable[[str], LabFinalizerAuthorityKey | None] = (
        _authority_verification_key_provider
    ),
) -> tuple[
    LabJobStore,
    LabScheduler,
    LabArtifactCommitSpool,
    LabJobArtifactStore,
    LabJobRecord,
    LabSealedJobArtifact,
    LabArtifactCommitEnvelope,
    list[datetime],
]:
    store, command_spool = _components(tmp_path)
    commit_spool = commit_spool_type(tmp_path / "artifact-commits")
    artifact_store = LabJobArtifactStore(tmp_path / "artifacts")
    clock = [NOW]
    scheduler = scheduler_type(
        store=store,
        spool=command_spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=commit_spool,
        artifact_store=artifact_store,
        finalizer_authority_key_provider=authority_key_provider,
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    assert scheduler.lease is not None
    submit = _envelope(
        spec=_spec(
            deadline=deadline,
            with_dataset_snapshot=with_dataset_snapshot,
            dataset_audit_run_id=dataset_audit_run_id,
        )
    )
    assert store.apply_command(submit, lease=scheduler.lease, now=NOW).status == "applied"
    store.plan_job(
        submit.command.job_id,
        (_definition(0),),
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=1),
    )
    claim = store.claim_next_shard(
        worker_id="worker-a",
        shard_lease_seconds=30,
        lease=scheduler.lease,
        now=NOW + timedelta(seconds=2),
    )
    assert claim is not None
    success = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW + timedelta(seconds=3),
        body=LabShardSucceeded.current(
            result_manifest_hash="9" * 64,
            worker_code_sha="1" * 40,
        ),
    )
    assert (
        store.apply_worker_report(
            success,
            lease=scheduler.lease,
            now=NOW + timedelta(seconds=3),
        ).status
        == "accepted"
    )
    job = LabJobReader(store.path).get_job(submit.command.job_id)
    assert job is not None and job.result_state is LabResultState.READY
    sealed = artifact_store.seal_candidate(
        artifact_store.prepare_candidate(
            job_id=job.job_id,
            spec=job.spec,
            plan_hash=PLAN_HASH,
            adapter_id="n-shape-replay",
            adapter_version="v1",
            result_contract_version=COMPLETE_RESULT_CONTRACT_VERSION,
            metrics={"shards": 1},
            report_markdown="# Complete result\n",
            tables={
                "result": (
                    artifact_frame if artifact_frame is not None else pd.DataFrame({"value": [1]})
                )
            },
        )
    )
    envelope = _signed_artifact_envelope(
        store,
        LabArtifactCommit(
            job_id=job.job_id,
            spec_hash=sealed.manifest.spec_hash,
            plan_hash=sealed.manifest.plan_hash,
            adapter_id=sealed.manifest.adapter_id,
            adapter_version=sealed.manifest.adapter_version,
            result_contract_version=sealed.manifest.result_contract_version,
            code_sha=sealed.manifest.code_sha,
            dataset_snapshot=sealed.manifest.dataset_snapshot,
            manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.manifest.complete_result_hash,
            sealed_path=sealed.path,
        ),
    )
    if publish:
        commit_spool.publish(envelope)
    clock[0] = NOW + timedelta(seconds=4)
    return store, scheduler, commit_spool, artifact_store, job, sealed, envelope, clock


def test_external_sql_cannot_revive_sealed_job_or_downgrade_contract(
    tmp_path: Path,
) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.run_once()

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="terminal|sealed|authorized|function"),
    ):
        connection.execute(
            """
            UPDATE lab_job
            SET status = 'running', result_state = 'ready',
                result_contract_version = NULL
            WHERE job_id = ?
            """,
            (str(job.job_id),),
        )


@pytest.mark.parametrize(
    "assignment",
    [
        "result_contract_version = NULL",
        "result_state = 'pending', result_contract_version = NULL",
        ("status = 'running', result_state = 'pending', result_contract_version = NULL"),
    ],
    ids=["contract-null", "ready-to-pending-null", "multi-column-regression"],
)
def test_ready_complete_result_cannot_regress_through_null_contract(
    tmp_path: Path,
    assignment: str,
) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )

    with (
        store._connect() as connection,
        pytest.raises(sqlite3.DatabaseError, match="consistent|immutable"),
    ):
        connection.execute(
            f"UPDATE lab_job SET {assignment} WHERE job_id = ?",
            (str(job.job_id),),
        )

    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert persisted is not None and persisted.result_state is LabResultState.READY
    assert persisted.result_contract_version == COMPLETE_RESULT_CONTRACT_VERSION
    assert scheduler.lease is not None
    assert (
        store.claim_next_shard(
            worker_id="worker-after-null-regression",
            shard_lease_seconds=30,
            lease=scheduler.lease,
            now=job.updated_at,
        )
        is None
    )


@pytest.mark.parametrize(
    "assignment",
    [
        "job_id = '00000000-0000-0000-0000-000000000001'",
        "spec_json = json_set(spec_json, '$.random_seed', 999)",
        f"spec_hash = '{'f' * 64}'",
        "job_type = 'mutated-job-type'",
        "resource_class = 'mutated-resource-class'",
        "deadline = '2027-01-01T00:00:00.000000+00:00'",
        "status = 'queued'",
        "control_intent = 'cancel_requested'",
        "version = version + 1",
        "attempt_count = attempt_count + 1",
        "max_attempts = max_attempts + 1",
        "recoverable = 1",
        "scheduler_fencing_token = scheduler_fencing_token + 1",
        "created_at = '1999-01-01T00:00:00.000000+00:00'",
        "updated_at = '2027-01-01T00:00:00.000000+00:00'",
        "result_contract_version = 'mutated-contract'",
        "result_state = 'pending'",
        "requires_complete_result = 0",
        (
            "job_id = '00000000-0000-0000-0000-000000000001', "
            "version = version + 5, attempt_count = attempt_count + 1, "
            "created_at = '1999-01-01T00:00:00.000000+00:00'"
        ),
    ],
    ids=[
        "job-id",
        "spec-json",
        "spec-hash",
        "job-type",
        "resource-class",
        "deadline",
        "status",
        "control-intent",
        "version",
        "attempt-count",
        "max-attempts",
        "recoverable",
        "scheduler-fence",
        "created-at",
        "updated-at",
        "result-contract",
        "result-state",
        "complete-result-marker",
        "combined-with-job-id",
    ],
)
def test_ready_complete_result_job_row_rejects_raw_sql_mutation(
    tmp_path: Path,
    assignment: str,
) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )

    with store._connect() as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0
        with pytest.raises(sqlite3.DatabaseError, match="ready|immutable|consistent"):
            connection.execute(
                f"UPDATE lab_job SET {assignment} WHERE job_id = ?",
                (str(job.job_id),),
            )

    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert persisted == job
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_shard WHERE job_id = ?",
            (str(job.job_id),),
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_job WHERE job_id = ?",
            ("00000000-0000-0000-0000-000000000001",),
        ).fetchone() == (0,)


def test_external_sql_without_store_authorization_cannot_update_ready_job(
    tmp_path: Path,
) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="function|authorized|immutable"),
    ):
        connection.execute(
            "UPDATE lab_job SET created_at = updated_at WHERE job_id = ?",
            (str(job.job_id),),
        )


_LAB_JOB_INSERT_COLUMNS = """
    job_id, spec_json, spec_hash, job_type, resource_class, deadline,
    status, control_intent, version, attempt_count, max_attempts,
    recoverable, scheduler_fencing_token, created_at, updated_at,
    result_contract_version, result_state, requires_complete_result
"""

_LAB_JOB_REPLACEMENT_SELECT = """
    SELECT job_id, spec_json, spec_hash, job_type, resource_class, deadline,
           'queued', 'none', 0, 0, max_attempts, 0, NULL,
           created_at, created_at, NULL, 'pending', 1
    FROM lab_job WHERE job_id = ?
"""


@pytest.mark.parametrize("result_state", ["ready", "sealed"])
@pytest.mark.parametrize(
    ("insert_prefix", "conflict_clause"),
    [
        ("REPLACE INTO", ""),
        ("INSERT OR REPLACE INTO", ""),
        (
            "INSERT INTO",
            "ON CONFLICT(job_id) DO UPDATE SET status = excluded.status, "
            "result_state = excluded.result_state, version = excluded.version",
        ),
        (
            "INSERT INTO",
            "ON CONFLICT DO UPDATE SET status = excluded.status, "
            "result_state = excluded.result_state, version = excluded.version",
        ),
        ("INSERT INTO", "ON CONFLICT(job_id) DO NOTHING"),
    ],
    ids=[
        "replace",
        "insert-or-replace",
        "targeted-upsert",
        "untargeted-upsert",
        "do-nothing-upsert",
    ],
)
def test_existing_ready_or_sealed_job_key_rejects_insert_replacement_forms(
    tmp_path: Path,
    result_state: str,
    insert_prefix: str,
    conflict_clause: str,
) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    if result_state == "sealed":
        _spool.publish(_envelope)
        scheduler.run_once()

    statement = (
        f"{insert_prefix} lab_job ({_LAB_JOB_INSERT_COLUMNS}) "
        f"{_LAB_JOB_REPLACEMENT_SELECT} {conflict_clause}"
    )
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA recursive_triggers").fetchone()[0] == 0
        connection.create_function(
            lab_jobs._SUBMIT_AUTH_FUNCTION,
            2,
            lambda _job_id, _spec_json: 1,
        )
        connection.create_function(
            lab_jobs._RETRY_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        connection.create_function(
            lab_jobs._READY_TERMINAL_AUTH_FUNCTION,
            6,
            lambda *_args: 1,
        )
        connection.create_function(
            lab_jobs._ARTIFACT_SUCCESS_AUTH_FUNCTION,
            5,
            lambda *_args: 1,
        )
        with pytest.raises(sqlite3.DatabaseError, match="existing job key"):
            connection.execute(statement, (str(job.job_id),))

    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert persisted is not None and persisted.result_state.value == result_state
    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_shard WHERE job_id = ?",
            (str(job.job_id),),
        ).fetchone() == (1,)


_LAB_SHARD_MINIMAL_COLUMNS = """
    shard_id, job_id, shard_index, status, version, attempt_count,
    max_attempts, created_at, updated_at
"""


@pytest.mark.parametrize(
    ("insert_prefix", "conflict_clause"),
    [
        ("INSERT INTO", ""),
        ("REPLACE INTO", ""),
        ("INSERT OR REPLACE INTO", ""),
        (
            "INSERT INTO",
            "ON CONFLICT(job_id, shard_id) DO UPDATE SET updated_at = excluded.updated_at",
        ),
    ],
    ids=["insert", "replace", "insert-or-replace", "upsert"],
)
def test_fk_off_connection_cannot_insert_orphan_shard(
    tmp_path: Path,
    insert_prefix: str,
    conflict_clause: str,
) -> None:
    store, _spool = _components(tmp_path)
    orphan_job_id = uuid4()
    statement = (
        f"{insert_prefix} lab_shard ({_LAB_SHARD_MINIMAL_COLUMNS}) "
        "VALUES (?, ?, 0, 'queued', 0, 0, 3, ?, ?) "
        f"{conflict_clause}"
    )
    timestamp = NOW.isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0
        with pytest.raises(sqlite3.DatabaseError, match="parent|orphan"):
            connection.execute(
                statement,
                (str(uuid4()), str(orphan_job_id), timestamp, timestamp),
            )

    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM lab_shard WHERE job_id = ?",
            (str(orphan_job_id),),
        ).fetchone() == (0,)


@pytest.mark.parametrize("result_state", ["ready", "sealed"])
@pytest.mark.parametrize(
    ("insert_prefix", "conflict_clause"),
    [
        ("INSERT INTO", ""),
        ("REPLACE INTO", ""),
        ("INSERT OR REPLACE INTO", ""),
        (
            "INSERT INTO",
            "ON CONFLICT(job_id, shard_id) DO UPDATE SET updated_at = excluded.updated_at",
        ),
    ],
    ids=["insert", "replace", "insert-or-replace", "upsert"],
)
def test_fk_off_connection_cannot_attach_new_shard_to_ready_or_sealed_job(
    tmp_path: Path,
    result_state: str,
    insert_prefix: str,
    conflict_clause: str,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    if result_state == "sealed":
        spool.publish(envelope)
        scheduler.run_once()
    statement = (
        f"{insert_prefix} lab_shard ({_LAB_SHARD_MINIMAL_COLUMNS}) "
        "VALUES (?, ?, 99, 'queued', 0, 0, 3, ?, ?) "
        f"{conflict_clause}"
    )
    timestamp = NOW.isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0
        with pytest.raises(sqlite3.DatabaseError, match="shard|immutable"):
            connection.execute(
                statement,
                (str(uuid4()), str(job.job_id), timestamp, timestamp),
            )

    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert persisted is not None and persisted.result_state.value == result_state
    assert len(LabJobReader(store.path).list_shards(job.job_id)) == 1


@pytest.mark.parametrize("result_state", ["ready", "sealed"])
def test_fk_off_connection_cannot_rehome_orphan_shard_to_complete_result_job(
    tmp_path: Path,
    result_state: str,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    if result_state == "sealed":
        spool.publish(envelope)
        scheduler.run_once()
    orphan_job_id = uuid4()
    orphan_shard_id = uuid4()
    timestamp = NOW.isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0
        connection.execute("DROP TRIGGER trg_lab_complete_result_shard_no_insert")
        connection.execute(
            f"""
            INSERT INTO lab_shard ({_LAB_SHARD_MINIMAL_COLUMNS})
            VALUES (?, ?, 99, 'queued', 0, 0, 3, ?, ?)
            """,
            (str(orphan_shard_id), str(orphan_job_id), timestamp, timestamp),
        )
        connection.execute(lab_jobs._V5_COMPLETE_RESULT_SHARD_NO_INSERT_TRIGGER)
        with pytest.raises(sqlite3.DatabaseError, match="shard|parent|ownership|immutable"):
            connection.execute(
                "UPDATE lab_shard SET job_id = ? WHERE job_id = ? AND shard_id = ?",
                (str(job.job_id), str(orphan_job_id), str(orphan_shard_id)),
            )

    with sqlite3.connect(store.path) as connection:
        assert connection.execute(
            "SELECT job_id FROM lab_shard WHERE shard_id = ?",
            (str(orphan_shard_id),),
        ).fetchone() == (str(orphan_job_id),)
    assert len(LabJobReader(store.path).list_shards(job.job_id)) == 1


@pytest.mark.parametrize("result_state", ["ready", "sealed"])
def test_fk_off_upsert_cannot_rehome_existing_shard_to_complete_result_job(
    tmp_path: Path,
    result_state: str,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    if result_state == "sealed":
        spool.publish(envelope)
        scheduler.run_once()
    assert scheduler.lease is not None
    donor_submit = _envelope()
    assert (
        store.apply_command(donor_submit, lease=scheduler.lease, now=clock[0]).status == "applied"
    )
    donor_definitions = (_definition(0), _definition(1))
    store.plan_job(
        donor_submit.command.job_id,
        donor_definitions,
        lease=scheduler.lease,
        now=clock[0],
    )
    donor_shard = LabJobReader(store.path).list_shards(donor_submit.command.job_id)[1]

    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0
        with pytest.raises(sqlite3.DatabaseError, match="shard|ownership|immutable"):
            connection.execute(
                """
                INSERT INTO lab_shard
                SELECT * FROM lab_shard
                WHERE job_id = ? AND shard_id = ?
                ON CONFLICT(job_id, shard_id) DO UPDATE
                SET job_id = ?, shard_index = 99
                """,
                (
                    str(donor_submit.command.job_id),
                    str(donor_shard.shard_id),
                    str(job.job_id),
                ),
            )

    persisted_donor = LabJobReader(store.path).list_shards(donor_submit.command.job_id)
    assert len(persisted_donor) == 2
    assert len(LabJobReader(store.path).list_shards(job.job_id)) == 1


def test_reader_rejects_persisted_sealed_job_without_any_shards(tmp_path: Path) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.run_once()
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TRIGGER trg_lab_complete_result_shard_no_delete")
        connection.execute("DELETE FROM lab_shard WHERE job_id = ?", (str(job.job_id),))
        connection.execute(lab_jobs._V5_COMPLETE_RESULT_SHARD_NO_DELETE_TRIGGER)

    with pytest.raises(InvalidStoredJobError, match="shard"):
        LabJobReader(store.path).get_job(job.job_id)


def test_external_sql_cannot_delete_shard_from_sealed_job(tmp_path: Path) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.run_once()

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.DatabaseError, match="shard|immutable"),
    ):
        connection.execute("DELETE FROM lab_shard WHERE job_id = ?", (str(job.job_id),))


def test_external_fk_off_connection_cannot_delete_sealed_job_parent(tmp_path: Path) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.run_once()

    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 0
        with pytest.raises(sqlite3.DatabaseError, match="job|sealed|delete"):
            connection.execute("DELETE FROM lab_job WHERE job_id = ?", (str(job.job_id),))

    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert persisted is not None and persisted.result_state is LabResultState.SEALED


@pytest.mark.parametrize(
    "assignment",
    [
        "created_at = '1999-01-01T00:00:00+00:00'",
        "attempt_count = attempt_count + 1",
        (
            "created_at = '1999-01-01T00:00:00+00:00', "
            "attempt_count = attempt_count + 1, spec_hash = 'f' || substr(spec_hash, 2)"
        ),
    ],
    ids=["created-at", "attempt-count", "multi-column"],
)
def test_complete_sealed_job_row_is_immutable(
    tmp_path: Path,
    assignment: str,
) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.run_once()

    with (
        store._connect() as connection,
        pytest.raises(sqlite3.DatabaseError, match="sealed|immutable"),
    ):
        connection.execute(
            f"UPDATE lab_job SET {assignment} WHERE job_id = ?",
            (str(job.job_id),),
        )


def test_reader_checks_sealed_ledger_without_reopening_artifact_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, _spool, _artifacts, job, sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.run_once()
    original_stat = Path.stat

    def reject_artifact_stat(path: Path, *args: object, **kwargs: object) -> object:
        if path == sealed.path or sealed.path in path.parents:
            raise AssertionError("reader attempted live artifact filesystem verification")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", reject_artifact_stat)

    persisted = LabJobReader(store.path).get_job(job.job_id)
    evidence = LabJobReader(store.path).get_result_artifact(job.job_id)

    assert persisted is not None and persisted.result_state is LabResultState.SEALED
    assert evidence is not None and evidence.sealed_path == sealed.path


def test_reader_rejects_accepted_commit_after_result_index_is_lost(tmp_path: Path) -> None:
    store, scheduler, _spool, _artifacts, _job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.run_once()
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TRIGGER trg_lab_result_artifact_no_delete")
        connection.execute(
            "DELETE FROM lab_job_result_artifact WHERE commit_request_id = ?",
            (str(envelope.request_id),),
        )
        connection.execute(lab_jobs._V5_RESULT_ARTIFACT_NO_DELETE_TRIGGER)

    with pytest.raises(InvalidStoredJobError, match="index"):
        LabJobReader(store.path).get_artifact_commit(envelope.request_id)


@pytest.mark.parametrize(
    ("commit_json_override", "receipt_json_override"),
    [("{}", None), (None, "{}")],
    ids=["empty-commit", "empty-receipt"],
)
def test_commit_json_shape_is_checked_even_with_spoofed_sql_authority(
    tmp_path: Path,
    commit_json_override: str | None,
    receipt_json_override: str | None,
) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact_committed",
        accepted_at=clock[0],
        job_version=job.version + 1,
    )
    commit_json = commit_json_override or _canonical_json(envelope)
    receipt_json = receipt_json_override or _canonical_json(receipt)
    timestamp = clock[0].isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        connection.create_function(
            lab_jobs._ARTIFACT_COMMIT_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        with pytest.raises(sqlite3.DatabaseError, match="CHECK|consistent"):
            connection.execute(
                """
                INSERT INTO lab_artifact_commit (
                    request_id, content_hash, job_id, commit_json,
                    status, reason, receipt_json, receipt_job_version,
                    received_at, applied_at
                ) VALUES (?, ?, ?, ?, 'accepted', 'artifact_committed', ?, ?, ?, ?)
                """,
                (
                    str(envelope.request_id),
                    envelope.content_hash,
                    str(job.job_id),
                    commit_json,
                    receipt_json,
                    receipt.job_version,
                    timestamp,
                    timestamp,
                ),
            )


@pytest.mark.parametrize(
    ("mutation", "dataset_snapshot"),
    [
        ("empty-object", {}),
        (
            "missing-required-key",
            {"binding_hash": None, "audit_run_id": None},
        ),
        (
            "required-json-null",
            {"snapshot_id": None, "binding_hash": None, "audit_run_id": None},
        ),
        (
            "malformed-required-value",
            {"snapshot_id": 7, "binding_hash": "b" * 64, "audit_run_id": None},
        ),
    ],
)
def test_spoofed_authority_cannot_insert_malformed_dataset_snapshot(
    tmp_path: Path,
    mutation: str,
    dataset_snapshot: object,
) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            publish=False,
            with_dataset_snapshot=False,
        )
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact_committed",
        accepted_at=clock[0],
        job_version=job.version + 1,
    )
    raw_envelope = envelope.model_dump(mode="json")
    raw_envelope["commit"]["dataset_snapshot"] = dataset_snapshot
    commit_json = json.dumps(raw_envelope, sort_keys=True, separators=(",", ":"))
    timestamp = clock[0].isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        connection.create_function(
            lab_jobs._ARTIFACT_COMMIT_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        with pytest.raises(sqlite3.DatabaseError, match="CHECK|consistent"):
            connection.execute(
                """
                INSERT INTO lab_artifact_commit (
                    request_id, content_hash, job_id, commit_json,
                    status, reason, receipt_json, receipt_job_version,
                    received_at, applied_at
                ) VALUES (?, ?, ?, ?, 'accepted', 'artifact_committed', ?, ?, ?, ?)
                """,
                (
                    str(envelope.request_id),
                    envelope.content_hash,
                    str(job.job_id),
                    commit_json,
                    _canonical_json(receipt),
                    receipt.job_version,
                    timestamp,
                    timestamp,
                ),
            )


@pytest.mark.parametrize(
    ("mutation", "extra_value"),
    [
        ("extra-key", True),
        ("nested-extra-object", {"source": "review-probe"}),
        ("alternate-snapshot-path", {"snapshot_id": "e" * 64}),
        ("dotted-alternate-key", "e" * 64),
    ],
)
def test_dataset_snapshot_extra_keys_are_rejected_by_protocol_and_sql(
    tmp_path: Path,
    mutation: str,
    extra_value: object,
) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact_committed",
        accepted_at=clock[0],
        job_version=job.version + 1,
    )
    raw_envelope = envelope.model_dump(mode="json")
    snapshot = raw_envelope["commit"]["dataset_snapshot"]
    assert isinstance(snapshot, dict)
    extra_key = {
        "extra-key": "unexpected",
        "nested-extra-object": "metadata",
        "alternate-snapshot-path": "snapshot",
        "dotted-alternate-key": "snapshot_id.extra",
    }[mutation]
    snapshot[extra_key] = extra_value

    with pytest.raises(ValidationError, match="extra"):
        LabArtifactCommitEnvelope.model_validate(raw_envelope)

    commit_json = json.dumps(raw_envelope, sort_keys=True, separators=(",", ":"))
    timestamp = clock[0].isoformat(timespec="microseconds")
    with sqlite3.connect(store.path) as connection:
        connection.create_function(
            lab_jobs._ARTIFACT_COMMIT_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        with pytest.raises(sqlite3.DatabaseError, match="CHECK|consistent"):
            connection.execute(
                """
                INSERT INTO lab_artifact_commit (
                    request_id, content_hash, job_id, commit_json,
                    status, reason, receipt_json, receipt_job_version,
                    received_at, applied_at
                ) VALUES (?, ?, ?, ?, 'accepted', 'artifact_committed', ?, ?, ?, ?)
                """,
                (
                    str(envelope.request_id),
                    envelope.content_hash,
                    str(job.job_id),
                    commit_json,
                    _canonical_json(receipt),
                    receipt.job_version,
                    timestamp,
                    timestamp,
                ),
            )


def test_reader_rejects_stored_commit_with_dataset_snapshot_extra_key(
    tmp_path: Path,
) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact_committed",
        accepted_at=clock[0],
        job_version=job.version + 1,
    )
    raw_envelope = envelope.model_dump(mode="json")
    snapshot = raw_envelope["commit"]["dataset_snapshot"]
    assert isinstance(snapshot, dict)
    snapshot["unexpected"] = {"alternate": {"snapshot_id": "e" * 64}}
    commit_json = json.dumps(raw_envelope, sort_keys=True, separators=(",", ":"))
    timestamp = clock[0].isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("DROP TRIGGER trg_lab_artifact_commit_insert")
        connection.execute(
            """
            INSERT INTO lab_artifact_commit (
                request_id, content_hash, job_id, commit_json,
                status, reason, receipt_json, receipt_job_version,
                received_at, applied_at
            ) VALUES (?, ?, ?, ?, 'accepted', 'artifact_committed', ?, ?, ?, ?)
            """,
            (
                str(envelope.request_id),
                envelope.content_hash,
                str(job.job_id),
                commit_json,
                _canonical_json(receipt),
                receipt.job_version,
                timestamp,
                timestamp,
            ),
        )
        connection.execute(lab_jobs._V5_ARTIFACT_COMMIT_INSERT_TRIGGER)

    with pytest.raises(InvalidStoredJobError, match="artifact commit|extra"):
        LabJobReader(store.path).get_artifact_commit(envelope.request_id)


def test_spoofed_authority_cannot_omit_dataset_snapshot_field(tmp_path: Path) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            publish=False,
            with_dataset_snapshot=False,
        )
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact_committed",
        accepted_at=clock[0],
        job_version=job.version + 1,
    )
    raw_envelope = envelope.model_dump(mode="json")
    del raw_envelope["commit"]["dataset_snapshot"]
    commit_json = json.dumps(raw_envelope, sort_keys=True, separators=(",", ":"))
    timestamp = clock[0].isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        connection.create_function(
            lab_jobs._ARTIFACT_COMMIT_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        with pytest.raises(sqlite3.DatabaseError, match="CHECK|consistent"):
            connection.execute(
                """
                INSERT INTO lab_artifact_commit (
                    request_id, content_hash, job_id, commit_json,
                    status, reason, receipt_json, receipt_job_version,
                    received_at, applied_at
                ) VALUES (?, ?, ?, ?, 'accepted', 'artifact_committed', ?, ?, ?, ?)
                """,
                (
                    str(envelope.request_id),
                    envelope.content_hash,
                    str(job.job_id),
                    commit_json,
                    _canonical_json(receipt),
                    receipt.job_version,
                    timestamp,
                    timestamp,
                ),
            )


@pytest.mark.parametrize(
    ("with_dataset_snapshot", "dataset_audit_run_id"),
    [
        (False, None),
        (True, None),
        (True, "d" * 64),
    ],
    ids=["json-null", "full-object-audit-null", "full-object-audit-hash"],
)
def test_verified_commit_accepts_canonical_dataset_snapshot_shapes(
    tmp_path: Path,
    with_dataset_snapshot: bool,
    dataset_audit_run_id: str | None,
) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            with_dataset_snapshot=with_dataset_snapshot,
            dataset_audit_run_id=dataset_audit_run_id,
        )
    )

    scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert completed is not None and completed.result_state is LabResultState.SEALED
    assert record is not None and record.receipt.status == "accepted"
    assert record.envelope.commit.dataset_snapshot == job.spec.dataset_snapshot


def test_accepted_commit_receipt_version_matches_ready_job_transition(
    tmp_path: Path,
) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact_committed",
        accepted_at=clock[0],
        job_version=job.version + 2,
    )
    timestamp = clock[0].isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        connection.create_function(
            lab_jobs._ARTIFACT_COMMIT_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        with pytest.raises(sqlite3.DatabaseError, match="consistent"):
            connection.execute(
                """
                INSERT INTO lab_artifact_commit (
                    request_id, content_hash, job_id, commit_json,
                    status, reason, receipt_json, receipt_job_version,
                    received_at, applied_at
                ) VALUES (?, ?, ?, ?, 'accepted', 'artifact_committed', ?, ?, ?, ?)
                """,
                (
                    str(envelope.request_id),
                    envelope.content_hash,
                    str(job.job_id),
                    _canonical_json(envelope),
                    _canonical_json(receipt),
                    receipt.job_version,
                    timestamp,
                    timestamp,
                ),
            )


def test_accepted_commit_requires_at_least_one_succeeded_shard_even_with_sql_authority(
    tmp_path: Path,
) -> None:
    store, _scheduler, _spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact_committed",
        accepted_at=clock[0],
        job_version=job.version + 1,
    )
    timestamp = clock[0].isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TRIGGER trg_lab_complete_result_shard_no_delete")
        connection.execute("DELETE FROM lab_shard WHERE job_id = ?", (str(job.job_id),))
        connection.execute(lab_jobs._V5_COMPLETE_RESULT_SHARD_NO_DELETE_TRIGGER)
        connection.create_function(
            lab_jobs._ARTIFACT_COMMIT_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        with pytest.raises(sqlite3.DatabaseError, match="consistent"):
            connection.execute(
                """
                INSERT INTO lab_artifact_commit (
                    request_id, content_hash, job_id, commit_json,
                    status, reason, receipt_json, receipt_job_version,
                    received_at, applied_at
                ) VALUES (?, ?, ?, ?, 'accepted', 'artifact_committed', ?, ?, ?, ?)
                """,
                (
                    str(envelope.request_id),
                    envelope.content_hash,
                    str(job.job_id),
                    _canonical_json(envelope),
                    _canonical_json(receipt),
                    receipt.job_version,
                    timestamp,
                    timestamp,
                ),
            )


@pytest.mark.parametrize(
    "mutation",
    ["empty-evidence", "missing-path", "wrong-job", "wrong-request", "wrong-hash"],
)
def test_result_index_cross_fields_reject_spoofed_sql_authority(
    tmp_path: Path,
    mutation: str,
) -> None:
    store, _scheduler, _spool, artifacts, job, sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact_committed",
        accepted_at=clock[0],
        job_version=job.version + 1,
    )
    with artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding:
        evidence = binding.evidence
    request_id = envelope.request_id
    indexed_job_id = job.job_id
    if mutation == "missing-path":
        evidence = evidence.model_copy(update={"sealed_path": Path("/does/not/exist")})
    elif mutation == "wrong-job":
        indexed_job_id = uuid4()
        evidence = evidence.model_copy(update={"job_id": indexed_job_id})
    elif mutation == "wrong-request":
        request_id = uuid4()
    elif mutation == "wrong-hash":
        evidence = evidence.model_copy(update={"manifest_hash": "0" * 64})
    evidence_json = "{}" if mutation == "empty-evidence" else _canonical_json(evidence)
    timestamp = clock[0].isoformat(timespec="microseconds")

    with sqlite3.connect(store.path) as connection:
        connection.create_function(
            lab_jobs._ARTIFACT_COMMIT_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        connection.create_function(
            lab_jobs._ARTIFACT_INDEX_AUTH_FUNCTION,
            3,
            lambda *_args: 1,
        )
        connection.execute(
            """
            INSERT INTO lab_artifact_commit (
                request_id, content_hash, job_id, commit_json,
                status, reason, receipt_json, receipt_job_version,
                received_at, applied_at
            ) VALUES (?, ?, ?, ?, 'accepted', 'artifact_committed', ?, ?, ?, ?)
            """,
            (
                str(envelope.request_id),
                envelope.content_hash,
                str(job.job_id),
                _canonical_json(envelope),
                _canonical_json(receipt),
                receipt.job_version,
                timestamp,
                timestamp,
            ),
        )
        with pytest.raises(sqlite3.DatabaseError, match="CHECK|consistent"):
            connection.execute(
                """
                INSERT INTO lab_job_result_artifact (
                    job_id, commit_request_id, sealed_path, manifest_hash,
                    complete_result_hash, bundle_device, bundle_inode,
                    evidence_json, indexed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(indexed_job_id),
                    str(request_id),
                    str(evidence.sealed_path),
                    evidence.manifest_hash,
                    evidence.complete_result_hash,
                    evidence.bundle_device,
                    evidence.bundle_inode,
                    evidence_json,
                    timestamp,
                ),
            )


class _CrashBeforeArtifactCommitScheduler(LabScheduler):
    @staticmethod
    def _after_artifact_commit_staged(
        _entry: LabArtifactCommitSpoolEntry,
        _binding: object,
    ) -> None:
        raise RuntimeError("simulated crash before SQLite commit")


def test_artifact_lifecycle_remains_locked_through_sqlite_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, artifacts, job, sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    report_path = sealed.path / "report.md"
    original_commit = lab_jobs._LabJobStoreConnection.commit
    mutation_attempted = Event()
    mutation_acquired = Event()
    sqlite_commit_finished = Event()
    lifecycle_active_at_commit: list[bool] = []
    mutation_acquired_before_commit: list[bool] = []
    mutation_threads: list[Thread] = []

    def mutate_under_artifact_lifecycle() -> None:
        mutation_attempted.set()
        with artifacts._artifact_operation_lifecycle(prepare=False):
            mutation_acquired_before_commit.append(not sqlite_commit_finished.is_set())
            os.chmod(sealed.path, 0o700)
            report_path.unlink()
            mutation_acquired.set()

    def observe_sqlite_commit(connection: sqlite3.Connection) -> None:
        staged_row = connection.execute(
            "SELECT 1 FROM lab_artifact_commit WHERE request_id = ?",
            (str(envelope.request_id),),
        ).fetchone()
        if staged_row is None:
            original_commit(connection)
            return
        lifecycle_entry = artifacts._process_lock_entry
        lifecycle_active_at_commit.append(
            lifecycle_entry is not None
            and lifecycle_entry.lifecycle_owner_thread_id == get_ident()
            and lifecycle_entry.lifecycle_depth > 0
        )
        mutation_thread = Thread(target=mutate_under_artifact_lifecycle)
        mutation_threads.append(mutation_thread)
        mutation_thread.start()
        assert mutation_attempted.wait(timeout=2)
        mutation_acquired.wait(timeout=0.25)
        original_commit(connection)
        sqlite_commit_finished.set()

    monkeypatch.setattr(
        lab_jobs._LabJobStoreConnection,
        "commit",
        observe_sqlite_commit,
    )

    tick = scheduler.run_once()
    for mutation_thread in mutation_threads:
        mutation_thread.join(timeout=2)

    persisted = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_accepted == 1
    assert persisted is not None and persisted.result_state is LabResultState.SEALED
    assert lifecycle_active_at_commit == [True]
    assert mutation_acquired_before_commit == [False]
    assert mutation_acquired.is_set()
    assert all(not mutation_thread.is_alive() for mutation_thread in mutation_threads)
    assert not report_path.exists()
    assert spool.pending() == ()


def test_artifact_commit_crash_before_sqlite_commit_rolls_back_and_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            scheduler_type=_CrashBeforeArtifactCommitScheduler,
        )
    )

    with pytest.raises(RuntimeError, match="before SQLite commit"):
        scheduler.run_once()

    pending = LabJobReader(store.path).get_job(job.job_id)
    assert pending is not None and pending.result_state is LabResultState.READY
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert len(spool.pending()) == 1

    monkeypatch.setattr(
        scheduler,
        "_after_artifact_commit_staged",
        lambda _entry, _binding: None,
    )
    replay = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    assert replay.artifact_commits_accepted == 1
    assert completed is not None and completed.result_state is LabResultState.SEALED
    assert spool.pending() == ()


class _ReplaceBoundArtifactScheduler(LabScheduler):
    @staticmethod
    def _after_artifact_commit_staged(
        entry: LabArtifactCommitSpoolEntry,
        _binding: object,
    ) -> None:
        bundle = entry.envelope.commit.sealed_path
        report = bundle / "report.md"
        displaced = bundle.parent.parent / "displaced-report.md"
        os.chmod(bundle, 0o700)
        os.rename(report, displaced)
        report.write_bytes(displaced.read_bytes())
        os.chmod(report, 0o400)
        os.chmod(bundle, 0o500)


def test_artifact_final_check_failure_rolls_back_sqlite_and_quarantines(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            scheduler_type=_ReplaceBoundArtifactScheduler,
        )
    )
    original_rollback = lab_jobs._LabStagedArtifactCommit.rollback
    rollback_held_lifecycle: list[bool] = []

    def observe_rollback(staged: lab_jobs._LabStagedArtifactCommit) -> None:
        lifecycle_entry = artifacts._process_lock_entry
        rollback_held_lifecycle.append(
            lifecycle_entry is not None
            and lifecycle_entry.lifecycle_owner_thread_id == get_ident()
            and lifecycle_entry.lifecycle_depth > 0
        )
        original_rollback(staged)

    monkeypatch.setattr(
        lab_jobs._LabStagedArtifactCommit,
        "rollback",
        observe_rollback,
    )

    tick = scheduler.run_once()

    pending = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_quarantined == 1
    assert pending is not None and pending.result_state is LabResultState.READY
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()
    assert rollback_held_lifecycle == [True]
    lifecycle_entry = artifacts._process_lock_entry
    assert lifecycle_entry is not None
    assert lifecycle_entry.lifecycle_owner_thread_id is None
    assert lifecycle_entry.lifecycle_depth == 0


class _CrashBeforeArtifactAckSpool(LabArtifactCommitSpool):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.crash = True

    def ack(
        self,
        entry: LabArtifactCommitSpoolEntry,
        receipt: LabArtifactCommitReceipt,
    ) -> LabAcknowledgedArtifactCommit:
        if self.crash:
            self.crash = False
            raise RuntimeError("simulated crash after artifact SQLite commit")
        return super().ack(entry, receipt)


def test_artifact_commit_after_sqlite_before_ack_replays_same_receipt(
    tmp_path: Path,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            commit_spool_type=_CrashBeforeArtifactAckSpool,
        )
    )

    with pytest.raises(RuntimeError, match="after artifact SQLite commit"):
        scheduler.run_once()
    committed = LabJobReader(store.path).get_job(job.job_id)
    first = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert first is not None and first.receipt.status == "accepted"
    assert len(spool.pending()) == 1

    replay = scheduler.run_once()

    assert replay.artifact_commits_accepted == 1
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) == first
    assert (
        len(
            [
                event
                for event in LabJobReader(store.path).list_events(job.job_id)
                if event.event_type == "job_result_sealed"
            ]
        )
        == 1
    )
    assert spool.pending() == ()


def test_artifact_commit_key_rotation_reuses_authenticated_ledger_semantics(
    tmp_path: Path,
) -> None:
    new_key = LabFinalizerAuthorityKey(key_id="scheduler-new-key", secret=b"n" * 32)
    keyring = {AUTHORITY_KEY.key_id: AUTHORITY_KEY, new_key.key_id: new_key}
    store, scheduler, spool, artifacts, job, sealed, envelope, clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            authority_key_provider=keyring.get,
        )
    )
    scheduler.run_once()
    recorded = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert recorded is not None
    assert envelope.authority_proof is not None
    rotated = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=envelope.commit,
        authority_proof=sign_finalizer_authority(
            envelope.authority_proof.claims,
            key_provider=lambda: new_key,
        ),
    )
    assert scheduler.lease is not None

    with (
        artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding,
        store.stage_artifact_commit(
            rotated,
            binding,
            authority_key_provider=keyring.get,
            lease=scheduler.lease,
            now=clock[0],
        ) as staged,
    ):
        replayed = staged.commit(lease=scheduler.lease, now=clock[0])

    assert replayed == recorded.receipt
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) == recorded
    assert spool.pending() == ()


def test_artifact_commit_key_rotation_rejects_changed_claims_and_untrusted_old_proof(
    tmp_path: Path,
) -> None:
    new_key = LabFinalizerAuthorityKey(key_id="scheduler-new-key", secret=b"n" * 32)
    keyring = {AUTHORITY_KEY.key_id: AUTHORITY_KEY, new_key.key_id: new_key}
    store, scheduler, _spool, artifacts, _job, sealed, envelope, clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            authority_key_provider=keyring.get,
        )
    )
    scheduler.run_once()
    assert envelope.authority_proof is not None
    changed_claims = envelope.authority_proof.claims.model_copy(
        update={"ready_event_id": envelope.authority_proof.claims.ready_event_id + 1}
    )
    changed = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=envelope.commit,
        authority_proof=sign_finalizer_authority(
            changed_claims,
            key_provider=lambda: new_key,
        ),
    )
    assert scheduler.lease is not None

    with (
        artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding,
        pytest.raises(RequestContentConflictError),
        store.stage_artifact_commit(
            changed,
            binding,
            authority_key_provider=keyring.get,
            lease=scheduler.lease,
            now=clock[0],
        ),
    ):
        pass

    changed_commit = envelope.commit.model_copy(update={"manifest_hash": "0" * 64})
    changed_commit_claims = envelope.authority_proof.claims.model_copy(
        update={
            "commit_content_hash": hashlib.sha256(
                changed_commit.canonical_json_bytes()
            ).hexdigest(),
            "artifact_manifest_hash": "0" * 64,
        }
    )
    changed_commit_envelope = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=changed_commit,
        authority_proof=sign_finalizer_authority(
            changed_commit_claims,
            key_provider=lambda: new_key,
        ),
    )
    with (
        artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding,
        pytest.raises(RequestContentConflictError),
        store.stage_artifact_commit(
            changed_commit_envelope,
            binding,
            authority_key_provider=keyring.get,
            lease=scheduler.lease,
            now=clock[0],
        ),
    ):
        pass

    keyring.pop(AUTHORITY_KEY.key_id)
    rotated = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=envelope.commit,
        authority_proof=sign_finalizer_authority(
            envelope.authority_proof.claims,
            key_provider=lambda: new_key,
        ),
    )
    with (
        artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding,
        pytest.raises(LabFinalizerAuthorityAuthenticationError, match="unknown key_id"),
        store.stage_artifact_commit(
            rotated,
            binding,
            authority_key_provider=keyring.get,
            lease=scheduler.lease,
            now=clock[0],
        ),
    ):
        pass

    keyring[AUTHORITY_KEY.key_id] = LabFinalizerAuthorityKey(
        key_id=AUTHORITY_KEY.key_id,
        secret=b"x" * 32,
    )
    with (
        artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding,
        pytest.raises(LabFinalizerAuthorityAuthenticationError, match="MAC is invalid"),
        store.stage_artifact_commit(
            rotated,
            binding,
            authority_key_provider=keyring.get,
            lease=scheduler.lease,
            now=clock[0],
        ),
    ):
        pass


def test_artifact_lifecycle_exit_failure_after_sqlite_commit_remains_replayable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    original_lifecycle = artifacts.artifact_commit_lifecycle

    @contextmanager
    def fail_after_lifecycle_release() -> Iterator[None]:
        with original_lifecycle():
            yield
        raise LabArtifactIntegrityError("simulated lifecycle release failure after commit")

    monkeypatch.setattr(
        artifacts,
        "artifact_commit_lifecycle",
        fail_after_lifecycle_release,
    )

    with pytest.raises(LabArtifactIntegrityError, match="release failure"):
        scheduler.run_once()

    committed = LabJobReader(store.path).get_job(job.job_id)
    first = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert first is not None and first.receipt.status == "accepted"
    assert len(spool.pending()) == 1

    monkeypatch.setattr(
        artifacts,
        "artifact_commit_lifecycle",
        original_lifecycle,
    )
    replay = scheduler.run_once()

    assert replay.artifact_commits_accepted == 1
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) == first
    assert spool.pending() == ()


def test_same_artifact_index_is_idempotent_across_distinct_requests(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    replay = _signed_artifact_envelope(store, envelope.commit)
    first_tick = scheduler.run_once()
    spool.publish(replay)

    second_tick = scheduler.run_once()

    second = LabJobReader(store.path).get_artifact_commit(replay.request_id)
    assert first_tick.artifact_commits_accepted == 1
    assert second_tick.artifact_commits_accepted == 1
    assert second is not None and second.receipt.reason == "artifact_already_committed"
    assert (
        len(
            [
                event
                for event in LabJobReader(store.path).list_events(job.job_id)
                if event.event_type == "job_result_sealed"
            ]
        )
        == 1
    )


def test_sealed_candidate_without_commit_keeps_job_ready(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )

    tick = scheduler.run_once()

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_processed == 0
    assert unchanged is not None and unchanged.result_state is LabResultState.READY
    assert spool.pending() == ()


def test_artifact_commit_identity_mismatch_is_rejected_without_index(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    mismatched = _signed_artifact_envelope(
        store,
        LabArtifactCommit.model_validate(
            {
                **envelope.commit.model_dump(),
                "manifest_hash": "0" * 64,
            }
        ),
        request_id=envelope.request_id,
    )
    spool.publish(mismatched)

    tick = scheduler.run_once()

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.artifact_commits_rejected == 1
    assert unchanged is not None and unchanged.result_state is LabResultState.READY
    assert record is not None and record.receipt.reason == "artifact_identity_mismatch"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


def test_bad_pending_inode_does_not_block_later_valid_artifact_commit(
    tmp_path: Path,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    bad = spool.pending_dir / f"00000000000000000000-{uuid4()}.json"
    bad.mkdir()

    tick = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_quarantined == 1
    assert tick.artifact_commits_accepted == 1
    assert completed is not None and completed.result_state is LabResultState.SEALED
    assert not bad.exists()
    assert spool.pending() == ()


def test_hardlinked_pending_does_not_starve_valid_commit_at_tick_limit_one(
    tmp_path: Path,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.max_artifact_commits_per_tick = 1
    external = tmp_path / "external-hardlink.json"
    external.write_text("external evidence", encoding="utf-8")
    bad = spool.pending_dir / f"00000000000000000000-{uuid4()}.json"
    os.link(external, bad)

    tick = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_quarantined == 1
    assert tick.artifact_commit_quarantine_failures == 0
    assert tick.artifact_commits_accepted == 1
    assert completed is not None and completed.result_state is LabResultState.SEALED
    assert not os.path.lexists(bad)
    assert external.read_text(encoding="utf-8") == "external evidence"
    assert external.stat().st_nlink == 2


def test_artifact_quarantine_failure_does_not_block_later_valid_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    bad = spool.pending_dir / f"00000000000000000000-{uuid4()}.json"
    bad.write_text("{}", encoding="utf-8")
    original_quarantine = spool.quarantine

    def fail_bad_quarantine(
        entry_or_path: LabArtifactCommitSpoolEntry | LabSpoolFileIdentity | Path,
        *,
        reason: str,
    ) -> object:
        source = entry_or_path.path if hasattr(entry_or_path, "path") else Path(entry_or_path)
        if source == bad:
            raise OSError("simulated quarantine failure")
        return original_quarantine(entry_or_path, reason=reason)

    monkeypatch.setattr(spool, "quarantine", fail_bad_quarantine)

    tick = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    assert tick.artifact_commits_quarantined == 0
    assert tick.artifact_commit_quarantine_failures == 1
    assert tick.artifact_commits_accepted == 1
    assert completed is not None and completed.result_state is LabResultState.SEALED
    assert bad.exists()


def test_command_and_artifact_quarantine_metrics_do_not_collide(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, artifact_spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    bad_command = scheduler.spool.pending_dir / f"{uuid4()}.json"
    bad_command.write_text("{}", encoding="utf-8")
    bad_artifact = artifact_spool.pending_dir / f"{uuid4()}.json"
    bad_artifact.write_text("{}", encoding="utf-8")
    original_quarantine = artifact_spool.quarantine

    def fail_bad_artifact_quarantine(
        entry_or_path: LabArtifactCommitSpoolEntry | LabSpoolFileIdentity | Path,
        *,
        reason: str,
    ) -> object:
        source = entry_or_path.path if hasattr(entry_or_path, "path") else Path(entry_or_path)
        if source == bad_artifact:
            raise OSError("simulated artifact isolation failure")
        return original_quarantine(entry_or_path, reason=reason)

    monkeypatch.setattr(artifact_spool, "quarantine", fail_bad_artifact_quarantine)

    tick = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    assert tick.quarantined == 1
    assert tick.artifact_commits_quarantined == 0
    assert tick.artifact_commit_quarantine_failures == 1
    assert tick.artifact_commits_accepted == 1
    assert completed is not None and completed.result_state is LabResultState.SEALED


def test_artifact_fair_scan_reaches_valid_commit_after_persistent_failures_and_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, _envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.max_artifact_commits_per_tick = 1
    bad_paths = tuple(spool.pending_dir / f"{uuid4()}.json" for _index in range(66))
    for path in bad_paths:
        path.write_text("{}", encoding="utf-8")
    original_quarantine = spool.quarantine

    def persist_bad_artifact(
        entry_or_path: LabArtifactCommitSpoolEntry | LabSpoolFileIdentity | Path,
        *,
        reason: str,
    ) -> object:
        source = entry_or_path.path if hasattr(entry_or_path, "path") else Path(entry_or_path)
        if source in bad_paths:
            raise OSError("persistent isolation failure")
        return original_quarantine(entry_or_path, reason=reason)

    monkeypatch.setattr(spool, "quarantine", persist_bad_artifact)
    first = scheduler.run_once()
    assert first.artifact_commits_accepted == 0
    assert first.artifact_commit_quarantine_failures == 65

    restarted_spool = LabArtifactCommitSpool(spool.root)
    monkeypatch.setattr(restarted_spool, "quarantine", persist_bad_artifact)
    scheduler.artifact_commit_spool = restarted_spool
    second = scheduler.run_once()

    completed = LabJobReader(store.path).get_job(job.job_id)
    assert second.artifact_commit_quarantine_failures == 1
    assert second.artifact_commits_accepted == 1
    assert completed is not None and completed.result_state is LabResultState.SEALED


def test_cancel_wins_ready_artifact_commit_race_without_reviving_job(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    assert scheduler.lease is not None
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=job.job_id,
            expected_version=job.version,
            reason="cancel before artifact commit",
        ),
    )
    assert (
        store.apply_command(
            cancel,
            lease=scheduler.lease,
            now=clock[0],
        ).status
        == "applied"
    )

    tick = scheduler.run_once()

    cancelled = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.artifact_commits_rejected == 1
    assert cancelled is not None and cancelled.status is JobStatus.CANCELLED
    assert cancelled.result_state is LabResultState.PENDING
    assert record is not None and record.receipt.reason == "invalid_state:cancelled"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


def test_replacement_scheduler_can_cancel_ready_job_without_mutating_ready_first(
    tmp_path: Path,
) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    scheduler.release()
    clock[0] = NOW + timedelta(seconds=5)
    replacement = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "replacement-cancel-commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: clock[0],
    )
    replacement.run_once()
    assert replacement.lease is not None
    still_ready = LabJobReader(store.path).get_job(job.job_id)
    assert still_ready == job

    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=job.job_id,
            expected_version=job.version,
            reason="replacement scheduler cancellation",
        ),
    )
    receipt = store.apply_command(
        cancel,
        lease=replacement.lease,
        now=clock[0],
    )

    cancelled = LabJobReader(store.path).get_job(job.job_id)
    assert receipt.status == "applied"
    assert cancelled is not None and cancelled.status is JobStatus.CANCELLED
    assert cancelled.result_state is LabResultState.PENDING
    assert cancelled.scheduler_fencing_token == replacement.lease.fencing_token


def test_pause_is_rejected_after_complete_result_becomes_ready(tmp_path: Path) -> None:
    store, scheduler, _spool, _artifacts, job, _sealed, _envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    assert scheduler.lease is not None
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=job.job_id,
            expected_version=job.version,
            reason="pause after shards completed",
        ),
    )

    receipt = store.apply_command(
        pause,
        lease=scheduler.lease,
        now=clock[0],
    )

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    assert receipt.status == "rejected"
    assert receipt.reason == "invalid_result_state:ready"
    assert unchanged == job


def test_deadline_wins_ready_artifact_commit_race(tmp_path: Path) -> None:
    deadline = NOW + timedelta(seconds=4)
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(tmp_path, deadline=deadline)
    )

    tick = scheduler.run_once()

    expired = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.deadlines_expired == 1
    assert tick.artifact_commits_rejected == 1
    assert expired is not None and expired.status is JobStatus.FAILED
    assert expired.result_state is LabResultState.PENDING
    assert record is not None and record.receipt.reason == "invalid_state:failed"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


def test_deadline_crossed_during_artifact_verification_wins_commit_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deadline = NOW + timedelta(seconds=5)
    store, scheduler, spool, artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, deadline=deadline)
    )
    original_bind = artifacts.bind_verified_sealed

    @contextmanager
    def advance_clock_during_verification(
        path: Path,
        *,
        indexed_at: datetime,
    ) -> Iterator[object]:
        with original_bind(path, indexed_at=indexed_at) as binding:
            clock[0] = deadline + timedelta(seconds=1)
            yield binding

    monkeypatch.setattr(
        artifacts,
        "bind_verified_sealed",
        advance_clock_during_verification,
    )

    tick = scheduler.run_once()

    expired = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.deadlines_expired == 1
    assert tick.artifact_commits_rejected == 1
    assert expired is not None and expired.status is JobStatus.FAILED
    assert expired.result_state is LabResultState.PENDING
    assert record is not None and record.receipt.reason == "invalid_state:failed"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert spool.pending() == ()


def test_deadline_crossed_during_artifact_exit_check_rolls_back_then_rejects_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deadline = NOW + timedelta(seconds=5)
    store, scheduler, spool, _artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, deadline=deadline)
    )

    def cross_deadline_after_stage(
        _entry: LabArtifactCommitSpoolEntry,
        _binding: object,
    ) -> None:
        clock[0] = deadline + timedelta(seconds=1)

    monkeypatch.setattr(
        scheduler,
        "_after_artifact_commit_staged",
        cross_deadline_after_stage,
    )

    with pytest.raises(ArtifactCommitDeadlineExpiredError, match="deadline"):
        scheduler.run_once()

    rolled_back = LabJobReader(store.path).get_job(job.job_id)
    assert rolled_back == job
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert len(spool.pending()) == 1
    assert not any(
        event.event_type == "job_result_sealed"
        for event in LabJobReader(store.path).list_events(job.job_id)
    )

    monkeypatch.setattr(
        scheduler,
        "_after_artifact_commit_staged",
        lambda _entry, _binding: None,
    )
    replay = scheduler.run_once()

    failed = LabJobReader(store.path).get_job(job.job_id)
    receipt = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert replay.deadlines_expired == 1
    assert replay.artifact_commits_rejected == 1
    assert failed is not None and failed.status is JobStatus.FAILED
    assert failed.result_state is LabResultState.PENDING
    assert receipt is not None and receipt.receipt.status == "rejected"
    assert receipt.receipt.reason == "invalid_state:failed"
    assert spool.pending() == ()


def test_lease_expired_during_artifact_exit_check_rolls_back_for_takeover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, spool, artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )

    def expire_lease_after_stage(
        _entry: LabArtifactCommitSpoolEntry,
        _binding: object,
    ) -> None:
        clock[0] = NOW + timedelta(seconds=61)

    monkeypatch.setattr(
        scheduler,
        "_after_artifact_commit_staged",
        expire_lease_after_stage,
    )

    with pytest.raises(SchedulerLeaseFencedError, match="expired"):
        scheduler.run_once()

    rolled_back = LabJobReader(store.path).get_job(job.job_id)
    assert rolled_back == job
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None
    assert len(spool.pending()) == 1

    replacement = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "replacement-final-check-commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=spool,
        artifact_store=artifacts,
        finalizer_authority_key_provider=_authority_verification_key_provider,
        clock=lambda: clock[0],
    )
    replay = replacement.run_once()

    committed = LabJobReader(store.path).get_job(job.job_id)
    receipt = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert replay.recovered == 1
    assert replay.artifact_commits_accepted == 1
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert receipt is not None and receipt.receipt.status == "accepted"
    assert spool.pending() == ()


def test_new_scheduler_recovers_ready_job_fence_before_commit(tmp_path: Path) -> None:
    store, scheduler, spool, artifacts, job, _sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path)
    )
    scheduler.release()
    clock[0] = NOW + timedelta(seconds=5)
    replacement = LabScheduler(
        store=store,
        spool=LabCommandSpool(tmp_path / "replacement-commands"),
        owner_id="scheduler-b",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        artifact_commit_spool=spool,
        artifact_store=artifacts,
        finalizer_authority_key_provider=_authority_verification_key_provider,
        clock=lambda: clock[0],
    )

    tick = replacement.run_once()

    committed = LabJobReader(store.path).get_job(job.job_id)
    record = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert replacement.lease is not None
    assert tick.recovered == 1
    assert tick.artifact_commits_accepted == 1
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert committed.scheduler_fencing_token == replacement.lease.fencing_token
    assert record is not None
    assert record.receipt.reason == "artifact_committed"
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is not None


def test_stale_scheduler_lease_cannot_stage_ready_artifact_commit(tmp_path: Path) -> None:
    store, scheduler, _spool, artifacts, job, sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    assert scheduler.lease is not None
    stale_lease = scheduler.lease
    scheduler.release()
    clock[0] = NOW + timedelta(seconds=5)
    store.acquire_scheduler_lease(
        owner_id="scheduler-b",
        lease_seconds=60,
        now=clock[0],
    )

    with (
        artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding,
        pytest.raises(SchedulerLeaseFencedError, match="lease"),
        store.stage_artifact_commit(
            envelope,
            binding,
            authority_key_provider=_authority_verification_key_provider,
            lease=stale_lease,
            now=clock[0],
        ),
    ):
        pass

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    assert unchanged == job
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None
    assert LabJobReader(store.path).get_result_artifact(job.job_id) is None


def test_staged_artifact_commit_does_not_begin_until_context_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, scheduler, _spool, artifacts, job, sealed, envelope, clock = (
        _ready_artifact_commit_scenario(tmp_path, publish=False)
    )
    assert scheduler.lease is not None
    connect_calls = 0
    original_connect = store._connect

    def observe_connect() -> sqlite3.Connection:
        nonlocal connect_calls
        connect_calls += 1
        return original_connect()

    monkeypatch.setattr(store, "_connect", observe_connect)
    with artifacts.bind_verified_sealed(sealed.path, indexed_at=clock[0]) as binding:
        stage_scope = store.stage_artifact_commit(
            envelope,
            binding,
            authority_key_provider=_authority_verification_key_provider,
            lease=scheduler.lease,
            now=clock[0],
        )
        assert connect_calls == 0
        with stage_scope as staged:
            assert connect_calls == 1
            staged.rollback()

    unchanged = LabJobReader(store.path).get_job(job.job_id)
    assert unchanged == job
    assert LabJobReader(store.path).get_artifact_commit(envelope.request_id) is None


def test_replayed_request_with_changed_content_is_quarantined(tmp_path: Path) -> None:
    store, scheduler, spool, _artifacts, job, _sealed, envelope, _clock = (
        _ready_artifact_commit_scenario(
            tmp_path,
            commit_spool_type=_CrashBeforeArtifactAckSpool,
        )
    )
    with pytest.raises(RuntimeError, match="after artifact SQLite commit"):
        scheduler.run_once()
    pending = spool.pending()[0]
    changed = LabArtifactCommitEnvelope(
        request_id=envelope.request_id,
        commit=LabArtifactCommit.model_validate(
            {
                **envelope.commit.model_dump(),
                "manifest_hash": "0" * 64,
            }
        ),
    )
    pending.path.write_text(changed.model_dump_json(), encoding="utf-8")

    tick = scheduler.run_once()

    committed = LabJobReader(store.path).get_job(job.job_id)
    original = LabJobReader(store.path).get_artifact_commit(envelope.request_id)
    assert tick.artifact_commits_quarantined == 1
    assert committed is not None and committed.result_state is LabResultState.SEALED
    assert original is not None and original.envelope == envelope
    assert spool.pending() == ()


def test_run_once_processes_only_bounded_batch(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    for _ in range(3):
        spool.publish(_envelope())
    scheduler = _scheduler(store, spool, batch_size=2)

    result = scheduler.run_once()

    assert result.processed == 2
    assert len(spool.pending()) == 1


class _CrashBeforeAckSpool(LabCommandSpool):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.crash = True

    def ack(
        self,
        entry: LabSpoolEntry,
        receipt: LabCommandReceipt,
    ) -> LabAcknowledgedCommand:
        if self.crash:
            self.crash = False
            raise RuntimeError("simulated crash after ledger commit")
        return super().ack(entry, receipt)


def test_commit_before_ack_crash_replays_without_duplicate_effect(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spool = _CrashBeforeAckSpool(tmp_path / "commands")
    envelope = _envelope()
    spool.publish(envelope)
    scheduler = _scheduler(store, spool)

    with pytest.raises(RuntimeError, match="after ledger commit"):
        scheduler.run_once()
    reader = LabJobReader(store.path)
    first_events = reader.list_events(envelope.command.job_id)
    assert len(first_events) == 1
    assert len(spool.pending()) == 1

    replay = scheduler.run_once()

    assert replay.processed == 1
    assert replay.applied == 1
    assert len(reader.list_events(envelope.command.job_id)) == 1
    assert spool.pending() == ()


def test_each_command_mutation_uses_a_fresh_clock_value(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    envelopes = (_envelope(), _envelope())
    for envelope in envelopes:
        spool.publish(envelope)
    moments = iter(
        (
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=3),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: next(moments),
    )

    scheduler.run_once()

    reader = LabJobReader(store.path)
    applied_at = {reader.get_command(envelope.request_id).applied_at for envelope in envelopes}
    assert applied_at == {
        NOW + timedelta(seconds=2),
        NOW + timedelta(seconds=3),
    }


class _SlowAckSpool(LabCommandSpool):
    def __init__(self, root: Path, current: list[datetime]) -> None:
        super().__init__(root)
        self.current = current

    def ack(
        self,
        entry: LabSpoolEntry,
        receipt: LabCommandReceipt,
    ) -> LabAcknowledgedCommand:
        acknowledged = super().ack(entry, receipt)
        self.current[0] += timedelta(seconds=70)
        return acknowledged


def test_slow_ack_expiry_fences_next_command_in_same_tick(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    current = [NOW]
    spool = _SlowAckSpool(tmp_path / "commands", current)
    for _ in range(2):
        spool.publish(_envelope())
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )

    with pytest.raises(SchedulerLeaseFencedError):
        scheduler.run_once()

    assert len(spool.pending()) == 1


def test_bad_json_is_quarantined_and_does_not_block_valid_command(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    bad = spool.pending_dir / "00000000-0000-0000-0000-000000000000.json"
    bad.write_text("{broken", encoding="utf-8")
    valid = _envelope()
    spool.publish(valid)

    result = _scheduler(store, spool).run_once()

    assert result.processed == 1
    assert result.quarantined == 1
    assert result.applied == 1
    assert not bad.exists()
    assert len(tuple(spool.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))) == 1
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None


def test_malformed_filename_is_quarantined_across_restart_without_blocking(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    bad = spool.pending_dir / "not-a-command.json"
    bad.write_text("{broken", encoding="utf-8")
    valid = _envelope()
    spool.publish(valid)
    restarted = LabCommandSpool(spool.root)

    result = _scheduler(store, restarted).run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert restarted.pending() == ()
    assert not bad.exists()
    evidence = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(evidence) == 1
    assert json.loads(evidence[0].read_text(encoding="utf-8"))["source_name"] == bad.name
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None
    assert LabCommandSpool(spool.root).pending() == ()


def test_pending_symlink_is_recorded_without_touching_target_or_blocking_after_restart(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    victim = tmp_path / "external-target.json"
    victim.write_text("do-not-touch", encoding="utf-8")
    symlink = spool.pending_dir / "not-a-command.json"
    symlink.symlink_to(victim)
    valid = _envelope()
    spool.publish(valid)
    restarted = LabCommandSpool(spool.root)

    result = _scheduler(store, restarted).run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert not symlink.exists()
    assert not symlink.is_symlink()
    assert victim.read_text(encoding="utf-8") == "do-not-touch"
    artifacts = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(artifacts) == 1
    assert artifacts[0].is_file()
    assert not artifacts[0].is_symlink()
    metadata = json.loads(artifacts[0].read_text(encoding="utf-8"))
    assert metadata["source_name"] == "not-a-command.json"
    assert metadata["link_target"] == str(victim)
    assert "invalid_envelope" in metadata["reason"]
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None
    assert LabCommandSpool(spool.root).pending() == ()


def test_semantic_request_conflict_is_quarantined_and_does_not_block_next_command(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    scheduler.run_once()
    assert scheduler.lease is not None
    request_id = uuid4()
    accepted = LabCommandEnvelope(
        request_id=request_id,
        command=SubmitJobCommand(job_id=uuid4(), spec=_spec(), max_attempts=3),
    )
    store.apply_command(accepted, lease=scheduler.lease, now=NOW)
    conflict = LabCommandEnvelope(
        request_id=request_id,
        command=SubmitJobCommand(job_id=uuid4(), spec=_spec(), max_attempts=3),
    )
    valid = _envelope()
    spool.publish(conflict)
    spool.publish(valid)

    result = scheduler.run_once()

    assert result.quarantined == 1
    assert result.processed == 1
    assert result.applied == 1
    assert spool.pending() == ()
    quarantine_records = tuple(spool.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(quarantine_records) == 1
    assert "request_content_conflict" in quarantine_records[0].read_text(encoding="utf-8")
    assert LabJobReader(store.path).get_job(valid.command.job_id) is not None


def test_second_scheduler_is_refused_while_first_lease_is_valid(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    first = _scheduler(store, spool, owner="scheduler-a")
    second = _scheduler(store, spool, owner="scheduler-b")
    first.run_once()

    with pytest.raises(SchedulerLeaseUnavailableError):
        second.run_once()


def test_scheduler_takeover_recovers_old_running_job_to_checkpointed(
    tmp_path: Path,
) -> None:
    store, spool = _components(tmp_path)
    old = store.acquire_scheduler_lease(
        owner_id="scheduler-old",
        lease_seconds=10,
        now=NOW,
    )
    envelope = _envelope()
    store.apply_command(envelope, lease=old, now=NOW)
    store.transition_job(
        envelope.command.job_id,
        expected_version=0,
        target_status=JobStatus.RUNNING,
        lease=old,
        reason="started",
        now=NOW + timedelta(seconds=1),
    )
    takeover = _scheduler(
        store,
        spool,
        owner="scheduler-new",
        now=NOW + timedelta(seconds=11),
    )

    result = takeover.run_once()
    recovered = LabJobReader(store.path).get_job(envelope.command.job_id)

    assert result.lease_acquired is True
    assert result.recovered == 1
    assert recovered is not None
    assert recovered.status is JobStatus.CHECKPOINTED


def test_subsequent_tick_renews_existing_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    current = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )
    first = scheduler.run_once()
    current[0] = NOW + timedelta(seconds=20)

    second = scheduler.run_once()

    assert first.lease_acquired is True
    assert second.lease_acquired is False
    assert scheduler.lease is not None
    assert scheduler.lease.heartbeat_at == NOW + timedelta(seconds=20)
    assert len(LabJobReader(store.path).list_leases()) == 1


def test_tick_before_heartbeat_deadline_does_not_write_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    current = [NOW]
    scheduler = LabScheduler(
        store=store,
        spool=spool,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=10,
        clock=lambda: current[0],
    )
    scheduler.run_once()
    current[0] = NOW + timedelta(seconds=5)

    scheduler.run_once()

    assert scheduler.lease is not None
    assert scheduler.lease.heartbeat_at == NOW
    assert LabJobReader(store.path).list_leases()[0].heartbeat_at == NOW


def test_run_forever_stops_cooperatively_and_releases_lease(tmp_path: Path) -> None:
    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    calls = 0
    original = scheduler.run_once

    def one_tick() -> SchedulerTickResult:
        nonlocal calls
        calls += 1
        result = original()
        scheduler.request_stop()
        return result

    scheduler.run_once = one_tick  # type: ignore[method-assign]
    thread = Thread(target=scheduler.run_forever)
    thread.start()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert calls == 1
    assert LabJobReader(store.path).list_leases()[-1].released_at is not None


def test_run_forever_logs_only_nonzero_structured_tick_anomalies(tmp_path: Path) -> None:
    from loguru import logger

    store, spool = _components(tmp_path)
    scheduler = _scheduler(store, spool)
    baseline = scheduler.run_once()
    scheduler.release()
    records: list[dict[str, object]] = []
    sink = logger.add(
        lambda message: records.append(dict(message.record["extra"])),
        level="WARNING",
    )

    def anomaly_tick() -> SchedulerTickResult:
        scheduler.request_stop()
        return baseline.model_copy(
            update={
                "plans_failed": 1,
                "claim_revoke_failures": 2,
            }
        )

    scheduler.run_once = anomaly_tick  # type: ignore[method-assign]
    try:
        scheduler._log_tick_anomalies(baseline)
        scheduler.run_forever()
    finally:
        logger.remove(sink)

    scheduler_records = [record for record in records if record.get("component") == "lab_scheduler"]
    assert scheduler_records == [
        {
            "component": "lab_scheduler",
            "owner_id": "scheduler-a",
            "failure": "tick_anomalies",
            "anomaly_counts": {
                "plans_failed": 1,
                "claim_revoke_failures": 2,
            },
        }
    ]
