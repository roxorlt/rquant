from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommandEnvelope,
    PauseJobCommand,
    RequestContentConflictError,
    ResumeJobCommand,
    RetryJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    ControlIntent,
    InvalidJobTransitionError,
    InvalidStoredJobError,
    JobStatus,
    LabArtifactRecord,
    LabCommandRecord,
    LabEventRecord,
    LabJobReader,
    LabJobRecord,
    LabJobStore,
    LabLeaseRecord,
    LabShardRecord,
    SchedulerLeaseFencedError,
    SchedulerLeaseUnavailableError,
    StaleJobVersionError,
)
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchJobType,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)

NOW = datetime(2026, 7, 24, 1, 0, tzinfo=UTC)


def _spec(
    *,
    job_type: ResearchJobType = ResearchJobType.STRATEGY_REPLAY,
    resource_class: ResourceClass = ResourceClass.STANDARD,
) -> ResearchRunSpec:
    return ResearchRunSpec(
        job_type=job_type,
        parameters=ResearchRunParameters(
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 14),
        ),
        code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="a" * 64,
            binding_hash="b" * 64,
        ),
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
        resource_class=resource_class,
        deadline=datetime(2026, 7, 25, 2, tzinfo=UTC),
        research_status="comparable",
    )


def _submit(
    *,
    request_id: UUID | None = None,
    job_id: UUID | None = None,
    spec: ResearchRunSpec | None = None,
    max_attempts: int = 3,
) -> LabCommandEnvelope:
    return LabCommandEnvelope(
        request_id=request_id or uuid4(),
        command=SubmitJobCommand(
            job_id=job_id or uuid4(),
            spec=spec or _spec(),
            max_attempts=max_attempts,
        ),
    )


def _store(tmp_path: Path, *, timeout: int = 1_234) -> LabJobStore:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3", busy_timeout_ms=timeout)
    store.initialize()
    return store


def _lease(
    store: LabJobStore,
    *,
    owner: str = "scheduler-a",
    now: datetime = NOW,
    seconds: int = 60,
) -> LabLeaseRecord:
    return store.acquire_scheduler_lease(
        owner_id=owner,
        lease_seconds=seconds,
        now=now,
    )


def _submit_job(
    store: LabJobStore,
    lease: LabLeaseRecord,
    *,
    max_attempts: int = 3,
) -> LabJobRecord:
    envelope = _submit(max_attempts=max_attempts)
    receipt = store.apply_command(envelope, lease=lease, now=NOW)
    assert receipt.status == "applied"
    job = LabJobReader(store.path).get_job(envelope.command.job_id)
    assert job is not None
    return job


def _transition_to(
    store: LabJobStore,
    lease: LabLeaseRecord,
    target: JobStatus,
) -> LabJobRecord:
    job = _submit_job(store, lease)
    if target is JobStatus.QUEUED:
        return job
    job = store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="worker started",
        now=NOW + timedelta(seconds=1),
    )
    if target is JobStatus.RUNNING:
        return job
    if target is JobStatus.CHECKPOINTED:
        return store.transition_job(
            job.job_id,
            expected_version=job.version,
            target_status=target,
            lease=lease,
            reason="checkpoint",
            now=NOW + timedelta(seconds=2),
        )
    return store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=target,
        lease=lease,
        reason="terminal",
        recoverable=target is JobStatus.FAILED,
        now=NOW + timedelta(seconds=2),
    )


def _count(path: Path, table: str) -> int:
    with sqlite3.connect(path) as connection:
        row = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    assert row is not None
    return int(row[0])


def test_initialize_creates_v1_six_table_schema_and_required_pragmas(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as connection:
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        application_id = connection.execute("PRAGMA application_id").fetchone()[0]
        user_version = connection.execute("PRAGMA user_version").fetchone()[0]
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
        schema_sql = " ".join(
            str(row[0])
            for row in connection.execute("SELECT sql FROM sqlite_master WHERE type = 'table'")
        ).upper()

    assert {
        "lab_command",
        "lab_job",
        "lab_shard",
        "lab_event",
        "lab_lease",
        "lab_artifact",
    } <= tables
    assert application_id == LabJobStore.APPLICATION_ID
    assert user_version == 1
    assert str(journal_mode).lower() == "wal"
    assert synchronous == 2
    assert "STRICT" not in schema_sql

    pragmas = store.connection_pragmas()
    assert pragmas.journal_mode == "wal"
    assert pragmas.synchronous == 2
    assert pragmas.foreign_keys == 1
    assert pragmas.busy_timeout_ms == 1_234


def test_reader_is_readonly_does_not_create_missing_database(tmp_path: Path) -> None:
    path = tmp_path / "missing.sqlite3"
    reader = LabJobReader(path)

    with pytest.raises(sqlite3.OperationalError):
        reader.get_job(uuid4())

    assert not path.exists()


def test_submit_roundtrips_validated_spec_and_typed_empty_rows(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    spec = _spec(
        job_type=ResearchJobType.ABLATION,
        resource_class=ResourceClass.HEAVY,
    )
    envelope = _submit(spec=spec)

    receipt = store.apply_command(envelope, lease=lease, now=NOW)
    reader = LabJobReader(store.path)
    job = reader.get_job(envelope.command.job_id)

    assert receipt.status == "applied"
    assert receipt.job_version == 0
    assert isinstance(job, LabJobRecord)
    assert job is not None
    assert job.spec == spec
    assert job.spec.spec_hash == spec.spec_hash
    assert job.job_type is ResearchJobType.ABLATION
    assert job.resource_class is ResourceClass.HEAVY
    assert job.status is JobStatus.QUEUED
    assert job.control_intent is ControlIntent.NONE
    assert job.attempt_count == 0
    command_record = reader.get_command(envelope.request_id)
    assert isinstance(command_record, LabCommandRecord)
    assert command_record is not None
    assert command_record.envelope == envelope
    assert command_record.receipt == receipt
    assert all(isinstance(row, LabEventRecord) for row in reader.list_events(job.job_id))
    assert reader.list_shards(job.job_id) == ()
    assert reader.list_artifacts(job.job_id) == ()
    assert isinstance(reader.list_leases()[0], LabLeaseRecord)
    assert LabShardRecord.model_fields
    assert LabArtifactRecord.model_fields


def test_same_request_and_hash_is_exactly_once_without_second_event(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()

    first = store.apply_command(envelope, lease=lease, now=NOW)
    event_count = _count(store.path, "lab_event")
    second = store.apply_command(envelope, lease=lease, now=NOW + timedelta(seconds=1))

    assert second == first
    assert _count(store.path, "lab_command") == 1
    assert _count(store.path, "lab_job") == 1
    assert _count(store.path, "lab_event") == event_count


def test_same_request_with_different_hash_conflicts_with_zero_modification(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    request_id = uuid4()
    first = _submit(request_id=request_id)
    store.apply_command(first, lease=lease, now=NOW)
    before = tuple(_count(store.path, table) for table in ("lab_command", "lab_job", "lab_event"))

    with pytest.raises(RequestContentConflictError):
        store.apply_command(
            _submit(request_id=request_id),
            lease=lease,
            now=NOW + timedelta(seconds=1),
        )

    assert (
        tuple(_count(store.path, table) for table in ("lab_command", "lab_job", "lab_event"))
        == before
    )


def test_reused_job_id_is_durably_rejected_and_replayed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job_id = uuid4()
    store.apply_command(_submit(job_id=job_id), lease=lease, now=NOW)
    reused = _submit(job_id=job_id)

    first = store.apply_command(reused, lease=lease, now=NOW + timedelta(seconds=1))
    event_count = _count(store.path, "lab_event")
    replay = store.apply_command(reused, lease=lease, now=NOW + timedelta(seconds=2))

    assert first.status == "rejected"
    assert first.reason == "job_id_reused"
    assert replay == first
    assert _count(store.path, "lab_job") == 1
    assert _count(store.path, "lab_event") == event_count


def test_illegal_cancel_is_durably_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _transition_to(store, lease, JobStatus.SUCCEEDED)
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=job.job_id,
            expected_version=job.version,
            reason="too late",
        ),
    )

    first = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=3))
    replay = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=4))

    assert first.status == "rejected"
    assert first.reason == "invalid_state:succeeded"
    assert replay == first
    assert LabJobReader(store.path).get_job(job.job_id).status is JobStatus.SUCCEEDED


def test_stale_command_version_is_durably_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    event_count = _count(store.path, "lab_event")
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=job.job_id,
            expected_version=job.version + 1,
            reason="stale UI",
        ),
    )

    first = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=1))
    replay = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=2))

    assert first.status == "rejected"
    assert first.reason == f"stale_version:{job.version}"
    assert replay == first
    assert _count(store.path, "lab_event") == event_count


def test_pause_and_resume_commands_are_persistent_and_exactly_once(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=running.job_id,
            expected_version=running.version,
            reason="free resources",
        ),
    )

    paused_receipt = store.apply_command(
        pause,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    paused = LabJobReader(store.path).get_job(running.job_id)
    assert paused is not None
    assert paused_receipt.status == "applied"
    assert paused_receipt.reason == "pause_requested"
    assert paused.status is JobStatus.RUNNING
    assert paused.control_intent is ControlIntent.PAUSE_REQUESTED
    event_count = _count(store.path, "lab_event")
    assert (
        store.apply_command(
            pause,
            lease=lease,
            now=NOW + timedelta(seconds=4),
        )
        == paused_receipt
    )
    assert _count(store.path, "lab_event") == event_count

    checkpointed = store.transition_job(
        paused.job_id,
        expected_version=paused.version,
        target_status=JobStatus.CHECKPOINTED,
        lease=lease,
        reason="worker reached safe point",
        now=NOW + timedelta(seconds=5),
    )
    assert checkpointed.control_intent is ControlIntent.NONE

    resume = LabCommandEnvelope(
        request_id=uuid4(),
        command=ResumeJobCommand(
            job_id=checkpointed.job_id,
            expected_version=checkpointed.version,
            reason="capacity restored",
        ),
    )
    resumed_receipt = store.apply_command(
        resume,
        lease=lease,
        now=NOW + timedelta(seconds=6),
    )
    resumed = LabJobReader(store.path).get_job(paused.job_id)
    assert resumed is not None
    assert resumed_receipt.status == "applied"
    assert resumed.status is JobStatus.RUNNING
    assert resumed.control_intent is ControlIntent.NONE


def test_resume_can_withdraw_unacknowledged_pause_intent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=running.job_id,
            expected_version=running.version,
            reason="pause",
        ),
    )
    store.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=3))
    paused = LabJobReader(store.path).get_job(running.job_id)
    assert paused is not None
    resume = LabCommandEnvelope(
        request_id=uuid4(),
        command=ResumeJobCommand(
            job_id=paused.job_id,
            expected_version=paused.version,
            reason="withdraw pause",
        ),
    )

    receipt = store.apply_command(
        resume,
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    resumed = LabJobReader(store.path).get_job(running.job_id)

    assert receipt.status == "applied"
    assert receipt.reason == "pause_withdrawn"
    assert resumed is not None
    assert resumed.status is JobStatus.RUNNING
    assert resumed.control_intent is ControlIntent.NONE


def test_running_cancel_records_intent_before_worker_terminal_ack(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    running = _transition_to(store, lease, JobStatus.RUNNING)
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(
            job_id=running.job_id,
            expected_version=running.version,
            reason="operator cancel",
        ),
    )

    receipt = store.apply_command(
        cancel,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    requested = LabJobReader(store.path).get_job(running.job_id)

    assert receipt.status == "applied"
    assert receipt.reason == "cancel_requested"
    assert requested is not None
    assert requested.status is JobStatus.RUNNING
    assert requested.control_intent is ControlIntent.CANCEL_REQUESTED

    cancelled = store.transition_job(
        requested.job_id,
        expected_version=requested.version,
        target_status=JobStatus.CANCELLED,
        lease=lease,
        reason="worker invalidated claim",
        now=NOW + timedelta(seconds=4),
    )
    assert cancelled.status is JobStatus.CANCELLED
    assert cancelled.control_intent is ControlIntent.NONE


def test_pause_in_wrong_state_is_durably_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    queued = _submit_job(store, lease)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(
            job_id=queued.job_id,
            expected_version=queued.version,
            reason="not running",
        ),
    )

    receipt = store.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=1))

    assert receipt.status == "rejected"
    assert receipt.reason == "invalid_state:queued"


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.QUEUED, JobStatus.CANCELLED),
        (JobStatus.RUNNING, JobStatus.CHECKPOINTED),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.CANCELLED),
        (JobStatus.CHECKPOINTED, JobStatus.RUNNING),
        (JobStatus.CHECKPOINTED, JobStatus.CANCELLED),
    ],
)
def test_complete_state_matrix_allows_only_documented_edges(
    tmp_path: Path,
    source: JobStatus,
    target: JobStatus,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _transition_to(store, lease, source)

    transitioned = store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=target,
        lease=lease,
        reason="matrix",
        recoverable=target is JobStatus.FAILED,
        now=NOW + timedelta(seconds=10),
    )

    assert transitioned.status is target
    assert transitioned.version == job.version + 1


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (JobStatus.QUEUED, JobStatus.SUCCEEDED),
        (JobStatus.QUEUED, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.QUEUED),
        (JobStatus.CHECKPOINTED, JobStatus.SUCCEEDED),
        (JobStatus.FAILED, JobStatus.RUNNING),
        (JobStatus.SUCCEEDED, JobStatus.RUNNING),
        (JobStatus.CANCELLED, JobStatus.QUEUED),
    ],
)
def test_complete_state_matrix_rejects_undocumented_edges(
    tmp_path: Path,
    source: JobStatus,
    target: JobStatus,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _transition_to(store, lease, source)

    with pytest.raises(InvalidJobTransitionError):
        store.transition_job(
            job.job_id,
            expected_version=job.version,
            target_status=target,
            lease=lease,
            reason="invalid",
            now=NOW + timedelta(seconds=10),
        )


def test_transition_rejects_stale_version_without_event(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    event_count = _count(store.path, "lab_event")

    with pytest.raises(StaleJobVersionError):
        store.transition_job(
            job.job_id,
            expected_version=job.version + 1,
            target_status=JobStatus.RUNNING,
            lease=lease,
            reason="stale",
            now=NOW + timedelta(seconds=1),
        )

    assert _count(store.path, "lab_event") == event_count


def test_failed_job_retries_only_explicitly_when_attempt_budget_remains(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    failed = _transition_to(store, lease, JobStatus.FAILED)
    command = LabCommandEnvelope(
        request_id=uuid4(),
        command=RetryJobCommand(
            job_id=failed.job_id,
            expected_version=failed.version,
            reason="source recovered",
        ),
    )

    receipt = store.apply_command(command, lease=lease, now=NOW + timedelta(seconds=3))
    retried = LabJobReader(store.path).get_job(failed.job_id)

    assert receipt.status == "applied"
    assert retried is not None
    assert retried.status is JobStatus.QUEUED
    assert retried.version == failed.version + 1


def test_retry_is_durably_rejected_after_attempt_budget_exhausted(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease, max_attempts=1)
    running = store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="start",
        now=NOW + timedelta(seconds=1),
    )
    failed = store.transition_job(
        job.job_id,
        expected_version=running.version,
        target_status=JobStatus.FAILED,
        lease=lease,
        reason="failed",
        recoverable=True,
        now=NOW + timedelta(seconds=2),
    )
    retry = LabCommandEnvelope(
        request_id=uuid4(),
        command=RetryJobCommand(
            job_id=job.job_id,
            expected_version=failed.version,
            reason="again",
        ),
    )

    receipt = store.apply_command(retry, lease=lease, now=NOW + timedelta(seconds=3))

    assert receipt.status == "rejected"
    assert receipt.reason == "attempts_exhausted"


def test_active_scheduler_lease_rejects_second_owner(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _lease(store, owner="scheduler-a")

    with pytest.raises(SchedulerLeaseUnavailableError):
        _lease(store, owner="scheduler-b", now=NOW + timedelta(seconds=30))

    assert first.released_at is None
    assert len(LabJobReader(store.path).list_leases()) == 1


def test_expired_lease_is_released_before_fenced_takeover(tmp_path: Path) -> None:
    store = _store(tmp_path)
    old = _lease(store, owner="scheduler-a", seconds=10)
    new = _lease(
        store,
        owner="scheduler-b",
        now=NOW + timedelta(seconds=11),
        seconds=20,
    )
    leases = LabJobReader(store.path).list_leases()

    assert new.fencing_token == old.fencing_token + 1
    assert leases[0].released_at == NOW + timedelta(seconds=11)
    assert leases[1] == new


def test_old_owner_cannot_complete_after_takeover_and_recovery(tmp_path: Path) -> None:
    store = _store(tmp_path)
    old = _lease(store, owner="scheduler-a", seconds=10)
    running = _transition_to(store, old, JobStatus.RUNNING)
    takeover_at = NOW + timedelta(seconds=11)
    new = _lease(store, owner="scheduler-b", now=takeover_at, seconds=60)
    recovered = store.recover_expired_jobs(new, now=takeover_at)

    assert recovered[0].status is JobStatus.CHECKPOINTED
    with pytest.raises(SchedulerLeaseFencedError):
        store.transition_job(
            running.job_id,
            expected_version=running.version,
            target_status=JobStatus.SUCCEEDED,
            lease=old,
            reason="late completion",
            now=takeover_at,
        )

    resumed = store.transition_job(
        running.job_id,
        expected_version=recovered[0].version,
        target_status=JobStatus.RUNNING,
        lease=new,
        reason="resume",
        now=takeover_at + timedelta(seconds=1),
    )
    completed = store.transition_job(
        running.job_id,
        expected_version=resumed.version,
        target_status=JobStatus.SUCCEEDED,
        lease=new,
        reason="complete",
        now=takeover_at + timedelta(seconds=2),
    )
    assert completed.status is JobStatus.SUCCEEDED


def test_heartbeat_renews_without_appending_event(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    event_count = _count(store.path, "lab_event")

    renewed = store.renew_scheduler_lease(
        lease,
        lease_seconds=60,
        now=NOW + timedelta(seconds=20),
    )

    assert renewed.heartbeat_at == NOW + timedelta(seconds=20)
    assert renewed.expires_at == NOW + timedelta(seconds=80)
    assert _count(store.path, "lab_event") == event_count


def test_submit_transaction_rolls_back_when_event_insert_fails(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_lab_event
            BEFORE INSERT ON lab_event
            BEGIN
                SELECT RAISE(ABORT, 'event rejected');
            END
            """
        )
    envelope = _submit()

    with pytest.raises(sqlite3.IntegrityError, match="event rejected"):
        store.apply_command(envelope, lease=lease, now=NOW)

    assert _count(store.path, "lab_command") == 0
    assert _count(store.path, "lab_job") == 0
    assert _count(store.path, "lab_event") == 0


def test_readonly_reader_sees_committed_wal_data(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    envelope = _submit()
    store.apply_command(envelope, lease=lease, now=NOW)

    reader = LabJobReader(store.path)
    assert reader.get_job(envelope.command.job_id) is not None
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        reader.execute_for_test("DELETE FROM lab_job")


@pytest.mark.parametrize(
    "column",
    ["spec_json", "spec_hash", "job_type", "resource_class", "deadline"],
)
def test_reader_fails_closed_on_tampered_spec_or_denormalized_columns(
    tmp_path: Path,
    column: str,
) -> None:
    store = _store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    replacements = {
        "spec_json": "{}",
        "spec_hash": "f" * 64,
        "job_type": ResearchJobType.ABLATION.value,
        "resource_class": ResourceClass.HEAVY.value,
        "deadline": "2030-01-01T00:00:00+00:00",
    }
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            f"UPDATE lab_job SET {column} = ? WHERE job_id = ?",
            (replacements[column], str(job.job_id)),
        )

    with pytest.raises(InvalidStoredJobError):
        LabJobReader(store.path).get_job(job.job_id)
