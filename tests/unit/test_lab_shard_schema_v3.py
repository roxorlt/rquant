from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

import rquant.lab_jobs as lab_jobs
from rquant.lab_job_protocol import LabCommandEnvelope, ResumeJobCommand
from rquant.lab_jobs import (
    InvalidStoredJobError,
    JobStatus,
    LabJobReader,
    LabJobStore,
    ShardStatus,
)
from rquant.lab_shard_protocol import LabShardDefinition, LabShardHeartbeat, LabWorkerReport

from .test_lab_jobs import NOW, _create_609c599_v1_fixture, _lease, _spec


def _create_real_v2_fixture(path: Path) -> tuple[str, str]:
    _create_609c599_v1_fixture(path)
    job_id = str(uuid4())
    shard_id = str(uuid4())
    spec = _spec()
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN IMMEDIATE")
        lab_jobs._migrate_v1_to_v2(connection)
        for statement in lab_jobs._V2_SCHEMA_STATEMENTS:
            connection.execute(statement)
        timestamp = NOW.isoformat(timespec="microseconds")
        connection.execute(
            """
            INSERT INTO lab_job (
                job_id, spec_json, spec_hash, job_type, resource_class,
                deadline, status, control_intent, version, attempt_count,
                max_attempts, recoverable, scheduler_fencing_token,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 'none', 0, 0, 3, 0, NULL, ?, ?)
            """,
            (
                job_id,
                spec.model_dump_json(round_trip=True),
                spec.spec_hash,
                spec.job_type.value,
                spec.resource_class.value,
                spec.deadline.isoformat(timespec="microseconds"),
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version,
                attempt_count, max_attempts, worker_id,
                scheduler_fencing_token, checkpoint_json, created_at, updated_at
            ) VALUES (?, ?, 0, 'queued', 0, 0, 3, NULL, NULL, NULL, ?, ?)
            """,
            (shard_id, job_id, timestamp, timestamp),
        )
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
    return job_id, shard_id


def test_initialize_creates_v3_report_table_and_claim_columns(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()

    with sqlite3.connect(store.path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        shard_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(lab_shard)")}
        report_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(lab_worker_report)")
        }

    assert version == 3
    assert "lab_worker_report" in tables
    assert "lab_scheduler_state" in tables
    assert {
        "plan_hash",
        "adapter_id",
        "adapter_version",
        "payload_json",
        "payload_hash",
        "claim_token",
        "claim_generation",
        "claimed_at",
        "heartbeat_at",
        "lease_expires_at",
        "result_manifest_hash",
        "failure_json",
        "finished_at",
    } <= shard_columns
    assert {
        "report_id",
        "content_hash",
        "report_json",
        "receipt_json",
        "claim_generation",
        "scheduler_fencing_token",
    } <= report_columns


def test_initialize_migrates_real_v2_shard_and_backfills_readable_identity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    job_id, shard_id = _create_real_v2_fixture(path)

    store = LabJobStore(path)
    store.initialize()

    reader = LabJobReader(path)
    job = reader.get_job(lab_jobs.UUID(job_id))
    shards = reader.list_shards(lab_jobs.UUID(job_id))
    assert job is not None
    assert len(shards) == 1
    shard = shards[0]
    expected_definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="legacy-v2",
        adapter_version="v0",
        plan_hash=lab_jobs._LEGACY_PLAN_HASH,
        payload_json="{}",
    )
    assert shard.shard_id == expected_definition.shard_id
    assert str(shard.shard_id) != shard_id
    assert shard.adapter_id == "legacy-v2"
    assert shard.adapter_version == "v0"
    assert shard.payload_json == "{}"
    assert shard.claim_generation == 0
    assert shard.claim_token is None
    assert shard.result_manifest_hash is None

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert connection.execute("SELECT COUNT(*) FROM lab_command").fetchone()[0] == 2


def test_v2_running_shard_is_safely_requeued_and_fenced_during_migration(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    job_id, shard_id = _create_real_v2_fixture(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE lab_job
            SET status = 'running', version = 1, scheduler_fencing_token = 7
            WHERE job_id = ?
            """,
            (job_id,),
        )
        connection.execute(
            """
            UPDATE lab_shard
            SET status = 'running', version = 1, attempt_count = 1,
                worker_id = 'legacy-worker', scheduler_fencing_token = 7
            WHERE job_id = ? AND shard_id = ?
            """,
            (job_id, shard_id),
        )

    store = LabJobStore(path)
    store.initialize()
    restarted = LabJobStore(path)
    restarted.initialize()
    reader = LabJobReader(path)
    job = reader.get_job(lab_jobs.UUID(job_id))
    shard = reader.list_shards(lab_jobs.UUID(job_id))[0]

    assert job is not None and job.status is JobStatus.RUNNING
    assert shard.status is ShardStatus.QUEUED
    assert shard.version == 2
    assert shard.checkpoint_json is None
    assert (
        shard.worker_id,
        shard.scheduler_fencing_token,
        shard.claim_token,
        shard.claimed_at,
        shard.heartbeat_at,
        shard.lease_expires_at,
    ) == (None, None, None, None, None, None)
    lease = _lease(restarted, owner="migration-scheduler", now=NOW + timedelta(seconds=2))
    forged = LabWorkerReport(
        report_id=uuid4(),
        job_id=lab_jobs.UUID(job_id),
        shard_id=lab_jobs.UUID(shard_id),
        spec_hash=job.spec_hash,
        payload_hash=shard.payload_hash,
        worker_id="legacy-worker",
        claim_token=uuid4(),
        claim_generation=1,
        scheduler_fencing_token=lease.fencing_token,
        reported_at=NOW + timedelta(seconds=3),
        body=LabShardHeartbeat(lease_extension_seconds=30),
    )
    rejected = restarted.apply_worker_report(
        forged,
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert rejected.status == "rejected"
    claim = restarted.claim_next_shard(
        worker_id="fresh-worker",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert claim is not None
    assert claim.worker_id == "fresh-worker"
    assert claim.claim_generation == 1


def test_v2_checkpointed_shard_becomes_claimable_only_after_resume(tmp_path: Path) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    job_id, shard_id = _create_real_v2_fixture(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE lab_job SET status = 'checkpointed', version = 1 WHERE job_id = ?",
            (job_id,),
        )
        connection.execute(
            """
            UPDATE lab_shard
            SET status = 'checkpointed', version = 1,
                worker_id = 'legacy-worker', scheduler_fencing_token = 7,
                checkpoint_json = '{"cursor":3}'
            WHERE job_id = ? AND shard_id = ?
            """,
            (job_id, shard_id),
        )

    store = LabJobStore(path)
    store.initialize()
    store = LabJobStore(path)
    store.initialize()
    reader = LabJobReader(path)
    before = reader.get_job(lab_jobs.UUID(job_id))
    shard = reader.list_shards(lab_jobs.UUID(job_id))[0]
    assert before is not None and before.status is JobStatus.CHECKPOINTED
    assert shard.status is ShardStatus.QUEUED
    assert shard.checkpoint_json is None
    assert shard.worker_id is None
    lease = _lease(store, owner="resume-scheduler", now=NOW + timedelta(seconds=2))
    assert (
        store.claim_next_shard(
            worker_id="premature-worker",
            shard_lease_seconds=30,
            lease=lease,
            now=NOW + timedelta(seconds=3),
        )
        is None
    )

    resumed = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=ResumeJobCommand(
                job_id=lab_jobs.UUID(job_id),
                expected_version=before.version,
                reason="resume migrated checkpoint",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    claim = store.claim_next_shard(
        worker_id="fresh-worker",
        shard_lease_seconds=30,
        lease=lease,
        now=NOW + timedelta(seconds=5),
    )

    assert resumed.status == "applied"
    assert claim is not None and claim.job_id == lab_jobs.UUID(job_id)


@pytest.mark.parametrize(
    "terminal_status",
    [ShardStatus.SUCCEEDED, ShardStatus.FAILED, ShardStatus.CANCELLED],
)
def test_v2_migration_normalizes_legacy_terminal_shard_claim_identity(
    tmp_path: Path,
    terminal_status: ShardStatus,
) -> None:
    path = tmp_path / f"{terminal_status.value}.sqlite3"
    job_id, shard_id = _create_real_v2_fixture(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE lab_job SET status = ?, version = 1 WHERE job_id = ?",
            (terminal_status.value, job_id),
        )
        connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = 1, worker_id = 'legacy-worker',
                scheduler_fencing_token = 7, checkpoint_json = '{"cursor":3}'
            WHERE job_id = ? AND shard_id = ?
            """,
            (terminal_status.value, job_id, shard_id),
        )

    LabJobStore(path).initialize()

    shard = LabJobReader(path).list_shards(lab_jobs.UUID(job_id))[0]
    assert shard.status is terminal_status
    assert shard.finished_at == NOW
    assert shard.updated_at == NOW
    assert shard.checkpoint_json is None
    assert (
        shard.worker_id,
        shard.scheduler_fencing_token,
        shard.claim_token,
        shard.claimed_at,
        shard.heartbeat_at,
        shard.lease_expires_at,
    ) == (None, None, None, None, None, None)


def test_reader_rejects_terminal_legacy_shard_with_claim_identity(tmp_path: Path) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    job_id, _legacy_shard_id = _create_real_v2_fixture(path)
    LabJobStore(path).initialize()
    shard_id = str(LabJobReader(path).list_shards(lab_jobs.UUID(job_id))[0].shard_id)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE lab_job SET status = 'cancelled', version = 1 WHERE job_id = ?
            """,
            (job_id,),
        )
        connection.execute(
            """
            UPDATE lab_shard
            SET status = 'cancelled', version = 1, worker_id = 'tampered',
                scheduler_fencing_token = 9, finished_at = updated_at
            WHERE job_id = ? AND shard_id = ?
            """,
            (job_id, shard_id),
        )

    with pytest.raises(InvalidStoredJobError, match="terminal shard retains claim identity"):
        LabJobReader(path).list_shards(lab_jobs.UUID(job_id))


def test_v2_to_v3_migration_fault_rolls_back_all_schema_and_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    job_id, shard_id = _create_real_v2_fixture(path)
    original = path.read_bytes()

    def explode(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("fault after v3 DDL")

    monkeypatch.setattr(lab_jobs, "_validate_v3_schema", explode)
    with pytest.raises(RuntimeError, match="fault after v3 DDL"):
        LabJobStore(path).initialize()

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(lab_shard)")}
        assert "claim_generation" not in columns
        assert connection.execute("SELECT job_id FROM lab_job").fetchone()[0] == job_id
        assert connection.execute("SELECT shard_id FROM lab_shard").fetchone()[0] == shard_id
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='lab_worker_report'"
            ).fetchone()[0]
            == 0
        )

    # WAL-free fixture bytes remain exactly unchanged after the rolled-back transaction.
    assert path.read_bytes() == original


def test_v3_identity_validation_rejects_incomplete_worker_report_table(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TABLE lab_worker_report")
        connection.execute("CREATE TABLE lab_worker_report (report_id TEXT PRIMARY KEY)")

    with pytest.raises(lab_jobs.LabDatabaseIdentityError, match="lab_worker_report columns"):
        LabJobReader(store.path).get_job(uuid4())
