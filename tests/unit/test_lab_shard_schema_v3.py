from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

import rquant.lab_jobs as lab_jobs
from rquant.lab_jobs import LabJobReader, LabJobStore

from .test_lab_jobs import NOW, _create_609c599_v1_fixture, _spec


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
    assert str(shard.shard_id) == shard_id
    assert shard.adapter_id == "legacy-v2"
    assert shard.adapter_version == "v0"
    assert shard.payload_json == "{}"
    assert shard.claim_generation == 0
    assert shard.claim_token is None
    assert shard.result_manifest_hash is None

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert connection.execute("SELECT COUNT(*) FROM lab_command").fetchone()[0] == 2


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
