from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

import rquant.lab_jobs as lab_jobs
from rquant.lab_job_protocol import (
    LabCommandEnvelope,
    ResumeJobCommand,
    RetryJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    InvalidStoredJobError,
    JobStatus,
    LabDatabaseIdentityError,
    LabJobReader,
    LabJobStore,
    ShardStatus,
)
from rquant.lab_shard_protocol import LabShardDefinition, LabShardHeartbeat, LabWorkerReport

from .test_lab_jobs import NOW, _create_609c599_v1_fixture, _lease, _spec, _submit_job

_COMPLETION_INDEX = "ix_lab_shard_job_completion_sequence"
_STATUS_INDEX = "ix_lab_shard_job_status_index"


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


def _assert_v6_epoch_authority(connection: sqlite3.Connection) -> int:
    epoch_row = connection.execute(
        "SELECT singleton, mutation_epoch FROM lab_ledger_epoch"
    ).fetchone()
    assert epoch_row is not None
    assert int(epoch_row[0]) == 1
    assert int(epoch_row[1]) >= 0
    epoch_triggers = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'trg_lab_epoch_%'"
        )
    }
    assert epoch_triggers == (
        set(lab_jobs._LEDGER_EPOCH_TRIGGER_SQL) | set(lab_jobs._V8_LEDGER_EPOCH_TRIGGER_SQL)
    )
    return int(epoch_row[1])


def test_initialize_creates_final_v12_result_telemetry_and_epoch_authority(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()

    with sqlite3.connect(store.path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        shard_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(lab_shard)")}
        job_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(lab_job)")}
        report_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(lab_worker_report)")
        }
        epoch = _assert_v6_epoch_authority(connection)

    assert version == LabJobStore.SCHEMA_VERSION
    assert epoch == 0
    assert "lab_worker_report" in tables
    assert "lab_scheduler_state" in tables
    assert "lab_ledger_epoch" in tables
    assert "result_contract_version" in job_columns
    assert "result_state" in job_columns
    assert {
        "plan_hash",
        "adapter_id",
        "adapter_version",
        "payload_json",
        "payload_hash",
        "payload_protocol_version",
        "claim_token",
        "claim_generation",
        "claimed_at",
        "heartbeat_at",
        "lease_expires_at",
        "result_manifest_hash",
        "failure_json",
        "finished_at",
        "phase",
        "work_unit_name",
        "work_units",
        "static_duration_ms",
        "duration_ms",
        "throughput_units_per_second",
        "completion_sequence",
    } <= shard_columns
    assert {
        "report_id",
        "content_hash",
        "report_json",
        "receipt_json",
        "claim_generation",
        "scheduler_fencing_token",
    } <= report_columns


def test_initialize_creates_exact_final_v12_publication_schema_identity(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()

    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
        publication_tables = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_schema "
                "WHERE type = 'table' AND name IN "
                "('lab_claim_publication', 'lab_claim_publication_audit')"
            )
        }
        publication_triggers = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'trigger' "
                "AND (name LIKE 'trg_lab_claim_publication_%' "
                "OR name LIKE 'trg_lab_epoch_lab_claim_publication%')"
            )
        }

    assert set(publication_tables) == {
        "lab_claim_publication",
        "lab_claim_publication_audit",
    }
    assert publication_triggers == set(lab_jobs._V8_PUBLICATION_TRIGGER_SQL)
    assert lab_jobs._sql_ddl_equivalent(
        lab_jobs._CLAIM_PUBLICATION_TABLE_STATEMENT,
        publication_tables["lab_claim_publication"],
    )
    assert lab_jobs._sql_ddl_equivalent(
        lab_jobs._CLAIM_PUBLICATION_AUDIT_TABLE_STATEMENT,
        publication_tables["lab_claim_publication_audit"],
    )
    assert "source_stage_authority_bytes" in publication_tables["lab_claim_publication"]
    assert "source_stage_db_path" not in publication_tables["lab_claim_publication"]


def test_initialize_migrates_v8_recovery_indexes_to_v12_idempotently(tmp_path: Path) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    with sqlite3.connect(path) as connection:
        for statement in lab_jobs._V8_SCHEMA_STATEMENTS:
            connection.execute(statement)
        connection.execute(f"PRAGMA application_id = {LabJobStore.APPLICATION_ID}")
        connection.execute("PRAGMA user_version = 8")

    store = LabJobStore(path)
    store.initialize()
    store.initialize()

    expected_indexes = {
        "ix_lab_shard_active_claims": """
            CREATE INDEX IF NOT EXISTS ix_lab_shard_active_claims
            ON lab_shard(
                status, scheduler_fencing_token, lease_expires_at,
                job_id, shard_index, shard_id
            )
            WHERE status = 'running'
        """,
        "ix_lab_shard_stale_recovery": """
            CREATE INDEX IF NOT EXISTS ix_lab_shard_stale_recovery
            ON lab_shard(
                status, payload_protocol_version, job_id, shard_index, shard_id, lease_expires_at
            )
            WHERE status = 'running' AND payload_protocol_version = 1
        """,
        "ix_lab_shard_v2_reconciliation": """
            CREATE INDEX IF NOT EXISTS ix_lab_shard_v2_reconciliation
            ON lab_shard(job_id, shard_id, lease_expires_at)
            WHERE status = 'running' AND payload_protocol_version = 2
        """,
        "ix_lab_shard_exhausted_queued_v1_recovery": """
            CREATE INDEX IF NOT EXISTS ix_lab_shard_exhausted_queued_v1_recovery
            ON lab_shard(status, payload_protocol_version, job_id, shard_index, shard_id)
            WHERE payload_protocol_version = 1
              AND status = 'queued'
              AND attempt_count >= max_attempts
        """,
        "ix_lab_shard_exhausted_checkpointed_v1_recovery": """
            CREATE INDEX IF NOT EXISTS ix_lab_shard_exhausted_checkpointed_v1_recovery
            ON lab_shard(status, payload_protocol_version, job_id, shard_index, shard_id)
            WHERE payload_protocol_version = 1
              AND status = 'checkpointed'
              AND attempt_count >= max_attempts
        """,
        "ix_lab_job_idle_control_recovery": """
            CREATE INDEX IF NOT EXISTS ix_lab_job_idle_control_recovery
            ON lab_job(status, created_at, job_id)
            WHERE status = 'running'
              AND control_intent IN ('pause_requested', 'cancel_requested')
        """,
        "ix_lab_shard_idle_control_eligibility": """
            CREATE INDEX IF NOT EXISTS ix_lab_shard_idle_control_eligibility
            ON lab_shard(job_id, status)
        """,
    }
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
        recovery_cursor_sql = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'lab_recovery_cursor'"
        ).fetchone()
        actual_indexes = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_schema WHERE type = 'index' "
                "AND name IN ('ix_lab_shard_active_claims', 'ix_lab_shard_stale_recovery', "
                "'ix_lab_shard_v2_reconciliation', "
                "'ix_lab_shard_exhausted_queued_v1_recovery', "
                "'ix_lab_shard_exhausted_checkpointed_v1_recovery', "
                "'ix_lab_job_idle_control_recovery', "
                "'ix_lab_shard_idle_control_eligibility')"
            )
        }

    assert set(actual_indexes) == set(expected_indexes)
    assert recovery_cursor_sql is not None
    assert lab_jobs._sql_ddl_equivalent(
        lab_jobs._RECOVERY_CURSOR_TABLE_STATEMENT,
        str(recovery_cursor_sql[0]),
    )
    for name, expected in expected_indexes.items():
        assert lab_jobs._sql_ddl_equivalent(expected, actual_indexes[name])


def test_initialize_migrates_v10_bounded_recovery_schema_to_v12_idempotently(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    store = LabJobStore(path)
    store.initialize()

    with sqlite3.connect(path) as connection:
        connection.execute("DROP INDEX ix_lab_shard_exhausted_queued_v1_recovery")
        connection.execute("DROP INDEX ix_lab_shard_exhausted_checkpointed_v1_recovery")
        connection.execute("DROP INDEX ix_lab_job_idle_control_recovery")
        connection.execute("DROP INDEX ix_lab_shard_idle_control_eligibility")
        connection.execute("DROP TABLE lab_recovery_cursor")
        connection.execute("PRAGMA user_version = 10")

    store.initialize()
    store.initialize()

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
        assert {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'index' "
                "AND name IN ('ix_lab_shard_exhausted_queued_v1_recovery', "
                "'ix_lab_shard_exhausted_checkpointed_v1_recovery', "
                "'ix_lab_job_idle_control_recovery', "
                "'ix_lab_shard_idle_control_eligibility')"
            )
        } == {
            "ix_lab_shard_exhausted_queued_v1_recovery",
            "ix_lab_shard_exhausted_checkpointed_v1_recovery",
            "ix_lab_job_idle_control_recovery",
            "ix_lab_shard_idle_control_eligibility",
        }
        assert connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'lab_recovery_cursor'"
        ).fetchone() == ("lab_recovery_cursor",)


def test_initialize_migrates_v11_idle_control_index_to_v12_idempotently(tmp_path: Path) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    store = LabJobStore(path)
    store.initialize()

    with sqlite3.connect(path) as connection:
        connection.execute("DROP INDEX ix_lab_shard_idle_control_eligibility")
        connection.execute("DROP INDEX ix_lab_job_idle_control_recovery")
        connection.execute(lab_jobs._V11_IDLE_CONTROL_RECOVERY_INDEX_STATEMENT)
        connection.execute("PRAGMA user_version = 11")

    store.initialize()
    store.initialize()

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
        idle_index = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' "
            "AND name = 'ix_lab_job_idle_control_recovery'"
        ).fetchone()
        shard_index = connection.execute(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' "
            "AND name = 'ix_lab_shard_idle_control_eligibility'"
        ).fetchone()

    assert idle_index is not None
    assert lab_jobs._sql_ddl_equivalent(
        lab_jobs._V12_IDLE_CONTROL_RECOVERY_INDEX_STATEMENT,
        str(idle_index[0]),
    )
    assert shard_index is not None
    assert lab_jobs._sql_ddl_equivalent(
        lab_jobs._V12_IDLE_CONTROL_SHARD_INDEX_STATEMENT,
        str(shard_index[0]),
    )


def test_v9_to_v10_backfill_rejects_unknown_payload_protocol_atomically(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)
    shard = store.plan_job(
        job.job_id,
        (
            LabShardDefinition.from_payload(
                shard_index=0,
                adapter_id="n-shape-replay",
                adapter_version="v1",
                plan_hash="a" * 64,
                payload_json='{"hold_days":1}',
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )[0]
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP INDEX ix_lab_shard_exhausted_queued_v1_recovery")
        connection.execute("DROP INDEX ix_lab_shard_exhausted_checkpointed_v1_recovery")
        connection.execute("DROP INDEX ix_lab_job_idle_control_recovery")
        connection.execute("DROP INDEX ix_lab_shard_idle_control_eligibility")
        connection.execute("DROP TABLE lab_recovery_cursor")
        connection.execute("DROP TRIGGER trg_lab_shard_payload_protocol_insert")
        connection.execute("DROP TRIGGER trg_lab_shard_payload_protocol_update")
        connection.execute("DROP INDEX ix_lab_shard_v2_reconciliation")
        connection.execute("DROP INDEX ix_lab_shard_stale_recovery")
        connection.execute("DROP INDEX ix_lab_shard_preclaim_candidate")
        connection.execute("ALTER TABLE lab_shard DROP COLUMN payload_protocol_version")
        connection.execute(lab_jobs._STALE_RECOVERY_INDEX_STATEMENT)
        connection.execute("PRAGMA user_version = 9")
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_shard SET payload_json = ? WHERE job_id = ? AND shard_id = ?",
            ('{"schema_version":3}', str(job.job_id), str(shard.shard_id)),
        )

    with pytest.raises(LabDatabaseIdentityError, match="payload protocol backfill failed"):
        LabJobStore(store.path).initialize()

    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9
        columns = {row[1] for row in connection.execute("PRAGMA table_info(lab_shard)")}
        assert "payload_protocol_version" not in columns


def test_v9_to_v10_backfill_rejects_oversize_payload_before_parse_and_can_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store)
    job = _submit_job(store, lease)
    definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        plan_hash="a" * 64,
        payload_json='{"hold_days":1}',
    )
    shard = store.plan_job(
        job.job_id,
        (definition,),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )[0]
    original_payload = definition.payload_json
    original_hash = definition.payload_hash
    oversize = '{"payload":"' + ("x" * 1_048_576) + '"}'
    parsed = 0
    original_loads = lab_jobs.strict_json_loads

    def counted_loads(*args: object, **kwargs: object) -> object:
        nonlocal parsed
        parsed += 1
        return original_loads(*args, **kwargs)

    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP INDEX ix_lab_shard_exhausted_queued_v1_recovery")
        connection.execute("DROP INDEX ix_lab_shard_exhausted_checkpointed_v1_recovery")
        connection.execute("DROP INDEX ix_lab_job_idle_control_recovery")
        connection.execute("DROP INDEX ix_lab_shard_idle_control_eligibility")
        connection.execute("DROP TABLE lab_recovery_cursor")
        connection.execute("DROP TRIGGER trg_lab_shard_payload_protocol_insert")
        connection.execute("DROP TRIGGER trg_lab_shard_payload_protocol_update")
        connection.execute("DROP INDEX ix_lab_shard_v2_reconciliation")
        connection.execute("DROP INDEX ix_lab_shard_stale_recovery")
        connection.execute("DROP INDEX ix_lab_shard_preclaim_candidate")
        connection.execute("ALTER TABLE lab_shard DROP COLUMN payload_protocol_version")
        connection.execute(lab_jobs._STALE_RECOVERY_INDEX_STATEMENT)
        connection.execute("PRAGMA user_version = 9")
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_shard SET payload_json = ? WHERE job_id = ? AND shard_id = ?",
            (oversize, str(job.job_id), str(shard.shard_id)),
        )

    monkeypatch.setattr(lab_jobs, "strict_json_loads", counted_loads)
    with pytest.raises(LabDatabaseIdentityError, match="payload protocol backfill failed") as error:
        LabJobStore(store.path).initialize()
    assert parsed == 0
    assert oversize not in str(error.value)
    assert "payload protocol backfill failed" in str(error.value)

    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9
        columns = {row[1] for row in connection.execute("PRAGMA table_info(lab_shard)")}
        assert "payload_protocol_version" not in columns
        connection.execute(
            "UPDATE lab_shard SET payload_json = ?, payload_hash = ? "
            "WHERE job_id = ? AND shard_id = ?",
            (original_payload, original_hash, str(job.job_id), str(shard.shard_id)),
        )

    LabJobStore(store.path).initialize()
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
        assert connection.execute(
            "SELECT payload_protocol_version FROM lab_shard WHERE job_id = ? AND shard_id = ?",
            (str(job.job_id), str(shard.shard_id)),
        ).fetchone() == (1,)


@pytest.mark.parametrize(
    ("index_name", "replacement_sql"),
    [
        (
            _COMPLETION_INDEX,
            f"""
            CREATE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, completion_sequence DESC)
            WHERE status = 'succeeded' AND completion_sequence IS NOT NULL
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, completion_sequence DESC)
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, completion_sequence DESC)
            WHERE status = 'failed' AND completion_sequence IS NOT NULL
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, completion_sequence DESC)
            WHERE status = 'SUCCEEDED' AND completion_sequence IS NOT NULL
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, completion_sequence DESC)
            WHERE status = 'succeeded'
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, completion_sequence)
            WHERE status = 'succeeded' AND completion_sequence IS NOT NULL
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(completion_sequence DESC, job_id)
            WHERE status = 'succeeded' AND completion_sequence IS NOT NULL
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, completion_sequence DESC, shard_index)
            WHERE status = 'succeeded' AND completion_sequence IS NOT NULL
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, -completion_sequence DESC)
            WHERE status = 'succeeded' AND completion_sequence IS NOT NULL
            """,
        ),
        (
            _COMPLETION_INDEX,
            f"""
            CREATE UNIQUE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id COLLATE NOCASE, completion_sequence DESC)
            WHERE status = 'succeeded' AND completion_sequence IS NOT NULL
            """,
        ),
        (
            _STATUS_INDEX,
            f"""
            CREATE INDEX {_STATUS_INDEX}
            ON lab_shard(job_id, shard_index, status)
            """,
        ),
    ],
    ids=[
        "completion-not-unique",
        "completion-not-partial",
        "completion-wrong-status-predicate",
        "completion-wrong-status-literal-case",
        "completion-missing-non-null-predicate",
        "completion-wrong-desc",
        "completion-wrong-order",
        "completion-extra-key-column",
        "completion-expression-key",
        "completion-wrong-collation",
        "status-wrong-columns",
    ],
)
def test_initialize_rejects_same_name_structurally_wrong_v4_indexes_without_replacing_them(
    tmp_path: Path,
    index_name: str,
    replacement_sql: str,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    LabJobStore(path).initialize()
    with sqlite3.connect(path) as connection:
        connection.execute(f"DROP INDEX {index_name}")
        connection.execute(replacement_sql)
        corrupted_sql = str(
            connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                (index_name,),
            ).fetchone()[0]
        )

    with pytest.raises(LabDatabaseIdentityError, match="telemetry index"):
        LabJobStore(path).initialize()

    with sqlite3.connect(path) as connection:
        retained_sql = str(
            connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                (index_name,),
            ).fetchone()[0]
        )
    assert retained_sql == corrupted_sql


def _insert_completed_telemetry_shard(
    connection: sqlite3.Connection,
    *,
    job_id: str,
    shard_index: int,
    completion_sequence: int,
) -> None:
    timestamp = NOW.isoformat(timespec="microseconds")
    connection.execute(
        """
        INSERT INTO lab_shard (
            shard_id, job_id, shard_index, status, version,
            attempt_count, max_attempts, plan_hash, adapter_id,
            adapter_version, payload_json, payload_hash,
            phase, work_unit_name, work_units, static_duration_ms,
            duration_ms, throughput_units_per_second, completion_sequence,
            result_manifest_hash, finished_at, created_at, updated_at
        ) VALUES (
            ?, ?, ?, 'succeeded', 1, 1, 3, ?, 'index-fixture', 'v1', '{}',
            '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
            'scan', 'trading_day', 1, 1000, 1000.0, 1.0, ?, ?, ?, ?, ?
        )
        """,
        (
            str(uuid4()),
            job_id,
            shard_index,
            "4" * 64,
            completion_sequence,
            "6" * 64,
            timestamp,
            timestamp,
            timestamp,
        ),
    )


def test_valid_completion_index_rejects_duplicate_sequence_for_one_job(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job = _submit_job(store, _lease(store))
    with sqlite3.connect(store.path) as connection:
        _insert_completed_telemetry_shard(
            connection,
            job_id=str(job.job_id),
            shard_index=0,
            completion_sequence=1,
        )

    with (
        sqlite3.connect(store.path) as connection,
        pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"),
    ):
        _insert_completed_telemetry_shard(
            connection,
            job_id=str(job.job_id),
            shard_index=1,
            completion_sequence=1,
        )


def test_same_name_non_unique_completion_index_allows_duplicates_but_fails_initialization(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job = _submit_job(store, _lease(store))
    with sqlite3.connect(store.path) as connection:
        connection.execute(f"DROP INDEX {_COMPLETION_INDEX}")
        connection.execute(
            f"""
            CREATE INDEX {_COMPLETION_INDEX}
            ON lab_shard(job_id, completion_sequence DESC)
            WHERE status = 'succeeded' AND completion_sequence IS NOT NULL
            """
        )
        _insert_completed_telemetry_shard(
            connection,
            job_id=str(job.job_id),
            shard_index=0,
            completion_sequence=1,
        )
        _insert_completed_telemetry_shard(
            connection,
            job_id=str(job.job_id),
            shard_index=1,
            completion_sequence=1,
        )

    with pytest.raises(LabDatabaseIdentityError, match="telemetry index"):
        LabJobStore(store.path).initialize()


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
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
        _assert_v6_epoch_authority(connection)
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
    "legacy_status",
    [ShardStatus.QUEUED, ShardStatus.RUNNING, ShardStatus.CHECKPOINTED],
)
def test_v2_exhausted_nonterminal_shard_fails_entire_job_during_migration(
    tmp_path: Path,
    legacy_status: ShardStatus,
) -> None:
    path = tmp_path / f"exhausted-{legacy_status.value}.sqlite3"
    job_id, shard_id = _create_real_v2_fixture(path)
    sibling_id = str(uuid4())
    timestamp = NOW.isoformat(timespec="microseconds")
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE lab_job
            SET status = ?, version = 1, attempt_count = 1,
                recoverable = 1, scheduler_fencing_token = 7
            WHERE job_id = ?
            """,
            (legacy_status.value, job_id),
        )
        connection.execute(
            """
            UPDATE lab_shard
            SET status = ?, version = 1, attempt_count = max_attempts,
                worker_id = 'legacy-exhausted', scheduler_fencing_token = 7,
                checkpoint_json = '{"cursor":9}'
            WHERE job_id = ? AND shard_id = ?
            """,
            (legacy_status.value, job_id, shard_id),
        )
        connection.execute(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version,
                attempt_count, max_attempts, worker_id,
                scheduler_fencing_token, checkpoint_json, created_at, updated_at
            ) VALUES (?, ?, 1, 'queued', 0, 0, 3, NULL, NULL, NULL, ?, ?)
            """,
            (sibling_id, job_id, timestamp, timestamp),
        )

    store = LabJobStore(path)
    store.initialize()
    restarted = LabJobStore(path)
    restarted.initialize()
    reader = LabJobReader(path)
    job = reader.get_job(lab_jobs.UUID(job_id))
    shards = reader.list_shards(lab_jobs.UUID(job_id))

    assert job is not None and job.status is JobStatus.FAILED
    assert job.recoverable is False
    assert job.control_intent.value == "none"
    assert job.version == 2
    assert all(shard.status is ShardStatus.FAILED for shard in shards)
    assert [shard.version for shard in shards] == [2, 1]
    assert shards[0].failure_json == '{"reason":"attempts_exhausted"}'
    assert shards[1].failure_json == '{"reason":"parent_failed_attempts_exhausted"}'
    for shard in shards:
        assert shard.finished_at == NOW
        assert shard.checkpoint_json is None
        assert (
            shard.worker_id,
            shard.scheduler_fencing_token,
            shard.claim_token,
            shard.claimed_at,
            shard.heartbeat_at,
            shard.lease_expires_at,
        ) == (None, None, None, None, None, None)
    lease = _lease(restarted, owner="migration-review", now=NOW + timedelta(seconds=2))
    retry = restarted.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=lab_jobs.UUID(job_id),
                expected_version=job.version,
                reason="must not retry migrated exhaustion",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=3),
    )
    assert retry.status == "rejected"
    assert retry.reason == "not_recoverable"


def test_reader_rejects_exhausted_queued_shard(tmp_path: Path) -> None:
    store = LabJobStore(tmp_path / "queued-exhausted.sqlite3")
    store.initialize()
    lease = _lease(store)
    submitted = LabCommandEnvelope(
        request_id=uuid4(),
        command=SubmitJobCommand(
            job_id=uuid4(),
            spec=_spec(),
            max_attempts=2,
        ),
    )
    receipt = store.apply_command(submitted, lease=lease, now=NOW)
    assert receipt.status == "applied"
    store.plan_job(
        submitted.command.job_id,
        (
            LabShardDefinition.from_payload(
                shard_index=0,
                adapter_id="reader-invariant",
                adapter_version="v1",
                plan_hash="7" * 64,
                payload_json="{}",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=1),
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            """
            UPDATE lab_shard SET attempt_count = max_attempts
            WHERE job_id = ? AND status = 'queued'
            """,
            (str(submitted.command.job_id),),
        )

    with pytest.raises(InvalidStoredJobError, match="queued shard exhausted attempts"):
        LabJobReader(store.path).list_shards(submitted.command.job_id)


def test_v2_migrated_idle_pause_converges_without_worker(tmp_path: Path) -> None:
    path = tmp_path / "idle-pause.sqlite3"
    job_id, shard_id = _create_real_v2_fixture(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE lab_job
            SET status = 'running', control_intent = 'pause_requested',
                version = 1, scheduler_fencing_token = 7
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
    lease = _lease(store, owner="migration-pause", now=NOW + timedelta(seconds=2))
    recovered = store.recover_stale_shards(lease, now=NOW + timedelta(seconds=3))
    job = LabJobReader(path).get_job(lab_jobs.UUID(job_id))
    shard = LabJobReader(path).list_shards(lab_jobs.UUID(job_id))[0]

    assert recovered == (lab_jobs.UUID(job_id),)
    assert job is not None and job.status is JobStatus.CHECKPOINTED
    assert job.control_intent.value == "none"
    assert shard.status is ShardStatus.QUEUED
    assert shard.worker_id is None and shard.claim_token is None


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
        connection.create_function(
            lab_jobs._ARTIFACT_SUCCESS_AUTH_FUNCTION,
            5,
            lambda *_args: 0,
        )
        connection.create_function(
            lab_jobs._RETRY_AUTH_FUNCTION,
            3,
            lambda *_args: 0,
        )
        connection.create_function(
            lab_jobs._READY_TERMINAL_AUTH_FUNCTION,
            6,
            lambda *_args: 0,
        )
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


def test_initialize_migrates_v3_additively_without_inventing_legacy_telemetry(
    tmp_path: Path,
) -> None:
    path = tmp_path / "lab_jobs.sqlite3"
    job_id, _shard_id = _create_real_v2_fixture(path)
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN IMMEDIATE")
        lab_jobs._migrate_v2_to_v3(connection)
        connection.execute("PRAGMA user_version = 3")
        connection.commit()
        before_job = tuple(
            connection.execute("SELECT * FROM lab_job WHERE job_id = ?", (job_id,)).fetchone()
        )
        before_shard = tuple(
            connection.execute("SELECT * FROM lab_shard WHERE job_id = ?", (job_id,)).fetchone()
        )

    store = LabJobStore(path)
    store.initialize()
    first = LabJobReader(path).list_shards(lab_jobs.UUID(job_id))[0]
    first_job = LabJobReader(path).get_job(lab_jobs.UUID(job_id))
    store.initialize()
    second = LabJobReader(path).list_shards(lab_jobs.UUID(job_id))[0]

    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        assert connection.execute("PRAGMA user_version").fetchone()[0] == LabJobStore.SCHEMA_VERSION
        _assert_v6_epoch_authority(connection)
        migrated_job = connection.execute(
            "SELECT * FROM lab_job WHERE job_id = ?", (job_id,)
        ).fetchone()
        migrated_shard = connection.execute(
            "SELECT * FROM lab_shard WHERE job_id = ?", (job_id,)
        ).fetchone()
        assert tuple(migrated_job)[: len(before_job)] == before_job
        assert tuple(migrated_shard)[: len(before_shard)] == before_shard
        assert migrated_job["result_contract_version"] is None
        assert (
            tuple(
                migrated_shard[name]
                for name in (
                    "phase",
                    "work_unit_name",
                    "work_units",
                    "static_duration_ms",
                    "duration_ms",
                    "throughput_units_per_second",
                    "completion_sequence",
                )
            )
            == (None,) * 7
        )

    assert first_job is not None and first_job.result_contract_version is None
    assert first == second
    assert first.phase is None
    assert first.work_unit_name is None
    assert first.work_units is None
    assert first.static_duration_ms is None
    assert first.duration_ms is None
    assert first.throughput_units_per_second is None
    assert first.completion_sequence is None


def test_v3_identity_validation_rejects_incomplete_worker_report_table(
    tmp_path: Path,
) -> None:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TABLE lab_worker_report")
        connection.execute("CREATE TABLE lab_worker_report (report_id TEXT PRIMARY KEY)")

    with pytest.raises(
        lab_jobs.LabDatabaseIdentityError,
        match="^lab jobs SQLite v16 current schema is invalid$",
    ) as error:
        LabJobReader(store.path).get_job(uuid4())
    assert isinstance(error.value.__cause__, lab_jobs.LabDatabaseIdentityError)
    assert "lab_worker_report columns" in str(error.value.__cause__)
