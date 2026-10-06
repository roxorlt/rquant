"""The original screening migration and data-center receipts share one ledger."""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

import duckdb

from rquant.storage.migrations import MIGRATIONS, initialize_schema

_RECEIPT_TABLES = {
    "ingestion_commit_receipt",
    "backfill_day_commit_receipt",
    "data_center_financial_runtime_receipt",
}
_V16_CHECKSUM = "6faaa181b322cb922706730372125e2140bd177be1d256f735b7dd389e02155e"
_ORIGINAL_RECEIPT_DDL_HASHES = (
    "1fbb216f001535b69019a3e2a24b9c08af616a70e54121116d05f930dfe1a0fd",
    "664cd5132f7d45a8dce7ebdced48727bcd03c9b3fcfc9b70f34f7f642d9eb2cb",
    "e8237e0e3d875f93962ad6cc47b495c90f4452c5c71da7d04d54fd2979693b62",
)


def _ledger(conn: duckdb.DuckDBPyConnection) -> list[tuple[int, str, str, datetime]]:
    return conn.execute(
        "SELECT version,name,checksum,applied_at FROM schema_migration ORDER BY version"
    ).fetchall()


def _tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {row[0] for row in conn.execute("SHOW TABLES").fetchall()}


def _seed_original_v16(conn: duckdb.DuckDBPyConnection) -> None:
    initialize_schema(conn, migrations=tuple(m for m in MIGRATIONS if m.version <= 16))
    # The migration must preserve even opaque persisted proof bytes exactly.
    conn.execute(
        "INSERT INTO screen_run_evidence VALUES (DATE '2026-09-29','retained-screen',?,?,?)",
        ["a" * 64, "b" * 64, '{"saved":"original-v16"}'],
    )


def test_upgrade_retains_original_v16_receipts_and_screen_fact(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "upgrade.duckdb")) as conn:
        _seed_original_v16(conn)
        before_ledger = _ledger(conn)
        before_fact = conn.execute("SELECT * FROM screen_run_evidence").fetchall()
        assert before_ledger[-1][1:3] == (
            "atomic daily screen input and ranking evidence",
            _V16_CHECKSUM,
        )
        initialize_schema(conn)
        assert _ledger(conn)[:16] == before_ledger
        assert conn.execute("SELECT * FROM screen_run_evidence").fetchall() == before_fact
        assert _ledger(conn)[-1][0] == 17


def test_v17_adds_only_the_three_original_receipt_ddls(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "ddl.duckdb")) as conn:
        _seed_original_v16(conn)
        before = _tables(conn)
        initialize_schema(conn)
        assert _tables(conn) - before == _RECEIPT_TABLES
        assert before <= _tables(conn)
        migration = MIGRATIONS[-1]
        assert migration.version == 17
        assert migration.name == "transaction-bound data center completion receipts"
        assert (
            tuple(hashlib.sha256(sql.encode()).hexdigest() for sql in migration.statements)
            == _ORIGINAL_RECEIPT_DDL_HASHES
        )


def test_fresh_database_has_screening_and_completion_domains(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "fresh.duckdb")) as conn:
        initialize_schema(conn)
        assert _RECEIPT_TABLES | {"screen_run_evidence"} <= _tables(conn)
        assert [row[0] for row in _ledger(conn)] == list(range(1, 18))
        assert _ledger(conn)[15][2] == _V16_CHECKSUM


def test_repeat_initialization_retains_fact_bytes_and_all_ledger_times(tmp_path: Path) -> None:
    with duckdb.connect(str(tmp_path / "repeat.duckdb")) as conn:
        _seed_original_v16(conn)
        initialize_schema(conn)
        conn.execute(
            "INSERT INTO ingestion_commit_receipt VALUES "
            "('event','receipt',1,DATE '2026-09-29','collector','run',?,"
            "TIMESTAMPTZ '2026-09-29 10:00:00+00')",
            ['{"immutable":"source-receipt"}'],
        )
        before = (
            _ledger(conn),
            conn.execute("SELECT * FROM screen_run_evidence").fetchall(),
            conn.execute("SELECT * FROM ingestion_commit_receipt").fetchall(),
        )
        initialize_schema(conn)
        assert (
            _ledger(conn),
            conn.execute("SELECT * FROM screen_run_evidence").fetchall(),
            conn.execute("SELECT * FROM ingestion_commit_receipt").fetchall(),
        ) == before
