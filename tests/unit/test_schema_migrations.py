"""Versioned DuckDB schema migration behavior."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

import rquant.storage.duckdb as duckdb_storage
from rquant.storage.duckdb import DuckDBStore
from rquant.storage.migrations import (
    MIGRATIONS,
    V1_LEGACY_COLUMN_ADDITIONS,
    Migration,
    SchemaMigrationError,
    initialize_schema,
)


def _migration_rows(
    conn: duckdb.DuckDBPyConnection,
) -> list[tuple[int, str, str, datetime]]:
    return conn.execute(
        "SELECT version, name, checksum, applied_at "
        "FROM schema_migration ORDER BY version"
    ).fetchall()


def test_migration_is_frozen_and_derives_stable_checksum() -> None:
    formatted = Migration(
        version=900,
        name="create probe",
        statements=("\n  CREATE TABLE probe (id INTEGER);\n",),
    )
    same_content = Migration(
        version=900,
        name="create probe",
        statements=("CREATE TABLE probe (id INTEGER);",),
    )

    assert formatted.checksum == same_content.checksum
    assert len(formatted.checksum) == 64
    with pytest.raises(ValidationError, match="checksum"):
        Migration(
            version=900,
            name="create probe",
            statements=("CREATE TABLE probe (id INTEGER);",),
            checksum="caller-controlled",
        )
    with pytest.raises(ValidationError, match="frozen"):
        formatted.name = "changed"  # type: ignore[misc]


def test_published_v1_statements_and_checksum_are_fixed() -> None:
    assert isinstance(V1_LEGACY_COLUMN_ADDITIONS, tuple)
    assert MIGRATIONS[0].statements == V1_LEGACY_COLUMN_ADDITIONS
    assert (
        MIGRATIONS[0].checksum
        == "049827c760b87a12e4fa3bffc560d4ffd2d4ad974377c6c84e0f849268911720"
    )


def test_v2_creates_metadata_tables_only_through_versioned_migration() -> None:
    from rquant.storage.schema import ALL_DDL, DATA_METADATA_TABLE_DDLS

    metadata_tables = {
        "dataset_snapshot",
        "dataset_coverage",
        "data_quality_issue",
    }
    assert [migration.version for migration in MIGRATIONS] == [1, 2]
    assert MIGRATIONS[1].statements == DATA_METADATA_TABLE_DDLS
    assert all(statement in ALL_DDL for statement in DATA_METADATA_TABLE_DDLS)

    conn = duckdb.connect(":memory:")
    initialize_schema(conn, migrations=MIGRATIONS[:1])
    before_v2 = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    assert not metadata_tables & before_v2
    assert [row[0] for row in _migration_rows(conn)] == [1]

    initialize_schema(conn)
    after_v2 = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    foreign_keys = conn.execute(
        "SELECT table_name FROM duckdb_constraints() "
        "WHERE constraint_type = 'FOREIGN KEY' "
        "AND table_name IN ('dataset_snapshot', 'dataset_coverage', "
        "'data_quality_issue')"
    ).fetchall()

    assert metadata_tables <= after_v2
    assert [row[0] for row in _migration_rows(conn)] == [1, 2]
    assert foreign_keys == []
    with pytest.raises(duckdb.ConstraintException):
        conn.execute(
            "INSERT INTO dataset_coverage VALUES ("
            "'snapshot', 'dataset', 'scope', 'table', "
            "1, 1, 0, NULL, CAST('[]' AS JSON), CURRENT_TIMESTAMP)"
        )
    conn.close()


def test_fresh_database_records_registered_migrations(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "fresh.duckdb")

    rows = _migration_rows(store._conn)
    assert [(version, name, checksum) for version, name, checksum, _ in rows] == [
        (migration.version, migration.name, migration.checksum)
        for migration in MIGRATIONS
    ]
    assert all(isinstance(applied_at, datetime) for *_, applied_at in rows)
    store.close()


def test_legacy_database_upgrades_and_second_open_is_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        "CREATE TABLE market_sentiment_daily (trade_date DATE PRIMARY KEY)"
    )
    conn.execute(
        "CREATE TABLE paper_position (position_id VARCHAR PRIMARY KEY)"
    )
    conn.execute(
        "CREATE TABLE moneyflow_daily ("
        "ts_code VARCHAR, trade_date DATE, source VARCHAR, "
        "PRIMARY KEY (ts_code, trade_date, source))"
    )
    conn.close()

    first = DuckDBStore(db_path)
    first_rows = _migration_rows(first._conn)
    sentiment_columns = {
        row[0]
        for row in first._conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'market_sentiment_daily'"
        ).fetchall()
    }
    position_columns = {
        row[0]
        for row in first._conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'paper_position'"
        ).fetchall()
    }
    moneyflow_columns = {
        row[0]
        for row in first._conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'moneyflow_daily'"
        ).fetchall()
    }
    first.close()

    second = DuckDBStore(db_path)
    second_rows = _migration_rows(second._conn)
    second.close()

    assert {"high_60d_ratio_pct", "above_ma20_ratio_pct"} <= sentiment_columns
    assert {
        "entry_price_raw",
        "take_profit_basis",
        "strategy_name",
        "signal_factors",
        "run_mode",
        "run_id",
    } <= position_columns
    assert {"buy_sm_vol", "sell_elg_amount"} <= moneyflow_columns
    assert second_rows == first_rows


def test_applied_migration_is_not_executed_twice() -> None:
    migration = Migration(
        version=900,
        name="non-idempotent probe",
        statements=("CREATE TABLE migration_once (id INTEGER);",),
    )
    conn = duckdb.connect(":memory:")

    initialize_schema(conn, migrations=(migration,))
    initialize_schema(conn, migrations=(migration,))

    assert conn.execute(
        "SELECT COUNT(*) FROM schema_migration WHERE version = 900"
    ).fetchone()[0] == 1
    conn.close()


def test_failed_migration_rolls_back_ddl_and_ledger() -> None:
    migration = Migration(
        version=901,
        name="rollback probe",
        statements=(
            "CREATE TABLE rollback_probe (id INTEGER);",
            "INSERT INTO table_that_does_not_exist VALUES (1);",
        ),
    )
    conn = duckdb.connect(":memory:")

    with pytest.raises(duckdb.Error, match="table_that_does_not_exist"):
        initialize_schema(conn, migrations=(migration,))

    assert conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'rollback_probe'"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM schema_migration WHERE version = 901"
    ).fetchone()[0] == 0
    conn.close()


def test_checksum_drift_rejects_startup() -> None:
    original = Migration(
        version=902,
        name="checksum probe",
        statements=("CREATE TABLE checksum_probe (id INTEGER);",),
    )
    changed = Migration(
        version=902,
        name="checksum probe",
        statements=("CREATE TABLE checksum_probe (id BIGINT);",),
    )
    conn = duckdb.connect(":memory:")
    initialize_schema(conn, migrations=(original,))

    with pytest.raises(SchemaMigrationError, match=r"version 902.*checksum"):
        initialize_schema(conn, migrations=(changed,))

    assert _migration_rows(conn)[-1][2] == original.checksum
    conn.close()


def test_non_prefix_migration_ledger_rejects_startup() -> None:
    first = Migration(
        version=1,
        name="prefix first",
        statements=("CREATE TABLE prefix_first (id INTEGER);",),
    )
    second = Migration(
        version=2,
        name="prefix second",
        statements=("CREATE TABLE prefix_second (id INTEGER);",),
    )
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE schema_migration ("
        "version INTEGER PRIMARY KEY, name VARCHAR NOT NULL, "
        "checksum VARCHAR NOT NULL, applied_at TIMESTAMP NOT NULL)"
    )
    conn.execute(
        "INSERT INTO schema_migration VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
        [second.version, second.name, second.checksum],
    )

    with pytest.raises(SchemaMigrationError, match="prefix"):
        initialize_schema(conn, migrations=(first, second))

    assert conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'prefix_first'"
    ).fetchone()[0] == 0
    conn.close()


def test_duplicate_migration_version_rejects_startup() -> None:
    first = Migration(
        version=1,
        name="duplicate first",
        statements=("CREATE TABLE duplicate_first (id INTEGER);",),
    )
    duplicate = Migration(
        version=1,
        name="duplicate second",
        statements=("CREATE TABLE duplicate_second (id INTEGER);",),
    )
    conn = duckdb.connect(":memory:")

    with pytest.raises(SchemaMigrationError, match="duplicate.*version 1"):
        initialize_schema(conn, migrations=(first, duplicate))

    assert conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name LIKE 'duplicate_%'"
    ).fetchone()[0] == 0
    conn.close()


def test_duckdb_store_uses_shared_initializer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[duckdb.DuckDBPyConnection] = []
    real_initialize_schema = duckdb_storage.initialize_schema

    def spy_initialize_schema(conn: duckdb.DuckDBPyConnection) -> None:
        calls.append(conn)
        real_initialize_schema(conn)

    monkeypatch.setattr(duckdb_storage, "initialize_schema", spy_initialize_schema)
    store = DuckDBStore(tmp_path / "store.duckdb")

    assert calls == [store._conn]
    store.close()


def test_read_only_store_does_not_run_migrations(tmp_path: Path) -> None:
    db_path = tmp_path / "readonly.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE legacy_probe (id INTEGER)")
    conn.close()

    store = DuckDBStore(db_path, read_only=True)
    tables = {
        row[0]
        for row in store._conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    store.close()

    assert tables == {"legacy_probe"}
