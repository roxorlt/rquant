"""Versioned DuckDB schema initialization and migration ledger."""

from __future__ import annotations

import hashlib
import json
import textwrap
from collections.abc import Mapping, Sequence
from typing import Any

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.storage.schema import (
    BASE_DDL,
    DATA_METADATA_TABLE_DDLS,
    MARKET_SENTIMENT_HIGH60_MIGRATION_DDL,
    MARKET_SENTIMENT_MA20_MIGRATION_DDL,
    MONEYFLOW_DAILY_FULL_MIGRATION_DDLS,
    PAPER_POSITION_ENTRY_RAW_MIGRATION_DDL,
    PAPER_POSITION_RUN_ID_MIGRATION_DDL,
    PAPER_POSITION_RUN_MODE_MIGRATION_DDL,
    PAPER_POSITION_SIGNAL_FACTORS_MIGRATION_DDL,
    PAPER_POSITION_STRATEGY_NAME_MIGRATION_DDL,
    PAPER_POSITION_TAKE_PROFIT_BASIS_MIGRATION_DDL,
    TRADE_CALENDAR_DDL,
)

SCHEMA_MIGRATION_DDL = """
CREATE TABLE IF NOT EXISTS schema_migration (
    version    INTEGER   PRIMARY KEY,
    name       VARCHAR   NOT NULL,
    checksum   VARCHAR   NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class SchemaMigrationError(RuntimeError):
    """Raised when the migration ledger disagrees with the code registry."""


def _normalize_statement(statement: str) -> str:
    normalized = textwrap.dedent(statement.replace("\r\n", "\n")).strip()
    return "\n".join(line.rstrip() for line in normalized.splitlines())


def _migration_checksum(
    version: int, name: str, statements: tuple[str, ...]
) -> str:
    payload = json.dumps(
        {
            "version": version,
            "name": name.strip(),
            "statements": [_normalize_statement(sql) for sql in statements],
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class Migration(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: int = Field(gt=0)
    name: str = Field(min_length=1)
    statements: tuple[str, ...] = Field(min_length=1)
    checksum: str = ""

    @model_validator(mode="before")
    @classmethod
    def checksum_is_derived(cls, data: Any) -> Any:
        if isinstance(data, Mapping) and "checksum" in data:
            raise ValueError("checksum is derived from migration content")
        return data

    @model_validator(mode="after")
    def derive_checksum(self) -> Migration:
        checksum = _migration_checksum(self.version, self.name, self.statements)
        object.__setattr__(self, "checksum", checksum)
        return self


# Published migration history is append-only. Never edit these statements;
# every schema change must add a new Migration version instead.
V1_LEGACY_COLUMN_ADDITIONS: tuple[str, ...] = (
    MARKET_SENTIMENT_HIGH60_MIGRATION_DDL,
    MARKET_SENTIMENT_MA20_MIGRATION_DDL,
    PAPER_POSITION_ENTRY_RAW_MIGRATION_DDL,
    PAPER_POSITION_TAKE_PROFIT_BASIS_MIGRATION_DDL,
    PAPER_POSITION_STRATEGY_NAME_MIGRATION_DDL,
    PAPER_POSITION_SIGNAL_FACTORS_MIGRATION_DDL,
    PAPER_POSITION_RUN_MODE_MIGRATION_DDL,
    PAPER_POSITION_RUN_ID_MIGRATION_DDL,
    *MONEYFLOW_DAILY_FULL_MIGRATION_DDLS,
)

MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version=1,
        name="legacy column additions",
        statements=V1_LEGACY_COLUMN_ADDITIONS,
    ),
    Migration(
        version=2,
        name="research data metadata",
        statements=DATA_METADATA_TABLE_DDLS,
    ),
    Migration(
        version=3,
        name="authoritative trade calendar",
        statements=(TRADE_CALENDAR_DDL,),
    ),
)


def _registry_by_version(
    migrations: Sequence[Migration],
) -> dict[int, Migration]:
    registry: dict[int, Migration] = {}
    for migration in migrations:
        if migration.version in registry:
            raise SchemaMigrationError(
                f"duplicate schema migration version {migration.version}"
            )
        registry[migration.version] = migration
    return registry


def _validate_applied_migrations(
    conn: duckdb.DuckDBPyConnection,
    registry: Mapping[int, Migration],
) -> set[int]:
    rows = conn.execute(
        "SELECT version, name, checksum FROM schema_migration ORDER BY version"
    ).fetchall()
    applied: set[int] = set()
    for version, applied_name, applied_checksum in rows:
        migration = registry.get(version)
        if migration is None:
            raise SchemaMigrationError(
                f"schema migration version {version} ({applied_name}) "
                "is applied in the database but missing from the code registry"
            )
        if applied_checksum != migration.checksum:
            raise SchemaMigrationError(
                f"schema migration version {version} checksum mismatch: "
                f"database={applied_checksum}, code={migration.checksum}"
            )
        applied.add(version)
    applied_versions = sorted(applied)
    expected_prefix = sorted(registry)[: len(applied_versions)]
    if applied_versions != expected_prefix:
        raise SchemaMigrationError(
            "applied schema migrations must form a registry prefix: "
            f"applied={applied_versions}, expected={expected_prefix}"
        )
    return applied


def _apply_migration(
    conn: duckdb.DuckDBPyConnection, migration: Migration
) -> None:
    conn.execute("BEGIN TRANSACTION")
    try:
        for statement in migration.statements:
            conn.execute(statement)
        conn.execute(
            "INSERT INTO schema_migration (version, name, checksum, applied_at) "
            "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
            [migration.version, migration.name, migration.checksum],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def initialize_schema(
    conn: duckdb.DuckDBPyConnection,
    *,
    migrations: Sequence[Migration] | None = None,
) -> None:
    """Create current tables and apply each pending migration exactly once."""
    selected = MIGRATIONS if migrations is None else tuple(migrations)
    registry = _registry_by_version(selected)

    conn.execute(SCHEMA_MIGRATION_DDL)
    applied = _validate_applied_migrations(conn, registry)
    for statement in BASE_DDL:
        conn.execute(statement)
    for version in sorted(registry):
        if version not in applied:
            _apply_migration(conn, registry[version])
