"""Recoverable bootstrap primitives for moving research data to the cloud."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, cast

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_catalog import ResearchCatalog, exclusive_file_lock
from rquant.research_lake import (
    ResearchDataset,
    ResearchPartitionManifest,
    export_research_dataset,
)

_SNAPSHOT_ID_PATTERN = re.compile(r"^research-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$")
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_AUXILIARY_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "monitor_event": ("trade_date", "ts_code", "level"),
    "intraday_feature_snapshot": ("snapshot_id",),
    "paper_position": ("position_id",),
    "paper_position_event": ("event_id",),
    "dataset_snapshot": ("snapshot_id",),
    "dataset_coverage": ("snapshot_id", "dataset_id", "coverage_scope"),
    "data_quality_issue": ("issue_id",),
}
_AUXILIARY_SCOPE_COLUMNS: dict[str, tuple[str, ...]] = {
    "monitor_event": ("trade_date", "trigger_time"),
    "intraday_feature_snapshot": ("trade_date", "as_of_time", "created_at"),
    "paper_position": (
        "trade_date",
        "entry_time",
        "exit_time",
        "created_at",
        "updated_at",
    ),
    "paper_position_event": ("event_time", "created_at"),
    "dataset_snapshot": ("as_of_time", "created_at", "completed_at"),
    "dataset_coverage": ("created_at",),
    "data_quality_issue": ("first_seen_at", "last_seen_at", "resolved_at"),
}


class _MigrationModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class RecoverySnapshotEvidence(_MigrationModel):
    schema_version: Literal[1] = 1
    snapshot_id: str
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    created_at: datetime
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_size: int = Field(gt=0)
    table_count: int = Field(gt=0)
    artifact_inventory_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    artifact_file_count: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_evidence(self) -> RecoverySnapshotEvidence:
        if _SNAPSHOT_ID_PATTERN.fullmatch(self.snapshot_id) is None:
            raise ValueError("invalid research migration snapshot_id")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return self


class RecoverySnapshotResult(_MigrationModel):
    status: Literal["created", "unchanged"]
    database_path: Path
    metadata_path: Path
    evidence: RecoverySnapshotEvidence


class MigrationFileEvidence(_MigrationModel):
    relative_path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_size: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_relative_path(self) -> MigrationFileEvidence:
        path = Path(self.relative_path)
        if path.is_absolute() or ".." in path.parts or self.relative_path in {"", "."}:
            raise ValueError("migration file path must stay inside the bundle")
        return self


class MigrationDatasetEvidence(_MigrationModel):
    dataset: ResearchDataset
    partition_count: int = Field(gt=0)
    row_count: int = Field(gt=0)
    earliest_date: date
    latest_date: date
    trade_dates: tuple[date, ...] = Field(min_length=1)
    duplicate_key_count: Literal[0] = 0
    total_amount: str
    total_vol: str
    sample_size: int = Field(ge=0, le=100)
    sample_match_count: int = Field(ge=0, le=100)
    sample_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    partition_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_dataset_evidence(self) -> MigrationDatasetEvidence:
        if self.sample_match_count != self.sample_size:
            raise ValueError("migration dataset sample must match its frozen source")
        if tuple(sorted(set(self.trade_dates))) != self.trade_dates:
            raise ValueError("migration dataset trade_dates must be unique and sorted")
        if self.earliest_date != self.trade_dates[0] or self.latest_date != self.trade_dates[-1]:
            raise ValueError("migration dataset date bounds must match trade_dates")
        return self


class AuxiliaryTableEvidence(_MigrationModel):
    table_name: str
    relative_path: str
    row_count: int = Field(ge=0)
    duplicate_key_count: Literal[0] = 0
    primary_key: tuple[str, ...] = Field(min_length=1)
    scope_end_date: date
    schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ResearchMigrationManifest(_MigrationModel):
    schema_version: Literal[1] = 1
    snapshot_id: str
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    created_at: datetime
    source_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_snapshot_size: int = Field(gt=0)
    datasets: tuple[MigrationDatasetEvidence, ...] = Field(min_length=2)
    auxiliary_tables: tuple[AuxiliaryTableEvidence, ...] = Field(min_length=1)
    artifact_file_count: int = Field(ge=0)
    files: tuple[MigrationFileEvidence, ...] = Field(min_length=1)
    total_file_size: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_manifest(self) -> ResearchMigrationManifest:
        if _SNAPSHOT_ID_PATTERN.fullmatch(self.snapshot_id) is None:
            raise ValueError("invalid research migration snapshot_id")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        if len(self.datasets) != 2 or {item.dataset for item in self.datasets} != {
            "minute_bar",
            "auction_bar",
        }:
            raise ValueError("migration bundle requires minute_bar and auction_bar")
        if len(self.auxiliary_tables) != len(_AUXILIARY_PRIMARY_KEYS) or {
            item.table_name for item in self.auxiliary_tables
        } != set(_AUXILIARY_PRIMARY_KEYS):
            raise ValueError("migration bundle auxiliary research tables are incomplete")
        if len({item.relative_path for item in self.files}) != len(self.files):
            raise ValueError("migration bundle contains duplicate file paths")
        if self.total_file_size != sum(item.file_size for item in self.files):
            raise ValueError("migration bundle total_file_size mismatch")
        return self


class ResearchMigrationPrepareResult(_MigrationModel):
    status: Literal["created", "unchanged"]
    bundle_path: Path
    manifest_path: Path
    manifest: ResearchMigrationManifest


class ResearchMigrationVerification(_MigrationModel):
    status: Literal["verified"] = "verified"
    snapshot_id: str
    code_commit: str
    file_count: int = Field(ge=1)
    total_file_size: int = Field(ge=0)
    partition_count: int = Field(ge=2)
    row_count: int = Field(ge=1)
    sample_size: int = Field(ge=0)
    sample_match_count: int = Field(ge=0)
    auxiliary_table_count: int = Field(ge=1)
    artifact_file_count: int = Field(ge=0)


class ResearchMigrationPublishState(_MigrationModel):
    schema_version: Literal[1] = 1
    status: Literal["publishing", "prepared"]
    snapshot_id: str
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    bundle_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_state(self) -> ResearchMigrationPublishState:
        if _SNAPSHOT_ID_PATTERN.fullmatch(self.snapshot_id) is None:
            raise ValueError("invalid research migration snapshot_id")
        for observed in (self.created_at, self.updated_at):
            if observed.tzinfo is None or observed.utcoffset() is None:
                raise ValueError("research migration state timestamps must be timezone-aware")
        return self


class ResearchAuthorityCandidate(_MigrationModel):
    schema_version: Literal[1] = 1
    status: Literal["candidate"] = "candidate"
    snapshot_id: str
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    published_at: datetime
    bundle_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    catalog_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    partition_count: int = Field(ge=2)
    row_count: int = Field(ge=1)
    auxiliary_table_count: int = Field(ge=1)
    artifact_file_count: int = Field(ge=0)
    local_retention_min_trading_days: Literal[10] = 10

    @model_validator(mode="after")
    def validate_candidate(self) -> ResearchAuthorityCandidate:
        if _SNAPSHOT_ID_PATTERN.fullmatch(self.snapshot_id) is None:
            raise ValueError("invalid research migration snapshot_id")
        if self.published_at.tzinfo is None or self.published_at.utcoffset() is None:
            raise ValueError("published_at must be timezone-aware")
        return self


class ResearchMigrationPublishResult(_MigrationModel):
    status: Literal["published", "unchanged"]
    snapshot_id: str
    target_data_dir: Path
    catalog_path: Path
    candidate_path: Path
    verification: ResearchMigrationVerification


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_model_atomic(path: Path, model: BaseModel) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(model.model_dump_json(indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _snapshot_table_count(path: Path) -> int:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchone()
        count = 0 if row is None else int(row[0])
    finally:
        connection.close()
    if count < 1:
        raise ValueError("recovery snapshot contains no main-schema tables")
    return count


def _evidence_for_snapshot(
    path: Path,
    *,
    snapshot_id: str,
    code_commit: str,
    created_at: datetime,
    artifact_inventory: tuple[tuple[str, int, str], ...] | None,
) -> RecoverySnapshotEvidence:
    return RecoverySnapshotEvidence(
        snapshot_id=snapshot_id,
        code_commit=code_commit,
        created_at=created_at.astimezone(UTC),
        sha256=_file_sha256(path),
        file_size=path.stat().st_size,
        table_count=_snapshot_table_count(path),
        artifact_inventory_hash=(
            None
            if artifact_inventory is None
            else _artifact_inventory_hash(artifact_inventory)
        ),
        artifact_file_count=0 if artifact_inventory is None else len(artifact_inventory),
    )


def _load_verified_existing_snapshot(
    database_path: Path,
    metadata_path: Path,
    pending_metadata_path: Path,
    *,
    snapshot_id: str,
    code_commit: str,
    artifact_inventory: tuple[tuple[str, int, str], ...] | None,
) -> RecoverySnapshotResult | None:
    if not database_path.exists() and not metadata_path.exists():
        if pending_metadata_path.exists():
            if not pending_metadata_path.is_file() or pending_metadata_path.is_symlink():
                raise RuntimeError("invalid pending recovery snapshot evidence")
            pending_metadata_path.unlink()
            _fsync_directory(pending_metadata_path.parent)
        return None
    if metadata_path.exists() and not database_path.is_file():
        raise RuntimeError("incomplete recovery snapshot publication")
    if database_path.is_file() and not metadata_path.exists():
        if not pending_metadata_path.is_file() or pending_metadata_path.is_symlink():
            raise RuntimeError("recovery snapshot is missing its pending evidence")
        evidence = RecoverySnapshotEvidence.model_validate_json(
            pending_metadata_path.read_text(encoding="utf-8")
        )
        _verify_recovery_snapshot_binding(
            database_path,
            evidence,
            snapshot_id=snapshot_id,
            code_commit=code_commit,
            artifact_inventory=artifact_inventory,
        )
        _write_metadata_atomic(metadata_path, evidence)
        pending_metadata_path.unlink()
        _fsync_directory(metadata_path.parent)
        return RecoverySnapshotResult(
            status="unchanged",
            database_path=database_path,
            metadata_path=metadata_path,
            evidence=evidence,
        )
    if not database_path.is_file() or not metadata_path.is_file():
        raise RuntimeError("invalid recovery snapshot publication paths")
    evidence = RecoverySnapshotEvidence.model_validate_json(
        metadata_path.read_text(encoding="utf-8")
    )
    _verify_recovery_snapshot_binding(
        database_path,
        evidence,
        snapshot_id=snapshot_id,
        code_commit=code_commit,
        artifact_inventory=artifact_inventory,
    )
    if pending_metadata_path.exists():
        if not pending_metadata_path.is_file() or pending_metadata_path.is_symlink():
            raise RuntimeError("invalid pending recovery snapshot evidence")
        pending = RecoverySnapshotEvidence.model_validate_json(
            pending_metadata_path.read_text(encoding="utf-8")
        )
        if pending != evidence:
            raise RuntimeError("pending recovery snapshot evidence mismatch")
        pending_metadata_path.unlink()
        _fsync_directory(metadata_path.parent)
    return RecoverySnapshotResult(
        status="unchanged",
        database_path=database_path,
        metadata_path=metadata_path,
        evidence=evidence,
    )


def _verify_recovery_snapshot_binding(
    database_path: Path,
    evidence: RecoverySnapshotEvidence,
    *,
    snapshot_id: str,
    code_commit: str,
    artifact_inventory: tuple[tuple[str, int, str], ...] | None,
) -> None:
    if evidence.snapshot_id != snapshot_id or evidence.code_commit != code_commit:
        raise RuntimeError("recovery snapshot metadata binding mismatch")
    if database_path.stat().st_size != evidence.file_size:
        raise RuntimeError("recovery snapshot size mismatch")
    if _file_sha256(database_path) != evidence.sha256:
        raise RuntimeError("recovery snapshot hash mismatch")
    if _snapshot_table_count(database_path) != evidence.table_count:
        raise RuntimeError("recovery snapshot table count mismatch")
    if evidence.artifact_inventory_hash is not None:
        if artifact_inventory is None:
            raise RuntimeError("recovery snapshot artifact evidence cannot be verified")
        if (
            evidence.artifact_file_count != len(artifact_inventory)
            or evidence.artifact_inventory_hash
            != _artifact_inventory_hash(artifact_inventory)
        ):
            raise RuntimeError("strategy lab artifacts changed after recovery snapshot")
    elif artifact_inventory:
        raise RuntimeError("recovery snapshot did not bind strategy lab artifacts")


def _write_metadata_atomic(
    metadata_path: Path,
    evidence: RecoverySnapshotEvidence,
) -> None:
    _write_model_atomic(metadata_path, evidence)


def create_recovery_snapshot(
    source_database: Path,
    *,
    recovery_dir: Path,
    artifact_dir: Path | None = None,
    snapshot_id: str,
    code_commit: str,
    now: Callable[[], datetime] | None = None,
) -> RecoverySnapshotResult:
    """Checkpoint and freeze one immutable local DuckDB recovery generation."""
    if _SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id) is None:
        raise ValueError("invalid research migration snapshot_id")
    if _COMMIT_PATTERN.fullmatch(code_commit) is None:
        raise ValueError("recovery snapshot requires a clean 40-character code commit")
    source_database = Path(source_database)
    recovery_dir = Path(recovery_dir)
    artifact_dir = None if artifact_dir is None else Path(artifact_dir)
    if not source_database.is_file() or source_database.is_symlink():
        raise FileNotFoundError(
            f"source DuckDB does not exist as a regular file: {source_database}"
        )
    snapshot_root = recovery_dir / snapshot_id
    database_path = snapshot_root / "rquant.duckdb"
    metadata_path = snapshot_root / "snapshot.json"
    pending_metadata_path = snapshot_root / ".snapshot-pending.json"
    if source_database.resolve() == database_path.resolve():
        raise ValueError("recovery snapshot must not overwrite its source DuckDB")
    snapshot_root.mkdir(parents=True, exist_ok=True)
    lock_path = recovery_dir / ".snapshot.lock"
    clock = now or (lambda: datetime.now(UTC))

    with exclusive_file_lock(lock_path):
        observed_at = clock()
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("research migration clock must be timezone-aware")
        artifact_inventory = (
            None if artifact_dir is None else _artifact_inventory(artifact_dir)
        )
        existing = _load_verified_existing_snapshot(
            database_path,
            metadata_path,
            pending_metadata_path,
            snapshot_id=snapshot_id,
            code_commit=code_commit,
            artifact_inventory=artifact_inventory,
        )
        if existing is not None:
            return existing

        created_at = observed_at
        nonce = uuid.uuid4().hex
        temp_database = snapshot_root / f".rquant.duckdb.tmp-{nonce}"
        try:
            connection = duckdb.connect(str(source_database))
            try:
                connection.execute("CHECKPOINT")
                if Path(f"{source_database}.wal").exists():
                    raise RuntimeError("source WAL remains after checkpoint")
                shutil.copy2(source_database, temp_database)
            finally:
                connection.close()
            if artifact_dir is not None and _artifact_inventory(artifact_dir) != artifact_inventory:
                raise RuntimeError("strategy lab artifacts changed during recovery snapshot")
            if Path(f"{temp_database}.wal").exists():
                raise RuntimeError("temporary recovery snapshot unexpectedly has a WAL")
            evidence = _evidence_for_snapshot(
                temp_database,
                snapshot_id=snapshot_id,
                code_commit=code_commit,
                created_at=created_at,
                artifact_inventory=artifact_inventory,
            )
            _fsync_file(temp_database)
            temp_database.chmod(0o400)
            _write_metadata_atomic(pending_metadata_path, evidence)
            os.replace(temp_database, database_path)
            _fsync_directory(snapshot_root)

            _write_metadata_atomic(metadata_path, evidence)
            pending_metadata_path.unlink()
            _fsync_directory(snapshot_root)
            return RecoverySnapshotResult(
                status="created",
                database_path=database_path,
                metadata_path=metadata_path,
                evidence=evidence,
            )
        finally:
            if temp_database.exists():
                temp_database.chmod(0o600)
                temp_database.unlink()


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quoted_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _canonical_rows_hash(rows: list[tuple[object, ...]]) -> str:
    payload = json.dumps(
        rows,
        default=str,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _schema_hash(columns: tuple[tuple[str, str], ...]) -> str:
    payload = json.dumps(columns, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_recovery_evidence(
    source_snapshot: Path,
    *,
    snapshot_id: str,
    code_commit: str,
) -> RecoverySnapshotEvidence:
    metadata_path = source_snapshot.with_name("snapshot.json")
    if not source_snapshot.is_file() or source_snapshot.is_symlink():
        raise FileNotFoundError(f"recovery snapshot is missing: {source_snapshot}")
    if not metadata_path.is_file() or metadata_path.is_symlink():
        raise FileNotFoundError(f"recovery snapshot metadata is missing: {metadata_path}")
    evidence = RecoverySnapshotEvidence.model_validate_json(
        metadata_path.read_text(encoding="utf-8")
    )
    if evidence.snapshot_id != snapshot_id or evidence.code_commit != code_commit:
        raise ValueError("recovery snapshot does not match migration identity")
    if evidence.file_size != source_snapshot.stat().st_size:
        raise ValueError("recovery snapshot size changed after sealing")
    if evidence.sha256 != _file_sha256(source_snapshot):
        raise ValueError("recovery snapshot hash changed after sealing")
    if evidence.table_count != _snapshot_table_count(source_snapshot):
        raise ValueError("recovery snapshot table count changed after sealing")
    return evidence


def _table_schema_and_primary_key(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    expected_primary_key: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    rows = connection.execute(
        f"PRAGMA table_info({_quoted_literal(table_name)})"
    ).fetchall()
    if not rows:
        raise ValueError(f"required research table is missing: {table_name}")
    columns = tuple((str(row[1]), str(row[2])) for row in rows)
    actual_primary_key = tuple(
        str(row[1]) for row in sorted(rows, key=lambda item: int(item[5])) if int(row[5]) > 0
    )
    if actual_primary_key != expected_primary_key:
        raise ValueError(
            f"research table primary key mismatch for {table_name}: "
            f"{actual_primary_key} != {expected_primary_key}"
        )
    return columns


def _auxiliary_scope_where_clause(
    table_name: str,
    columns: tuple[tuple[str, str], ...],
    *,
    end_date: date,
) -> str:
    types = {name: column_type.upper() for name, column_type in columns}
    predicates: list[str] = []
    for column in _AUXILIARY_SCOPE_COLUMNS[table_name]:
        column_type = types.get(column)
        if column_type is None:
            continue
        quoted = _quoted_identifier(column)
        if "WITH TIME ZONE" in column_type:
            scoped_date = f"CAST({quoted} AT TIME ZONE 'Asia/Shanghai' AS DATE)"
        else:
            scoped_date = f"CAST({quoted} AS DATE)"
        predicates.append(
            f"({quoted} IS NULL OR {scoped_date} <= DATE "
            f"{_quoted_literal(end_date.isoformat())})"
        )
    if not predicates:
        return ""
    return "WHERE " + " AND ".join(predicates)


def _export_auxiliary_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    bundle_root: Path,
    snapshot_id: str,
    table_name: str,
    primary_key: tuple[str, ...],
    end_date: date,
) -> AuxiliaryTableEvidence:
    columns = _table_schema_and_primary_key(connection, table_name, primary_key)
    quoted_table = _quoted_identifier(table_name)
    quoted_keys = ", ".join(_quoted_identifier(column) for column in primary_key)
    scope_where = _auxiliary_scope_where_clause(
        table_name,
        columns,
        end_date=end_date,
    )
    duplicate = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {quoted_keys}
            FROM {quoted_table}
            {scope_where}
            GROUP BY {quoted_keys}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    duplicate_count = 0 if duplicate is None else int(duplicate[0])
    if duplicate_count:
        raise ValueError(f"duplicate primary key in research table {table_name}")
    count_row = connection.execute(
        f"SELECT COUNT(*) FROM {quoted_table} {scope_where}"
    ).fetchone()
    row_count = 0 if count_row is None else int(count_row[0])
    relative_path = Path(
        "snapshots",
        f"dataset_id={table_name}",
        f"snapshot_id={snapshot_id}",
        "data.parquet",
    )
    data_path = bundle_root / relative_path
    data_path.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"""
        COPY (
            SELECT *
            FROM {quoted_table}
            {scope_where}
            ORDER BY {quoted_keys}
        ) TO {_quoted_literal(str(data_path))}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    _fsync_file(data_path)
    parquet = f"read_parquet({_quoted_literal(str(data_path))}, hive_partitioning = false)"
    validator = duckdb.connect()
    try:
        parquet_count = validator.execute(f"SELECT COUNT(*) FROM {parquet}").fetchone()
        if parquet_count is None or int(parquet_count[0]) != row_count:
            raise ValueError(f"auxiliary row count mismatch for {table_name}")
        selected = ", ".join(_quoted_identifier(column) for column, _ in columns)
        rows = validator.execute(
            f"SELECT {selected} FROM {parquet} ORDER BY {quoted_keys}"
        ).fetchall()
    finally:
        validator.close()
    evidence = AuxiliaryTableEvidence(
        table_name=table_name,
        relative_path=relative_path.as_posix(),
        row_count=row_count,
        duplicate_key_count=0,
        primary_key=primary_key,
        scope_end_date=end_date,
        schema_hash=_schema_hash(columns),
        content_hash=_canonical_rows_hash(rows),
        file_hash=_file_sha256(data_path),
    )
    manifest_path = data_path.with_name("manifest.json")
    manifest_path.write_text(evidence.model_dump_json(indent=2) + "\n", encoding="utf-8")
    _fsync_file(manifest_path)
    return evidence


def _dataset_relation(paths: list[Path]) -> str:
    if not paths:
        raise ValueError("migration dataset has no published Parquet files")
    values = ", ".join(_quoted_literal(str(path)) for path in paths)
    return f"read_parquet([{values}], hive_partitioning = false)"


def _dataset_predicate(dataset: ResearchDataset, start_date: date, end_date: date) -> str:
    start = _quoted_literal(start_date.isoformat())
    end = _quoted_literal(end_date.isoformat())
    if dataset == "minute_bar":
        return f"CAST(trade_time AS DATE) BETWEEN DATE {start} AND DATE {end}"
    return f"trade_date BETWEEN DATE {start} AND DATE {end}"


def _dataset_evidence(
    source: duckdb.DuckDBPyConnection,
    *,
    bundle_root: Path,
    dataset: ResearchDataset,
    start_date: date,
    end_date: date,
    partitions: tuple[ResearchPartitionManifest, ...],
) -> MigrationDatasetEvidence:
    paths = [bundle_root / manifest.relative_path for manifest in partitions]
    relation = _dataset_relation(paths)
    table = _quoted_identifier(dataset)
    predicate = _dataset_predicate(dataset, start_date, end_date)
    if dataset == "minute_bar":
        primary_key = ("ts_code", "trade_time", "freq", "source")
        columns = (
            "ts_code",
            "trade_time",
            "freq",
            "open",
            "high",
            "low",
            "close",
            "vol",
            "amount",
            "source",
            "created_at",
        )
    else:
        primary_key = ("ts_code", "trade_date", "auction_type", "source")
        columns = (
            "ts_code",
            "trade_date",
            "auction_type",
            "price",
            "vol",
            "amount",
            "turnover_rate",
            "volume_ratio",
            "source",
            "created_at",
        )
    quoted_columns = ", ".join(_quoted_identifier(column) for column in columns)
    quoted_keys = ", ".join(_quoted_identifier(column) for column in primary_key)
    hash_args = ", ".join(_quoted_identifier(column) for column in primary_key)
    sample_order = f"hash({hash_args}), {quoted_keys}"
    source_rows = source.execute(
        f"""
        SELECT {quoted_columns}
        FROM {table}
        WHERE {predicate}
        ORDER BY {sample_order}
        LIMIT 100
        """
    ).fetchall()
    verifier = duckdb.connect()
    try:
        parquet_rows = verifier.execute(
            f"""
            SELECT {quoted_columns}
            FROM {relation}
            ORDER BY {sample_order}
            LIMIT 100
            """
        ).fetchall()
        parquet_totals = verifier.execute(
            f"""
            SELECT
                CAST(COALESCE(SUM(CAST(amount AS DECIMAL(38, 4))), 0) AS VARCHAR),
                CAST(COALESCE(SUM(CAST(vol AS DECIMAL(38, 4))), 0) AS VARCHAR)
            FROM {relation}
            """
        ).fetchone()
    finally:
        verifier.close()
    source_totals = source.execute(
        f"""
        SELECT
            CAST(COALESCE(SUM(CAST(amount AS DECIMAL(38, 4))), 0) AS VARCHAR),
            CAST(COALESCE(SUM(CAST(vol AS DECIMAL(38, 4))), 0) AS VARCHAR)
        FROM {table}
        WHERE {predicate}
        """
    ).fetchone()
    if source_rows != parquet_rows:
        raise ValueError(f"deterministic source sample mismatch for {dataset}")
    if source_totals != parquet_totals or source_totals is None:
        raise ValueError(f"aggregate amount/volume mismatch for {dataset}")
    trade_dates = tuple(sorted({manifest.partition.trade_date for manifest in partitions}))
    digest = hashlib.sha256()
    for manifest in sorted(partitions, key=lambda item: item.partition.partition_id):
        digest.update(manifest.content_hash.encode("ascii"))
    return MigrationDatasetEvidence(
        dataset=dataset,
        partition_count=len(partitions),
        row_count=sum(manifest.row_count for manifest in partitions),
        earliest_date=trade_dates[0],
        latest_date=trade_dates[-1],
        trade_dates=trade_dates,
        duplicate_key_count=0,
        total_amount=str(source_totals[0]),
        total_vol=str(source_totals[1]),
        sample_size=len(source_rows),
        sample_match_count=len(parquet_rows),
        sample_hash=_canonical_rows_hash(source_rows),
        partition_content_hash=digest.hexdigest(),
    )


def _artifact_inventory(source_dir: Path) -> tuple[tuple[str, int, str], ...]:
    if not source_dir.exists():
        return ()
    if not source_dir.is_dir() or source_dir.is_symlink():
        raise ValueError("strategy lab artifact source must be a regular directory")
    inventory: list[tuple[str, int, str]] = []
    for source in sorted(source_dir.rglob("*")):
        if source.is_symlink():
            raise ValueError(f"strategy lab artifact must not be a symlink: {source}")
        if source.is_dir():
            continue
        if not source.is_file():
            raise ValueError(f"unsupported strategy lab artifact: {source}")
        inventory.append(
            (
                source.relative_to(source_dir).as_posix(),
                source.stat().st_size,
                _file_sha256(source),
            )
        )
    return tuple(inventory)


def _artifact_inventory_hash(
    inventory: tuple[tuple[str, int, str], ...],
) -> str:
    payload = json.dumps(inventory, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _copy_artifacts(source_dir: Path, bundle_root: Path) -> int:
    target_root = bundle_root / "artifacts" / "strategy_lab_runs"
    if not source_dir.exists():
        target_root.mkdir(parents=True, exist_ok=True)
        return 0
    before = _artifact_inventory(source_dir)
    for relative_path, _, _ in before:
        source = source_dir / relative_path
        relative = Path(relative_path)
        target = target_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        _fsync_file(target)
    after = _artifact_inventory(source_dir)
    copied = _artifact_inventory(target_root)
    if before != after or before != copied:
        raise RuntimeError("strategy lab artifacts changed during migration copy")
    return len(copied)


def _bundle_files(bundle_root: Path) -> tuple[MigrationFileEvidence, ...]:
    files: list[MigrationFileEvidence] = []
    for path in sorted(bundle_root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"migration bundle must not contain symlinks: {path}")
        if not path.is_file() or path.name == "bundle-manifest.json":
            continue
        if path.name.endswith(".lock") or ".tmp-" in path.name:
            raise ValueError(f"migration bundle contains transient file: {path}")
        relative = path.relative_to(bundle_root).as_posix()
        files.append(
            MigrationFileEvidence(
                relative_path=relative,
                sha256=_file_sha256(path),
                file_size=path.stat().st_size,
            )
        )
    return tuple(files)


def _load_verified_bundle(
    bundle_path: Path,
    *,
    snapshot_id: str,
    code_commit: str,
    recovery: RecoverySnapshotEvidence,
) -> ResearchMigrationManifest | None:
    if not bundle_path.exists():
        return None
    manifest_path = bundle_path / "bundle-manifest.json"
    if not bundle_path.is_dir() or bundle_path.is_symlink() or not manifest_path.is_file():
        raise RuntimeError("incomplete research migration bundle publication")
    manifest = ResearchMigrationManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    if manifest.snapshot_id != snapshot_id or manifest.code_commit != code_commit:
        raise RuntimeError("research migration bundle identity mismatch")
    if (
        manifest.source_snapshot_sha256 != recovery.sha256
        or manifest.source_snapshot_size != recovery.file_size
    ):
        raise RuntimeError("research migration bundle recovery snapshot evidence mismatch")
    actual = _bundle_files(bundle_path)
    if actual != manifest.files:
        raise RuntimeError("research migration bundle file evidence mismatch")
    return manifest


def _read_verified_bundle(bundle_path: Path) -> ResearchMigrationManifest:
    manifest_path = bundle_path / "bundle-manifest.json"
    if not bundle_path.is_dir() or bundle_path.is_symlink() or not manifest_path.is_file():
        raise RuntimeError("incomplete research migration bundle publication")
    manifest = ResearchMigrationManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    actual = _bundle_files(bundle_path)
    if actual != manifest.files:
        raise RuntimeError("research migration bundle file evidence mismatch")
    return manifest


def _dataset_columns_and_primary_key(
    dataset: ResearchDataset,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if dataset == "minute_bar":
        return (
            (
                "ts_code",
                "trade_time",
                "freq",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
                "source",
                "created_at",
            ),
            ("ts_code", "trade_time", "freq", "source"),
        )
    return (
        (
            "ts_code",
            "trade_date",
            "auction_type",
            "price",
            "vol",
            "amount",
            "turnover_rate",
            "volume_ratio",
            "source",
            "created_at",
        ),
        ("ts_code", "trade_date", "auction_type", "source"),
    )


def _verify_partition_semantics(
    data_path: Path,
    manifest: ResearchPartitionManifest,
) -> None:
    if not data_path.is_file() or data_path.is_symlink():
        raise RuntimeError(f"research partition file is missing: {data_path}")
    if data_path.stat().st_size != manifest.file_size:
        raise RuntimeError(f"research partition size mismatch: {manifest.partition.partition_id}")
    if _file_sha256(data_path) != manifest.file_hash:
        raise RuntimeError(f"research partition hash mismatch: {manifest.partition.partition_id}")
    _, primary_key = _dataset_columns_and_primary_key(manifest.dataset)
    keys = ", ".join(_quoted_identifier(column) for column in primary_key)
    relation = f"read_parquet({_quoted_literal(str(data_path))}, hive_partitioning = false)"
    if manifest.dataset == "minute_bar":
        trade_date = _quoted_literal(manifest.partition.trade_date.isoformat())
        predicate = (
            f"CAST(trade_time AS DATE) = DATE {trade_date} "
            f"AND freq = {_quoted_literal(cast(str, manifest.partition.freq))}"
        )
    else:
        predicate = (
            f"trade_date = DATE {_quoted_literal(manifest.partition.trade_date.isoformat())}"
        )
    connection = duckdb.connect()
    try:
        row = connection.execute(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE NOT ({predicate})) FROM {relation}"
        ).fetchone()
        duplicate = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {keys}
                FROM {relation}
                GROUP BY {keys}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
    finally:
        connection.close()
    if row is None or int(row[0]) != manifest.row_count or int(row[1]) != 0:
        raise RuntimeError(
            f"research partition semantic mismatch: {manifest.partition.partition_id}"
        )
    if duplicate is None or int(duplicate[0]) != 0:
        raise RuntimeError(f"research partition duplicate key: {manifest.partition.partition_id}")


def _verify_dataset_semantics(
    bundle_path: Path,
    evidence: MigrationDatasetEvidence,
    partitions: tuple[ResearchPartitionManifest, ...],
) -> None:
    paths = [bundle_path / "lake" / manifest.relative_path for manifest in partitions]
    for data_path, partition in zip(paths, partitions, strict=True):
        _verify_partition_semantics(data_path, partition)
    relation = _dataset_relation(paths)
    columns, primary_key = _dataset_columns_and_primary_key(evidence.dataset)
    quoted_columns = ", ".join(_quoted_identifier(column) for column in columns)
    quoted_keys = ", ".join(_quoted_identifier(column) for column in primary_key)
    hash_args = ", ".join(_quoted_identifier(column) for column in primary_key)
    connection = duckdb.connect()
    try:
        row = connection.execute(
            f"""
            SELECT
                COUNT(*),
                CAST(COALESCE(SUM(CAST(amount AS DECIMAL(38, 4))), 0) AS VARCHAR),
                CAST(COALESCE(SUM(CAST(vol AS DECIMAL(38, 4))), 0) AS VARCHAR)
            FROM {relation}
            """
        ).fetchone()
        sample = connection.execute(
            f"""
            SELECT {quoted_columns}
            FROM {relation}
            ORDER BY hash({hash_args}), {quoted_keys}
            LIMIT 100
            """
        ).fetchall()
    finally:
        connection.close()
    if row is None or int(row[0]) != evidence.row_count:
        raise RuntimeError(f"research dataset row count mismatch: {evidence.dataset}")
    if str(row[1]) != evidence.total_amount or str(row[2]) != evidence.total_vol:
        raise RuntimeError(f"research dataset aggregate mismatch: {evidence.dataset}")
    if len(sample) != evidence.sample_size or _canonical_rows_hash(sample) != evidence.sample_hash:
        raise RuntimeError(f"research dataset sample mismatch: {evidence.dataset}")
    dates = tuple(sorted({manifest.partition.trade_date for manifest in partitions}))
    digest = hashlib.sha256()
    for partition in sorted(partitions, key=lambda item: item.partition.partition_id):
        digest.update(partition.content_hash.encode("ascii"))
    if (
        len(partitions) != evidence.partition_count
        or dates != evidence.trade_dates
        or digest.hexdigest() != evidence.partition_content_hash
    ):
        raise RuntimeError(f"research dataset partition evidence mismatch: {evidence.dataset}")


def _verify_auxiliary_table(bundle_path: Path, evidence: AuxiliaryTableEvidence) -> None:
    data_path = bundle_path / evidence.relative_path
    manifest_path = data_path.with_name("manifest.json")
    if not data_path.is_file() or data_path.is_symlink() or not manifest_path.is_file():
        raise RuntimeError(f"auxiliary research snapshot is missing: {evidence.table_name}")
    stored = AuxiliaryTableEvidence.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    if stored != evidence or _file_sha256(data_path) != evidence.file_hash:
        raise RuntimeError(f"auxiliary research manifest mismatch: {evidence.table_name}")
    relation = f"read_parquet({_quoted_literal(str(data_path))}, hive_partitioning = false)"
    keys = ", ".join(_quoted_identifier(column) for column in evidence.primary_key)
    connection = duckdb.connect()
    try:
        described = connection.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        columns = tuple((str(row[0]), str(row[1])) for row in described)
        row = connection.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()
        scope_where = _auxiliary_scope_where_clause(
            evidence.table_name,
            columns,
            end_date=evidence.scope_end_date,
        )
        scoped_row = connection.execute(
            f"SELECT COUNT(*) FROM {relation} {scope_where}"
        ).fetchone()
        duplicate = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {keys}
                FROM {relation}
                GROUP BY {keys}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
        selected = ", ".join(_quoted_identifier(column) for column, _ in columns)
        rows = connection.execute(
            f"SELECT {selected} FROM {relation} ORDER BY {keys}"
        ).fetchall()
    finally:
        connection.close()
    if _schema_hash(columns) != evidence.schema_hash:
        raise RuntimeError(f"auxiliary research schema mismatch: {evidence.table_name}")
    if row is None or int(row[0]) != evidence.row_count:
        raise RuntimeError(f"auxiliary research row count mismatch: {evidence.table_name}")
    if scoped_row is None or int(scoped_row[0]) != evidence.row_count:
        raise RuntimeError(f"auxiliary research date scope mismatch: {evidence.table_name}")
    if duplicate is None or int(duplicate[0]) != 0:
        raise RuntimeError(f"auxiliary research duplicate key: {evidence.table_name}")
    if _canonical_rows_hash(rows) != evidence.content_hash:
        raise RuntimeError(f"auxiliary research content mismatch: {evidence.table_name}")


def _verify_catalog(bundle_path: Path, manifest: ResearchMigrationManifest) -> None:
    catalog_path = bundle_path / "research.duckdb"
    connection = duckdb.connect(str(catalog_path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT dataset, COUNT(*), SUM(row_count)
            FROM research_partition
            GROUP BY dataset
            ORDER BY dataset
            """
        ).fetchall()
    finally:
        connection.close()
    expected = sorted(
        (item.dataset, item.partition_count, item.row_count) for item in manifest.datasets
    )
    actual = sorted((str(row[0]), int(row[1]), int(row[2])) for row in rows)
    if actual != expected:
        raise RuntimeError("research catalog does not match partition evidence")


def verify_research_migration_bundle(bundle_path: Path) -> ResearchMigrationVerification:
    """Recompute transfer and semantic evidence before any cloud publication."""
    bundle_path = Path(bundle_path)
    manifest = _read_verified_bundle(bundle_path)
    partition_manifests: list[ResearchPartitionManifest] = []
    for manifest_path in sorted((bundle_path / "lake").rglob("manifest.json")):
        partition_manifests.append(
            ResearchPartitionManifest.model_validate_json(
                manifest_path.read_text(encoding="utf-8")
            )
        )
    for dataset in manifest.datasets:
        partitions = tuple(
            item for item in partition_manifests if item.dataset == dataset.dataset
        )
        _verify_dataset_semantics(bundle_path, dataset, partitions)
    for auxiliary in manifest.auxiliary_tables:
        _verify_auxiliary_table(bundle_path, auxiliary)
    artifact_root = bundle_path / "artifacts" / "strategy_lab_runs"
    artifact_count = sum(
        path.is_file() and not path.is_symlink() for path in artifact_root.rglob("*")
    )
    if artifact_count != manifest.artifact_file_count:
        raise RuntimeError("strategy lab artifact count mismatch")
    _verify_catalog(bundle_path, manifest)
    return ResearchMigrationVerification(
        snapshot_id=manifest.snapshot_id,
        code_commit=manifest.code_commit,
        file_count=len(manifest.files),
        total_file_size=manifest.total_file_size,
        partition_count=sum(item.partition_count for item in manifest.datasets),
        row_count=sum(item.row_count for item in manifest.datasets),
        sample_size=sum(item.sample_size for item in manifest.datasets),
        sample_match_count=sum(item.sample_match_count for item in manifest.datasets),
        auxiliary_table_count=len(manifest.auxiliary_tables),
        artifact_file_count=manifest.artifact_file_count,
    )


def _bundle_partition_manifests(bundle_path: Path) -> tuple[ResearchPartitionManifest, ...]:
    manifests: list[ResearchPartitionManifest] = []
    for dataset_root in (bundle_path / "lake/minute", bundle_path / "lake/auction"):
        for manifest_path in sorted(dataset_root.rglob("manifest.json")):
            manifests.append(
                ResearchPartitionManifest.model_validate_json(
                    manifest_path.read_text(encoding="utf-8")
                )
            )
    return tuple(manifests)


def _expected_raw_partition_paths(
    bundle_path: Path,
    target_data_dir: Path,
) -> set[Path]:
    expected: set[Path] = set()
    for manifest in _bundle_partition_manifests(bundle_path):
        data_path = target_data_dir / "lake" / manifest.relative_path
        expected.add(data_path)
        expected.add(data_path.parent.parent / "manifest.json")
    return expected


def _assert_target_partition_scope(
    bundle_path: Path,
    target_data_dir: Path,
    *,
    allow_missing: bool,
) -> None:
    expected = _expected_raw_partition_paths(bundle_path, target_data_dir)
    observed: set[Path] = set()
    for dataset_root in (
        target_data_dir / "lake/minute",
        target_data_dir / "lake/auction",
    ):
        if not dataset_root.exists():
            continue
        if not dataset_root.is_dir() or dataset_root.is_symlink():
            raise RuntimeError(f"invalid published research dataset root: {dataset_root}")
        for path in dataset_root.rglob("*"):
            if path.is_symlink():
                raise RuntimeError(f"invalid published research path: {path}")
            if path.name == ".export.lock":
                if not path.is_file():
                    raise RuntimeError(f"invalid research export lock: {path}")
                continue
            if path.is_file():
                observed.add(path)
    outside = observed - expected
    if outside:
        raise RuntimeError("published research lake contains partitions outside migration bundle")
    if not allow_missing and observed != expected:
        raise RuntimeError("published research lake is missing migration bundle partitions")


def _publication_files(
    bundle_path: Path,
    manifest: ResearchMigrationManifest,
    target_data_dir: Path,
) -> tuple[tuple[Path, Path, MigrationFileEvidence], ...]:
    files: list[tuple[Path, Path, MigrationFileEvidence]] = []
    for evidence in manifest.files:
        relative = Path(evidence.relative_path)
        if relative == Path("research.duckdb"):
            continue
        if relative.parts[0] == "lake":
            target = target_data_dir / relative
        elif relative.parts[0] == "snapshots":
            target = target_data_dir / "lake" / relative
        elif relative.parts[:2] == ("artifacts", "strategy_lab_runs"):
            target = (
                target_data_dir
                / "research_artifacts"
                / f"snapshot_id={manifest.snapshot_id}"
                / Path(*relative.parts[1:])
            )
        else:
            raise ValueError(f"unsupported research migration file: {evidence.relative_path}")
        files.append((bundle_path / relative, target, evidence))
    files.sort(key=lambda item: (item[1].name == "manifest.json", item[1].as_posix()))
    return tuple(files)


def _validate_publish_target_root(target_data_dir: Path) -> None:
    if target_data_dir.exists():
        if not target_data_dir.is_dir() or target_data_dir.is_symlink():
            raise ValueError("research migration target must be a regular directory")
    else:
        target_data_dir.mkdir(parents=True)


def _validate_target_path(target_data_dir: Path, target: Path) -> None:
    relative = target.relative_to(target_data_dir)
    current = target_data_dir
    for part in relative.parts[:-1]:
        current /= part
        if current.is_symlink():
            raise RuntimeError(f"research target path contains a symlink: {current}")


def _preflight_publication_files(
    files: tuple[tuple[Path, Path, MigrationFileEvidence], ...],
    *,
    target_data_dir: Path,
) -> None:
    for source, target, evidence in files:
        if not source.is_file() or source.is_symlink():
            raise RuntimeError(f"research bundle source file is missing: {source}")
        _validate_target_path(target_data_dir, target)
        if not target.exists():
            continue
        if (
            not target.is_file()
            or target.is_symlink()
            or target.stat().st_size != evidence.file_size
            or _file_sha256(target) != evidence.sha256
        ):
            raise RuntimeError(f"conflicting research target file: {target}")


def _validate_export_lock_identity(lock_path: Path, descriptor: int) -> None:
    descriptor_stat = os.fstat(descriptor)
    try:
        path_stat = lock_path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeError(f"invalid research export lock: {lock_path}") from exc
    if (
        not stat.S_ISREG(descriptor_stat.st_mode)
        or not stat.S_ISREG(path_stat.st_mode)
        or descriptor_stat.st_nlink != 1
        or path_stat.st_nlink != 1
        or descriptor_stat.st_dev != path_stat.st_dev
        or descriptor_stat.st_ino != path_stat.st_ino
    ):
        raise RuntimeError(f"invalid research export lock: {lock_path}")


def _remove_owned_stale_publication_temps(
    files: tuple[tuple[Path, Path, MigrationFileEvidence], ...],
    *,
    catalog_path: Path,
    candidate_path: Path,
    state_path: Path,
) -> None:
    target_data_dir = catalog_path.parent
    targets = {target for _, target, _ in files}
    targets.update((catalog_path, candidate_path, state_path))
    stale_by_target: dict[Path, tuple[Path, ...]] = {}
    export_lock_paths: set[Path] = set()
    for target in targets:
        if not target.parent.is_dir() or target.parent.is_symlink():
            continue
        pattern = re.compile(
            rf"^\.{re.escape(target.name)}\.tmp-([0-9a-f]{{32}})(?:\.lock)?$"
        )
        stale_paths = tuple(
            stale
            for stale in target.parent.glob(f".{target.name}.tmp-*")
            if pattern.fullmatch(stale.name) is not None
        )
        if not stale_paths:
            continue
        stale_by_target[target] = stale_paths
        relative = target.relative_to(target_data_dir)
        if relative.parts[:2] not in (("lake", "minute"), ("lake", "auction")):
            continue
        if target.name == "manifest.json":
            export_lock_paths.add(target.parent / ".export.lock")
        elif target.parent.name == "versions":
            export_lock_paths.add(target.parent.parent / ".export.lock")

    acquired_locks: list[tuple[Path, int]] = []
    touched_directories: set[Path] = set()
    try:
        for lock_path in sorted(export_lock_paths):
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                existing_stat = lock_path.lstat()
            except FileNotFoundError:
                existing_stat = None
            if existing_stat is not None and (
                not stat.S_ISREG(existing_stat.st_mode) or existing_stat.st_nlink != 1
            ):
                raise RuntimeError(f"invalid research export lock: {lock_path}")
            try:
                descriptor = os.open(
                    lock_path,
                    os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
            except OSError as exc:
                raise RuntimeError(f"invalid research export lock: {lock_path}") from exc
            try:
                _validate_export_lock_identity(lock_path, descriptor)
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                os.close(descriptor)
                raise RuntimeError(
                    f"active research export blocks migration cleanup: {lock_path}"
                ) from exc
            except Exception:
                os.close(descriptor)
                raise
            acquired_locks.append((lock_path, descriptor))
            _validate_export_lock_identity(lock_path, descriptor)

        for lock_path, descriptor in acquired_locks:
            _validate_export_lock_identity(lock_path, descriptor)
        for stale_paths in stale_by_target.values():
            for stale in stale_paths:
                if not stale.exists():
                    continue
                if not stale.is_file() or stale.is_symlink():
                    raise RuntimeError(f"invalid stale research publication temp: {stale}")
                stale.unlink()
                touched_directories.add(stale.parent)
        for directory in touched_directories:
            _fsync_directory(directory)
    finally:
        for _, descriptor in reversed(acquired_locks):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)


def _publish_immutable_file(
    source: Path,
    target: Path,
    evidence: MigrationFileEvidence,
) -> None:
    if target.is_file():
        if target.stat().st_size == evidence.file_size and _file_sha256(target) == evidence.sha256:
            return
        raise RuntimeError(f"conflicting research target file: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_name(f".{target.name}.tmp-{uuid.uuid4().hex}")
    try:
        shutil.copy2(source, temp_path)
        if temp_path.stat().st_size != evidence.file_size:
            raise RuntimeError(f"research publish size mismatch: {target}")
        if _file_sha256(temp_path) != evidence.sha256:
            raise RuntimeError(f"research publish hash mismatch: {target}")
        _fsync_file(temp_path)
        os.replace(temp_path, target)
        _fsync_directory(target.parent)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _rebuild_published_catalog(
    target_data_dir: Path,
    *,
    manifests: tuple[ResearchPartitionManifest, ...],
    published_at: datetime,
) -> Path:
    catalog_path = target_data_dir / "research.duckdb"
    temp_catalog = target_data_dir / f".research.duckdb.tmp-{uuid.uuid4().hex}"
    temp_lock = temp_catalog.with_name(f"{temp_catalog.name}.lock")
    try:
        catalog = ResearchCatalog(temp_catalog)
        for manifest in manifests:
            run_id = catalog.begin_run(
                dataset=manifest.dataset,
                partition_id=manifest.partition.partition_id,
                code_commit=manifest.code_commit,
                started_at=manifest.created_at,
            )
            catalog.finish_run(
                run_id,
                status="exported",
                manifest=manifest,
                finished_at=published_at,
            )
        if temp_lock.exists():
            temp_lock.unlink()
        _fsync_file(temp_catalog)
        os.replace(temp_catalog, catalog_path)
        _fsync_directory(target_data_dir)
    finally:
        if temp_catalog.exists():
            temp_catalog.unlink()
        if temp_lock.exists():
            temp_lock.unlink()
    return catalog_path


def _verify_published_catalog(
    catalog_path: Path,
    manifests: tuple[ResearchPartitionManifest, ...],
) -> None:
    connection = duckdb.connect(str(catalog_path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT partition_id, dataset, row_count, content_hash, file_hash, schema_hash
            FROM research_partition
            """
        ).fetchall()
    finally:
        connection.close()
    actual = {
        str(row[0]): (str(row[1]), int(row[2]), str(row[3]), str(row[4]), str(row[5]))
        for row in rows
    }
    expected = {
        item.partition.partition_id: (
            item.dataset,
            item.row_count,
            item.content_hash,
            item.file_hash,
            item.schema_hash,
        )
        for item in manifests
    }
    if actual != expected:
        raise RuntimeError("published research catalog does not exactly match verified partitions")


def _verify_published_target(
    bundle_path: Path,
    target_data_dir: Path,
    manifest: ResearchMigrationManifest,
) -> None:
    _assert_target_partition_scope(bundle_path, target_data_dir, allow_missing=False)
    files = _publication_files(bundle_path, manifest, target_data_dir)
    _preflight_publication_files(files, target_data_dir=target_data_dir)
    partition_manifests = _bundle_partition_manifests(bundle_path)
    artifact_root = (
        target_data_dir
        / "research_artifacts"
        / f"snapshot_id={manifest.snapshot_id}"
        / "strategy_lab_runs"
    )
    artifact_count = sum(
        path.is_file() and not path.is_symlink() for path in artifact_root.rglob("*")
    )
    if artifact_count != manifest.artifact_file_count:
        raise RuntimeError("published strategy lab artifact count mismatch")
    _verify_published_catalog(target_data_dir / "research.duckdb", partition_manifests)


def publish_research_migration_bundle(
    bundle_path: Path,
    *,
    target_data_dir: Path,
    now: Callable[[], datetime] | None = None,
) -> ResearchMigrationPublishResult:
    """Verify and atomically publish a research-only bundle as a cloud candidate."""
    bundle_path = Path(bundle_path)
    target_data_dir = Path(target_data_dir)
    verification = verify_research_migration_bundle(bundle_path)
    manifest = _read_verified_bundle(bundle_path)
    _validate_publish_target_root(target_data_dir)
    clock = now or (lambda: datetime.now(UTC))
    observed_at = clock()
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("research migration clock must be timezone-aware")
    observed_at = observed_at.astimezone(UTC)
    manifest_hash = _file_sha256(bundle_path / "bundle-manifest.json")
    catalog_path = target_data_dir / "research.duckdb"
    candidate_path = target_data_dir / "research-authority-candidate.json"
    state_path = (
        target_data_dir
        / "research_migrations"
        / f"snapshot_id={manifest.snapshot_id}"
        / "publish-state.json"
    )
    for protected_path in (catalog_path, candidate_path, state_path):
        _validate_target_path(target_data_dir, protected_path)
        if protected_path.is_symlink():
            raise RuntimeError(f"research publish path must not be a symlink: {protected_path}")

    with (
        exclusive_file_lock(target_data_dir / "research-publish.lock"),
        exclusive_file_lock(target_data_dir / ".research-migration.lock"),
    ):
        if candidate_path.exists():
            if not candidate_path.is_file():
                raise RuntimeError("research authority candidate must be a regular file")
            candidate = ResearchAuthorityCandidate.model_validate_json(
                candidate_path.read_text(encoding="utf-8")
            )
            if (
                candidate.snapshot_id != manifest.snapshot_id
                or candidate.code_commit != manifest.code_commit
                or candidate.bundle_manifest_sha256 != manifest_hash
            ):
                raise RuntimeError("a different research authority candidate is already published")
            if (
                candidate.source_snapshot_sha256 != manifest.source_snapshot_sha256
                or candidate.partition_count != verification.partition_count
                or candidate.row_count != verification.row_count
                or candidate.auxiliary_table_count != verification.auxiliary_table_count
                or candidate.artifact_file_count != verification.artifact_file_count
            ):
                raise RuntimeError("research authority candidate evidence mismatch")
            if (
                not catalog_path.is_file()
                or catalog_path.is_symlink()
                or _file_sha256(catalog_path) != candidate.catalog_sha256
            ):
                raise RuntimeError("research authority candidate catalog hash mismatch")
            _verify_published_target(bundle_path, target_data_dir, manifest)
            return ResearchMigrationPublishResult(
                status="unchanged",
                snapshot_id=manifest.snapshot_id,
                target_data_dir=target_data_dir,
                catalog_path=catalog_path,
                candidate_path=candidate_path,
                verification=verification,
            )

        existing_state: ResearchMigrationPublishState | None = None
        if state_path.exists():
            existing_state = ResearchMigrationPublishState.model_validate_json(
                state_path.read_text(encoding="utf-8")
            )
            if (
                existing_state.snapshot_id != manifest.snapshot_id
                or existing_state.code_commit != manifest.code_commit
                or existing_state.bundle_manifest_sha256 != manifest_hash
            ):
                raise RuntimeError("research migration publish state binding mismatch")
        elif catalog_path.exists():
            raise RuntimeError("existing research catalog is not owned by this migration")

        files = _publication_files(bundle_path, manifest, target_data_dir)
        if existing_state is not None:
            _remove_owned_stale_publication_temps(
                files,
                catalog_path=catalog_path,
                candidate_path=candidate_path,
                state_path=state_path,
            )
        _assert_target_partition_scope(bundle_path, target_data_dir, allow_missing=True)
        _preflight_publication_files(files, target_data_dir=target_data_dir)
        created_at = observed_at if existing_state is None else existing_state.created_at
        _write_model_atomic(
            state_path,
            ResearchMigrationPublishState(
                status="publishing",
                snapshot_id=manifest.snapshot_id,
                code_commit=manifest.code_commit,
                bundle_manifest_sha256=manifest_hash,
                created_at=created_at,
                updated_at=observed_at,
            ),
        )
        for source, target, evidence in files:
            _publish_immutable_file(source, target, evidence)
        partition_manifests = _bundle_partition_manifests(bundle_path)
        _rebuild_published_catalog(
            target_data_dir,
            manifests=partition_manifests,
            published_at=observed_at,
        )
        _verify_published_target(bundle_path, target_data_dir, manifest)
        _write_model_atomic(
            state_path,
            ResearchMigrationPublishState(
                status="prepared",
                snapshot_id=manifest.snapshot_id,
                code_commit=manifest.code_commit,
                bundle_manifest_sha256=manifest_hash,
                created_at=created_at,
                updated_at=observed_at,
            ),
        )
        candidate = ResearchAuthorityCandidate(
            snapshot_id=manifest.snapshot_id,
            code_commit=manifest.code_commit,
            published_at=observed_at,
            bundle_manifest_sha256=manifest_hash,
            source_snapshot_sha256=manifest.source_snapshot_sha256,
            catalog_sha256=_file_sha256(catalog_path),
            partition_count=verification.partition_count,
            row_count=verification.row_count,
            auxiliary_table_count=verification.auxiliary_table_count,
            artifact_file_count=verification.artifact_file_count,
        )
        _write_model_atomic(candidate_path, candidate)
        return ResearchMigrationPublishResult(
            status="published",
            snapshot_id=manifest.snapshot_id,
            target_data_dir=target_data_dir,
            catalog_path=catalog_path,
            candidate_path=candidate_path,
            verification=verification,
        )


def prepare_research_migration_bundle(
    source_snapshot: Path,
    *,
    bundle_dir: Path,
    artifact_dir: Path,
    snapshot_id: str,
    code_commit: str,
    start_date: date,
    end_date: date,
    now: Callable[[], datetime] | None = None,
) -> ResearchMigrationPrepareResult:
    """Export one immutable, self-verifying migration bundle from a frozen snapshot."""
    if _SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id) is None:
        raise ValueError("invalid research migration snapshot_id")
    if _COMMIT_PATTERN.fullmatch(code_commit) is None:
        raise ValueError("migration bundle requires a clean 40-character code commit")
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    source_snapshot = Path(source_snapshot)
    bundle_dir = Path(bundle_dir)
    artifact_dir = Path(artifact_dir)
    recovery = _load_recovery_evidence(
        source_snapshot,
        snapshot_id=snapshot_id,
        code_commit=code_commit,
    )
    bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = bundle_dir / snapshot_id
    manifest_path = bundle_path / "bundle-manifest.json"
    artifact_inventory = _artifact_inventory(artifact_dir)
    if recovery.artifact_inventory_hash is not None:
        if (
            recovery.artifact_file_count != len(artifact_inventory)
            or recovery.artifact_inventory_hash
            != _artifact_inventory_hash(artifact_inventory)
        ):
            raise RuntimeError("strategy lab artifacts changed after recovery snapshot")
    elif artifact_inventory:
        raise RuntimeError("recovery snapshot did not bind strategy lab artifacts")
    clock = now or (lambda: datetime.now(UTC))
    with exclusive_file_lock(bundle_dir / ".migration.lock"):
        existing = _load_verified_bundle(
            bundle_path,
            snapshot_id=snapshot_id,
            code_commit=code_commit,
            recovery=recovery,
        )
        if existing is not None:
            return ResearchMigrationPrepareResult(
                status="unchanged",
                bundle_path=bundle_path,
                manifest_path=manifest_path,
                manifest=existing,
            )
        created_at = clock()
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise ValueError("research migration clock must be timezone-aware")
        temp_bundle = bundle_dir / f".{snapshot_id}.tmp-{uuid.uuid4().hex}"
        try:
            temp_bundle.mkdir(parents=True)
            connection = duckdb.connect(str(source_snapshot), read_only=True)
            try:
                catalog = ResearchCatalog(temp_bundle / "research.duckdb")
                dataset_evidence: list[MigrationDatasetEvidence] = []
                for dataset in cast(tuple[ResearchDataset, ...], ("minute_bar", "auction_bar")):
                    summary = export_research_dataset(
                        connection,
                        catalog=catalog,
                        lake_root=temp_bundle / "lake",
                        dataset=dataset,
                        start_date=start_date,
                        end_date=end_date,
                        code_commit=code_commit,
                        now=clock,
                    )
                    manifests = tuple(
                        cast(ResearchPartitionManifest, result.manifest)
                        for result in summary.partitions
                        if result.manifest is not None
                    )
                    if not manifests or len(manifests) != summary.partition_count:
                        raise ValueError(f"migration dataset is empty or incomplete: {dataset}")
                    dataset_evidence.append(
                        _dataset_evidence(
                            connection,
                            bundle_root=temp_bundle / "lake",
                            dataset=dataset,
                            start_date=start_date,
                            end_date=end_date,
                            partitions=manifests,
                        )
                    )
                auxiliary = tuple(
                    _export_auxiliary_table(
                        connection,
                        bundle_root=temp_bundle,
                        snapshot_id=snapshot_id,
                        table_name=table_name,
                        primary_key=primary_key,
                        end_date=end_date,
                    )
                    for table_name, primary_key in _AUXILIARY_PRIMARY_KEYS.items()
                )
            finally:
                connection.close()
            artifact_count = _copy_artifacts(artifact_dir, temp_bundle)
            for lock_file in temp_bundle.rglob("*.lock"):
                lock_file.unlink()
            files = _bundle_files(temp_bundle)
            manifest = ResearchMigrationManifest(
                snapshot_id=snapshot_id,
                code_commit=code_commit,
                created_at=created_at.astimezone(UTC),
                source_snapshot_sha256=recovery.sha256,
                source_snapshot_size=recovery.file_size,
                datasets=tuple(dataset_evidence),
                auxiliary_tables=auxiliary,
                artifact_file_count=artifact_count,
                files=files,
                total_file_size=sum(item.file_size for item in files),
            )
            temp_manifest = temp_bundle / "bundle-manifest.json"
            temp_manifest.write_text(
                manifest.model_dump_json(indent=2) + "\n",
                encoding="utf-8",
            )
            _fsync_file(temp_manifest)
            _fsync_directory(temp_bundle)
            os.replace(temp_bundle, bundle_path)
            _fsync_directory(bundle_dir)
            return ResearchMigrationPrepareResult(
                status="created",
                bundle_path=bundle_path,
                manifest_path=manifest_path,
                manifest=manifest,
            )
        finally:
            if temp_bundle.exists():
                shutil.rmtree(temp_bundle)
