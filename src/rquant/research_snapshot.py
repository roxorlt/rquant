"""Immutable execution artifacts resolved from the research lake."""

from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import duckdb
import pandas as pd

from rquant.data_contracts import research_dataset_contract, research_export_schema
from rquant.data_metadata import (
    DatasetSnapshot,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    normalize_utc_datetime,
    utc_now,
)
from rquant.research_catalog import ResearchCatalog, ResearchPartitionRecord
from rquant.research_lake import (
    ResearchDataset,
    ResearchPartitionKey,
    ResearchPartitionManifest,
    _event_is_after_as_of,
    _file_sha256,
    _fsync_directory,
    _fsync_file,
    _logical_content_hash,
    _quoted_identifier,
    _quoted_literal,
    _schema_hash,
    _validate_temp_partition,
    partition_version_relative_path,
    verify_research_partition,
)
from rquant.strategy_dependencies import (
    SUSPENSION_SESSION_EVIDENCE_DATASET,
    StrategyExecutionDependencies,
    StrategyTableDependency,
    strategy_execution_dependencies,
)
from rquant.suspension_evidence import suspension_session_evidence_sql

if TYPE_CHECKING:
    from rquant.backfill_manifest import EligibilityResolution
    from rquant.storage.duckdb import DuckDBStore


class SnapshotMetadataStore(Protocol):
    def get_dataset_snapshot(self, snapshot_id: str) -> DatasetSnapshot | None: ...

    def get_dataset_snapshot_binding(
        self,
        snapshot_id: str,
    ) -> DatasetSnapshotBinding | None: ...

    def begin_dataset_snapshot_binding(
        self,
        binding: DatasetSnapshotBinding,
    ) -> DatasetSnapshotBinding: ...

    def finalize_dataset_snapshot_binding(
        self,
        snapshot_id: str,
        finalization: DatasetSnapshotBindingFinalization,
    ) -> DatasetSnapshotBinding: ...


def _source_table_schema(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]]:
    try:
        rows = connection.execute(
            f"PRAGMA table_info({_quoted_literal(table_name)})"
        ).fetchall()
    except duckdb.CatalogException as exc:
        raise ValueError(f"source table missing: {table_name}") from exc
    if not rows:
        raise ValueError(f"source table missing: {table_name}")
    columns = tuple((str(row[1]), str(row[2])) for row in rows)
    primary_key = tuple(
        str(row[1])
        for row in sorted(rows, key=lambda item: int(item[5]))
        if int(row[5]) > 0
    )
    if not primary_key:
        raise ValueError(f"source table requires a primary key: {table_name}")
    return columns, primary_key


def materialize_table_dependency(
    connection: duckdb.DuckDBPyConnection,
    *,
    dependency: StrategyTableDependency,
    artifact_root: Path,
    start_date: date,
    end_date: date,
    as_of_time: datetime,
    ts_codes: tuple[str, ...] | None = None,
    source_table_name: str | None = None,
) -> DatasetSnapshotArtifact:
    """Materialize a PIT-filtered small table as a content-addressed Parquet."""
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    as_of_time = normalize_utc_datetime(as_of_time)
    source_table = source_table_name or dependency.table_name
    columns, primary_key = _source_table_schema(connection, source_table)
    column_names = {name for name, _ in columns}
    declared_columns = (
        dependency.date_column,
        dependency.code_column,
        dependency.available_at_column,
    )
    missing_columns = [
        column for column in declared_columns if column and column not in column_names
    ]
    if missing_columns:
        raise ValueError(
            f"source table {dependency.table_name} missing declared columns: "
            + ", ".join(missing_columns)
        )

    predicates: list[str] = []
    parameters: list[object] = []
    if dependency.date_column is not None:
        date_column = _quoted_identifier(dependency.date_column)
        predicates.append(f"CAST({date_column} AS DATE) BETWEEN ? AND ?")
        parameters.extend((start_date, end_date))
    if dependency.available_at_column is not None:
        available = _quoted_identifier(dependency.available_at_column)
        predicates.append(f"{available} <= ?")
        parameters.append(as_of_time)
    selected_codes = None if ts_codes is None else tuple(sorted(set(ts_codes)))
    if dependency.code_column is not None and selected_codes is not None:
        if selected_codes:
            placeholders = ",".join("?" for _ in selected_codes)
            predicates.append(
                f"{_quoted_identifier(dependency.code_column)} IN ({placeholders})"
            )
            parameters.extend(selected_codes)
        else:
            predicates.append("FALSE")

    selected = ", ".join(_quoted_identifier(name) for name, _ in columns)
    ordered = ", ".join(_quoted_identifier(name) for name in primary_key)
    where = "" if not predicates else " WHERE " + " AND ".join(predicates)
    query = (
        f"SELECT {selected} FROM {_quoted_identifier(source_table)}"
        f"{where} ORDER BY {ordered}"
    )
    row = connection.execute(f"SELECT COUNT(*) FROM ({query})", parameters).fetchone()
    row_count = 0 if row is None else int(row[0])

    versions_root = (
        Path(artifact_root)
        / "tables"
        / dependency.table_name
        / "versions"
    )
    versions_root.mkdir(parents=True, exist_ok=True)
    temp_path = versions_root / f".data.parquet.tmp-{uuid.uuid4().hex}"
    try:
        connection.execute(
            f"""
            COPY ({query}) TO {_quoted_literal(str(temp_path))}
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            parameters,
        )
        _fsync_file(temp_path)
        file_hash = _file_sha256(temp_path)
        relative_path = (
            Path("tables")
            / dependency.table_name
            / "versions"
            / f"{file_hash}.parquet"
        )
        final_path = Path(artifact_root) / relative_path
        if final_path.is_file():
            if _file_sha256(final_path) != file_hash:
                raise ValueError(
                    "content-addressed materialized table is corrupt: "
                    f"{dependency.table_name}/{file_hash}"
                )
            temp_path.unlink()
        else:
            os.replace(temp_path, final_path)
            _fsync_directory(versions_root)

        schema_hash = _schema_hash(columns)
        content_hash = _logical_content_hash(
            final_path,
            columns=columns,
            primary_key=primary_key,
        )
        earliest_time: str | None = None
        latest_time: str | None = None
        if dependency.date_column is not None:
            date_column = _quoted_identifier(dependency.date_column)
            with duckdb.connect() as validation:
                bounds = validation.execute(
                    f"SELECT MIN({date_column}), MAX({date_column}) "
                    "FROM read_parquet(?)",
                    [str(final_path)],
                ).fetchone()
            if bounds is not None and bounds[0] is not None:
                earliest_time = cast(date | datetime, bounds[0]).isoformat()
                latest_time = cast(date | datetime, bounds[1]).isoformat()
        return DatasetSnapshotArtifact(
            artifact_type="materialized_table",
            dataset_id=dependency.dataset_id,
            table_name=dependency.table_name,
            artifact_key=(
                f"{dependency.dataset_id}:{start_date.isoformat()}:"
                f"{end_date.isoformat()}"
            ),
            relative_path=relative_path.as_posix(),
            row_count=row_count,
            schema_hash=schema_hash,
            content_hash=content_hash,
            file_hash=file_hash,
            file_size=final_path.stat().st_size,
            earliest_time=earliest_time,
            latest_time=latest_time,
            event_column=dependency.date_column,
            source="snapshot_materialization",
            primary_key=primary_key,
        )
    finally:
        if temp_path.exists():
            temp_path.unlink()


def materialize_suspension_session_evidence(
    connection: duckdb.DuckDBPyConnection,
    *,
    artifact_root: Path,
    start_date: date,
    end_date: date,
    as_of_time: datetime,
) -> DatasetSnapshotArtifact:
    """Freeze suspension evidence after resolving its full historical inputs."""
    as_of_time = normalize_utc_datetime(as_of_time)
    newer_coverage = connection.execute(
        """
        SELECT COUNT(*), MIN(trade_date), MAX(trade_date)
        FROM stock_suspend_coverage
        WHERE source = 'tushare'
          AND trade_date <= ?
          AND queried_at > ?
        """,
        [end_date, as_of_time],
    ).fetchone()
    if newer_coverage is not None and int(newer_coverage[0]) > 0:
        raise ValueError(
            "cannot reconstruct suspension evidence at requested as_of: "
            f"{int(newer_coverage[0])} current coverage versions from "
            f"{newer_coverage[1]} through {newer_coverage[2]} are newer"
        )

    temp_table = f"_snapshot_suspension_evidence_{uuid.uuid4().hex}"
    connection.execute(
        f"""
        CREATE TEMP TABLE {_quoted_identifier(temp_table)} (
            source VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            evidence_state VARCHAR NOT NULL,
            PRIMARY KEY (source, ts_code, trade_date)
        )
        """
    )
    try:
        evidence_sql = suspension_session_evidence_sql(
            "suspension.source = 'tushare' "
            "AND suspension.trade_date <= ? "
            "AND suspension.available_at <= ? "
            "AND coverage.queried_at <= ?"
        )
        connection.execute(
            f"""
            INSERT INTO {_quoted_identifier(temp_table)}
            SELECT source, ts_code, trade_date, evidence_state
            FROM ({evidence_sql})
            """,
            [end_date, as_of_time, as_of_time],
        )
        return materialize_table_dependency(
            connection,
            dependency=StrategyTableDependency(
                dataset_id=SUSPENSION_SESSION_EVIDENCE_DATASET,
                table_name=SUSPENSION_SESSION_EVIDENCE_DATASET,
            ),
            source_table_name=temp_table,
            artifact_root=artifact_root,
            start_date=start_date,
            end_date=end_date,
            as_of_time=as_of_time,
        )
    finally:
        connection.execute(
            f"DROP TABLE IF EXISTS {_quoted_identifier(temp_table)}"
        )


def materialize_eligibility_resolution(
    connection: duckdb.DuckDBPyConnection,
    *,
    resolution: EligibilityResolution,
    artifact_root: Path,
    as_of_time: datetime,
) -> DatasetSnapshotArtifact:
    """Publish the exact manifest eligibility keys as an execution table."""
    temp_table = f"_snapshot_eligibility_{uuid.uuid4().hex}"
    connection.execute(
        f"""
        CREATE TEMP TABLE {_quoted_identifier(temp_table)} (
            eligibility_id VARCHAR PRIMARY KEY,
            strategy_id VARCHAR NOT NULL,
            strategy_version VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            eligibility_date DATE NOT NULL,
            entry_date DATE NOT NULL,
            decision_at TIMESTAMPTZ NOT NULL,
            variant VARCHAR NOT NULL,
            resolution_hash VARCHAR NOT NULL
        )
        """
    )
    try:
        if resolution.records:
            connection.executemany(
                f"""
                INSERT INTO {_quoted_identifier(temp_table)}
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.eligibility_id,
                        row.strategy_id,
                        row.strategy_version,
                        row.ts_code,
                        row.eligibility_date,
                        row.entry_date,
                        row.decision_at,
                        row.variant,
                        resolution.resolution_hash,
                    )
                    for row in resolution.records
                ],
            )
        artifact = materialize_table_dependency(
            connection,
            dependency=StrategyTableDependency(
                dataset_id="strategy_eligibility",
                table_name="strategy_eligibility",
                date_column="eligibility_date",
                code_column="ts_code",
            ),
            source_table_name=temp_table,
            artifact_root=artifact_root,
            start_date=min(resolution.requested_dates),
            end_date=max(resolution.requested_dates),
            as_of_time=as_of_time,
        )
        return artifact.model_copy(
            update={
                "artifact_key": (
                    f"strategy_eligibility:{resolution.resolution_hash}"
                )
            }
        )
    finally:
        connection.execute(
            f"DROP TABLE IF EXISTS {_quoted_identifier(temp_table)}"
        )


def verify_materialized_table_artifact(
    artifact: DatasetSnapshotArtifact,
    *,
    lake_root: Path,
    as_of_time: datetime,
) -> Path:
    if artifact.artifact_type != "materialized_table":
        raise ValueError(
            "verify_materialized_table_artifact requires materialized_table"
        )
    expected_relative = (
        Path("tables")
        / artifact.table_name
        / "versions"
        / f"{artifact.file_hash}.parquet"
    )
    if Path(artifact.relative_path) != expected_relative:
        raise ValueError(
            f"materialized artifact path is not content-addressed: "
            f"{artifact.artifact_key}"
        )
    root = Path(lake_root).resolve()
    path = (root / expected_relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError(
            f"materialized artifact escapes lake root: {artifact.artifact_key}"
        )
    if not path.is_file():
        raise ValueError(
            f"materialized artifact file missing: {artifact.artifact_key}"
        )
    if artifact.file_size is not None and path.stat().st_size != artifact.file_size:
        raise ValueError(
            f"materialized artifact file size mismatch: {artifact.artifact_key}"
        )
    if _file_sha256(path) != artifact.file_hash:
        raise ValueError(
            f"materialized artifact file hash mismatch: {artifact.artifact_key}"
        )
    if not artifact.primary_key:
        raise ValueError(
            f"materialized artifact primary key missing: {artifact.artifact_key}"
        )

    parquet = _quoted_literal(str(path))
    reader = f"read_parquet({parquet}, hive_partitioning = false)"
    with duckdb.connect() as validation:
        described = validation.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
        columns = tuple((str(row[0]), str(row[1])) for row in described)
        if _schema_hash(columns) != artifact.schema_hash:
            raise ValueError(
                f"materialized artifact schema hash mismatch: "
                f"{artifact.artifact_key}"
            )
        row = validation.execute(f"SELECT COUNT(*) FROM {reader}").fetchone()
        row_count = 0 if row is None else int(row[0])
        if row_count != artifact.row_count:
            raise ValueError(
                f"materialized artifact row count mismatch: "
                f"{artifact.artifact_key}"
            )
        keys = ", ".join(
            _quoted_identifier(column) for column in artifact.primary_key
        )
        duplicate = validation.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT {keys} FROM {reader}
                GROUP BY {keys} HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
        if duplicate is not None and int(duplicate[0]) > 0:
            raise ValueError(
                f"materialized artifact duplicate primary key: "
                f"{artifact.artifact_key}"
            )
        bounds: tuple[object, object] | None = None
        if artifact.event_column is not None:
            event = _quoted_identifier(artifact.event_column)
            bounds = validation.execute(
                f"SELECT MIN({event}), MAX({event}) FROM {reader}"
            ).fetchone()

    if (
        _logical_content_hash(
            path,
            columns=columns,
            primary_key=artifact.primary_key,
        )
        != artifact.content_hash
    ):
        raise ValueError(
            f"materialized artifact content hash mismatch: {artifact.artifact_key}"
        )
    if bounds is not None and bounds[0] is not None:
        earliest = cast(date | datetime, bounds[0])
        latest = cast(date | datetime, bounds[1])
        if (
            artifact.earliest_time is not None
            and earliest.isoformat() != artifact.earliest_time
        ):
            raise ValueError(
                f"materialized artifact earliest_time mismatch: "
                f"{artifact.artifact_key}"
            )
        if (
            artifact.latest_time is not None
            and latest.isoformat() != artifact.latest_time
        ):
            raise ValueError(
                f"materialized artifact latest_time mismatch: "
                f"{artifact.artifact_key}"
            )
        if _event_is_after_as_of(latest, normalize_utc_datetime(as_of_time)):
            raise ValueError(
                f"materialized artifact contains future data: "
                f"{artifact.artifact_key}"
            )
    return path


def _manifest_from_record(
    record: ResearchPartitionRecord,
) -> ResearchPartitionManifest:
    manifest = ResearchPartitionManifest.model_validate_json(record.manifest_json)
    record_payload = (
        record.partition_id,
        record.dataset,
        record.trade_date,
        record.freq,
        record.relative_path,
        record.row_count,
        record.content_hash,
        record.file_hash,
        record.schema_hash,
    )
    manifest_payload = (
        manifest.partition.partition_id,
        manifest.dataset,
        manifest.partition.trade_date,
        manifest.partition.freq,
        manifest.relative_path,
        manifest.row_count,
        manifest.content_hash,
        manifest.file_hash,
        manifest.schema_hash,
    )
    if record_payload != manifest_payload:
        raise ValueError(
            f"research catalog manifest mismatch: {record.partition_id}"
        )
    return manifest


def _partition_key_from_artifact(
    artifact: DatasetSnapshotArtifact,
) -> ResearchPartitionKey:
    if artifact.partition_id is None:
        raise ValueError(
            f"lake artifact is missing partition_id: {artifact.artifact_key}"
        )
    parts = artifact.partition_id.split(":")
    if len(parts) not in {2, 3}:
        raise ValueError(f"invalid research partition id: {artifact.partition_id}")
    dataset = parts[0]
    if dataset not in {"minute_bar", "auction_bar"}:
        raise ValueError(f"unsupported research dataset: {dataset}")
    return ResearchPartitionKey(
        dataset=cast(ResearchDataset, dataset),
        trade_date=date.fromisoformat(parts[1]),
        freq=None if len(parts) == 2 else parts[2],
    )


def verify_snapshot_artifact(
    artifact: DatasetSnapshotArtifact,
    *,
    lake_root: Path,
    as_of_time: datetime,
) -> Path:
    """Verify a bound lake artifact using only immutable manifest evidence."""
    if artifact.artifact_type != "lake_partition":
        raise ValueError(
            "verify_snapshot_artifact currently requires a lake_partition"
        )
    key = _partition_key_from_artifact(artifact)
    if key.dataset != artifact.dataset_id:
        raise ValueError(
            f"artifact dataset disagrees with partition: {artifact.artifact_key}"
        )
    expected_relative = partition_version_relative_path(key, artifact.file_hash)
    if Path(artifact.relative_path) != expected_relative:
        raise ValueError(
            f"artifact path is not content-addressed: {artifact.artifact_key}"
        )
    root = lake_root.resolve()
    path = (root / expected_relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f"artifact path escapes lake root: {artifact.artifact_key}")
    if not path.is_file():
        raise ValueError(f"artifact file missing: {artifact.artifact_key}")
    if artifact.file_size is not None and path.stat().st_size != artifact.file_size:
        raise ValueError(f"artifact file size mismatch: {artifact.artifact_key}")
    if _file_sha256(path) != artifact.file_hash:
        raise ValueError(f"artifact file hash mismatch: {artifact.artifact_key}")

    contract = research_dataset_contract(cast(ResearchDataset, artifact.dataset_id))
    columns = research_export_schema(cast(ResearchDataset, artifact.dataset_id))
    if _schema_hash(columns) != artifact.schema_hash:
        raise ValueError(f"artifact schema hash mismatch: {artifact.artifact_key}")
    if artifact.primary_key and artifact.primary_key != contract.physical_primary_key:
        raise ValueError(f"artifact primary key mismatch: {artifact.artifact_key}")
    earliest, latest = _validate_temp_partition(
        path,
        key=key,
        expected_rows=artifact.row_count,
        expected_columns=columns,
        contract=contract,
    )
    if (
        artifact.earliest_time is not None
        and earliest.isoformat() != artifact.earliest_time
    ):
        raise ValueError(f"artifact earliest_time mismatch: {artifact.artifact_key}")
    if artifact.latest_time is not None and latest.isoformat() != artifact.latest_time:
        raise ValueError(f"artifact latest_time mismatch: {artifact.artifact_key}")
    if (
        _logical_content_hash(
            path,
            columns=columns,
            primary_key=contract.physical_primary_key,
        )
        != artifact.content_hash
    ):
        raise ValueError(f"artifact content hash mismatch: {artifact.artifact_key}")
    if _event_is_after_as_of(latest, as_of_time):
        raise ValueError(
            f"artifact contains future data after as_of_time: {artifact.artifact_key}"
        )
    return path


class SnapshotArtifactResolver:
    """Resolve current catalog heads into immutable, verified version files."""

    def __init__(self, *, catalog: ResearchCatalog, lake_root: Path) -> None:
        self.catalog = catalog
        self.lake_root = Path(lake_root)

    def resolve_lake_partitions(
        self,
        *,
        dataset: ResearchDataset,
        start_date: date,
        end_date: date,
        freq: str | None = None,
        as_of_time: datetime,
    ) -> tuple[DatasetSnapshotArtifact, ...]:
        records = self.catalog.list_partitions(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            freq=freq,
        )
        artifacts: list[DatasetSnapshotArtifact] = []
        for record in records:
            manifest = _manifest_from_record(record)
            verify_research_partition(
                lake_root=self.lake_root,
                manifest=manifest,
                as_of_time=as_of_time,
            )
            artifact = DatasetSnapshotArtifact(
                artifact_type="lake_partition",
                dataset_id=manifest.dataset,
                table_name=manifest.dataset,
                artifact_key=manifest.partition.partition_id,
                partition_id=manifest.partition.partition_id,
                relative_path=manifest.relative_path,
                row_count=manifest.row_count,
                schema_hash=manifest.schema_hash,
                content_hash=manifest.content_hash,
                file_hash=manifest.file_hash,
                file_size=manifest.file_size,
                earliest_time=manifest.earliest_time.isoformat(),
                latest_time=manifest.latest_time.isoformat(),
                event_column=(
                    research_dataset_contract(manifest.dataset).event_time_column
                    or research_dataset_contract(manifest.dataset).event_date_column
                ),
                source=manifest.source,
                primary_key=manifest.primary_key,
                revision_created_at=manifest.created_at,
                catalog_updated_at=record.updated_at,
            )
            verify_snapshot_artifact(
                artifact,
                lake_root=self.lake_root,
                as_of_time=as_of_time,
            )
            artifacts.append(artifact)
        return tuple(artifacts)


@contextmanager
def _shadow_lake_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: ResearchDataset,
    artifacts: tuple[DatasetSnapshotArtifact, ...],
    lake_root: Path,
    as_of_time: datetime,
) -> Iterator[None]:
    selected = tuple(
        artifact
        for artifact in artifacts
        if artifact.dataset_id == table_name
        and artifact.table_name == table_name
    )
    if not selected or len(selected) != len(artifacts):
        raise ValueError(
            f"eligibility input must contain only {table_name} artifacts"
        )
    paths = tuple(
        verify_snapshot_artifact(
            artifact,
            lake_root=lake_root,
            as_of_time=as_of_time,
        )
        for artifact in selected
    )
    readers = ", ".join(_quoted_literal(str(path)) for path in paths)
    connection.execute(
        f"""
        CREATE TEMP VIEW {_quoted_identifier(table_name)} AS
        SELECT * FROM read_parquet(
            [{readers}], hive_partitioning = false
        )
        """
    )
    try:
        yield
    finally:
        connection.execute(
            f"DROP VIEW IF EXISTS {_quoted_identifier(table_name)}"
        )


def resolve_strategy_eligibility_from_artifacts(
    store: DuckDBStore,
    *,
    strategy_id: str,
    start_date: date,
    end_date: date,
    input_artifacts: tuple[DatasetSnapshotArtifact, ...],
    lake_root: Path,
    as_of_time: datetime,
) -> EligibilityResolution:
    """Resolve auction eligibility from the exact lake files later bound."""
    from rquant.backfill_manifest import (
        EligibilityResolution,
        resolve_strategy_eligibility,
    )

    if strategy_id != "auction_gap":
        if input_artifacts:
            raise ValueError(
                "non-auction eligibility cannot use auction lake artifacts"
            )
        return resolve_strategy_eligibility(
            store,
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
        )
    with _shadow_lake_table(
        store._conn,
        table_name="auction_bar",
        artifacts=input_artifacts,
        lake_root=lake_root,
        as_of_time=as_of_time,
    ):
        resolution = resolve_strategy_eligibility(
            store,
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
        )
    return EligibilityResolution(
        strategy_id=resolution.strategy_id,
        strategy_version=resolution.strategy_version,
        requested_dates=resolution.requested_dates,
        evaluated_dates=resolution.evaluated_dates,
        complete_dates=resolution.complete_dates,
        incomplete=resolution.incomplete,
        records=resolution.records,
        input_artifacts=input_artifacts,
    )


def _publish_binding_manifest(
    *,
    lake_root: Path,
    binding: DatasetSnapshotBinding,
) -> Path:
    root = Path(lake_root).resolve()
    path = (root / binding.manifest_relative_path).resolve()
    if not path.is_relative_to(root):
        raise ValueError("binding manifest path escapes lake root")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        binding.manifest.model_dump_json(
            exclude_computed_fields=True,
            indent=2,
        )
        + "\n"
    )
    if path.is_file():
        existing = DatasetSnapshotBindingManifest.model_validate_json(
            path.read_text(encoding="utf-8")
        )
        if existing != binding.manifest:
            raise ValueError(
                f"immutable binding manifest conflict: {binding.snapshot_id}"
            )
        return path

    temp_path = path.parent / f".manifest.json.tmp-{uuid.uuid4().hex}"
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    published = DatasetSnapshotBindingManifest.model_validate_json(
        path.read_text(encoding="utf-8")
    )
    if published != binding.manifest:
        raise ValueError(
            f"published binding manifest verification failed: {binding.snapshot_id}"
        )
    return path


def build_dataset_snapshot_binding(
    *,
    metadata_store: SnapshotMetadataStore,
    source_connection: duckdb.DuckDBPyConnection,
    catalog: ResearchCatalog,
    lake_root: Path,
    snapshot_id: str,
    start_date: date,
    end_date: date,
    ts_codes: tuple[str, ...] | None = None,
    dependencies: StrategyExecutionDependencies | None = None,
    lake_artifacts: tuple[DatasetSnapshotArtifact, ...] | None = None,
    eligibility_resolution: EligibilityResolution | None = None,
    now: Callable[[], datetime] = utc_now,
) -> DatasetSnapshotBinding:
    """Build and publish one immutable execution binding for a ready snapshot."""
    snapshot = metadata_store.get_dataset_snapshot(snapshot_id)
    if snapshot is None:
        raise KeyError(f"dataset snapshot not found: {snapshot_id}")
    if snapshot.status != "ready":
        raise ValueError(f"dataset snapshot is not ready: {snapshot_id}")
    selected_dependencies = dependencies or strategy_execution_dependencies(
        snapshot.strategy_name
    )
    if selected_dependencies.strategy_id != snapshot.strategy_name:
        raise ValueError("dependency contract does not match snapshot strategy")

    artifacts: list[DatasetSnapshotArtifact] = []
    if eligibility_resolution is not None:
        if eligibility_resolution.strategy_id != snapshot.strategy_name:
            raise ValueError(
                "eligibility resolution does not match snapshot strategy"
            )
        expected_resolution_hash = snapshot.table_watermarks.get(
            "eligibility_resolution_hash"
        )
        if expected_resolution_hash != eligibility_resolution.resolution_hash:
            raise ValueError(
                "eligibility resolution does not match snapshot watermark"
            )
        if not eligibility_resolution.requested_dates:
            raise ValueError("eligibility resolution has no requested dates")
        artifacts.append(
            materialize_eligibility_resolution(
                source_connection,
                resolution=eligibility_resolution,
                artifact_root=lake_root,
                as_of_time=snapshot.as_of_time,
            )
        )
    if lake_artifacts is None:
        resolver = SnapshotArtifactResolver(
            catalog=catalog,
            lake_root=lake_root,
        )
        for dataset in selected_dependencies.lake_datasets:
            resolved = resolver.resolve_lake_partitions(
                dataset=dataset,
                start_date=start_date,
                end_date=end_date,
                freq="1min" if dataset == "minute_bar" else None,
                as_of_time=snapshot.as_of_time,
            )
            if not resolved:
                raise ValueError(
                    f"research lake has no {dataset} partitions in requested range"
                )
            artifacts.extend(resolved)
    else:
        required_datasets = set(selected_dependencies.lake_datasets)
        observed_datasets = {
            artifact.dataset_id for artifact in lake_artifacts
        }
        if observed_datasets != required_datasets:
            raise ValueError(
                "pinned lake artifacts do not match strategy dependencies: "
                f"required={sorted(required_datasets)} "
                f"observed={sorted(observed_datasets)}"
            )
        keys: set[str] = set()
        for artifact in lake_artifacts:
            if artifact.artifact_key in keys:
                raise ValueError(
                    f"duplicate pinned lake artifact: {artifact.artifact_key}"
                )
            keys.add(artifact.artifact_key)
            key = _partition_key_from_artifact(artifact)
            if not start_date <= key.trade_date <= end_date:
                raise ValueError(
                    "pinned lake artifact is outside binding range: "
                    f"{artifact.artifact_key}"
                )
            if key.dataset == "minute_bar" and key.freq != "1min":
                raise ValueError(
                    "pinned minute artifact has unexpected frequency: "
                    f"{artifact.artifact_key}"
                )
            verify_snapshot_artifact(
                artifact,
                lake_root=lake_root,
                as_of_time=snapshot.as_of_time,
            )
        artifacts.extend(lake_artifacts)
    for dependency in selected_dependencies.materialized_tables:
        if dependency.dataset_id == SUSPENSION_SESSION_EVIDENCE_DATASET:
            artifacts.append(
                materialize_suspension_session_evidence(
                    source_connection,
                    artifact_root=lake_root,
                    start_date=start_date,
                    end_date=end_date,
                    as_of_time=snapshot.as_of_time,
                )
            )
        else:
            artifacts.append(
                materialize_table_dependency(
                    source_connection,
                    dependency=dependency,
                    artifact_root=lake_root,
                    start_date=start_date,
                    end_date=end_date,
                    as_of_time=snapshot.as_of_time,
                    ts_codes=ts_codes,
                )
            )

    manifest = DatasetSnapshotBindingManifest(
        snapshot_id=snapshot.snapshot_id,
        strategy_name=snapshot.strategy_name,
        start_date=start_date,
        end_date=end_date,
        as_of_time=snapshot.as_of_time,
        code_commit=snapshot.code_commit,
        dependency_contract_version=selected_dependencies.contract_version,
        builder_version="snapshot-builder-v2",
        eligibility_resolution_hash=(
            None
            if eligibility_resolution is None
            else eligibility_resolution.resolution_hash
        ),
        eligibility_expected_dates=(
            None
            if eligibility_resolution is None
            else eligibility_resolution.expected_count
        ),
        eligibility_complete_dates=(
            None
            if eligibility_resolution is None
            else eligibility_resolution.available_count
        ),
        artifacts=tuple(
            sorted(artifacts, key=lambda artifact: artifact.artifact_key)
        ),
    )
    built_at = normalize_utc_datetime(now())
    provisional = DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="research_lake",
        manifest_relative_path="pending/manifest.json",
        created_at=built_at,
    )
    manifest_relative_path = (
        Path("snapshots")
        / snapshot.snapshot_id
        / provisional.binding_hash
        / "manifest.json"
    ).as_posix()
    binding = DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="research_lake",
        manifest_relative_path=manifest_relative_path,
        created_at=built_at,
    )
    if binding.binding_hash != provisional.binding_hash:
        raise RuntimeError("binding hash unexpectedly depends on artifact location")
    _publish_binding_manifest(lake_root=lake_root, binding=binding)
    stored = metadata_store.begin_dataset_snapshot_binding(binding)
    if stored.status == "ready":
        return stored
    return metadata_store.finalize_dataset_snapshot_binding(
        snapshot.snapshot_id,
        DatasetSnapshotBindingFinalization(
            completed_at=normalize_utc_datetime(now())
        ),
    )


class ResearchExecutionSession:
    """One verified immutable DuckDB connection shared by gate and compute."""

    def __init__(
        self,
        *,
        binding: DatasetSnapshotBinding,
        lake_root: Path,
    ) -> None:
        if binding.status != "ready":
            raise ValueError("research execution requires a ready binding")
        self.binding = binding
        self.lake_root = Path(lake_root)
        sessions_root = self.lake_root.resolve() / ".execution_sessions"
        sessions_root.mkdir(parents=True, exist_ok=True)
        self._session_dir = Path(
            tempfile.mkdtemp(
                prefix=f"{binding.snapshot_id[:12]}-",
                dir=sessions_root,
            )
        )
        self._conn = duckdb.connect(":memory:")
        self._closed = False
        try:
            self._open_verified_views()
        except Exception:
            self._conn.close()
            shutil.rmtree(self._session_dir, ignore_errors=True)
            self._closed = True
            raise

    @property
    def snapshot_id(self) -> str:
        return self.binding.snapshot_id

    @property
    def binding_hash(self) -> str:
        return self.binding.binding_hash

    def _open_verified_views(self) -> None:
        root = self.lake_root.resolve()
        manifest_path = (root / self.binding.manifest_relative_path).resolve()
        if not manifest_path.is_relative_to(root):
            raise ValueError("binding manifest path escapes lake root")
        if not manifest_path.is_file():
            raise ValueError("binding manifest file missing")
        published = DatasetSnapshotBindingManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
        if (
            published != self.binding.manifest
            or published.manifest_hash != self.binding.manifest_hash
        ):
            raise ValueError("binding manifest hash or content mismatch")

        by_table: dict[str, list[Path]] = {}
        for index, artifact in enumerate(published.artifacts):
            path = (
                verify_snapshot_artifact(
                    artifact,
                    lake_root=root,
                    as_of_time=published.as_of_time,
                )
                if artifact.artifact_type == "lake_partition"
                else verify_materialized_table_artifact(
                    artifact,
                    lake_root=root,
                    as_of_time=published.as_of_time,
                )
            )
            session_path = (
                self._session_dir
                / f"{index:06d}-{artifact.file_hash}.parquet"
            )
            os.link(path, session_path)
            if (
                (
                    artifact.file_size is not None
                    and session_path.stat().st_size != artifact.file_size
                )
                or _file_sha256(session_path) != artifact.file_hash
            ):
                raise ValueError(
                    "bound artifact changed while opening execution session: "
                    f"{artifact.artifact_key}"
                )
            by_table.setdefault(artifact.table_name, []).append(session_path)

        for table_name, paths in by_table.items():
            readers = ", ".join(_quoted_literal(str(path)) for path in paths)
            self._conn.execute(
                f"""
                CREATE VIEW {_quoted_identifier(table_name)} AS
                SELECT * FROM read_parquet(
                    [{readers}], hive_partitioning = false
                )
                """
            )

    def query_minute_bars(
        self,
        ts_code: str,
        start: str | date | pd.Timestamp,
        end: str | date | pd.Timestamp,
        *,
        freq: str = "1min",
    ) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, trade_time, freq, open, high, low, close,
                   vol, amount, source
            FROM minute_bar
            WHERE ts_code = ?
              AND freq = ?
              AND trade_time >= ?
              AND trade_time <= ?
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ts_code, trade_time, freq
                ORDER BY CASE source
                    WHEN 'tushare' THEN 0
                    WHEN 'tushare_rt' THEN 1
                    ELSE 2
                END
            ) = 1
            ORDER BY trade_time
            """,
            [ts_code, freq, start, end],
        ).fetchdf()

    def query_market_sentiment(
        self,
        trade_date: date | str,
    ) -> pd.DataFrame | None:
        frame = self._conn.execute(
            """
            SELECT trade_date, stock_count, up_count, down_count, flat_count,
                   limit_up_count, first_limit_up_count, limit_down_count,
                   yiziban_count, max_consecutive_limit_ups, high_board_count,
                   up_ratio_pct, limit_up_ratio_pct,
                   avg_pct_chg, median_pct_chg, total_amount
            FROM market_sentiment_daily
            WHERE trade_date = ?
            """,
            [trade_date],
        ).fetchdf()
        return None if frame.empty else frame

    def query_index_daily(self, trade_date: date | str) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, trade_date, open, high, low, close,
                   pre_close, change, pct_chg, vol, amount
            FROM index_daily_bar
            WHERE trade_date = ?
            ORDER BY ts_code
            """,
            [trade_date],
        ).fetchdf()

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            shutil.rmtree(self._session_dir, ignore_errors=True)
            self._closed = True

    def __enter__(self) -> ResearchExecutionSession:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def open_research_execution_session(
    metadata_store: SnapshotMetadataStore,
    *,
    snapshot_id: str,
    lake_root: Path,
) -> ResearchExecutionSession:
    binding = metadata_store.get_dataset_snapshot_binding(snapshot_id)
    if binding is None:
        raise ValueError(f"dataset snapshot execution binding missing: {snapshot_id}")
    return ResearchExecutionSession(binding=binding, lake_root=lake_root)
