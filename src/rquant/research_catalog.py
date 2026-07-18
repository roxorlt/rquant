"""Short-lived, serialized metadata transactions for the research lake."""

from __future__ import annotations

import fcntl
import json
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import duckdb
from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from rquant.research_lake import ResearchPartitionManifest


class _CatalogModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResearchPartitionRecord(_CatalogModel):
    partition_id: str
    dataset: str
    trade_date: date
    freq: str | None
    relative_path: str
    row_count: int
    content_hash: str
    file_hash: str
    schema_hash: str
    manifest_json: str
    created_at: datetime
    updated_at: datetime


class ResearchIngestRun(_CatalogModel):
    run_id: str
    dataset: str
    partition_id: str
    status: Literal["running", "exported", "unchanged", "replaced", "failed"]
    previous_content_hash: str | None
    previous_file_hash: str | None
    observed_previous_file_hash: str | None
    content_hash: str | None
    file_hash: str | None
    row_count: int | None
    error: str | None
    code_commit: str
    started_at: datetime
    finished_at: datetime | None


class ResearchDatasetCoverage(_CatalogModel):
    dataset: str
    earliest_date: date
    latest_date: date
    partition_count: int
    row_count: int
    updated_at: datetime


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS research_partition (
    partition_id VARCHAR PRIMARY KEY,
    dataset VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    freq VARCHAR,
    relative_path VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    content_hash VARCHAR NOT NULL,
    file_hash VARCHAR NOT NULL,
    schema_hash VARCHAR NOT NULL,
    manifest_json JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS research_ingest_run (
    run_id VARCHAR PRIMARY KEY,
    dataset VARCHAR NOT NULL,
    partition_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    previous_content_hash VARCHAR,
    previous_file_hash VARCHAR,
    observed_previous_file_hash VARCHAR,
    content_hash VARCHAR,
    file_hash VARCHAR,
    row_count BIGINT,
    error VARCHAR,
    code_commit VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS research_dataset_coverage (
    dataset VARCHAR PRIMARY KEY,
    earliest_date DATE NOT NULL,
    latest_date DATE NOT NULL,
    partition_count BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
"""


def _utc_now() -> datetime:
    return datetime.now(UTC)


@contextmanager
def exclusive_file_lock(path: Path) -> Iterator[None]:
    """Serialize publishers on macOS/Linux without holding a DuckDB connection."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


class ResearchCatalog:
    """A path-only catalog handle with explicit reader and publisher modes."""

    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        self.path = Path(path)
        self.read_only = read_only
        if read_only and (
            not self.path.is_file() or self.path.is_symlink()
        ):
            raise ValueError(
                f"read-only research catalog is invalid: {self.path}"
            )

    @property
    def lock_path(self) -> Path:
        return self.path.with_name(f"{self.path.name}.lock")

    @contextmanager
    def _connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        if self.read_only:
            connection = duckdb.connect(
                str(self.path),
                read_only=True,
                config={"temp_directory": ""},
            )
            try:
                yield connection
            finally:
                connection.close()
            return
        with exclusive_file_lock(self.lock_path):
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = duckdb.connect(str(self.path))
            try:
                connection.execute(_SCHEMA_SQL)
                yield connection
            finally:
                connection.close()

    def _require_writable(self) -> None:
        if self.read_only:
            raise RuntimeError("read-only research catalog cannot publish")

    def begin_run(
        self,
        *,
        dataset: str,
        partition_id: str,
        code_commit: str,
        started_at: datetime | None = None,
    ) -> str:
        self._require_writable()
        run_id = uuid.uuid4().hex
        began_at = started_at or _utc_now()
        with self._connection() as connection:
            connection.execute("BEGIN")
            connection.execute(
                """
                UPDATE research_ingest_run
                SET status = 'failed',
                    error = 'superseded after interrupted export',
                    finished_at = ?
                WHERE partition_id = ? AND status = 'running'
                """,
                [began_at, partition_id],
            )
            connection.execute(
                """
                INSERT INTO research_ingest_run (
                    run_id, dataset, partition_id, status, code_commit, started_at
                ) VALUES (?, ?, ?, 'running', ?, ?)
                """,
                [run_id, dataset, partition_id, code_commit, began_at],
            )
            connection.execute("COMMIT")
        return run_id

    def finish_run(
        self,
        run_id: str,
        *,
        status: Literal["exported", "unchanged", "replaced", "failed"],
        manifest: ResearchPartitionManifest | None = None,
        previous_content_hash: str | None = None,
        previous_file_hash: str | None = None,
        observed_previous_file_hash: str | None = None,
        error: str | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        self._require_writable()
        completed_at = finished_at or _utc_now()
        with self._connection() as connection:
            connection.execute("BEGIN")
            if manifest is not None:
                # The atomic partition manifest is authoritative. The caller holds
                # that partition's publish lock, so this index can safely catch up
                # after any number of missed catalog writes.
                connection.execute(
                    """
                    INSERT INTO research_partition (
                        partition_id, dataset, trade_date, freq, relative_path,
                        row_count, content_hash, file_hash, schema_hash,
                        manifest_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (partition_id) DO UPDATE SET
                        relative_path = excluded.relative_path,
                        row_count = excluded.row_count,
                        content_hash = excluded.content_hash,
                        file_hash = excluded.file_hash,
                        schema_hash = excluded.schema_hash,
                        manifest_json = excluded.manifest_json,
                        updated_at = excluded.updated_at
                    """,
                    [
                        manifest.partition.partition_id,
                        manifest.dataset,
                        manifest.partition.trade_date,
                        manifest.partition.freq,
                        manifest.relative_path,
                        manifest.row_count,
                        manifest.content_hash,
                        manifest.file_hash,
                        manifest.schema_hash,
                        manifest.model_dump_json(),
                        manifest.created_at,
                        completed_at,
                    ],
                )
                self._refresh_coverage(connection, manifest.dataset, completed_at)

            connection.execute(
                """
                UPDATE research_ingest_run
                SET status = ?, previous_content_hash = ?, previous_file_hash = ?,
                    observed_previous_file_hash = ?, content_hash = ?, file_hash = ?,
                    row_count = ?, error = ?, finished_at = ?
                WHERE run_id = ?
                """,
                [
                    status,
                    previous_content_hash,
                    previous_file_hash,
                    observed_previous_file_hash,
                    None if manifest is None else manifest.content_hash,
                    None if manifest is None else manifest.file_hash,
                    None if manifest is None else manifest.row_count,
                    error,
                    completed_at,
                    run_id,
                ],
            )
            connection.execute("COMMIT")

    @staticmethod
    def _refresh_coverage(
        connection: duckdb.DuckDBPyConnection,
        dataset: str,
        updated_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO research_dataset_coverage
            SELECT dataset, MIN(trade_date), MAX(trade_date), COUNT(*), SUM(row_count), ?
            FROM research_partition
            WHERE dataset = ?
            GROUP BY dataset
            ON CONFLICT (dataset) DO UPDATE SET
                earliest_date = excluded.earliest_date,
                latest_date = excluded.latest_date,
                partition_count = excluded.partition_count,
                row_count = excluded.row_count,
                updated_at = excluded.updated_at
            """,
            [updated_at, dataset],
        )

    def get_partition(self, partition_id: str) -> ResearchPartitionRecord | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT partition_id, dataset, trade_date, freq, relative_path,
                       row_count, content_hash, file_hash, schema_hash, manifest_json,
                       created_at, updated_at
                FROM research_partition
                WHERE partition_id = ?
                """,
                [partition_id],
            ).fetchone()
        if row is None:
            return None
        return ResearchPartitionRecord(
            partition_id=str(row[0]),
            dataset=str(row[1]),
            trade_date=cast(date, row[2]),
            freq=None if row[3] is None else str(row[3]),
            relative_path=str(row[4]),
            row_count=int(row[5]),
            content_hash=str(row[6]),
            file_hash=str(row[7]),
            schema_hash=str(row[8]),
            manifest_json=json.dumps(row[9]) if not isinstance(row[9], str) else row[9],
            created_at=cast(datetime, row[10]),
            updated_at=cast(datetime, row[11]),
        )

    def list_partitions(
        self,
        *,
        dataset: str,
        start_date: date,
        end_date: date,
        freq: str | None = None,
    ) -> list[ResearchPartitionRecord]:
        if start_date > end_date:
            raise ValueError("start_date cannot be after end_date")
        parameters: list[object] = [dataset, start_date, end_date]
        freq_predicate = ""
        if freq is not None:
            freq_predicate = " AND freq = ?"
            parameters.append(freq)
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT partition_id, dataset, trade_date, freq, relative_path,
                       row_count, content_hash, file_hash, schema_hash,
                       manifest_json, created_at, updated_at
                FROM research_partition
                WHERE dataset = ? AND trade_date BETWEEN ? AND ?
                """
                + freq_predicate
                + " ORDER BY trade_date, freq, partition_id",
                parameters,
            ).fetchall()
        return [
            ResearchPartitionRecord(
                partition_id=str(row[0]),
                dataset=str(row[1]),
                trade_date=cast(date, row[2]),
                freq=None if row[3] is None else str(row[3]),
                relative_path=str(row[4]),
                row_count=int(row[5]),
                content_hash=str(row[6]),
                file_hash=str(row[7]),
                schema_hash=str(row[8]),
                manifest_json=(
                    json.dumps(row[9]) if not isinstance(row[9], str) else row[9]
                ),
                created_at=cast(datetime, row[10]),
                updated_at=cast(datetime, row[11]),
            )
            for row in rows
        ]

    def get_coverage(self, dataset: str) -> ResearchDatasetCoverage | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT dataset, earliest_date, latest_date, partition_count,
                       row_count, updated_at
                FROM research_dataset_coverage
                WHERE dataset = ?
                """,
                [dataset],
            ).fetchone()
        if row is None:
            return None
        return ResearchDatasetCoverage(
            dataset=str(row[0]),
            earliest_date=cast(date, row[1]),
            latest_date=cast(date, row[2]),
            partition_count=int(row[3]),
            row_count=int(row[4]),
            updated_at=cast(datetime, row[5]),
        )

    def list_ingest_runs(self, dataset: str) -> list[ResearchIngestRun]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT run_id, dataset, partition_id, status,
                       previous_content_hash, previous_file_hash,
                       observed_previous_file_hash, content_hash, file_hash,
                       row_count, error, code_commit, started_at, finished_at
                FROM research_ingest_run
                WHERE dataset = ?
                ORDER BY started_at, finished_at, run_id
                """,
                [dataset],
            ).fetchall()
        return [
            ResearchIngestRun(
                run_id=str(row[0]),
                dataset=str(row[1]),
                partition_id=str(row[2]),
                status=cast(
                    Literal["running", "exported", "unchanged", "replaced", "failed"],
                    row[3],
                ),
                previous_content_hash=None if row[4] is None else str(row[4]),
                previous_file_hash=None if row[5] is None else str(row[5]),
                observed_previous_file_hash=None if row[6] is None else str(row[6]),
                content_hash=None if row[7] is None else str(row[7]),
                file_hash=None if row[8] is None else str(row[8]),
                row_count=None if row[9] is None else int(row[9]),
                error=None if row[10] is None else str(row[10]),
                code_commit=str(row[11]),
                started_at=cast(datetime, row[12]),
                finished_at=None if row[13] is None else cast(datetime, row[13]),
            )
            for row in rows
        ]
