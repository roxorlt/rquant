"""Validated, partitioned Parquet exports for research-only market data."""

from __future__ import annotations

import hashlib
import json
import os
import re
import struct
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Protocol, cast
from zoneinfo import ZoneInfo

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_contracts import (
    DatasetContract,
    research_dataset_contract,
    research_export_schema,
)
from rquant.research_catalog import ResearchCatalog, exclusive_file_lock

ResearchDataset = Literal["minute_bar", "auction_bar"]
ResearchRunStatus = Literal["planned", "completed"]
PartitionStatus = Literal["planned", "exported", "unchanged", "replaced"]

_FREQUENCIES = {"1min", "5min", "15min", "30min", "60min"}
_CLEAN_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class _HashWriter(Protocol):
    def update(self, data: bytes) -> object: ...


class _ResearchModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResearchPartitionKey(_ResearchModel):
    dataset: ResearchDataset
    trade_date: date
    freq: str | None = None

    @model_validator(mode="after")
    def validate_dimensions(self) -> ResearchPartitionKey:
        if self.dataset == "minute_bar":
            if self.freq not in _FREQUENCIES:
                raise ValueError("minute_bar partition requires a supported freq")
        elif self.freq is not None:
            raise ValueError("auction_bar partition must not include freq")
        return self

    @property
    def partition_id(self) -> str:
        suffix = "" if self.freq is None else f":{self.freq}"
        return f"{self.dataset}:{self.trade_date.isoformat()}{suffix}"


class ResearchPartitionManifest(_ResearchModel):
    manifest_version: Literal[2] = 2
    dataset: ResearchDataset
    partition: ResearchPartitionKey
    relative_path: str
    row_count: int = Field(ge=1)
    earliest_time: datetime | date
    latest_time: datetime | date
    schema_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_content_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    source: str
    sources: tuple[str, ...] = Field(min_length=1)
    primary_key: tuple[str, ...] = Field(min_length=1)
    created_at: datetime
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    file_size: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_manifest(self) -> ResearchPartitionManifest:
        if self.dataset != self.partition.dataset:
            raise ValueError("manifest dataset must match partition dataset")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        if self.latest_time < self.earliest_time:
            raise ValueError("latest_time cannot precede earliest_time")
        return self


class ResearchPartitionResult(_ResearchModel):
    dataset: ResearchDataset
    trade_date: date
    freq: str | None
    row_count: int = Field(ge=1)
    status: PartitionStatus
    partition_path: str
    data_path: str | None = None
    manifest: ResearchPartitionManifest | None = None


class ResearchExportSummary(_ResearchModel):
    dataset: ResearchDataset
    start_date: date
    end_date: date
    status: ResearchRunStatus
    partition_count: int = Field(ge=0)
    row_count: int = Field(ge=0)
    exported_count: int = Field(ge=0)
    unchanged_count: int = Field(ge=0)
    replaced_count: int = Field(ge=0)
    partitions: tuple[ResearchPartitionResult, ...]


def partition_directory(key: ResearchPartitionKey) -> Path:
    day = key.trade_date.isoformat()
    calendar = Path(
        f"year={key.trade_date.year:04d}",
        f"month={key.trade_date.month:02d}",
        f"trade_date={day}",
    )
    if key.dataset == "minute_bar":
        return Path("minute", f"freq={key.freq}") / calendar
    return Path("auction") / calendar


def partition_manifest_relative_path(key: ResearchPartitionKey) -> Path:
    return partition_directory(key) / "manifest.json"


def partition_version_relative_path(key: ResearchPartitionKey, file_hash: str) -> Path:
    if re.fullmatch(r"[0-9a-f]{64}", file_hash) is None:
        raise ValueError("file_hash must be a 64-character SHA256")
    return partition_directory(key) / "versions" / f"{file_hash}.parquet"


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quoted_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table_columns(
    connection: duckdb.DuckDBPyConnection,
    contract: DatasetContract,
) -> tuple[tuple[str, str], ...]:
    rows = connection.execute(
        f"PRAGMA table_info({_quoted_literal(contract.table_name)})"
    ).fetchall()
    if not rows:
        raise ValueError(f"source table missing: {contract.table_name}")
    columns = tuple((str(row[1]), str(row[2])) for row in rows)
    expected_columns = research_export_schema(contract.dataset_id)
    if columns != expected_columns:
        raise ValueError(
            f"schema mismatch for {contract.dataset_id}: {columns} != {expected_columns}"
        )
    actual_pk = tuple(
        str(row[1]) for row in sorted(rows, key=lambda item: int(item[5])) if int(row[5]) > 0
    )
    if actual_pk != contract.physical_primary_key:
        raise ValueError(
            f"primary key mismatch for {contract.dataset_id}: "
            f"{actual_pk} != {contract.physical_primary_key}"
        )
    return columns


def _schema_hash(columns: tuple[tuple[str, str], ...]) -> str:
    payload = json.dumps(columns, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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


def _write_manifest_temp(path: Path, manifest: ResearchPartitionManifest) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write(manifest.model_dump_json(indent=2) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _update_logical_hash(digest: _HashWriter, value: object) -> None:
    if value is None:
        payload = b""
        marker = b"N"
    elif isinstance(value, bool):
        payload = b"1" if value else b"0"
        marker = b"B"
    elif isinstance(value, int):
        payload = str(value).encode("ascii")
        marker = b"I"
    elif isinstance(value, float):
        payload = struct.pack(">d", value)
        marker = b"F"
    elif isinstance(value, datetime):
        payload = value.isoformat(timespec="microseconds").encode("ascii")
        marker = b"T"
    elif isinstance(value, date):
        payload = value.isoformat().encode("ascii")
        marker = b"D"
    elif isinstance(value, str):
        payload = value.encode("utf-8")
        marker = b"S"
    else:
        raise TypeError(f"unsupported logical hash value: {type(value).__name__}")
    digest.update(marker)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _logical_content_hash(
    path: Path,
    *,
    columns: tuple[tuple[str, str], ...],
    primary_key: tuple[str, ...],
) -> str:
    parquet = _quoted_literal(str(path))
    selected = ", ".join(_quoted_identifier(column) for column, _ in columns)
    ordered = ", ".join(_quoted_identifier(column) for column in primary_key)
    digest = hashlib.sha256()
    with duckdb.connect(
        config={"temp_directory": ""},
    ) as connection:
        cursor = connection.execute(
            f"""
            SELECT {selected}
            FROM read_parquet({parquet}, hive_partitioning = false)
            ORDER BY {ordered}
            """
        )
        while rows := cursor.fetchmany(10_000):
            for row in rows:
                digest.update(b"R")
                for value in row:
                    _update_logical_hash(digest, value)
    return digest.hexdigest()


def _partition_predicate(key: ResearchPartitionKey, contract: DatasetContract) -> str:
    day = _quoted_literal(key.trade_date.isoformat())
    if key.dataset == "minute_bar":
        event_column = _quoted_identifier(cast(str, contract.event_time_column))
        return (
            f"CAST({event_column} AS DATE) = DATE {day} "
            f"AND {_quoted_identifier('freq')} = {_quoted_literal(cast(str, key.freq))}"
        )
    event_column = _quoted_identifier(cast(str, contract.event_date_column))
    return f"{event_column} = DATE {day}"


def _discover_partitions(
    connection: duckdb.DuckDBPyConnection,
    *,
    dataset: ResearchDataset,
    start_date: date,
    end_date: date,
    contract: DatasetContract,
) -> tuple[tuple[ResearchPartitionKey, int], ...]:
    table = _quoted_identifier(contract.table_name)
    start = _quoted_literal(start_date.isoformat())
    end = _quoted_literal(end_date.isoformat())
    if dataset == "minute_bar":
        event = _quoted_identifier(cast(str, contract.event_time_column))
        rows = connection.execute(
            f"""
            SELECT CAST({event} AS DATE), freq, COUNT(*)
            FROM {table}
            WHERE CAST({event} AS DATE) BETWEEN DATE {start} AND DATE {end}
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        ).fetchall()
        return tuple(
            (
                ResearchPartitionKey(
                    dataset="minute_bar", trade_date=cast(date, row[0]), freq=str(row[1])
                ),
                int(row[2]),
            )
            for row in rows
        )
    event = _quoted_identifier(cast(str, contract.event_date_column))
    rows = connection.execute(
        f"""
        SELECT {event}, COUNT(*)
        FROM {table}
        WHERE {event} BETWEEN DATE {start} AND DATE {end}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()
    return tuple(
        (
            ResearchPartitionKey(dataset="auction_bar", trade_date=cast(date, row[0])),
            int(row[1]),
        )
        for row in rows
    )


def _partition_sources(
    connection: duckdb.DuckDBPyConnection,
    key: ResearchPartitionKey,
    contract: DatasetContract,
) -> tuple[str, ...]:
    rows = connection.execute(
        f"""
        SELECT DISTINCT source
        FROM {_quoted_identifier(contract.table_name)}
        WHERE {_partition_predicate(key, contract)}
        ORDER BY source
        """
    ).fetchall()
    sources = tuple(str(row[0]) for row in rows)
    unknown = sorted(set(sources) - set(contract.sources))
    if unknown:
        raise ValueError(f"unknown source for {key.partition_id}: {', '.join(unknown)}")
    return sources


def _validate_range_sources(
    connection: duckdb.DuckDBPyConnection,
    *,
    start_date: date,
    end_date: date,
    contract: DatasetContract,
) -> None:
    event_column = contract.event_time_column or contract.event_date_column
    event = _quoted_identifier(cast(str, event_column))
    rows = connection.execute(
        f"""
        SELECT DISTINCT source
        FROM {_quoted_identifier(contract.table_name)}
        WHERE CAST({event} AS DATE)
              BETWEEN DATE {_quoted_literal(start_date.isoformat())}
                  AND DATE {_quoted_literal(end_date.isoformat())}
        """
    ).fetchall()
    unknown = sorted({str(row[0]) for row in rows} - set(contract.sources))
    if unknown:
        raise ValueError(f"unknown source for {contract.dataset_id}: {', '.join(unknown)}")


def _validate_partition_dates(
    connection: duckdb.DuckDBPyConnection,
    partitions: tuple[tuple[ResearchPartitionKey, int], ...],
    *,
    contract: DatasetContract,
    as_of_date: date,
) -> None:
    dates = tuple(sorted({key.trade_date for key, _ in partitions}))
    if not dates:
        return
    if contract.earliest_date is not None and dates[0] < contract.earliest_date:
        raise ValueError(
            f"{contract.dataset_id} partition precedes earliest date "
            f"{contract.earliest_date}: {dates[0]}"
        )
    if dates[-1] > as_of_date:
        raise ValueError(
            f"{contract.dataset_id} partition is in the future: {dates[-1]} > {as_of_date}"
        )
    placeholders = ",".join("?" for _ in dates)
    try:
        rows = connection.execute(
            f"""
            SELECT cal_date, is_open
            FROM trade_calendar
            WHERE exchange = 'SSE' AND cal_date IN ({placeholders})
            """,
            list(dates),
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError("authoritative SSE trade calendar is required") from exc
    open_dates = {cast(date, row[0]) for row in rows if bool(row[1])}
    invalid = tuple(day for day in dates if day not in open_dates)
    if invalid:
        raise ValueError(
            "closed or missing trade date in authoritative SSE calendar: "
            + ", ".join(day.isoformat() for day in invalid)
        )


def _write_partition_temp(
    connection: duckdb.DuckDBPyConnection,
    *,
    temp_path: Path,
    key: ResearchPartitionKey,
    contract: DatasetContract,
    columns: tuple[tuple[str, str], ...],
) -> None:
    selected = ", ".join(_quoted_identifier(column) for column, _ in columns)
    ordered = ", ".join(_quoted_identifier(column) for column in contract.physical_primary_key)
    connection.execute(
        f"""
        COPY (
            SELECT {selected}
            FROM {_quoted_identifier(contract.table_name)}
            WHERE {_partition_predicate(key, contract)}
            ORDER BY {ordered}
        ) TO {_quoted_literal(str(temp_path))}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def _validate_temp_partition(
    path: Path,
    *,
    key: ResearchPartitionKey,
    expected_rows: int,
    expected_columns: tuple[tuple[str, str], ...],
    contract: DatasetContract,
) -> tuple[datetime | date, datetime | date]:
    parquet = _quoted_literal(str(path))
    reader = f"read_parquet({parquet}, hive_partitioning = false)"
    with duckdb.connect(
        config={"temp_directory": ""},
    ) as validation:
        described = validation.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
        actual_columns = tuple((str(row[0]), str(row[1])) for row in described)
        if actual_columns != expected_columns:
            raise ValueError(
                f"schema mismatch for {key.partition_id}: {actual_columns} != {expected_columns}"
            )
        row = validation.execute(f"SELECT COUNT(*) FROM {reader}").fetchone()
        actual_rows = 0 if row is None else int(row[0])
        if actual_rows != expected_rows:
            raise ValueError(
                f"row count mismatch for {key.partition_id}: {actual_rows} != {expected_rows}"
            )
        keys = ", ".join(_quoted_identifier(column) for column in contract.physical_primary_key)
        duplicate = validation.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {keys}
                FROM {reader}
                GROUP BY {keys}
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()
        if duplicate is not None and int(duplicate[0]) > 0:
            raise ValueError(f"duplicate primary key in {key.partition_id}")

        mismatched = validation.execute(
            f"SELECT COUNT(*) FROM {reader} WHERE NOT ({_partition_predicate(key, contract)})"
        ).fetchone()
        if mismatched is not None and int(mismatched[0]) > 0:
            raise ValueError(
                f"partition mismatch for {key.partition_id}: {int(mismatched[0])} rows"
            )

        event_column = contract.event_time_column or contract.event_date_column
        event = _quoted_identifier(cast(str, event_column))
        bounds = validation.execute(f"SELECT MIN({event}), MAX({event}) FROM {reader}").fetchone()
        if bounds is None or bounds[0] is None or bounds[1] is None:
            raise ValueError(f"empty event bounds for {key.partition_id}")
        return cast(datetime | date, bounds[0]), cast(datetime | date, bounds[1])


def _load_existing_manifest(path: Path) -> ResearchPartitionManifest | None:
    if not path.is_file():
        return None
    return ResearchPartitionManifest.model_validate_json(path.read_text(encoding="utf-8"))


def _event_date(value: datetime | date) -> date:
    return value.date() if isinstance(value, datetime) else value


def _validate_existing_manifest(
    manifest: ResearchPartitionManifest,
    *,
    key: ResearchPartitionKey,
    contract: DatasetContract,
    schema_hash: str,
) -> Path:
    expected_path = partition_version_relative_path(key, manifest.file_hash)
    expected_source = manifest.sources[0] if len(manifest.sources) == 1 else "mixed"
    problems: list[str] = []
    if manifest.dataset != key.dataset or manifest.partition != key:
        problems.append("partition")
    if Path(manifest.relative_path) != expected_path:
        problems.append("relative_path")
    if manifest.primary_key != contract.physical_primary_key:
        problems.append("primary_key")
    if manifest.schema_hash != schema_hash:
        problems.append("schema_hash")
    if not set(manifest.sources) <= set(contract.sources) or manifest.source != expected_source:
        problems.append("source")
    if _event_date(manifest.earliest_time) != key.trade_date:
        problems.append("earliest_time")
    if _event_date(manifest.latest_time) != key.trade_date:
        problems.append("latest_time")
    if problems:
        raise ValueError(f"manifest binding mismatch for {key.partition_id}: {', '.join(problems)}")
    return expected_path


def _event_is_after_as_of(
    value: datetime | date,
    as_of_time: datetime,
) -> bool:
    if as_of_time.tzinfo is None or as_of_time.utcoffset() is None:
        raise ValueError("as_of_time must be timezone-aware")
    market_zone = ZoneInfo("Asia/Shanghai")
    if isinstance(value, datetime):
        event_time = (
            value.replace(tzinfo=market_zone)
            if value.tzinfo is None or value.utcoffset() is None
            else value
        )
        return event_time.astimezone(UTC) > as_of_time.astimezone(UTC)
    return value > as_of_time.astimezone(market_zone).date()


def verify_research_partition(
    *,
    lake_root: Path,
    manifest: ResearchPartitionManifest,
    as_of_time: datetime,
) -> Path:
    """Verify an immutable partition version without consulting the catalog head."""
    contract = research_dataset_contract(manifest.dataset)
    columns = research_export_schema(manifest.dataset)
    expected_schema_hash = _schema_hash(columns)
    relative_path = _validate_existing_manifest(
        manifest,
        key=manifest.partition,
        contract=contract,
        schema_hash=expected_schema_hash,
    )
    root = lake_root.resolve()
    path = (root / relative_path).resolve()
    if not path.is_relative_to(root):
        raise ValueError(
            f"partition path escapes lake root: {manifest.partition.partition_id}"
        )
    if not path.is_file():
        raise ValueError(
            f"partition file missing: {manifest.partition.partition_id}"
        )
    if path.stat().st_size != manifest.file_size:
        raise ValueError(
            f"partition file size mismatch: {manifest.partition.partition_id}"
        )
    if _file_sha256(path) != manifest.file_hash:
        raise ValueError(
            f"partition file hash mismatch: {manifest.partition.partition_id}"
        )
    earliest, latest = _validate_temp_partition(
        path,
        key=manifest.partition,
        expected_rows=manifest.row_count,
        expected_columns=columns,
        contract=contract,
    )
    if earliest != manifest.earliest_time or latest != manifest.latest_time:
        raise ValueError(
            f"partition event bounds mismatch: {manifest.partition.partition_id}"
        )
    observed_content_hash = _logical_content_hash(
        path,
        columns=columns,
        primary_key=contract.physical_primary_key,
    )
    if observed_content_hash != manifest.content_hash:
        raise ValueError(
            f"partition content hash mismatch: {manifest.partition.partition_id}"
        )
    if _event_is_after_as_of(latest, as_of_time):
        raise ValueError(
            "partition contains future data after as_of_time: "
            f"{manifest.partition.partition_id}"
        )
    return path


def _export_partition(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog: ResearchCatalog,
    lake_root: Path,
    key: ResearchPartitionKey,
    expected_rows: int,
    contract: DatasetContract,
    columns: tuple[tuple[str, str], ...],
    code_commit: str,
    now: Callable[[], datetime],
) -> ResearchPartitionResult:
    try:
        sources = _partition_sources(connection, key, contract)
    except Exception as exc:
        run_id = catalog.begin_run(
            dataset=key.dataset,
            partition_id=key.partition_id,
            code_commit=code_commit,
            started_at=now(),
        )
        catalog.finish_run(
            run_id,
            status="failed",
            error=f"{type(exc).__name__}: {exc}",
            finished_at=now(),
        )
        raise

    partition_root = lake_root / partition_directory(key)
    versions_root = partition_root / "versions"
    manifest_path = lake_root / partition_manifest_relative_path(key)
    lock_path = partition_root / ".export.lock"
    temp_path: Path | None = None
    temp_manifest_path: Path | None = None
    run_id: str | None = None
    partition_root.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(lock_path):
        started_at = now()
        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise ValueError("research export clock must be timezone-aware")
        run_id = catalog.begin_run(
            dataset=key.dataset,
            partition_id=key.partition_id,
            code_commit=code_commit,
            started_at=started_at,
        )
        try:
            versions_root.mkdir(parents=True, exist_ok=True)
            nonce = uuid.uuid4().hex
            temp_path = versions_root / f".data.parquet.tmp-{nonce}"
            temp_manifest_path = partition_root / f".manifest.json.tmp-{nonce}"
            _write_partition_temp(
                connection,
                temp_path=temp_path,
                key=key,
                contract=contract,
                columns=columns,
            )
            _fsync_file(temp_path)
            earliest, latest = _validate_temp_partition(
                temp_path,
                key=key,
                expected_rows=expected_rows,
                expected_columns=columns,
                contract=contract,
            )
            schema_hash = _schema_hash(columns)
            content_hash = _logical_content_hash(
                temp_path,
                columns=columns,
                primary_key=contract.physical_primary_key,
            )
            file_hash = _file_sha256(temp_path)
            existing = _load_existing_manifest(manifest_path)
            existing_path: Path | None = None
            observed_previous_file_hash: str | None = None
            existing_intact = False
            if existing is not None:
                existing_relative_path = _validate_existing_manifest(
                    existing,
                    key=key,
                    contract=contract,
                    schema_hash=schema_hash,
                )
                existing_path = lake_root / existing_relative_path
                if existing_path.is_file():
                    observed_previous_file_hash = _file_sha256(existing_path)
                    physical_intact = (
                        observed_previous_file_hash == existing.file_hash
                        and existing_path.stat().st_size == existing.file_size
                    )
                    existing_intact = physical_intact and (
                        _logical_content_hash(
                            existing_path,
                            columns=columns,
                            primary_key=contract.physical_primary_key,
                        )
                        == existing.content_hash
                    )

            unchanged = (
                existing is not None
                and existing_intact
                and existing.content_hash == content_hash
                and existing.row_count == expected_rows
                and existing.sources == sources
                and existing.earliest_time == earliest
                and existing.latest_time == latest
            )
            if unchanged:
                temp_path.unlink()
                temp_path = None
                catalog.finish_run(
                    run_id,
                    status="unchanged",
                    manifest=existing,
                    observed_previous_file_hash=observed_previous_file_hash,
                    finished_at=now(),
                )
                return ResearchPartitionResult(
                    dataset=key.dataset,
                    trade_date=key.trade_date,
                    freq=key.freq,
                    row_count=expected_rows,
                    status="unchanged",
                    partition_path=partition_directory(key).as_posix(),
                    data_path=existing.relative_path,
                    manifest=existing,
                )

            version_relative_path = partition_version_relative_path(key, file_hash)
            version_path = lake_root / version_relative_path
            if version_path.is_file() and _file_sha256(version_path) == file_hash:
                temp_path.unlink()
            else:
                os.replace(temp_path, version_path)
                _fsync_directory(versions_root)
            temp_path = None

            manifest = ResearchPartitionManifest(
                dataset=key.dataset,
                partition=key,
                relative_path=version_relative_path.as_posix(),
                row_count=expected_rows,
                earliest_time=earliest,
                latest_time=latest,
                schema_hash=schema_hash,
                content_hash=content_hash,
                file_hash=file_hash,
                parent_content_hash=None if existing is None else existing.content_hash,
                source=sources[0] if len(sources) == 1 else "mixed",
                sources=sources,
                primary_key=contract.physical_primary_key,
                created_at=started_at.astimezone(UTC),
                code_commit=code_commit,
                file_size=version_path.stat().st_size,
            )
            _write_manifest_temp(temp_manifest_path, manifest)
            os.replace(temp_manifest_path, manifest_path)
            temp_manifest_path = None
            _fsync_directory(partition_root)

            previous_hash = None if existing is None else existing.content_hash
            previous_file_hash = None if existing is None else existing.file_hash
            status: Literal["exported", "replaced"] = "exported" if existing is None else "replaced"
            catalog.finish_run(
                run_id,
                status=status,
                manifest=manifest,
                previous_content_hash=previous_hash,
                previous_file_hash=previous_file_hash,
                observed_previous_file_hash=observed_previous_file_hash,
                finished_at=now(),
            )
            return ResearchPartitionResult(
                dataset=key.dataset,
                trade_date=key.trade_date,
                freq=key.freq,
                row_count=expected_rows,
                status=status,
                partition_path=partition_directory(key).as_posix(),
                data_path=manifest.relative_path,
                manifest=manifest,
            )
        except Exception as exc:
            if run_id is not None:
                catalog.finish_run(
                    run_id,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                    finished_at=now(),
                )
            raise
        finally:
            for candidate in (temp_path, temp_manifest_path):
                if candidate is not None and candidate.exists():
                    candidate.unlink()


def export_research_dataset(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog: ResearchCatalog,
    lake_root: Path,
    dataset: ResearchDataset,
    start_date: date,
    end_date: date,
    code_commit: str,
    dry_run: bool = False,
    now: Callable[[], datetime] | None = None,
    as_of_date: date | None = None,
) -> ResearchExportSummary:
    """Plan or publish all non-empty partitions in an inclusive date range."""
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    if not dry_run and _CLEAN_COMMIT_PATTERN.fullmatch(code_commit) is None:
        raise ValueError("non-dry-run export requires a clean 40-character code commit")
    contract = research_dataset_contract(dataset)
    columns = _table_columns(connection, contract)
    partitions = _discover_partitions(
        connection,
        dataset=dataset,
        start_date=start_date,
        end_date=end_date,
        contract=contract,
    )
    _validate_partition_dates(
        connection,
        partitions,
        contract=contract,
        as_of_date=as_of_date or datetime.now(ZoneInfo("Asia/Shanghai")).date(),
    )
    if dry_run:
        _validate_range_sources(
            connection,
            start_date=start_date,
            end_date=end_date,
            contract=contract,
        )
        planned = tuple(
            ResearchPartitionResult(
                dataset=key.dataset,
                trade_date=key.trade_date,
                freq=key.freq,
                row_count=row_count,
                status="planned",
                partition_path=partition_directory(key).as_posix(),
            )
            for key, row_count in partitions
        )
        return ResearchExportSummary(
            dataset=dataset,
            start_date=start_date,
            end_date=end_date,
            status="planned",
            partition_count=len(planned),
            row_count=sum(result.row_count for result in planned),
            exported_count=0,
            unchanged_count=0,
            replaced_count=0,
            partitions=planned,
        )

    clock = now or (lambda: datetime.now(UTC))
    results = tuple(
        _export_partition(
            connection,
            catalog=catalog,
            lake_root=lake_root,
            key=key,
            expected_rows=row_count,
            contract=contract,
            columns=columns,
            code_commit=code_commit,
            now=clock,
        )
        for key, row_count in partitions
    )
    return ResearchExportSummary(
        dataset=dataset,
        start_date=start_date,
        end_date=end_date,
        status="completed",
        partition_count=len(results),
        row_count=sum(result.row_count for result in results),
        exported_count=sum(result.status == "exported" for result in results),
        unchanged_count=sum(result.status == "unchanged" for result in results),
        replaced_count=sum(result.status == "replaced" for result in results),
        partitions=results,
    )
