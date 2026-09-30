"""Observe and freeze a sidecar-bound replica for retrospective factor research.

Counts describe the requested calculation scope. They do not prove provider
coverage, historical membership, or when a market fact was first received.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal, Protocol

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.data_metadata import (
    DatasetSnapshot,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotFinalization,
    normalize_utc_datetime,
    utc_now,
)
from rquant.factor.stream_snapshot import (
    FactorStreamSnapshotAdmissionRequest,
    _materialize_factor_stream_artifacts,
    _publish_factor_stream_binding,
    _validated_binding,
    open_factor_stream_snapshot_admission,
)
from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.replica_generation import ReplicaGenerationMetadata, replica_generation_path
from rquant.research_lake import _quoted_identifier
from rquant.research_snapshot import (
    _FACTOR_TABLE_COLUMNS,
    _FACTOR_TABLE_KEYS,
    FactorComputationScope,
    SnapshotMetadataStore,
    _source_table_schema,
)
from rquant.runtime_contracts import canonical_sha256

_IMMUTABLE = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
_SHA = r"^[0-9a-f]{64}$"
_TABLES = ("daily_bar", "adj_factor", "trade_calendar")
_MAX_SIDECAR_BYTES = 65_536


class FactorSourcePreparationMetadataStore(SnapshotMetadataStore, Protocol):
    def begin_dataset_snapshot(self, snapshot: DatasetSnapshot) -> DatasetSnapshot: ...

    def finalize_dataset_snapshot(
        self, snapshot_id: str, finalization: DatasetSnapshotFinalization
    ) -> DatasetSnapshot: ...


class FactorSourcePrepareRequest(BaseModel):
    model_config = _IMMUTABLE

    replica_path: Path
    expected_primary_path: Path
    scope: FactorComputationScope
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    @model_validator(mode="after")
    def validate_paths(self) -> FactorSourcePrepareRequest:
        # The expected primary is only a trusted name; never resolve or stat it.
        for path in (self.replica_path, self.expected_primary_path):
            if not path.is_absolute() or path != Path(os.path.abspath(path)):
                raise ValueError("source paths must be absolute and canonical")
        if self.replica_path == self.expected_primary_path:
            raise ValueError("source must be a distinct read-only replica")
        if self.replica_path.parent.resolve() != self.replica_path.parent:
            raise ValueError("replica parent must not contain symlinks")
        if self.scope.end_date > self.scope.as_of_time.date():
            raise ValueError("source scope extends beyond its cutoff")
        return self


class FactorSourceFileIdentity(BaseModel):
    model_config = _IMMUTABLE

    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)


class FactorSourceGeneration(BaseModel):
    model_config = _IMMUTABLE

    replica: FactorSourceFileIdentity
    sidecar: FactorSourceFileIdentity
    sidecar_sha256: str = Field(pattern=_SHA)
    metadata: ReplicaGenerationMetadata


class FactorSourceCodeCount(BaseModel):
    model_config = _IMMUTABLE

    code: str
    count: int = Field(ge=0)


class FactorSourceDateCount(BaseModel):
    model_config = _IMMUTABLE

    date: date
    count: int = Field(ge=0)


class FactorSourceNullCount(BaseModel):
    model_config = _IMMUTABLE

    column: str
    count: int = Field(ge=0)


class _ObservedTable(BaseModel):
    model_config = _IMMUTABLE

    table_name: Literal["daily_bar", "adj_factor", "trade_calendar"]
    row_count: int = Field(ge=0)
    min_date: date | None
    max_date: date | None
    code_counts: tuple[FactorSourceCodeCount, ...] = Field(max_length=7000)
    date_counts: tuple[FactorSourceDateCount, ...] = Field(max_length=4096)
    null_counts: tuple[FactorSourceNullCount, ...]
    structural_missing_rows: int = Field(ge=0)
    rows_on_closed_dates: int = Field(ge=0)


class FactorSourceTableObservation(_ObservedTable):
    artifact: DatasetSnapshotArtifact


class FactorSourcePreparationReceipt(BaseModel):
    model_config = _IMMUTABLE

    request: FactorSourcePrepareRequest
    request_sha256: str = Field(pattern=_SHA)
    generation: FactorSourceGeneration
    read_mode: Literal["descriptor", "in_place"]
    observed_at: datetime
    completed_read_at: datetime
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    source_read_boundary: Literal["single_snapshot_transaction"] = "single_snapshot_transaction"
    calendar_open_days: tuple[date, ...] = Field(max_length=4096)
    calendar_first_pretrade_date: date | None
    calendar_boundary_status: Literal["outside_anchor_unverified"] = "outside_anchor_unverified"
    tables: tuple[FactorSourceTableObservation, ...] = Field(min_length=3, max_length=3)
    scope_content_hash: str = Field(pattern=_SHA)
    sha256: str = Field(pattern=_SHA)

    @field_validator("observed_at", "completed_read_at")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def validate_observations(self) -> FactorSourcePreparationReceipt:
        if self.request_sha256 != canonical_sha256(self.request):
            raise ValueError("source preparation request digest mismatch")
        if self.completed_read_at < self.observed_at:
            raise ValueError("source observation clock moved backwards")
        if tuple(table.table_name for table in self.tables) != _TABLES:
            raise ValueError("source preparation requires exactly the three raw tables")
        scope = self.request.scope
        dates = _dates(scope)
        if tuple(sorted(set(self.calendar_open_days))) != self.calendar_open_days or not set(
            self.calendar_open_days
        ) <= set(dates):
            raise ValueError("source calendar open dates are invalid")
        for table in self.tables:
            codes = ("SSE",) if table.table_name == "trade_calendar" else scope.stock_codes
            if (
                tuple(item.code for item in table.code_counts) != codes
                or tuple(item.date for item in table.date_counts) != dates
            ):
                raise ValueError("source distributions differ from the calculation scope")
            nonempty = tuple(item.date for item in table.date_counts if item.count)
            if (
                sum(item.count for item in table.code_counts) != table.row_count
                or sum(item.count for item in table.date_counts) != table.row_count
                or table.min_date != (nonempty[0] if nonempty else None)
                or table.max_date != (nonempty[-1] if nonempty else None)
                or table.artifact.table_name != table.table_name
                or table.artifact.row_count != table.row_count
                or table.artifact.earliest_time
                != (table.min_date.isoformat() if table.min_date else None)
                or table.artifact.latest_time
                != (table.max_date.isoformat() if table.max_date else None)
            ):
                raise ValueError("source observations differ from exported artifact")
            if tuple(item.column for item in table.null_counts) != tuple(
                sorted(
                    _FACTOR_TABLE_COLUMNS[table.table_name]
                    - set(_FACTOR_TABLE_KEYS[table.table_name])
                )
            ) or any(item.count > table.row_count for item in table.null_counts):
                raise ValueError("source NULL observations are invalid")
            closed_rows = sum(
                item.count for item in table.date_counts if item.date not in self.calendar_open_days
            )
            if table.table_name == "trade_calendar":
                if table.row_count != len(dates) or any(
                    item.count != 1 for item in table.date_counts
                ):
                    raise ValueError("source SSE calendar does not cover the observed scope")
                missing, closed_rows = 0, 0
            else:
                missing = len(codes) * len(self.calendar_open_days) - table.row_count + closed_rows
            if (
                table.structural_missing_rows != missing
                or table.rows_on_closed_dates != closed_rows
            ):
                raise ValueError("source structural row observations are invalid")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("source preparation receipt digest mismatch")
        return self


class FactorPreparedStreamSource(BaseModel):
    model_config = _IMMUTABLE

    receipt: FactorSourcePreparationReceipt
    snapshot: DatasetSnapshot
    binding: DatasetSnapshotBinding
    admission_request: FactorStreamSnapshotAdmissionRequest
    scope_content_hash: str = Field(pattern=_SHA)
    sha256: str = Field(pattern=_SHA)

    @model_validator(mode="after")
    def validate_completed_source(self) -> FactorPreparedStreamSource:
        snapshot = DatasetSnapshot.model_validate(
            self.snapshot.model_dump(exclude_computed_fields=True)
        )
        binding = DatasetSnapshotBinding.model_validate(
            self.binding.model_dump(exclude_computed_fields=True)
        )
        scope = self.receipt.request.scope
        artifacts = {item.table_name: item for item in binding.manifest.artifacts}
        if (
            snapshot.status != "ready"
            or binding.status != "ready"
            or snapshot.manifest_id != self.receipt.sha256
            or snapshot.code_commit != self.receipt.request.code_commit
            or snapshot.as_of_time != scope.as_of_time
            or self.admission_request
            != FactorStreamSnapshotAdmissionRequest(
                snapshot_id=snapshot.snapshot_id, binding_hash=binding.binding_hash, scope=scope
            )
            or binding.snapshot_id != snapshot.snapshot_id
            or self.scope_content_hash != self.receipt.scope_content_hash
            or set(artifacts) != {*_TABLES, "factor_computation_scope"}
            or artifacts["factor_computation_scope"].content_hash != self.scope_content_hash
            or any(artifacts[table.table_name] != table.artifact for table in self.receipt.tables)
        ):
            raise ValueError("prepared source completion binding mismatch")
        _validated_binding(self.admission_request, snapshot, binding)
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("prepared source digest mismatch")
        return self


def _identity(value: os.stat_result) -> FactorSourceFileIdentity:
    return FactorSourceFileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
        ctime_ns=value.st_ctime_ns,
    )


def _regular_identity(path: Path) -> FactorSourceFileIdentity:
    observed = path.lstat()
    if not stat.S_ISREG(observed.st_mode):
        raise ValueError("source replica and sidecar must be regular non-symlink files")
    return _identity(observed)


def _generation(request: FactorSourcePrepareRequest) -> FactorSourceGeneration:
    replica = _regular_identity(request.replica_path)
    if os.path.lexists(f"{request.replica_path}.wal"):
        raise ValueError("source replica has an uncheckpointed WAL")
    path = replica_generation_path(request.replica_path)
    sidecar = _regular_identity(path)
    if not 0 < sidecar.size <= _MAX_SIDECAR_BYTES:
        raise ValueError("source generation sidecar size is invalid")
    descriptor = os.open(
        path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        if _identity(os.fstat(descriptor)) != sidecar:
            raise ValueError("source sidecar changed while opening")
        payload = os.read(descriptor, _MAX_SIDECAR_BYTES + 1)
        if (
            len(payload) != sidecar.size
            or _identity(os.fstat(descriptor)) != sidecar
            or _regular_identity(path) != sidecar
        ):
            raise ValueError("source sidecar changed while reading")
    finally:
        os.close(descriptor)
    metadata = ReplicaGenerationMetadata.model_validate_json(payload, strict=True)
    if (
        metadata.source_database != request.expected_primary_path
        or metadata.source_before != metadata.source_after
        or (metadata.source_before.main.device, metadata.source_before.main.inode)
        == (replica.device, replica.inode)
        or metadata.replica.model_dump() != replica.model_dump(exclude={"ctime_ns"})
        or replica.ctime_ns > sidecar.ctime_ns
    ):
        raise ValueError("source replica generation does not match its sidecar")
    return FactorSourceGeneration(
        replica=replica,
        sidecar=sidecar,
        sidecar_sha256=hashlib.sha256(payload).hexdigest(),
        metadata=metadata,
    )


def _check_generation(
    request: FactorSourcePrepareRequest, expected: FactorSourceGeneration, descriptor: int
) -> None:
    if _identity(os.fstat(descriptor)) != expected.replica or _generation(request) != expected:
        raise ValueError("source generation changed during preparation")


def _dates(scope: FactorComputationScope) -> tuple[date, ...]:
    return tuple(
        scope.start_date + timedelta(days=i)
        for i in range((scope.end_date - scope.start_date).days + 1)
    )


def _observe_source(
    connection: duckdb.DuckDBPyConnection, scope: FactorComputationScope
) -> tuple[tuple[_ObservedTable, ...], tuple[date, ...], date | None]:
    for table in _TABLES:
        columns, key = _source_table_schema(connection, table)
        types = dict(columns)
        if key != _FACTOR_TABLE_KEYS[table] or not _FACTOR_TABLE_COLUMNS[table] <= types.keys():
            raise ValueError(f"source required columns or business key mismatch: {table}")
        day_column = "cal_date" if table == "trade_calendar" else "trade_date"
        code_column = "exchange" if table == "trade_calendar" else "ts_code"
        if types[day_column] != "DATE" or types[code_column] != "VARCHAR":
            raise ValueError(f"source date or code schema mismatch: {table}")
    calendar = connection.execute(
        "SELECT cal_date, is_open, pretrade_date FROM trade_calendar "
        "WHERE exchange='SSE' AND cal_date BETWEEN ? AND ? ORDER BY cal_date",
        [scope.start_date, scope.end_date],
    ).fetchall()
    dates = _dates(scope)
    if tuple(row[0] for row in calendar) != dates:
        raise ValueError("source SSE calendar does not cover every natural date")
    anchor = calendar[0][2]
    if anchor is not None and (not isinstance(anchor, date) or anchor >= dates[0]):
        raise ValueError("source calendar first previous-day anchor is invalid")
    previous = anchor
    open_days = []
    for day, is_open, pretrade in calendar:
        if is_open not in (True, False) or is_open is None or pretrade != previous:
            raise ValueError("source SSE calendar previous-day chain is inconsistent")
        if is_open:
            open_days.append(day)
            previous = day
    observations = []
    for table in _TABLES:
        day_column = "cal_date" if table == "trade_calendar" else "trade_date"
        code_column = "exchange" if table == "trade_calendar" else "ts_code"
        codes = ("SSE",) if table == "trade_calendar" else scope.stock_codes
        where = (
            f"{day_column} BETWEEN ? AND ? AND {code_column} IN ({','.join('?' for _ in codes)})"
        )
        parameters = [scope.start_date, scope.end_date, *codes]
        null_columns = sorted(_FACTOR_TABLE_COLUMNS[table] - set(_FACTOR_TABLE_KEYS[table]))
        null_sql = ", ".join(
            f"count(*) FILTER (WHERE {_quoted_identifier(column)} IS NULL)"
            for column in null_columns
        )
        row = connection.execute(
            f"SELECT count(*), min({day_column}), max({day_column}), {null_sql} "
            f"FROM {table} WHERE {where}",
            parameters,
        ).fetchone()
        by_code = dict(
            connection.execute(
                f"SELECT {code_column}, count(*) FROM {table} WHERE {where} "
                f"GROUP BY {code_column} ORDER BY {code_column}",
                parameters,
            ).fetchall()
        )
        by_date = dict(
            connection.execute(
                f"SELECT {day_column}, count(*) FROM {table} WHERE {where} "
                f"GROUP BY {day_column} ORDER BY {day_column}",
                parameters,
            ).fetchall()
        )
        assert row is not None
        closed_rows = sum(count for day, count in by_date.items() if day not in open_days)
        observations.append(
            _ObservedTable(
                table_name=table,
                row_count=row[0],
                min_date=row[1],
                max_date=row[2],
                code_counts=tuple(
                    FactorSourceCodeCount(code=code, count=by_code.get(code, 0)) for code in codes
                ),
                date_counts=tuple(
                    FactorSourceDateCount(date=day, count=by_date.get(day, 0)) for day in dates
                ),
                null_counts=tuple(
                    FactorSourceNullCount(column=column, count=count)
                    for column, count in zip(null_columns, row[3:], strict=True)
                ),
                structural_missing_rows=0
                if table == "trade_calendar"
                else len(codes) * len(open_days) - row[0] + closed_rows,
                rows_on_closed_dates=0 if table == "trade_calendar" else closed_rows,
            )
        )
    return tuple(observations), tuple(open_days), anchor


def prepare_factor_stream_source(
    request: FactorSourcePrepareRequest,
    *,
    metadata_store: FactorSourcePreparationMetadataStore,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorPreparedStreamSource:
    """Prepare actual raw evidence; only final admission constitutes complete success."""
    request = FactorSourcePrepareRequest.model_validate(request)
    generation = _generation(request)
    root = Path(lake_root).resolve()
    if root.is_relative_to(request.replica_path.parent):
        raise ValueError("preparation lake and scratch must be outside the replica directory")
    root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".factor-source-prepare-", dir=root) as scratch:
        descriptor = os.open(
            request.replica_path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        connection = None
        transaction = False
        try:
            _check_generation(request, generation, descriptor)
            connection, mode = connect_pinned_readonly(request.replica_path, descriptor)
            connection.execute("SET temp_directory = ?", [scratch])
            connection.execute("SET threads=1")
            _check_generation(request, generation, descriptor)
            connection.execute("BEGIN TRANSACTION")
            transaction = True
            observed_at = normalize_utc_datetime(now())
            observations, open_days, anchor = _observe_source(connection, request.scope)
            artifacts = _materialize_factor_stream_artifacts(
                source_connection=connection, lake_root=root, scope=request.scope
            )
            exported = {artifact.table_name: artifact for artifact in artifacts}
            tables = tuple(
                FactorSourceTableObservation(
                    **table.model_dump(), artifact=exported[table.table_name]
                )
                for table in observations
            )
            receipt_fields = dict(
                request=request,
                request_sha256=canonical_sha256(request),
                generation=generation,
                read_mode=mode,
                observed_at=observed_at,
                completed_read_at=normalize_utc_datetime(now()),
                source_mode="historical_retrospective",
                source_read_boundary="single_snapshot_transaction",
                calendar_open_days=open_days,
                calendar_first_pretrade_date=anchor,
                calendar_boundary_status="outside_anchor_unverified",
                tables=tables,
                scope_content_hash=exported["factor_computation_scope"].content_hash,
            )
            receipt = FactorSourcePreparationReceipt(
                **receipt_fields, sha256=canonical_sha256(receipt_fields)
            )
            connection.execute("COMMIT")
            transaction = False
            _check_generation(request, generation, descriptor)
            # Later replica refreshes do not invalidate the verified historical artifacts.
            building = DatasetSnapshot.create(
                strategy_name="factor_eval",
                manifest_id=receipt.sha256,
                as_of_time=request.scope.as_of_time,
                code_commit=request.code_commit,
                origin="factor-readonly-source-prepare-v1",
                created_at=normalize_utc_datetime(now()),
            )
            metadata_store.begin_dataset_snapshot(building)
            calendar = tables[2]
            watermarks = {
                "manifest_start_date": calendar.min_date.isoformat(),
                "manifest_end_date": calendar.max_date.isoformat(),
                "source_preparation_sha256": receipt.sha256,
                "source_generation_sha256": canonical_sha256(generation),
            }
            for table in tables[:2]:
                if table.min_date is not None:
                    watermarks[f"{table.table_name}_min_date"] = table.min_date.isoformat()
                    watermarks[f"{table.table_name}_max_date"] = table.max_date.isoformat()
            snapshot = metadata_store.finalize_dataset_snapshot(
                building.snapshot_id,
                DatasetSnapshotFinalization(
                    table_watermarks=watermarks, completed_at=normalize_utc_datetime(now())
                ),
            )
            binding = _publish_factor_stream_binding(
                metadata_store=metadata_store,
                lake_root=root,
                snapshot=snapshot,
                scope=request.scope,
                artifacts=artifacts,
                now=now,
            )
            admission = FactorStreamSnapshotAdmissionRequest(
                snapshot_id=snapshot.snapshot_id,
                binding_hash=binding.binding_hash,
                scope=request.scope,
            )
            with open_factor_stream_snapshot_admission(
                admission, metadata_store=metadata_store, lake_root=root
            ) as (_lease, decision):
                fields = dict(
                    receipt=receipt,
                    snapshot=snapshot,
                    binding=binding,
                    admission_request=admission,
                    scope_content_hash=decision.scope_content_hash,
                )
                prepared = FactorPreparedStreamSource(**fields, sha256=canonical_sha256(fields))
        finally:
            try:
                if connection is not None:
                    if transaction:
                        with suppress(Exception):
                            connection.execute("ROLLBACK")
                    connection.close()
            finally:
                os.close(descriptor)
    return prepared
