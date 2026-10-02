"""Stored daily facts from two tables in the prepared price replica generation.

These inventory values are retrospective, not recomputed or a verified PIT
indicator series. Only the adapter assigns the existing next-day 09:25 assumption.
"""

from __future__ import annotations

import math
import os
import shutil
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.data_metadata import DatasetSnapshotArtifact, normalize_utc_datetime, utc_now
from rquant.factor.result_artifact import _open_private_root, _require_same_root, _root_path
from rquant.factor.source_prepare import (
    FactorPreparedStreamSource,
    FactorSourceCodeCount,
    FactorSourceDateCount,
    FactorSourceGeneration,
    _check_generation,
    _dates,
    _generation,
)
from rquant.factor.universe import StockCode
from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.research_lake import _quoted_literal
from rquant.research_snapshot import (
    FactorComputationScope,
    _source_table_schema,
    materialize_table_dependency,
    verify_materialized_table_artifact,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_dependencies import StrategyTableDependency

_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
_SHA = r"^[0-9a-f]{64}$"
DailyStoredColumn = Literal[
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "rsi6",
    "rsi14",
    "macd",
    "macd_signal",
    "macd_hist",
    "kdj_k",
    "kdj_d",
    "kdj_j",
    "turnover_rate",
    "volume_ratio",
    "total_mv",
    "circ_mv",
]
DailyFeatureStatus = Literal["valid", "missing", "null", "non_finite"]


class FactorDailyStoredField(BaseModel):
    model_config = _MODEL
    column: DailyStoredColumn
    table: Literal["daily_indicator", "daily_basic"]
    name_zh: str
    unit: Literal["stored_price", "indicator", "percent", "ratio", "CNY_10000"]
    description_zh: str


STORED_DAILY_FIELDS = tuple(
    sorted(
        (
            *(
                FactorDailyStoredField(
                    column=c,
                    table="daily_indicator",
                    name_zh=n,
                    unit="stored_price",
                    description_zh=d,
                )
                for c, n, d in (
                    ("ma5", "5日均线", "已存5日均线，价格基准未核验。"),
                    ("ma10", "10日均线", "已存10日均线，价格基准未核验。"),
                    ("ma20", "20日均线", "已存20日均线，价格基准未核验。"),
                    ("ma60", "60日均线", "已存60日均线，价格基准未核验。"),
                    ("macd", "MACD DIF", "已存MACD DIF，价格基准及初始化未核验。"),
                    ("macd_signal", "MACD信号线", "已存MACD信号线，价格基准及初始化未核验。"),
                    (
                        "macd_hist",
                        "MACD柱差值",
                        "已存DIF−DEA，不另乘2；价格基准及初始化未核验。",
                    ),
                )
            ),
            *(
                FactorDailyStoredField(
                    column=c,
                    table="daily_indicator",
                    name_zh=n,
                    unit="indicator",
                    description_zh=d,
                )
                for c, n, d in (
                    ("rsi6", "6日RSI", "已存6日RSI点值，初始化未核验。"),
                    ("rsi14", "14日RSI", "已存14日RSI点值，初始化未核验。"),
                    ("kdj_k", "KDJ K", "已存KDJ K点值，初始化未核验。"),
                    ("kdj_d", "KDJ D", "已存KDJ D点值，初始化未核验。"),
                    ("kdj_j", "KDJ J", "已存KDJ J点值，初始化未核验；保留原值，不裁到0–100。"),
                )
            ),
            FactorDailyStoredField(
                column="turnover_rate",
                table="daily_basic",
                name_zh="换手率",
                unit="percent",
                description_zh="百分数原值；0.5387表示0.5387%，不乘100。",
            ),
            FactorDailyStoredField(
                column="volume_ratio",
                table="daily_basic",
                name_zh="量比",
                unit="ratio",
                description_zh="供应方当日量比原值，不由日线重算。",
            ),
            FactorDailyStoredField(
                column="total_mv",
                table="daily_basic",
                name_zh="总市值",
                unit="CNY_10000",
                description_zh="当日总市值原值，单位万元。",
            ),
            FactorDailyStoredField(
                column="circ_mv",
                table="daily_basic",
                name_zh="流通市值",
                unit="CNY_10000",
                description_zh="当日流通市值原值，单位万元。",
            ),
        ),
        key=lambda f: f.column,
    )
)
_FIELDS = {field.column: field for field in STORED_DAILY_FIELDS}
_TABLES = ("daily_indicator", "daily_basic")


class FactorDailyFeaturePrepareRequest(BaseModel):
    model_config = _MODEL
    prepared_source: FactorPreparedStreamSource


class FactorDailyFeatureCounts(BaseModel):
    model_config = _MODEL
    column: DailyStoredColumn
    valid: int = Field(ge=0)
    missing: int = Field(ge=0)
    null: int = Field(ge=0)
    non_finite: int = Field(ge=0)


class FactorDailyFeatureTable(BaseModel):
    model_config = _MODEL
    table_name: Literal["daily_indicator", "daily_basic"]
    artifact: DatasetSnapshotArtifact
    row_count: int = Field(ge=0)
    code_counts: tuple[FactorSourceCodeCount, ...] = Field(min_length=1, max_length=7000)
    date_counts: tuple[FactorSourceDateCount, ...] = Field(min_length=1, max_length=4096)
    counts: tuple[FactorDailyFeatureCounts, ...] = Field(min_length=1, max_length=12)
    structural_missing_rows: int = Field(ge=0)
    rows_on_closed_dates: int = Field(ge=0)


class FactorDailyFeatureSources(BaseModel):
    """The actual dependency subset, omitted entirely for original six-field definitions."""

    model_config = _MODEL
    source_sha256: str = Field(pattern=_SHA)
    prepared_source_sha256: str = Field(pattern=_SHA)
    prepared_snapshot_id: str
    prepared_binding_hash: str = Field(pattern=_SHA)
    scope_content_hash: str = Field(pattern=_SHA)
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    fields: tuple[FactorDailyStoredField, ...] = Field(min_length=1, max_length=16)
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    value_semantics: Literal["stored_not_recomputed"] = "stored_not_recomputed"
    price_basis: Literal["unverified"] = "unverified"
    recursive_initialization: Literal["unverified"] = "unverified"

    @model_validator(mode="after")
    def _fields(self) -> FactorDailyFeatureSources:
        columns = tuple(field.column for field in self.fields)
        if columns != tuple(sorted(set(columns))) or any(
            _FIELDS[f.column] != f for f in self.fields
        ):
            raise ValueError("stored daily field contract differs from inventory contract")
        return self


class FactorDailyFeatureSource(BaseModel):
    model_config = _MODEL
    schema_version: Literal[1] = 1
    prepared_source_sha256: str = Field(pattern=_SHA)
    prepared_snapshot_id: str
    prepared_binding_hash: str = Field(pattern=_SHA)
    scope_content_hash: str = Field(pattern=_SHA)
    scope: FactorComputationScope
    generation: FactorSourceGeneration
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_open_days: tuple[date, ...] = Field(max_length=4096)
    fields: tuple[FactorDailyStoredField, ...] = STORED_DAILY_FIELDS
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    value_semantics: Literal["stored_not_recomputed"] = "stored_not_recomputed"
    price_basis: Literal["unverified"] = "unverified"
    recursive_initialization: Literal["unverified"] = "unverified"
    source_read_boundary: Literal["single_snapshot_transaction"] = "single_snapshot_transaction"
    read_mode: Literal["descriptor", "in_place"]
    observed_at: datetime
    completed_read_at: datetime
    tables: tuple[FactorDailyFeatureTable, ...] = Field(min_length=2, max_length=2)
    sha256: str = Field(pattern=_SHA)

    @field_validator("observed_at", "completed_read_at")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def _binding(self) -> FactorDailyFeatureSource:
        dates = _dates(self.scope)
        if (
            self.fields != STORED_DAILY_FIELDS
            or self.completed_read_at < self.observed_at
            or self.calendar_open_days != tuple(sorted(set(self.calendar_open_days)))
            or not set(self.calendar_open_days) <= set(dates)
            or tuple(t.table_name for t in self.tables) != _TABLES
        ):
            raise ValueError("daily feature source contract, clock or calendar mismatch")
        for table in self.tables:
            columns = tuple(f.column for f in self.fields if f.table == table.table_name)
            closed = sum(
                d.count for d in table.date_counts if d.date not in self.calendar_open_days
            )
            nonempty = tuple(d.date for d in table.date_counts if d.count)
            artifact = table.artifact
            if (
                tuple(c.code for c in table.code_counts) != self.scope.stock_codes
                or tuple(d.date for d in table.date_counts) != dates
                or sum(c.count for c in table.code_counts) != table.row_count
                or sum(d.count for d in table.date_counts) != table.row_count
                or any(c.count > len(dates) for c in table.code_counts)
                or any(d.count > len(self.scope.stock_codes) for d in table.date_counts)
                or tuple(c.column for c in table.counts) != columns
                or any(
                    c.valid + c.null + c.non_finite != table.row_count or c.missing != 0
                    for c in table.counts
                )
                or table.rows_on_closed_dates != closed
                or table.structural_missing_rows
                != len(self.scope.stock_codes) * len(self.calendar_open_days)
                - table.row_count
                + closed
                or artifact.artifact_type != "materialized_table"
                or artifact.dataset_id != "factor_daily_features"
                or artifact.table_name != table.table_name
                or artifact.primary_key != ("ts_code", "trade_date")
                or artifact.event_column != "trade_date"
                or artifact.row_count != table.row_count
                or artifact.earliest_time != (nonempty[0].isoformat() if nonempty else None)
                or artifact.latest_time != (nonempty[-1].isoformat() if nonempty else None)
                or artifact.file_size is None
                or artifact.relative_path
                != f"tables/{table.table_name}/versions/{artifact.file_hash}.parquet"
            ):
                raise ValueError("daily feature table scope, counts or artifact mismatch")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("daily feature source digest mismatch")
        return self

    def require_prepared(self, prepared: FactorPreparedStreamSource) -> None:
        prepared = FactorPreparedStreamSource.model_validate(prepared)
        if (
            self.prepared_source_sha256 != prepared.sha256
            or self.prepared_snapshot_id != prepared.snapshot.snapshot_id
            or self.prepared_binding_hash != prepared.binding.binding_hash
            or self.scope_content_hash != prepared.scope_content_hash
            or self.scope != prepared.receipt.request.scope
            or self.generation != prepared.receipt.generation
            or self.code_commit != prepared.snapshot.code_commit
            or self.calendar_open_days != prepared.receipt.calendar_open_days
            or self.observed_at < prepared.receipt.completed_read_at
        ):
            raise ValueError("daily features differ from paired prepared prices")

    def select(self, columns: tuple[str, ...]) -> FactorDailyFeatureSources:
        return FactorDailyFeatureSources(
            source_sha256=self.sha256,
            prepared_source_sha256=self.prepared_source_sha256,
            prepared_snapshot_id=self.prepared_snapshot_id,
            prepared_binding_hash=self.prepared_binding_hash,
            scope_content_hash=self.scope_content_hash,
            code_commit=self.code_commit,
            fields=tuple(_FIELDS[c] for c in sorted(set(columns))),
        )


class FactorDailyFeatureQuery(BaseModel):
    model_config = _MODEL
    source_sha256: str = Field(pattern=_SHA)
    trade_date: date
    stock_codes: tuple[StockCode, ...] = Field(min_length=1, max_length=500)
    fields: tuple[DailyStoredColumn, ...] = Field(min_length=1, max_length=16)

    @field_validator("stock_codes", "fields")
    @classmethod
    def _unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("daily feature query repeats codes or fields")
        return tuple(sorted(value))


class FactorDailyFeatureValue(BaseModel):
    model_config = _MODEL
    status: DailyFeatureStatus
    value: float | None = Field(allow_inf_nan=False)
    non_finite_value: Literal["NaN", "Infinity", "-Infinity"] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @model_validator(mode="after")
    def _value(self) -> FactorDailyFeatureValue:
        if (self.status == "valid") != (self.value is not None) or (
            self.status == "non_finite"
        ) != (self.non_finite_value is not None):
            raise ValueError("daily feature value and missing state differ")
        return self


class FactorDailyFeatureFact(FactorDailyFeatureValue):
    stock_code: StockCode
    trade_date: date
    column: DailyStoredColumn


class FactorDailyFeatureInputRow(BaseModel):
    model_config = _MODEL
    stock_code: StockCode
    values: tuple[FactorDailyFeatureValue, ...] = Field(min_length=1, max_length=16)


class FactorDailyFeatureInput(BaseModel):
    """One compact original day for the journal; positions follow sources.fields."""

    model_config = _MODEL
    sources: FactorDailyFeatureSources
    trade_date: date
    panel_date: date
    rows: tuple[FactorDailyFeatureInputRow, ...] = Field(min_length=1, max_length=7000)
    counts: tuple[FactorDailyFeatureCounts, ...] = Field(min_length=1, max_length=16)
    sha256: str = Field(pattern=_SHA)

    @model_validator(mode="after")
    def _grid(self) -> FactorDailyFeatureInput:
        columns = tuple(f.column for f in self.sources.fields)
        codes = tuple(r.stock_code for r in self.rows)
        if any(len(r.values) != len(columns) for r in self.rows):
            raise ValueError("daily feature original input row width differs")
        counts = Counter(
            (columns[i], v.status) for row in self.rows for i, v in enumerate(row.values)
        )
        expected = tuple(
            FactorDailyFeatureCounts(
                column=c, **{s: counts[c, s] for s in ("valid", "missing", "null", "non_finite")}
            )
            for c in columns
        )
        if (
            self.panel_date >= self.trade_date
            or codes != tuple(sorted(set(codes)))
            or expected != self.counts
            or self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"}))
        ):
            raise ValueError("daily feature original input grid or digest differs")
        return self


def read_factor_daily_feature_input(
    lease: FactorDailyFeatureReadLease,
    sources: FactorDailyFeatureSources,
    *,
    trade_date: date,
    panel_date: date,
    stock_codes: tuple[str, ...],
) -> FactorDailyFeatureInput:
    columns = tuple(field.column for field in sources.fields)
    if sources != lease.source.select(columns) or not 1 <= len(stock_codes) <= 7000:
        raise ValueError("daily feature input differs from selected sealed source")
    rows = []
    for start in range(0, len(stock_codes), 500):
        batch = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=sources.source_sha256,
                trade_date=panel_date,
                stock_codes=stock_codes[start : start + 500],
                fields=columns,
            )
        )
        for i, code in enumerate(batch.query.stock_codes):
            rows.append(
                FactorDailyFeatureInputRow(
                    stock_code=code,
                    values=tuple(
                        FactorDailyFeatureValue(
                            status=f.status, value=f.value, non_finite_value=f.non_finite_value
                        )
                        for f in batch.facts[i * len(columns) : (i + 1) * len(columns)]
                    ),
                )
            )
        del batch
    counts = Counter((columns[i], v.status) for row in rows for i, v in enumerate(row.values))
    fields = dict(
        sources=sources,
        trade_date=trade_date,
        panel_date=panel_date,
        rows=tuple(rows),
        counts=tuple(
            FactorDailyFeatureCounts(
                column=c, **{s: counts[c, s] for s in ("valid", "missing", "null", "non_finite")}
            )
            for c in columns
        ),
    )
    return FactorDailyFeatureInput(**fields, sha256=canonical_sha256(fields))


def _counts(
    facts: tuple[FactorDailyFeatureFact, ...], columns: tuple[str, ...]
) -> tuple[FactorDailyFeatureCounts, ...]:
    count = Counter((f.column, f.status) for f in facts)
    return tuple(
        FactorDailyFeatureCounts(
            column=c, **{s: count[c, s] for s in ("valid", "missing", "null", "non_finite")}
        )
        for c in columns
    )


class FactorDailyFeatureDayBatch(BaseModel):
    model_config = _MODEL
    source_sha256: str = Field(pattern=_SHA)
    prepared_source_sha256: str = Field(pattern=_SHA)
    query: FactorDailyFeatureQuery
    trade_date: date
    facts: tuple[FactorDailyFeatureFact, ...] = Field(min_length=1, max_length=8000)
    counts: tuple[FactorDailyFeatureCounts, ...] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def _grid(self) -> FactorDailyFeatureDayBatch:
        if (
            self.source_sha256 != self.query.source_sha256
            or self.trade_date != self.query.trade_date
            or tuple((f.stock_code, f.column) for f in self.facts)
            != tuple(
                (code, column) for code in self.query.stock_codes for column in self.query.fields
            )
            or any(f.trade_date != self.trade_date for f in self.facts)
            or self.counts != _counts(self.facts, self.query.fields)
        ):
            raise ValueError("daily feature batch differs from complete requested grid")
        return self


def _check_schema(columns: tuple[tuple[str, str], ...], table: str) -> None:
    types = dict(columns)
    if (
        types.get("ts_code") != "VARCHAR"
        or types.get("trade_date") != "DATE"
        or any(types.get(f.column) != "DOUBLE" for f in STORED_DAILY_FIELDS if f.table == table)
    ):
        raise ValueError("stored daily source schema differs from declared fields")


def _observe(
    connection: duckdb.DuckDBPyConnection,
    scope: FactorComputationScope,
    open_days: tuple[date, ...],
    table: str,
    artifact: DatasetSnapshotArtifact,
) -> FactorDailyFeatureTable:
    where = "trade_date BETWEEN ? AND ? AND ts_code IN (SELECT unnest(?))"
    params = [scope.start_date, scope.end_date, list(scope.stock_codes)]
    fields = tuple(f.column for f in STORED_DAILY_FIELDS if f.table == table)
    counts = connection.execute(
        "SELECT count(*), "
        + ", ".join(
            f"count(*) FILTER (WHERE isfinite({c})), count(*) FILTER (WHERE {c} IS NULL), "
            f"count(*) FILTER (WHERE {c} IS NOT NULL AND NOT isfinite({c}))"
            for c in fields
        )
        + f" FROM {table} WHERE {where}",
        params,
    ).fetchone()
    row_count = int(counts[0])
    by_code = dict(
        connection.execute(
            f"SELECT ts_code, count(*) FROM {table} WHERE {where} "
            "GROUP BY ts_code ORDER BY ts_code",
            params,
        ).fetchall()
    )
    by_date = dict(
        connection.execute(
            f"SELECT trade_date, count(*) FROM {table} WHERE {where} "
            "GROUP BY trade_date ORDER BY trade_date",
            params,
        ).fetchall()
    )
    closed = sum(count for day, count in by_date.items() if day not in open_days)
    return FactorDailyFeatureTable(
        table_name=table,
        artifact=artifact,
        row_count=row_count,
        code_counts=tuple(
            FactorSourceCodeCount(code=c, count=by_code.get(c, 0)) for c in scope.stock_codes
        ),
        date_counts=tuple(
            FactorSourceDateCount(date=d, count=by_date.get(d, 0)) for d in _dates(scope)
        ),
        counts=tuple(
            FactorDailyFeatureCounts(
                column=c,
                valid=int(counts[1 + i * 3]),
                null=int(counts[2 + i * 3]),
                non_finite=int(counts[3 + i * 3]),
                missing=0,
            )
            for i, c in enumerate(fields)
        ),
        structural_missing_rows=len(scope.stock_codes) * len(open_days) - row_count + closed,
        rows_on_closed_dates=closed,
    )


def prepare_factor_daily_feature_source(
    request: FactorDailyFeaturePrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorDailyFeatureSource:
    request = FactorDailyFeaturePrepareRequest.model_validate(request)
    prepared, original = request.prepared_source, request.prepared_source.receipt.request
    generation = _generation(original)
    if generation != prepared.receipt.generation:
        raise ValueError("daily feature generation differs from prepared prices")
    root = _root_path(lake_root)
    if root.is_relative_to(original.replica_path.parent):
        raise ValueError("daily feature lake must be outside replica directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_descriptor = _open_private_root(root)
    try:
        with TemporaryDirectory(prefix=".daily-feature-prepare-", dir=root) as scratch:
            descriptor = os.open(original.replica_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            connection, transaction = None, False
            try:
                _check_generation(original, generation, descriptor)
                connection, mode = connect_pinned_readonly(original.replica_path, descriptor)
                connection.execute("SET temp_directory = ?", [scratch])
                connection.execute("SET threads=1")
                _check_generation(original, generation, descriptor)
                connection.execute("BEGIN TRANSACTION")
                transaction = True
                observed_at = normalize_utc_datetime(now())
                if observed_at < prepared.receipt.completed_read_at:
                    raise ValueError("daily feature read precedes prepared prices")
                # Validate both schemas before publishing either table artifact.
                for table in _TABLES:
                    columns, key = _source_table_schema(connection, table)
                    _check_schema(columns, table)
                    if key != ("ts_code", "trade_date"):
                        raise ValueError("stored daily business primary key mismatch")
                tables = []
                for table in _TABLES:
                    artifact = materialize_table_dependency(
                        connection,
                        dependency=StrategyTableDependency(
                            dataset_id="factor_daily_features",
                            table_name=table,
                            date_column="trade_date",
                            code_column="ts_code",
                        ),
                        artifact_root=root,
                        start_date=original.scope.start_date,
                        end_date=original.scope.end_date,
                        as_of_time=original.scope.as_of_time,
                        ts_codes=original.scope.stock_codes,
                        source_table_name=table,
                    )
                    verify_materialized_table_artifact(
                        artifact, lake_root=root, as_of_time=original.scope.as_of_time
                    )
                    tables.append(
                        _observe(
                            connection,
                            original.scope,
                            prepared.receipt.calendar_open_days,
                            table,
                            artifact,
                        )
                    )
                _check_generation(original, generation, descriptor)
                connection.execute("COMMIT")
                transaction = False
                fields = dict(
                    prepared_source_sha256=prepared.sha256,
                    prepared_snapshot_id=prepared.snapshot.snapshot_id,
                    prepared_binding_hash=prepared.binding.binding_hash,
                    scope_content_hash=prepared.scope_content_hash,
                    scope=original.scope,
                    generation=generation,
                    code_commit=original.code_commit,
                    calendar_open_days=prepared.receipt.calendar_open_days,
                    fields=STORED_DAILY_FIELDS,
                    source_mode="historical_retrospective",
                    value_semantics="stored_not_recomputed",
                    price_basis="unverified",
                    recursive_initialization="unverified",
                    source_read_boundary="single_snapshot_transaction",
                    read_mode=mode,
                    observed_at=observed_at,
                    completed_read_at=normalize_utc_datetime(now()),
                    tables=tuple(tables),
                )
                source = FactorDailyFeatureSource(
                    **fields, sha256=canonical_sha256({"schema_version": 1, **fields})
                )
                source.require_prepared(prepared)
                _check_generation(original, generation, descriptor)
                _require_same_root(root, root_descriptor)
            finally:
                try:
                    if connection is not None:
                        if transaction:
                            with suppress(Exception):
                                connection.execute("ROLLBACK")
                        connection.close()
                finally:
                    os.close(descriptor)
    finally:
        os.close(root_descriptor)
    return source


class FactorDailyFeatureReadLease:
    def __init__(
        self,
        source: FactorDailyFeatureSource,
        connection: duckdb.DuckDBPyConnection,
        private_root: Path,
    ) -> None:
        self.source, self._connection, self._private_root = source, connection, private_root
        self._codes = frozenset(source.scope.stock_codes)
        self._closed, self.query_count = False, 0

    @property
    def closed(self) -> bool:
        return self._closed

    def query(self, query: FactorDailyFeatureQuery) -> FactorDailyFeatureDayBatch:
        if self.closed:
            raise RuntimeError("daily feature reader is closed")
        query = FactorDailyFeatureQuery.model_validate(query)
        if (
            query.source_sha256 != self.source.sha256
            or not self._codes.issuperset(query.stock_codes)
            or not self.source.scope.start_date <= query.trade_date <= self.source.scope.end_date
        ):
            raise ValueError("daily feature query exceeds source or scope")
        values = {}
        for table in _TABLES:
            fields = tuple(c for c in query.fields if _FIELDS[c].table == table)
            if not fields:
                continue
            rows = self._connection.execute(
                f"SELECT ts_code, {','.join(fields)} FROM {table} "
                "WHERE trade_date=? AND ts_code IN (SELECT unnest(?)) ORDER BY ts_code",
                [query.trade_date, list(query.stock_codes)],
            ).fetchmany(501)
            if len(rows) > len(query.stock_codes) or len({row[0] for row in rows}) != len(rows):
                raise ValueError("daily feature private query exceeds unique bounded grid")
            for row in rows:
                for column, value in zip(fields, row[1:], strict=True):
                    values[row[0], column] = value
        facts = []
        for code in query.stock_codes:
            for column in query.fields:
                value = values.get((code, column))
                tag = None
                if (code, column) not in values:
                    status = "missing"
                elif value is None:
                    status = "null"
                elif not math.isfinite(value):
                    status, tag = (
                        "non_finite",
                        "NaN" if math.isnan(value) else "Infinity" if value > 0 else "-Infinity",
                    )
                    value = None
                else:
                    status = "valid"
                facts.append(
                    FactorDailyFeatureFact(
                        stock_code=code,
                        trade_date=query.trade_date,
                        column=column,
                        status=status,
                        value=value,
                        non_finite_value=tag,
                    )
                )
        self.query_count += 1
        return FactorDailyFeatureDayBatch(
            source_sha256=self.source.sha256,
            prepared_source_sha256=self.source.prepared_source_sha256,
            query=query,
            trade_date=query.trade_date,
            facts=tuple(facts),
            counts=_counts(tuple(facts), query.fields),
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._connection.close()


@contextmanager
def open_factor_daily_feature_source(
    source: FactorDailyFeatureSource, *, lake_root: Path
) -> Iterator[FactorDailyFeatureReadLease]:
    source = FactorDailyFeatureSource.model_validate(source)
    root = _root_path(lake_root)
    descriptor = _open_private_root(root)
    connection, lease = None, None
    try:
        with TemporaryDirectory(prefix=".daily-feature-reader-", dir=root) as scratch:
            private_root = Path(scratch)
            for table in source.tables:
                original = verify_materialized_table_artifact(
                    table.artifact, lake_root=root, as_of_time=source.scope.as_of_time
                )
                target = private_root / table.artifact.relative_path
                target.parent.mkdir(parents=True)
                shutil.copyfile(original, target)
                os.chmod(target, 0o600)
                verify_materialized_table_artifact(
                    table.artifact, lake_root=private_root, as_of_time=source.scope.as_of_time
                )
            _require_same_root(root, descriptor)
            connection = duckdb.connect(":memory:")
            try:
                connection.execute("SET threads=1")
                connection.execute("SET temp_directory = ?", [scratch])
                for table in source.tables:
                    path = private_root / table.artifact.relative_path
                    connection.execute(
                        f"CREATE VIEW {table.table_name} AS SELECT * FROM read_parquet("
                        f"{_quoted_literal(str(path))}, hive_partitioning=false)"
                    )
                    columns = tuple(
                        (str(row[0]), str(row[1]))
                        for row in connection.execute(f"DESCRIBE {table.table_name}").fetchall()
                    )
                    _check_schema(columns, table.table_name)
                    invalid = connection.execute(
                        f"SELECT count(*) FROM {table.table_name} "
                        "WHERE ts_code IS NULL OR trade_date IS NULL "
                        "OR trade_date NOT BETWEEN ? AND ? OR ts_code NOT IN (SELECT unnest(?))",
                        [
                            source.scope.start_date,
                            source.scope.end_date,
                            list(source.scope.stock_codes),
                        ],
                    ).fetchone()
                    duplicates = connection.execute(
                        f"SELECT count(*) FROM (SELECT ts_code, trade_date FROM {table.table_name} "
                        "GROUP BY ts_code, trade_date HAVING count(*)>1)"
                    ).fetchone()
                    if (
                        int(invalid[0])
                        or int(duplicates[0])
                        or _observe(
                            connection,
                            source.scope,
                            source.calendar_open_days,
                            table.table_name,
                            table.artifact,
                        )
                        != table
                    ):
                        raise ValueError("private daily feature rows differ from scope or counts")
                lease = FactorDailyFeatureReadLease(source, connection, private_root)
                yield lease
                for table in source.tables:
                    verify_materialized_table_artifact(
                        table.artifact, lake_root=root, as_of_time=source.scope.as_of_time
                    )
                _require_same_root(root, descriptor)
            finally:
                if lease is not None:
                    lease.close()
                else:
                    connection.close()
    finally:
        os.close(descriptor)
