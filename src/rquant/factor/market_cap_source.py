"""Retrospective daily total market cap paired with one prepared RO generation.

Values retain Tushare's CNY 10,000 unit and panel dates. Trading-day pairing and
neutralization belong to the subsequent adapter, not this raw source.
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
_DEPENDENCY = StrategyTableDependency(
    dataset_id="factor_market_cap",
    table_name="daily_basic",
    date_column="trade_date",
    code_column="ts_code",
)
MarketCapStatus = Literal["valid", "missing", "null", "non_positive", "non_finite"]


class FactorMarketCapPrepareRequest(BaseModel):
    model_config = _MODEL

    prepared_source: FactorPreparedStreamSource


class FactorMarketCapObservation(BaseModel):
    model_config = _MODEL

    row_count: int = Field(ge=0)
    code_counts: tuple[FactorSourceCodeCount, ...] = Field(min_length=1, max_length=7000)
    date_counts: tuple[FactorSourceDateCount, ...] = Field(min_length=1, max_length=4096)
    null_rows: int = Field(ge=0)
    non_positive_rows: int = Field(ge=0)
    non_finite_rows: int = Field(ge=0)
    valid_rows: int = Field(ge=0)
    structural_missing_rows: int = Field(ge=0)
    rows_on_closed_dates: int = Field(ge=0)


class FactorMarketCapSource(BaseModel):
    model_config = _MODEL

    schema_version: Literal[1] = 1
    prepared_source_sha256: str = Field(pattern=_SHA)
    prepared_snapshot_id: str = Field(min_length=1)
    prepared_binding_hash: str = Field(pattern=_SHA)
    scope_content_hash: str = Field(pattern=_SHA)
    scope: FactorComputationScope
    generation: FactorSourceGeneration
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    calendar_open_days: tuple[date, ...] = Field(max_length=4096)
    unit: Literal["CNY_10000"] = "CNY_10000"
    value_field: Literal["total_mv"] = "total_mv"
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    source_read_boundary: Literal["single_snapshot_transaction"] = "single_snapshot_transaction"
    read_mode: Literal["descriptor", "in_place"]
    observed_at: datetime
    completed_read_at: datetime
    artifact: DatasetSnapshotArtifact
    observation: FactorMarketCapObservation
    sha256: str = Field(pattern=_SHA)

    @field_validator("observed_at", "completed_read_at")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def _binding(self) -> FactorMarketCapSource:
        if self.completed_read_at < self.observed_at:
            raise ValueError("market cap observation clock moved backwards")
        days = _dates(self.scope)
        if tuple(sorted(set(self.calendar_open_days))) != self.calendar_open_days or not set(
            self.calendar_open_days
        ) <= set(days):
            raise ValueError("market cap calendar differs from bound scope")
        observation = self.observation
        if (
            tuple(item.code for item in observation.code_counts) != self.scope.stock_codes
            or tuple(item.date for item in observation.date_counts) != days
            or sum(item.count for item in observation.code_counts) != observation.row_count
            or sum(item.count for item in observation.date_counts) != observation.row_count
            or observation.null_rows
            + observation.non_positive_rows
            + observation.non_finite_rows
            + observation.valid_rows
            != observation.row_count
            or any(item.count > len(days) for item in observation.code_counts)
            or any(item.count > len(self.scope.stock_codes) for item in observation.date_counts)
        ):
            raise ValueError("market cap observations differ from bound scope")
        closed = sum(
            item.count
            for item in observation.date_counts
            if item.date not in self.calendar_open_days
        )
        missing = (
            len(self.scope.stock_codes) * len(self.calendar_open_days)
            - observation.row_count
            + closed
        )
        nonempty = tuple(item.date for item in observation.date_counts if item.count)
        artifact = self.artifact
        if (
            observation.rows_on_closed_dates != closed
            or observation.structural_missing_rows != missing
            or artifact.artifact_type != "materialized_table"
            or artifact.dataset_id != "factor_market_cap"
            or artifact.table_name != "daily_basic"
            or artifact.primary_key != ("ts_code", "trade_date")
            or artifact.event_column != "trade_date"
            or artifact.row_count != observation.row_count
            or artifact.earliest_time != (nonempty[0].isoformat() if nonempty else None)
            or artifact.latest_time != (nonempty[-1].isoformat() if nonempty else None)
            or artifact.file_size is None
            or artifact.relative_path != f"tables/daily_basic/versions/{artifact.file_hash}.parquet"
        ):
            raise ValueError("market cap artifact or coverage binding mismatch")
        if self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"})):
            raise ValueError("market cap source receipt digest mismatch")
        return self


class FactorMarketCapQuery(BaseModel):
    model_config = _MODEL

    source_sha256: str = Field(pattern=_SHA)
    trade_date: date
    stock_codes: tuple[StockCode, ...] = Field(min_length=1, max_length=500)

    @field_validator("stock_codes")
    @classmethod
    def _codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("market cap query contains duplicate codes")
        return tuple(sorted(value))


class FactorMarketCapFact(BaseModel):
    model_config = ConfigDict(**_MODEL, ser_json_inf_nan="strings")

    stock_code: StockCode
    trade_date: date
    total_mv: float | None
    status: MarketCapStatus

    @model_validator(mode="after")
    def _value(self) -> FactorMarketCapFact:
        if self.status in ("missing", "null"):
            if self.total_mv is not None:
                raise ValueError("absent market cap has a numeric value")
        elif self.total_mv is None or self.status != _value_status(self.total_mv):
            raise ValueError("market cap value and status differ")
        return self


class FactorMarketCapCounts(BaseModel):
    model_config = _MODEL

    valid: int = Field(ge=0)
    missing: int = Field(ge=0)
    null: int = Field(ge=0)
    non_positive: int = Field(ge=0)
    non_finite: int = Field(ge=0)


class FactorMarketCapDayBatch(BaseModel):
    model_config = _MODEL

    source_sha256: str = Field(pattern=_SHA)
    prepared_source_sha256: str = Field(pattern=_SHA)
    query: FactorMarketCapQuery
    trade_date: date
    unit: Literal["CNY_10000"] = "CNY_10000"
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    facts: tuple[FactorMarketCapFact, ...] = Field(min_length=1, max_length=500)
    counts: FactorMarketCapCounts

    @model_validator(mode="after")
    def _complete(self) -> FactorMarketCapDayBatch:
        if (
            self.source_sha256 != self.query.source_sha256
            or self.trade_date != self.query.trade_date
            or tuple(fact.stock_code for fact in self.facts) != self.query.stock_codes
            or any(fact.trade_date != self.trade_date for fact in self.facts)
            or self.counts.model_dump() != _counts(self.facts).model_dump()
        ):
            raise ValueError("market cap batch does not describe every requested code")
        return self


def _value_status(value: float | None) -> MarketCapStatus:
    if value is None:
        return "null"
    if not math.isfinite(value):
        return "non_finite"
    return "valid" if value > 0 else "non_positive"


def _counts(facts: tuple[FactorMarketCapFact, ...]) -> FactorMarketCapCounts:
    counts = Counter(fact.status for fact in facts)
    return FactorMarketCapCounts(
        **{
            status: counts[status]
            for status in ("valid", "missing", "null", "non_positive", "non_finite")
        }
    )


def _lake_root(path: Path) -> tuple[Path, int]:
    root = _root_path(path)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root, _open_private_root(root)


def _check_schema(columns: tuple[tuple[str, str], ...]) -> None:
    types = dict(columns)
    if (
        types.get("ts_code") != "VARCHAR"
        or types.get("trade_date") != "DATE"
        or types.get("total_mv") != "DOUBLE"
    ):
        raise ValueError(
            "daily_basic requires VARCHAR ts_code, DATE trade_date and DOUBLE total_mv"
        )


def _observe(
    connection: duckdb.DuckDBPyConnection,
    scope: FactorComputationScope,
    open_days: tuple[date, ...],
) -> FactorMarketCapObservation:
    where = "trade_date BETWEEN ? AND ? AND ts_code IN (SELECT unnest(?))"
    parameters = [scope.start_date, scope.end_date, list(scope.stock_codes)]
    counts = connection.execute(
        "SELECT count(*), count(*) FILTER (WHERE total_mv IS NULL), "
        "count(*) FILTER (WHERE isfinite(total_mv) AND total_mv <= 0), "
        "count(*) FILTER (WHERE total_mv IS NOT NULL AND NOT isfinite(total_mv)), "
        "count(*) FILTER (WHERE isfinite(total_mv) AND total_mv > 0) "
        f"FROM daily_basic WHERE {where}",
        parameters,
    ).fetchone()
    by_code = dict(
        connection.execute(
            f"SELECT ts_code, count(*) FROM daily_basic WHERE {where} "
            "GROUP BY ts_code ORDER BY ts_code",
            parameters,
        ).fetchall()
    )
    by_date = dict(
        connection.execute(
            f"SELECT trade_date, count(*) FROM daily_basic WHERE {where} "
            "GROUP BY trade_date ORDER BY trade_date",
            parameters,
        ).fetchall()
    )
    dates = _dates(scope)
    row_count = int(counts[0])
    closed = sum(count for day, count in by_date.items() if day not in open_days)
    return FactorMarketCapObservation(
        row_count=row_count,
        code_counts=tuple(
            FactorSourceCodeCount(code=code, count=by_code.get(code, 0))
            for code in scope.stock_codes
        ),
        date_counts=tuple(
            FactorSourceDateCount(date=day, count=by_date.get(day, 0)) for day in dates
        ),
        null_rows=int(counts[1]),
        non_positive_rows=int(counts[2]),
        non_finite_rows=int(counts[3]),
        valid_rows=int(counts[4]),
        structural_missing_rows=len(scope.stock_codes) * len(open_days) - row_count + closed,
        rows_on_closed_dates=closed,
    )


def prepare_factor_market_cap_source(
    request: FactorMarketCapPrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorMarketCapSource:
    """Export actual total_mv in one transaction from the prepared price generation."""
    request = FactorMarketCapPrepareRequest.model_validate(request)
    prepared = request.prepared_source
    original = prepared.receipt.request
    generation = _generation(original)
    if generation != prepared.receipt.generation:
        raise ValueError("market cap replica generation differs from prepared prices")
    if _root_path(lake_root).is_relative_to(original.replica_path.parent):
        raise ValueError("market cap lake and scratch must be outside the replica directory")
    root, root_descriptor = _lake_root(lake_root)
    try:
        with TemporaryDirectory(prefix=".market-cap-prepare-", dir=root) as scratch:
            descriptor = os.open(original.replica_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            connection = None
            transaction = False
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
                    raise ValueError("market cap read precedes paired price preparation")
                columns, key = _source_table_schema(connection, "daily_basic")
                _check_schema(columns)
                if key != ("ts_code", "trade_date"):
                    raise ValueError("daily_basic business primary key mismatch")
                observation = _observe(
                    connection, original.scope, prepared.receipt.calendar_open_days
                )
                artifact = materialize_table_dependency(
                    connection,
                    dependency=_DEPENDENCY,
                    artifact_root=root,
                    start_date=original.scope.start_date,
                    end_date=original.scope.end_date,
                    as_of_time=original.scope.as_of_time,
                    ts_codes=original.scope.stock_codes,
                    source_table_name="daily_basic",
                )
                verify_materialized_table_artifact(
                    artifact, lake_root=root, as_of_time=original.scope.as_of_time
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
                    unit="CNY_10000",
                    value_field="total_mv",
                    source_mode="historical_retrospective",
                    source_read_boundary="single_snapshot_transaction",
                    read_mode=mode,
                    observed_at=observed_at,
                    completed_read_at=normalize_utc_datetime(now()),
                    artifact=artifact,
                    observation=observation,
                )
                source = FactorMarketCapSource(
                    **fields, sha256=canonical_sha256({"schema_version": 1, **fields})
                )
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


class FactorMarketCapReadLease:
    def __init__(
        self,
        source: FactorMarketCapSource,
        connection: duckdb.DuckDBPyConnection,
        private_root: Path,
    ) -> None:
        self._source = source
        self._connection = connection
        self._private_root = private_root
        self._codes = frozenset(source.scope.stock_codes)
        self._closed = False

    @property
    def source(self) -> FactorMarketCapSource:
        return self._source

    def query(self, query: FactorMarketCapQuery) -> FactorMarketCapDayBatch:
        if self._closed:
            raise RuntimeError("market cap reader is closed")
        checked = FactorMarketCapQuery.model_validate(query)
        if (
            checked.source_sha256 != self.source.sha256
            or not self.source.scope.start_date <= checked.trade_date <= self.source.scope.end_date
            or not self._codes.issuperset(checked.stock_codes)
        ):
            raise ValueError("market cap query exceeds the bound source or scope")
        rows = self._connection.execute(
            "SELECT ts_code, total_mv FROM daily_basic WHERE trade_date=? "
            "AND ts_code IN (SELECT unnest(?)) ORDER BY ts_code",
            [checked.trade_date, list(checked.stock_codes)],
        ).fetchmany(501)
        if len(rows) > len(checked.stock_codes):
            raise ValueError("market cap private daily read exceeds query bound")
        values = dict(rows)
        facts = tuple(
            FactorMarketCapFact(
                stock_code=code,
                trade_date=checked.trade_date,
                total_mv=values.get(code),
                status="missing" if code not in values else _value_status(values[code]),
            )
            for code in checked.stock_codes
        )
        return FactorMarketCapDayBatch(
            source_sha256=self.source.sha256,
            prepared_source_sha256=self.source.prepared_source_sha256,
            query=checked,
            trade_date=checked.trade_date,
            facts=facts,
            counts=_counts(facts),
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._connection.close()


@contextmanager
def open_factor_market_cap_source(
    source: FactorMarketCapSource, *, lake_root: Path
) -> Iterator[FactorMarketCapReadLease]:
    """Only verified private Parquet copies serve this lease; never reopen the RO DB."""
    source = FactorMarketCapSource.model_validate(source)
    root = _root_path(lake_root)
    root_descriptor = _open_private_root(root)
    connection = None
    lease = None
    try:
        with TemporaryDirectory(prefix=".market-cap-reader-", dir=root) as scratch:
            private_root = Path(scratch)
            original = verify_materialized_table_artifact(
                source.artifact, lake_root=root, as_of_time=source.scope.as_of_time
            )
            private_path = private_root / source.artifact.relative_path
            private_path.parent.mkdir(parents=True)
            shutil.copyfile(original, private_path)
            os.chmod(private_path, 0o600)
            verify_materialized_table_artifact(
                source.artifact, lake_root=private_root, as_of_time=source.scope.as_of_time
            )
            _require_same_root(root, root_descriptor)
            connection = duckdb.connect(":memory:")
            try:
                connection.execute("SET threads=1")
                connection.execute("SET temp_directory = ?", [scratch])
                connection.execute(
                    "CREATE VIEW daily_basic AS SELECT * FROM read_parquet("
                    f"{_quoted_literal(str(private_path))}, hive_partitioning=false)"
                )
                columns = tuple(
                    (str(row[0]), str(row[1]))
                    for row in connection.execute("DESCRIBE daily_basic").fetchall()
                )
                _check_schema(columns)
                invalid = connection.execute(
                    "SELECT count(*) FROM daily_basic WHERE ts_code IS NULL OR trade_date IS NULL "
                    "OR trade_date NOT BETWEEN ? AND ? OR ts_code NOT IN (SELECT unnest(?))",
                    [
                        source.scope.start_date,
                        source.scope.end_date,
                        list(source.scope.stock_codes),
                    ],
                ).fetchone()
                if (
                    int(invalid[0])
                    or _observe(connection, source.scope, source.calendar_open_days)
                    != source.observation
                ):
                    raise ValueError(
                        "market cap private rows differ from bound scope or observations"
                    )
                lease = FactorMarketCapReadLease(source, connection, private_root)
                yield lease
            finally:
                if lease is not None:
                    lease.close()
                else:
                    connection.close()
    finally:
        os.close(root_descriptor)
