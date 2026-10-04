"""Original market percentages from the same pinned retrospective price replica."""

from __future__ import annotations

import os
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshotArtifact, normalize_utc_datetime, utc_now
from rquant.factor.result_artifact import _open_private_root, _require_same_root, _root_path
from rquant.factor.source_prepare import (
    FactorSourceDateCount,
    _check_generation,
    _dates,
    _generation,
)
from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.research_snapshot import FactorComputationScope, materialize_table_dependency
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_dependencies import StrategyTableDependency

if TYPE_CHECKING:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureSource,
        FactorMarketTemperaturePrepareRequest,
    )

_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
MarketTemperatureColumn = Literal["market_high_60d_ratio_pct", "market_above_ma20_ratio_pct"]
MarketTemperatureReason = Literal[
    "missing_market_temperature",
    "market_temperature_null",
    "market_temperature_non_finite",
    "invalid_market_percentage",
]
MARKET_TEMPERATURE_COLUMNS = ("market_above_ma20_ratio_pct", "market_high_60d_ratio_pct")
MARKET_TEMPERATURE_RAW_COLUMNS = ("high_60d_ratio_pct", "above_ma20_ratio_pct")
MARKET_TEMPERATURE_DESCRIPTIONS = (
    (
        "market_above_ma20_ratio_pct",
        "20日均线上方占比",
        "全市场20日均线上方股票占比，百分比原值；前一完整SSE交易日，历史回顾。",
    ),
    (
        "market_high_60d_ratio_pct",
        "60日新高占比",
        "全市场60日新高股票占比，百分比原值；前一完整SSE交易日，历史回顾。",
    ),
)


class FactorMarketTemperaturePolicy(BaseModel):
    model_config = _MODEL
    version: Literal["market-temperature-original-daily-v1"] = (
        "market-temperature-original-daily-v1"
    )
    source_table: Literal["market_sentiment_daily"] = "market_sentiment_daily"
    unit: Literal["percent"] = "percent"
    universe: Literal["original_market_no_pool_recalculation"] = (
        "original_market_no_pool_recalculation"
    )
    evaluation_clock: Literal["previous_complete_sse_session_at_next_day_09:25"] = (
        "previous_complete_sse_session_at_next_day_09:25"
    )
    history_mode: Literal["retrospective_no_row_first_observed_time"] = (
        "retrospective_no_row_first_observed_time"
    )
    missing_policy: Literal["explicit_reason_no_zero_or_neighbor_fallback"] = (
        "explicit_reason_no_zero_or_neighbor_fallback"
    )
    minimum_percentage: Literal[0] = 0
    maximum_percentage: Literal[100] = 100


class FactorMarketTemperatureSummary(BaseModel):
    model_config = _MODEL
    policy: FactorMarketTemperaturePolicy
    row_count: int = Field(ge=0, le=4096)
    input_content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FactorMarketTemperatureReceipt(BaseModel):
    model_config = _MODEL
    policy: FactorMarketTemperaturePolicy = FactorMarketTemperaturePolicy()
    artifact: DatasetSnapshotArtifact
    row_count: int = Field(ge=0, le=4096)
    date_counts: tuple[FactorSourceDateCount, ...] = Field(min_length=1, max_length=4096)

    @model_validator(mode="after")
    def _artifact(self) -> FactorMarketTemperatureReceipt:
        artifact = self.artifact
        nonempty = tuple(d.date for d in self.date_counts if d.count)
        if (
            tuple(d.date for d in self.date_counts)
            != tuple(sorted({d.date for d in self.date_counts}))
            or any(d.count > 1 for d in self.date_counts)
            or sum(d.count for d in self.date_counts) != self.row_count
            or artifact.artifact_type != "materialized_table"
            or artifact.dataset_id != "factor_market_temperature"
            or artifact.table_name != "market_temperature_daily"
            or artifact.primary_key != ("trade_date",)
            or artifact.event_column != "trade_date"
            or artifact.row_count != self.row_count
            or artifact.earliest_time != (nonempty[0].isoformat() if nonempty else None)
            or artifact.latest_time != (nonempty[-1].isoformat() if nonempty else None)
            or artifact.file_size is None
            or artifact.relative_path
            != f"tables/market_temperature_daily/versions/{artifact.file_hash}.parquet"
        ):
            raise ValueError("market temperature receipt differs from original daily artifact")
        return self

    def summary(self) -> FactorMarketTemperatureSummary:
        return FactorMarketTemperatureSummary(
            policy=self.policy,
            row_count=self.row_count,
            input_content_sha256=self.artifact.content_hash,
        )

    def causal_policy(self) -> FactorMarketTemperaturePolicy:
        return self.policy


def _observe_market_temperature(
    connection: duckdb.DuckDBPyConnection,
    scope: FactorComputationScope,
    artifact: DatasetSnapshotArtifact,
) -> FactorMarketTemperatureReceipt:
    counts = dict(
        connection.execute(
            "SELECT trade_date,count(*) FROM market_temperature_daily "
            "GROUP BY trade_date ORDER BY trade_date"
        ).fetchall()
    )
    if not set(counts) <= set(_dates(scope)) or any(count != 1 for count in counts.values()):
        raise ValueError("market temperature artifact exceeds unique daily scope")
    return FactorMarketTemperatureReceipt(
        artifact=artifact,
        row_count=sum(counts.values()),
        date_counts=tuple(
            FactorSourceDateCount(date=d, count=counts.get(d, 0)) for d in _dates(scope)
        ),
    )


def prepare_factor_market_temperature_source(
    request: FactorMarketTemperaturePrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorDailyFeatureSource:
    from rquant.factor.daily_feature_source import (
        MARKET_TEMPERATURE_FIELDS,
        FactorDailyFeatureSource,
        FactorMarketTemperaturePrepareRequest,
    )

    request = FactorMarketTemperaturePrepareRequest.model_validate(request)
    prepared = request.prepared_source
    original, base = prepared.receipt.request, request.base_daily_source
    if base is not None:
        base.require_prepared(prepared)
        if base.schema_version == 5:
            raise ValueError("market temperature source cannot extend another temperature source")
    generation = _generation(original)
    if generation != prepared.receipt.generation:
        raise ValueError("market temperature generation differs from prepared prices")
    root = _root_path(lake_root)
    if root.is_relative_to(original.replica_path.parent):
        raise ValueError("market temperature lake must be outside replica directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_fd = _open_private_root(root)
    try:
        with TemporaryDirectory(prefix=".market-temperature-prepare-", dir=root) as scratch:
            fd = os.open(original.replica_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            connection, transaction = None, False
            try:
                _check_generation(original, generation, fd)
                connection, mode = connect_pinned_readonly(original.replica_path, fd)
                connection.execute("SET threads=1")
                connection.execute("SET memory_limit='512MB'")
                connection.execute("SET temp_directory = ?", [scratch])
                connection.execute("BEGIN TRANSACTION")
                transaction = True
                observed = normalize_utc_datetime(now())
                columns = dict(
                    (str(r[0]), str(r[1]))
                    for r in connection.execute("DESCRIBE market_sentiment_daily").fetchall()
                )
                if columns.get("trade_date") != "DATE" or any(
                    columns.get(c) != "DOUBLE" for c in MARKET_TEMPERATURE_RAW_COLUMNS
                ):
                    raise ValueError(
                        "market temperature source schema differs from original daily fields"
                    )
                params = [
                    prepared.receipt.request.scope.start_date,
                    prepared.receipt.request.scope.end_date,
                ]
                duplicate = connection.execute(
                    "SELECT count(*) FROM (SELECT trade_date FROM market_sentiment_daily "
                    "WHERE trade_date BETWEEN ? AND ? GROUP BY trade_date HAVING count(*)>1)",
                    params,
                ).fetchone()[0]
                if duplicate:
                    raise ValueError("duplicate market temperature source date")
                if connection.execute(
                    "SELECT count(*) FROM market_sentiment_daily WHERE trade_date IS NULL"
                ).fetchone()[0]:
                    raise ValueError("market temperature source has a null date")
                connection.execute(
                    "CREATE TEMP TABLE market_temperature_daily(trade_date DATE PRIMARY KEY,"
                    "high_60d_ratio_pct DOUBLE,above_ma20_ratio_pct DOUBLE)"
                )
                connection.execute(
                    "INSERT INTO market_temperature_daily SELECT trade_date,high_60d_ratio_pct,"
                    "above_ma20_ratio_pct FROM market_sentiment_daily "
                    "WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
                    params,
                )
                artifact = materialize_table_dependency(
                    connection,
                    dependency=StrategyTableDependency(
                        dataset_id="factor_market_temperature",
                        table_name="market_temperature_daily",
                        date_column="trade_date",
                    ),
                    artifact_root=root,
                    start_date=original.scope.start_date,
                    end_date=original.scope.end_date,
                    as_of_time=original.scope.as_of_time,
                )
                receipt = _observe_market_temperature(connection, original.scope, artifact)
                fields = dict(
                    schema_version=5,
                    prepared_source_sha256=prepared.sha256,
                    prepared_snapshot_id=prepared.snapshot.snapshot_id,
                    prepared_binding_hash=prepared.binding.binding_hash,
                    scope_content_hash=prepared.scope_content_hash,
                    scope=original.scope,
                    generation=generation,
                    code_commit=original.code_commit,
                    calendar_open_days=prepared.receipt.calendar_open_days,
                    fields=tuple(
                        sorted(
                            (() if base is None else base.fields) + MARKET_TEMPERATURE_FIELDS,
                            key=lambda f: f.column,
                        )
                    ),
                    value_semantics="market_temperature_stored",
                    price_basis="field_specific",
                    recursive_initialization="field_specific",
                    source_mode="historical_retrospective",
                    source_read_boundary="single_snapshot_transaction",
                    market_temperature=receipt,
                    read_mode=mode,
                    observed_at=observed,
                    completed_read_at=normalize_utc_datetime(now()),
                    tables=() if base is None else base.tables,
                )
                if base is not None:
                    fields["base_daily_source"] = base
                    for key in ("technical_history", "stock_features", "minute_features"):
                        if getattr(base, key) is not None:
                            fields[key] = getattr(base, key)
                source = FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))
                source.require_prepared(prepared)
                _check_generation(original, generation, fd)
                _require_same_root(root, root_fd)
                connection.execute("COMMIT")
                transaction = False
                _check_generation(original, generation, fd)
                return source
            finally:
                if connection is not None:
                    if transaction:
                        with suppress(Exception):
                            connection.execute("ROLLBACK")
                    connection.close()
                os.close(fd)
    finally:
        os.close(root_fd)


def __getattr__(name: str) -> object:
    if name in ("MARKET_TEMPERATURE_FIELDS", "FactorMarketTemperaturePrepareRequest"):
        from rquant.factor import daily_feature_source

        return getattr(daily_feature_source, name)
    raise AttributeError(name)
