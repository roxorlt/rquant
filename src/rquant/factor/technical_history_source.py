"""Versioned technical values from the first valid observed price/factor pair."""

from __future__ import annotations

import hashlib
import importlib.metadata
import math
import os
import stat
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import TYPE_CHECKING, Literal

import duckdb
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshotArtifact, normalize_utc_datetime, utc_now
from rquant.factor.result_artifact import (
    _file_identity,
    _open_private_root,
    _require_same_root,
    _root_path,
)
from rquant.factor.source_prepare import FactorPreparedStreamSource, _check_generation, _generation
from rquant.factor.universe import StockCode
from rquant.indicator import technical
from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.research_snapshot import (
    _source_table_schema,
    materialize_table_dependency,
    verify_materialized_table_artifact,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_dependencies import StrategyTableDependency

if TYPE_CHECKING:
    from rquant.factor.daily_feature_source import FactorDailyFeatureSource

_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
_SHA = r"^[0-9a-f]{64}$"
TECHNICAL_COLUMNS = tuple(
    sorted(
        (
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
        )
    )
)
_InputIdentity = tuple[int, int, int, int, int, int, int, int]
_TECHNICAL_INPUT_VALIDATION_LIMIT = 32
_TECHNICAL_INPUT_VALIDATIONS: OrderedDict[tuple[str, datetime], None] = OrderedDict()
_TECHNICAL_INPUT_VALIDATION_LOCK = Lock()


def _verify_technical_history_input(
    artifact: DatasetSnapshotArtifact,
    *,
    lake_root: Path,
    as_of_time: datetime,
    expected_identity: _InputIdentity | None = None,
) -> tuple[Path, _InputIdentity]:
    """Reuse only full validation of the exact metadata and cryptographic bytes."""
    artifact = DatasetSnapshotArtifact.model_validate(artifact)
    as_of_time = normalize_utc_datetime(as_of_time)
    relative = Path("tables/technical_history_input/versions") / f"{artifact.file_hash}.parquet"
    if (
        artifact.artifact_type != "materialized_table"
        or artifact.dataset_id != "factor_technical_history_inputs"
        or artifact.table_name != "technical_history_input"
        or Path(artifact.relative_path) != relative
    ):
        raise ValueError("technical input artifact is not the declared content-addressed table")
    root = _root_path(lake_root)
    root_descriptor = _open_private_root(root)
    file_descriptor = None
    try:
        path = root / relative
        if path.resolve() != path:
            raise ValueError("technical input path changed or traverses a symbolic link")
        file_descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
        observed = os.fstat(file_descriptor)
        identity = _file_identity(observed)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or _file_identity(path.stat(follow_symlinks=False)) != identity
            or (expected_identity is not None and identity != expected_identity)
        ):
            raise ValueError("technical input file changed during access")
        if artifact.file_size is not None and observed.st_size != artifact.file_size:
            raise ValueError("technical input file size mismatch")
        digest = hashlib.sha256()
        while chunk := os.read(file_descriptor, 1024 * 1024):
            digest.update(chunk)
        if digest.hexdigest() != artifact.file_hash:
            raise ValueError("technical input file hash mismatch")
        # The key covers every declared field and the time at which it was verified.
        key = canonical_sha256(artifact), as_of_time
        with _TECHNICAL_INPUT_VALIDATION_LOCK:
            cached = key in _TECHNICAL_INPUT_VALIDATIONS
            if cached:
                _TECHNICAL_INPUT_VALIDATIONS.move_to_end(key)
        if not cached:
            verify_materialized_table_artifact(artifact, lake_root=root, as_of_time=as_of_time)
        if (
            _file_identity(os.fstat(file_descriptor)) != identity
            or _file_identity(path.stat(follow_symlinks=False)) != identity
        ):
            raise ValueError("technical input file changed during validation")
        _require_same_root(root, root_descriptor)
        if not cached:
            with _TECHNICAL_INPUT_VALIDATION_LOCK:
                _TECHNICAL_INPUT_VALIDATIONS[key] = None
                _TECHNICAL_INPUT_VALIDATIONS.move_to_end(key)
                while len(_TECHNICAL_INPUT_VALIDATIONS) > _TECHNICAL_INPUT_VALIDATION_LIMIT:
                    _TECHNICAL_INPUT_VALIDATIONS.popitem(last=False)
        return path, identity
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(root_descriptor)


TechnicalHistoryReason = Literal[
    "insufficient_window",
    "no_initialization",
    "history_break",
    "missing_observation",
    "derived_non_finite",
]
_WINDOWS = dict(
    ma5=5,
    ma10=10,
    ma20=20,
    ma60=60,
    rsi6=6,
    rsi14=14,
    macd=26,
    macd_signal=34,
    macd_hist=34,
    kdj_k=1,
    kdj_d=1,
    kdj_j=1,
)


class FactorTechnicalHistoryPolicy(BaseModel):
    model_config = _MODEL
    algorithm_version: Literal["rquant-ta-0.11.0-v1"] = "rquant-ta-0.11.0-v1"
    implementation_sha256: str = Field(pattern=_SHA)
    price_basis: Literal["observation_factor_then_output_session_scale"] = (
        "observation_factor_then_output_session_scale"
    )
    initialization: Literal["first_valid_observation_no_restart"] = (
        "first_valid_observation_no_restart"
    )
    bad_observation: Literal["blocks_reliable_suffix"] = "blocks_reliable_suffix"
    absent_bar: Literal["no_synthetic_observation"] = "no_synthetic_observation"


class FactorTechnicalHistoryCode(BaseModel):
    model_config = _MODEL
    stock_code: StockCode
    input_observations: int = Field(ge=0, le=50_000)
    raw_start_date: date | None
    first_valid_date: date | None
    reliable_end_date: date | None
    leading_invalid_observations: int = Field(ge=0, le=50_000)
    break_date: date | None
    break_reason: Literal["invalid_ohlc", "invalid_factor", "non_finite_adjusted_price"] | None

    @model_validator(mode="after")
    def _bound(self) -> FactorTechnicalHistoryCode:
        if (
            (self.input_observations == 0) != (self.raw_start_date is None)
            or self.leading_invalid_observations > self.input_observations
            or (self.first_valid_date is None) != (self.reliable_end_date is None)
            or (self.break_date is None) != (self.break_reason is None)
            or (self.first_valid_date is None and self.break_date is not None)
            or (
                self.first_valid_date is not None
                and (
                    self.raw_start_date is None
                    or not self.raw_start_date <= self.first_valid_date <= self.reliable_end_date
                )
            )
            or (self.break_date is not None and self.break_date <= self.reliable_end_date)
        ):
            raise ValueError("technical initialization boundaries differ")
        return self


class FactorTechnicalHistorySummary(BaseModel):
    model_config = _MODEL
    policy: FactorTechnicalHistoryPolicy
    source_history_start: date | None
    initialized_codes: int = Field(ge=0, le=7000)
    uninitialized_codes: int = Field(ge=0, le=7000)
    broken_codes: int = Field(ge=0, le=7000)
    leading_invalid_observations: int = Field(ge=0, le=16_000_000)


class FactorTechnicalHistoryReceipt(BaseModel):
    model_config = _MODEL
    policy: FactorTechnicalHistoryPolicy
    inputs: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1, max_length=1)
    codes: tuple[FactorTechnicalHistoryCode, ...] = Field(min_length=1, max_length=7000)
    input_rows: int = Field(ge=0, le=16_000_000)
    max_input_rows: int = Field(gt=0, le=16_000_000)
    max_code_observations: int = Field(gt=0, le=50_000)
    max_output_cells: int = Field(gt=0, le=64_000_000)

    @model_validator(mode="after")
    def _inputs(self) -> FactorTechnicalHistoryReceipt:
        (artifact,) = self.inputs
        if (
            tuple(code.stock_code for code in self.codes)
            != tuple(sorted(set(code.stock_code for code in self.codes)))
            or sum(code.input_observations for code in self.codes) != self.input_rows
            or self.input_rows > self.max_input_rows
            or any(code.input_observations > self.max_code_observations for code in self.codes)
            or artifact.dataset_id != "factor_technical_history_inputs"
            or artifact.table_name != "technical_history_input"
            or artifact.row_count != self.input_rows
            or artifact.primary_key != ("ts_code", "trade_date")
            or artifact.event_column != "trade_date"
            or artifact.artifact_type != "materialized_table"
        ):
            raise ValueError("technical input receipt differs from complete code scope")
        return self

    def summary(self) -> FactorTechnicalHistorySummary:
        dates = [code.raw_start_date for code in self.codes if code.raw_start_date is not None]
        initialized = sum(code.first_valid_date is not None for code in self.codes)
        return FactorTechnicalHistorySummary(
            policy=self.policy,
            source_history_start=min(dates) if dates else None,
            initialized_codes=initialized,
            uninitialized_codes=len(self.codes) - initialized,
            broken_codes=sum(code.break_date is not None for code in self.codes),
            leading_invalid_observations=sum(
                code.leading_invalid_observations for code in self.codes
            ),
        )

    def causal_policy(self, panel_date: date) -> tuple[object, ...]:
        # A future seed or break is provenance for the package, not an earlier panel's input.
        return (
            self.policy,
            tuple(
                (
                    code.stock_code,
                    code.raw_start_date
                    if code.raw_start_date is not None and code.raw_start_date <= panel_date
                    else None,
                    code.first_valid_date
                    if code.first_valid_date is not None and code.first_valid_date <= panel_date
                    else None,
                    code.break_date
                    if code.break_date is not None and code.break_date <= panel_date
                    else None,
                    code.break_reason
                    if code.break_date is not None and code.break_date <= panel_date
                    else None,
                )
                for code in self.codes
            ),
        )

    def require_sealed_initialization(self, connection: duckdb.DuckDBPyConnection) -> None:
        for start in range(0, len(self.codes), 500):
            batch = self.codes[start : start + 500]
            facts = connection.execute(
                "WITH observations AS (SELECT *, CASE "
                "WHEN high IS NULL OR low IS NULL OR close IS NULL "
                "OR NOT isfinite(high) OR NOT isfinite(low) OR NOT isfinite(close) "
                "OR high<=0 OR low<=0 OR close<=0 OR low>close OR close>high THEN 'invalid_ohlc' "
                "WHEN adj_factor IS NULL OR NOT isfinite(adj_factor) OR adj_factor<=0 "
                "OR factor_present IS NOT TRUE THEN 'invalid_factor' "
                "WHEN NOT isfinite(high*adj_factor) OR NOT isfinite(low*adj_factor) "
                "OR NOT isfinite(close*adj_factor) THEN 'non_finite_adjusted_price' END AS fault "
                "FROM technical_history_input WHERE ts_code IN (SELECT unnest(?))), "
                "seeds AS (SELECT ts_code,count(*) AS n,min(trade_date) AS raw_start, "
                "min(trade_date) FILTER(WHERE fault IS NULL) AS seed "
                "FROM observations GROUP BY ts_code), "
                "breaks AS (SELECT s.ts_code,min(o.trade_date) FILTER(WHERE o.fault IS NOT NULL "
                "AND o.trade_date>s.seed) AS broken FROM seeds s "
                "JOIN observations o USING(ts_code) GROUP BY s.ts_code) "
                "SELECT s.ts_code,s.n,s.raw_start,s.seed, "
                "max(o.trade_date) FILTER(WHERE o.trade_date>=s.seed "
                "AND (b.broken IS NULL OR o.trade_date<b.broken)), "
                "count(*) FILTER(WHERE s.seed IS NULL OR o.trade_date<s.seed), b.broken, "
                "any_value(o.fault) FILTER(WHERE o.trade_date=b.broken) "
                "FROM seeds s JOIN breaks b USING(ts_code) JOIN observations o USING(ts_code) "
                "GROUP BY s.ts_code,s.n,s.raw_start,s.seed,b.broken ORDER BY s.ts_code",
                [[code.stock_code for code in batch]],
            ).fetchmany(501)
            expected = [
                (
                    code.stock_code,
                    code.input_observations,
                    code.raw_start_date,
                    code.first_valid_date,
                    code.reliable_end_date,
                    code.leading_invalid_observations,
                    code.break_date,
                    code.break_reason,
                )
                for code in batch
                if code.input_observations
            ]
            if facts != expected:
                raise ValueError("technical initialization differs from sealed observations")


class FactorTechnicalHistoryPrepareRequest(BaseModel):
    model_config = _MODEL
    prepared_source: FactorPreparedStreamSource
    max_input_rows: int = Field(default=16_000_000, gt=0, le=16_000_000)
    max_code_observations: int = Field(default=50_000, gt=0, le=50_000)
    max_output_cells: int = Field(default=32_000_000, gt=0, le=64_000_000)


def _valid_observation(row: tuple[object, ...]) -> str | None:
    high, low, close, factor = row[2:6]
    if (
        any(value is None or not math.isfinite(value) or value <= 0 for value in (high, low, close))
        or not low <= close <= high
    ):
        return "invalid_ohlc"
    if factor is None or not math.isfinite(factor) or factor <= 0:
        return "invalid_factor"
    if any(not math.isfinite(value * factor) for value in (high, low, close)):
        return "non_finite_adjusted_price"
    return None


def _derive_code(
    code: str, rows: list[tuple[object, ...]], start: date
) -> tuple[pd.DataFrame, FactorTechnicalHistoryCode]:
    seed = next((i for i, row in enumerate(rows) if _valid_observation(row) is None), None)
    broken = (
        None
        if seed is None
        else next(
            (i for i in range(seed + 1, len(rows)) if _valid_observation(rows[i]) is not None), None
        )
    )
    stop = len(rows) if broken is None else broken
    calculated = None
    if seed is not None:
        valid = rows[seed:stop]
        frame = pd.DataFrame(
            {
                "ts_code": [code] * len(valid),
                "trade_date": [r[1] for r in valid],
                "qfq_high": [r[2] * r[5] for r in valid],
                "qfq_low": [r[3] * r[5] for r in valid],
                "qfq_close": [r[4] * r[5] for r in valid],
            }
        )
        calculated = technical.compute_indicators(frame)
        del frame
    result = []
    for index, row in enumerate(rows):
        if row[1] < start:
            continue
        values = {"ts_code": code, "trade_date": row[1]}
        calculated_row = (
            None if seed is None or not seed <= index < stop else calculated.iloc[index - seed]
        )
        for column in TECHNICAL_COLUMNS:
            value, reason = None, None
            if seed is None or index < seed:
                reason = "no_initialization"
            elif index >= stop:
                reason = "history_break"
            elif index - seed + 1 < _WINDOWS[column]:
                reason = "insufficient_window"
            else:
                value = float(calculated_row[column])
                if column in ("ma5", "ma10", "ma20", "ma60", "macd", "macd_signal", "macd_hist"):
                    value /= row[5]
                if not math.isfinite(value):
                    reason = "derived_non_finite"
            values[column], values[column + "__reason"] = value, reason
        result.append(values)
    schema = [
        "ts_code",
        "trade_date",
        *TECHNICAL_COLUMNS,
        *(c + "__reason" for c in TECHNICAL_COLUMNS),
    ]
    output = pd.DataFrame(result, columns=schema)
    receipt = FactorTechnicalHistoryCode(
        stock_code=code,
        input_observations=len(rows),
        raw_start_date=rows[0][1] if rows else None,
        first_valid_date=rows[seed][1] if seed is not None else None,
        reliable_end_date=rows[stop - 1][1] if seed is not None else None,
        leading_invalid_observations=len(rows) if seed is None else seed,
        break_date=None if broken is None else rows[broken][1],
        break_reason=None if broken is None else _valid_observation(rows[broken]),
    )
    return output, receipt


def _code_rows(
    connection: duckdb.DuckDBPyConnection, codes: tuple[str, ...], end: date
) -> Iterator[tuple[str, list[tuple[object, ...]]]]:
    for code in codes:
        rows = connection.execute(
            "SELECT ts_code,trade_date,high,low,close,adj_factor,factor_present "
            "FROM technical_history_input WHERE ts_code=? AND trade_date<=? ORDER BY trade_date",
            [code, end],
        ).fetchmany(50_001)
        if rows:
            yield code, rows


def prepare_factor_technical_history_source(
    request: FactorTechnicalHistoryPrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorDailyFeatureSource:
    from rquant.factor.daily_feature_source import (
        DERIVED_DAILY_FIELDS,
        FactorDailyFeatureSource,
        _check_schema,
        _observe,
    )

    request = FactorTechnicalHistoryPrepareRequest.model_validate(request)
    if importlib.metadata.version("ta") != "0.11.0":
        raise ValueError("technical algorithm version requires ta0.11.0")
    prepared, original = request.prepared_source, request.prepared_source.receipt.request
    scope = original.scope
    if (
        len(scope.stock_codes) * ((scope.end_date - scope.start_date).days + 1) * 16
        > request.max_output_cells
    ):
        raise ValueError("technical output cell budget exceeded")
    generation = _generation(original)
    if generation != prepared.receipt.generation:
        raise ValueError("technical generation differs from paired prices")
    root = _root_path(lake_root)
    if root.is_relative_to(original.replica_path.parent):
        raise ValueError("technical lake must be outside replica directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_fd = _open_private_root(root)
    try:
        with TemporaryDirectory(prefix=".technical-history-prepare-", dir=root) as scratch:
            descriptor = os.open(original.replica_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            connection, transaction = None, False
            try:
                _check_generation(original, generation, descriptor)
                connection, mode = connect_pinned_readonly(original.replica_path, descriptor)
                connection.execute("SET threads=1")
                connection.execute("SET temp_directory=?", [scratch])
                connection.execute("BEGIN TRANSACTION")
                transaction = True
                observed = normalize_utc_datetime(now())
                if observed < prepared.receipt.completed_read_at:
                    raise ValueError("technical read precedes paired prices")
                for table, columns in (
                    ("daily_bar", ("high", "low", "close")),
                    ("adj_factor", ("adj_factor",)),
                    ("daily_basic", ()),
                ):
                    schema, key = _source_table_schema(connection, table)
                    types = dict(schema)
                    if (
                        key != ("ts_code", "trade_date")
                        or types.get("ts_code") != "VARCHAR"
                        or types.get("trade_date") != "DATE"
                        or any(types.get(c) != "DOUBLE" for c in columns)
                    ):
                        raise ValueError("technical source schema/business key differs")
                    if table == "daily_basic":
                        _check_schema(schema, table)
                totals, input_rows = {}, 0
                for start in range(0, len(scope.stock_codes), 500):
                    batch = scope.stock_codes[start : start + 500]
                    counts = connection.execute(
                        "SELECT ts_code,count(*) FROM daily_bar "
                        "WHERE ts_code IN (SELECT unnest(?)) AND trade_date<=? GROUP BY ts_code",
                        [list(batch), scope.end_date],
                    ).fetchall()
                    totals.update((code, int(count)) for code, count in counts)
                    input_rows += sum(int(count) for _, count in counts)
                    if input_rows > request.max_input_rows or any(
                        int(count) > request.max_code_observations for _, count in counts
                    ):
                        raise ValueError("technical historical input budget exceeded")
                connection.execute(
                    "CREATE TEMP TABLE technical_history_input(ts_code VARCHAR,trade_date DATE,"
                    "high DOUBLE,low DOUBLE,close DOUBLE,adj_factor DOUBLE,factor_present BOOLEAN,"
                    "PRIMARY KEY(ts_code,trade_date))"
                )
                definitions = (
                    ",".join(c + " DOUBLE" for c in TECHNICAL_COLUMNS)
                    + ","
                    + ",".join(c + "__reason VARCHAR" for c in TECHNICAL_COLUMNS)
                )
                connection.execute(
                    "CREATE TEMP TABLE technical_derived(ts_code VARCHAR,trade_date DATE,"
                    + definitions
                    + ",PRIMARY KEY(ts_code,trade_date))"
                )
                receipts = {}
                nan_columns: set[str] = set()
                for start in range(0, len(scope.stock_codes), 500):
                    codes_batch = scope.stock_codes[start : start + 500]
                    connection.execute(
                        "INSERT INTO technical_history_input SELECT b.ts_code,b.trade_date,"
                        "b.high,b.low,b.close,a.adj_factor,a.ts_code IS NOT NULL FROM daily_bar b "
                        "LEFT JOIN adj_factor a USING(ts_code,trade_date) "
                        "WHERE b.ts_code IN (SELECT unnest(?)) AND b.trade_date<=?",
                        [list(codes_batch), scope.end_date],
                    )
                    for code, rows in _code_rows(connection, codes_batch, scope.end_date):
                        if len(rows) != totals[code] or len(rows) > request.max_code_observations:
                            raise ValueError("technical code rows differ from bounded preflight")
                        output, receipts[code] = _derive_code(code, rows, scope.start_date)
                        if not output.empty:
                            connection.register("technical_code_output", output)
                            connection.execute(
                                "INSERT INTO technical_derived BY NAME "
                                "SELECT * FROM technical_code_output"
                            )
                            connection.unregister("technical_code_output")
                            for column in TECHNICAL_COLUMNS:
                                if (
                                    (output[column + "__reason"] == "derived_non_finite")
                                    & output[column].isna()
                                ).any():
                                    nan_columns.add(column)
                        del output, rows
                for column in sorted(nan_columns):
                    connection.execute(
                        "UPDATE technical_derived SET "
                        + column
                        + "='NaN'::DOUBLE WHERE "
                        + column
                        + " IS NULL AND "
                        + column
                        + "__reason='derived_non_finite'"
                    )
                codes = tuple(
                    receipts.get(code) or _derive_code(code, [], scope.start_date)[1]
                    for code in scope.stock_codes
                )
                input_start = min(
                    (code.raw_start_date for code in codes if code.raw_start_date is not None),
                    default=scope.start_date,
                )
                inputs = materialize_table_dependency(
                    connection,
                    dependency=StrategyTableDependency(
                        dataset_id="factor_technical_history_inputs",
                        table_name="technical_history_input",
                        date_column="trade_date",
                        code_column="ts_code",
                    ),
                    artifact_root=root,
                    start_date=input_start,
                    end_date=scope.end_date,
                    as_of_time=scope.as_of_time,
                    ts_codes=scope.stock_codes,
                )
                tables = []
                for table, source_table in (
                    ("daily_indicator", "technical_derived"),
                    ("daily_basic", "daily_basic"),
                ):
                    artifact = materialize_table_dependency(
                        connection,
                        dependency=StrategyTableDependency(
                            dataset_id="factor_daily_features",
                            table_name=table,
                            date_column="trade_date",
                            code_column="ts_code",
                        ),
                        artifact_root=root,
                        start_date=scope.start_date,
                        end_date=scope.end_date,
                        as_of_time=scope.as_of_time,
                        ts_codes=scope.stock_codes,
                        source_table_name=source_table,
                    )
                    if table == "daily_indicator":
                        connection.execute(
                            "CREATE TEMP VIEW daily_indicator AS SELECT * FROM technical_derived"
                        )
                    tables.append(
                        _observe(
                            connection,
                            scope,
                            prepared.receipt.calendar_open_days,
                            table,
                            artifact,
                            technical=table == "daily_indicator",
                        )
                    )
                policy = FactorTechnicalHistoryPolicy(
                    implementation_sha256=hashlib.sha256(
                        Path(technical.__file__).read_bytes()
                    ).hexdigest()
                )
                history = FactorTechnicalHistoryReceipt(
                    policy=policy,
                    inputs=(inputs,),
                    codes=codes,
                    input_rows=input_rows,
                    max_input_rows=request.max_input_rows,
                    max_code_observations=request.max_code_observations,
                    max_output_cells=request.max_output_cells,
                )
                fields = dict(
                    schema_version=2,
                    prepared_source_sha256=prepared.sha256,
                    prepared_snapshot_id=prepared.snapshot.snapshot_id,
                    prepared_binding_hash=prepared.binding.binding_hash,
                    scope_content_hash=prepared.scope_content_hash,
                    scope=scope,
                    generation=generation,
                    code_commit=original.code_commit,
                    calendar_open_days=prepared.receipt.calendar_open_days,
                    fields=DERIVED_DAILY_FIELDS,
                    value_semantics="history_derived",
                    price_basis="observation_factor_then_output_session_scale",
                    recursive_initialization="first_valid_observation_no_restart",
                    technical_history=history,
                    source_read_boundary="single_snapshot_transaction",
                    read_mode=mode,
                    observed_at=observed,
                    completed_read_at=normalize_utc_datetime(now()),
                    tables=tuple(tables),
                )
                fields["source_mode"] = "historical_retrospective"
                source = FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))
                source.require_prepared(prepared)
                _check_generation(original, generation, descriptor)
                _require_same_root(root, root_fd)
                connection.execute("COMMIT")
                transaction = False
                _check_generation(original, generation, descriptor)
                return source
            finally:
                if connection is not None:
                    if transaction:
                        with suppress(Exception):
                            connection.execute("ROLLBACK")
                    connection.close()
                os.close(descriptor)
    finally:
        os.close(root_fd)
