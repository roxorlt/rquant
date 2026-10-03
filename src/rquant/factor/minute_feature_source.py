"""Fixed 15:00 retrospective observations from the existing minute screening kernel."""

from __future__ import annotations

import hashlib
import os
import stat
from collections import OrderedDict
from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime, time
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import TYPE_CHECKING, Literal
from zoneinfo import ZoneInfo

import pandas as pd

from rquant.data_metadata import normalize_utc_datetime, utc_now
from rquant.factor.result_artifact import _open_private_root, _require_same_root, _root_path
from rquant.factor.source_prepare import _check_generation, _generation
from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.research_snapshot import (
    _source_table_schema,
    materialize_table_dependency,
    verify_materialized_table_artifact,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_dependencies import StrategyTableDependency

if TYPE_CHECKING:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureSource,
        FactorMinuteFeaturePrepareRequest,
    )

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshotArtifact
from rquant.factor.universe import StockCode

_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
MinuteFeatureColumn = Literal[
    "signal_minute_amount",
    "signal_cum_amount_asof",
    "hist_same_minute_amount_median_20d",
    "hist_cum_amount_asof_median_20d",
    "signal_rel_amount_same_minute_20d",
    "signal_rel_cum_amount_asof_20d",
    "hist_intraday_days_20d",
    "signal_opening_segment",
    "signal_opening_segment_amount",
    "signal_amount_accel_5m",
    "signal_amount_accel_10m",
]
MinuteFeatureReason = Literal[
    "missing_target_minute",
    "missing_history",
    "missing_same_minute_history",
    "zero_same_minute_baseline",
    "zero_cumulative_baseline",
    "not_applicable",
    "no_acceleration_history",
    "undefined_statistic",
]
MINUTE_FEATURE_DESCRIPTIONS = tuple(
    sorted(
        (
            (
                "signal_minute_amount",
                "15:00分钟成交额",
                "CNY",
                "精确15:00分钟成交额，单位元；缺该分钟不回退。",
            ),
            (
                "signal_cum_amount_asof",
                "15:00累计成交额",
                "CNY",
                "目标分钟存在时，当日截至15:00实际分钟成交额累计，单位元。",
            ),
            (
                "hist_same_minute_amount_median_20d",
                "历史同分钟成交额中位数",
                "CNY",
                "前最多20个实际观察日的15:00分钟成交额中位数，单位元。",
            ),
            (
                "hist_cum_amount_asof_median_20d",
                "历史累计成交额中位数",
                "CNY",
                "原核前最多20个实际观察日截至15:00累计成交额中位数，单位元。",
            ),
            (
                "signal_rel_amount_same_minute_20d",
                "同分钟相对成交额",
                "ratio",
                "目标成交额/历史同分钟成交额中位数；零基准无值。",
            ),
            (
                "signal_rel_cum_amount_asof_20d",
                "累计相对成交额",
                "ratio",
                "当日累计成交额/历史累计成交额中位数；零基准无值。",
            ),
            (
                "hist_intraday_days_20d",
                "历史分钟观察日数",
                "observations",
                "原核截至15:00实际有分钟记录的历史日数，0有效。",
            ),
            (
                "signal_opening_segment",
                "开盘段标记",
                "binary",
                "实际15:00观察存在时为0；缺目标分钟无值。",
            ),
            (
                "signal_opening_segment_amount",
                "开盘段成交额",
                "CNY",
                "固定15:00观察不适用，保留原核无值。",
            ),
            (
                "signal_amount_accel_5m",
                "5观察成交额加速",
                "ratio",
                "15:00成交额/此前最近最多5个正成交额分钟中位数；短窗口仍计算。",
            ),
            (
                "signal_amount_accel_10m",
                "10观察成交额加速",
                "ratio",
                "15:00成交额/此前最近最多10个正成交额分钟中位数；短窗口仍计算。",
            ),
        ),
        key=lambda item: item[0],
    )
)
MINUTE_FEATURE_COLUMNS = tuple(item[0] for item in MINUTE_FEATURE_DESCRIPTIONS)


class FactorMinuteFeaturePolicy(BaseModel):
    model_config = _MODEL
    algorithm_version: Literal["rquant-intraday-relative-volume-v1"] = (
        "rquant-intraday-relative-volume-v1"
    )
    implementation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    frequency: Literal["1min"] = "1min"
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    panel_clock: Literal["15:00:00"] = "15:00:00"
    missing_target: Literal["missing_target_minute_no_fallback"] = (
        "missing_target_minute_no_fallback"
    )
    evaluation_clock: Literal["next_sse_day_09:25"] = "next_sse_day_09:25"
    lookback_days: Literal[20] = 20
    history_selection: Literal["previous_actual_dates_before_panel"] = (
        "previous_actual_dates_before_panel"
    )
    source_priority: tuple[Literal["tushare", "tushare_rt", "other"], ...] = (
        "tushare",
        "tushare_rt",
        "other",
    )
    amount_unit: Literal["CNY"] = "CNY"
    volume_unit: Literal["shares"] = "shares"
    current_cumulative: Literal["all_observed_minutes_through_target"] = (
        "all_observed_minutes_through_target"
    )
    current_cumulative_sum: Literal["pandas_float64_series_sum"] = "pandas_float64_series_sum"
    historical_cumulative_sum: Literal["pandas_groupby_sum_then_series_median"] = (
        "pandas_groupby_sum_then_series_median"
    )
    historical_start: Literal["earliest_selected_date_09:30_original_store_range"] = (
        "earliest_selected_date_09:30_original_store_range"
    )
    rounding_digits: Literal[4] = 4

    @model_validator(mode="after")
    def _priority(self) -> FactorMinuteFeaturePolicy:
        if self.source_priority != ("tushare", "tushare_rt", "other"):
            raise ValueError("minute source priority differs from the original reader")
        return self


class FactorMinuteFeatureDiagnostic(BaseModel):
    model_config = _MODEL
    panel_time: datetime
    target_present: bool
    current_day_observations: int = Field(ge=0, le=300_000)
    selected_history_days: int = Field(ge=0, le=20)
    historical_days: int = Field(ge=0, le=20)
    same_clock_days: int = Field(ge=0, le=20)
    regular_prior_observations: int = Field(ge=0, le=300_000)

    @model_validator(mode="after")
    def _window(self) -> FactorMinuteFeatureDiagnostic:
        from datetime import time
        from zoneinfo import ZoneInfo

        if (
            self.panel_time.tzinfo is None
            or self.panel_time.astimezone(ZoneInfo("Asia/Shanghai")).time() != time(15)
            or not self.same_clock_days <= self.historical_days <= self.selected_history_days
        ):
            raise ValueError("minute diagnostic clock or observed windows differ")
        return self


class FactorMinuteFeatureCode(BaseModel):
    model_config = _MODEL
    stock_code: StockCode
    input_rows: int = Field(ge=0, le=300_000)
    input_observations: int = Field(ge=0, le=300_000)
    raw_start_date: date | None
    raw_end_date: date | None

    @model_validator(mode="after")
    def _bounds(self) -> FactorMinuteFeatureCode:
        if (
            self.input_observations > self.input_rows
            or (self.input_observations == 0) != (self.raw_start_date is None)
            or (self.raw_start_date is None) != (self.raw_end_date is None)
            or (self.raw_start_date is not None and self.raw_start_date > self.raw_end_date)
        ):
            raise ValueError("minute input observation boundaries differ")
        return self


class FactorMinuteFeatureSummary(BaseModel):
    model_config = _MODEL
    policy: FactorMinuteFeaturePolicy
    source_history_start: date | None
    codes_with_history: int = Field(ge=0, le=7000)
    codes_without_history: int = Field(ge=0, le=7000)


class FactorMinuteFeatureReceipt(BaseModel):
    model_config = _MODEL
    policy: FactorMinuteFeaturePolicy
    inputs: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1, max_length=1)
    codes: tuple[FactorMinuteFeatureCode, ...] = Field(min_length=1, max_length=7000)
    input_rows: int = Field(ge=0, le=128_000_000)
    max_input_rows: int = Field(gt=0, le=128_000_000)
    max_code_rows: int = Field(gt=0, le=300_000)
    max_output_cells: int = Field(gt=0, le=128_000_000)

    @model_validator(mode="after")
    def _input(self) -> FactorMinuteFeatureReceipt:
        (artifact,) = self.inputs
        if (
            tuple(c.stock_code for c in self.codes)
            != tuple(sorted(set(c.stock_code for c in self.codes)))
            or sum(c.input_rows for c in self.codes) != self.input_rows
            or self.input_rows > self.max_input_rows
            or any(c.input_rows > self.max_code_rows for c in self.codes)
            or artifact.dataset_id != "factor_minute_feature_input"
            or artifact.table_name != "minute_feature_input"
            or artifact.row_count != self.input_rows
            or artifact.primary_key != ("ts_code", "trade_time", "freq", "source")
            or artifact.event_column != "trade_date"
            or artifact.artifact_type != "materialized_table"
        ):
            raise ValueError("minute receipt differs from complete bounded input scope")
        return self

    def summary(self) -> FactorMinuteFeatureSummary:
        return FactorMinuteFeatureSummary(
            policy=self.policy,
            source_history_start=min(
                (c.raw_start_date for c in self.codes if c.raw_start_date is not None), default=None
            ),
            codes_with_history=sum(c.input_observations > 0 for c in self.codes),
            codes_without_history=sum(c.input_observations == 0 for c in self.codes),
        )

    def causal_policy(self) -> FactorMinuteFeaturePolicy:
        return self.policy


# Dedicated input validation cache: every access still checks original bytes and identity.
_VALIDATED: OrderedDict[tuple[str, datetime], None] = OrderedDict()
_VALIDATION_LOCK = Lock()
_InputIdentity = tuple[int, int, int, int, int, int, int]


def _identity(node: os.stat_result) -> _InputIdentity:
    return (
        node.st_dev,
        node.st_ino,
        node.st_uid,
        node.st_mode,
        node.st_size,
        node.st_mtime_ns,
        node.st_ctime_ns,
    )


def _verify_minute_feature_input(
    artifact: DatasetSnapshotArtifact,
    *,
    lake_root: Path,
    as_of_time: datetime,
    expected_identity: _InputIdentity | None = None,
) -> tuple[Path, _InputIdentity]:
    root = _root_path(lake_root)
    root_fd = _open_private_root(root)
    fd = None
    try:
        if (
            artifact.dataset_id != "factor_minute_feature_input"
            or artifact.table_name != "minute_feature_input"
            or artifact.relative_path
            != f"tables/minute_feature_input/versions/{artifact.file_hash}.parquet"
        ):
            raise ValueError("minute input artifact path differs")
        path = root / artifact.relative_path
        if path.resolve() != path:
            raise ValueError("minute input path traverses a symbolic link")
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
        node = os.fstat(fd)
        identity = _identity(node)
        if (
            not stat.S_ISREG(node.st_mode)
            or node.st_uid != os.getuid()
            or _identity(path.stat(follow_symlinks=False)) != identity
            or (expected_identity is not None and identity != expected_identity)
            or artifact.file_size != node.st_size
        ):
            raise ValueError("minute input file identity changed")
        digest = hashlib.sha256()
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
        if digest.hexdigest() != artifact.file_hash:
            raise ValueError("minute input file hash mismatch")
        key = canonical_sha256(artifact), as_of_time
        with _VALIDATION_LOCK:
            cached = key in _VALIDATED
            if cached:
                _VALIDATED.move_to_end(key)
        if not cached:
            verify_materialized_table_artifact(artifact, lake_root=root, as_of_time=as_of_time)
        if (
            _identity(os.fstat(fd)) != identity
            or _identity(path.stat(follow_symlinks=False)) != identity
        ):
            raise ValueError("minute input changed during validation")
        _require_same_root(root, root_fd)
        if not cached:
            with _VALIDATION_LOCK:
                _VALIDATED[key] = None
                _VALIDATED.move_to_end(key)
                while len(_VALIDATED) > 128:
                    _VALIDATED.popitem(last=False)
        return path, identity
    finally:
        if fd is not None:
            os.close(fd)
        os.close(root_fd)


_RAW_COLUMNS = (
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
)


def _derive_code(code: str, rows: list[tuple], days: tuple[date, ...]) -> pd.DataFrame:
    from rquant.stock_features import build_intraday_relative_volume_features_from_history

    frame = pd.DataFrame(rows, columns=_RAW_COLUMNS)
    # fetchmany loses DuckDB's DOUBLE dtype when a code has only NULL amounts.
    frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce").astype(float)
    stamps = pd.to_datetime(frame["trade_time"])
    frame["date"] = stamps.dt.date
    frame["clock"] = stamps.dt.time
    result_rows = []
    for day in days:
        signal = datetime.combine(day, time(15))
        current = frame[(frame["date"] == day) & (frame["clock"] <= time(15))]
        target = current[current["clock"] == time(15)]
        previous_dates = tuple(sorted(set(frame.loc[frame["date"] < day, "date"]))[-20:])
        history = frame[frame["date"].isin(previous_dates) & (frame["clock"] <= time(15))]
        if previous_dates:
            history = history[
                history["trade_time"] >= datetime.combine(previous_dates[0], time(9, 30))
            ]
        regular = current[
            (current["clock"] > time(9, 32))
            & (current["clock"] < time(15))
            & (current["amount"] > 0)
        ]
        diagnostic = FactorMinuteFeatureDiagnostic(
            panel_time=signal.replace(tzinfo=ZoneInfo("Asia/Shanghai")),
            target_present=not target.empty,
            current_day_observations=len(current),
            selected_history_days=len(previous_dates),
            historical_days=history["date"].nunique(),
            same_clock_days=history.loc[history["clock"] == time(15), "date"].nunique(),
            regular_prior_observations=len(regular),
        )
        output = {
            "ts_code": code,
            "trade_date": day,
            "minute_diagnostic": diagnostic.model_dump_json(),
        }
        if target.empty:
            features = {c: None for c in MINUTE_FEATURE_COLUMNS}
        else:
            features = build_intraday_relative_volume_features_from_history(
                frame.loc[:, list(_RAW_COLUMNS)],
                previous_dates,
                signal,
                current_minute_amount=float(
                    pd.to_numeric(target["amount"], errors="coerce").iloc[0]
                ),
                current_cum_amount=float(current["amount"].sum()),
                current_day_amounts=tuple(zip(current["clock"], current["amount"], strict=True)),
            )
        for column in MINUTE_FEATURE_COLUMNS:
            value = features[column]
            reason = None
            if value is None:
                if target.empty:
                    reason = "missing_target_minute"
                elif column == "signal_opening_segment_amount":
                    reason = "not_applicable"
                elif column.startswith("signal_amount_accel_"):
                    reason = "no_acceleration_history" if regular.empty else "undefined_statistic"
                elif column in (
                    "hist_same_minute_amount_median_20d",
                    "signal_rel_amount_same_minute_20d",
                ):
                    if history.empty:
                        reason = "missing_history"
                    elif not diagnostic.same_clock_days:
                        reason = "missing_same_minute_history"
                    elif column.startswith("signal_rel_") and (
                        pd.to_numeric(
                            history.loc[history["clock"] == time(15), "amount"], errors="coerce"
                        ).median()
                        <= 0
                    ):
                        reason = "zero_same_minute_baseline"
                    else:
                        reason = "undefined_statistic"
                elif column in (
                    "hist_cum_amount_asof_median_20d",
                    "signal_rel_cum_amount_asof_20d",
                ):
                    reason = (
                        "missing_history"
                        if history.empty
                        else "zero_cumulative_baseline"
                        if column.startswith("signal_rel_")
                        and history.groupby("date")["amount"].sum().median() <= 0
                        else "undefined_statistic"
                    )
                else:
                    reason = "undefined_statistic"
            output[column], output[column + "__reason"] = value, reason
        result_rows.append(output)
    return pd.DataFrame(result_rows)


def prepare_factor_minute_feature_source(
    request: FactorMinuteFeaturePrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorDailyFeatureSource:
    from rquant import stock_features
    from rquant.factor.daily_feature_source import (
        MINUTE_FEATURE_FIELDS,
        FactorDailyFeatureSource,
        FactorMinuteFeaturePrepareRequest,
        _observe,
        open_factor_daily_feature_source,
    )

    request = FactorMinuteFeaturePrepareRequest.model_validate(request)
    prepared, original = request.prepared_source, request.prepared_source.receipt.request
    scope, base = original.scope, request.base_daily_source
    if base is not None:
        if base.schema_version not in (1, 2, 3):
            raise ValueError("minute base must be an original v1/v2/v3 daily source")
        base.require_prepared(prepared)
        with open_factor_daily_feature_source(base, lake_root=lake_root):
            pass
    width = 11 + (len(base.fields) if base is not None else 0)
    if (
        len(scope.stock_codes) * ((scope.end_date - scope.start_date).days + 1) * width
        > request.max_output_cells
    ):
        raise ValueError("minute output cell budget exceeded")
    generation = _generation(original)
    if generation != prepared.receipt.generation:
        raise ValueError("minute generation differs from paired prices")
    root = _root_path(lake_root)
    if root.is_relative_to(original.replica_path.parent):
        raise ValueError("minute feature lake must be outside replica directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_fd = _open_private_root(root)
    cutoff = scope.as_of_time.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    try:
        with TemporaryDirectory(prefix=".minute-feature-prepare-", dir=root) as scratch:
            fd = os.open(original.replica_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            connection, transaction = None, False
            try:
                _check_generation(original, generation, fd)
                connection, mode = connect_pinned_readonly(original.replica_path, fd)
                connection.execute("SET threads=1")
                connection.execute("SET memory_limit='512MB'")
                connection.execute("SET temp_directory=?", [scratch])
                connection.execute("BEGIN TRANSACTION")
                transaction = True
                observed = normalize_utc_datetime(now())
                if (
                    observed < prepared.receipt.completed_read_at
                    or base is not None
                    and observed < base.completed_read_at
                ):
                    raise ValueError("minute read precedes paired source")
                schema, key = _source_table_schema(connection, "minute_bar")
                types = dict(schema)
                if (
                    key != ("ts_code", "trade_time", "freq", "source")
                    or types.get("ts_code") != "VARCHAR"
                    or types.get("trade_time") != "TIMESTAMP"
                    or types.get("freq") != "VARCHAR"
                    or types.get("source") != "VARCHAR"
                    or any(types.get(c) != "DOUBLE" for c in _RAW_COLUMNS[3:-1])
                ):
                    raise ValueError("minute source schema/business key differs")
                connection.execute(
                    "CREATE TEMP TABLE minute_feature_input(ts_code "
                    "VARCHAR,trade_time TIMESTAMP,freq VARCHAR,"
                    + ",".join(c + " DOUBLE" for c in _RAW_COLUMNS[3:-1])
                    + ",source VARCHAR,trade_date DATE,PRIMARY KEY(ts_code,trade_time,freq,source))"
                )
                first_panel = min(prepared.receipt.calendar_open_days, default=scope.start_date)
                bounded = (
                    "WITH dates AS (SELECT DISTINCT ts_code,CAST(trade_time AS "
                    "DATE) d FROM minute_bar "
                    "WHERE ts_code IN (SELECT unnest(?)) AND freq='1min' AND "
                    "CAST(trade_time AS DATE)<? AND trade_time<=?), "
                    "previous AS (SELECT ts_code,d,row_number() OVER(PARTITION "
                    "BY ts_code ORDER BY d DESC) n FROM dates), "
                    "wanted AS (SELECT ts_code,d FROM previous WHERE n<=20 "
                    "UNION SELECT DISTINCT ts_code,CAST(trade_time AS DATE) d "
                    "FROM minute_bar WHERE ts_code IN (SELECT unnest(?)) AND "
                    "freq='1min' AND CAST(trade_time AS DATE) BETWEEN ? AND ? "
                    "AND trade_time<=?) "
                    "SELECT "
                    + ",".join("m." + c for c in _RAW_COLUMNS)
                    + ",w.d AS trade_date FROM minute_bar m JOIN wanted w "
                    "ON m.ts_code=w.ts_code AND CAST(m.trade_time AS DATE)=w.d "
                    "WHERE m.freq='1min' AND m.trade_time<=?"
                )
                input_rows = 0
                for start in range(0, len(scope.stock_codes), 500):
                    batch = list(scope.stock_codes[start : start + 500])
                    params = [
                        batch,
                        first_panel,
                        cutoff,
                        batch,
                        scope.start_date,
                        scope.end_date,
                        cutoff,
                        cutoff,
                    ]
                    counts = connection.execute(
                        "SELECT ts_code,count(*) FROM (" + bounded + ") GROUP BY ts_code", params
                    ).fetchall()
                    input_rows += sum(int(n) for _, n in counts)
                    if input_rows > request.max_input_rows or any(
                        n > request.max_code_rows for _, n in counts
                    ):
                        raise ValueError("minute complete historical input budget exceeded")
                    connection.execute("INSERT INTO minute_feature_input " + bounded, params)
                connection.execute(
                    "CREATE TEMP TABLE daily_minute_feature(ts_code VARCHAR,trade_date DATE,"
                    + ",".join(
                        c + " DOUBLE," + c + "__reason VARCHAR" for c in MINUTE_FEATURE_COLUMNS
                    )
                    + ",minute_diagnostic VARCHAR,PRIMARY KEY(ts_code,trade_date))"
                )
                physical_by_code = {
                    c: (int(n), first, last)
                    for c, n, first, last in connection.execute(
                        "SELECT ts_code,count(*),min(trade_date),max(trade_date) "
                        "FROM minute_feature_input GROUP BY ts_code"
                    ).fetchall()
                }
                codes = []
                for code in scope.stock_codes:
                    physical = physical_by_code.get(code, (0, None, None))
                    rows = connection.execute(
                        "SELECT "
                        + ",".join(_RAW_COLUMNS)
                        + " FROM minute_feature_input WHERE ts_code=? "
                        "QUALIFY row_number() OVER(PARTITION BY "
                        "ts_code,trade_time,freq ORDER BY CASE source WHEN "
                        "'tushare' THEN 0 WHEN 'tushare_rt' THEN 1 ELSE 2 END)=1 "
                        "ORDER BY trade_time",
                        [code],
                    ).fetchmany(request.max_code_rows + 1)
                    if len(rows) > request.max_code_rows:
                        raise ValueError("minute single-code input budget exceeded")
                    codes.append(
                        FactorMinuteFeatureCode(
                            stock_code=code,
                            input_rows=int(physical[0]),
                            input_observations=len(rows),
                            raw_start_date=physical[1],
                            raw_end_date=physical[2],
                        )
                    )
                    output = _derive_code(code, rows, prepared.receipt.calendar_open_days)
                    if not output.empty:
                        connection.register("minute_code_output", output)
                        try:
                            connection.execute(
                                "INSERT INTO daily_minute_feature BY NAME SELECT * FROM "
                                "minute_code_output"
                            )
                        finally:
                            connection.unregister("minute_code_output")
                    del output, rows
                start_date = (
                    connection.execute(
                        "SELECT min(trade_date) FROM minute_feature_input"
                    ).fetchone()[0]
                    or scope.start_date
                )
                inputs = materialize_table_dependency(
                    connection,
                    dependency=StrategyTableDependency(
                        dataset_id="factor_minute_feature_input",
                        table_name="minute_feature_input",
                        date_column="trade_date",
                        code_column="ts_code",
                    ),
                    artifact_root=root,
                    start_date=start_date,
                    end_date=scope.end_date,
                    as_of_time=scope.as_of_time,
                    ts_codes=scope.stock_codes,
                )
                artifact = materialize_table_dependency(
                    connection,
                    dependency=StrategyTableDependency(
                        dataset_id="factor_daily_features",
                        table_name="daily_minute_feature",
                        date_column="trade_date",
                        code_column="ts_code",
                    ),
                    artifact_root=root,
                    start_date=scope.start_date,
                    end_date=scope.end_date,
                    as_of_time=scope.as_of_time,
                    ts_codes=scope.stock_codes,
                )
                receipt = FactorMinuteFeatureReceipt(
                    policy=FactorMinuteFeaturePolicy(
                        implementation_sha256=hashlib.sha256(
                            Path(stock_features.__file__).read_bytes()
                        ).hexdigest()
                    ),
                    inputs=(inputs,),
                    codes=tuple(codes),
                    input_rows=input_rows,
                    max_input_rows=request.max_input_rows,
                    max_code_rows=request.max_code_rows,
                    max_output_cells=request.max_output_cells,
                )
                fields = dict(
                    schema_version=4,
                    prepared_source_sha256=prepared.sha256,
                    prepared_snapshot_id=prepared.snapshot.snapshot_id,
                    prepared_binding_hash=prepared.binding.binding_hash,
                    scope_content_hash=prepared.scope_content_hash,
                    scope=scope,
                    generation=generation,
                    code_commit=original.code_commit,
                    calendar_open_days=prepared.receipt.calendar_open_days,
                    fields=tuple(
                        sorted(
                            (() if base is None else base.fields) + MINUTE_FEATURE_FIELDS,
                            key=lambda f: f.column,
                        )
                    ),
                    source_mode="historical_retrospective",
                    value_semantics="minute_features_derived",
                    price_basis="field_specific",
                    recursive_initialization="field_specific",
                    minute_features=receipt,
                    source_read_boundary="single_snapshot_transaction",
                    read_mode=mode,
                    observed_at=observed,
                    completed_read_at=normalize_utc_datetime(now()),
                    tables=(() if base is None else base.tables)
                    + (
                        _observe(
                            connection,
                            scope,
                            prepared.receipt.calendar_open_days,
                            "daily_minute_feature",
                            artifact,
                            minute=True,
                        ),
                    ),
                )
                if base is not None:
                    fields["base_daily_source"] = base
                    if base.technical_history is not None:
                        fields["technical_history"] = base.technical_history
                    if base.stock_features is not None:
                        fields["stock_features"] = base.stock_features
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
    if name in ("MINUTE_FEATURE_FIELDS", "FactorMinuteFeaturePrepareRequest"):
        from rquant.factor import daily_feature_source

        return getattr(daily_feature_source, name)
    raise AttributeError(name)
