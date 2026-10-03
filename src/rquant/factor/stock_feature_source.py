"""Sealed retrospective screening windows from the existing daily stock kernel."""

from __future__ import annotations

import hashlib
import os
import stat
from collections import OrderedDict
from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock
from typing import TYPE_CHECKING, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshotArtifact, normalize_utc_datetime, utc_now
from rquant.factor.result_artifact import _open_private_root, _require_same_root, _root_path
from rquant.factor.source_prepare import (
    _check_generation,
    _generation,
)
from rquant.factor.universe import StockCode
from rquant.price_adjustment import PriceFactorUnavailableReason
from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.research_snapshot import (
    _source_table_schema,
    materialize_table_dependency,
    verify_materialized_table_artifact,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.stock_features import (
    DailyStockFeatureResult,
    build_daily_stock_feature_result_from_history,
)
from rquant.strategy_dependencies import StrategyTableDependency

if TYPE_CHECKING:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureSource,
        FactorStockFeaturePrepareRequest,
    )

_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
_SHA = r"^[0-9a-f]{64}$"
StockFeatureColumn = Literal[
    "price_window_days_90d",
    "price_position_90d_pct",
    "price_rank_90d_pct",
    "distance_to_high_90d_pct",
    "distance_to_low_90d_pct",
    "price_window_days_120d",
    "price_position_120d_pct",
    "price_rank_120d_pct",
    "distance_to_high_120d_pct",
    "distance_to_low_120d_pct",
    "price_window_days_250d",
    "price_position_250d_pct",
    "price_rank_250d_pct",
    "distance_to_high_250d_pct",
    "distance_to_low_250d_pct",
    "accum_window_days_20d",
    "accum_obv_change_20d_pct",
    "accum_ad_flow_20d_pct",
    "accum_up_down_amount_ratio_20d",
    "accum_heavy_no_drop_days_20d",
    "accum_close_position_avg_20d_pct",
    "ma_alignment",
    "price_percentile_250d",
]
StockFeatureReason = (
    PriceFactorUnavailableReason
    | Literal["missing_daily_data", "insufficient_history", "undefined_statistic"]
)
STOCK_FEATURE_DESCRIPTIONS = tuple(
    sorted(
        (
            *(
                (
                    f"price_window_days_{n}d",
                    f"{n}日实际观察数",
                    "observations",
                    f"含参考日，最近最多{n}个日线观察；短窗口保留实际观察数。",
                )
                for n in (90, 120, 250)
            ),
            *(
                (
                    f"price_position_{n}d_pct",
                    f"{n}日价格位置",
                    "percent",
                    "含参考日的实际观察，完整复权到参考日；平价为50%，短窗口仍计算。",
                )
                for n in (90, 120, 250)
            ),
            *(
                (
                    f"price_rank_{n}d_pct",
                    f"{n}日价格排名",
                    "percent",
                    f"含参考日的最近最多{n}观察，复权收盘价小于或等于参考价的百分数，包含并列。",
                )
                for n in (90, 120, 250)
            ),
            *(
                (
                    f"distance_to_high_{n}d_pct",
                    f"距{n}日最高价",
                    "percent",
                    "窗口最高价/参考收盘价−1的百分数；按参考日复权，不二次乘100。",
                )
                for n in (90, 120, 250)
            ),
            *(
                (
                    f"distance_to_low_{n}d_pct",
                    f"距{n}日最低价",
                    "percent",
                    "参考收盘价/窗口最低价−1的百分数；按参考日复权，不二次乘100。",
                )
                for n in (90, 120, 250)
            ),
            (
                "accum_window_days_20d",
                "吸筹实际观察数",
                "observations",
                "参考日之前最近最多20观察，不含参考日。",
            ),
            (
                "accum_obv_change_20d_pct",
                "20日能量潮变化",
                "percent",
                "排除参考日，完整窗口及参考日复权基准；收盘涨跌方向量变化/绝对成交量的百分数。",
            ),
            (
                "accum_ad_flow_20d_pct",
                "20日资金流代理",
                "percent",
                "排除参考日；原日线价格尺度不变的资金流乘数×成交量/绝对成交量，不依赖复权。",
            ),
            (
                "accum_up_down_amount_ratio_20d",
                "20日涨跌成交额比",
                "ratio",
                "排除参考日；按原涨跌幅正负累积成交额，跌日成交额为零时无值。",
            ),
            (
                "accum_heavy_no_drop_days_20d",
                "20日放量不跌观察数",
                "observations",
                "排除参考日；成交额≥窗口中位数1.5倍且涨跌幅≥−1%的观察数。",
            ),
            (
                "accum_close_position_avg_20d_pct",
                "20日收盘位置均值",
                "percent",
                "排除参考日；原日线(收盘−最低)/(最高−最低)裁到0–1后均值百分数。",
            ),
            (
                "ma_alignment",
                "均线多头排列",
                "binary",
                "满60个日线观察，按参考日复权，5日>10日>20日>60日均线时为1，否则为0。",
            ),
            (
                "price_percentile_250d",
                "250日收盘百分位",
                "ratio",
                "满250个日线观察，含参考日及并列；小于或等于参考价的比例，取值0–1。",
            ),
        ),
        key=lambda item: item[0],
    )
)
STOCK_FEATURE_COLUMNS = tuple(item[0] for item in STOCK_FEATURE_DESCRIPTIONS)
_RAW_COLUMNS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "pct_chg",
    "vol",
    "amount",
)


class FactorStockFeaturePolicy(BaseModel):
    model_config = _MODEL
    algorithm_version: Literal["rquant-stock-features-v1"] = "rquant-stock-features-v1"
    implementation_sha256: str = Field(pattern=_SHA)
    price_adjustment_sha256: str = Field(pattern=_SHA)
    max_observations: Literal[250] = 250
    price_windows: tuple[Literal[90, 120, 250], ...] = (90, 120, 250)
    price_reference: Literal["included_actual_observations"] = "included_actual_observations"
    adjustment: Literal["complete_window_and_reference_factor"] = (
        "complete_window_and_reference_factor"
    )
    accumulation_window: Literal[20] = 20
    accumulation_reference: Literal["excluded"] = "excluded"
    short_price_window: Literal["use_actual_count"] = "use_actual_count"
    ma_alignment_observations: Literal[60] = 60
    percentile_observations: Literal[250] = 250
    rounding_digits: Literal[4] = 4

    @model_validator(mode="after")
    def _windows(self) -> FactorStockFeaturePolicy:
        if self.price_windows != (90, 120, 250):
            raise ValueError("stock feature window policy differs from the kernel")
        return self


class FactorStockFeatureDiagnostic(BaseModel):
    """The kernel's per-window outcome without repeating every valid factor ratio."""

    model_config = _MODEL
    feature_family: str = Field(min_length=1, max_length=64)
    window_days: int = Field(ge=1, le=250)
    actual_observations: int = Field(ge=0, le=250)
    available: bool
    reason: StockFeatureReason | None = Field(default=None, exclude_if=lambda v: v is None)
    unavailable_dates: tuple[date, ...] = Field(
        default=(), max_length=250, exclude_if=lambda v: not v
    )

    @model_validator(mode="after")
    def _state(self) -> FactorStockFeatureDiagnostic:
        if (
            self.available == (self.reason is not None)
            or self.actual_observations > self.window_days
        ):
            raise ValueError("stock window diagnostic state differs")
        if self.unavailable_dates != tuple(sorted(set(self.unavailable_dates))):
            raise ValueError("stock window diagnostic dates differ")
        return self


class FactorStockFeatureCode(BaseModel):
    model_config = _MODEL
    stock_code: StockCode
    input_rows: int = Field(ge=0, le=50_000)
    input_observations: int = Field(ge=0, le=50_000)
    raw_start_date: date | None
    raw_end_date: date | None

    @model_validator(mode="after")
    def _bound(self) -> FactorStockFeatureCode:
        if (
            self.input_observations > self.input_rows
            or (self.input_observations == 0) != (self.raw_start_date is None)
            or (self.raw_start_date is None) != (self.raw_end_date is None)
            or (self.raw_start_date is not None and self.raw_start_date > self.raw_end_date)
        ):
            raise ValueError("stock input observation boundaries differ")
        return self


class FactorStockFeatureSummary(BaseModel):
    model_config = _MODEL
    policy: FactorStockFeaturePolicy
    source_history_start: date | None
    codes_with_history: int = Field(ge=0, le=7000)
    codes_without_history: int = Field(ge=0, le=7000)


class FactorStockFeatureReceipt(BaseModel):
    model_config = _MODEL
    policy: FactorStockFeaturePolicy
    inputs: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1, max_length=1)
    codes: tuple[FactorStockFeatureCode, ...] = Field(min_length=1, max_length=7000)
    input_rows: int = Field(ge=0, le=16_000_000)
    max_input_rows: int = Field(gt=0, le=16_000_000)
    max_code_observations: int = Field(gt=0, le=50_000)
    max_output_cells: int = Field(gt=0, le=64_000_000)

    @model_validator(mode="after")
    def _input(self) -> FactorStockFeatureReceipt:
        (artifact,) = self.inputs
        if (
            tuple(c.stock_code for c in self.codes)
            != tuple(sorted(set(c.stock_code for c in self.codes)))
            or sum(c.input_rows for c in self.codes) != self.input_rows
            or self.input_rows > self.max_input_rows
            or any(c.input_rows > self.max_code_observations for c in self.codes)
            or artifact.dataset_id != "factor_stock_feature_inputs"
            or artifact.table_name != "stock_feature_input"
            or artifact.row_count != self.input_rows
            or artifact.primary_key != ("ts_code", "trade_date")
            or artifact.event_column != "trade_date"
            or artifact.artifact_type != "materialized_table"
        ):
            raise ValueError("stock input receipt differs from bounded code scope")
        return self

    def summary(self) -> FactorStockFeatureSummary:
        return FactorStockFeatureSummary(
            policy=self.policy,
            source_history_start=min(
                (c.raw_start_date for c in self.codes if c.raw_start_date is not None), default=None
            ),
            codes_with_history=sum(c.input_observations > 0 for c in self.codes),
            codes_without_history=sum(c.input_observations == 0 for c in self.codes),
        )

    def causal_policy(self) -> FactorStockFeaturePolicy:
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


def _verify_stock_feature_input(
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
            artifact.dataset_id != "factor_stock_feature_inputs"
            or artifact.table_name != "stock_feature_input"
            or artifact.relative_path
            != f"tables/stock_feature_input/versions/{artifact.file_hash}.parquet"
        ):
            raise ValueError("stock input artifact path differs")
        path = root / artifact.relative_path
        if path.resolve() != path:
            raise ValueError("stock input path traverses a symbolic link")
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
            raise ValueError("stock input file identity changed")
        digest = hashlib.sha256()
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
        if digest.hexdigest() != artifact.file_hash:
            raise ValueError("stock input file hash mismatch")
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
            raise ValueError("stock input changed during validation")
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


def _diagnostic(
    result: DailyStockFeatureResult, column: str, daily: pd.DataFrame
) -> FactorStockFeatureDiagnostic:
    if column.startswith(("price_", "distance_")) and column != "price_percentile_250d":
        n = int(column.split("_")[-2 if column.endswith("_pct") else -1][:-1])
        family = f"price_position_{n}d"
        observations = min(len(daily), n)
    elif column == "ma_alignment":
        n, family, observations = 60, "ma_alignment_60d", min(len(daily), 60)
    elif column == "price_percentile_250d":
        n, family, observations = 250, "price_percentile_250d", min(len(daily), 250)
    else:
        n = 20
        family = (
            "accumulation_obv_20d"
            if column == "accum_obv_change_20d_pct"
            else "accumulation_scale_invariant_20d"
        )
        observations = (
            min(int((daily["trade_date"] < result.reference_date).sum()), 20)
            if not daily.empty
            else 0
        )
    core = result.diagnostics.get(family)
    return FactorStockFeatureDiagnostic(
        feature_family=family,
        window_days=n,
        actual_observations=observations,
        available=core.available if core else False,
        reason=core.reason if core else "missing_daily_data",
        unavailable_dates=core.basis.unavailable_dates if core and core.basis else (),
    )


def _derive_code(code: str, rows: list[tuple], days: tuple[date, ...]) -> pd.DataFrame:
    frame = pd.DataFrame(
        rows, columns=(*_RAW_COLUMNS, "bar_present", "adj_factor", "factor_present")
    )
    bars = frame[frame["bar_present"] == True].loc[:, list(_RAW_COLUMNS)].copy()  # noqa: E712
    factors = {
        row[1]: None if row[11] is None or pd.isna(row[11]) else float(row[11]) for row in rows
    }
    result_rows = []
    for day in days:
        daily = bars[bars["trade_date"] <= day].tail(250).reset_index(drop=True)
        result = build_daily_stock_feature_result_from_history(daily, factors, code, day)
        output = {"ts_code": code, "trade_date": day}
        for column in STOCK_FEATURE_COLUMNS:
            value = result.features.get(column)
            diag = _diagnostic(result, column, daily)
            reason = None if value is not None else diag.reason or "undefined_statistic"
            output[column] = value
            output[column + "__reason"] = reason
            output[column + "__diagnostic"] = diag.model_dump_json()
        result_rows.append(output)
    return pd.DataFrame(result_rows)


def prepare_factor_stock_feature_source(
    request: FactorStockFeaturePrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorDailyFeatureSource:
    from rquant import price_adjustment, stock_features
    from rquant.factor.daily_feature_source import (
        STOCK_FEATURE_FIELDS,
        FactorDailyFeatureSource,
        FactorStockFeaturePrepareRequest,
        _observe,
        open_factor_daily_feature_source,
    )

    request = FactorStockFeaturePrepareRequest.model_validate(request)
    prepared, original = request.prepared_source, request.prepared_source.receipt.request
    scope, base = original.scope, request.base_daily_source
    if base is not None:
        if base.schema_version not in (1, 2):
            raise ValueError("stock base must be an original v1/v2 daily source")
        base.require_prepared(prepared)
        with open_factor_daily_feature_source(base, lake_root=lake_root):
            pass
    width = 23 + (0 if base is None else len(base.fields))
    if (
        len(scope.stock_codes) * ((scope.end_date - scope.start_date).days + 1) * width
        > request.max_output_cells
    ):
        raise ValueError("stock output cell budget exceeded")
    generation = _generation(original)
    if generation != prepared.receipt.generation:
        raise ValueError("stock generation differs from paired prices")
    root = _root_path(lake_root)
    if root.is_relative_to(original.replica_path.parent):
        raise ValueError("stock feature lake must be outside replica directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_fd = _open_private_root(root)
    try:
        with TemporaryDirectory(prefix=".stock-feature-prepare-", dir=root) as scratch:
            fd = os.open(original.replica_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            connection, transaction = None, False
            try:
                _check_generation(original, generation, fd)
                connection, mode = connect_pinned_readonly(original.replica_path, fd)
                connection.execute("SET threads=1")
                connection.execute("SET temp_directory=?", [scratch])
                connection.execute("BEGIN TRANSACTION")
                transaction = True
                observed = normalize_utc_datetime(now())
                if observed < prepared.receipt.completed_read_at or (
                    base is not None and observed < base.completed_read_at
                ):
                    raise ValueError("stock read precedes paired source")
                for table, expected in (
                    ("daily_bar", _RAW_COLUMNS[2:]),
                    ("adj_factor", ("adj_factor",)),
                ):
                    schema, key = _source_table_schema(connection, table)
                    types = dict(schema)
                    if (
                        key != ("ts_code", "trade_date")
                        or types.get("ts_code") != "VARCHAR"
                        or types.get("trade_date") != "DATE"
                        or any(types.get(c) != "DOUBLE" for c in expected)
                    ):
                        raise ValueError("stock source schema/business key differs")
                connection.execute(
                    "CREATE TEMP TABLE stock_feature_input(ts_code VARCHAR,trade_date DATE,"
                    + ",".join(c + " DOUBLE" for c in _RAW_COLUMNS[2:])
                    + ",bar_present BOOLEAN,adj_factor DOUBLE,factor_present "
                    "BOOLEAN,PRIMARY KEY(ts_code,trade_date))"
                )
                bounded = (
                    "WITH historical AS (SELECT ts_code,trade_date,row_number() "
                    "OVER(PARTITION BY ts_code ORDER BY trade_date DESC) AS n FROM "
                    "daily_bar WHERE ts_code IN (SELECT unnest(?)) AND trade_date<?), "
                    ""
                    "wanted AS (SELECT ts_code,trade_date FROM historical WHERE n<=250 "
                    "UNION SELECT ts_code,trade_date FROM daily_bar WHERE ts_code IN "
                    "(SELECT unnest(?)) AND trade_date BETWEEN ? AND ? UNION SELECT c AS "
                    "ts_code,d AS trade_date FROM unnest(?) codes(c) CROSS JOIN unnest(?) "
                    "days(d)) "
                    "SELECT w.ts_code,w.trade_date,"
                    + ",".join("b." + c for c in _RAW_COLUMNS[2:])
                    + ",b.ts_code IS NOT NULL AS bar_present,a.adj_factor,a.ts_code IS NOT "
                    "NULL AS factor_present FROM wanted w LEFT JOIN daily_bar b "
                    "USING(ts_code,trade_date) LEFT JOIN adj_factor a "
                    "USING(ts_code,trade_date)"
                )
                input_rows = 0
                for start in range(0, len(scope.stock_codes), 500):
                    batch = list(scope.stock_codes[start : start + 500])
                    params = [
                        batch,
                        scope.start_date,
                        batch,
                        scope.start_date,
                        scope.end_date,
                        batch,
                        list(prepared.receipt.calendar_open_days),
                    ]
                    counts = connection.execute(
                        "SELECT ts_code,count(*) FROM (" + bounded + ") GROUP BY ts_code", params
                    ).fetchall()
                    input_rows += sum(int(c) for _, c in counts)
                    if input_rows > request.max_input_rows or any(
                        c > request.max_code_observations for _, c in counts
                    ):
                        raise ValueError("stock historical input budget exceeded")
                    connection.execute("INSERT INTO stock_feature_input " + bounded, params)
                definitions = ",".join(
                    c + " DOUBLE," + c + "__reason VARCHAR," + c + "__diagnostic VARCHAR"
                    for c in STOCK_FEATURE_COLUMNS
                )
                connection.execute(
                    "CREATE TEMP TABLE daily_stock_feature(ts_code VARCHAR,trade_date DATE,"
                    + definitions
                    + ",PRIMARY KEY(ts_code,trade_date))"
                )
                codes = []
                for code in scope.stock_codes:
                    rows = connection.execute(
                        "SELECT * FROM stock_feature_input WHERE ts_code=? ORDER BY trade_date",
                        [code],
                    ).fetchmany(request.max_code_observations + 1)
                    if len(rows) > request.max_code_observations:
                        raise ValueError("stock code input budget exceeded")
                    observed_dates = [r[1] for r in rows if r[10]]
                    codes.append(
                        FactorStockFeatureCode(
                            stock_code=code,
                            input_rows=len(rows),
                            input_observations=len(observed_dates),
                            raw_start_date=min(observed_dates, default=None),
                            raw_end_date=max(observed_dates, default=None),
                        )
                    )
                    output = _derive_code(code, rows, prepared.receipt.calendar_open_days)
                    if not output.empty:
                        connection.register("stock_code_output", output)
                        try:
                            connection.execute(
                                "INSERT INTO daily_stock_feature BY NAME SELECT * FROM "
                                "stock_code_output"
                            )
                        finally:
                            connection.unregister("stock_code_output")
                    del output, rows
                start_date = (
                    connection.execute(
                        "SELECT min(trade_date) FROM stock_feature_input"
                    ).fetchone()[0]
                    or scope.start_date
                )
                inputs = materialize_table_dependency(
                    connection,
                    dependency=StrategyTableDependency(
                        dataset_id="factor_stock_feature_inputs",
                        table_name="stock_feature_input",
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
                        table_name="daily_stock_feature",
                        date_column="trade_date",
                        code_column="ts_code",
                    ),
                    artifact_root=root,
                    start_date=scope.start_date,
                    end_date=scope.end_date,
                    as_of_time=scope.as_of_time,
                    ts_codes=scope.stock_codes,
                )
                history = FactorStockFeatureReceipt(
                    policy=FactorStockFeaturePolicy(
                        implementation_sha256=hashlib.sha256(
                            Path(stock_features.__file__).read_bytes()
                        ).hexdigest(),
                        price_adjustment_sha256=hashlib.sha256(
                            Path(price_adjustment.__file__).read_bytes()
                        ).hexdigest(),
                    ),
                    inputs=(inputs,),
                    codes=tuple(codes),
                    input_rows=input_rows,
                    max_input_rows=request.max_input_rows,
                    max_code_observations=request.max_code_observations,
                    max_output_cells=request.max_output_cells,
                )
                fields = dict(
                    schema_version=3,
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
                            (() if base is None else base.fields) + STOCK_FEATURE_FIELDS,
                            key=lambda f: f.column,
                        )
                    ),
                    source_mode="historical_retrospective",
                    value_semantics="stock_features_derived",
                    price_basis="field_specific",
                    recursive_initialization="field_specific",
                    stock_features=history,
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
                            "daily_stock_feature",
                            artifact,
                            stock=True,
                        ),
                    ),
                )
                if base is not None:
                    fields["base_daily_source"] = base
                    if base.technical_history is not None:
                        fields["technical_history"] = base.technical_history
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
    if name in ("STOCK_FEATURE_FIELDS", "FactorStockFeaturePrepareRequest"):
        from rquant.factor import daily_feature_source

        return getattr(daily_feature_source, name)
    raise AttributeError(name)
