"""Original 90-date minute volume profiles, sealed for retrospective factor use."""

from __future__ import annotations

import hashlib
import math
import os
from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime, time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal

import duckdb
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_metadata import DatasetSnapshotArtifact, normalize_utc_datetime, utc_now
from rquant.factor.named_lake import NamedLakeInput, NamedLakeReceipt, materialize_named_lake
from rquant.factor.result_artifact import _open_private_root, _require_same_root, _root_path
from rquant.factor.source_prepare import _check_generation, _generation
from rquant.price_adjustment import PriceFactorUnavailableReason, resolve_price_factor_basis
from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.research_snapshot import materialize_table_dependency, verify_snapshot_artifact
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_dependencies import StrategyTableDependency
from rquant.volume_profile import calculate_volume_profile_from_minutes

if TYPE_CHECKING:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeatureSource,
        FactorVolumeProfilePrepareRequest,
    )

_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
_SHA = r"^[0-9a-f]{64}$"
VolumeProfileColumn = Literal[
    "vp90_vwap",
    "vp90_poc_price",
    "vp90_value_area_low",
    "vp90_value_area_high",
    "vp90_concentration_top5_pct",
    "vp90_below_reference_amount_pct",
    "vp90_above_reference_amount_pct",
    "vp90_below_reference_volume_pct",
    "vp90_above_reference_volume_pct",
    "vp90_total_vol",
    "vp90_total_amount",
]
VolumeProfileReason = (
    PriceFactorUnavailableReason
    | Literal[
        "no_trading_dates",
        "missing_or_invalid_reference_price",
        "missing_minute_data",
        "unmapped_price_basis_ratio",
        "empty_price_bins",
        "non_positive_profile_totals",
        "volume_profile_non_finite",
        "invalid_volume_profile_data",
    ]
)
VOLUME_PROFILE_DESCRIPTIONS = tuple(
    sorted(
        (
            (
                "vp90_vwap",
                "90日成交均价",
                "session_price",
                "原分钟成交近似均价，按参考日复权基准计算；前90个实际日线日期窗口，可用分钟不足90日仍按实际计算。",
            ),
            (
                "vp90_poc_price",
                "90日成交峰值价",
                "session_price",
                "可比成交量最大的价格桶；并列先取距参考价最近，再取较低价。分钟成交近似，价格单位元。",
            ),
            (
                "vp90_value_area_low",
                "90日价值区下沿",
                "session_price",
                "从成交峰值价连续扩展至70%可比成交量的区间下沿；保留原0.5%分桶口径，单位元。",
            ),
            (
                "vp90_value_area_high",
                "90日价值区上沿",
                "session_price",
                "从成交峰值价连续扩展至70%可比成交量的区间上沿；保留原0.5%分桶口径，单位元。",
            ),
            (
                "vp90_concentration_top5_pct",
                "90日成交集中度",
                "percent",
                "成交量最大的5个价格桶占全部可比股数的百分比；0–100原值，非0–1比例。",
            ),
            (
                "vp90_below_reference_amount_pct",
                "参考价下方成交额占比",
                "percent",
                "低于参考价价格桶的原始成交额占比；0–100百分比原值，不复权成交额。",
            ),
            (
                "vp90_above_reference_amount_pct",
                "参考价上方成交额占比",
                "percent",
                "高于参考价价格桶的原始成交额占比；等于参考价不计两侧，0–100百分比原值。",
            ),
            (
                "vp90_below_reference_volume_pct",
                "参考价下方成交量占比",
                "percent",
                "低于参考价价格桶的可比股数占比；价格复权、股数逆向换算，0–100百分比原值。",
            ),
            (
                "vp90_above_reference_volume_pct",
                "参考价上方成交量占比",
                "percent",
                "高于参考价价格桶的可比股数占比；等于参考价不计两侧，0–100百分比原值。",
            ),
            (
                "vp90_total_vol",
                "90日可比成交量",
                "shares",
                "原分钟股数按参考日价格基准逆向换算后求和；单位股。90日指日期窗口，覆盖按实际分钟日数另列。",
            ),
            (
                "vp90_total_amount",
                "90日原始成交额",
                "CNY",
                "日期窗口内原始分钟成交额总和，不复权；单位元。缺失输出不补零，实际覆盖按分钟日数另列。",
            ),
        ),
        key=lambda item: item[0],
    )
)
VOLUME_PROFILE_COLUMNS = tuple(item[0] for item in VOLUME_PROFILE_DESCRIPTIONS)


class FactorVolumeProfilePolicy(BaseModel):
    model_config = _MODEL
    version: Literal["original-volume-profile-v1"] = "original-volume-profile-v1"
    implementation_sha256: str = Field(pattern=_SHA)
    lookback_days: Literal[90] = 90
    frequency: Literal["1min"] = "1min"
    bin_pct: Literal[0.005] = 0.005
    value_area_ratio: Literal[0.7] = 0.7
    evaluation_clock: Literal["previous_complete_sse_session_at_next_day_09:25"] = (
        "previous_complete_sse_session_at_next_day_09:25"
    )
    window_selection: Literal["previous_global_actual_daily_dates_strictly_before_reference"] = (
        "previous_global_actual_daily_dates_strictly_before_reference"
    )
    history_mode: Literal["retrospective_no_row_first_observed_time"] = (
        "retrospective_no_row_first_observed_time"
    )
    weight_basis: Literal["adjusted_share_volume"] = "adjusted_share_volume"
    amount_unit: Literal["CNY_unadjusted"] = "CNY_unadjusted"
    minute_price: Literal["positive_amount_divided_by_positive_shares_else_close"] = (
        "positive_amount_divided_by_positive_shares_else_close"
    )
    source_priority: tuple[Literal["tushare", "tushare_rt", "other"], ...] = (
        "tushare",
        "tushare_rt",
        "other",
    )

    @model_validator(mode="after")
    def _priority(self) -> FactorVolumeProfilePolicy:
        if self.source_priority != ("tushare", "tushare_rt", "other"):
            raise ValueError("VP source priority differs from the original reader")
        return self


class FactorVolumeProfileLakeInput(NamedLakeInput):
    @model_validator(mode="after")
    def _minutes(self) -> FactorVolumeProfileLakeInput:
        if any(a.dataset_id != "minute_bar" for a in self.artifacts):
            raise ValueError("VP named lake input must contain minute_bar partitions")
        return self


class FactorVolumeProfileDiagnostic(BaseModel):
    model_config = _MODEL
    reference_date: date
    window_start_date: date | None
    window_end_date: date | None
    window_days: int = Field(ge=0, le=90)
    observed_days: int = Field(ge=0, le=90)
    outside_window_days: int = Field(ge=0, le=4096)
    minute_rows: int = Field(ge=0, le=300_000)
    reference_factor: float | None = Field(
        default=None, allow_inf_nan=False, exclude_if=lambda v: v is None
    )
    unavailable_factor_dates: tuple[date, ...] = Field(default=(), max_length=4096)

    @model_validator(mode="after")
    def _dates(self) -> FactorVolumeProfileDiagnostic:
        if (
            self.observed_days > self.window_days
            or (self.window_days == 0) != (self.window_start_date is None)
            or (self.window_start_date is None) != (self.window_end_date is None)
            or self.window_end_date is not None
            and not self.window_start_date <= self.window_end_date < self.reference_date
            or self.unavailable_factor_dates != tuple(sorted(set(self.unavailable_factor_dates)))
            or any(d > self.reference_date for d in self.unavailable_factor_dates)
        ):
            raise ValueError("VP diagnostic window or price basis dates differ")
        return self


class FactorVolumeProfileSummary(BaseModel):
    model_config = _MODEL
    policy: FactorVolumeProfilePolicy
    input_content_sha256: str = Field(pattern=_SHA)
    output_content_sha256: str = Field(pattern=_SHA)
    input_rows: int = Field(ge=0, le=128_000_000)
    output_rows: int = Field(ge=0, le=28_672_000)
    lake: NamedLakeReceipt | None = Field(default=None, exclude_if=lambda v: v is None)


class FactorVolumeProfileReceipt(BaseModel):
    model_config = _MODEL
    policy: FactorVolumeProfilePolicy
    inputs: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=4, max_length=4)
    artifact: DatasetSnapshotArtifact
    input_rows: int = Field(ge=0, le=128_000_000)
    output_rows: int = Field(ge=0, le=28_672_000)
    profile_evaluations: int = Field(ge=0, le=28_672_000)
    max_input_rows: int = Field(gt=0, le=128_000_000)
    max_code_rows: int = Field(gt=0, le=300_000)
    max_output_cells: int = Field(gt=0, le=128_000_000)
    lake: NamedLakeReceipt | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def _artifacts(self) -> FactorVolumeProfileReceipt:
        if (
            tuple(a.table_name for a in self.inputs)
            != ("vp_date_input", "vp_close_input", "vp_adjustment_input", "vp_minute_input")
            or any(
                a.dataset_id != "factor_volume_profile_input"
                or a.artifact_type != "materialized_table"
                for a in self.inputs
            )
            or sum(a.row_count for a in self.inputs) != self.input_rows
            or self.input_rows > self.max_input_rows
            or self.artifact.dataset_id != "factor_volume_profile_feature"
            or self.artifact.table_name != "daily_volume_profile_feature"
            or self.artifact.artifact_type != "materialized_table"
            or self.artifact.row_count != self.output_rows
            or self.profile_evaluations != self.output_rows
            or self.artifact.primary_key != ("ts_code", "trade_date")
            or self.lake is not None
            and self.lake.artifact.table_name != "volume_profile_partition_manifest"
        ):
            raise ValueError("VP receipt differs from sealed input and output")
        return self

    def summary(self) -> FactorVolumeProfileSummary:
        return FactorVolumeProfileSummary(
            policy=self.policy,
            input_content_sha256=canonical_sha256(self.inputs),
            output_content_sha256=self.artifact.content_hash,
            input_rows=self.input_rows,
            output_rows=self.output_rows,
            lake=self.lake,
        )

    def causal_policy(self) -> FactorVolumeProfilePolicy:
        return self.policy


def _stage_inputs(
    connection: duckdb.DuckDBPyConnection,
    request: FactorVolumeProfilePrepareRequest,
    *,
    scratch: Path,
    root: Path,
) -> tuple[tuple[DatasetSnapshotArtifact, ...], NamedLakeReceipt | None]:
    scope = request.prepared_source.receipt.request.scope
    panels = request.prepared_source.receipt.calendar_open_days
    first, last = (panels[0], panels[-1]) if panels else (scope.start_date, scope.end_date)
    connection.execute("CREATE TEMP TABLE vp_date_input(trade_date DATE PRIMARY KEY)")
    connection.execute(
        "INSERT INTO vp_date_input SELECT DISTINCT trade_date FROM daily_bar "
        "WHERE trade_date<? ORDER BY trade_date DESC LIMIT 90",
        [first],
    )
    connection.execute(
        "INSERT INTO vp_date_input SELECT DISTINCT trade_date FROM daily_bar "
        "WHERE trade_date>=? AND trade_date<?",
        [first, last],
    )
    dates = tuple(
        r[0]
        for r in connection.execute(
            "SELECT trade_date FROM vp_date_input ORDER BY trade_date"
        ).fetchall()
    )
    start = dates[0] if dates else first
    connection.execute(
        "CREATE TEMP TABLE vp_close_input(ts_code VARCHAR,trade_date DATE,close DOUBLE,"
        "PRIMARY KEY(ts_code,trade_date))"
    )
    connection.execute(
        "CREATE TEMP TABLE vp_adjustment_input(ts_code VARCHAR,trade_date DATE,"
        "adj_factor DOUBLE,PRIMARY KEY(ts_code,trade_date))"
    )
    connection.execute(
        "CREATE TEMP TABLE vp_minute_input(ts_code VARCHAR,trade_time TIMESTAMP,"
        "trade_date DATE,freq VARCHAR,close DOUBLE,vol DOUBLE,amount DOUBLE,source VARCHAR,"
        "PRIMARY KEY(ts_code,trade_time,freq,source))"
    )
    staged = len(dates)
    if staged > request.max_input_rows:
        raise ValueError("VP complete input row budget exceeded")
    for table, select, args in (
        (
            "vp_close_input",
            "SELECT ts_code,trade_date,close FROM daily_bar "
            "WHERE ts_code IN (SELECT unnest(?)) AND trade_date IN (SELECT unnest(?))",
            [list(scope.stock_codes), list(panels)],
        ),
        (
            "vp_adjustment_input",
            "SELECT ts_code,trade_date,adj_factor FROM adj_factor "
            "WHERE ts_code IN (SELECT unnest(?)) AND trade_date BETWEEN ? AND ?",
            [list(scope.stock_codes), start, last],
        ),
    ):
        rows = connection.execute("SELECT count(*) FROM (" + select + ")", args).fetchone()[0]
        staged += rows
        if staged > request.max_input_rows:
            raise ValueError("VP complete input row budget exceeded")
        connection.execute("INSERT INTO " + table + " " + select, args)
    begin, finish = datetime.combine(start, time(9, 30)), datetime.combine(last, time(15))
    lake = None
    if request.lake_input is not None:
        lake = materialize_named_lake(
            request.lake_input,
            connection,
            scratch,
            output_root=root,
            as_of=scope.as_of_time,
            dataset_id="factor_volume_profile_manifest",
            table_name="volume_profile_partition_manifest",
        )
        sources = tuple(
            (
                a,
                verify_snapshot_artifact(
                    a, lake_root=request.lake_input.lake_root, as_of_time=scope.as_of_time
                ),
            )
            for a in request.lake_input.artifacts
        )
    else:
        sources = ((None, None),)
    for artifact, path in sources:
        source = "minute_bar" if path is None else "read_parquet(?,hive_partitioning=false)"
        select = (
            "SELECT ts_code,trade_time,cast(trade_time AS DATE),"
            "'1min',close,vol,amount,source FROM "
            + source
            + " WHERE freq='1min' AND ts_code IN (SELECT unnest(?)) AND trade_time BETWEEN ? AND ?"
        )
        args = ([] if path is None else [str(path)]) + [list(scope.stock_codes), begin, finish]
        rows = connection.execute("SELECT count(*) FROM (" + select + ")", args).fetchone()[0]
        staged += rows
        if staged > request.max_input_rows:
            raise ValueError("VP complete input row budget exceeded")
        connection.execute("INSERT INTO vp_minute_input " + select, args)
        if artifact is not None:
            verify_snapshot_artifact(
                artifact, lake_root=request.lake_input.lake_root, as_of_time=scope.as_of_time
            )
    maximum = connection.execute(
        "SELECT coalesce(max(n),0) FROM (SELECT count(*) n FROM vp_minute_input GROUP BY ts_code)"
    ).fetchone()[0]
    if maximum > request.max_code_rows:
        raise ValueError("VP per-code row budget exceeded")
    artifacts = []
    for table in ("vp_date_input", "vp_close_input", "vp_adjustment_input", "vp_minute_input"):
        earliest = connection.execute("SELECT min(trade_date) FROM " + table).fetchone()[0] or start
        artifacts.append(
            materialize_table_dependency(
                connection,
                dependency=StrategyTableDependency(
                    dataset_id="factor_volume_profile_input",
                    table_name=table,
                    date_column="trade_date",
                ),
                artifact_root=root,
                start_date=earliest,
                end_date=last,
                as_of_time=scope.as_of_time,
            )
        )
    return tuple(artifacts), lake


def _profile_values(
    code: str,
    panel: date,
    dates: list[date],
    ref: float | None,
    minutes: pd.DataFrame,
    factors: dict[date, float | None],
) -> tuple[tuple[float | None, ...], str | None, FactorVolumeProfileDiagnostic]:
    used = (
        minutes.iloc[:0]
        if not dates
        else minutes.loc[
            (minutes.trade_time >= datetime.combine(dates[0], time(9, 30)))
            & (minutes.trade_time <= datetime.combine(dates[-1], time(15)))
        ]
    )
    observed = set(pd.to_datetime(used.trade_time).dt.date.tolist())
    diag = dict(
        reference_date=panel,
        window_start_date=dates[0] if dates else None,
        window_end_date=dates[-1] if dates else None,
        window_days=len(dates),
        observed_days=len(observed & set(dates)),
        outside_window_days=len(observed - set(dates)),
        minute_rows=len(used),
    )
    reason, profile = None, None
    if not dates:
        reason = "no_trading_dates"
    elif ref is None or not math.isfinite(ref) or ref <= 0:
        reason = "missing_or_invalid_reference_price"
    elif used.empty:
        reason = "missing_minute_data"
    else:
        basis = resolve_price_factor_basis(
            required_dates=observed, factor_by_date=factors, reference_date=panel
        )
        if basis.reference_factor is not None and math.isfinite(basis.reference_factor):
            diag["reference_factor"] = basis.reference_factor
        diag["unavailable_factor_dates"] = basis.unavailable_dates
        if not basis.available:
            reason = basis.unavailable_reason
        else:
            try:
                outcome = calculate_volume_profile_from_minutes(
                    code,
                    reference_date=panel,
                    lookback_days=90,
                    dates=dates,
                    ref_price=ref,
                    minutes=used,
                    price_basis=basis,
                )
                profile, reason = outcome.profile, outcome.reason
            except (ValueError, ArithmeticError):
                reason = "invalid_volume_profile_data"
    values = tuple(
        None if profile is None else float(getattr(profile, c.removeprefix("vp90_")))
        for c in VOLUME_PROFILE_COLUMNS
    )
    if any(v is not None and not math.isfinite(v) for v in values):
        values, reason = (None,) * len(values), "volume_profile_non_finite"
    return values, reason, FactorVolumeProfileDiagnostic(**diag)


def _derive(
    connection: duckdb.DuckDBPyConnection, request: FactorVolumeProfilePrepareRequest, *, root: Path
) -> DatasetSnapshotArtifact:
    scope = request.prepared_source.receipt.request.scope
    panels = request.prepared_source.receipt.calendar_open_days
    global_dates = [
        r[0]
        for r in connection.execute(
            "SELECT trade_date FROM vp_date_input ORDER BY trade_date"
        ).fetchall()
    ]
    windows = {panel: [d for d in global_dates if d < panel][-90:] for panel in panels}
    columns = ",".join(c + " DOUBLE," + c + "__reason VARCHAR" for c in VOLUME_PROFILE_COLUMNS)
    connection.execute(
        "CREATE TEMP TABLE daily_volume_profile_feature(ts_code VARCHAR,trade_date DATE,"
        + columns
        + ",volume_profile_diagnostic VARCHAR,PRIMARY KEY(ts_code,trade_date))"
    )
    insert = (
        "INSERT INTO daily_volume_profile_feature VALUES("
        + ",".join("?" for _ in range(3 + 2 * len(VOLUME_PROFILE_COLUMNS)))
        + ")"
    )
    for code in scope.stock_codes:
        raw = connection.execute(
            "SELECT trade_time,close,vol,amount FROM vp_minute_input WHERE ts_code=? "
            "QUALIFY ROW_NUMBER() OVER(PARTITION BY trade_time,freq "
            "ORDER BY CASE source WHEN 'tushare' THEN 0 WHEN 'tushare_rt' THEN 1 ELSE 2 END)=1 "
            "ORDER BY trade_time",
            [code],
        ).fetchmany(request.max_code_rows + 1)
        if len(raw) > request.max_code_rows:
            raise ValueError("VP per-code row budget exceeded")
        minutes = pd.DataFrame(raw, columns=("trade_time", "close", "vol", "amount"))
        minutes["trade_time"] = pd.to_datetime(minutes["trade_time"])
        factors = dict(
            connection.execute(
                "SELECT trade_date,adj_factor FROM vp_adjustment_input "
                "WHERE ts_code=? ORDER BY trade_date",
                [code],
            ).fetchall()
        )
        closes = dict(
            connection.execute(
                "SELECT trade_date,close FROM vp_close_input WHERE ts_code=?", [code]
            ).fetchall()
        )
        rows = []
        for panel in panels:
            values, reason, diag = _profile_values(
                code, panel, windows[panel], closes.get(panel), minutes, factors
            )
            rows.append(
                (
                    code,
                    panel,
                    *(v for value in values for v in (value, reason)),
                    diag.model_dump_json(),
                )
            )
        if rows:
            connection.executemany(insert, rows)
    return materialize_table_dependency(
        connection,
        dependency=StrategyTableDependency(
            dataset_id="factor_volume_profile_feature",
            table_name="daily_volume_profile_feature",
            date_column="trade_date",
        ),
        artifact_root=root,
        start_date=scope.start_date,
        end_date=scope.end_date,
        as_of_time=scope.as_of_time,
    )


def prepare_factor_volume_profile_source(
    request: FactorVolumeProfilePrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorDailyFeatureSource:
    from rquant.factor.daily_feature_source import (
        VOLUME_PROFILE_FIELDS,
        FactorDailyFeatureSource,
        FactorVolumeProfilePrepareRequest,
        open_factor_daily_feature_source,
    )

    request = FactorVolumeProfilePrepareRequest.model_validate(request)
    prepared, base = request.prepared_source, request.base_daily_source
    original, scope = prepared.receipt.request, prepared.receipt.request.scope
    if base is not None:
        if base.schema_version == 7:
            raise ValueError("VP source cannot extend a VP source")
        base.require_prepared(prepared)
        with open_factor_daily_feature_source(base, lake_root=lake_root):
            pass
    width = len(VOLUME_PROFILE_COLUMNS) + (0 if base is None else len(base.fields))
    if (
        len(scope.stock_codes) * len(prepared.receipt.calendar_open_days) * width
        > request.max_output_cells
    ):
        raise ValueError("VP output cell budget exceeded")
    generation = _generation(original)
    if generation != prepared.receipt.generation:
        raise ValueError("VP generation differs from paired prices")
    root = _root_path(lake_root)
    if root.is_relative_to(original.replica_path.parent):
        raise ValueError("VP lake must be outside replica directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_fd = _open_private_root(root)
    try:
        with TemporaryDirectory(prefix=".vp-prepare-", dir=root) as scratch:
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
                inputs, lake = _stage_inputs(connection, request, scratch=Path(scratch), root=root)
                artifact = _derive(connection, request, root=root)
                policy = FactorVolumeProfilePolicy(
                    implementation_sha256=hashlib.sha256(
                        Path(__file__).read_bytes()
                        + Path(__file__).parents[1].joinpath("volume_profile.py").read_bytes()
                    ).hexdigest()
                )
                receipt = FactorVolumeProfileReceipt(
                    policy=policy,
                    inputs=inputs,
                    artifact=artifact,
                    input_rows=sum(a.row_count for a in inputs),
                    output_rows=artifact.row_count,
                    profile_evaluations=artifact.row_count,
                    max_input_rows=request.max_input_rows,
                    max_code_rows=request.max_code_rows,
                    max_output_cells=request.max_output_cells,
                    lake=lake,
                )
                fields = dict(
                    schema_version=7,
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
                            (() if base is None else base.fields) + VOLUME_PROFILE_FIELDS,
                            key=lambda f: f.column,
                        )
                    ),
                    value_semantics="volume_profile_derived",
                    price_basis="field_specific",
                    recursive_initialization="field_specific",
                    source_mode="historical_retrospective",
                    source_read_boundary="single_snapshot_transaction",
                    volume_profile=receipt,
                    read_mode=mode,
                    observed_at=observed,
                    completed_read_at=normalize_utc_datetime(now()),
                    tables=() if base is None else base.tables,
                )
                if base is not None:
                    fields["base_daily_source"] = base
                    for key in (
                        "technical_history",
                        "stock_features",
                        "minute_features",
                        "market_temperature",
                        "auction",
                    ):
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
    if name == "FactorVolumeProfilePrepareRequest":
        from rquant.factor.daily_feature_source import FactorVolumeProfilePrepareRequest

        return FactorVolumeProfilePrepareRequest
    raise AttributeError(name)
