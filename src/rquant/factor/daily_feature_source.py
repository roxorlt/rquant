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
from typing import Annotated, Literal

import duckdb
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.data_metadata import DatasetSnapshotArtifact, normalize_utc_datetime, utc_now
from rquant.factor.auction_source import (
    AUCTION_COLUMNS,
    AUCTION_DESCRIPTIONS,
    AuctionColumn,
    AuctionReason,
    FactorAuctionDiagnostic,
    FactorAuctionLakeInput,
    FactorAuctionReceipt,
    FactorAuctionSummary,
)
from rquant.factor.market_temperature_source import (
    MARKET_TEMPERATURE_COLUMNS,
    MARKET_TEMPERATURE_DESCRIPTIONS,
    FactorMarketTemperatureReceipt,
    FactorMarketTemperatureSummary,
    MarketTemperatureColumn,
    MarketTemperatureReason,
    _observe_market_temperature,
)
from rquant.factor.minute_feature_source import (
    MINUTE_FEATURE_COLUMNS,
    MINUTE_FEATURE_DESCRIPTIONS,
    FactorMinuteFeatureCode,
    FactorMinuteFeatureDiagnostic,
    FactorMinuteFeaturePolicy,
    FactorMinuteFeatureReceipt,
    FactorMinuteFeatureSummary,
    MinuteFeatureColumn,
    MinuteFeatureReason,
    _verify_minute_feature_input,
)
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
from rquant.factor.stock_feature_source import (
    STOCK_FEATURE_COLUMNS,
    STOCK_FEATURE_DESCRIPTIONS,
    FactorStockFeatureCode,
    FactorStockFeatureDiagnostic,
    FactorStockFeaturePolicy,
    FactorStockFeatureReceipt,
    FactorStockFeatureSummary,
    StockFeatureColumn,
    StockFeatureReason,
    _verify_stock_feature_input,
)
from rquant.factor.technical_history_source import (
    TECHNICAL_COLUMNS,
    FactorTechnicalHistoryCode,
    FactorTechnicalHistoryPolicy,
    FactorTechnicalHistoryReceipt,
    FactorTechnicalHistorySummary,
    TechnicalHistoryReason,
    _InputIdentity,
    _verify_technical_history_input,
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
DailyInventoryColumn = Literal[
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
DailyStoredColumn = (
    DailyInventoryColumn
    | StockFeatureColumn
    | MinuteFeatureColumn
    | MarketTemperatureColumn
    | AuctionColumn
)
DailyFeatureStatus = Literal["valid", "missing", "null", "non_finite"]


class FactorDailyStoredField(BaseModel):
    model_config = _MODEL
    column: DailyStoredColumn
    table: Literal[
        "daily_indicator",
        "daily_basic",
        "daily_stock_feature",
        "daily_minute_feature",
        "market_temperature_daily",
        "daily_auction_feature",
    ]
    name_zh: str
    unit: Literal[
        "stored_price",
        "session_price",
        "indicator",
        "percent",
        "ratio",
        "CNY_10000",
        "CNY",
        "observations",
        "binary",
    ]
    description_zh: str
    value_semantics: (
        Literal[
            "history_derived",
            "stock_features_derived",
            "minute_features_derived",
            "market_temperature_stored",
            "auction_derived",
        ]
        | None
    ) = Field(default=None, exclude_if=lambda v: v is None)


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
DERIVED_DAILY_FIELDS = tuple(
    FactorDailyStoredField.model_validate(
        {
            **field.model_dump(),
            "unit": "session_price" if field.unit == "stored_price" else field.unit,
            "value_semantics": "history_derived",
            "description_zh": (
                "从首个有效历史观察推导，按观察日复权因子计算、当日因子还原价格。"
                if field.unit == "stored_price"
                else "从首个有效历史观察推导；历史断裂不重置，KDJ J保留原值。"
                if field.column == "kdj_j"
                else "从首个有效历史观察推导；历史断裂不重置。"
            ),
        }
    )
    if field.column in TECHNICAL_COLUMNS
    else field
    for field in STORED_DAILY_FIELDS
)
_DERIVED_FIELDS = {field.column: field for field in DERIVED_DAILY_FIELDS}
STOCK_FEATURE_FIELDS = tuple(
    FactorDailyStoredField(
        column=c,
        table="daily_stock_feature",
        name_zh=n,
        unit=u,
        description_zh=d,
        value_semantics="stock_features_derived",
    )
    for c, n, u, d in STOCK_FEATURE_DESCRIPTIONS
)
_STOCK_FIELDS = {f.column: f for f in STOCK_FEATURE_FIELDS}
MINUTE_FEATURE_FIELDS = tuple(
    FactorDailyStoredField(
        column=c,
        table="daily_minute_feature",
        name_zh=n,
        unit=u,
        description_zh=d,
        value_semantics="minute_features_derived",
    )
    for c, n, u, d in MINUTE_FEATURE_DESCRIPTIONS
)
_MINUTE_FIELDS = {f.column: f for f in MINUTE_FEATURE_FIELDS}
MARKET_TEMPERATURE_FIELDS = tuple(
    FactorDailyStoredField(
        column=c,
        table="market_temperature_daily",
        name_zh=n,
        unit="percent",
        description_zh=d,
        value_semantics="market_temperature_stored",
    )
    for c, n, d in MARKET_TEMPERATURE_DESCRIPTIONS
)
_MARKET_FIELDS = {f.column: f for f in MARKET_TEMPERATURE_FIELDS}
AUCTION_FIELDS = tuple(
    FactorDailyStoredField(
        column=c,
        table="daily_auction_feature",
        name_zh=n,
        unit=u,
        description_zh=d,
        value_semantics="auction_derived",
    )
    for c, n, u, d in AUCTION_DESCRIPTIONS
)
_AUCTION_FIELDS = {f.column: f for f in AUCTION_FIELDS}
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
    reasons: tuple[FactorDailyFeatureReasonCount, ...] = Field(
        default=(),
        max_length=5,
        exclude_if=lambda v: not v,
        # A schema default makes generated clients require this omitted empty array.
        json_schema_extra=lambda schema: schema.pop("default", None),
    )
    stock_reasons: tuple[FactorDailyFeatureReasonCount, ...] = Field(
        default=(),
        max_length=9,
        exclude_if=lambda v: not v,
        json_schema_extra=lambda schema: schema.pop("default", None),
    )

    minute_reasons: tuple[FactorDailyFeatureReasonCount, ...] = Field(
        default=(),
        max_length=8,
        exclude_if=lambda v: not v,
        json_schema_extra=lambda schema: schema.pop("default", None),
    )
    market_reasons: tuple[FactorDailyFeatureReasonCount, ...] = Field(
        default=(),
        max_length=4,
        exclude_if=lambda v: not v,
        json_schema_extra=lambda schema: schema.pop("default", None),
    )
    auction_reasons: tuple[FactorDailyFeatureReasonCount, ...] = Field(
        default=(),
        max_length=10,
        exclude_if=lambda v: not v,
        json_schema_extra=lambda schema: schema.pop("default", None),
    )

    @model_validator(mode="after")
    def _reason_family(self) -> FactorDailyFeatureCounts:
        family = (
            "auction"
            if self.column in AUCTION_COLUMNS
            else "market"
            if self.column in MARKET_TEMPERATURE_COLUMNS
            else "minute"
            if self.column in MINUTE_FEATURE_COLUMNS
            else "stock"
            if self.column in STOCK_FEATURE_COLUMNS
            else "technical"
        )
        if (
            (family != "technical" and self.reasons)
            or (family != "stock" and self.stock_reasons)
            or (family != "minute" and self.minute_reasons)
            or (family != "market" and self.market_reasons)
            or (family != "auction" and self.auction_reasons)
        ):
            raise ValueError("daily coverage reasons differ from the field family")
        return self


class FactorDailyFeatureReasonCount(BaseModel):
    model_config = _MODEL
    reason: (
        TechnicalHistoryReason
        | StockFeatureReason
        | MinuteFeatureReason
        | MarketTemperatureReason
        | AuctionReason
    )
    count: int = Field(gt=0)


class FactorDailyFeatureTable(BaseModel):
    model_config = _MODEL
    table_name: Literal[
        "daily_indicator", "daily_basic", "daily_stock_feature", "daily_minute_feature"
    ]
    artifact: DatasetSnapshotArtifact
    row_count: int = Field(ge=0)
    code_counts: tuple[FactorSourceCodeCount, ...] = Field(min_length=1, max_length=7000)
    date_counts: tuple[FactorSourceDateCount, ...] = Field(min_length=1, max_length=4096)
    counts: tuple[FactorDailyFeatureCounts, ...] = Field(min_length=1, max_length=23)
    structural_missing_rows: int = Field(ge=0)
    rows_on_closed_dates: int = Field(ge=0)


_ObservationCount = Annotated[int, Field(ge=0, le=50_000)]
_TechnicalCodeRow = tuple[
    _ObservationCount,
    date | None,
    date | None,
    date | None,
    _ObservationCount,
    date | None,
    Literal["invalid_ohlc", "invalid_factor", "non_finite_adjusted_price"] | None,
]
_StockCodeRow = tuple[_ObservationCount, _ObservationCount, date | None, date | None]
_CodeCountRow = tuple[Annotated[int, Field(ge=0, le=4096)]]


class _V3TechnicalReceipt(BaseModel):
    model_config = _MODEL
    code_format: Literal["scope_ordered_rows_v1"] = "scope_ordered_rows_v1"
    policy: FactorTechnicalHistoryPolicy
    inputs: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1, max_length=1)
    # Parent scope supplies stock_code; the remaining seven fields retain their original order.
    codes: tuple[_TechnicalCodeRow, ...] = Field(min_length=1, max_length=7000)
    input_rows: int = Field(ge=0, le=16_000_000)
    max_input_rows: int = Field(gt=0, le=16_000_000)
    max_code_observations: int = Field(gt=0, le=50_000)
    max_output_cells: int = Field(gt=0, le=64_000_000)


class _V3StockReceipt(BaseModel):
    model_config = _MODEL
    code_format: Literal["scope_ordered_rows_v1"] = "scope_ordered_rows_v1"
    policy: FactorStockFeaturePolicy
    inputs: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1, max_length=1)
    # Parent scope supplies stock_code; the remaining four fields include both raw boundaries.
    codes: tuple[_StockCodeRow, ...] = Field(min_length=1, max_length=7000)
    input_rows: int = Field(ge=0, le=16_000_000)
    max_input_rows: int = Field(gt=0, le=16_000_000)
    max_code_observations: int = Field(gt=0, le=50_000)
    max_output_cells: int = Field(gt=0, le=64_000_000)


class _V3Table(BaseModel):
    model_config = _MODEL
    code_counts_format: Literal["scope_ordered_rows_v1"] = "scope_ordered_rows_v1"
    table_name: Literal[
        "daily_indicator", "daily_basic", "daily_stock_feature", "daily_minute_feature"
    ]
    artifact: DatasetSnapshotArtifact
    row_count: int = Field(ge=0)
    code_counts: tuple[_CodeCountRow, ...] = Field(min_length=1, max_length=7000)
    date_counts: tuple[FactorSourceDateCount, ...] = Field(min_length=1, max_length=4096)
    counts: tuple[FactorDailyFeatureCounts, ...] = Field(min_length=1, max_length=23)
    structural_missing_rows: int = Field(ge=0)
    rows_on_closed_dates: int = Field(ge=0)


class _V3BaseReference(BaseModel):
    model_config = _MODEL
    representation: Literal["shared_parent_v1"] = "shared_parent_v1"
    schema_version: Literal[1, 2]
    sha256: str = Field(pattern=_SHA)
    read_mode: Literal["descriptor", "in_place"]
    observed_at: datetime
    completed_read_at: datetime


_DateIndex = Annotated[int, Field(ge=0, le=27999)]
_V4TechnicalRow = tuple[
    _ObservationCount,
    _DateIndex | None,
    _DateIndex | None,
    _DateIndex | None,
    _ObservationCount,
    _DateIndex | None,
    Literal["invalid_ohlc", "invalid_factor", "non_finite_adjusted_price"] | None,
]
_V4StockRow = tuple[_ObservationCount, _ObservationCount, _DateIndex | None, _DateIndex | None]
_MinuteRowCount = Annotated[int, Field(ge=0, le=300_000)]
_V4MinuteRow = tuple[_MinuteRowCount, _MinuteRowCount, _DateIndex | None, _DateIndex | None]


class _V4TechnicalReceipt(_V3TechnicalReceipt):
    code_format: Literal["scope_ordered_date_indices_v1"] = "scope_ordered_date_indices_v1"
    dates: tuple[date, ...] = Field(max_length=28000)
    codes: tuple[_V4TechnicalRow, ...] = Field(min_length=1, max_length=7000)


class _V4StockReceipt(_V3StockReceipt):
    code_format: Literal["scope_ordered_date_indices_v1"] = "scope_ordered_date_indices_v1"
    dates: tuple[date, ...] = Field(max_length=14000)
    codes: tuple[_V4StockRow, ...] = Field(min_length=1, max_length=7000)


class _V4MinuteReceipt(BaseModel):
    model_config = _MODEL
    code_format: Literal["scope_ordered_date_indices_v1"] = "scope_ordered_date_indices_v1"
    policy: FactorMinuteFeaturePolicy
    inputs: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1, max_length=1)
    dates: tuple[date, ...] = Field(max_length=14000)
    codes: tuple[_V4MinuteRow, ...] = Field(min_length=1, max_length=7000)
    input_rows: int = Field(ge=0, le=128_000_000)
    max_input_rows: int = Field(gt=0, le=128_000_000)
    max_code_rows: int = Field(gt=0, le=300_000)
    max_output_cells: int = Field(gt=0, le=128_000_000)


class _V4BaseReference(_V3BaseReference):
    schema_version: Literal[1, 2, 3]
    base_daily_source: _V3BaseReference | None = Field(default=None, exclude_if=lambda v: v is None)


class _MarketBaseReference(_V3BaseReference):
    schema_version: Literal[1, 2, 3, 4]
    base_daily_source: _MarketBaseReference | None = Field(
        default=None, exclude_if=lambda v: v is None
    )


class _AuctionBaseReference(_V3BaseReference):
    schema_version: Literal[1, 2, 3, 4, 5]
    base_daily_source: _AuctionBaseReference | None = Field(
        default=None, exclude_if=lambda v: v is None
    )


def _v4_receipt_wire(
    value: BaseModel, wire_type: type[BaseModel], date_positions: tuple[int, ...]
) -> BaseModel:
    keys = tuple(type(value.codes[0]).model_fields)[1:]
    rows = tuple(tuple(getattr(code, key) for key in keys) for code in value.codes)
    dates = tuple(sorted({row[i] for row in rows for i in date_positions if row[i] is not None}))
    index = {d: i for i, d in enumerate(dates)}
    fields = value.model_dump(exclude={"codes"})
    fields.update(
        dates=dates,
        codes=tuple(
            tuple(
                index[v] if i in date_positions and v is not None else v for i, v in enumerate(row)
            )
            for row in rows
        ),
    )
    return wire_type.model_validate(fields)


def _v4_receipt_expand(
    wire: BaseModel,
    code_type: type[BaseModel],
    receipt_type: type[BaseModel],
    date_positions: tuple[int, ...],
    info: ValidationInfo,
) -> BaseModel:
    if wire.dates != tuple(sorted(set(wire.dates))):
        raise ValueError("compact date dictionary repeats or changes order")
    used = {row[i] for row in wire.codes for i in date_positions if row[i] is not None}
    if used != set(range(len(wire.dates))):
        raise ValueError("compact date dictionary has missing, extra or out-of-range entries")
    fields = wire.model_dump(exclude={"code_format", "dates", "codes"})
    keys = tuple(code_type.model_fields)[1:]
    fields["codes"] = tuple(
        code_type(
            stock_code=code,
            **dict(
                zip(
                    keys,
                    tuple(
                        wire.dates[v] if i in date_positions and v is not None else v
                        for i, v in enumerate(row)
                    ),
                    strict=True,
                )
            ),
        )
        for code, row in zip(_scope_row_codes(wire.codes, info), wire.codes, strict=True)
    )
    return receipt_type.model_validate(fields)


def _validate_v3_wire(model: type[BaseModel], value: object, info: ValidationInfo) -> BaseModel:
    if info.mode == "json":
        return model.model_validate_json(TypeAdapter(object).dump_json(value))
    return model.model_validate(value)


def _scope_row_codes(rows: tuple[object, ...], info: ValidationInfo) -> tuple[StockCode, ...]:
    scope = info.data.get("scope")
    if not isinstance(scope, FactorComputationScope) or len(rows) != len(scope.stock_codes):
        raise ValueError("compact rows differ from the complete validated code scope")
    return scope.stock_codes


class FactorDailyFeatureSources(BaseModel):
    """The actual dependency subset, omitted entirely for original six-field definitions."""

    model_config = _MODEL
    source_sha256: str = Field(pattern=_SHA)
    prepared_source_sha256: str = Field(pattern=_SHA)
    prepared_snapshot_id: str
    prepared_binding_hash: str = Field(pattern=_SHA)
    scope_content_hash: str = Field(pattern=_SHA)
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    fields: tuple[FactorDailyStoredField, ...] = Field(min_length=1, max_length=52)
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    value_semantics: Literal[
        "stored_not_recomputed",
        "history_derived",
        "stock_features_derived",
        "minute_features_derived",
        "market_temperature_stored",
        "auction_derived",
    ] = "stored_not_recomputed"
    price_basis: Literal[
        "unverified", "observation_factor_then_output_session_scale", "field_specific"
    ] = "unverified"
    recursive_initialization: Literal[
        "unverified", "first_valid_observation_no_restart", "field_specific"
    ] = "unverified"
    technical_history: FactorTechnicalHistorySummary | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    stock_features: FactorStockFeatureSummary | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    minute_features: FactorMinuteFeatureSummary | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    market_temperature: FactorMarketTemperatureSummary | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    auction: FactorAuctionSummary | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def _fields(self) -> FactorDailyFeatureSources:
        columns = tuple(field.column for field in self.fields)
        minute = self.minute_features is not None
        stock = self.stock_features is not None
        technical = self.technical_history is not None
        market = self.market_temperature is not None
        auction = self.auction is not None
        semantic = (
            "auction_derived"
            if auction
            else "market_temperature_stored"
            if market
            else "minute_features_derived"
            if minute
            else "stock_features_derived"
            if stock
            else "history_derived"
            if technical
            else "stored_not_recomputed"
        )
        catalog = {
            **(_DERIVED_FIELDS if technical else _FIELDS),
            **(_STOCK_FIELDS if stock else {}),
            **(_MINUTE_FIELDS if minute else {}),
            **(_MARKET_FIELDS if market else {}),
            **(_AUCTION_FIELDS if auction else {}),
        }
        if (
            self.value_semantics != semantic
            or (minute and not set(columns) & set(MINUTE_FEATURE_COLUMNS))
            or (stock and not set(columns) & set(STOCK_FEATURE_COLUMNS))
            or (market and not set(columns) & set(MARKET_TEMPERATURE_COLUMNS))
            or (auction and not set(columns) & set(AUCTION_COLUMNS))
            or self.price_basis
            != (
                "field_specific"
                if minute or stock or market or auction
                else "observation_factor_then_output_session_scale"
                if technical
                else "unverified"
            )
            or self.recursive_initialization
            != (
                "field_specific"
                if minute or stock or market or auction
                else "first_valid_observation_no_restart"
                if technical
                else "unverified"
            )
            or columns != tuple(sorted(set(columns)))
            or sum(c not in MARKET_TEMPERATURE_COLUMNS for c in columns) > 50
            or any(catalog.get(f.column) != f for f in self.fields)
        ):
            raise ValueError("daily field contract differs from actual source contract")
        return self


class FactorDailyFeatureSource(BaseModel):
    model_config = _MODEL
    schema_version: Literal[1, 2, 3, 4, 5, 6] = 1
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
    value_semantics: Literal[
        "stored_not_recomputed",
        "history_derived",
        "stock_features_derived",
        "minute_features_derived",
        "market_temperature_stored",
        "auction_derived",
    ] = "stored_not_recomputed"
    price_basis: Literal[
        "unverified", "observation_factor_then_output_session_scale", "field_specific"
    ] = "unverified"
    recursive_initialization: Literal[
        "unverified", "first_valid_observation_no_restart", "field_specific"
    ] = "unverified"
    technical_history: FactorTechnicalHistoryReceipt | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    stock_features: FactorStockFeatureReceipt | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    minute_features: FactorMinuteFeatureReceipt | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    market_temperature: FactorMarketTemperatureReceipt | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    auction: FactorAuctionReceipt | None = Field(default=None, exclude_if=lambda v: v is None)
    source_read_boundary: Literal["single_snapshot_transaction"] = "single_snapshot_transaction"
    read_mode: Literal["descriptor", "in_place"]
    observed_at: datetime
    completed_read_at: datetime
    tables: tuple[FactorDailyFeatureTable, ...] = Field(max_length=4)
    sha256: str = Field(pattern=_SHA)

    # Its wire reference uses the already validated shared fields and tables.
    base_daily_source: FactorDailyFeatureSource | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @field_validator(
        "technical_history",
        mode="before",
        json_schema_input_type=FactorTechnicalHistoryReceipt
        | _V3TechnicalReceipt
        | _V4TechnicalReceipt
        | None,
    )
    @classmethod
    def _expand_technical_wire(cls, value: object, info: ValidationInfo) -> object:
        if isinstance(value, dict) and "code_format" in value:
            if info.data.get("schema_version") in (4, 5, 6):
                wire = _validate_v3_wire(_V4TechnicalReceipt, value, info)
                return _v4_receipt_expand(
                    wire,
                    FactorTechnicalHistoryCode,
                    FactorTechnicalHistoryReceipt,
                    (1, 2, 3, 5),
                    info,
                )
            if info.data.get("schema_version") != 3:
                raise ValueError("compact technical receipt requires a v3 source")
            wire = _validate_v3_wire(_V3TechnicalReceipt, value, info)
            expanded = wire.model_dump(exclude={"code_format", "codes"})
            expanded["codes"] = tuple(
                FactorTechnicalHistoryCode(
                    stock_code=code,
                    **dict(
                        zip(tuple(FactorTechnicalHistoryCode.model_fields)[1:], row, strict=True)
                    ),
                )
                for code, row in zip(_scope_row_codes(wire.codes, info), wire.codes, strict=True)
            )
            return FactorTechnicalHistoryReceipt.model_validate(expanded)
        if value is not None and info.mode == "json":
            return _validate_v3_wire(FactorTechnicalHistoryReceipt, value, info)
        return value

    @field_validator(
        "stock_features",
        mode="before",
        json_schema_input_type=FactorStockFeatureReceipt | _V3StockReceipt | _V4StockReceipt | None,
    )
    @classmethod
    def _expand_stock_wire(cls, value: object, info: ValidationInfo) -> object:
        if isinstance(value, dict) and "code_format" in value:
            if info.data.get("schema_version") in (4, 5, 6):
                wire = _validate_v3_wire(_V4StockReceipt, value, info)
                return _v4_receipt_expand(
                    wire, FactorStockFeatureCode, FactorStockFeatureReceipt, (2, 3), info
                )
            if info.data.get("schema_version") != 3:
                raise ValueError("compact stock receipt requires a v3 source")
            wire = _validate_v3_wire(_V3StockReceipt, value, info)
            expanded = wire.model_dump(exclude={"code_format", "codes"})
            expanded["codes"] = tuple(
                FactorStockFeatureCode(
                    stock_code=code,
                    **dict(zip(tuple(FactorStockFeatureCode.model_fields)[1:], row, strict=True)),
                )
                for code, row in zip(_scope_row_codes(wire.codes, info), wire.codes, strict=True)
            )
            return FactorStockFeatureReceipt.model_validate(expanded)
        if value is not None and info.mode == "json":
            return _validate_v3_wire(FactorStockFeatureReceipt, value, info)
        return value

    @field_validator(
        "minute_features",
        mode="before",
        json_schema_input_type=FactorMinuteFeatureReceipt | _V4MinuteReceipt | None,
    )
    @classmethod
    def _expand_minute_wire(cls, value: object, info: ValidationInfo) -> object:
        if isinstance(value, dict) and "code_format" in value:
            if info.data.get("schema_version") not in (4, 5, 6):
                raise ValueError("compact minute receipt requires a v4 source")
            wire = _validate_v3_wire(_V4MinuteReceipt, value, info)
            return _v4_receipt_expand(
                wire, FactorMinuteFeatureCode, FactorMinuteFeatureReceipt, (2, 3), info
            )
        if value is not None and info.mode == "json":
            return _validate_v3_wire(FactorMinuteFeatureReceipt, value, info)
        return value

    @field_validator(
        "tables",
        mode="before",
        json_schema_input_type=tuple[FactorDailyFeatureTable | _V3Table, ...],
    )
    @classmethod
    def _expand_tables_wire(cls, value: object, info: ValidationInfo) -> object:
        if not isinstance(value, (list, tuple)):
            return value
        tables = []
        for table in value:
            if isinstance(table, dict) and "code_counts_format" in table:
                if info.data.get("schema_version") not in (3, 4, 5, 6):
                    raise ValueError("compact table counts require a v3/v4 source")
                wire = _validate_v3_wire(_V3Table, table, info)
                expanded = wire.model_dump(exclude={"code_counts_format", "code_counts"})
                expanded["code_counts"] = tuple(
                    FactorSourceCodeCount(code=code, count=row[0])
                    for code, row in zip(
                        _scope_row_codes(wire.code_counts, info), wire.code_counts, strict=True
                    )
                )
                table = FactorDailyFeatureTable.model_validate(expanded)
            elif info.mode == "json":
                table = _validate_v3_wire(FactorDailyFeatureTable, table, info)
            tables.append(table)
        return tuple(tables) if info.mode == "json" or isinstance(value, tuple) else value

    @field_validator(
        "base_daily_source",
        mode="before",
        json_schema_input_type="FactorDailyFeatureSource | _V3BaseReference | "
        "_V4BaseReference | _MarketBaseReference | _AuctionBaseReference | None",
    )
    @classmethod
    def _expand_base_wire(cls, value: object, info: ValidationInfo) -> object:
        if isinstance(value, dict) and "representation" in value:
            version = info.data.get("schema_version")
            if version == 6:
                reference = _validate_v3_wire(_AuctionBaseReference, value, info)
                return cls._expand_auction_base(reference, info.data, info.data["tables"])
            if version == 5:
                reference = _validate_v3_wire(_MarketBaseReference, value, info)
                return cls._expand_market_base(reference, info.data, info.data["tables"])
            if version not in (3, 4):
                raise ValueError("shared base reference requires a v3/v4 source")
            reference = _validate_v3_wire(
                _V4BaseReference if version == 4 else _V3BaseReference, value, info
            )
            derived = reference.schema_version == 2
            required = (
                "prepared_source_sha256",
                "prepared_snapshot_id",
                "prepared_binding_hash",
                "scope_content_hash",
                "scope",
                "generation",
                "code_commit",
                "calendar_open_days",
                "tables",
            )
            if any(key not in info.data for key in required):
                raise ValueError("shared base reference has an invalid parent")
            shared = {key: info.data[key] for key in required[:-1]}
            shared.update(
                reference.model_dump(exclude={"representation", "base_daily_source"}),
                fields=DERIVED_DAILY_FIELDS if derived else STORED_DAILY_FIELDS,
                value_semantics="history_derived" if derived else "stored_not_recomputed",
                price_basis="observation_factor_then_output_session_scale"
                if derived
                else "unverified",
                recursive_initialization="first_valid_observation_no_restart"
                if derived
                else "unverified",
                tables=info.data["tables"][:-1],
            )
            if reference.schema_version == 3:
                nested = reference.base_daily_source
                if nested is not None:
                    inner = {key: info.data[key] for key in required[:-1]}
                    inner.update(
                        nested.model_dump(exclude={"representation"}),
                        fields=DERIVED_DAILY_FIELDS
                        if nested.schema_version == 2
                        else STORED_DAILY_FIELDS,
                        value_semantics="history_derived"
                        if nested.schema_version == 2
                        else "stored_not_recomputed",
                        price_basis="observation_factor_then_output_session_scale"
                        if nested.schema_version == 2
                        else "unverified",
                        recursive_initialization="first_valid_observation_no_restart"
                        if nested.schema_version == 2
                        else "unverified",
                        tables=info.data["tables"][:-2],
                    )
                    if nested.schema_version == 2:
                        inner["technical_history"] = info.data.get("technical_history")
                    shared["base_daily_source"] = cls.model_validate(inner)
                shared.update(
                    fields=tuple(
                        sorted(
                            (() if nested is None else shared["base_daily_source"].fields)
                            + STOCK_FEATURE_FIELDS,
                            key=lambda f: f.column,
                        )
                    ),
                    value_semantics="stock_features_derived",
                    price_basis="field_specific",
                    recursive_initialization="field_specific",
                    stock_features=info.data.get("stock_features"),
                )
                if info.data.get("technical_history") is not None:
                    shared["technical_history"] = info.data["technical_history"]
            elif derived:
                shared["technical_history"] = info.data.get("technical_history")
            elif version == 4 and reference.base_daily_source is not None:
                raise ValueError("inventory base reference cannot contain another base")
            # Validate the original v1/v2 digest before accepting the shared representation.
            return cls.model_validate(shared)
        if value is not None and info.mode == "json":
            return _validate_v3_wire(cls, value, info)
        return value

    @classmethod
    def _expand_auction_base(
        cls,
        reference: _AuctionBaseReference,
        shared: dict[str, object],
        tables: tuple[FactorDailyFeatureTable, ...],
    ) -> FactorDailyFeatureSource:
        if reference.schema_version != 5:
            return cls._expand_market_base(
                _MarketBaseReference.model_validate(reference.model_dump()), shared, tables
            )
        fields = {
            key: shared[key]
            for key in (
                "prepared_source_sha256",
                "prepared_snapshot_id",
                "prepared_binding_hash",
                "scope_content_hash",
                "scope",
                "generation",
                "code_commit",
                "calendar_open_days",
            )
        }
        base = None
        if reference.base_daily_source is not None:
            base = cls._expand_auction_base(reference.base_daily_source, shared, tables)
        fields.update(
            reference.model_dump(exclude={"representation", "base_daily_source"}),
            fields=tuple(
                sorted(
                    (() if base is None else base.fields) + MARKET_TEMPERATURE_FIELDS,
                    key=lambda f: f.column,
                )
            ),
            value_semantics="market_temperature_stored",
            price_basis="field_specific",
            recursive_initialization="field_specific",
            tables=tables,
            market_temperature=shared["market_temperature"],
        )
        if base is not None:
            fields["base_daily_source"] = base
            for key in ("technical_history", "stock_features", "minute_features"):
                if getattr(base, key) is not None:
                    fields[key] = getattr(base, key)
        return cls.model_validate(fields)

    @classmethod
    def _expand_market_base(
        cls,
        reference: _MarketBaseReference,
        shared: dict[str, object],
        tables: tuple[FactorDailyFeatureTable, ...],
    ) -> FactorDailyFeatureSource:
        version, nested = reference.schema_version, reference.base_daily_source
        if nested is not None and (version not in (3, 4) or nested.schema_version >= version):
            raise ValueError("market temperature shared base chain is invalid")
        base = None if nested is None else cls._expand_market_base(nested, shared, tables[:-1])
        fields = {
            key: shared[key]
            for key in (
                "prepared_source_sha256",
                "prepared_snapshot_id",
                "prepared_binding_hash",
                "scope_content_hash",
                "scope",
                "generation",
                "code_commit",
                "calendar_open_days",
            )
        }
        fields.update(
            reference.model_dump(exclude={"representation", "base_daily_source"}), tables=tables
        )
        if version in (1, 2):
            fields.update(
                fields=DERIVED_DAILY_FIELDS if version == 2 else STORED_DAILY_FIELDS,
                value_semantics="history_derived" if version == 2 else "stored_not_recomputed",
                price_basis="observation_factor_then_output_session_scale"
                if version == 2
                else "unverified",
                recursive_initialization="first_valid_observation_no_restart"
                if version == 2
                else "unverified",
            )
            if version == 2:
                fields["technical_history"] = shared["technical_history"]
        else:
            fields.update(
                fields=tuple(
                    sorted(
                        (() if base is None else base.fields)
                        + (MINUTE_FEATURE_FIELDS if version == 4 else STOCK_FEATURE_FIELDS),
                        key=lambda f: f.column,
                    )
                ),
                value_semantics="minute_features_derived"
                if version == 4
                else "stock_features_derived",
                price_basis="field_specific",
                recursive_initialization="field_specific",
            )
            fields["minute_features" if version == 4 else "stock_features"] = shared[
                "minute_features" if version == 4 else "stock_features"
            ]
            if base is not None:
                fields["base_daily_source"] = base
                for key in ("technical_history", "stock_features"):
                    if getattr(base, key) is not None:
                        fields[key] = getattr(base, key)
        return cls.model_validate(fields)

    @field_serializer("technical_history", when_used="json")
    def _technical_wire(
        self, value: FactorTechnicalHistoryReceipt | None
    ) -> FactorTechnicalHistoryReceipt | _V3TechnicalReceipt | _V4TechnicalReceipt | None:
        if value is None:
            return value
        if self.schema_version in (4, 5, 6):
            return _v4_receipt_wire(value, _V4TechnicalReceipt, (1, 2, 3, 5))
        if self.schema_version != 3:
            return value
        fields = value.model_dump(exclude={"codes"})
        fields["codes"] = tuple(
            tuple(getattr(code, key) for key in tuple(FactorTechnicalHistoryCode.model_fields)[1:])
            for code in value.codes
        )
        return _V3TechnicalReceipt.model_validate(fields)

    @field_serializer("stock_features", when_used="json")
    def _stock_wire(
        self, value: FactorStockFeatureReceipt | None
    ) -> _V3StockReceipt | _V4StockReceipt | None:
        if value is None:
            return None
        if self.schema_version in (4, 5, 6):
            return _v4_receipt_wire(value, _V4StockReceipt, (2, 3))
        fields = value.model_dump(exclude={"codes"})
        fields["codes"] = tuple(
            tuple(getattr(code, key) for key in tuple(FactorStockFeatureCode.model_fields)[1:])
            for code in value.codes
        )
        return _V3StockReceipt.model_validate(fields)

    @field_serializer("minute_features", when_used="json")
    def _minute_wire(self, value: FactorMinuteFeatureReceipt | None) -> _V4MinuteReceipt | None:
        return None if value is None else _v4_receipt_wire(value, _V4MinuteReceipt, (2, 3))

    @field_serializer("tables", when_used="json")
    def _tables_wire(
        self, value: tuple[FactorDailyFeatureTable, ...]
    ) -> tuple[FactorDailyFeatureTable | _V3Table, ...]:
        if self.schema_version not in (3, 4, 5, 6):
            return value
        return tuple(
            _V3Table(
                **table.model_dump(exclude={"code_counts"}),
                code_counts=tuple((code.count,) for code in table.code_counts),
            )
            for table in value
        )

    @field_serializer("base_daily_source", when_used="json")
    def _base_wire(
        self, value: FactorDailyFeatureSource | None
    ) -> _V3BaseReference | _V4BaseReference | _MarketBaseReference | _AuctionBaseReference | None:
        if value is None:
            return None
        if self.schema_version == 6:
            return self._auction_base_wire(value)
        if self.schema_version == 5:
            return self._market_base_wire(value)
        fields = dict(
            schema_version=value.schema_version,
            sha256=value.sha256,
            read_mode=value.read_mode,
            observed_at=value.observed_at,
            completed_read_at=value.completed_read_at,
        )
        if self.schema_version == 4:
            if value.base_daily_source is not None:
                inner = value.base_daily_source
                fields["base_daily_source"] = _V3BaseReference(
                    schema_version=inner.schema_version,
                    sha256=inner.sha256,
                    read_mode=inner.read_mode,
                    observed_at=inner.observed_at,
                    completed_read_at=inner.completed_read_at,
                )
            return _V4BaseReference(**fields)
        return _V3BaseReference(**fields)

    @classmethod
    def _auction_base_wire(cls, value: FactorDailyFeatureSource) -> _AuctionBaseReference:
        fields = {
            key: getattr(value, key)
            for key in (
                "schema_version",
                "sha256",
                "read_mode",
                "observed_at",
                "completed_read_at",
            )
        }
        if value.base_daily_source is not None:
            fields["base_daily_source"] = cls._auction_base_wire(value.base_daily_source)
        return _AuctionBaseReference(**fields)

    @classmethod
    def _market_base_wire(cls, value: FactorDailyFeatureSource) -> _MarketBaseReference:
        fields = {
            key: getattr(value, key)
            for key in (
                "schema_version",
                "sha256",
                "read_mode",
                "observed_at",
                "completed_read_at",
            )
        }
        if value.base_daily_source is not None:
            fields["base_daily_source"] = cls._market_base_wire(value.base_daily_source)
        return _MarketBaseReference(**fields)

    @field_validator("observed_at", "completed_read_at")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return normalize_utc_datetime(value)

    @model_validator(mode="after")
    def _binding(self) -> FactorDailyFeatureSource:
        dates = _dates(self.scope)
        if (self.schema_version == 6) != (self.auction is not None):
            raise ValueError("auction facts require the explicit v6 source")
        if self.schema_version == 6:
            return self._auction_binding(dates)
        if (self.schema_version == 5) != (self.market_temperature is not None):
            raise ValueError("market temperature requires the explicit v5 source")
        if self.schema_version == 5:
            return self._market_binding(dates)
        minute = self.schema_version == 4
        stock = self.schema_version == 3
        derived = self.schema_version == 2
        extension = minute or stock
        base = self.base_daily_source
        extra = MINUTE_FEATURE_FIELDS if minute else STOCK_FEATURE_FIELDS
        expected_fields = (
            tuple(sorted((() if base is None else base.fields) + extra, key=lambda f: f.column))
            if extension
            else DERIVED_DAILY_FIELDS
            if derived
            else STORED_DAILY_FIELDS
        )
        expected_tables = (
            (() if base is None else tuple(t.table_name for t in base.tables))
            + (("daily_minute_feature",) if minute else ("daily_stock_feature",))
            if extension
            else _TABLES
        )
        receipt = (
            self.minute_features
            if minute
            else self.stock_features
            if stock
            else self.technical_history
            if derived
            else None
        )
        if (
            minute != (self.minute_features is not None)
            or (self.stock_features is not None)
            != (stock or (minute and base is not None and base.stock_features is not None))
            or (not extension and base is not None)
            or (
                base is not None
                and (
                    base.schema_version not in ((1, 2, 3) if minute else (1, 2))
                    or self.technical_history != base.technical_history
                    or (minute and self.stock_features != base.stock_features)
                )
            )
            or (extension and base is None and self.technical_history is not None)
            or (not extension and derived != (self.technical_history is not None))
            or self.value_semantics
            != (
                "minute_features_derived"
                if minute
                else "stock_features_derived"
                if stock
                else "history_derived"
                if derived
                else "stored_not_recomputed"
            )
            or self.price_basis
            != (
                "field_specific"
                if extension
                else "observation_factor_then_output_session_scale"
                if derived
                else "unverified"
            )
            or self.recursive_initialization
            != (
                "field_specific"
                if extension
                else "first_valid_observation_no_restart"
                if derived
                else "unverified"
            )
            or (
                receipt is not None
                and (
                    tuple(c.stock_code for c in receipt.codes) != self.scope.stock_codes
                    or len(self.scope.stock_codes) * len(dates) * len(expected_fields)
                    > receipt.max_output_cells
                )
            )
            or self.fields != expected_fields
            or self.completed_read_at < self.observed_at
            or self.calendar_open_days != tuple(sorted(set(self.calendar_open_days)))
            or not set(self.calendar_open_days) <= set(dates)
            or tuple(t.table_name for t in self.tables) != expected_tables
        ):
            raise ValueError("daily feature source contract, clock or calendar mismatch")
        if base is not None:
            for key in (
                "prepared_source_sha256",
                "prepared_snapshot_id",
                "prepared_binding_hash",
                "scope_content_hash",
                "scope",
                "generation",
                "code_commit",
                "calendar_open_days",
            ):
                if getattr(base, key) != getattr(self, key):
                    raise ValueError("feature base differs from paired prepared source")
            if self.tables[:-1] != base.tables or self.observed_at < base.completed_read_at:
                raise ValueError("feature base table or read boundary differs")
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

    def _auction_binding(self, dates: tuple[date, ...]) -> FactorDailyFeatureSource:
        base, receipt = self.base_daily_source, self.auction
        if (
            self.fields
            != tuple(
                sorted(
                    (() if base is None else base.fields) + AUCTION_FIELDS, key=lambda f: f.column
                )
            )
            or self.value_semantics != "auction_derived"
            or self.price_basis != "field_specific"
            or self.recursive_initialization != "field_specific"
            or self.completed_read_at < self.observed_at
            or self.calendar_open_days != tuple(sorted(set(self.calendar_open_days)))
            or not set(self.calendar_open_days) <= set(dates)
            or self.tables != (() if base is None else base.tables)
            or receipt.output_rows != len(self.scope.stock_codes) * len(self.calendar_open_days)
            or receipt.output_rows * len(self.fields) > receipt.max_output_cells
            or receipt.artifact.earliest_time
            != (self.calendar_open_days[0].isoformat() if self.calendar_open_days else None)
            or receipt.artifact.latest_time
            != (self.calendar_open_days[-1].isoformat() if self.calendar_open_days else None)
            or self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"}))
        ):
            raise ValueError("auction source contract, clock, scope or digest mismatch")
        for key in ("technical_history", "stock_features", "minute_features", "market_temperature"):
            if getattr(self, key) != (None if base is None else getattr(base, key)):
                raise ValueError("auction base receipt differs")
        if base is not None:
            if base.schema_version == 6 or self.observed_at < base.completed_read_at:
                raise ValueError("auction base read boundary differs")
            for key in (
                "prepared_source_sha256",
                "prepared_snapshot_id",
                "prepared_binding_hash",
                "scope_content_hash",
                "scope",
                "generation",
                "code_commit",
                "calendar_open_days",
            ):
                if getattr(base, key) != getattr(self, key):
                    raise ValueError("auction base differs from paired prepared source")
        return self

    def _market_binding(self, dates: tuple[date, ...]) -> FactorDailyFeatureSource:
        base, receipt = self.base_daily_source, self.market_temperature
        expected_fields = tuple(
            sorted(
                (() if base is None else base.fields) + MARKET_TEMPERATURE_FIELDS,
                key=lambda f: f.column,
            )
        )
        if (
            self.fields != expected_fields
            or self.value_semantics != "market_temperature_stored"
            or self.price_basis != "field_specific"
            or self.recursive_initialization != "field_specific"
            or self.completed_read_at < self.observed_at
            or self.calendar_open_days != tuple(sorted(set(self.calendar_open_days)))
            or not set(self.calendar_open_days) <= set(dates)
            or tuple(d.date for d in receipt.date_counts) != dates
            or self.tables != (() if base is None else base.tables)
            or self.sha256 != canonical_sha256(self.model_dump(exclude={"sha256"}))
        ):
            raise ValueError("market temperature source contract, clock, scope or digest mismatch")
        for key in ("technical_history", "stock_features", "minute_features"):
            if getattr(self, key) != (None if base is None else getattr(base, key)):
                raise ValueError("market temperature base receipt differs")
        if base is not None:
            if base.schema_version == 5 or self.observed_at < base.completed_read_at:
                raise ValueError("market temperature base read boundary differs")
            for key in (
                "prepared_source_sha256",
                "prepared_snapshot_id",
                "prepared_binding_hash",
                "scope_content_hash",
                "scope",
                "generation",
                "code_commit",
                "calendar_open_days",
            ):
                if getattr(base, key) != getattr(self, key):
                    raise ValueError("market temperature base differs from paired prepared source")
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
        available = {f.column: f for f in self.fields}
        selected = tuple(sorted(set(columns)))
        if not selected or any(c not in available for c in selected):
            raise ValueError("daily feature column is absent from the actual sealed source")
        minute = bool(set(selected) & set(MINUTE_FEATURE_COLUMNS))
        stock = bool(set(selected) & set(STOCK_FEATURE_COLUMNS))
        technical = self.technical_history is not None and bool(
            set(selected) & set(TECHNICAL_COLUMNS)
        )
        market = bool(set(selected) & set(MARKET_TEMPERATURE_COLUMNS))
        auction = bool(set(selected) & set(AUCTION_COLUMNS))
        return FactorDailyFeatureSources(
            source_sha256=self.sha256,
            prepared_source_sha256=self.prepared_source_sha256,
            prepared_snapshot_id=self.prepared_snapshot_id,
            prepared_binding_hash=self.prepared_binding_hash,
            scope_content_hash=self.scope_content_hash,
            code_commit=self.code_commit,
            fields=tuple(available[c] for c in selected),
            value_semantics="auction_derived"
            if auction
            else "market_temperature_stored"
            if market
            else "minute_features_derived"
            if minute
            else "stock_features_derived"
            if stock
            else "history_derived"
            if technical
            else "stored_not_recomputed",
            price_basis="field_specific"
            if stock or minute or market or auction
            else "observation_factor_then_output_session_scale"
            if technical
            else "unverified",
            recursive_initialization="field_specific"
            if stock or minute or market or auction
            else "first_valid_observation_no_restart"
            if technical
            else "unverified",
            technical_history=self.technical_history.summary() if technical else None,
            stock_features=self.stock_features.summary() if stock else None,
            minute_features=self.minute_features.summary() if minute else None,
            market_temperature=self.market_temperature.summary() if market else None,
            auction=self.auction.summary() if auction else None,
        )

    def input_artifacts(self) -> tuple[DatasetSnapshotArtifact, ...]:
        return (
            tuple(t.artifact for t in self.tables)
            + (() if self.technical_history is None else self.technical_history.inputs)
            + (() if self.stock_features is None else self.stock_features.inputs)
            + (() if self.minute_features is None else self.minute_features.inputs)
            + (() if self.market_temperature is None else (self.market_temperature.artifact,))
            + (
                ()
                if self.auction is None
                else self.auction.inputs
                + (self.auction.artifact,)
                + (() if self.auction.lake is None else (self.auction.lake.artifact,))
            )
        )


class FactorStockFeaturePrepareRequest(BaseModel):
    model_config = _MODEL
    prepared_source: FactorPreparedStreamSource
    base_daily_source: FactorDailyFeatureSource | None = None
    max_input_rows: int = Field(default=16_000_000, gt=0, le=16_000_000)
    max_code_observations: int = Field(default=50_000, gt=0, le=50_000)
    max_output_cells: int = Field(default=32_000_000, gt=0, le=64_000_000)


class FactorMinuteFeaturePrepareRequest(BaseModel):
    model_config = _MODEL
    prepared_source: FactorPreparedStreamSource
    base_daily_source: FactorDailyFeatureSource | None = None
    max_input_rows: int = Field(default=64_000_000, gt=0, le=128_000_000)
    max_code_rows: int = Field(default=300_000, gt=0, le=300_000)
    max_output_cells: int = Field(default=32_000_000, gt=0, le=128_000_000)


class FactorMarketTemperaturePrepareRequest(BaseModel):
    model_config = _MODEL
    prepared_source: FactorPreparedStreamSource
    base_daily_source: FactorDailyFeatureSource | None = None


class FactorAuctionPrepareRequest(BaseModel):
    model_config = _MODEL
    prepared_source: FactorPreparedStreamSource
    base_daily_source: FactorDailyFeatureSource | None = None
    lake_input: FactorAuctionLakeInput | None = None
    max_input_rows: int = Field(default=16_000_000, gt=0, le=16_000_000)
    max_output_cells: int = Field(default=32_000_000, gt=0, le=128_000_000)


class FactorDailyFeatureQuery(BaseModel):
    model_config = _MODEL
    source_sha256: str = Field(pattern=_SHA)
    trade_date: date
    stock_codes: tuple[StockCode, ...] = Field(min_length=1, max_length=500)
    fields: tuple[DailyStoredColumn, ...] = Field(min_length=1, max_length=50)

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
    reason: (
        TechnicalHistoryReason
        | StockFeatureReason
        | MinuteFeatureReason
        | MarketTemperatureReason
        | AuctionReason
        | None
    ) = Field(default=None, exclude_if=lambda v: v is None)

    diagnostic: FactorStockFeatureDiagnostic | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    minute_diagnostic: FactorMinuteFeatureDiagnostic | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    auction_diagnostic: FactorAuctionDiagnostic | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @model_validator(mode="after")
    def _value(self) -> FactorDailyFeatureValue:
        if (self.status == "valid") != (self.value is not None) or (
            self.status == "non_finite"
        ) != (self.non_finite_value is not None):
            raise ValueError("daily feature value and missing state differ")
        if self.reason is not None and (
            self.status == "valid"
            or (
                self.reason
                in ("derived_non_finite", "market_temperature_non_finite", "auction_non_finite")
            )
            != (self.status == "non_finite")
        ):
            raise ValueError("technical reason differs from missing value status")
        return self


class FactorDailyFeatureFact(FactorDailyFeatureValue):
    stock_code: StockCode
    trade_date: date
    column: DailyStoredColumn

    @model_validator(mode="after")
    def _auction_fact(self) -> FactorDailyFeatureFact:
        if self.column not in AUCTION_COLUMNS:
            if self.auction_diagnostic is not None:
                raise ValueError("auction diagnostic attached to another field family")
            return self
        diagnostic = self.auction_diagnostic
        expected = {
            "valid": (None,),
            "missing": (
                "missing_board_membership",
                "missing_board_auction",
                "missing_previous_close",
                "missing_auction_history",
            ),
            "null": (
                "zero_auction_baseline",
                "auction_null",
                "invalid_auction_price",
                "invalid_auction_amount",
                "invalid_previous_close",
            ),
            "non_finite": ("auction_non_finite",),
        }
        if self.reason not in expected[self.status]:
            raise ValueError("auction reason differs from original value status")
        if (
            (self.status != "valid" and self.reason is None)
            or (
                self.status == "valid"
                and (
                    self.column == "board_gap_up_ratio"
                    and not 0 <= self.value <= 1
                    or self.column == "board_auction_amount_ratio"
                    and self.value < 0
                    or self.column == "board_member_count"
                    and (
                        self.value < 1
                        or not self.value.is_integer()
                        or diagnostic is not None
                        and self.value != diagnostic.member_count
                    )
                )
            )
            or diagnostic is not None
            and (
                diagnostic.membership_date is not None
                and diagnostic.membership_date >= self.trade_date
                or diagnostic.previous_close_date is not None
                and diagnostic.previous_close_date >= self.trade_date
            )
        ):
            raise ValueError("auction value, date, member count or missing reason differs")
        return self


class FactorMarketTemperatureDayValue(FactorDailyFeatureValue):
    column: MarketTemperatureColumn
    reason: MarketTemperatureReason | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def _market_reason(self) -> FactorMarketTemperatureDayValue:
        expected = {
            "valid": (None,),
            "missing": ("missing_market_temperature",),
            "null": ("market_temperature_null", "invalid_market_percentage"),
            "non_finite": ("market_temperature_non_finite",),
        }
        if self.reason not in expected[self.status] or (
            self.status == "valid" and not 0 <= self.value <= 100
        ):
            raise ValueError(
                "market daily value differs from original percentage or missing reason"
            )
        return self


class FactorDailyFeatureInputRow(BaseModel):
    model_config = _MODEL
    stock_code: StockCode
    values: tuple[FactorDailyFeatureValue, ...] = Field(max_length=50)


class FactorAuctionPreviewValue(FactorDailyFeatureValue):
    column: AuctionColumn
    reason: AuctionReason | None = Field(default=None, exclude_if=lambda v: v is None)


class FactorAuctionPreviewStock(BaseModel):
    model_config = _MODEL
    stock_code: StockCode
    values: tuple[FactorAuctionPreviewValue, ...] = Field(min_length=1, max_length=3)
    diagnostic: FactorAuctionDiagnostic

    @model_validator(mode="after")
    def _columns(self) -> FactorAuctionPreviewStock:
        columns = tuple(v.column for v in self.values)
        if columns != tuple(sorted(set(columns))) or any(
            v.auction_diagnostic is not None
            or v.diagnostic is not None
            or v.minute_diagnostic is not None
            for v in self.values
        ):
            raise ValueError("auction preview repeats fields or duplicates shared diagnostic")
        return self


MAX_AUCTION_PREVIEW_DAYS = 32


class FactorDailyFeatureInput(BaseModel):
    """Stock values follow stock_fields; shared market values follow selected market fields."""

    model_config = _MODEL
    sources: FactorDailyFeatureSources
    trade_date: date
    panel_date: date
    rows: tuple[FactorDailyFeatureInputRow, ...] = Field(min_length=1, max_length=7000)
    counts: tuple[FactorDailyFeatureCounts, ...] = Field(min_length=1, max_length=52)
    sha256: str = Field(pattern=_SHA)
    market_values: tuple[FactorDailyFeatureValue, ...] | None = Field(
        default=None,
        min_length=1,
        max_length=2,
        exclude_if=lambda v: v is None,
    )

    @property
    def stock_fields(self) -> tuple[FactorDailyStoredField, ...]:
        return tuple(f for f in self.sources.fields if f.column not in MARKET_TEMPERATURE_COLUMNS)

    def market_value(self, column: str) -> FactorDailyFeatureValue | None:
        fields = tuple(f for f in self.sources.fields if f.column in MARKET_TEMPERATURE_COLUMNS)
        if self.market_values is not None:
            return next(
                (v for f, v in zip(fields, self.market_values, strict=True) if f.column == column),
                None,
            )
        return None

    @property
    def market_temperature_values(self) -> tuple[FactorMarketTemperatureDayValue, ...] | None:
        if self.market_values is None:
            return None
        fields = tuple(f for f in self.sources.fields if f.column in MARKET_TEMPERATURE_COLUMNS)
        return tuple(
            FactorMarketTemperatureDayValue(column=f.column, **v.model_dump())
            for f, v in zip(fields, self.market_values, strict=True)
        )

    @property
    def auction_values(self) -> tuple[FactorAuctionPreviewStock, ...] | None:
        indexes = tuple(
            (i, f.column) for i, f in enumerate(self.stock_fields) if f.column in AUCTION_COLUMNS
        )
        if not indexes:
            return None
        return tuple(
            FactorAuctionPreviewStock(
                stock_code=row.stock_code,
                values=tuple(
                    FactorAuctionPreviewValue(
                        column=column, **row.values[i].model_dump(exclude={"auction_diagnostic"})
                    )
                    for i, column in indexes
                ),
                diagnostic=row.values[indexes[0][0]].auction_diagnostic,
            )
            for row in self.rows[:10]
        )

    @model_validator(mode="after")
    def _grid(self) -> FactorDailyFeatureInput:
        columns = tuple(f.column for f in self.stock_fields)
        codes = tuple(r.stock_code for r in self.rows)
        if any(len(r.values) != len(columns) for r in self.rows):
            raise ValueError("daily feature original input row width differs")
        market = tuple(
            f.column for f in self.sources.fields if f.column in MARKET_TEMPERATURE_COLUMNS
        )
        if (
            bool(market) != (self.market_values is not None)
            or market
            and len(market) != len(self.market_values)
            or (self.sources.market_temperature is not None) != bool(market)
        ):
            raise ValueError("market temperature witness differs from selected source fields")
        expected = _input_counts(self.sources, self.rows, self.market_values)
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
    stock_columns = tuple(c for c in columns if c not in MARKET_TEMPERATURE_COLUMNS)
    market_columns = tuple(c for c in columns if c in MARKET_TEMPERATURE_COLUMNS)
    rows, market_values = [], None
    if market_columns:
        batch = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=sources.source_sha256,
                trade_date=panel_date,
                stock_codes=stock_codes[:1],
                fields=market_columns,
            )
        )
        market_values = tuple(
            FactorDailyFeatureValue(**f.model_dump(exclude={"stock_code", "trade_date", "column"}))
            for f in batch.facts
        )
        del batch
    for start in range(0, len(stock_codes), 500):
        if not stock_columns:
            rows.extend(
                FactorDailyFeatureInputRow(stock_code=c, values=())
                for c in stock_codes[start : start + 500]
            )
            continue
        batch = lease.query(
            FactorDailyFeatureQuery(
                source_sha256=sources.source_sha256,
                trade_date=panel_date,
                stock_codes=stock_codes[start : start + 500],
                fields=stock_columns,
            )
        )
        for i, code in enumerate(batch.query.stock_codes):
            rows.append(
                FactorDailyFeatureInputRow(
                    stock_code=code,
                    values=tuple(
                        FactorDailyFeatureValue(
                            status=f.status,
                            value=f.value,
                            non_finite_value=f.non_finite_value,
                            reason=f.reason,
                            diagnostic=f.diagnostic,
                            minute_diagnostic=f.minute_diagnostic,
                            auction_diagnostic=f.auction_diagnostic,
                        )
                        for f in batch.facts[i * len(stock_columns) : (i + 1) * len(stock_columns)]
                    ),
                )
            )
        del batch
    fields = dict(
        sources=sources,
        trade_date=trade_date,
        panel_date=panel_date,
        rows=tuple(rows),
        counts=_input_counts(sources, tuple(rows), market_values),
    )
    if market_values is not None:
        fields["market_values"] = market_values
    return FactorDailyFeatureInput(**fields, sha256=canonical_sha256(fields))


def _input_counts(
    sources: FactorDailyFeatureSources,
    rows: tuple[FactorDailyFeatureInputRow, ...],
    market_values: tuple[FactorDailyFeatureValue, ...] | None,
) -> tuple[FactorDailyFeatureCounts, ...]:
    stock = tuple(f.column for f in sources.fields if f.column not in MARKET_TEMPERATURE_COLUMNS)
    market = tuple(f.column for f in sources.fields if f.column in MARKET_TEMPERATURE_COLUMNS)
    counts = []
    for field in sources.fields:
        values = (
            (market_values[market.index(field.column)],) * len(rows)
            if field.column in market
            else tuple(row.values[stock.index(field.column)] for row in rows)
        )
        tally = Counter(v.status for v in values)
        counts.append(
            FactorDailyFeatureCounts(
                column=field.column,
                **{s: tally[s] for s in ("valid", "missing", "null", "non_finite")},
                **_count_reasons(field.column, values),
            )
        )
    return tuple(counts)


def _counts(
    facts: tuple[FactorDailyFeatureFact, ...], columns: tuple[str, ...]
) -> tuple[FactorDailyFeatureCounts, ...]:
    count = Counter((f.column, f.status) for f in facts)
    return tuple(
        FactorDailyFeatureCounts(
            column=c,
            **{s: count[c, s] for s in ("valid", "missing", "null", "non_finite")},
            **_count_reasons(c, tuple(f for f in facts if f.column == c)),
        )
        for c in columns
    )


def _count_reasons(
    column: str, values: tuple[FactorDailyFeatureValue, ...]
) -> dict[str, tuple[FactorDailyFeatureReasonCount, ...]]:
    return {
        "auction_reasons"
        if column in AUCTION_COLUMNS
        else "market_reasons"
        if column in MARKET_TEMPERATURE_COLUMNS
        else "minute_reasons"
        if column in MINUTE_FEATURE_COLUMNS
        else "stock_reasons"
        if column in STOCK_FEATURE_COLUMNS
        else "reasons": _reason_counts(values)
    }


def _reason_counts(
    values: tuple[FactorDailyFeatureValue, ...],
) -> tuple[FactorDailyFeatureReasonCount, ...]:
    counts = Counter(v.reason for v in values if v.reason is not None)
    return tuple(
        FactorDailyFeatureReasonCount(reason=reason, count=counts[reason])
        for reason in sorted(counts)
    )


class FactorDailyFeatureDayBatch(BaseModel):
    model_config = _MODEL
    source_sha256: str = Field(pattern=_SHA)
    prepared_source_sha256: str = Field(pattern=_SHA)
    query: FactorDailyFeatureQuery
    trade_date: date
    facts: tuple[FactorDailyFeatureFact, ...] = Field(min_length=1, max_length=25000)
    counts: tuple[FactorDailyFeatureCounts, ...] = Field(min_length=1, max_length=50)

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


def _check_schema(
    columns: tuple[tuple[str, str], ...],
    table: str,
    *,
    technical: bool = False,
    stock: bool = False,
    minute: bool = False,
) -> None:
    types = dict(columns)
    catalog = (
        MINUTE_FEATURE_FIELDS if minute else STOCK_FEATURE_FIELDS if stock else STORED_DAILY_FIELDS
    )
    if (
        types.get("ts_code") != "VARCHAR"
        or types.get("trade_date") != "DATE"
        or any(types.get(f.column) != "DOUBLE" for f in catalog if f.table == table)
        or (
            minute
            and (
                types.get("minute_diagnostic") != "VARCHAR"
                or any(types.get(c + "__reason") != "VARCHAR" for c in MINUTE_FEATURE_COLUMNS)
            )
        )
        or (technical and any(types.get(c + "__reason") != "VARCHAR" for c in TECHNICAL_COLUMNS))
        or (
            stock
            and any(
                types.get(c + suffix) != "VARCHAR"
                for c in STOCK_FEATURE_COLUMNS
                for suffix in ("__reason", "__diagnostic")
            )
        )
    ):
        raise ValueError("stored daily source schema differs from declared fields")


def _observe(
    connection: duckdb.DuckDBPyConnection,
    scope: FactorComputationScope,
    open_days: tuple[date, ...],
    table: str,
    artifact: DatasetSnapshotArtifact,
    *,
    technical: bool = False,
    stock: bool = False,
    minute: bool = False,
) -> FactorDailyFeatureTable:
    where = "trade_date BETWEEN ? AND ? AND ts_code IN (SELECT unnest(?))"
    params = [scope.start_date, scope.end_date, list(scope.stock_codes)]
    fields = tuple(
        f.column
        for f in (
            MINUTE_FEATURE_FIELDS
            if minute
            else STOCK_FEATURE_FIELDS
            if stock
            else STORED_DAILY_FIELDS
        )
        if f.table == table
    )
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
                **{
                    "minute_reasons" if minute else "stock_reasons" if stock else "reasons": tuple(
                        FactorDailyFeatureReasonCount(reason=reason, count=int(count))
                        for reason, count in connection.execute(
                            f"SELECT {c}__reason,count(*) FROM {table} WHERE {where} "
                            f"AND {c}__reason IS NOT NULL GROUP BY {c}__reason ORDER BY "
                            f"{c}__reason",
                            params,
                        ).fetchall()
                    )
                    if technical or stock or minute
                    else ()
                },
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
        self._fields = {f.column: f for f in source.fields}

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
            or not set(query.fields) <= self._fields.keys()
            or not self.source.scope.start_date <= query.trade_date <= self.source.scope.end_date
        ):
            raise ValueError("daily feature query exceeds source or scope")
        values, reasons, diagnostics, minute_diagnostics = {}, {}, {}, {}
        auction_diagnostics, auction_tags = {}, {}
        auction_fields = tuple(c for c in query.fields if c in AUCTION_COLUMNS)
        if auction_fields:
            selected = tuple(
                v for c in auction_fields for v in (c, c + "__reason", c + "__non_finite")
            )
            auction_rows = self._connection.execute(
                "SELECT ts_code,"
                + ",".join(selected)
                + ",auction_diagnostic FROM daily_auction_feature "
                "WHERE trade_date=? AND ts_code IN (SELECT unnest(?)) ORDER BY ts_code",
                [query.trade_date, list(query.stock_codes)],
            ).fetchmany(501)
            if len(auction_rows) > len(query.stock_codes) or len(
                {r[0] for r in auction_rows}
            ) != len(auction_rows):
                raise ValueError("auction private query exceeds unique bounded grid")
            for row in auction_rows:
                diagnostic = FactorAuctionDiagnostic.model_validate_json(row[-1])
                for i, column in enumerate(auction_fields):
                    (
                        values[row[0], column],
                        reasons[row[0], column],
                        auction_tags[row[0], column],
                    ) = row[1 + i * 3 : 4 + i * 3]
                    auction_diagnostics[row[0], column] = diagnostic
        market_columns = tuple(c for c in query.fields if c in MARKET_TEMPERATURE_COLUMNS)
        market_row = None
        if market_columns:
            market_rows = self._connection.execute(
                "SELECT "
                + ",".join(c.removeprefix("market_") for c in market_columns)
                + " FROM market_temperature_daily WHERE trade_date=?",
                [query.trade_date],
            ).fetchmany(2)
            if len(market_rows) > 1:
                raise ValueError("duplicate private market temperature date")
            market_row = market_rows[0] if market_rows else None
        for table in (t.table_name for t in self.source.tables):
            fields = tuple(c for c in query.fields if self._fields[c].table == table)
            if not fields:
                continue
            minute = table == "daily_minute_feature"
            stock = table == "daily_stock_feature"
            derived = (
                stock
                or minute
                or table == "daily_indicator"
                and self.source.technical_history is not None
            )
            selected = fields + tuple(c + "__reason" for c in fields) if derived else fields
            if stock:
                selected += tuple(c + "__diagnostic" for c in fields)
            if minute:
                selected += ("minute_diagnostic",)
            rows = self._connection.execute(
                f"SELECT ts_code, {','.join(selected)} FROM {table} "
                "WHERE trade_date=? AND ts_code IN (SELECT unnest(?)) ORDER BY ts_code",
                [query.trade_date, list(query.stock_codes)],
            ).fetchmany(501)
            if len(rows) > len(query.stock_codes) or len({row[0] for row in rows}) != len(rows):
                raise ValueError("daily feature private query exceeds unique bounded grid")
            for row in rows:
                for column, value in zip(fields, row[1 : 1 + len(fields)], strict=True):
                    values[row[0], column] = value
                if derived:
                    reasons.update(
                        ((row[0], c), reason)
                        for c, reason in zip(
                            fields, row[1 + len(fields) : 1 + len(fields) * 2], strict=True
                        )
                    )
                if stock:
                    diagnostics.update(
                        ((row[0], c), FactorStockFeatureDiagnostic.model_validate_json(value))
                        for c, value in zip(fields, row[1 + len(fields) * 2 :], strict=True)
                    )
                if minute:
                    diag = FactorMinuteFeatureDiagnostic.model_validate_json(row[-1])
                    minute_diagnostics.update(((row[0], c), diag) for c in fields)
        facts = []
        for code in query.stock_codes:
            for column in query.fields:
                value = values.get((code, column))
                tag = None
                if column in auction_fields:
                    reason, tag = reasons.get((code, column)), auction_tags.get((code, column))
                    if (code, column) not in values:
                        status = "missing"
                        reasons[code, column] = "missing_board_membership"
                    elif tag:
                        status = "non_finite"
                    elif value is None:
                        status = (
                            "missing"
                            if reason
                            in (
                                "missing_board_membership",
                                "missing_board_auction",
                                "missing_previous_close",
                                "missing_auction_history",
                            )
                            else "null"
                        )
                    else:
                        status = "valid"
                elif column in market_columns:
                    if market_row is None:
                        status, reason = "missing", "missing_market_temperature"
                    else:
                        value = market_row[market_columns.index(column)]
                        if value is None:
                            status, reason = "null", "market_temperature_null"
                        elif not math.isfinite(value):
                            status, reason = "non_finite", "market_temperature_non_finite"
                            tag = (
                                "NaN"
                                if math.isnan(value)
                                else "Infinity"
                                if value > 0
                                else "-Infinity"
                            )
                            value = None
                        elif not 0 <= value <= 100:
                            status, reason, value = "null", "invalid_market_percentage", None
                        else:
                            status, reason = "valid", None
                    reasons[code, column] = reason
                elif (code, column) not in values:
                    status = "missing"
                    if column in MINUTE_FEATURE_COLUMNS:
                        reasons[code, column] = "missing_target_minute"
                    if self.source.technical_history is not None and column in TECHNICAL_COLUMNS:
                        initialized = next(
                            c for c in self.source.technical_history.codes if c.stock_code == code
                        )
                        reasons[code, column] = (
                            "no_initialization"
                            if initialized.first_valid_date is None
                            or query.trade_date < initialized.first_valid_date
                            else "history_break"
                            if initialized.break_date is not None
                            and query.trade_date >= initialized.break_date
                            else "missing_observation"
                        )
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
                        reason=reasons.get((code, column)),
                        diagnostic=diagnostics.get((code, column)),
                        minute_diagnostic=minute_diagnostics.get((code, column)),
                        auction_diagnostic=auction_diagnostics.get((code, column)),
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
    connection, lease, private_descriptor = None, None, None
    technical_identities: dict[str, tuple[_InputIdentity, _InputIdentity]] = {}
    try:
        with TemporaryDirectory(prefix=".daily-feature-reader-", dir=root) as scratch:
            private_root = Path(scratch)
            if (
                source.technical_history is not None
                or source.stock_features is not None
                or source.minute_features is not None
            ):
                private_descriptor = _open_private_root(private_root)
            for artifact in source.input_artifacts():
                technical = artifact.table_name == "technical_history_input"
                stock = artifact.table_name == "stock_feature_input"
                minute = artifact.table_name == "minute_feature_input"
                verifier = (
                    _verify_minute_feature_input
                    if minute
                    else _verify_stock_feature_input
                    if stock
                    else _verify_technical_history_input
                )
                if technical or stock or minute:
                    original, original_identity = verifier(
                        artifact, lake_root=root, as_of_time=source.scope.as_of_time
                    )
                else:
                    original = verify_materialized_table_artifact(
                        artifact, lake_root=root, as_of_time=source.scope.as_of_time
                    )
                target = private_root / artifact.relative_path
                target.parent.mkdir(parents=True)
                shutil.copyfile(original, target)
                os.chmod(target, 0o600)
                if technical or stock or minute:
                    _, copied_identity = verifier(
                        artifact, lake_root=private_root, as_of_time=source.scope.as_of_time
                    )
                    technical_identities[artifact.relative_path] = (
                        original_identity,
                        copied_identity,
                    )
                else:
                    verify_materialized_table_artifact(
                        artifact, lake_root=private_root, as_of_time=source.scope.as_of_time
                    )
            _require_same_root(root, descriptor)
            connection = duckdb.connect(":memory:")
            try:
                connection.execute("SET threads=1")
                connection.execute("SET temp_directory = ?", [scratch])
                if source.auction is not None:
                    receipt = source.auction
                    path = private_root / receipt.artifact.relative_path
                    connection.execute(
                        "CREATE VIEW daily_auction_feature AS SELECT * FROM read_parquet("
                        + _quoted_literal(str(path))
                        + ",hive_partitioning=false)"
                    )
                    columns = tuple(
                        (str(r[0]), str(r[1]))
                        for r in connection.execute("DESCRIBE daily_auction_feature").fetchall()
                    )
                    expected = (
                        (("ts_code", "VARCHAR"), ("trade_date", "DATE"))
                        + tuple(
                            v
                            for c in AUCTION_COLUMNS
                            for v in (
                                (c, "DOUBLE"),
                                (c + "__reason", "VARCHAR"),
                                (c + "__non_finite", "VARCHAR"),
                            )
                        )
                        + (("auction_diagnostic", "VARCHAR"),)
                    )
                    invalid = connection.execute(
                        (
                            "SELECT count(*) FROM daily_auction_feature WHERE ts_code NOT IN (SEL"
                            "ECT unnest(?)) OR trade_date NOT IN (SELECT unnest(?))"
                        ),
                        [list(source.scope.stock_codes), list(source.calendar_open_days)],
                    ).fetchone()[0]
                    duplicates = connection.execute(
                        "SELECT count(*) FROM (SELECT ts_code,trade_date FROM daily_auction_f"
                        "eature GROUP BY ALL HAVING count(*)>1)"
                    ).fetchone()[0]
                    if (
                        columns != expected
                        or invalid
                        or duplicates
                        or connection.execute(
                            "SELECT count(*) FROM daily_auction_feature"
                        ).fetchone()[0]
                        != receipt.output_rows
                    ):
                        raise ValueError("auction private output differs from complete sealed grid")
                if source.market_temperature is not None:
                    receipt = source.market_temperature
                    path = private_root / receipt.artifact.relative_path
                    connection.execute(
                        "CREATE VIEW market_temperature_daily AS SELECT * FROM read_parquet("
                        + _quoted_literal(str(path))
                        + ", hive_partitioning=false)"
                    )
                    columns = tuple(
                        (str(r[0]), str(r[1]))
                        for r in connection.execute("DESCRIBE market_temperature_daily").fetchall()
                    )
                    if (
                        columns
                        != (
                            ("trade_date", "DATE"),
                            ("high_60d_ratio_pct", "DOUBLE"),
                            ("above_ma20_ratio_pct", "DOUBLE"),
                        )
                        or _observe_market_temperature(connection, source.scope, receipt.artifact)
                        != receipt
                    ):
                        raise ValueError("market temperature private original artifact differs")
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
                    derived = (
                        source.technical_history is not None
                        and table.table_name == "daily_indicator"
                    )
                    stock = table.table_name == "daily_stock_feature"
                    minute = table.table_name == "daily_minute_feature"
                    _check_schema(
                        columns, table.table_name, technical=derived, stock=stock, minute=minute
                    )
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
                            technical=derived,
                            stock=stock,
                            minute=minute,
                        )
                        != table
                    ):
                        raise ValueError("private daily feature rows differ from scope or counts")
                if source.technical_history is not None:
                    (artifact,) = source.technical_history.inputs
                    original = private_root / artifact.relative_path
                    connection.execute(
                        "CREATE VIEW technical_history_input AS SELECT * FROM read_parquet("
                        f"{_quoted_literal(str(original))},hive_partitioning=false)"
                    )
                    facts = connection.execute(
                        "SELECT ts_code,count(*),min(trade_date) FROM technical_history_input "
                        "GROUP BY ts_code ORDER BY ts_code"
                    ).fetchall()
                    expected = [
                        (c.stock_code, c.input_observations, c.raw_start_date)
                        for c in source.technical_history.codes
                        if c.input_observations
                    ]
                    if facts != expected:
                        raise ValueError("technical sealed input differs from initialization scope")
                    invalid = connection.execute(
                        "SELECT count(*) FROM technical_history_input WHERE trade_date IS NULL "
                        "OR trade_date>? OR ts_code NOT IN (SELECT unnest(?))",
                        [source.scope.end_date, list(source.scope.stock_codes)],
                    ).fetchone()
                    repeated = connection.execute(
                        "SELECT count(*) FROM (SELECT ts_code,trade_date "
                        "FROM technical_history_input "
                        "GROUP BY ts_code,trade_date HAVING count(*)>1)"
                    ).fetchone()
                    if invalid[0] or repeated[0]:
                        raise ValueError("technical sealed history has invalid keys or dates")
                    source.technical_history.require_sealed_initialization(connection)
                    _require_same_root(private_root, private_descriptor)
                if source.stock_features is not None:
                    (artifact,) = source.stock_features.inputs
                    path = private_root / artifact.relative_path
                    connection.execute(
                        "CREATE VIEW stock_feature_input AS SELECT * FROM read_parquet("
                        + _quoted_literal(str(path))
                        + ",hive_partitioning=false)"
                    )
                    observed = connection.execute(
                        "SELECT ts_code,count(*),count(*) FILTER(WHERE "
                        "bar_present),min(trade_date) FILTER(WHERE "
                        "bar_present),max(trade_date) FILTER(WHERE bar_present) FROM "
                        "stock_feature_input GROUP BY ts_code ORDER BY ts_code"
                    ).fetchall()
                    expected = [
                        (
                            c.stock_code,
                            c.input_rows,
                            c.input_observations,
                            c.raw_start_date,
                            c.raw_end_date,
                        )
                        for c in source.stock_features.codes
                        if c.input_rows
                    ]
                    invalid = connection.execute(
                        "SELECT count(*) FROM stock_feature_input WHERE trade_date IS NULL "
                        "OR trade_date>? OR ts_code NOT IN (SELECT unnest(?))",
                        [source.scope.end_date, list(source.scope.stock_codes)],
                    ).fetchone()[0]
                    if observed != expected or invalid:
                        raise ValueError("stock sealed input differs from bounded scope")
                    _require_same_root(private_root, private_descriptor)
                if source.minute_features is not None:
                    from zoneinfo import ZoneInfo

                    (artifact,) = source.minute_features.inputs
                    path = private_root / artifact.relative_path
                    connection.execute(
                        "CREATE VIEW minute_feature_input AS SELECT * FROM read_parquet("
                        + _quoted_literal(str(path))
                        + ",hive_partitioning=false)"
                    )
                    observed = connection.execute(
                        "SELECT ts_code,count(*),count(DISTINCT "
                        "trade_time),min(trade_date),max(trade_date) FROM "
                        "minute_feature_input GROUP BY ts_code ORDER BY ts_code"
                    ).fetchall()
                    expected = [
                        (
                            c.stock_code,
                            c.input_rows,
                            c.input_observations,
                            c.raw_start_date,
                            c.raw_end_date,
                        )
                        for c in source.minute_features.codes
                        if c.input_rows
                    ]
                    invalid = connection.execute(
                        "SELECT count(*) FROM minute_feature_input WHERE "
                        "trade_time IS NULL OR trade_date IS NULL OR "
                        "trade_date!=CAST(trade_time AS DATE) OR trade_date>? OR "
                        "trade_time>? OR freq!='1min' OR ts_code NOT IN (SELECT "
                        "unnest(?))",
                        [
                            source.scope.end_date,
                            source.scope.as_of_time.astimezone(ZoneInfo("Asia/Shanghai")).replace(
                                tzinfo=None
                            ),
                            list(source.scope.stock_codes),
                        ],
                    ).fetchone()[0]
                    if observed != expected or invalid:
                        raise ValueError(
                            "minute sealed input differs from bounded clock/code scope"
                        )
                    _require_same_root(private_root, private_descriptor)
                lease = FactorDailyFeatureReadLease(source, connection, private_root)
                yield lease
                for artifact in source.input_artifacts():
                    if artifact.relative_path in technical_identities:
                        _require_same_root(private_root, private_descriptor)
                        original_identity, copied_identity = technical_identities[
                            artifact.relative_path
                        ]
                        verifier = (
                            _verify_minute_feature_input
                            if artifact.table_name == "minute_feature_input"
                            else _verify_stock_feature_input
                            if artifact.table_name == "stock_feature_input"
                            else _verify_technical_history_input
                        )
                        verifier(
                            artifact,
                            lake_root=private_root,
                            as_of_time=source.scope.as_of_time,
                            expected_identity=copied_identity,
                        )
                        verifier(
                            artifact,
                            lake_root=root,
                            as_of_time=source.scope.as_of_time,
                            expected_identity=original_identity,
                        )
                    else:
                        verify_materialized_table_artifact(
                            artifact, lake_root=root, as_of_time=source.scope.as_of_time
                        )
                _require_same_root(root, descriptor)
            finally:
                if lease is not None:
                    lease.close()
                else:
                    connection.close()
    finally:
        if private_descriptor is not None:
            os.close(private_descriptor)
        os.close(descriptor)
