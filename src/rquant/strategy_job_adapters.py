"""Typed deterministic shard adapters for Strategy Lab research jobs."""

from __future__ import annotations

import base64
import hashlib
import io
import json
from collections.abc import Iterable, Sequence
from datetime import date, timedelta
from decimal import Decimal
from functools import lru_cache
from typing import Annotated, Literal, Protocol, TypeAlias
from uuid import UUID

import pandas as pd
from pandas.api.types import is_dtype_equal
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from rquant.canonical_json_stream import (
    CanonicalJsonEscapedStringSink,
    CanonicalJsonStreamWriter,
    PandasJsonColumnAccessor,
    write_pandas_json_value,
)
from rquant.lab_shard_protocol import (
    LabShardClaim,
    LabShardClaimV2,
    LabShardDefinition,
    LabShardWorkPlan,
    StrategyShardPayloadV2,
)
from rquant.research_run_spec import (
    FeatureContractIdentity,
    ResearchJobType,
    ResearchRunSpec,
)
from rquant.resource_admission import (
    ResearchAdapterSourceUsage,
    require_research_adapter_source_usage,
)
from rquant.strategy_execution_costs import apply_round_trip_execution_costs
from rquant.strategy_replay_metrics import (
    auction_gap_metric_rows,
    growth_board_metric_rows,
)
from rquant.strict_json import canonical_json_bytes

DATE_BUCKET_DAYS = 20
ADAPTER_VERSION = "1"
EXECUTION_CONTRACT_ID = "strategy-adapter-execution"
EXECUTION_CONTRACT_VERSION = "p13b-adapter-v1"
N_SHAPE_COMPARE_MS_PER_CASE = 30_000
N_SHAPE_OPTIMIZE_MS_PER_CASE = 120_000
AUCTION_GAP_MS_PER_DAY = 30_000
GROWTH_BOARD_SURGE_MS_PER_DAY = 45_000
MAX_RESULT_WIRE_BYTES = 80 * 1024 * 1024
MAX_RESULT_JSON_OVERHEAD_BYTES = 1024 * 1024
MAX_TABLES = 8
MAX_PARQUET_BYTES_PER_TABLE = 32 * 1024 * 1024
_MAX_RESULT_BASE64_BYTES = MAX_RESULT_WIRE_BYTES - MAX_RESULT_JSON_OVERHEAD_BYTES
MAX_AGGREGATE_PARQUET_BYTES = 3 * (_MAX_RESULT_BASE64_BYTES // 4 - MAX_TABLES)
_MAX_RESULT_ENVELOPE_BYTES = 1024
_LEGACY_STRATEGY_ALIASES: dict[
    tuple[str, ResearchJobType],
    tuple[str, ResearchJobType, str, frozenset[str]],
] = {
    ("NShapeCompare", ResearchJobType.STRATEGY_REPLAY): (
        "n_shape",
        ResearchJobType.STRATEGY_REPLAY,
        EXECUTION_CONTRACT_ID,
        frozenset({EXECUTION_CONTRACT_VERSION}),
    ),
    ("NShapeOptimize", ResearchJobType.PARAMETER_SEARCH): (
        "n_shape",
        ResearchJobType.PARAMETER_SEARCH,
        EXECUTION_CONTRACT_ID,
        frozenset({EXECUTION_CONTRACT_VERSION}),
    ),
    ("AuctionGap", ResearchJobType.STRATEGY_REPLAY): (
        "auction_gap",
        ResearchJobType.STRATEGY_REPLAY,
        EXECUTION_CONTRACT_ID,
        frozenset({EXECUTION_CONTRACT_VERSION}),
    ),
    ("GrowthBoardSurge", ResearchJobType.STRATEGY_REPLAY): (
        "growth_board_surge",
        ResearchJobType.STRATEGY_REPLAY,
        EXECUTION_CONTRACT_ID,
        frozenset({EXECUTION_CONTRACT_VERSION}),
    ),
}
_LEGACY_SPARSE_EMPTY_TABLE_COLUMNS: dict[tuple[str, str], tuple[str, ...]] = {
    ("auction-gap", "candidates"): ("signal_date", "ts_code", "name"),
    ("auction-gap", "trades"): (),
    ("growth-board-surge", "trades"): (),
}


def build_adapter_execution_contract(
    adapter_id: str,
    adapter_version: str,
    code_sha: str,
) -> FeatureContractIdentity:
    canonical = json.dumps(
        {
            "adapter_id": adapter_id,
            "adapter_version": adapter_version,
            "code_sha": code_sha,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return FeatureContractIdentity(
        contract_id=EXECUTION_CONTRACT_ID,
        contract_version=EXECUTION_CONTRACT_VERSION,
        contract_hash=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    )


EntryMode: TypeAlias = Literal[
    "first_break",
    "break_retest",
    "late_confirm",
    "vwap_confirm",
    "amount_surge",
    "factor_confirm",
]
ProfileVariant: TypeAlias = Literal["baseline", "vp_risk_only", "vp_90"]
MinuteFreq: TypeAlias = Literal["1min", "5min", "15min", "30min", "60min"]
ScoreProfileName: TypeAlias = Literal[
    "v1",
    "no_intraday",
    "no_accumulation",
    "no_position",
    "no_market",
    "intraday_heavy",
    "accumulation_heavy",
    "position_heavy",
    "v2_low_position",
    "v2_momentum",
    "v2_env_gate",
]
GrowthVariant: TypeAlias = Literal[
    "full",
    "no_vwap",
    "no_same_minute",
    "no_accel_5m",
    "cum_only",
]


class StrategyAdapterModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
        strict=True,
    )


class NShapeCompareParameters(StrategyAdapterModel):
    hold_days: tuple[int, ...]
    entry_modes: tuple[EntryMode, ...]
    profile_variants: tuple[ProfileVariant, ...] = ("baseline",)
    preset_name: Literal[
        "n-shape-pool1",
        "n-shape-pool2",
        "n-shape-combined",
    ] = "n-shape-pool1"
    freq: MinuteFreq = "1min"
    factor_score_threshold: Decimal = Field(default=Decimal("35"), ge=0)

    @model_validator(mode="after")
    def validate_collections(self) -> NShapeCompareParameters:
        if not self.hold_days or any(value < 1 or value > 20 for value in self.hold_days):
            raise ValueError("hold_days must contain values from 1 through 20")
        if not self.entry_modes:
            raise ValueError("entry_modes must not be empty")
        if not self.profile_variants:
            raise ValueError("profile_variants must not be empty")
        return self


class NShapeOptimizeParameters(StrategyAdapterModel):
    hold_days: tuple[int, ...]
    entry_modes: tuple[EntryMode, ...]
    profile_variants: tuple[ProfileVariant, ...]
    preset_name: Literal[
        "n-shape-pool1",
        "n-shape-pool2",
        "n-shape-combined",
    ] = "n-shape-pool1"
    validation_ratio: Decimal = Field(default=Decimal("0.3"), ge=0, lt=1)
    min_trades: int = Field(default=5, ge=1)
    top_n_options: tuple[int, ...] = (1, 2, 3, 5)
    score_profile_names: tuple[ScoreProfileName, ...] = ("v1",)
    walk_forward_folds: int = Field(default=0, ge=0)
    freq: MinuteFreq = "1min"

    @model_validator(mode="after")
    def validate_collections(self) -> NShapeOptimizeParameters:
        if not self.hold_days or any(value < 1 or value > 20 for value in self.hold_days):
            raise ValueError("hold_days must contain values from 1 through 20")
        if not self.entry_modes or not self.profile_variants:
            raise ValueError("entry_modes and profile_variants must not be empty")
        if not self.top_n_options or any(value < 1 for value in self.top_n_options):
            raise ValueError("top_n_options must contain positive values")
        if not self.score_profile_names:
            raise ValueError("score_profile_names must not be empty")
        return self


class AuctionGapParameters(StrategyAdapterModel):
    max_hold_days: int = Field(ge=1, le=10)
    gap_mode: Literal["close", "strict_high"] = "close"
    min_auction_vol_ratio_5d: Decimal = Field(default=Decimal("0.15"), ge=0)
    max_auction_vol_ratio_5d: Decimal = Field(default=Decimal("5"), gt=0)
    st_filter: Literal["case_insensitive", "literal_lower", "none"] = "case_insensitive"
    freq: MinuteFreq = "1min"

    @model_validator(mode="after")
    def validate_ratio_range(self) -> AuctionGapParameters:
        if self.min_auction_vol_ratio_5d > self.max_auction_vol_ratio_5d:
            raise ValueError("auction volume ratio minimum cannot exceed maximum")
        return self


class GrowthBoardSurgeParameters(StrategyAdapterModel):
    variants: tuple[GrowthVariant, ...]
    max_hold_days: int = Field(ge=1, le=10)
    lookback_days: int = Field(default=20, ge=1, le=90)
    min_hist_days: int = Field(default=10, ge=1, le=90)
    min_cum_amount_ratio: Decimal = Field(default=Decimal("1.4"), gt=0)
    min_same_minute_amount_ratio: Decimal = Field(default=Decimal("2"), gt=0)
    min_amount_accel_5m: Decimal = Field(default=Decimal("2"), gt=0)
    require_vwap_strength: bool = True

    @model_validator(mode="after")
    def validate_variants(self) -> GrowthBoardSurgeParameters:
        if not self.variants:
            raise ValueError("variants must not be empty")
        if self.min_hist_days > self.lookback_days:
            raise ValueError("min_hist_days cannot exceed lookback_days")
        return self


class HoldDaysShardInput(StrategyAdapterModel):
    kind: Literal["hold_days"] = "hold_days"
    hold_days: int = Field(ge=1)


class DateBucketShardInput(StrategyAdapterModel):
    kind: Literal["date_bucket"] = "date_bucket"
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_range(self) -> DateBucketShardInput:
        if self.start_date > self.end_date:
            raise ValueError("date bucket start_date cannot follow end_date")
        return self


class GrowthDateVariantShardInput(StrategyAdapterModel):
    kind: Literal["growth_date_variant"] = "growth_date_variant"
    start_date: date
    end_date: date
    variant: GrowthVariant

    @model_validator(mode="after")
    def validate_range(self) -> GrowthDateVariantShardInput:
        if self.start_date > self.end_date:
            raise ValueError("growth bucket start_date cannot follow end_date")
        return self


StrategyShardInput = Annotated[
    HoldDaysShardInput | DateBucketShardInput | GrowthDateVariantShardInput,
    Field(discriminator="kind"),
]


class StrategyShardPayload(StrategyAdapterModel):
    schema_version: Literal[1] = 1
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    spec: ResearchRunSpec
    shard: StrategyShardInput


class ValidatedStrategyShard(StrategyAdapterModel):
    claim: LabShardClaim | LabShardClaimV2
    spec: ResearchRunSpec
    shard: StrategyShardInput


class LabShardMetric(StrategyAdapterModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    value: int | Decimal | str


class LabShardTable(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
        revalidate_instances="always",
    )

    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    frame: pd.DataFrame


class LabShardExecutionResult(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
        revalidate_instances="always",
    )

    shard_id: UUID
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_id: str
    adapter_version: str
    tables: tuple[LabShardTable, ...]
    metrics: tuple[LabShardMetric, ...] = ()

    @model_validator(mode="after")
    def validate_table_names(self) -> LabShardExecutionResult:
        names = tuple(table.name for table in self.tables)
        if not names:
            raise ValueError("shard execution must return at least one table")
        if len(names) != len(set(names)):
            raise ValueError("shard execution table names must be unique")
        metric_names = tuple(metric.name for metric in self.metrics)
        if len(metric_names) != len(set(metric_names)):
            raise ValueError("shard execution metric names must be unique")
        return self

    @classmethod
    def from_validated(
        cls,
        validated: ValidatedStrategyShard,
        *,
        tables: tuple[LabShardTable, ...],
        metrics: tuple[LabShardMetric, ...] = (),
    ) -> LabShardExecutionResult:
        claim = validated.claim
        return cls(
            shard_id=claim.shard_id,
            spec_hash=claim.spec_hash,
            payload_hash=claim.payload_hash,
            plan_hash=claim.plan_hash,
            adapter_id=claim.definition.adapter_id,
            adapter_version=claim.definition.adapter_version,
            tables=tables,
            metrics=metrics,
        )


def shard_wire_base64_size(byte_size: int) -> int:
    if type(byte_size) is not int or byte_size < 0:
        raise ValueError("shard wire byte size must be a non-negative integer")
    return 4 * ((byte_size + 2) // 3)


def validate_shard_wire_capacity(table_sizes: Sequence[int]) -> None:
    table_count = len(table_sizes)
    if table_count < 1 or table_count > MAX_TABLES:
        raise ValueError(f"shard wire result must contain at most {MAX_TABLES} tables")
    aggregate = 0
    for byte_size in table_sizes:
        if type(byte_size) is not int or not 1 <= byte_size <= MAX_PARQUET_BYTES_PER_TABLE:
            raise ValueError("shard wire result exceeds the per-table Parquet byte limit")
        aggregate += byte_size
    if aggregate > MAX_AGGREGATE_PARQUET_BYTES:
        raise ValueError("shard wire result exceeds the aggregate Parquet byte limit")


class _BoundedParquetBuffer(io.BytesIO):
    def write(self, value: bytes, /) -> int:
        if self.tell() + len(value) > MAX_PARQUET_BYTES_PER_TABLE:
            raise ValueError("shard wire result exceeds the per-table Parquet byte limit")
        return super().write(value)


class LabShardWireTable(StrategyAdapterModel):
    name: str = Field(max_length=128, pattern=r"^[a-z][a-z0-9_]*$")
    parquet_base64: str = Field(
        max_length=shard_wire_base64_size(MAX_PARQUET_BYTES_PER_TABLE),
        pattern=r"^[A-Za-z0-9+/]*={0,2}$",
    )
    byte_size: int = Field(ge=1, le=MAX_PARQUET_BYTES_PER_TABLE)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_base64_size(self) -> LabShardWireTable:
        if len(self.parquet_base64) != shard_wire_base64_size(self.byte_size):
            raise ValueError("shard wire table base64 length does not match byte size")
        return self


class LabShardExecutionWireResult(StrategyAdapterModel):
    schema_version: Literal[1] = 1
    shard_id: UUID
    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_id: str = Field(min_length=1, max_length=128)
    adapter_version: str = Field(min_length=1, max_length=128)
    tables: tuple[LabShardWireTable, ...] = Field(min_length=1, max_length=MAX_TABLES)
    metrics: tuple[LabShardMetric, ...] = Field(default=(), max_length=256)

    @model_validator(mode="after")
    def validate_wire_capacity(self) -> LabShardExecutionWireResult:
        validate_shard_wire_capacity(tuple(table.byte_size for table in self.tables))
        names = tuple(table.name for table in self.tables)
        if len(names) != len(set(names)):
            raise ValueError("shard wire result table names must be unique")
        metadata = self.model_dump(mode="json", round_trip=True)
        metadata_tables = metadata["tables"]
        assert isinstance(metadata_tables, list)
        for table in metadata_tables:
            assert isinstance(table, dict)
            table["parquet_base64"] = ""
        metadata_size = len(canonical_json_bytes(metadata))
        if metadata_size + _MAX_RESULT_ENVELOPE_BYTES > MAX_RESULT_JSON_OVERHEAD_BYTES:
            raise ValueError("shard wire result JSON metadata exceeds the size limit")
        encoded_table_size = sum(len(table.parquet_base64) for table in self.tables)
        if encoded_table_size + metadata_size + _MAX_RESULT_ENVELOPE_BYTES > MAX_RESULT_WIRE_BYTES:
            raise ValueError("shard wire result exceeds the total wire size limit")
        return self

    @classmethod
    def from_result(cls, value: LabShardExecutionResult) -> LabShardExecutionWireResult:
        result = LabShardExecutionResult.model_validate(value)
        validate_shard_wire_capacity((1,) * len(result.tables))
        tables: list[LabShardWireTable] = []
        for table in result.tables:
            buffer = _BoundedParquetBuffer()
            table.frame.to_parquet(buffer, index=False)
            payload = buffer.getvalue()
            validate_shard_wire_capacity(
                tuple(existing.byte_size for existing in tables) + (len(payload),)
            )
            tables.append(
                LabShardWireTable(
                    name=table.name,
                    parquet_base64=base64.b64encode(payload).decode("ascii"),
                    byte_size=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
            )
        return cls(
            shard_id=result.shard_id,
            spec_hash=result.spec_hash,
            payload_hash=result.payload_hash,
            plan_hash=result.plan_hash,
            adapter_id=result.adapter_id,
            adapter_version=result.adapter_version,
            tables=tuple(tables),
            metrics=result.metrics,
        )

    def to_result(self) -> LabShardExecutionResult:
        validated = LabShardExecutionWireResult.model_validate(self)
        tables: list[LabShardTable] = []
        for table in validated.tables:
            try:
                payload = base64.b64decode(table.parquet_base64, validate=True)
            except ValueError as exc:
                raise ValueError("shard wire table is not canonical base64") from exc
            if len(payload) != table.byte_size:
                raise ValueError("shard wire table byte size mismatch")
            if hashlib.sha256(payload).hexdigest() != table.sha256:
                raise ValueError("shard wire table hash mismatch")
            tables.append(
                LabShardTable(
                    name=table.name,
                    frame=pd.read_parquet(io.BytesIO(payload)),
                )
            )
        return LabShardExecutionResult(
            shard_id=validated.shard_id,
            spec_hash=validated.spec_hash,
            payload_hash=validated.payload_hash,
            plan_hash=validated.plan_hash,
            adapter_id=validated.adapter_id,
            adapter_version=validated.adapter_version,
            tables=tuple(tables),
            metrics=validated.metrics,
        )


class StrategyAdapterRegistryIdentity(StrategyAdapterModel):
    adapter_id: str
    adapter_version: str


class StrategyAdapterRegistryDescriptor(StrategyAdapterModel):
    schema_version: Literal[1] = 1
    registry_id: Literal["rquant.strategy-adapters.builtin"] = "rquant.strategy-adapters.builtin"
    registry_version: Literal[1] = 1
    adapters: tuple[StrategyAdapterRegistryIdentity, ...]
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class LabJobExecutionResult(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
        revalidate_instances="always",
    )

    spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    adapter_id: str
    adapter_version: str
    tables: tuple[LabShardTable, ...]

    @model_validator(mode="after")
    def validate_table_names(self) -> LabJobExecutionResult:
        names = tuple(table.name for table in self.tables)
        if not names or len(names) != len(set(names)):
            raise ValueError("job execution table names must be nonempty and unique")
        return self

    @property
    def result_hash(self) -> str:
        digest = hashlib.sha256()
        writer = CanonicalJsonStreamWriter(digest.update)

        digest.update(b'{"adapter_id":')
        writer.write_string(self.adapter_id)
        digest.update(b',"adapter_version":')
        writer.write_string(self.adapter_version)
        digest.update(b',"plan_hash":')
        writer.write_string(self.plan_hash)
        digest.update(b',"spec_hash":')
        writer.write_string(self.spec_hash)
        digest.update(b',"tables":[')
        for table_index, table in enumerate(self.tables):
            if table_index:
                digest.update(b",")
            digest.update(b'{"frame":"')
            escaped = CanonicalJsonEscapedStringSink(digest.update)
            inner = CanonicalJsonStreamWriter(escaped.update)
            accessors = tuple(
                PandasJsonColumnAccessor(table.frame.iloc[:, position])
                for position in range(len(table.frame.columns))
            )
            inner.write_ascii(b'{"columns":[')
            for column_index, column in enumerate(table.frame.columns):
                if column_index:
                    inner.write_ascii(b",")
                write_pandas_json_value(
                    inner,
                    column,
                    escape_forward_slash=True,
                    sort_mapping_keys=False,
                )
            inner.write_ascii(b'],"data":[')
            for row_index in range(len(table.frame)):
                if row_index:
                    inner.write_ascii(b",")
                inner.write_ascii(b"[")
                for value_index, accessor in enumerate(accessors):
                    if value_index:
                        inner.write_ascii(b",")
                    accessor.write_pandas_value(
                        inner,
                        row_index,
                        escape_forward_slash=True,
                        sort_mapping_keys=False,
                    )
                inner.write_ascii(b"]")
            inner.write_ascii(b"]}")
            escaped.finish()
            digest.update(b'","name":')
            writer.write_string(table.name)
            digest.update(b"}")
        digest.update(b"]}")
        return digest.hexdigest()


class StrategyJobAdapter(Protocol):
    adapter_id: str
    adapter_version: str
    strategy_name: str
    snapshot_strategy_name: str
    job_type: ResearchJobType

    def source_usage(self) -> ResearchAdapterSourceUsage: ...

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]: ...

    def build_work_plan(
        self,
        spec: ResearchRunSpec,
        shard: StrategyShardInput,
    ) -> LabShardWorkPlan: ...

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult: ...


def _local_snapshot_source_usage(adapter_id: str) -> ResearchAdapterSourceUsage:
    return ResearchAdapterSourceUsage(
        adapter_id=adapter_id,
        external=False,
        immutable_snapshot=True,
        expected_calls=0,
        actual_calls=0,
    )


def _parameter_values(spec: ResearchRunSpec) -> dict[str, object]:
    return {parameter.name: parameter.value for parameter in spec.parameters.arguments}


def _parse_parameters(
    spec: ResearchRunSpec,
    model: type[StrategyAdapterModel],
) -> StrategyAdapterModel:
    try:
        return model.model_validate(_parameter_values(spec))
    except ValidationError as exc:
        raise ValueError(f"invalid {spec.parameters.strategy_name} parameters: {exc}") from exc


def _date_buckets(start_date: date, end_date: date) -> tuple[DateBucketShardInput, ...]:
    buckets: list[DateBucketShardInput] = []
    cursor = start_date
    while cursor <= end_date:
        bucket_end = min(cursor + timedelta(days=DATE_BUCKET_DAYS - 1), end_date)
        buckets.append(DateBucketShardInput(start_date=cursor, end_date=bucket_end))
        cursor = bucket_end + timedelta(days=1)
    return tuple(buckets)


class NShapeCompareAdapter:
    adapter_id = "nshape-compare"
    adapter_version = ADAPTER_VERSION
    strategy_name = "n_shape"
    snapshot_strategy_name = "n_shape"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return _local_snapshot_source_usage(self.adapter_id)

    def parameters(self, spec: ResearchRunSpec) -> NShapeCompareParameters:
        return NShapeCompareParameters.model_validate(
            _parse_parameters(spec, NShapeCompareParameters)
        )

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        parameters = self.parameters(spec)
        return tuple(HoldDaysShardInput(hold_days=value) for value in parameters.hold_days)

    def build_work_plan(
        self,
        spec: ResearchRunSpec,
        shard: StrategyShardInput,
    ) -> LabShardWorkPlan:
        if not isinstance(shard, HoldDaysShardInput):
            raise TypeError("NShapeCompare requires a hold_days shard")
        parameters = self.parameters(spec)
        work_units = len(parameters.entry_modes) * len(parameters.profile_variants)
        return LabShardWorkPlan(
            phase="nshape_compare",
            work_unit_name="parameter_case",
            work_units=work_units,
            static_duration_ms=work_units * N_SHAPE_COMPARE_MS_PER_CASE,
        )

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        from rquant.paper import PaperTradeConfig
        from rquant.strategy_compare import run_entry_mode_comparison

        if not isinstance(validated.shard, HoldDaysShardInput):
            raise TypeError("NShapeCompare requires a hold_days shard")
        parameters = self.parameters(validated.spec)
        result = run_entry_mode_comparison(
            store,
            start_date=validated.spec.parameters.start_date,
            end_date=validated.spec.parameters.end_date,
            entry_modes=list(parameters.entry_modes),
            profile_variants=list(parameters.profile_variants),
            preset_name=parameters.preset_name,
            max_hold_days=validated.shard.hold_days,
            freq=parameters.freq,
            factor_score_threshold=float(parameters.factor_score_threshold),
            paper_config=PaperTradeConfig(entry_slippage_pct=0),
            execution_costs=validated.spec.execution_costs,
        )
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(name="summary", frame=result.summary),
                LabShardTable(name="trades", frame=result.trades),
            ),
            metrics=(LabShardMetric(name="candidates_count", value=result.candidates_count),),
        )


class NShapeOptimizeAdapter:
    adapter_id = "nshape-optimize"
    adapter_version = ADAPTER_VERSION
    strategy_name = "n_shape"
    snapshot_strategy_name = "n_shape"
    job_type = ResearchJobType.PARAMETER_SEARCH

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return _local_snapshot_source_usage(self.adapter_id)

    def parameters(self, spec: ResearchRunSpec) -> NShapeOptimizeParameters:
        return NShapeOptimizeParameters.model_validate(
            _parse_parameters(spec, NShapeOptimizeParameters)
        )

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        parameters = self.parameters(spec)
        return tuple(HoldDaysShardInput(hold_days=value) for value in parameters.hold_days)

    def build_work_plan(
        self,
        spec: ResearchRunSpec,
        shard: StrategyShardInput,
    ) -> LabShardWorkPlan:
        if not isinstance(shard, HoldDaysShardInput):
            raise TypeError("NShapeOptimize requires a hold_days shard")
        parameters = self.parameters(spec)
        work_units = (
            len(parameters.entry_modes)
            * len(parameters.profile_variants)
            * len(parameters.top_n_options)
            * len(parameters.score_profile_names)
            * max(1, parameters.walk_forward_folds + 1)
        )
        return LabShardWorkPlan(
            phase="nshape_optimize",
            work_unit_name="parameter_case",
            work_units=work_units,
            static_duration_ms=work_units * N_SHAPE_OPTIMIZE_MS_PER_CASE,
        )

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        from rquant.strategy_optimizer import run_strategy_optimization

        if not isinstance(validated.shard, HoldDaysShardInput):
            raise TypeError("NShapeOptimize requires a hold_days shard")
        parameters = self.parameters(validated.spec)
        result = run_strategy_optimization(
            store,
            start_date=validated.spec.parameters.start_date,
            end_date=validated.spec.parameters.end_date,
            preset_name=parameters.preset_name,
            entry_modes=list(parameters.entry_modes),
            profile_variants=list(parameters.profile_variants),
            max_hold_days_options=[validated.shard.hold_days],
            validation_ratio=float(parameters.validation_ratio),
            min_trades=parameters.min_trades,
            top_n_options=list(parameters.top_n_options),
            score_profile_names=list(parameters.score_profile_names),
            walk_forward_folds=parameters.walk_forward_folds,
            freq=parameters.freq,
            execution_costs=validated.spec.execution_costs,
        )
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(name="rankings", frame=result.rankings),
                LabShardTable(name="trades", frame=result.trades),
                LabShardTable(name="topn_rankings", frame=result.topn_rankings),
                LabShardTable(name="topn_trades", frame=result.topn_trades),
                LabShardTable(
                    name="walk_forward_rankings",
                    frame=result.walk_forward_rankings,
                ),
                LabShardTable(name="walk_forward_trades", frame=result.walk_forward_trades),
            ),
        )


class AuctionGapAdapter:
    adapter_id = "auction-gap"
    adapter_version = ADAPTER_VERSION
    strategy_name = "auction_gap"
    snapshot_strategy_name = "auction_gap"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return _local_snapshot_source_usage(self.adapter_id)

    def parameters(self, spec: ResearchRunSpec) -> AuctionGapParameters:
        return AuctionGapParameters.model_validate(_parse_parameters(spec, AuctionGapParameters))

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        self.parameters(spec)
        return _date_buckets(spec.parameters.start_date, spec.parameters.end_date)

    def build_work_plan(
        self,
        spec: ResearchRunSpec,
        shard: StrategyShardInput,
    ) -> LabShardWorkPlan:
        self.parameters(spec)
        if not isinstance(shard, DateBucketShardInput):
            raise TypeError("AuctionGap requires a date_bucket shard")
        work_units = (shard.end_date - shard.start_date).days + 1
        return LabShardWorkPlan(
            phase="auction_gap_replay",
            work_unit_name="calendar_day",
            work_units=work_units,
            static_duration_ms=work_units * AUCTION_GAP_MS_PER_DAY,
        )

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        from rquant.auction_gap_strategy import (
            AuctionGapMinuteReplayConfig,
            run_auction_gap_minute_replay,
            run_auction_gap_replay,
        )

        if not isinstance(validated.shard, DateBucketShardInput):
            raise TypeError("AuctionGap requires a date_bucket shard")
        parameters = self.parameters(validated.spec)
        config = AuctionGapMinuteReplayConfig(
            start_date=validated.shard.start_date.isoformat(),
            end_date=validated.shard.end_date.isoformat(),
            gap_mode=parameters.gap_mode,
            min_auction_vol_ratio_5d=float(parameters.min_auction_vol_ratio_5d),
            max_auction_vol_ratio_5d=float(parameters.max_auction_vol_ratio_5d),
            st_filter=parameters.st_filter,
            freq=parameters.freq,
            max_hold_days=parameters.max_hold_days,
        )
        candidates = run_auction_gap_replay(store, config.auction_config())
        trades = run_auction_gap_minute_replay(store, config, candidates=candidates)
        trades = apply_round_trip_execution_costs(
            trades,
            validated.spec.execution_costs,
        )
        summary = auction_gap_metric_rows(candidates, trades)
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(name="candidates", frame=candidates),
                LabShardTable(name="trades", frame=trades),
                LabShardTable(name="summary", frame=summary),
            ),
        )


_GROWTH_VARIANT_FLAGS: dict[GrowthVariant, tuple[bool, bool, bool]] = {
    "full": (True, True, True),
    "no_vwap": (False, True, True),
    "no_same_minute": (True, False, True),
    "no_accel_5m": (True, True, False),
    "cum_only": (False, False, False),
}


class GrowthBoardSurgeAdapter:
    adapter_id = "growth-board-surge"
    adapter_version = ADAPTER_VERSION
    strategy_name = "growth_board_surge"
    snapshot_strategy_name = "growth_board_surge"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def source_usage(self) -> ResearchAdapterSourceUsage:
        return _local_snapshot_source_usage(self.adapter_id)

    def parameters(self, spec: ResearchRunSpec) -> GrowthBoardSurgeParameters:
        return GrowthBoardSurgeParameters.model_validate(
            _parse_parameters(spec, GrowthBoardSurgeParameters)
        )

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        parameters = self.parameters(spec)
        return tuple(
            GrowthDateVariantShardInput(
                start_date=bucket.start_date,
                end_date=bucket.end_date,
                variant=variant,
            )
            for bucket in _date_buckets(spec.parameters.start_date, spec.parameters.end_date)
            for variant in parameters.variants
        )

    def build_work_plan(
        self,
        spec: ResearchRunSpec,
        shard: StrategyShardInput,
    ) -> LabShardWorkPlan:
        self.parameters(spec)
        if not isinstance(shard, GrowthDateVariantShardInput):
            raise TypeError("GrowthBoardSurge requires a growth_date_variant shard")
        work_units = (shard.end_date - shard.start_date).days + 1
        return LabShardWorkPlan(
            phase="growth_board_surge_replay",
            work_unit_name="calendar_day",
            work_units=work_units,
            static_duration_ms=work_units * GROWTH_BOARD_SURGE_MS_PER_DAY,
        )

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        from rquant.growth_board_surge_strategy import (
            GrowthBoardSurgeConfig,
            run_growth_board_surge_replay,
        )

        if not isinstance(validated.shard, GrowthDateVariantShardInput):
            raise TypeError("GrowthBoardSurge requires a growth_date_variant shard")
        parameters = self.parameters(validated.spec)
        require_vwap, use_same_minute, use_accel = _GROWTH_VARIANT_FLAGS[validated.shard.variant]
        config = GrowthBoardSurgeConfig(
            lookback_days=parameters.lookback_days,
            min_hist_days=parameters.min_hist_days,
            min_cum_amount_ratio=float(parameters.min_cum_amount_ratio),
            min_same_minute_amount_ratio=float(parameters.min_same_minute_amount_ratio),
            min_amount_accel_5m=float(parameters.min_amount_accel_5m),
            max_hold_days=parameters.max_hold_days,
            require_vwap_strength=parameters.require_vwap_strength and require_vwap,
            use_same_minute_surge=use_same_minute,
            use_accel_surge=use_accel,
        )
        trades = run_growth_board_surge_replay(
            store,
            start_date=validated.shard.start_date,
            end_date=validated.shard.end_date,
            config=config,
        )
        trades = apply_round_trip_execution_costs(
            trades,
            validated.spec.execution_costs,
        )
        if not trades.empty:
            trades = trades.copy()
            trades.insert(0, "variant", validated.shard.variant)
        summary = growth_board_metric_rows(
            trades,
            strategy_name=validated.shard.variant,
        )
        summary.insert(0, "variant", validated.shard.variant)
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(name="trades", frame=trades),
                LabShardTable(name="summary", frame=summary),
            ),
        )


class StrategyJobAdapterRegistry:
    def __init__(self, adapters: Iterable[StrategyJobAdapter]) -> None:
        ordered = tuple(adapters)
        identities = tuple((adapter.adapter_id, adapter.adapter_version) for adapter in ordered)
        strategies = tuple((adapter.strategy_name, adapter.job_type) for adapter in ordered)
        if len(identities) != len(set(identities)):
            raise ValueError("adapter registry identities must be unique")
        if len(strategies) != len(set(strategies)):
            raise ValueError("adapter registry strategy names must be unique")
        for adapter in ordered:
            source_usage = getattr(adapter, "source_usage", None)
            if not callable(source_usage):
                raise ValueError("research adapter source usage is required")
            try:
                usage = source_usage()
            except Exception as exc:
                raise ValueError("research adapter source usage is invalid") from exc
            if not isinstance(usage, ResearchAdapterSourceUsage):
                raise ValueError("research adapter source usage is invalid")
            require_research_adapter_source_usage(
                adapter_id=adapter.adapter_id,
                usage=usage,
            )
        self._adapters = ordered

    def closed_descriptor(self) -> StrategyAdapterRegistryDescriptor:
        adapters = tuple(
            StrategyAdapterRegistryIdentity(
                adapter_id=adapter.adapter_id,
                adapter_version=adapter.adapter_version,
            )
            for adapter in self._adapters
        )
        payload = {
            "adapters": [item.model_dump(mode="json") for item in adapters],
            "registry_id": "rquant.strategy-adapters.builtin",
            "registry_version": 1,
            "schema_version": 1,
        }
        return StrategyAdapterRegistryDescriptor(
            adapters=adapters,
            manifest_hash=hashlib.sha256(
                json.dumps(
                    payload,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("ascii")
            ).hexdigest(),
        )

    def for_spec(self, spec: ResearchRunSpec) -> StrategyJobAdapter:
        validated = ResearchRunSpec.model_validate(spec)
        requested_key = (validated.parameters.strategy_name, validated.job_type)
        legacy_alias = _LEGACY_STRATEGY_ALIASES.get(requested_key)
        key = requested_key if legacy_alias is None else legacy_alias[:2]
        matches = tuple(
            adapter
            for adapter in self._adapters
            if (adapter.strategy_name, adapter.job_type) == key
        )
        if len(matches) != 1:
            raise ValueError(
                "unsupported strategy/job_type: "
                f"{validated.parameters.strategy_name}/{validated.job_type.value}"
            )
        adapter = matches[0]
        if legacy_alias is not None:
            _strategy_name, _job_type, contract_id, contract_versions = legacy_alias
            if (
                validated.feature_contract.contract_id != contract_id
                or validated.feature_contract.contract_version not in contract_versions
            ):
                raise ValueError(
                    f"{validated.parameters.strategy_name} legacy execution contract "
                    "version is not supported"
                )
        expected_contract = build_adapter_execution_contract(
            adapter.adapter_id,
            adapter.adapter_version,
            validated.code_sha,
        )
        if validated.feature_contract != expected_contract:
            raise ValueError(
                f"{adapter.strategy_name} execution contract does not match adapter/code identity"
            )
        return adapter

    def get(self, adapter_id: str, adapter_version: str) -> StrategyJobAdapter:
        matches = tuple(
            adapter
            for adapter in self._adapters
            if (adapter.adapter_id, adapter.adapter_version) == (adapter_id, adapter_version)
        )
        if len(matches) != 1:
            raise ValueError(f"unknown adapter identity: {adapter_id}@{adapter_version}")
        return matches[0]

    def plan(self, spec: ResearchRunSpec) -> tuple[LabShardDefinition, ...]:
        validated = ResearchRunSpec.model_validate(spec)
        adapter = self.for_spec(validated)
        shard_inputs = adapter.build_shard_inputs(validated)
        if not shard_inputs:
            raise ValueError("strategy adapter produced an empty shard plan")
        work_plans = tuple(adapter.build_work_plan(validated, shard) for shard in shard_inputs)
        plan_payload = {
            "adapter_id": adapter.adapter_id,
            "adapter_version": adapter.adapter_version,
            "shards": [
                {
                    "input": shard.model_dump(mode="json"),
                    "work_plan": work_plan.model_dump(mode="json"),
                }
                for shard, work_plan in zip(shard_inputs, work_plans, strict=True)
            ],
            "spec_hash": validated.spec_hash,
        }
        canonical_plan = json.dumps(
            plan_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        plan_hash = hashlib.sha256(canonical_plan.encode("utf-8")).hexdigest()
        return tuple(
            LabShardDefinition.from_payload(
                shard_index=index,
                adapter_id=adapter.adapter_id,
                adapter_version=adapter.adapter_version,
                plan_hash=plan_hash,
                payload_json=StrategyShardPayload(
                    adapter_id=adapter.adapter_id,
                    adapter_version=adapter.adapter_version,
                    spec=validated,
                    shard=shard,
                ).model_dump_json(round_trip=True),
                work_plan=work_plans[index],
            )
            for index, shard in enumerate(shard_inputs)
        )

    def _plan_p13_legacy(self, spec: ResearchRunSpec) -> tuple[LabShardDefinition, ...]:
        validated = ResearchRunSpec.model_validate(spec)
        adapter = self.for_spec(validated)
        shard_inputs = adapter.build_shard_inputs(validated)
        if not shard_inputs:
            raise ValueError("strategy adapter produced an empty shard plan")
        plan_payload = {
            "adapter_id": adapter.adapter_id,
            "adapter_version": adapter.adapter_version,
            "shards": [item.model_dump(mode="json") for item in shard_inputs],
            "spec_hash": validated.spec_hash,
        }
        canonical_plan = json.dumps(
            plan_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        plan_hash = hashlib.sha256(canonical_plan.encode("utf-8")).hexdigest()
        return tuple(
            LabShardDefinition.from_payload(
                shard_index=index,
                adapter_id=adapter.adapter_id,
                adapter_version=adapter.adapter_version,
                plan_hash=plan_hash,
                payload_json=StrategyShardPayload(
                    adapter_id=adapter.adapter_id,
                    adapter_version=adapter.adapter_version,
                    spec=validated,
                    shard=shard,
                ).model_dump_json(round_trip=True),
            )
            for index, shard in enumerate(shard_inputs)
        )

    def validate_claim(self, claim: LabShardClaim | LabShardClaimV2) -> ValidatedStrategyShard:
        if isinstance(claim, LabShardClaimV2):
            return self._validate_source_claim_v2(claim)
        validated_claim = LabShardClaim.model_validate(claim)
        payload = StrategyShardPayload.model_validate_json(validated_claim.definition.payload_json)
        if payload.spec.spec_hash != validated_claim.spec_hash:
            raise ValueError("claim spec_hash does not match embedded ResearchRunSpec")
        if (
            payload.adapter_id,
            payload.adapter_version,
        ) != (
            validated_claim.definition.adapter_id,
            validated_claim.definition.adapter_version,
        ):
            raise ValueError("claim adapter identity does not match payload")
        adapter = self.get(payload.adapter_id, payload.adapter_version)
        if self.for_spec(payload.spec) is not adapter:
            raise ValueError("claim adapter does not match ResearchRunSpec")
        definitions = (
            self._plan_p13_legacy(payload.spec)
            if validated_claim.definition.work_plan is None
            else self.plan(payload.spec)
        )
        if validated_claim.shard_index >= len(definitions):
            raise ValueError("claim shard_index is outside the regenerated plan")
        if definitions[validated_claim.shard_index] != validated_claim.definition:
            raise ValueError("claim definition does not match regenerated plan identity")
        return ValidatedStrategyShard(
            claim=validated_claim,
            spec=payload.spec,
            shard=payload.shard,
        )

    def _validate_source_claim_v2(self, claim: LabShardClaimV2) -> ValidatedStrategyShard:
        """Validate the signed V2 execution payload without changing V2 identity."""

        validated_claim = LabShardClaimV2.model_validate(claim, strict=True)
        source_payload = StrategyShardPayloadV2.model_validate_json(
            validated_claim.definition.payload_json
        )
        payload = StrategyShardPayload.model_validate_json(source_payload.payload_json)
        if (
            payload.adapter_id,
            payload.adapter_version,
        ) != (
            validated_claim.definition.adapter_id,
            validated_claim.definition.adapter_version,
        ):
            raise ValueError("source execution payload adapter identity does not match claim")
        adapter = self.get(payload.adapter_id, payload.adapter_version)
        if self.for_spec(payload.spec) is not adapter:
            raise ValueError("source execution payload adapter does not match ResearchRunSpec")
        definitions = (
            self._plan_p13_legacy(payload.spec)
            if validated_claim.definition.work_plan is None
            else self.plan(payload.spec)
        )
        if validated_claim.shard_index >= len(definitions):
            raise ValueError("source claim shard_index is outside the regenerated plan")
        expected_payload = StrategyShardPayload.model_validate_json(
            definitions[validated_claim.shard_index].payload_json
        )
        if expected_payload != payload:
            raise ValueError("source execution payload does not match regenerated shard")
        return ValidatedStrategyShard(
            claim=validated_claim,
            spec=payload.spec,
            shard=payload.shard,
        )

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        adapter = self.get(
            validated.claim.definition.adapter_id,
            validated.claim.definition.adapter_version,
        )
        return adapter.execute_shard(validated, store)

    def aggregate_results(
        self,
        spec: ResearchRunSpec,
        results: tuple[LabShardExecutionResult, ...],
    ) -> LabJobExecutionResult:
        validated = ResearchRunSpec.model_validate(spec)
        adapter = self.for_spec(validated)
        definitions = self.plan(validated)
        if len(results) != len(definitions):
            raise ValueError("aggregation requires the complete shard plan")

        by_shard_id = {result.shard_id: result for result in results}
        if len(by_shard_id) != len(results):
            raise ValueError("aggregation shard results must have unique shard identities")

        ordered: list[LabShardExecutionResult] = []
        for definition in definitions:
            result = by_shard_id.get(definition.shard_id)
            if result is None:
                raise ValueError("aggregation requires the complete shard plan")
            if (
                result.spec_hash,
                result.payload_hash,
                result.plan_hash,
                result.adapter_id,
                result.adapter_version,
            ) != (
                validated.spec_hash,
                definition.payload_hash,
                definition.plan_hash,
                definition.adapter_id,
                definition.adapter_version,
            ):
                raise ValueError("aggregation shard result identity conflicts with plan")
            ordered.append(result)

        expected_table_names = tuple(table.name for table in ordered[0].tables)
        if any(
            tuple(table.name for table in result.tables) != expected_table_names
            for result in ordered[1:]
        ):
            raise ValueError("aggregation shard table schemas do not match")

        recompute_summary = adapter.adapter_id in {
            AuctionGapAdapter.adapter_id,
            GrowthBoardSurgeAdapter.adapter_id,
        }
        aggregated_frames = {
            name: _concat_shard_frames(
                _normalize_legacy_sparse_empty_frames(
                    adapter_id=adapter.adapter_id,
                    table_name=name,
                    frames=tuple(result.tables[index].frame for result in ordered),
                )
            )
            for index, name in enumerate(expected_table_names)
            if not (recompute_summary and name == "summary")
        }
        if recompute_summary:
            aggregated_frames["summary"] = _recompute_derived_summary(
                adapter_id=adapter.adapter_id,
                spec=validated,
                aggregated_frames=aggregated_frames,
            )
        tables = tuple(
            LabShardTable(name=name, frame=aggregated_frames[name]) for name in expected_table_names
        )
        if adapter.adapter_id == NShapeOptimizeAdapter.adapter_id:
            tables = tuple(
                LabShardTable(name=table.name, frame=_rerank_optimizer_table(table))
                for table in tables
            )
        return LabJobExecutionResult(
            spec_hash=validated.spec_hash,
            plan_hash=definitions[0].plan_hash,
            adapter_id=adapter.adapter_id,
            adapter_version=adapter.adapter_version,
            tables=tables,
        )


def _recompute_derived_summary(
    *,
    adapter_id: str,
    spec: ResearchRunSpec,
    aggregated_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    if adapter_id == AuctionGapAdapter.adapter_id:
        return auction_gap_metric_rows(
            aggregated_frames["candidates"],
            aggregated_frames["trades"],
        )
    if adapter_id == GrowthBoardSurgeAdapter.adapter_id:
        parameters = GrowthBoardSurgeParameters.model_validate(
            _parse_parameters(spec, GrowthBoardSurgeParameters)
        )
        trades = aggregated_frames["trades"]
        if not trades.empty and "variant" not in trades.columns:
            raise ValueError("aggregated growth trades are missing variant identity")
        summaries: list[pd.DataFrame] = []
        for variant in parameters.variants:
            variant_trades = (
                trades.loc[trades["variant"] == variant] if "variant" in trades.columns else trades
            )
            summary = growth_board_metric_rows(
                variant_trades,
                strategy_name=variant,
            )
            summary.insert(0, "variant", variant)
            summaries.append(summary)
        return pd.concat(summaries, ignore_index=True)
    raise ValueError(f"adapter does not define a derived summary: {adapter_id}")


def _normalize_legacy_sparse_empty_frames(
    *,
    adapter_id: str,
    table_name: str,
    frames: tuple[pd.DataFrame, ...],
) -> tuple[pd.DataFrame, ...]:
    expected_sparse_columns = _LEGACY_SPARSE_EMPTY_TABLE_COLUMNS.get((adapter_id, table_name))
    if expected_sparse_columns is None:
        return frames
    populated = tuple(frame for frame in frames if not frame.empty)
    if not populated:
        return frames
    reference = populated[0].iloc[0:0].copy()
    return tuple(
        reference.copy()
        if frame.empty and tuple(frame.columns) == expected_sparse_columns
        else frame
        for frame in frames
    )


def _concat_shard_frames(frames: tuple[pd.DataFrame, ...]) -> pd.DataFrame:
    expected_columns = tuple(frames[0].columns)
    expected_dtypes = tuple(frames[0].dtypes)
    for index, frame in enumerate(frames[1:], start=1):
        if tuple(frame.columns) != expected_columns or any(
            not is_dtype_equal(actual, expected)
            for actual, expected in zip(frame.dtypes, expected_dtypes, strict=True)
        ):
            raise ValueError(
                f"aggregation shard frame {index} schema does not match the first shard"
            )
    populated = tuple(frame for frame in frames if not frame.empty)
    aggregated = pd.concat(populated, ignore_index=True) if populated else frames[0].copy()
    if tuple(aggregated.columns) != expected_columns or any(
        not is_dtype_equal(actual, expected)
        for actual, expected in zip(aggregated.dtypes, expected_dtypes, strict=True)
    ):
        raise ValueError("aggregation output schema does not match the shard schema")
    return aggregated


def _rerank_optimizer_table(table: LabShardTable) -> pd.DataFrame:
    from rquant.strategy_ranking import StrategyRankingTable, rank_strategy_table

    ranking_name: StrategyRankingTable
    if table.name == "rankings":
        ranking_name = "rankings"
    elif table.name == "topn_rankings":
        ranking_name = "topn_rankings"
    elif table.name == "walk_forward_rankings":
        ranking_name = "walk_forward_rankings"
    else:
        return table.frame
    return rank_strategy_table(table.frame, table_name=ranking_name)


@lru_cache(maxsize=1)
def default_strategy_job_adapter_registry() -> StrategyJobAdapterRegistry:
    return StrategyJobAdapterRegistry(
        (
            NShapeCompareAdapter(),
            NShapeOptimizeAdapter(),
            AuctionGapAdapter(),
            GrowthBoardSurgeAdapter(),
        )
    )
