"""Typed deterministic shard adapters for Strategy Lab research jobs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import date, timedelta
from decimal import Decimal
from functools import lru_cache
from typing import Annotated, Literal, Protocol, TypeAlias
from uuid import UUID

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from rquant.lab_shard_protocol import LabShardClaim, LabShardDefinition
from rquant.research_run_spec import ResearchJobType, ResearchRunSpec

DATE_BUCKET_DAYS = 20
ADAPTER_VERSION = "1"

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
    claim: LabShardClaim
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


class StrategyJobAdapter(Protocol):
    adapter_id: str
    adapter_version: str
    strategy_name: str
    job_type: ResearchJobType

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]: ...

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult: ...


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
    strategy_name = "NShapeCompare"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def parameters(self, spec: ResearchRunSpec) -> NShapeCompareParameters:
        return NShapeCompareParameters.model_validate(
            _parse_parameters(spec, NShapeCompareParameters)
        )

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        parameters = self.parameters(spec)
        return tuple(HoldDaysShardInput(hold_days=value) for value in parameters.hold_days)

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
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
    strategy_name = "NShapeOptimize"
    job_type = ResearchJobType.PARAMETER_SEARCH

    def parameters(self, spec: ResearchRunSpec) -> NShapeOptimizeParameters:
        return NShapeOptimizeParameters.model_validate(
            _parse_parameters(spec, NShapeOptimizeParameters)
        )

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        parameters = self.parameters(spec)
        return tuple(HoldDaysShardInput(hold_days=value) for value in parameters.hold_days)

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
    strategy_name = "AuctionGap"
    job_type = ResearchJobType.STRATEGY_REPLAY

    def parameters(self, spec: ResearchRunSpec) -> AuctionGapParameters:
        return AuctionGapParameters.model_validate(_parse_parameters(spec, AuctionGapParameters))

    def build_shard_inputs(self, spec: ResearchRunSpec) -> tuple[StrategyShardInput, ...]:
        self.parameters(spec)
        return _date_buckets(spec.parameters.start_date, spec.parameters.end_date)

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
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(name="candidates", frame=candidates),
                LabShardTable(name="trades", frame=trades),
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
    strategy_name = "GrowthBoardSurge"
    job_type = ResearchJobType.STRATEGY_REPLAY

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
        if not trades.empty:
            trades = trades.copy()
            trades.insert(0, "variant", validated.shard.variant)
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(LabShardTable(name="trades", frame=trades),),
        )


class StrategyJobAdapterRegistry:
    def __init__(self, adapters: Iterable[StrategyJobAdapter]) -> None:
        ordered = tuple(adapters)
        identities = tuple((adapter.adapter_id, adapter.adapter_version) for adapter in ordered)
        strategies = tuple(adapter.strategy_name for adapter in ordered)
        if len(identities) != len(set(identities)):
            raise ValueError("adapter registry identities must be unique")
        if len(strategies) != len(set(strategies)):
            raise ValueError("adapter registry strategy names must be unique")
        self._adapters = ordered

    def for_spec(self, spec: ResearchRunSpec) -> StrategyJobAdapter:
        validated = ResearchRunSpec.model_validate(spec)
        matches = tuple(
            adapter
            for adapter in self._adapters
            if adapter.strategy_name == validated.parameters.strategy_name
        )
        if len(matches) != 1:
            raise ValueError(f"unsupported strategy_name: {validated.parameters.strategy_name}")
        adapter = matches[0]
        if validated.job_type is not adapter.job_type:
            raise ValueError(f"{adapter.strategy_name} requires job_type {adapter.job_type.value}")
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

    def validate_claim(self, claim: LabShardClaim) -> ValidatedStrategyShard:
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
        definitions = self.plan(payload.spec)
        if validated_claim.shard_index >= len(definitions):
            raise ValueError("claim shard_index is outside the regenerated plan")
        if definitions[validated_claim.shard_index] != validated_claim.definition:
            raise ValueError("claim definition does not match regenerated plan identity")
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
