"""Complete immutable parameters for the legacy minute math owners."""

from __future__ import annotations

import base64
from datetime import date, time
from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, field_validator, model_validator

from rquant.auction_gap_strategy import AuctionGapMinuteReplayConfig
from rquant.growth_board_surge_strategy import GrowthBoardSurgeConfig
from rquant.minute_replay import MinuteReplayConfig
from rquant.paper import PaperTradeConfig
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_compare import _volume_profile_config
from rquant.volume_profile import VolumeProfileRuleConfig

MinuteFrequency = Literal["1min", "5min", "15min", "30min", "60min"]
MinuteProfileVariant = Literal["baseline", "vp_risk_only", "vp_90"]
_IMMUTABLE_CONFIG = ConfigDict(
    extra="forbid", frozen=True, revalidate_instances="always", allow_inf_nan=False
)


class MinutePaperParameters(PaperTradeConfig):
    model_config = _IMMUTABLE_CONFIG


class MinuteVolumeProfileParameters(VolumeProfileRuleConfig):
    model_config = _IMMUTABLE_CONFIG

    @field_validator("lookback_days")
    @classmethod
    def valid_lookbacks(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if not value or any(days <= 0 for days in value) or len(set(value)) != len(value):
            raise ValueError("volume profile lookbacks must be positive and unique")
        return value


def n_shape_volume_profile_parameters(variant: MinuteProfileVariant) -> MinuteVolumeProfileParameters:
    if variant not in ("baseline", "vp_risk_only", "vp_90"):
        raise ValueError("unknown volume profile variant")
    return MinuteVolumeProfileParameters.model_validate(_volume_profile_config(variant).model_dump())


def _local_clock(value: time) -> time:
    if value.tzinfo is not None:
        raise ValueError("strategy clock must be local Asia/Shanghai wall time")
    return value


class MinuteNShapeParameters(MinuteReplayConfig):
    model_config = _IMMUTABLE_CONFIG

    family: Literal["n_shape"] = "n_shape"
    preset_name: Literal["n-shape-pool1", "n-shape-pool2", "n-shape-combined"] = "n-shape-pool1"
    paper: MinutePaperParameters = Field(default_factory=MinutePaperParameters)
    volume_profile: MinuteVolumeProfileParameters = Field(default_factory=MinuteVolumeProfileParameters)

    _validate_clock = field_validator("late_confirm_at")(_local_clock)

    def owner_config(self) -> MinuteReplayConfig:
        return MinuteReplayConfig.model_validate(self.model_dump(exclude={"family"}))


class MinuteAuctionGapParameters(AuctionGapMinuteReplayConfig):
    model_config = _IMMUTABLE_CONFIG

    family: Literal["auction_gap"] = "auction_gap"
    freq: MinuteFrequency = "1min"
    paper: MinutePaperParameters = Field(default_factory=MinutePaperParameters)
    next_day_price_policy: Literal["keep_candidate_mark_unavailable"] | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )

    _validate_clocks = field_validator("entry_start_time", "next_morning_exit_until")(_local_clock)

    @model_validator(mode="after")
    def valid_window(self) -> Self:
        start, end = date.fromisoformat(self.start_date), date.fromisoformat(self.end_date)
        if start.isoformat() != self.start_date or end.isoformat() != self.end_date or start > end:
            raise ValueError("auction window must be an ordered ISO date range")
        if not 0 <= self.min_auction_vol_ratio_5d <= self.max_auction_vol_ratio_5d:
            raise ValueError("auction volume ratio bounds must be nonnegative and ordered")
        if self.price_tol <= 0:
            raise ValueError("auction price tolerance must be positive")
        return self

    def owner_config(self) -> AuctionGapMinuteReplayConfig:
        return AuctionGapMinuteReplayConfig.model_validate(self.model_dump(exclude={"family", "next_day_price_policy"}))

class MinuteGrowthParameters(GrowthBoardSurgeConfig):
    model_config = _IMMUTABLE_CONFIG

    family: Literal["growth_board_surge"] = "growth_board_surge"
    freq: MinuteFrequency = "1min"
    paper: MinutePaperParameters = Field(default_factory=lambda: MinutePaperParameters(
        candidate_id="growth_board_surge_v0", stop_loss_pct=0.05,
        take_profit_pct=0.08, trailing_stop_pct=0.03,
    ))

    _validate_clock = field_validator("min_signal_time")(_local_clock)

    def owner_config(self) -> GrowthBoardSurgeConfig:
        return GrowthBoardSurgeConfig.model_validate(self.model_dump(exclude={"family"}))


MinuteParameters = Annotated[
    MinuteNShapeParameters | MinuteAuctionGapParameters | MinuteGrowthParameters,
    Field(discriminator="family"),
]


class MinuteParameterSet(RuntimeContractModel):
    kind: Literal["minute-parameter-set"] = "minute-parameter-set"
    schema_version: Literal[1, 2] = 1
    parameters: MinuteParameters

    @model_validator(mode="after")
    def complete_next_day_policy(self) -> Self:
        policy = self.parameters.next_day_price_policy if isinstance(self.parameters, MinuteAuctionGapParameters) else None
        if self.schema_version == 2:
            if not isinstance(self.parameters, MinuteAuctionGapParameters) or policy is None:
                raise ValueError("parameter schema 2 requires the explicit minute auction next-day price policy")
        elif policy is not None:
            raise ValueError("explicit minute auction next-day price policy requires parameter schema 2")
        return self

    @property
    def fingerprint(self) -> str:
        # The original canonical codec has no time-only scalar; the complete
        # JSON contract retains each wall clock without inventing a timestamp.
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def definition_id(self) -> str:
        prefix = {"n_shape": "np.", "auction_gap": "ap.", "growth_board_surge": "gp."}[self.parameters.family]
        complete_digest = base64.b32encode(bytes.fromhex(self.fingerprint)).decode("ascii").rstrip("=").lower()
        return prefix + complete_digest

    @property
    def definition_version(self) -> Literal[1]:
        return 1

    @property
    def evaluator_semantic_version(self) -> Literal["2.0.0", "2.1.0"]:
        return "2.1.0" if self.schema_version == 2 else "2.0.0"
