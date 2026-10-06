"""Shared condition alert definitions; price alert v4 contracts remain separate."""

from __future__ import annotations

from datetime import date, time
from typing import Annotated, Literal, Self

from pydantic import (
    Field,
    StrictBool,
    StrictInt,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.delivery_contracts import DeliveryChannel
from rquant.llm.registry import get_rule_spec
from rquant.llm.schemas import RuleCall
from rquant.manual_watchlist import OwnerId
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.screen.intraday_contracts import INTRADAY_FIELD_LABELS
from rquant.screen.intraday_reference import Code, Sha256
from rquant.web.models.screen import ScreenCondition, ScreenRankingPlan
from rquant.web.screen_catalog import validate_screen_choices

ConditionAlertRuleId = Annotated[str, StringConstraints(min_length=1, max_length=128)]


class ConditionAlertMarketScope(RuntimeContractModel):
    kind: Literal["market"] = "market"
    universe_policy: Literal["trusted_current"] = "trusted_current"


class ConditionAlertPoolScope(RuntimeContractModel):
    kind: Literal["pool"] = "pool"
    pool_name: str = Field(min_length=1, max_length=80)
    definition_version: Sha256
    result_version: Sha256


class ConditionAlertWatchlistScope(RuntimeContractModel):
    kind: Literal["watchlist"] = "watchlist"
    membership_version: Sha256


class ConditionAlertSectorScope(RuntimeContractModel):
    kind: Literal["sector"] = "sector"
    sector_system: Literal["industry", "concept"]
    sector_code: str = Field(min_length=1, max_length=80)
    component_source_version: Sha256


ConditionAlertScope = Annotated[
    ConditionAlertMarketScope
    | ConditionAlertPoolScope
    | ConditionAlertWatchlistScope
    | ConditionAlertSectorScope,
    Field(discriminator="kind"),
]


class ConditionAlertTimeWindow(RuntimeContractModel):
    start: time
    end: time

    @field_serializer("start", "end")
    def serialize_clock(self, value: time) -> str:
        return value.isoformat()

    @model_validator(mode="after")
    def require_continuous_session(self) -> Self:
        if (
            self.start.tzinfo is not None
            or self.end.tzinfo is not None
            or self.start >= self.end
            or not (
                time(9, 30) <= self.start < self.end <= time(11, 30)
                or time(13) <= self.start < self.end <= time(14, 57)
            )
        ):
            raise ValueError(
                "condition alert window must stay inside one Shanghai continuous session"
            )
        return self


class ConditionAlertTradingHours(RuntimeContractModel):
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    windows: tuple[ConditionAlertTimeWindow, ...] = (
        ConditionAlertTimeWindow(start=time(9, 30), end=time(11, 30)),
        ConditionAlertTimeWindow(start=time(13), end=time(14, 57)),
    )

    @model_validator(mode="after")
    def require_ordered_windows(self) -> Self:
        if not 1 <= len(self.windows) <= 2 or any(
            a.end > b.start for a, b in zip(self.windows, self.windows[1:], strict=False)
        ):
            raise ValueError("condition alert windows must be bounded and ordered")
        return self


class ConditionAlertEveryEvaluationFrequency(RuntimeContractModel):
    kind: Literal["every_evaluation"] = "every_evaluation"


class ConditionAlertPerSymbolMinutesFrequency(RuntimeContractModel):
    kind: Literal["per_symbol_minutes"] = "per_symbol_minutes"
    minutes: StrictInt = Field(ge=1, le=60)


class ConditionAlertBarCloseFrequency(RuntimeContractModel):
    kind: Literal["bar_close"] = "bar_close"
    bar_size: Literal["1min"] = "1min"


ConditionAlertFrequency = Annotated[
    ConditionAlertEveryEvaluationFrequency
    | ConditionAlertPerSymbolMinutesFrequency
    | ConditionAlertBarCloseFrequency,
    Field(discriminator="kind"),
]


class ConditionAlertGovernance(RuntimeContractModel):
    dedup_window_seconds: StrictInt = Field(default=60, ge=0, le=3600)
    notify_recovery: StrictBool = False
    channels: tuple[DeliveryChannel, ...] = Field(min_length=1, max_length=2)

    @model_validator(mode="after")
    def require_unique_channels(self) -> Self:
        if len(set(self.channels)) != len(self.channels):
            raise ValueError("condition alert channels must be unique")
        return self


class ConditionAlertSourcePolicy(RuntimeContractModel):
    daily_anchor: Literal["previous_closed_session"] = "previous_closed_session"
    condition_semantics_version: Literal["screen-registry/v1"] = "screen-registry/v1"
    intraday_contract_id: Literal["intraday-pit"] = "intraday-pit"
    minimum_intraday_contract_version: Literal[3, 4] = 3


class ConditionAlertOrigin(RuntimeContractModel):
    draft_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{24}$")
    execution_id: str = Field(min_length=1, max_length=128)
    command_hash: Sha256
    definition_hash: Sha256
    result_digest: Sha256
    member_rank_digest: Sha256
    source_identity: Sha256
    mode: Literal["daily", "intraday"]
    trade_date: date
    cutoff: AwareUtcDatetime | None = None


class ConditionAlertRuleDefinition(RuntimeContractModel):
    schema_version: Literal[1] = 1
    rule_id: ConditionAlertRuleId
    name: str = Field(min_length=1, max_length=80)
    priority: Literal["P0", "P1", "P2", "P3"]
    enabled: StrictBool
    conditions: tuple[RuleCall, ...] = Field(min_length=1, max_length=26)
    ranking: ScreenRankingPlan | None = None
    scope: ConditionAlertScope
    trading_hours: ConditionAlertTradingHours = ConditionAlertTradingHours()
    frequency: ConditionAlertFrequency = ConditionAlertEveryEvaluationFrequency()
    governance: ConditionAlertGovernance
    source_policy: ConditionAlertSourcePolicy = ConditionAlertSourcePolicy()
    origin: ConditionAlertOrigin | None = None

    @field_validator("conditions", mode="before")
    @classmethod
    def reject_extra_rule_context(cls, value: object) -> object:
        if isinstance(value, (tuple, list)) and any(
            isinstance(item, dict) and set(item) - {"name", "args"} for item in value
        ):
            raise ValueError("condition rule calls cannot carry extra context")
        return value

    @model_validator(mode="after")
    def normalize_original_registry_calls(self) -> Self:
        if not self.name.strip():
            raise ValueError("condition alert name must not be blank")
        arguments = validate_screen_choices(
            [ScreenCondition(key=call.name, args=call.args) for call in self.conditions],
            dynamic_ma=True,
            dynamic_rsi=True,
            fundamental_fields=("pe_ttm", "pb", "dv_ttm", "roe", "or_yoy", "netprofit_yoy"),
            extra_fields=tuple(INTRADAY_FIELD_LABELS.items()),
        )
        normalized = []
        for call, args in zip(self.conditions, arguments, strict=True):
            spec = get_rule_spec(call.name)
            if set(args) - set(spec.args_model.model_fields):
                raise ValueError("condition rule arguments contain unknown fields")
            normalized.append(
                RuleCall(
                    name=call.name,
                    args=spec.args_model.model_validate(args).model_dump(mode="json"),
                )
            )
        object.__setattr__(self, "conditions", tuple(normalized))
        new_fields = {"INTRADAY_SPEED_5M[0]", "INTRADAY_VOLUME_RATIO[0]"}
        if any(
            value in new_fields
            for call in self.conditions
            for value in call.args.values()
            if type(value) is str
        ):
            object.__setattr__(
                self,
                "source_policy",
                self.source_policy.model_copy(update={"minimum_intraday_contract_version": 4}),
            )
        canonical_sha256(self)
        return self

    @property
    def rule_body_hash(self) -> str:
        return canonical_sha256(self)


class OwnedConditionAlertRule(RuntimeContractModel):
    owner_id: OwnerId
    version: StrictInt = Field(ge=1)
    rule: ConditionAlertRuleDefinition
    updated_at: AwareUtcDatetime


class ConditionAlertScopeEvidence(RuntimeContractModel):
    owner_id: OwnerId
    scope: ConditionAlertScope
    scope_version: Sha256
    member_codes: tuple[Code, ...] = Field(max_length=8000)
    member_digest: Sha256
    available_at: AwareUtcDatetime

    @model_validator(mode="after")
    def require_exact_members(self) -> Self:
        if tuple(
            sorted(set(self.member_codes))
        ) != self.member_codes or self.member_digest != canonical_sha256(self.member_codes):
            raise ValueError("condition scope member evidence changed")
        return self
