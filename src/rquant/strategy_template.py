"""Bounded strategy rules compiled to the original immutable StrategySpec."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal
from types import MappingProxyType
from typing import Annotated, Literal, Self

from pydantic import Field, JsonValue, field_serializer, field_validator, model_validator

from rquant.backtest.benchmark import SUPPORTED_BENCHMARK_CODES
from rquant.backtest.contracts import RebalanceRule, Sha256
from rquant.feature_contracts import FeatureRequirement, RequirementLevel
from rquant.llm.registry import get_rule_spec
from rquant.portfolio.weights import PortfolioWeightRule
from rquant.research_run_spec import _decimal_components, _parse_decimal
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.screen.loader import BASIC_COLS_MAP, FUNDAMENTAL_COLS_MAP, IND_COLS_MAP, PRICE_COLS_MAP, STATE_COLS_MAP
from rquant.strategy_spec import StateTransition, StrategyLifecycleState, StrategyRunMode, StrategySpec

TEMPLATE_CONTRACT = "strategy-template/v1"
TEMPLATE_FEATURE_CONTRACT = "strategy-template-entry/v1"
TEMPLATE_ID_PATTERN = r"^template_[0-9a-f]{32}$"
MAX_TEMPLATE_BYTES = 32 * 1024
EXIT_REASONS = ("stop_loss", "trailing_profit", "take_profit", "max_holding_days", "exit_time")
FEATURE_NAMES = ("template_entry", "template_index", "position_sellable", "entry_price_raw", "eligible_high_price_raw", "holding_trading_sessions")
_FIELD = re.compile(r"([A-Z][A-Z0-9_]*)\[(0|[1-9][0-9]?)\]\Z")
_COLUMN_NAMES = frozenset((*PRICE_COLS_MAP.values(), *IND_COLS_MAP.values(), *STATE_COLS_MAP.values(), *BASIC_COLS_MAP.values(), *FUNDAMENTAL_COLS_MAP.values()))


def _checked_numbers(value: object) -> None:
    if isinstance(value, bool):
        raise ValueError("strategy numeric and integer fields do not accept booleans")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("strategy numbers must be finite")
    if isinstance(value, Mapping):
        for item in value.values():
            _checked_numbers(item)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            _checked_numbers(item)


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw(item) for item in value]
    return value


def validate_template_weight_numbers(value: object) -> None:
    data = value.model_dump(mode="python") if isinstance(value, PortfolioWeightRule) else value
    if not isinstance(data, Mapping):
        return
    for name in ("max_stock_weight", "max_industry_weight", "cash_reserve", "min_target_amount"):
        if name not in data or data[name] is None:
            continue
        number = _parse_decimal(data[name], field_name=name)
        if name == "min_target_amount" and _decimal_components(number, field_name=name)[2] < -2:
            raise ValueError("minimum target amount must be exact to a cent")


class TemplateCondition(RuntimeContractModel):
    key: str = Field(min_length=1, max_length=64)
    args: Mapping[str, JsonValue] = Field(default_factory=dict)

    @field_validator("args", mode="before")
    @classmethod
    def check_numeric_values(cls, value: object) -> object:
        _checked_numbers(value)
        return _thaw(value)

    @model_validator(mode="after")
    def validate_original_rule(self) -> Self:
        original = get_rule_spec(self.key)
        normalized = original.args_model.model_validate(dict(self.args), strict=True).model_dump(mode="json")
        for name in ("left", "right", "field"):
            value = normalized.get(name)
            if isinstance(value, str):
                match = _FIELD.fullmatch(value)
                if match is None or match.group(1) not in _COLUMN_NAMES or int(match.group(2)) > 30:
                    raise ValueError("comparison field is outside the controlled catalog")
        if "fast" in normalized or "slow" in normalized:
            if any(normalized[name] not in {"MA5", "MA10", "MA20", "MA60"} for name in ("fast", "slow")):
                raise ValueError("MA field is outside the controlled catalog")
        if self.key == "between" and normalized["low"] > normalized["high"]:
            raise ValueError("condition range is reversed")
        original.fn(**normalized)
        canonical_sha256(normalized)
        object.__setattr__(self, "args", _freeze(normalized))
        return self

    @field_serializer("args")
    def serialize_args(self, value: Mapping[str, JsonValue]) -> object:
        return _thaw(value)


class PoolTemplateEntry(RuntimeContractModel):
    kind: Literal["pool"]
    pool_key: str = Field(pattern=r"^[a-zA-Z0-9_./:-]{1,128}$")
    version: int = Field(strict=True, ge=1)
    body_hash: Sha256


class ConditionTemplateEntry(RuntimeContractModel):
    kind: Literal["conditions"]
    conditions: tuple[TemplateCondition, ...] = Field(min_length=1, max_length=26)


class SignalTemplateEntry(RuntimeContractModel):
    kind: Literal["signal"]
    strategy_id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    version: int = Field(strict=True, ge=1)
    action: Literal["watch", "b_intent", "b_confirm", "reduce", "s_intent", "s_confirm"]
    source_hash: Sha256


TemplateEntry = Annotated[PoolTemplateEntry | ConditionTemplateEntry | SignalTemplateEntry, Field(discriminator="kind")]


class TemplateExitRules(RuntimeContractModel):
    stop_loss: Decimal | None = Field(default=None, gt=0, lt=1, allow_inf_nan=False)
    take_profit: Decimal | None = Field(default=None, gt=0, le=10, allow_inf_nan=False)
    trailing_profit: Decimal | None = Field(default=None, gt=0, lt=1, allow_inf_nan=False)
    max_holding_days: int | None = Field(default=None, strict=True, ge=1, le=2520)
    exit_time: str | None = None

    @field_validator("stop_loss", "take_profit", "trailing_profit", mode="before")
    @classmethod
    def validate_rate_representation(cls, value: object) -> Decimal | None:
        return None if value is None else _parse_decimal(value, field_name="template exit rate")

    @field_validator("exit_time")
    @classmethod
    def validate_whole_market_minute(cls, value: str | None) -> str | None:
        if value is not None and (re.fullmatch(r"[0-2][0-9]:[0-5][0-9]", value) is None or not ("09:31" <= value <= "11:30" or "13:00" <= value <= "15:00")):
            raise ValueError("exit time must be a Shanghai market minute")
        return value


class TemplateIndexFilter(RuntimeContractModel):
    benchmark_code: str
    ma_days: int = Field(strict=True, ge=1, le=250)
    direction: Literal["above", "below"]

    @field_validator("benchmark_code")
    @classmethod
    def validate_benchmark(cls, value: str) -> str:
        if value not in SUPPORTED_BENCHMARK_CODES:
            raise ValueError("unsupported benchmark")
        return value


class StrategyTemplate(RuntimeContractModel):
    template_contract: Literal["strategy-template/v1"] = TEMPLATE_CONTRACT
    entry: TemplateEntry
    exit: TemplateExitRules = Field(default_factory=TemplateExitRules)
    weight_rule: PortfolioWeightRule
    rebalance_rule: RebalanceRule
    index_filter: TemplateIndexFilter | None = None

    @model_validator(mode="before")
    @classmethod
    def reject_boolean_numbers(cls, value: object) -> object:
        _checked_numbers(value)
        if isinstance(value, Mapping):
            validate_template_weight_numbers(value.get("weight_rule"))
        return value

    @model_validator(mode="after")
    def validate_budget(self) -> Self:
        if self.weight_rule.max_positions > 500:
            raise ValueError("position count exceeds 500")
        if self.rebalance_rule.every_n_days is not None and self.rebalance_rule.every_n_days > 252:
            raise ValueError("rebalance period exceeds 252 trading days")
        if len(self.model_dump_json().encode("utf-8")) > MAX_TEMPLATE_BYTES:
            raise ValueError("strategy template exceeds 32 KiB")
        return self

    @property
    def rules_hash(self) -> str:
        return canonical_sha256(self)


def compile_strategy_template(rules: StrategyTemplate, *, strategy_id: str, version: int, producer_commit: str) -> StrategySpec:
    rules = StrategyTemplate.model_validate(rules.model_dump(mode="python"))
    if re.fullmatch(TEMPLATE_ID_PATTERN, strategy_id) is None or type(version) is not int:
        raise ValueError("invalid controlled strategy identity")
    transitions = [StateTransition(from_state=StrategyLifecycleState.IDLE, event="entry_filled", to_state=StrategyLifecycleState.HOLDING)]
    for reason in EXIT_REASONS:
        transitions.extend((StateTransition(from_state=StrategyLifecycleState.HOLDING, event=reason, to_state=StrategyLifecycleState.HOLDING), StateTransition(from_state=StrategyLifecycleState.HOLDING, event=f"{reason}_filled", to_state=StrategyLifecycleState.TERMINAL)))
    return StrategySpec(strategy_id=strategy_id, version=version, feature_contract_id=TEMPLATE_FEATURE_CONTRACT, min_feature_contract_version=1, required_features=tuple(FeatureRequirement(name=name, level=RequirementLevel.REQUIRED, min_contract_version=1) for name in FEATURE_NAMES), optional_features=(), initial_state=StrategyLifecycleState.IDLE, transitions=tuple(transitions), parameters={"template_contract": TEMPLATE_CONTRACT, "rules": rules.model_dump(mode="python")}, allowed_actions=("b_intent", "s_intent"), run_mode=StrategyRunMode.PAPER, producer_commit=producer_commit)
