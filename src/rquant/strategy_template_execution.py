"""Fixed trusted functions consume controlled rules and original source facts."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import Field, JsonValue, field_validator

from rquant.backtest.contracts import Sha256
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strategy_template import ConditionTemplateEntry, PoolTemplateEntry, StrategyTemplate


class TemplateSignal(RuntimeContractModel):
    strategy_id: str
    version: int = Field(strict=True, ge=1)
    action: str
    ts_code: str
    observed_at: AwareUtcDatetime
    source_hash: Sha256


class TemplateEntryEvidence(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    source_hash: Sha256
    pool_key: str | None = None
    pool_version: int | None = Field(default=None, strict=True, ge=1)
    pool_codes: tuple[str, ...] = ()
    rows: tuple[dict[str, JsonValue], ...] = ()
    signals: tuple[TemplateSignal, ...] = ()


class TemplatePosition(RuntimeContractModel):
    ts_code: str
    entry_price: Decimal = Field(gt=0, allow_inf_nan=False)
    eligible_high: Decimal = Field(gt=0, allow_inf_nan=False)
    holding_days: int = Field(strict=True, ge=0)
    sellable_quantity: int = Field(strict=True, ge=0)

    @field_validator("entry_price", "eligible_high", mode="before")
    @classmethod
    def validate_price_representation(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="template position price")


class TemplatePrice(RuntimeContractModel):
    ts_code: str
    price: Decimal = Field(gt=0, allow_inf_nan=False)
    event_time: AwareUtcDatetime
    observed_at: AwareUtcDatetime
    source_hash: Sha256
    suspended: bool = Field(strict=True)
    sell_limit_locked: bool = Field(strict=True)
    basis: Literal["minute_close", "daily_reference", "daily_close"]

    @field_validator("price", mode="before")
    @classmethod
    def validate_price_representation(cls, value: object) -> Decimal:
        return _parse_decimal(value, field_name="template price")


def strategy_template_feature(features: Mapping[str, object], name: str) -> object:
    return features[name]


class TemplateEntryProjection(RuntimeContractModel):
    evidence: TemplateEntryEvidence
    rules_hash: Sha256
    evidence_hash: Sha256
    eligible_codes: tuple[str, ...]


def strategy_template_entry(
    rules: StrategyTemplate, projection: TemplateEntryProjection, *, decision_time: datetime
) -> tuple[str, ...]:
    evidence = projection.evidence
    if evidence.observed_at > decision_time:
        raise ValueError("future entry evidence")
    if projection.rules_hash != rules.rules_hash:
        raise ValueError("entry projection rules differ")
    entry = rules.entry
    if isinstance(entry, PoolTemplateEntry):
        if (entry.pool_key, entry.version, entry.body_hash) != (
            evidence.pool_key,
            evidence.pool_version,
            evidence.source_hash,
        ):
            raise ValueError("pool reference differs")
        return tuple(sorted(set(evidence.pool_codes)))
    if isinstance(entry, ConditionTemplateEntry):
        return projection.eligible_codes
    if entry.source_hash != evidence.source_hash:
        raise ValueError("signal source reference differs")
    if any(signal.observed_at > decision_time for signal in evidence.signals):
        raise ValueError("future signal evidence")
    return tuple(
        sorted(
            {
                signal.ts_code
                for signal in evidence.signals
                if (signal.strategy_id, signal.version, signal.action, signal.source_hash)
                == (entry.strategy_id, entry.version, entry.action, entry.source_hash)
            }
        )
    )


def strategy_template_exit(
    rules: StrategyTemplate,
    position: TemplatePosition,
    quote: TemplatePrice,
    *,
    decision_time: datetime,
) -> str | None:
    if quote.observed_at > decision_time or quote.event_time > quote.observed_at:
        raise ValueError("future exit evidence")
    if position.ts_code != quote.ts_code:
        raise ValueError("exit symbol differs")
    if not position.sellable_quantity or quote.suspended or quote.sell_limit_locked:
        return None
    exits = rules.exit
    gain = quote.price / position.entry_price - Decimal("1")
    if exits.stop_loss is not None and gain <= -exits.stop_loss:
        return "stop_loss"
    if exits.trailing_profit is not None and quote.price <= position.eligible_high * (
        Decimal("1") - exits.trailing_profit
    ):
        return "trailing_profit"
    if exits.take_profit is not None and gain >= exits.take_profit:
        return "take_profit"
    if exits.max_holding_days is not None and position.holding_days >= exits.max_holding_days:
        return "max_holding_days"
    if (
        exits.exit_time is not None
        and decision_time.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%H:%M") == exits.exit_time
    ):
        if (
            quote.basis != "minute_close"
            or quote.event_time != decision_time
            or quote.event_time.second
            or quote.event_time.microsecond
        ):
            raise ValueError("timed exit requires exact minute evidence")
        return "exit_time"
    return None


def strategy_template_runtime(
    rules: StrategyTemplate,
    position: TemplatePosition | None,
    quote: TemplatePrice | None,
    evidence: TemplateEntryProjection,
    *,
    decision_time: datetime,
) -> tuple[str, ...] | str | None:
    if position is not None:
        if quote is None:
            raise ValueError("position requires verified price evidence")
        return strategy_template_exit(rules, position, quote, decision_time=decision_time)
    return strategy_template_entry(rules, evidence, decision_time=decision_time)
