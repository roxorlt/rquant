"""Pure price-alert evaluation from explicit, validated market evidence."""

from __future__ import annotations

from datetime import date, time, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StrictBool, StrictInt, StringConstraints, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel

TsCode = Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MORNING_OPEN = time(9, 30)
_MORNING_CLOSE = time(11, 30)
_AFTERNOON_OPEN = time(13, 0)
_AFTERNOON_CLOSE = time(14, 57)


class PriceRuleReason(StrEnum):
    THRESHOLD_REACHED = "threshold_reached"
    THRESHOLD_NOT_REACHED = "threshold_not_reached"
    DISABLED = "disabled"
    MARKET_CLOSED = "market_closed"
    OUTSIDE_CONTINUOUS_SESSION = "outside_continuous_session"
    OUTSIDE_RULE_WINDOW = "outside_rule_window"
    OUT_OF_SCOPE = "out_of_scope"
    MARKET_DAY_UNKNOWN = "market_day_unknown"
    WRONG_MARKET_DAY = "wrong_market_day"
    SCOPE_UNKNOWN = "scope_unknown"
    QUOTE_MISSING = "quote_missing"
    WRONG_QUOTE_CODE = "wrong_quote_code"
    WRONG_QUOTE_DAY = "wrong_quote_day"
    FUTURE_QUOTE = "future_quote"
    STALE_QUOTE = "stale_quote"
    QUOTE_OUTSIDE_CONTINUOUS_SESSION = "quote_outside_continuous_session"


class PriceAlertRule(RuntimeContractModel):
    rule_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=80)
    priority: Literal["P0", "P1", "P2", "P3"]
    enabled: StrictBool
    comparison: Literal["gte", "lte"]
    threshold: Decimal = Field(gt=0, allow_inf_nan=False)
    valid_from: time
    valid_until: time

    @model_validator(mode="after")
    def require_valid_local_window(self) -> Self:
        if self.valid_from.tzinfo is not None or self.valid_until.tzinfo is not None:
            raise ValueError("price rule window must use Shanghai wall-clock times")
        if self.valid_from >= self.valid_until:
            raise ValueError("price rule window must have a positive duration")
        return self


class ObservedPriceQuote(RuntimeContractModel):
    ts_code: TsCode
    price: Decimal = Field(gt=0, allow_inf_nan=False)
    observed_at: AwareUtcDatetime
    trade_date: date


class MarketDayEvidence(RuntimeContractModel):
    trade_date: date | None
    is_trading_day: StrictBool | None


class ScopeMembershipEvidence(RuntimeContractModel):
    member_codes: frozenset[TsCode] | None


class PriceAlertEvaluationContext(RuntimeContractModel):
    ts_code: TsCode
    quote: ObservedPriceQuote | None
    evaluated_at: AwareUtcDatetime
    market: MarketDayEvidence
    scope: ScopeMembershipEvidence
    max_quote_age_seconds: StrictInt = Field(gt=0)


class PriceRuleDecision(RuntimeContractModel):
    rule_id: str
    ts_code: TsCode
    state: Literal["triggered", "not_triggered", "unavailable"]
    reason: PriceRuleReason


def evaluate_price_rule(
    rule: PriceAlertRule, context: PriceAlertEvaluationContext
) -> PriceRuleDecision:
    """Compare a verified quote only in the rule's active continuous session."""

    def decision(
        state: Literal["triggered", "not_triggered", "unavailable"],
        reason: PriceRuleReason,
    ) -> PriceRuleDecision:
        return PriceRuleDecision(
            rule_id=rule.rule_id, ts_code=context.ts_code, state=state, reason=reason
        )

    if not rule.enabled:
        return decision("not_triggered", PriceRuleReason.DISABLED)

    local = context.evaluated_at.astimezone(_SHANGHAI)
    wall = local.time()
    if _MORNING_OPEN <= wall < _MORNING_CLOSE:
        session_start = _MORNING_OPEN
    elif _AFTERNOON_OPEN <= wall < _AFTERNOON_CLOSE:
        session_start = _AFTERNOON_OPEN
    else:
        return decision("not_triggered", PriceRuleReason.OUTSIDE_CONTINUOUS_SESSION)
    if not rule.valid_from <= wall < rule.valid_until:
        return decision("not_triggered", PriceRuleReason.OUTSIDE_RULE_WINDOW)

    market = context.market
    if market.trade_date is None:
        return decision("unavailable", PriceRuleReason.MARKET_DAY_UNKNOWN)
    if market.trade_date != local.date():
        return decision("unavailable", PriceRuleReason.WRONG_MARKET_DAY)
    if market.is_trading_day is False:
        return decision("not_triggered", PriceRuleReason.MARKET_CLOSED)
    if market.is_trading_day is None:
        return decision("unavailable", PriceRuleReason.MARKET_DAY_UNKNOWN)

    members = context.scope.member_codes
    if members is None:
        return decision("unavailable", PriceRuleReason.SCOPE_UNKNOWN)
    if context.ts_code not in members:
        return decision("not_triggered", PriceRuleReason.OUT_OF_SCOPE)

    quote = context.quote
    if quote is None:
        return decision("unavailable", PriceRuleReason.QUOTE_MISSING)
    if quote.ts_code != context.ts_code:
        return decision("unavailable", PriceRuleReason.WRONG_QUOTE_CODE)
    if (
        quote.trade_date != market.trade_date
        or quote.observed_at.astimezone(_SHANGHAI).date() != market.trade_date
    ):
        return decision("unavailable", PriceRuleReason.WRONG_QUOTE_DAY)
    age = context.evaluated_at - quote.observed_at
    if age < timedelta(0):
        return decision("unavailable", PriceRuleReason.FUTURE_QUOTE)
    if age > timedelta(seconds=context.max_quote_age_seconds):
        return decision("unavailable", PriceRuleReason.STALE_QUOTE)
    if quote.observed_at.astimezone(_SHANGHAI).time() < session_start:
        return decision("unavailable", PriceRuleReason.QUOTE_OUTSIDE_CONTINUOUS_SESSION)

    reached = (
        quote.price >= rule.threshold if rule.comparison == "gte" else quote.price <= rule.threshold
    )
    if reached:
        return decision("triggered", PriceRuleReason.THRESHOLD_REACHED)
    return decision("not_triggered", PriceRuleReason.THRESHOLD_NOT_REACHED)
