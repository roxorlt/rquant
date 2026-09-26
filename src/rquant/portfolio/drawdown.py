"""Pure as-of-NAV drawdown state and risk decisions for research portfolios."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal, localcontext
from fractions import Fraction
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_DRAWDOWN_QUANTUM = Decimal("0.000000000000000001")


class DrawdownInputError(ValueError):
    """A NAV observation cannot continue this drawdown series."""


def _require_aware_time(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("观测时间必须带时区")
    return value


def _utc_instant(value: datetime) -> datetime:
    return _require_aware_time(value).astimezone(UTC)


class DrawdownRule(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    trigger_drawdown: Decimal = Field(gt=0, lt=1, allow_inf_nan=False)
    release_drawdown: Decimal = Field(default=Decimal("0"), ge=0, lt=1, allow_inf_nan=False)
    action: Literal["block_new_positions", "cap_total_risk_weight"]
    total_risk_weight_cap: Decimal | None = Field(default=None, ge=0, lt=1, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_action_and_hysteresis(self) -> Self:
        if self.release_drawdown >= self.trigger_drawdown:
            raise ValueError("恢复回撤必须低于触发回撤")
        if self.action == "cap_total_risk_weight" and self.total_risk_weight_cap is None:
            raise ValueError("降仓动作需要总风险仓位上限")
        if self.action == "block_new_positions" and self.total_risk_weight_cap is not None:
            raise ValueError("停止开新仓动作不接受总风险仓位上限")
        return self


class DrawdownState(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rule: DrawdownRule
    peak_nav: Decimal = Field(gt=0, allow_inf_nan=False)
    peak_at: datetime
    last_nav: Decimal = Field(gt=0, allow_inf_nan=False)
    last_at: datetime
    active: bool = Field(strict=True)

    @field_validator("peak_at", "last_at")
    @classmethod
    def require_aware_time(cls, value: datetime) -> datetime:
        return _require_aware_time(value)

    @model_validator(mode="after")
    def validate_peak(self) -> Self:
        if self.peak_nav < self.last_nav:
            raise ValueError("峰值净值不能低于末次净值")
        peak_instant = _utc_instant(self.peak_at)
        last_instant = _utc_instant(self.last_at)
        if peak_instant > last_instant:
            raise ValueError("峰值时间不能晚于末次观测时间")
        if peak_instant == last_instant and self.peak_nav != self.last_nav:
            raise ValueError("峰值与末次观测同刻时净值必须一致")
        drawdown = (Fraction(self.peak_nav) - Fraction(self.last_nav)) / Fraction(self.peak_nav)
        if self.active and drawdown <= Fraction(self.rule.release_drawdown):
            raise ValueError("回撤状态已达到恢复边界，不能仍标记为限制中")
        if not self.active and drawdown >= Fraction(self.rule.trigger_drawdown):
            raise ValueError("回撤状态已达到触发边界，不能标记为未限制")
        return self


class DrawdownDecision(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    state: DrawdownState
    drawdown: Decimal
    allow_new_positions: bool
    max_total_risk_weight: Decimal | None


def _display_drawdown(exact: Fraction) -> Decimal:
    if not exact:
        return Decimal("0")
    with localcontext() as context:
        context.prec = max(40, len(str(exact.numerator)) + len(str(exact.denominator)) + 20)
        return (Decimal(exact.numerator) / Decimal(exact.denominator)).quantize(
            _DRAWDOWN_QUANTUM, rounding=ROUND_DOWN
        )


def evaluate_drawdown(
    nav: Decimal,
    observed_at: datetime,
    rule: DrawdownRule,
    previous: DrawdownState | None = None,
) -> DrawdownDecision:
    """Evaluate only current/past NAV; caller saves the returned state for next time.

    Blocking new positions or capping target risk does not block exits/reductions.
    The caller applies this decision; this function never edits holdings or orders.
    """
    if not isinstance(rule, DrawdownRule):
        raise DrawdownInputError("回撤规则必须使用已校验模型")
    if not isinstance(nav, Decimal) or not nav.is_finite() or nav <= 0:
        raise DrawdownInputError("净值必须是有限正数")
    try:
        _require_aware_time(observed_at)
    except ValueError as exc:
        raise DrawdownInputError(str(exc)) from exc
    if previous is not None:
        if not isinstance(previous, DrawdownState):
            raise DrawdownInputError("前态必须使用已校验模型")
        if previous.rule != rule:
            raise DrawdownInputError("回撤规则变化须从历史重算或开启新序列")
        if _utc_instant(observed_at) <= _utc_instant(previous.last_at):
            raise DrawdownInputError("观测时间必须严格晚于前次观测")

    if previous is None or nav > previous.peak_nav:
        peak_nav, peak_at = nav, observed_at
    else:
        peak_nav, peak_at = previous.peak_nav, previous.peak_at
    exact_drawdown = (Fraction(peak_nav) - Fraction(nav)) / Fraction(peak_nav)
    if previous is not None and previous.active:
        active = exact_drawdown > Fraction(rule.release_drawdown)
    else:
        active = exact_drawdown >= Fraction(rule.trigger_drawdown)
    state = DrawdownState(
        rule=rule,
        peak_nav=peak_nav,
        peak_at=peak_at,
        last_nav=nav,
        last_at=observed_at,
        active=active,
    )
    return DrawdownDecision(
        state=state,
        drawdown=_display_drawdown(exact_drawdown),
        allow_new_positions=not (active and rule.action == "block_new_positions"),
        max_total_risk_weight=(
            rule.total_risk_weight_cap
            if active and rule.action == "cap_total_risk_weight"
            else None
        ),
    )
