from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant.portfolio import (
    DrawdownInputError,
    DrawdownRule,
    DrawdownState,
    evaluate_drawdown,
)


def observed(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, 27, hour, minute, tzinfo=UTC)


def block_rule() -> DrawdownRule:
    return DrawdownRule(
        trigger_drawdown=Decimal("0.20"),
        release_drawdown=Decimal("0.05"),
        action="block_new_positions",
    )


def test_hand_calculated_peak_drawdown_trigger_and_hysteresis() -> None:
    rule = block_rule()
    first = evaluate_drawdown(Decimal("100"), observed(9), rule)
    ten_percent = evaluate_drawdown(Decimal("90"), observed(10), rule, first.state)
    trigger = evaluate_drawdown(Decimal("80"), observed(11), rule, ten_percent.state)
    still_active = evaluate_drawdown(Decimal("85"), observed(12), rule, trigger.state)
    recovered = evaluate_drawdown(Decimal("95"), observed(13), rule, still_active.state)

    assert [step.drawdown for step in (first, ten_percent, trigger, still_active, recovered)] == [
        Decimal("0"),
        Decimal("0.10"),
        Decimal("0.20"),
        Decimal("0.15"),
        Decimal("0.05"),
    ]
    assert [
        step.state.active for step in (first, ten_percent, trigger, still_active, recovered)
    ] == [False, False, True, True, False]
    assert [
        step.allow_new_positions for step in (first, ten_percent, trigger, still_active, recovered)
    ] == [True, True, False, False, True]
    assert all(
        step.state.peak_nav == Decimal("100")
        for step in (first, ten_percent, trigger, still_active, recovered)
    )


def test_exact_trigger_and_release_edges_use_present_observation_only() -> None:
    rule = block_rule()
    first = evaluate_drawdown(Decimal("100"), observed(9), rule)
    just_above_trigger = evaluate_drawdown(Decimal("80.01"), observed(10), rule, first.state)
    triggered = evaluate_drawdown(Decimal("80.00"), observed(11), rule, just_above_trigger.state)
    just_below_release = evaluate_drawdown(Decimal("94.99"), observed(12), rule, triggered.state)
    released = evaluate_drawdown(Decimal("95.00"), observed(13), rule, just_below_release.state)

    assert just_above_trigger.state.active is False
    assert triggered.state.active is True
    assert just_below_release.state.active is True
    assert released.state.active is False


def test_new_high_updates_peak_and_revisiting_equal_high_keeps_first_peak_time() -> None:
    rule = block_rule()
    first = evaluate_drawdown(Decimal("100"), observed(9), rule)
    below = evaluate_drawdown(Decimal("80"), observed(10), rule, first.state)
    new_high = evaluate_drawdown(Decimal("120"), observed(11), rule, below.state)
    equal_high = evaluate_drawdown(Decimal("120"), observed(12), rule, new_high.state)

    assert new_high.state.peak_nav == Decimal("120")
    assert new_high.state.peak_at == observed(11)
    assert new_high.drawdown == 0
    assert new_high.state.active is False
    assert equal_high.state.peak_at == observed(11)


def test_cap_action_returns_target_risk_limit_only_while_active() -> None:
    rule = DrawdownRule(
        trigger_drawdown=Decimal("0.10"),
        release_drawdown=Decimal("0.02"),
        action="cap_total_risk_weight",
        total_risk_weight_cap=Decimal("0.40"),
    )
    first = evaluate_drawdown(Decimal("100"), observed(9), rule)
    capped = evaluate_drawdown(Decimal("90"), observed(10), rule, first.state)
    recovered = evaluate_drawdown(Decimal("98"), observed(11), rule, capped.state)

    assert first.max_total_risk_weight is None
    assert capped.state.active is True
    assert capped.allow_new_positions is True
    assert capped.max_total_risk_weight == Decimal("0.40")
    assert recovered.state.active is False
    assert recovered.max_total_risk_weight is None


def test_lost_state_starts_a_new_peak_so_caller_must_preserve_state() -> None:
    rule = block_rule()
    prior = evaluate_drawdown(Decimal("100"), observed(9), rule)
    with_state = evaluate_drawdown(Decimal("80"), observed(10), rule, prior.state)
    without_state = evaluate_drawdown(Decimal("80"), observed(10), rule)

    assert with_state.state.active is True
    assert without_state.state.active is False
    assert without_state.state.peak_nav == Decimal("80")


@pytest.mark.parametrize("nav", [Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")])
def test_invalid_current_nav_is_rejected(nav: Decimal) -> None:
    with pytest.raises(DrawdownInputError, match="净值"):
        evaluate_drawdown(nav, observed(9), block_rule())


def test_equal_backward_and_naive_observation_times_are_rejected() -> None:
    rule = block_rule()
    first = evaluate_drawdown(Decimal("100"), observed(10), rule)
    for at in (observed(10), observed(9), datetime(2026, 9, 27, 11)):
        with pytest.raises(DrawdownInputError, match="时间"):
            evaluate_drawdown(Decimal("90"), at, rule, first.state)


def test_timezone_offsets_compare_by_instant_not_wall_clock() -> None:
    rule = block_rule()
    first = evaluate_drawdown(Decimal("100"), observed(9), rule)
    same_instant = observed(9).astimezone(timezone(timedelta(hours=8)))
    later_instant = observed(10).astimezone(timezone(timedelta(hours=8)))

    with pytest.raises(DrawdownInputError, match="时间"):
        evaluate_drawdown(Decimal("90"), same_instant, rule, first.state)
    later = evaluate_drawdown(Decimal("90"), later_instant, rule, first.state)
    assert later.state.last_at == later_instant


def test_dst_fold_rejects_actual_time_reversal_despite_later_wall_clock() -> None:
    ny = ZoneInfo("America/New_York")
    later_instant = datetime(2026, 11, 1, 1, 15, tzinfo=ny, fold=1)
    earlier_instant = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=0)
    rule = block_rule()
    first = evaluate_drawdown(Decimal("100"), later_instant, rule)

    with pytest.raises(DrawdownInputError, match="时间"):
        evaluate_drawdown(Decimal("90"), earlier_instant, rule, first.state)
    with pytest.raises(ValidationError, match="峰值时间"):
        DrawdownState(
            rule=rule,
            peak_nav=Decimal("100"),
            peak_at=later_instant,
            last_nav=Decimal("90"),
            last_at=earlier_instant,
            active=False,
        )


def test_dst_fold_accepts_actual_forward_time_despite_earlier_wall_clock() -> None:
    ny = ZoneInfo("America/New_York")
    earlier_instant = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=0)
    later_instant = datetime(2026, 11, 1, 1, 15, tzinfo=ny, fold=1)
    rule = block_rule()
    first = evaluate_drawdown(Decimal("100"), earlier_instant, rule)

    next_step = evaluate_drawdown(Decimal("90"), later_instant, rule, first.state)
    assert next_step.drawdown == Decimal("0.10")
    assert next_step.state.last_at == later_instant


@pytest.mark.parametrize(
    "kwargs",
    [
        {"trigger_drawdown": Decimal("0"), "action": "block_new_positions"},
        {"trigger_drawdown": Decimal("1"), "action": "block_new_positions"},
        {"trigger_drawdown": Decimal("NaN"), "action": "block_new_positions"},
        {
            "trigger_drawdown": Decimal("0.2"),
            "release_drawdown": Decimal("0.2"),
            "action": "block_new_positions",
        },
        {
            "trigger_drawdown": Decimal("0.2"),
            "release_drawdown": Decimal("-0.1"),
            "action": "block_new_positions",
        },
        {"trigger_drawdown": Decimal("0.2"), "action": "cap_total_risk_weight"},
        {
            "trigger_drawdown": Decimal("0.2"),
            "action": "cap_total_risk_weight",
            "total_risk_weight_cap": Decimal("1"),
        },
        {
            "trigger_drawdown": Decimal("0.2"),
            "action": "block_new_positions",
            "total_risk_weight_cap": Decimal("0.4"),
        },
    ],
)
def test_invalid_rule_is_rejected(kwargs: dict) -> None:
    with pytest.raises(ValidationError):
        DrawdownRule(**kwargs)


def test_inconsistent_peak_state_is_rejected() -> None:
    rule = block_rule()
    with pytest.raises(ValidationError, match="峰值"):
        DrawdownState(
            rule=rule,
            peak_nav=Decimal("90"),
            peak_at=observed(9),
            last_nav=Decimal("100"),
            last_at=observed(10),
            active=False,
        )
    with pytest.raises(ValidationError, match="峰值"):
        DrawdownState(
            rule=rule,
            peak_nav=Decimal("100"),
            peak_at=observed(11),
            last_nav=Decimal("90"),
            last_at=observed(10),
            active=False,
        )
    with pytest.raises(ValidationError, match="峰值"):
        DrawdownState(
            rule=rule,
            peak_nav=Decimal("100"),
            peak_at=observed(10),
            last_nav=Decimal("90"),
            last_at=observed(10),
            active=False,
        )


@pytest.mark.parametrize(
    ("last_nav", "active"),
    [(Decimal("100"), True), (Decimal("80"), False)],
)
def test_state_activity_must_match_its_own_drawdown(last_nav: Decimal, active: bool) -> None:
    with pytest.raises(ValidationError, match="回撤状态"):
        DrawdownState(
            rule=block_rule(),
            peak_nav=Decimal("100"),
            peak_at=observed(9),
            last_nav=last_nav,
            last_at=observed(10),
            active=active,
        )


def test_changed_rule_requires_new_series_or_historical_rebuild() -> None:
    first = evaluate_drawdown(Decimal("100"), observed(9), block_rule())
    changed = DrawdownRule(trigger_drawdown=Decimal("0.15"), action="block_new_positions")
    with pytest.raises(DrawdownInputError, match="规则"):
        evaluate_drawdown(Decimal("90"), observed(10), changed, first.state)
