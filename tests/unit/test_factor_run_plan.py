"""Trusted run planning keeps complete history and forward visibility anchors."""

from datetime import date, timedelta

import pytest


def test_period_plan_keeps_warmup_and_complete_forward_anchor() -> None:
    from rquant.factor.run_plan import compile_factor_run_schedule

    days = tuple(date(2026, 9, 1) + timedelta(days=i) for i in range(40))
    plan = compile_factor_run_schedule(
        days,
        start_date=days[5],
        end_date=days[16],
        holding_sessions=5,
        history_window=3,
    )
    assert plan.evaluation_days == (days[5], days[10], days[15])
    assert plan.calculation_days == days[3:20]
    assert plan.panel_day == days[2]
    assert plan.return_visibility_day == days[20]


@pytest.mark.parametrize("history_window, end_offset", [(7, 20), (2, 38)])
def test_plan_refuses_missing_history_or_tail_instead_of_shortening(
    history_window: int,
    end_offset: int,
) -> None:
    from rquant.factor.run_plan import compile_factor_run_schedule

    days = tuple(date(2026, 9, 1) + timedelta(days=i) for i in range(40))
    with pytest.raises(ValueError):
        compile_factor_run_schedule(
            days,
            start_date=days[5],
            end_date=days[end_offset],
            holding_sessions=5,
            history_window=history_window,
        )


def test_plan_refuses_over_1024_calculation_days() -> None:
    from rquant.factor.run_plan import compile_factor_run_schedule

    days = tuple(date(2026, 1, 1) + timedelta(days=i) for i in range(1027))
    with pytest.raises(ValueError, match="1024"):
        compile_factor_run_schedule(
            days, start_date=days[1], end_date=days[-2], holding_sessions=1, history_window=1
        )


def test_plan_empty_trading_interval_is_editable_parameter_rejection() -> None:
    from rquant.factor.run_plan import FactorRunPlanRejectedError, compile_factor_run_schedule

    first = date(2026, 9, 1)
    days = tuple(first + timedelta(days=i) for i in (0, 2, 4))
    with pytest.raises(FactorRunPlanRejectedError) as failure:
        compile_factor_run_schedule(
            days,
            start_date=first + timedelta(days=1),
            end_date=first + timedelta(days=1),
            holding_sessions=1,
            history_window=1,
        )
    assert failure.value.reason == "所选区间没有交易日，请调整日期。"
