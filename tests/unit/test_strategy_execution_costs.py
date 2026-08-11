from __future__ import annotations

from decimal import Decimal

import pandas as pd
import pytest

from rquant.research_run_spec import ExecutionCostSpec


def _costs(
    *,
    commission: str = "0",
    stamp: str = "0",
    transfer: str = "0",
    slippage: str = "0",
) -> ExecutionCostSpec:
    return ExecutionCostSpec(
        commission_bps=Decimal(commission),
        stamp_duty_bps=Decimal(stamp),
        transfer_fee_bps=Decimal(transfer),
        slippage_bps=Decimal(slippage),
    )


def test_zero_execution_costs_preserve_legacy_rows_exactly() -> None:
    from rquant.strategy_execution_costs import apply_round_trip_execution_costs

    legacy = pd.DataFrame([{"ret_pct": 10.0, "name": "fixture"}])

    actual = apply_round_trip_execution_costs(legacy, _costs())

    pd.testing.assert_frame_equal(actual, legacy)


def test_round_trip_cost_formula_uses_multiplicative_gross_factor() -> None:
    from rquant.strategy_execution_costs import (
        COST_MODEL_VERSION,
        apply_round_trip_execution_costs,
    )

    costs = _costs(commission="10", stamp="5", transfer="1", slippage="2")
    actual = apply_round_trip_execution_costs(pd.DataFrame([{"ret_pct": 10.0}]), costs)
    expected = (
        Decimal("1.1")
        * (1 - Decimal("18") / 10_000)
        / (1 + Decimal("13") / 10_000)
        - 1
    ) * 100

    assert actual.loc[0, "ret_pct"] == pytest.approx(float(expected))
    assert actual.loc[0, "gross_ret_pct"] == 10.0
    assert actual.loc[0, "execution_cost_model"] == COST_MODEL_VERSION
    assert actual.loc[0, "buy_cost_bps"] == 13.0
    assert actual.loc[0, "sell_cost_bps"] == 18.0


def test_nonzero_costs_fail_closed_when_trade_return_is_unavailable() -> None:
    from rquant.strategy_execution_costs import apply_round_trip_execution_costs

    with pytest.raises(ValueError, match="ret_pct"):
        apply_round_trip_execution_costs(
            pd.DataFrame([{"trade_id": "missing-return"}]),
            _costs(commission="1"),
        )


def test_execution_cost_spec_rejects_nonpositive_sell_factor() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="sell-side.*10000"):
        _costs(commission="6000", stamp="6000")


def test_apply_defensively_rejects_nonpositive_factors() -> None:
    from rquant.strategy_execution_costs import apply_round_trip_execution_costs

    invalid = ExecutionCostSpec.model_construct(
        commission_bps=Decimal("6000"),
        stamp_duty_bps=Decimal("6000"),
        transfer_fee_bps=Decimal("0"),
        slippage_bps=Decimal("0"),
    )
    with pytest.raises(ValueError, match="sell-side.*factor"):
        apply_round_trip_execution_costs(pd.DataFrame([{"ret_pct": 1.0}]), invalid)
    with pytest.raises(ValueError, match="gross return factor"):
        apply_round_trip_execution_costs(
            pd.DataFrame([{"ret_pct": -100.0}]),
            _costs(commission="1"),
        )


def test_valid_cost_application_never_returns_less_than_total_loss() -> None:
    from rquant.strategy_execution_costs import apply_round_trip_execution_costs

    actual = apply_round_trip_execution_costs(
        pd.DataFrame([{"ret_pct": -99.9}, {"ret_pct": 10.0}]),
        _costs(commission="10", stamp="5", transfer="1", slippage="2"),
    )

    assert (actual["ret_pct"] > -100).all()
