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
