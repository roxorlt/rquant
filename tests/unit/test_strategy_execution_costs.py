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
    schema_version: int = 1,
    minimum: str = "0",
    notional: str | None = None,
) -> ExecutionCostSpec:
    values: dict[str, object] = dict(
        schema_version=schema_version,
        commission_bps=Decimal(commission),
        stamp_duty_bps=Decimal(stamp),
        transfer_fee_bps=Decimal(transfer),
        slippage_bps=Decimal(slippage),
    )
    if schema_version == 2:
        values["minimum_commission"] = Decimal(minimum)
        values["research_notional_per_trade"] = None if notional is None else Decimal(notional)
    return ExecutionCostSpec.model_validate(values)


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
        Decimal("1.1") * (1 - Decimal("18") / 10_000) / (1 + Decimal("13") / 10_000) - 1
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


def test_notional_costs_emit_cash_amounts_effective_bps_and_provenance() -> None:
    from rquant.strategy_execution_costs import apply_round_trip_execution_costs

    actual = apply_round_trip_execution_costs(
        pd.DataFrame(
            [
                {
                    "entry_price": 10.0,
                    "exit_price": 11.0,
                    "ret_pct": 10.0,
                }
            ]
        ),
        _costs(
            schema_version=2,
            commission="30",
            minimum="5",
            notional="1000",
        ),
    )

    expected_net = (Decimal("1095") / Decimal("1005") - 1) * 100
    row = actual.iloc[0]
    assert row["gross_ret_pct"] == 10.0
    assert row["ret_pct"] == pytest.approx(float(expected_net))
    assert row["research_quantity"] == 100
    assert row["buy_commission_amount"] == 5.0
    assert row["sell_commission_amount"] == 5.0
    assert row["execution_cost_amount"] == 10.0
    assert row["effective_execution_cost_bps"] == 100.0
    assert row["execution_cost_mode"] == "notional"
    assert not bool(row["paper_execution_comparable"])
    assert row["paper_execution_comparability_reason"] == "UNBOUND_RESEARCH_COST"
    assert row["minimum_commission"] == 5.0
    assert row["research_notional_per_trade"] == 1000.0


def test_v2_rate_only_costs_are_explicitly_not_paper_comparable() -> None:
    from rquant.strategy_execution_costs import apply_round_trip_execution_costs

    actual = apply_round_trip_execution_costs(
        pd.DataFrame([{"ret_pct": 10.0}]),
        _costs(schema_version=2, commission="3"),
    )

    assert actual.loc[0, "execution_cost_mode"] == "rate_only"
    assert not bool(actual.loc[0, "paper_execution_comparable"])
    assert "execution_cost_amount" not in actual.columns
