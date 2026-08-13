from __future__ import annotations

from decimal import Decimal

import pytest


def test_order_costs_apply_minimum_commission_and_side_specific_fees() -> None:
    from rquant.order_execution_costs import calculate_order_execution_costs

    buy = calculate_order_execution_costs(
        side="buy",
        reference_price=Decimal("10"),
        quantity=100,
        commission_rate=Decimal("0.0003"),
        minimum_commission=Decimal("5"),
        transfer_fee_rate=Decimal("0.0001"),
        sell_stamp_duty_rate=Decimal("0.001"),
        slippage_bps=Decimal("10"),
    )
    sell = calculate_order_execution_costs(
        side="sell",
        reference_price=Decimal("11"),
        quantity=100,
        commission_rate=Decimal("0.0003"),
        minimum_commission=Decimal("5"),
        transfer_fee_rate=Decimal("0.0001"),
        sell_stamp_duty_rate=Decimal("0.001"),
        slippage_bps=Decimal("10"),
    )

    assert buy.executed_price == Decimal("10.0100")
    assert buy.executed_notional == Decimal("1001.0000")
    assert buy.commission == Decimal("5.00")
    assert buy.transfer_fee == Decimal("0.10")
    assert buy.stamp_duty == Decimal("0.00")
    assert buy.slippage_amount == Decimal("1.00")
    assert buy.total_cost == Decimal("6.10")
    assert sell.executed_price == Decimal("10.9890")
    assert sell.executed_notional == Decimal("1098.9000")
    assert sell.commission == Decimal("5.00")
    assert sell.transfer_fee == Decimal("0.11")
    assert sell.stamp_duty == Decimal("1.10")
    assert sell.slippage_amount == Decimal("1.10")
    assert sell.total_cost == Decimal("7.31")


def test_order_costs_use_rate_commission_above_minimum_threshold_on_both_sides() -> None:
    from rquant.order_execution_costs import calculate_order_execution_costs

    buy = calculate_order_execution_costs(
        side="buy",
        reference_price=Decimal("10"),
        quantity=10_000,
        commission_rate=Decimal("0.001"),
        minimum_commission=Decimal("5"),
        transfer_fee_rate=Decimal("0"),
        sell_stamp_duty_rate=Decimal("0.001"),
        slippage_bps=Decimal("0"),
    )
    sell = calculate_order_execution_costs(
        side="sell",
        reference_price=Decimal("11"),
        quantity=10_000,
        commission_rate=Decimal("0.001"),
        minimum_commission=Decimal("5"),
        transfer_fee_rate=Decimal("0"),
        sell_stamp_duty_rate=Decimal("0.001"),
        slippage_bps=Decimal("0"),
    )

    assert buy.commission == Decimal("100.00")
    assert sell.commission == Decimal("110.00")
    assert buy.stamp_duty == Decimal("0.00")
    assert sell.stamp_duty == Decimal("110.00")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("side", "hold", "side"),
        ("fee_notional_basis", "unknown", "fee_notional_basis"),
    ],
)
def test_order_costs_reject_unknown_units(
    field: str,
    value: str,
    message: str,
) -> None:
    from rquant.order_execution_costs import calculate_order_execution_costs

    values: dict[str, object] = {
        "side": "buy",
        "reference_price": Decimal("10"),
        "quantity": 100,
        "commission_rate": Decimal("0.0003"),
        "minimum_commission": Decimal("5"),
        "transfer_fee_rate": Decimal("0"),
        "sell_stamp_duty_rate": Decimal("0.001"),
        "slippage_bps": Decimal("0"),
        "fee_notional_basis": "executed",
    }
    values[field] = value

    with pytest.raises(ValueError, match=message):
        calculate_order_execution_costs(**values)  # type: ignore[arg-type]
