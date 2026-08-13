"""Pure, side-aware order execution cost calculations."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

OrderSide = Literal["buy", "sell"]
FeeNotionalBasis = Literal["reference", "executed"]
BPS_SCALE = Decimal("10000")
DEFAULT_PRICE_TICK = Decimal("0.0001")
DEFAULT_MONEY_QUANTUM = Decimal("0.01")


class OrderExecutionCosts(BaseModel):
    model_config = ConfigDict(frozen=True)

    side: OrderSide
    quantity: int
    reference_price: Decimal
    executed_price: Decimal
    reference_notional: Decimal
    executed_notional: Decimal
    commission: Decimal
    transfer_fee: Decimal
    stamp_duty: Decimal
    slippage_amount: Decimal
    fee_amount: Decimal
    total_cost: Decimal


def _require_nonnegative(value: Decimal, *, field_name: str) -> Decimal:
    if not value.is_finite() or value < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return value


def _quantize(value: Decimal, quantum: Decimal | None) -> Decimal:
    if quantum is None:
        return value
    if not quantum.is_finite() or quantum <= 0:
        raise ValueError("rounding quantum must be finite and positive")
    return value.quantize(quantum, rounding=ROUND_HALF_UP)


def calculate_slipped_price(
    reference_price: Decimal,
    *,
    side: OrderSide,
    slippage_bps: Decimal,
    price_tick: Decimal | None = DEFAULT_PRICE_TICK,
) -> Decimal:
    if side not in {"buy", "sell"}:
        raise ValueError("order side must be buy or sell")
    if not reference_price.is_finite() or reference_price <= 0:
        raise ValueError("reference_price must be finite and positive")
    _require_nonnegative(slippage_bps, field_name="slippage_bps")
    if slippage_bps >= BPS_SCALE:
        raise ValueError("slippage_bps must be below 10000")
    direction = Decimal("1") if side == "buy" else Decimal("-1")
    executed = reference_price * (Decimal("1") + direction * slippage_bps / BPS_SCALE)
    if executed <= 0:
        raise ValueError("slippage-adjusted execution price must be positive")
    return _quantize(executed, price_tick)


def calculate_order_execution_costs(
    *,
    side: OrderSide,
    reference_price: Decimal,
    quantity: int,
    commission_rate: Decimal,
    minimum_commission: Decimal,
    transfer_fee_rate: Decimal,
    sell_stamp_duty_rate: Decimal,
    slippage_bps: Decimal,
    fee_notional_basis: FeeNotionalBasis = "executed",
    price_tick: Decimal | None = DEFAULT_PRICE_TICK,
    money_quantum: Decimal | None = DEFAULT_MONEY_QUANTUM,
) -> OrderExecutionCosts:
    """Calculate one buy or sell order with explicit side and unit semantics."""
    if fee_notional_basis not in {"reference", "executed"}:
        raise ValueError("fee_notional_basis must be reference or executed")
    if type(quantity) is not int or quantity <= 0:
        raise ValueError("quantity must be a positive integer")
    for field_name, value in (
        ("commission_rate", commission_rate),
        ("minimum_commission", minimum_commission),
        ("transfer_fee_rate", transfer_fee_rate),
        ("sell_stamp_duty_rate", sell_stamp_duty_rate),
    ):
        _require_nonnegative(value, field_name=field_name)
    if any(value >= 1 for value in (commission_rate, transfer_fee_rate, sell_stamp_duty_rate)):
        raise ValueError("order fee rates must be below one")

    executed_price = calculate_slipped_price(
        reference_price,
        side=side,
        slippage_bps=slippage_bps,
        price_tick=price_tick,
    )
    reference_notional = reference_price * quantity
    executed_notional = executed_price * quantity
    fee_notional = reference_notional if fee_notional_basis == "reference" else executed_notional
    commission = _quantize(
        max(fee_notional * commission_rate, minimum_commission),
        money_quantum,
    )
    transfer_fee = _quantize(fee_notional * transfer_fee_rate, money_quantum)
    stamp_duty = _quantize(
        fee_notional * sell_stamp_duty_rate if side == "sell" else Decimal("0"),
        money_quantum,
    )
    slippage_amount = _quantize(
        abs(executed_notional - reference_notional),
        money_quantum,
    )
    fee_amount = commission + transfer_fee + stamp_duty
    return OrderExecutionCosts(
        side=side,
        quantity=quantity,
        reference_price=reference_price,
        executed_price=executed_price,
        reference_notional=reference_notional,
        executed_notional=executed_notional,
        commission=commission,
        transfer_fee=transfer_fee,
        stamp_duty=stamp_duty,
        slippage_amount=slippage_amount,
        fee_amount=fee_amount,
        total_cost=fee_amount + slippage_amount,
    )
