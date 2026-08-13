"""Versioned A-share round-trip execution cost model for research replays."""

from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal, InvalidOperation

import pandas as pd

from rquant.order_execution_costs import (
    BPS_SCALE,
    OrderExecutionCosts,
    OrderSide,
    calculate_order_execution_costs,
)
from rquant.research_run_spec import ExecutionCostSpec

COST_MODEL_VERSION = "a-share-round-trip-v1"
RATE_ONLY_COST_MODEL_VERSION = "a-share-round-trip-rate-only-v2"
NOTIONAL_COST_MODEL_VERSION = "a-share-round-trip-notional-v2"
_LOT_SIZE = Decimal("100")


def _cost_totals(costs: ExecutionCostSpec) -> tuple[Decimal, Decimal]:
    buy_cost_bps = costs.commission_bps + costs.transfer_fee_bps + costs.slippage_bps
    sell_cost_bps = buy_cost_bps + costs.stamp_duty_bps
    return buy_cost_bps, sell_cost_bps


def _validate_positive_cost_factors(costs: ExecutionCostSpec) -> None:
    buy_cost_bps, sell_cost_bps = _cost_totals(costs)
    if Decimal(1) + buy_cost_bps / BPS_SCALE <= 0:
        raise ValueError("buy-side execution cost factor must be positive")
    if Decimal(1) - sell_cost_bps / BPS_SCALE <= 0:
        raise ValueError("sell-side execution cost factor must be positive")


def execution_costs_are_zero(costs: ExecutionCostSpec) -> bool:
    validated = ExecutionCostSpec.model_validate(costs)
    return all(
        value == 0
        for value in (
            validated.commission_bps,
            validated.stamp_duty_bps,
            validated.transfer_fee_bps,
            validated.slippage_bps,
            validated.minimum_commission,
        )
    )


def _finite_decimal(value: object, *, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must contain finite numeric values") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must contain finite numeric values")
    return parsed


def _order_costs(
    *,
    side: OrderSide,
    reference_price: Decimal,
    quantity: int,
    costs: ExecutionCostSpec,
    notional_mode: bool,
) -> OrderExecutionCosts:
    return calculate_order_execution_costs(
        side=side,
        reference_price=reference_price,
        quantity=quantity,
        commission_rate=costs.commission_bps / BPS_SCALE,
        minimum_commission=costs.minimum_commission if notional_mode else Decimal("0"),
        transfer_fee_rate=costs.transfer_fee_bps / BPS_SCALE,
        sell_stamp_duty_rate=costs.stamp_duty_bps / BPS_SCALE,
        slippage_bps=costs.slippage_bps,
        fee_notional_basis=("reference" if costs.schema_version == 1 else "executed"),
        price_tick=Decimal("0.0001") if notional_mode else None,
        money_quantum=Decimal("0.01") if notional_mode else None,
    )


def _net_cash_return_pct(
    buy: OrderExecutionCosts,
    sell: OrderExecutionCosts,
) -> Decimal:
    buy_cash = buy.executed_notional + buy.fee_amount
    sell_cash = sell.executed_notional - sell.fee_amount
    if buy_cash <= 0 or sell_cash < 0:
        raise ValueError("execution costs must retain positive buy and non-negative sell cash")
    return (sell_cash / buy_cash - Decimal("1")) * Decimal("100")


def _rate_only_row(gross_ret_pct: object, costs: ExecutionCostSpec) -> dict[str, object]:
    gross = _finite_decimal(gross_ret_pct, field_name="ret_pct")
    gross_factor = Decimal("1") + gross / Decimal("100")
    if gross_factor <= 0:
        raise ValueError("gross return factor must be positive")
    buy = _order_costs(
        side="buy",
        reference_price=Decimal("1"),
        quantity=1,
        costs=costs,
        notional_mode=False,
    )
    sell = _order_costs(
        side="sell",
        reference_price=gross_factor,
        quantity=1,
        costs=costs,
        notional_mode=False,
    )
    return {
        "ret_pct": float(_net_cash_return_pct(buy, sell)),
        "buy_cost_bps": float(buy.total_cost / buy.reference_notional * BPS_SCALE),
        "sell_cost_bps": float(sell.total_cost / sell.reference_notional * BPS_SCALE),
    }


def _research_quantity(entry_price: Decimal, target_notional: Decimal) -> int:
    lots = (target_notional / entry_price / _LOT_SIZE).to_integral_value(rounding=ROUND_FLOOR)
    quantity = int(lots * _LOT_SIZE)
    if quantity < 100:
        raise ValueError("research notional must fund at least one 100-share lot")
    return quantity


def _notional_row(
    row: pd.Series,
    costs: ExecutionCostSpec,
) -> dict[str, object]:
    assert costs.research_notional_per_trade is not None
    gross = _finite_decimal(row["ret_pct"], field_name="ret_pct")
    if Decimal("1") + gross / Decimal("100") <= 0:
        raise ValueError("gross return factor must be positive")
    entry_price = _finite_decimal(row["entry_price"], field_name="entry_price")
    exit_price = _finite_decimal(row["exit_price"], field_name="exit_price")
    if entry_price <= 0 or exit_price <= 0:
        raise ValueError("entry_price and exit_price must be positive")
    quantity = _research_quantity(entry_price, costs.research_notional_per_trade)
    buy = _order_costs(
        side="buy",
        reference_price=entry_price,
        quantity=quantity,
        costs=costs,
        notional_mode=True,
    )
    sell = _order_costs(
        side="sell",
        reference_price=exit_price,
        quantity=quantity,
        costs=costs,
        notional_mode=True,
    )
    execution_cost_amount = buy.total_cost + sell.total_cost
    return {
        "ret_pct": float(_net_cash_return_pct(buy, sell)),
        "research_quantity": quantity,
        "buy_reference_notional": float(buy.reference_notional),
        "sell_reference_notional": float(sell.reference_notional),
        "buy_executed_notional": float(buy.executed_notional),
        "sell_executed_notional": float(sell.executed_notional),
        "buy_commission_amount": float(buy.commission),
        "sell_commission_amount": float(sell.commission),
        "buy_transfer_fee_amount": float(buy.transfer_fee),
        "sell_transfer_fee_amount": float(sell.transfer_fee),
        "sell_stamp_duty_amount": float(sell.stamp_duty),
        "buy_slippage_amount": float(buy.slippage_amount),
        "sell_slippage_amount": float(sell.slippage_amount),
        "execution_cost_amount": float(execution_cost_amount),
        "buy_cost_bps": float(buy.total_cost / buy.reference_notional * BPS_SCALE),
        "sell_cost_bps": float(sell.total_cost / sell.reference_notional * BPS_SCALE),
        "effective_execution_cost_bps": float(
            execution_cost_amount / buy.reference_notional * BPS_SCALE
        ),
    }


def apply_round_trip_execution_costs(
    trades: pd.DataFrame,
    costs: ExecutionCostSpec,
) -> pd.DataFrame:
    """Apply versioned buy/sell costs to gross ``ret_pct`` with provenance."""
    _validate_positive_cost_factors(costs)
    validated = ExecutionCostSpec.model_validate(costs)
    if execution_costs_are_zero(validated) or trades.empty:
        return trades.copy()
    if "ret_pct" not in trades.columns:
        raise ValueError("nonzero execution costs require a ret_pct trade column")

    notional_mode = validated.research_notional_per_trade is not None
    if notional_mode:
        missing = {"entry_price", "exit_price"}.difference(trades.columns)
        if missing:
            raise ValueError(f"notional execution costs require trade columns: {sorted(missing)}")
        calculated = trades.apply(
            lambda row: _notional_row(row, validated),
            axis=1,
            result_type="expand",
        )
    else:
        calculated = pd.DataFrame(
            [_rate_only_row(value, validated) for value in trades["ret_pct"]],
            index=trades.index,
        )

    output = trades.copy()
    output["gross_ret_pct"] = output["ret_pct"]
    for column in calculated.columns:
        output[column] = calculated[column]
    output["execution_cost_model"] = (
        NOTIONAL_COST_MODEL_VERSION
        if notional_mode
        else COST_MODEL_VERSION
        if validated.schema_version == 1
        else RATE_ONLY_COST_MODEL_VERSION
    )
    output["execution_cost_mode"] = "notional" if notional_mode else "rate_only"
    output["paper_execution_comparable"] = notional_mode
    output["execution_cost_schema_version"] = validated.schema_version
    output["commission_bps"] = float(validated.commission_bps)
    output["stamp_duty_bps"] = float(validated.stamp_duty_bps)
    output["transfer_fee_bps"] = float(validated.transfer_fee_bps)
    output["slippage_bps"] = float(validated.slippage_bps)
    output["minimum_commission"] = float(validated.minimum_commission)
    output["research_notional_per_trade"] = (
        float(validated.research_notional_per_trade)
        if validated.research_notional_per_trade is not None
        else None
    )
    return output
