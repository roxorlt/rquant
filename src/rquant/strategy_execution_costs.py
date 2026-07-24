"""Versioned A-share round-trip execution cost model for research replays."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

import pandas as pd

from rquant.research_run_spec import ExecutionCostSpec

COST_MODEL_VERSION = "a-share-round-trip-v1"
_BPS_SCALE = Decimal(10_000)


def _cost_totals(costs: ExecutionCostSpec) -> tuple[Decimal, Decimal]:
    buy_cost_bps = (
        costs.commission_bps + costs.transfer_fee_bps + costs.slippage_bps
    )
    sell_cost_bps = buy_cost_bps + costs.stamp_duty_bps
    return buy_cost_bps, sell_cost_bps


def _validate_positive_cost_factors(costs: ExecutionCostSpec) -> None:
    buy_cost_bps, sell_cost_bps = _cost_totals(costs)
    if Decimal(1) + buy_cost_bps / _BPS_SCALE <= 0:
        raise ValueError("buy-side execution cost factor must be positive")
    if Decimal(1) - sell_cost_bps / _BPS_SCALE <= 0:
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
        )
    )


def _net_return_pct(
    gross_ret_pct: object,
    *,
    buy_cost_bps: Decimal,
    sell_cost_bps: Decimal,
) -> float:
    try:
        gross = Decimal(str(gross_ret_pct))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("ret_pct must contain finite numeric values") from exc
    if not gross.is_finite():
        raise ValueError("ret_pct must contain finite numeric values")
    gross_factor = Decimal(1) + gross / Decimal(100)
    buy_factor = Decimal(1) + buy_cost_bps / _BPS_SCALE
    sell_factor = Decimal(1) - sell_cost_bps / _BPS_SCALE
    if gross_factor <= 0:
        raise ValueError("gross return factor must be positive")
    if buy_factor <= 0 or sell_factor <= 0:
        raise ValueError("round-trip execution cost factors must be positive")
    net_factor = (
        gross_factor
        * sell_factor
        / buy_factor
    )
    return float((net_factor - Decimal(1)) * Decimal(100))


def apply_round_trip_execution_costs(
    trades: pd.DataFrame,
    costs: ExecutionCostSpec,
) -> pd.DataFrame:
    """Apply buy/sell costs to gross ``ret_pct`` while retaining provenance."""
    _validate_positive_cost_factors(costs)
    validated = ExecutionCostSpec.model_validate(costs)
    if execution_costs_are_zero(validated) or trades.empty:
        return trades.copy()
    if "ret_pct" not in trades.columns:
        raise ValueError("nonzero execution costs require a ret_pct trade column")

    buy_cost_bps, sell_cost_bps = _cost_totals(validated)
    output = trades.copy()
    output["gross_ret_pct"] = output["ret_pct"]
    output["ret_pct"] = output["gross_ret_pct"].map(
        lambda value: _net_return_pct(
            value,
            buy_cost_bps=buy_cost_bps,
            sell_cost_bps=sell_cost_bps,
        )
    )
    output["execution_cost_model"] = COST_MODEL_VERSION
    output["commission_bps"] = float(validated.commission_bps)
    output["stamp_duty_bps"] = float(validated.stamp_duty_bps)
    output["transfer_fee_bps"] = float(validated.transfer_fee_bps)
    output["slippage_bps"] = float(validated.slippage_bps)
    output["buy_cost_bps"] = float(buy_cost_bps)
    output["sell_cost_bps"] = float(sell_cost_bps)
    return output
