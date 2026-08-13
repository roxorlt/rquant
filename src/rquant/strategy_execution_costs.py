"""Versioned A-share round-trip execution cost model for research replays."""

from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from typing import Literal
from weakref import WeakKeyDictionary

import pandas as pd
from pydantic import Field, model_validator

from rquant.order_execution_costs import (
    BPS_SCALE,
    ExecutionCostCalculation,
    ExecutionCostOrderInput,
    OrderExecutionCosts,
    OrderSide,
    calculate_execution_costs,
    calculate_order_execution_costs,
)
from rquant.research_run_spec import ExecutionCostSpec, InstrumentContext
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

COST_MODEL_VERSION = "a-share-round-trip-v1"
RATE_ONLY_COST_MODEL_VERSION = "a-share-round-trip-rate-only-v2"
NOTIONAL_COST_MODEL_VERSION = "a-share-round-trip-notional-v2"
V3_NOTIONAL_COST_MODEL_VERSION = "a-share-shared-fill-v3"
_LOT_SIZE = Decimal("100")


class ExecutionCostBindingEvidence(RuntimeContractModel):
    """Exact fill-level evidence required before research can claim paper parity."""

    provenance_state: Literal["KNOWN_V3", "LEGACY_UNKNOWN", "V3_UNBOUND"]
    execution_cost_spec: ExecutionCostSpec | None = None
    calculations: tuple[ExecutionCostCalculation, ...] = ()
    rate_only: bool = False

    @model_validator(mode="after")
    def validate_known_v3_evidence(self) -> ExecutionCostBindingEvidence:
        if self.provenance_state != "KNOWN_V3":
            return self
        if (
            self.execution_cost_spec is None
            or not self.execution_cost_spec.is_alignment_eligible
            or not self.calculations
            or self.rate_only
        ):
            raise ValueError("KNOWN_V3 evidence requires a v3 spec and resolved fill calculations")
        assert self.execution_cost_spec.cost_spec_id is not None
        for calculation in self.calculations:
            if (
                calculation.cost_spec_id != self.execution_cost_spec.cost_spec_id
                or calculation.cost_engine_version != self.execution_cost_spec.cost_engine_version
            ):
                raise ValueError("KNOWN_V3 calculation does not bind its execution cost spec")
        return self


class PaperExecutionCostBindingExport:
    """Opaque capability issued only from a reconciled persisted paper ledger."""

    __slots__ = ("__weakref__",)

    def __new__(cls) -> PaperExecutionCostBindingExport:
        raise TypeError("paper execution cost binding exports are ledger-issued only")


class _VerifiedPaperExecutionCostBinding(RuntimeContractModel):
    """Private immutable facts retained behind a ledger-issued export capability."""

    account_id: str = Field(min_length=1)
    execution_ids: tuple[str, ...] = Field(min_length=1)
    receipt_execution_ids: tuple[str, ...] = Field(min_length=1)
    fill_ids: tuple[str, ...] = Field(min_length=1)
    execution_cost_spec: ExecutionCostSpec
    calculations: tuple[ExecutionCostCalculation, ...] = Field(min_length=1)
    runtime_generation: str = Field(pattern=r"^[0-9a-f]{64}$")
    attestation_head_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    attestation_revision: int = Field(ge=1)
    account_evidence_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    binding_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_binding(self) -> _VerifiedPaperExecutionCostBinding:
        if (
            len(self.execution_ids) != len(set(self.execution_ids))
            or self.receipt_execution_ids != self.execution_ids
            or len(self.fill_ids) != len(self.execution_ids)
            or len(self.calculations) != len(self.execution_ids)
        ):
            raise ValueError(
                "paper binding execution, receipt, fill, and calculation topology differs"
            )
        if not self.execution_cost_spec.is_alignment_eligible:
            raise ValueError("paper binding requires an alignment-eligible v3 cost spec")
        assert self.execution_cost_spec.cost_spec_id is not None
        for calculation in self.calculations:
            if (
                calculation.cost_spec_id != self.execution_cost_spec.cost_spec_id
                or calculation.cost_engine_version != self.execution_cost_spec.cost_engine_version
            ):
                raise ValueError("paper binding calculation does not match its v3 cost spec")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"binding_fingerprint"}))
        if self.binding_fingerprint is None:
            object.__setattr__(self, "binding_fingerprint", expected)
        elif self.binding_fingerprint != expected:
            raise ValueError("paper binding fingerprint does not match immutable evidence")
        return self


_TRUSTED_PAPER_EXECUTION_BINDINGS: WeakKeyDictionary[
    PaperExecutionCostBindingExport,
    _VerifiedPaperExecutionCostBinding,
] = WeakKeyDictionary()


def _issue_reconciled_paper_execution_cost_binding(
    *,
    account_id: str,
    execution_ids: tuple[str, ...],
    receipt_execution_ids: tuple[str, ...],
    fill_ids: tuple[str, ...],
    execution_cost_spec: ExecutionCostSpec,
    calculations: tuple[ExecutionCostCalculation, ...],
    runtime_generation: str,
    attestation_head_fingerprint: str,
    attestation_revision: int,
    account_evidence_fingerprint: str,
) -> PaperExecutionCostBindingExport:
    """Register an opaque export after the broker has independently verified it."""

    evidence = _VerifiedPaperExecutionCostBinding(
        account_id=account_id,
        execution_ids=execution_ids,
        receipt_execution_ids=receipt_execution_ids,
        fill_ids=fill_ids,
        execution_cost_spec=execution_cost_spec,
        calculations=calculations,
        runtime_generation=runtime_generation,
        attestation_head_fingerprint=attestation_head_fingerprint,
        attestation_revision=attestation_revision,
        account_evidence_fingerprint=account_evidence_fingerprint,
    )
    export = object.__new__(PaperExecutionCostBindingExport)
    _TRUSTED_PAPER_EXECUTION_BINDINGS[export] = evidence
    return export


def _trusted_paper_execution_binding(
    value: object,
) -> _VerifiedPaperExecutionCostBinding | None:
    if not isinstance(value, PaperExecutionCostBindingExport):
        return None
    return _TRUSTED_PAPER_EXECUTION_BINDINGS.get(value)


class ExecutionCostComparability(RuntimeContractModel):
    is_comparable: bool
    reason: str = Field(min_length=1)


def _not_comparable(reason: str) -> ExecutionCostComparability:
    return ExecutionCostComparability(is_comparable=False, reason=reason)


def compare_execution_cost_bindings(
    research: ExecutionCostBindingEvidence,
    paper: object,
) -> ExecutionCostComparability:
    """Fail closed unless both sources bind the identical v3 fill calculation evidence."""

    if research.rate_only:
        return _not_comparable("RATE_ONLY_RESEARCH_COST")
    if research.provenance_state == "V3_UNBOUND":
        return _not_comparable("UNBOUND_RESEARCH_COST")
    if research.provenance_state == "LEGACY_UNKNOWN":
        return _not_comparable("LEGACY_UNKNOWN_COST_PROVENANCE")
    verified_paper = _trusted_paper_execution_binding(paper)
    if verified_paper is None:
        return _not_comparable("UNTRUSTED_PAPER_EVIDENCE")
    if research.execution_cost_spec is None:
        return _not_comparable("MISSING_COST_SPEC")
    research_spec = research.execution_cost_spec
    paper_spec = verified_paper.execution_cost_spec
    if not research_spec.is_alignment_eligible or not paper_spec.is_alignment_eligible:
        return _not_comparable("LEGACY_COST_SCHEMA")
    if research_spec.cost_spec_id != paper_spec.cost_spec_id:
        return _not_comparable("COST_SPEC_ID_MISMATCH")
    if research_spec.cost_engine_version != paper_spec.cost_engine_version:
        return _not_comparable("COST_ENGINE_VERSION_MISMATCH")
    assert research_spec.slippage is not None and paper_spec.slippage is not None
    if research_spec.slippage.owner != paper_spec.slippage.owner:
        return _not_comparable("SLIPPAGE_OWNER_MISMATCH")
    if (
        research_spec.slippage.buy_bps != paper_spec.slippage.buy_bps
        or research_spec.slippage.sell_bps != paper_spec.slippage.sell_bps
        or research_spec.slippage.price_tick != paper_spec.slippage.price_tick
        or research_spec.slippage.price_rounding != paper_spec.slippage.price_rounding
    ):
        return _not_comparable("SLIPPAGE_OR_PRICE_ROUNDING_MISMATCH")
    assert research_spec.money is not None and paper_spec.money is not None
    if (
        research_spec.money.quantum != paper_spec.money.quantum
        or research_spec.money.rounding != paper_spec.money.rounding
        or research_spec.fee_notional_basis != paper_spec.fee_notional_basis
        or research_spec.assessment_unit != paper_spec.assessment_unit
    ):
        return _not_comparable("FEE_BASIS_OR_MONEY_ROUNDING_MISMATCH")
    if len(research.calculations) != len(verified_paper.calculations):
        return _not_comparable("FILL_TOPOLOGY_MISMATCH")
    for research_calculation, paper_calculation in zip(
        research.calculations,
        verified_paper.calculations,
        strict=True,
    ):
        if research_calculation.order_input != paper_calculation.order_input:
            return _not_comparable("NOTIONAL_OR_FILL_TOPOLOGY_MISMATCH")
        if research_calculation.instrument_context != paper_calculation.instrument_context:
            return _not_comparable("INSTRUMENT_CONTEXT_MISMATCH")
        if research_calculation.selected_rule_ids != paper_calculation.selected_rule_ids:
            return _not_comparable("SELECTED_RULE_MISMATCH")
        if research_calculation != paper_calculation:
            return _not_comparable("RESOLVED_CALCULATION_MISMATCH")
    return ExecutionCostComparability(is_comparable=True, reason="EXACT_V3_BOUND")


def _cost_totals(costs: ExecutionCostSpec) -> tuple[Decimal, Decimal]:
    if costs.schema_version == 3:
        raise ValueError("v3 execution costs do not have legacy scalar totals")
    assert costs.commission_bps is not None
    assert costs.transfer_fee_bps is not None
    assert costs.slippage_bps is not None
    assert costs.stamp_duty_bps is not None
    buy_cost_bps = costs.commission_bps + costs.transfer_fee_bps + costs.slippage_bps
    sell_cost_bps = buy_cost_bps + costs.stamp_duty_bps
    return buy_cost_bps, sell_cost_bps


def _validate_positive_cost_factors(costs: ExecutionCostSpec) -> None:
    if costs.schema_version == 3:
        return
    buy_cost_bps, sell_cost_bps = _cost_totals(costs)
    if Decimal(1) + buy_cost_bps / BPS_SCALE <= 0:
        raise ValueError("buy-side execution cost factor must be positive")
    if Decimal(1) - sell_cost_bps / BPS_SCALE <= 0:
        raise ValueError("sell-side execution cost factor must be positive")


def execution_costs_are_zero(costs: ExecutionCostSpec) -> bool:
    validated = ExecutionCostSpec.model_validate(costs)
    if validated.schema_version == 3:
        return False
    assert validated.commission_bps is not None
    assert validated.stamp_duty_bps is not None
    assert validated.transfer_fee_bps is not None
    assert validated.slippage_bps is not None
    assert validated.minimum_commission is not None
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
    if costs.schema_version == 3:
        raise ValueError("v3 execution costs must use calculate_execution_costs")
    assert costs.commission_bps is not None
    assert costs.minimum_commission is not None
    assert costs.transfer_fee_bps is not None
    assert costs.stamp_duty_bps is not None
    assert costs.slippage_bps is not None
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


def _total_fees(costs: OrderExecutionCosts | ExecutionCostCalculation) -> Decimal:
    return costs.fee_amount if isinstance(costs, OrderExecutionCosts) else costs.total_fees


def _net_cash_return_pct(
    buy: OrderExecutionCosts | ExecutionCostCalculation,
    sell: OrderExecutionCosts | ExecutionCostCalculation,
) -> Decimal:
    buy_cash = buy.executed_notional + _total_fees(buy)
    sell_cash = sell.executed_notional - _total_fees(sell)
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


def _v3_notional_row(
    row: pd.Series,
    costs: ExecutionCostSpec,
) -> dict[str, object]:
    assert costs.schema_version == 3
    assert costs.research_notional_per_trade is not None
    assert costs.cost_spec_id is not None
    gross = _finite_decimal(row["ret_pct"], field_name="ret_pct")
    if Decimal("1") + gross / Decimal("100") <= 0:
        raise ValueError("gross return factor must be positive")
    entry_price = _finite_decimal(row["entry_price"], field_name="entry_price")
    exit_price = _finite_decimal(row["exit_price"], field_name="exit_price")
    if entry_price <= 0 or exit_price <= 0:
        raise ValueError("entry_price and exit_price must be positive")
    if "instrument_context" not in row or row["instrument_context"] is None:
        raise ValueError("v3 notional execution costs require instrument_context")
    context = InstrumentContext.model_validate(row["instrument_context"])
    quantity = _research_quantity(entry_price, costs.research_notional_per_trade)
    buy = calculate_execution_costs(
        costs,
        ExecutionCostOrderInput(
            side="BUY",
            reference_price=entry_price,
            quantity=quantity,
        ),
        context,
    )
    sell = calculate_execution_costs(
        costs,
        ExecutionCostOrderInput(
            side="SELL",
            reference_price=exit_price,
            quantity=quantity,
        ),
        context,
    )
    execution_cost_amount = (
        buy.total_fees + buy.slippage_amount + sell.total_fees + sell.slippage_amount
    )
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
        "buy_total_fees": float(buy.total_fees),
        "sell_total_fees": float(sell.total_fees),
        "buy_cost_context_fingerprint": buy.cost_context_fingerprint,
        "sell_cost_context_fingerprint": sell.cost_context_fingerprint,
        "execution_cost_amount": float(execution_cost_amount),
        "buy_cost_bps": float(
            (buy.total_fees + buy.slippage_amount) / buy.reference_notional * BPS_SCALE
        ),
        "sell_cost_bps": float(
            (sell.total_fees + sell.slippage_amount) / sell.reference_notional * BPS_SCALE
        ),
        "effective_execution_cost_bps": float(
            execution_cost_amount / buy.reference_notional * BPS_SCALE
        ),
        "execution_cost_spec_id": costs.cost_spec_id,
        "cost_engine_version": costs.cost_engine_version,
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
    if validated.schema_version == 3 and not notional_mode:
        raise ValueError("v3 execution costs require research_notional_per_trade for fill replay")
    if notional_mode:
        required_columns = {"entry_price", "exit_price"}
        if validated.schema_version == 3:
            required_columns.add("instrument_context")
        missing = required_columns.difference(trades.columns)
        if missing:
            raise ValueError(f"notional execution costs require trade columns: {sorted(missing)}")
        calculated = trades.apply(
            lambda row: (
                _v3_notional_row(row, validated)
                if validated.schema_version == 3
                else _notional_row(row, validated)
            ),
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
        V3_NOTIONAL_COST_MODEL_VERSION
        if validated.schema_version == 3
        else NOTIONAL_COST_MODEL_VERSION
        if notional_mode
        else COST_MODEL_VERSION
        if validated.schema_version == 1
        else RATE_ONLY_COST_MODEL_VERSION
    )
    output["execution_cost_mode"] = "notional" if notional_mode else "rate_only"
    output["paper_execution_comparable"] = False
    output["paper_execution_comparability_reason"] = "UNBOUND_RESEARCH_COST"
    output["execution_cost_schema_version"] = validated.schema_version
    if validated.schema_version != 3:
        assert validated.commission_bps is not None
        assert validated.stamp_duty_bps is not None
        assert validated.transfer_fee_bps is not None
        assert validated.slippage_bps is not None
        assert validated.minimum_commission is not None
        output["commission_bps"] = float(validated.commission_bps)
        output["stamp_duty_bps"] = float(validated.stamp_duty_bps)
        output["transfer_fee_bps"] = float(validated.transfer_fee_bps)
        output["slippage_bps"] = float(validated.slippage_bps)
        output["minimum_commission"] = float(validated.minimum_commission)
    else:
        assert validated.cost_spec_id is not None
        output["execution_cost_spec_id"] = validated.cost_spec_id
        output["cost_engine_version"] = validated.cost_engine_version
    output["research_notional_per_trade"] = (
        float(validated.research_notional_per_trade)
        if validated.research_notional_per_trade is not None
        else None
    )
    return output
