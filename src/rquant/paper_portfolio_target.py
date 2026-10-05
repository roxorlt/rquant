"""Prepare bounded board-lot targets using the original allocation and cost engines."""

from decimal import Decimal
from datetime import datetime
from fractions import Fraction

from rquant.order_execution_costs import calculate_execution_costs
from rquant.paper_portfolio_models import PaperTargetMaterials, PaperTargetQuantityAuthority
from rquant.portfolio.weights import allocate_target_weights
from rquant.research_run_spec import ExecutionCostOrderInput
from rquant.paper_broker import BrokerExecutionContext
from rquant.signal_contracts import SignalEnvelopeFamily


class PaperPortfolioAdmissionError(ValueError):
    """A new entry cannot meet its sourced portfolio constraints."""


def validate_paper_target_quantity(authority: PaperTargetQuantityAuthority, *, signal: SignalEnvelopeFamily,
                                   account_id: str, decision_at: datetime, quote_snapshot_id: str,
                                   quote: BrokerExecutionContext, quote_event_time: datetime,
                                   quote_available_at: datetime) -> PaperTargetQuantityAuthority:
    value = PaperTargetQuantityAuthority.model_validate(authority.model_dump(mode="python"))
    basis = value.basis
    if (basis.signal != signal or basis.configuration.binding.account_id != account_id
            or basis.decision_at != decision_at or basis.quote_snapshot_id != quote_snapshot_id
            or basis.quote != quote or basis.quote_event_time != quote_event_time
            or basis.quote_available_at != quote_available_at):
        raise ValueError("target quantity source differs from the original queued signal/quote")
    if prepare_paper_target_quantity(basis) != value:
        raise ValueError("target quantity differs from the original allocation and cost calculation")
    return value


def prepare_paper_target_quantity(materials: PaperTargetMaterials) -> PaperTargetQuantityAuthority:
    basis = PaperTargetMaterials.model_validate(materials.model_dump(mode="python"))
    configuration = basis.configuration
    rule = configuration.weight_rule
    account = basis.account.snapshot
    code = basis.signal.candidate_id
    held = {item.code: item for item in account.holdings}
    if code not in held and len(held) >= rule.max_positions:
        raise PaperPortfolioAdmissionError("持股数已达上限")
    risk = basis.risk_observation.decision if basis.risk_observation is not None else None
    if risk is not None and not risk.allow_new_positions and code not in held:
        raise PaperPortfolioAdmissionError("回撤限制，暂停开新仓")
    if risk is not None and risk.max_total_risk_weight is not None:
        rule = rule.model_copy(update={"cash_reserve": max(rule.cash_reserve, Decimal(1) - risk.max_total_risk_weight)})
    target = allocate_target_weights(basis.candidates, rule, capital=account.nav)
    selected = next((item for item in target.positions if item.ts_code == code and item.status == "selected"), None)
    if selected is None:
        raise PaperPortfolioAdmissionError("股票未入选本次目标")
    held_values = {key: Fraction(item.quantity) * Fraction(item.market_price) for key, item in held.items()}
    held_amount = held_values.get(code, Fraction(0))
    remaining = Fraction(selected.target_amount) - held_amount
    price = basis.quote.executable_price
    lots = int(remaining / Fraction(price)) // 100
    if lots <= 0:
        raise PaperPortfolioAdmissionError("目标已满足或不足一手")
    if rule.max_industry_weight is not None:
        if any(not basis.industry_by_code.get(key) for key in held):
            raise PaperPortfolioAdmissionError("持仓缺少行业，无法核对上限")
        if any(basis.industry_by_code.get(item.ts_code) != item.industry_l1 for item in basis.candidates):
            raise PaperPortfolioAdmissionError("候选行业与原来源不一致")

    def feasible(quantity: int):
        costs = calculate_execution_costs(configuration.execution_cost_spec,
                                          ExecutionCostOrderInput(side="BUY", reference_price=price, quantity=quantity),
                                          basis.quote.instrument_context)
        debit = Fraction(costs.executed_notional) + Fraction(costs.total_fees)
        acquired = Fraction(quantity) * Fraction(price)
        nav_after = Fraction(account.nav) + acquired - debit
        cash_after = Fraction(account.available_cash) - debit
        if nav_after <= 0 or cash_after < Fraction(rule.cash_reserve) * nav_after:
            return None
        positions = {**held_values, code: held_amount + acquired}
        if any(amount > Fraction(rule.max_stock_weight) * nav_after for amount in positions.values()):
            return None
        if sum(positions.values()) > (Fraction(1) - Fraction(rule.cash_reserve)) * nav_after:
            return None
        if rule.max_industry_weight is not None:
            industries = {}
            for key, amount in positions.items():
                group = basis.industry_by_code.get(key)
                if not group:
                    return None
                industries[group] = industries.get(group, Fraction(0)) + amount
            if any(amount > Fraction(rule.max_industry_weight) * nav_after for amount in industries.values()):
                return None
        return costs

    low, high = 0, lots
    while low < high:
        middle = (low + high + 1) // 2
        if feasible(middle * 100) is None:
            high = middle - 1
        else:
            low = middle
    if low == 0:
        raise PaperPortfolioAdmissionError("资金、费用或持仓上限不足一手")
    costs = feasible(low * 100)
    assert costs is not None
    return PaperTargetQuantityAuthority(basis=basis, target=target, quantity=low * 100, costs=costs)
