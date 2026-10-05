"""Bounded portfolio materials and original paper-account identities."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.paper_broker import BrokerExecutionContext, PaperAccountAuthoritySnapshot
from rquant.portfolio.drawdown import DrawdownDecision, DrawdownRule
from rquant.portfolio.weights import PortfolioCandidate, PortfolioTarget, PortfolioWeightRule
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.research_run_spec import ExecutionCostCalculation, ExecutionCostSpec, _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.signal_contracts import SignalAction, SignalEnvelopeFamily

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
RoleId = Annotated[str, StringConstraints(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")]
MAX_PAPER_CODES = 500
MAX_PAPER_MATERIAL_BYTES = 5 * 1024 * 1024


class PaperPortfolioBinding(RuntimeContractModel):
    role_id: RoleId
    account_id: str = Field(min_length=1, max_length=128)
    owner_id: str = Field(min_length=1, max_length=128)
    strategy_id: str = Field(min_length=1, max_length=128)
    strategy_version: str = Field(min_length=1, max_length=128)
    parameter_fingerprint: Sha256
    cost_spec_id: Sha256
    ledger_id: str = Field(min_length=1, max_length=128)
    manifest_fingerprint: Sha256


class PaperPortfolioStateIdentity(RuntimeContractModel):
    path: str = Field(min_length=1, max_length=4096)
    instance_id: str
    st_dev: int = Field(strict=True, ge=0)
    st_ino: int = Field(strict=True, ge=0)


class PaperPortfolioRules(RuntimeContractModel):
    weight_rule: PortfolioWeightRule
    drawdown_rule: DrawdownRule | None = None

    @model_validator(mode="before")
    @classmethod
    def numeric_admission(cls, value: object) -> object:
        if isinstance(value, cls):
            value = value.model_dump(mode="python")
        if not isinstance(value, Mapping):
            return value
        PortfolioBacktestConfig.validate_numeric_admission(value)
        weight = value.get("weight_rule")
        if isinstance(weight, PortfolioWeightRule):
            weight = weight.model_dump(mode="python")
        if isinstance(weight, Mapping):
            positions = weight.get("max_positions")
            if type(positions) is not int or not 1 <= positions <= MAX_PAPER_CODES:
                raise ValueError("paper position count exceeds the original 500-code budget")
        drawdown = value.get("drawdown_rule")
        if isinstance(drawdown, DrawdownRule):
            drawdown = drawdown.model_dump(mode="python")
        if isinstance(drawdown, Mapping):
            for name in ("trigger_drawdown", "release_drawdown", "total_risk_weight_cap"):
                if name in drawdown and drawdown[name] is not None:
                    _parse_decimal(drawdown[name], field_name=name)
        return value


class PaperPortfolioConfiguration(PaperPortfolioRules):
    binding: PaperPortfolioBinding
    version: int = Field(strict=True, ge=1, le=4096)
    configured_at: AwareUtcDatetime
    execution_cost_spec: ExecutionCostSpec

    @model_validator(mode="after")
    def bound_cost(self) -> Self:
        if not self.execution_cost_spec.is_alignment_eligible or self.binding.cost_spec_id != self.execution_cost_spec.cost_spec_id:
            raise ValueError("paper configuration requires its exact original v3 cost spec")
        if len(self.model_dump_json().encode()) > 32768:
            raise ValueError("paper configuration exceeds the original command byte budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperRiskObservation(RuntimeContractModel):
    account_id: str
    configuration_fingerprint: Sha256
    observed_at: AwareUtcDatetime
    ledger_revision: int = Field(strict=True, ge=1)
    source_fingerprint: Sha256
    nav: Decimal = Field(gt=0, le=Decimal("1000000000000"), allow_inf_nan=False)
    decision: DrawdownDecision | None

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


def _admit_account_numbers(value: object) -> None:
    if isinstance(value, PaperAccountAuthoritySnapshot):
        value = value.model_dump(mode="python")
    if not isinstance(value, Mapping):
        return
    account = value.get("snapshot")
    if not isinstance(account, Mapping):
        return
    for name in ("cash", "available_cash", "frozen_cash", "realized_pnl", "unrealized_pnl", "nav"):
        if name in account and abs(_parse_decimal(account[name], field_name=name)) > Decimal("1000000000000"):
            raise ValueError("paper monetary input exceeds its admission budget")
    holdings = account.get("holdings", ())
    if not isinstance(holdings, (tuple, list)) or len(holdings) > MAX_PAPER_CODES:
        raise ValueError("paper holdings exceed the original 500-code budget")
    for holding in holdings:
        if isinstance(holding, Mapping):
            for name in ("average_cost", "market_price"):
                if name in holding:
                    _parse_decimal(holding[name], field_name=name)


class PaperTargetMaterials(RuntimeContractModel):
    configuration: PaperPortfolioConfiguration
    signal: SignalEnvelopeFamily
    account: PaperAccountAuthoritySnapshot
    decision_at: AwareUtcDatetime
    candidates: tuple[PortfolioCandidate, ...] = Field(min_length=1, max_length=MAX_PAPER_CODES)
    industry_by_code: dict[str, str]
    ranking_source_fingerprint: Sha256
    ranking_available_at: AwareUtcDatetime
    quote_snapshot_id: Sha256
    quote: BrokerExecutionContext
    quote_event_time: AwareUtcDatetime
    quote_available_at: AwareUtcDatetime
    risk_observation: PaperRiskObservation | None = None

    @model_validator(mode="before")
    @classmethod
    def numeric_admission(cls, value: object) -> object:
        if isinstance(value, cls):
            value = value.model_dump(mode="python")
        if not isinstance(value, Mapping):
            return value
        _admit_account_numbers(value.get("account"))
        for candidate in value.get("candidates", ()):
            if isinstance(candidate, PortfolioCandidate):
                candidate = candidate.model_dump(mode="python")
            if isinstance(candidate, Mapping) and "rank_score" in candidate:
                _parse_decimal(candidate["rank_score"], field_name="rank score")
        quote = value.get("quote")
        if isinstance(quote, BrokerExecutionContext):
            quote = quote.model_dump(mode="python")
        if isinstance(quote, Mapping) and "executable_price" in quote:
            price = _parse_decimal(quote["executable_price"], field_name="decision price")
            if not Decimal("0.0001") <= price <= Decimal("1000000000"):
                raise ValueError("paper decision price exceeds the original tick/amount budget")
        return value

    @model_validator(mode="after")
    def require_sources(self) -> Self:
        binding = self.configuration.binding
        if (self.signal.action is not SignalAction.B_INTENT
                or self.signal.strategy_id != binding.strategy_id
                or self.signal.strategy_version != binding.strategy_version
                or self.signal.parameter_fingerprint != binding.parameter_fingerprint
                or self.account.snapshot.account_id != binding.account_id):
            raise ValueError("paper target materials do not bind the exact account/strategy")
        if (self.configuration.configured_at > self.decision_at or self.ranking_available_at > self.decision_at
                or self.quote_available_at > self.decision_at or self.quote_event_time > self.quote_available_at
                or self.account.snapshot.as_of_time != self.decision_at
                or self.signal.available_at > self.decision_at):
            raise ValueError("paper target material is not available at the decision cutoff")
        codes = {item.ts_code for item in self.candidates}
        codes.update(item.code for item in self.account.snapshot.holdings)
        codes.update(self.industry_by_code)
        if len(codes) > MAX_PAPER_CODES or len({c.ts_code for c in self.candidates}) != len(self.candidates):
            raise ValueError("complete paper candidate/holding code union exceeds its budget")
        if self.quote.instrument_context.ts_code != self.signal.candidate_id:
            raise ValueError("paper quote belongs to a different candidate")
        if self.risk_observation is not None:
            risk = self.risk_observation
            if (risk.account_id != binding.account_id or risk.configuration_fingerprint != self.configuration.fingerprint
                    or risk.observed_at != self.decision_at or risk.nav != self.account.snapshot.nav
                    or risk.ledger_revision != self.account.revision or risk.source_fingerprint != self.account.state_fingerprint
                    or risk.decision is None
                    or risk.decision.state.rule != self.configuration.drawdown_rule):
                raise ValueError("paper drawdown observation does not bind this configuration and account")
        if self.configuration.drawdown_rule is not None and self.risk_observation is None:
            raise ValueError("paper target requires an available drawdown observation")
        if len(self.model_dump_json().encode()) > MAX_PAPER_MATERIAL_BYTES:
            raise ValueError("paper target materials exceed the research byte budget")
        return self


class PaperTargetQuantityAuthority(RuntimeContractModel):
    contract: Literal["paper-target-quantity/v1"] = "paper-target-quantity/v1"
    basis: PaperTargetMaterials
    target: PortfolioTarget
    quantity: int = Field(strict=True, ge=100, multiple_of=100)
    costs: ExecutionCostCalculation

    @model_validator(mode="after")
    def exact_quantity_cost(self) -> Self:
        if self.costs.order_input.quantity != self.quantity or self.costs.cost_spec_id != self.basis.configuration.binding.cost_spec_id:
            raise ValueError("paper target quantity and original cost receipt differ")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))
