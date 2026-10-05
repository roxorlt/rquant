"""Use original exposure and BF calculations with bound industry provenance."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from typing import Literal, Self

from pydantic import Field, model_validator

from rquant.paper_portfolio_ledger import PaperPortfolioLedgerFrame
from rquant.paper_portfolio_models import Sha256
from rquant.paper_portfolio_source import PaperPortfolioRawFact
from rquant.portfolio.exposure import (AttributionResult, ExposureInput, ExposureResult, IndustryReturn, IndustryWeight,
                                      PortfolioAttributionError, attribute_brinson_fachler, calculate_industry_exposure)
from rquant.research_run_spec import _parse_decimal
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256, normalize_aware_utc


class PaperBenchmarkIndustryWeight(RuntimeContractModel):
    industry_l1: str = Field(min_length=1, max_length=80)
    weight: Decimal = Field(ge=0, le=1, allow_inf_nan=False)

    @model_validator(mode="before")
    @classmethod
    def bounded(cls, value: object) -> object:
        if isinstance(value, Mapping) and "weight" in value:
            _parse_decimal(value["weight"], field_name="benchmark industry weight")
        return value


class PaperIndustryMaterials(RuntimeContractModel):
    configuration_fingerprint: Sha256
    ledger_frame_fingerprint: Sha256
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    benchmark_source_identity: Sha256
    benchmark_weights: tuple[PaperBenchmarkIndustryWeight, ...] = Field(max_length=500)
    benchmark_cash_weight: Decimal = Field(ge=0, le=1, allow_inf_nan=False)
    facts: tuple[PaperPortfolioRawFact, ...] = Field(max_length=500)

    @model_validator(mode="before")
    @classmethod
    def bounded(cls, value: object) -> object:
        if isinstance(value, Mapping) and "benchmark_cash_weight" in value:
            _parse_decimal(value["benchmark_cash_weight"], field_name="benchmark cash weight")
        return value

    @model_validator(mode="after")
    def original_weights(self) -> Self:
        ExposureInput(industries=tuple(IndustryWeight(industry_l1=item.industry_l1, portfolio_weight=0, benchmark_weight=item.weight)
                                      for item in self.benchmark_weights if item.weight),
                      portfolio_cash_weight=1, benchmark_cash_weight=self.benchmark_cash_weight)
        if (len({item.ts_code for item in self.facts}) != len(self.facts) or self.observed_at > self.available_at
                or any(item.observed_at > self.observed_at or item.available_at > self.available_at for item in self.facts)):
            raise ValueError("paper industry facts have inconsistent original cutoffs")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperIndustryReturnFact(RuntimeContractModel):
    industry_l1: str = Field(min_length=1, max_length=80)
    portfolio_return: Decimal | None = Field(default=None, allow_inf_nan=False)
    benchmark_return: Decimal = Field(allow_inf_nan=False)
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    source_identity: Sha256

    @model_validator(mode="before")
    @classmethod
    def bounded(cls, value: object) -> object:
        if isinstance(value, Mapping):
            for key in ("portfolio_return", "benchmark_return"):
                if value.get(key) is not None and abs(_parse_decimal(value[key], field_name=key)) > Decimal("1000000000000"):
                    raise ValueError("paper industry return exceeds its representation budget")
        return value


class PaperAttributionMaterials(RuntimeContractModel):
    configuration_fingerprint: Sha256
    start_frame_fingerprint: Sha256
    start_at: AwareUtcDatetime
    end_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    source_identity: Sha256
    returns: tuple[PaperIndustryReturnFact, ...] = Field(max_length=500)

    @model_validator(mode="after")
    def period(self) -> Self:
        if (self.start_at >= self.end_at or self.end_at > self.available_at
                or len({item.industry_l1 for item in self.returns}) != len(self.returns)
                or any(item.observed_at != self.end_at or item.available_at > self.available_at for item in self.returns)):
            raise ValueError("paper BF period does not bind its source visibility")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperExposureView(RuntimeContractModel):
    configuration_fingerprint: Sha256
    ledger_frame_fingerprint: Sha256
    material_fingerprint: Sha256
    as_of: AwareUtcDatetime
    status: Literal["complete", "unavailable"]
    exposure: ExposureResult
    attribution: AttributionResult | None = None
    attribution_material_fingerprint: Sha256 | None = None
    reason: str | None = Field(default=None, max_length=512)


def calculate_paper_exposure(frame: PaperPortfolioLedgerFrame, material: PaperIndustryMaterials, *,
                             as_of: datetime, attribution: PaperAttributionMaterials | None = None) -> PaperExposureView:
    material = PaperIndustryMaterials.model_validate(material.model_dump(mode="python"))
    cutoff = normalize_aware_utc(as_of)
    if (frame.account is None or material.configuration_fingerprint != frame.configuration_fingerprint or material.ledger_frame_fingerprint != frame.fingerprint
            or material.available_at > frame.as_of or frame.as_of > cutoff or frame.account.nav <= 0):
        raise ValueError("paper exposure differs from its original ledger, configuration or cutoff")
    by_code = {item.ts_code: item for item in material.facts}
    amounts: dict[str | None, Decimal] = {}
    for holding in frame.account.holdings:
        fact = by_code.get(holding.code)
        if fact is not None and fact.valuation_price != holding.market_price:
            raise ValueError("paper industry material has different actual valuations")
        industry = fact.industry_l1 if fact else None
        amounts[industry] = amounts.get(industry, Decimal(0))+holding.market_price*holding.quantity
    with localcontext() as context:
        context.prec = 34
        weights = {industry: (amount/frame.account.nav).quantize(Decimal("1e-18"), rounding=ROUND_HALF_EVEN)
                   for industry, amount in amounts.items() if amount}
        cash = Decimal(1)-sum(weights.values(), Decimal(0))
    if cash < 0:
        raise ValueError("rounded industry weights exceed the original NAV")
    benchmark = {item.industry_l1: item.weight for item in material.benchmark_weights}
    industries = tuple(IndustryWeight(industry_l1=name, portfolio_weight=weights.get(name, Decimal(0)), benchmark_weight=benchmark.get(name, Decimal(0)))
                       for name in sorted(weights.keys() | benchmark.keys(), key=lambda key: (key is None, key or ""))
                       if weights.get(name, 0) or benchmark.get(name, 0))
    spec = ExposureInput(industries=industries, portfolio_cash_weight=cash, benchmark_cash_weight=material.benchmark_cash_weight)
    exposure = calculate_industry_exposure(spec)
    result = None
    reason = "缺少同期行业收益" if attribution is None else None
    if None in weights:
        reason = "持仓缺少申万一级行业"
    elif attribution is not None:
        attribution = PaperAttributionMaterials.model_validate(attribution.model_dump(mode="python"))
        if (attribution.configuration_fingerprint != frame.configuration_fingerprint or attribution.start_frame_fingerprint != frame.fingerprint
                or attribution.start_at != frame.as_of or attribution.available_at > cutoff):
            raise ValueError("paper attribution does not bind its actual period-start weights")
        try:
            result = attribute_brinson_fachler(spec, tuple(IndustryReturn(industry_l1=item.industry_l1, portfolio_return=item.portfolio_return,
                                                                        benchmark_return=item.benchmark_return) for item in attribution.returns))
        except PortfolioAttributionError as exc:
            reason = str(exc)
    return PaperExposureView(configuration_fingerprint=frame.configuration_fingerprint, ledger_frame_fingerprint=frame.fingerprint,
                             material_fingerprint=material.fingerprint, as_of=cutoff, status="complete" if result is not None else "unavailable",
                             exposure=exposure, attribution=result, attribution_material_fingerprint=attribution.fingerprint if attribution else None,
                             reason=reason)
