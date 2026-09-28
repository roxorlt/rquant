"""Pure industry exposure and Brinson–Fachler attribution with cash separate."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from fractions import Fraction
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_ONE = Fraction(1)
_QUANTUM = Decimal("0.000000000000000001")
_RECONCILIATION_TOLERANCE = Decimal("0.000000000001")


class PortfolioAttributionError(ValueError):
    """The supplied industry coverage cannot support a faithful attribution."""


class IndustryWeight(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    industry_l1: str | None
    portfolio_weight: Decimal = Field(ge=0, le=1, allow_inf_nan=False)
    benchmark_weight: Decimal = Field(ge=0, le=1, allow_inf_nan=False)

    @field_validator("industry_l1")
    @classmethod
    def normalize_industry(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip() or None


class ExposureInput(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    industries: tuple[IndustryWeight, ...]
    portfolio_cash_weight: Decimal = Field(ge=0, le=1, allow_inf_nan=False)
    benchmark_cash_weight: Decimal = Field(ge=0, le=1, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_weights(self) -> Self:
        industries = [item.industry_l1 for item in self.industries]
        if len(industries) != len(set(industries)):
            raise ValueError("重复申万一级行业")
        if any(
            item.portfolio_weight == 0 and item.benchmark_weight == 0 for item in self.industries
        ):
            raise ValueError("零权重行业不应进入暴露输入")
        portfolio_total = Fraction(self.portfolio_cash_weight) + sum(
            (Fraction(item.portfolio_weight) for item in self.industries), Fraction(0)
        )
        benchmark_total = Fraction(self.benchmark_cash_weight) + sum(
            (Fraction(item.benchmark_weight) for item in self.industries), Fraction(0)
        )
        if portfolio_total != _ONE or benchmark_total != _ONE:
            raise ValueError("组合与基准的行业加现金权重须各自精确合计为 1")
        return self


class ExposureSlice(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["industry", "unknown", "cash"]
    industry_l1: str | None
    portfolio_weight: Decimal
    benchmark_weight: Decimal
    deviation: Decimal


class ExposureResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: tuple[ExposureSlice, ...]


class IndustryReturn(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    industry_l1: str
    portfolio_return: Decimal | None = Field(default=None, allow_inf_nan=False)
    benchmark_return: Decimal = Field(allow_inf_nan=False)

    @field_validator("industry_l1")
    @classmethod
    def require_industry(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("申万一级行业不能为空")
        return normalized


class AttributionSlice(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["industry", "cash"]
    industry_l1: str | None
    portfolio_weight: Decimal
    benchmark_weight: Decimal
    deviation: Decimal
    portfolio_return: Decimal | None
    benchmark_return: Decimal
    allocation: Decimal
    selection_and_interaction: Decimal
    total: Decimal


class AttributionResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    rows: tuple[AttributionSlice, ...]
    portfolio_return: Decimal
    benchmark_return: Decimal
    active_return: Decimal
    attributed_return: Decimal
    residual: Decimal


def _exact_decimal(value: Fraction) -> Decimal:
    if not value:
        return Decimal("0")
    with localcontext() as context:
        context.prec = len(str(abs(value.numerator))) + len(str(value.denominator)) + 10
        return Decimal(value.numerator) / Decimal(value.denominator)


def _rounded_decimal(value: Fraction) -> Decimal:
    if not value:
        return Decimal("0")
    with localcontext() as context:
        context.prec = len(str(abs(value.numerator))) + len(str(value.denominator)) + 30
        return (Decimal(value.numerator) / Decimal(value.denominator)).quantize(
            _QUANTUM, rounding=ROUND_HALF_EVEN
        )


def _ordered_industries(spec: ExposureInput) -> list[IndustryWeight]:
    return sorted(
        spec.industries, key=lambda item: (item.industry_l1 is None, item.industry_l1 or "")
    )


def calculate_industry_exposure(spec: ExposureInput) -> ExposureResult:
    """List industry weight deviations, then unknown exposure and cash as distinct rows."""
    if not isinstance(spec, ExposureInput):
        raise PortfolioAttributionError("行业权重必须使用已校验的输入模型")
    rows = [
        ExposureSlice(
            kind="unknown" if item.industry_l1 is None else "industry",
            industry_l1=item.industry_l1,
            portfolio_weight=item.portfolio_weight,
            benchmark_weight=item.benchmark_weight,
            deviation=_exact_decimal(
                Fraction(item.portfolio_weight) - Fraction(item.benchmark_weight)
            ),
        )
        for item in _ordered_industries(spec)
    ]
    rows.append(
        ExposureSlice(
            kind="cash",
            industry_l1=None,
            portfolio_weight=spec.portfolio_cash_weight,
            benchmark_weight=spec.benchmark_cash_weight,
            deviation=_exact_decimal(
                Fraction(spec.portfolio_cash_weight) - Fraction(spec.benchmark_cash_weight)
            ),
        )
    )
    return ExposureResult(rows=tuple(rows))


def attribute_brinson_fachler(
    spec: ExposureInput, returns: Sequence[IndustryReturn]
) -> AttributionResult:
    """Combine BF selection and interaction; reconcile cash and industries exactly."""
    exposure = calculate_industry_exposure(spec)
    if any(row.kind == "unknown" for row in exposure.rows):
        raise PortfolioAttributionError("未知行业无法做申万一级归因")
    if any(not isinstance(item, IndustryReturn) for item in returns):
        raise PortfolioAttributionError("行业收益必须使用已校验的输入模型")
    by_industry: dict[str, IndustryReturn] = {}
    for item in returns:
        if item.industry_l1 in by_industry:
            raise PortfolioAttributionError(f"重复行业收益：{item.industry_l1}")
        by_industry[item.industry_l1] = item
    expected = {row.industry_l1 for row in exposure.rows if row.kind == "industry"}
    for industry in sorted(expected):
        if industry not in by_industry:
            raise PortfolioAttributionError(f"缺少基准行业收益：{industry}")
    unexpected = set(by_industry) - expected
    if unexpected:
        raise PortfolioAttributionError(f"行业收益未匹配暴露：{min(unexpected)}")

    industry_rows = [row for row in exposure.rows if row.kind == "industry"]
    for row in industry_rows:
        record = by_industry[row.industry_l1 or ""]
        if row.portfolio_weight > 0 and record.portfolio_return is None:
            raise PortfolioAttributionError(f"缺少组合行业收益：{row.industry_l1}")

    benchmark_total = sum(
        (
            Fraction(row.benchmark_weight)
            * Fraction(by_industry[row.industry_l1 or ""].benchmark_return)
            for row in industry_rows
        ),
        Fraction(0),
    )
    portfolio_total = sum(
        (
            Fraction(row.portfolio_weight)
            * Fraction(by_industry[row.industry_l1 or ""].portfolio_return)
            for row in industry_rows
            if row.portfolio_weight > 0
        ),
        Fraction(0),
    )
    exact_contributions: list[Fraction] = []
    result_rows: list[AttributionSlice] = []
    for row in industry_rows:
        record = by_industry[row.industry_l1 or ""]
        wp, wb = Fraction(row.portfolio_weight), Fraction(row.benchmark_weight)
        rb = Fraction(record.benchmark_return)
        allocation = (wp - wb) * (rb - benchmark_total)
        selection = (
            wp * (Fraction(record.portfolio_return) - rb)
            if record.portfolio_return is not None
            else Fraction(0)
        )
        contribution = allocation + selection
        exact_contributions.append(contribution)
        result_rows.append(
            AttributionSlice(
                kind="industry",
                industry_l1=row.industry_l1,
                portfolio_weight=row.portfolio_weight,
                benchmark_weight=row.benchmark_weight,
                deviation=row.deviation,
                portfolio_return=record.portfolio_return,
                benchmark_return=record.benchmark_return,
                allocation=_rounded_decimal(allocation),
                selection_and_interaction=_rounded_decimal(selection),
                total=_rounded_decimal(contribution),
            )
        )
    cash = exposure.rows[-1]
    cash_allocation = (Fraction(cash.portfolio_weight) - Fraction(cash.benchmark_weight)) * (
        -benchmark_total
    )
    exact_contributions.append(cash_allocation)
    result_rows.append(
        AttributionSlice(
            kind="cash",
            industry_l1=None,
            portfolio_weight=cash.portfolio_weight,
            benchmark_weight=cash.benchmark_weight,
            deviation=cash.deviation,
            portfolio_return=Decimal("0"),
            benchmark_return=Decimal("0"),
            allocation=_rounded_decimal(cash_allocation),
            selection_and_interaction=Decimal("0"),
            total=_rounded_decimal(cash_allocation),
        )
    )
    exact_active = portfolio_total - benchmark_total
    if sum(exact_contributions, Fraction(0)) != exact_active:
        raise PortfolioAttributionError("行业与现金贡献未能对账")
    active = _rounded_decimal(exact_active)
    with localcontext() as context:
        context.prec = max(
            50, max(len(row.total.as_tuple().digits) for row in result_rows) + len(result_rows) + 20
        )
        attributed = sum((row.total for row in result_rows), Decimal("0"))
        residual = active - attributed
    if abs(residual) > _RECONCILIATION_TOLERANCE:
        raise PortfolioAttributionError("展示精度下归因残差超过容差")
    return AttributionResult(
        rows=tuple(result_rows),
        portfolio_return=_rounded_decimal(portfolio_total),
        benchmark_return=_rounded_decimal(benchmark_total),
        active_return=active,
        attributed_return=attributed,
        residual=residual,
    )
