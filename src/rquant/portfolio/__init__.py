"""Pure portfolio weights, drawdown decisions, and industry attribution."""

from rquant.portfolio.drawdown import (
    DrawdownDecision,
    DrawdownInputError,
    DrawdownRule,
    DrawdownState,
    evaluate_drawdown,
)
from rquant.portfolio.exposure import (
    AttributionResult,
    ExposureInput,
    ExposureResult,
    IndustryReturn,
    IndustryWeight,
    PortfolioAttributionError,
    attribute_brinson_fachler,
    calculate_industry_exposure,
)
from rquant.portfolio.weights import (
    PortfolioAllocationError,
    PortfolioCandidate,
    PortfolioTarget,
    PortfolioWeightRule,
    TargetPosition,
    allocate_target_weights,
)

__all__ = [
    "AttributionResult",
    "DrawdownDecision",
    "DrawdownInputError",
    "DrawdownRule",
    "DrawdownState",
    "ExposureInput",
    "ExposureResult",
    "IndustryReturn",
    "IndustryWeight",
    "PortfolioAllocationError",
    "PortfolioAttributionError",
    "PortfolioCandidate",
    "PortfolioTarget",
    "PortfolioWeightRule",
    "TargetPosition",
    "allocate_target_weights",
    "attribute_brinson_fachler",
    "calculate_industry_exposure",
    "evaluate_drawdown",
]
