"""Pure factor research calculations."""

from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.evaluate import (
    CorrelationResult,
    DailyFactorResult,
    FactorEvaluation,
    FactorEvaluationInput,
    FactorSample,
    GroupingResult,
    GroupReturn,
    evaluate_factor,
)
from rquant.factor.expression import (
    FactorExpressionError,
    FeatureCatalog,
    ParsedFactorExpression,
    parse_factor_expression,
)
from rquant.factor.portfolio import (
    FactorPortfolioDay,
    FactorPortfolioDiagnostics,
    PortfolioGroupingDay,
    PortfolioGroupPoint,
    evaluate_factor_portfolios,
)
from rquant.factor.summary import FactorICSummary, ICSeriesSummary, summarize_factor_ic
from rquant.factor.time_series import (
    DecisionTime,
    FactorTimeSeriesError,
    FactorTimeSeriesInput,
    FactorTimeSeriesResult,
    FactorTimeSeriesValue,
    FeatureObservation,
    IndustryObservation,
    MarketCapObservation,
    evaluate_factor_time_series,
)

__all__ = [
    "CorrelationResult",
    "DailyFactorResult",
    "DecisionTime",
    "FactorDefinition",
    "FactorEvaluation",
    "FactorEvaluationInput",
    "FactorSample",
    "FactorPortfolioDay",
    "FactorPortfolioDiagnostics",
    "FactorTimeSeriesError",
    "FactorTimeSeriesInput",
    "FactorTimeSeriesResult",
    "FactorTimeSeriesValue",
    "FactorExpressionError",
    "FeatureCatalog",
    "FeatureObservation",
    "IndustryObservation",
    "MarketCapObservation",
    "FactorICSummary",
    "GroupReturn",
    "GroupingResult",
    "ICSeriesSummary",
    "ParsedFactorExpression",
    "PortfolioGroupPoint",
    "PortfolioGroupingDay",
    "build_factor_definition",
    "evaluate_factor",
    "evaluate_factor_portfolios",
    "evaluate_factor_time_series",
    "parse_factor_expression",
    "summarize_factor_ic",
]
