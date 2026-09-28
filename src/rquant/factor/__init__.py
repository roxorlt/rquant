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
from rquant.factor.summary import FactorICSummary, ICSeriesSummary, summarize_factor_ic

__all__ = [
    "CorrelationResult",
    "DailyFactorResult",
    "FactorDefinition",
    "FactorEvaluation",
    "FactorEvaluationInput",
    "FactorSample",
    "FactorExpressionError",
    "FeatureCatalog",
    "FactorICSummary",
    "GroupReturn",
    "GroupingResult",
    "ICSeriesSummary",
    "ParsedFactorExpression",
    "build_factor_definition",
    "evaluate_factor",
    "parse_factor_expression",
    "summarize_factor_ic",
]
