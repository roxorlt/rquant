"""Pure factor research calculations."""

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
from rquant.factor.summary import FactorICSummary, ICSeriesSummary, summarize_factor_ic

__all__ = [
    "CorrelationResult",
    "DailyFactorResult",
    "FactorEvaluation",
    "FactorEvaluationInput",
    "FactorSample",
    "FactorICSummary",
    "GroupReturn",
    "GroupingResult",
    "ICSeriesSummary",
    "evaluate_factor",
    "summarize_factor_ic",
]
