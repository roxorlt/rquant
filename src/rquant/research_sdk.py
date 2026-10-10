"""Shared local research entry points for notebooks and production.

Readers require explicit sealed sources and private absolute roots. Feature
queries keep the existing one-day, 500-code, 50-field contract. These are the
original library callables and Pydantic models; no calculation is duplicated.
"""

from rquant.factor import (
    DecisionTime,
    FactorDefinition,
    FactorEvaluation,
    FactorEvaluationInput,
    FactorExpressionError,
    FactorForwardReturn,
    FactorPortfolioDiagnostics,
    FactorResearchRequest,
    FactorResearchResult,
    FactorSample,
    FactorTimeSeriesInput,
    FactorTimeSeriesResult,
    FeatureCatalog,
    FeatureObservation,
    assemble_factor_research_result,
    build_factor_definition,
    evaluate_factor,
    evaluate_factor_portfolios,
    evaluate_factor_time_series,
    parse_factor_expression,
    summarize_factor_ic,
)
from rquant.factor.daily_feature_source import (
    FactorDailyFeatureCounts,
    FactorDailyFeatureDayBatch,
    FactorDailyFeatureFact,
    FactorDailyFeatureQuery,
    FactorDailyFeatureReadLease,
    FactorDailyFeatureSource,
    FactorDailyStoredField,
    open_factor_daily_feature_source,
)
from rquant.factor.display_artifact import (
    FactorDisplayArtifactV1,
    load_factor_display_artifact,
)
from rquant.factor.result_artifact import (
    FactorResearchArtifactV1,
    load_factor_research_artifact,
)

__all__ = [
    "DecisionTime",
    "FactorDailyFeatureCounts",
    "FactorDailyFeatureDayBatch",
    "FactorDailyFeatureFact",
    "FactorDailyFeatureQuery",
    "FactorDailyFeatureReadLease",
    "FactorDailyFeatureSource",
    "FactorDailyStoredField",
    "FactorDefinition",
    "FactorDisplayArtifactV1",
    "FactorEvaluation",
    "FactorEvaluationInput",
    "FactorExpressionError",
    "FactorForwardReturn",
    "FactorPortfolioDiagnostics",
    "FactorResearchArtifactV1",
    "FactorResearchRequest",
    "FactorResearchResult",
    "FactorSample",
    "FactorTimeSeriesInput",
    "FactorTimeSeriesResult",
    "FeatureCatalog",
    "FeatureObservation",
    "assemble_factor_research_result",
    "build_factor_definition",
    "evaluate_factor",
    "evaluate_factor_portfolios",
    "evaluate_factor_time_series",
    "load_factor_display_artifact",
    "load_factor_research_artifact",
    "open_factor_daily_feature_source",
    "parse_factor_expression",
    "summarize_factor_ic",
]
