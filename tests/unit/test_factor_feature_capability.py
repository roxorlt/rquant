"""The daily factor editor catalog describes implementation support, not data coverage."""

from __future__ import annotations

from datetime import date

import pytest

import rquant.factor as factor
from rquant.factor.capability import HISTORICAL_DAILY_V1
from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.expression import FeatureCatalog, parse_factor_expression


def _definition(expression: str, *, catalog: FeatureCatalog | None = None) -> FactorDefinition:
    return build_factor_definition(
        factor_id="daily_capability",
        name_zh="日线能力因子",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=date(2026, 7, 13),
        expression=expression,
        feature_catalog=catalog or HISTORICAL_DAILY_V1.feature_catalog(),
    )


def test_daily_capability_has_one_typed_source_for_six_historical_fields() -> None:
    capability = HISTORICAL_DAILY_V1

    assert factor.HISTORICAL_DAILY_V1 is capability
    assert capability.version == "daily_v1"
    assert capability.source_mode == "historical_retrospective"
    assert tuple(field.column for field in capability.fields) == (
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
    )
    assert all(field.name_zh and field.description_zh for field in capability.fields)
    assert capability.feature_catalog() == FeatureCatalog(
        columns=("open", "high", "low", "close", "vol", "amount")
    )
    assert set(capability.runnable_operators) == {
        "+",
        "-",
        "*",
        "/",
        "<",
        "<=",
        ">",
        ">=",
        "ts_mean",
        "ts_std",
        "ts_delta",
        "ts_rank",
        "ref",
        "ts_corr",
        "cs_rank",
        "cs_zscore",
        "cs_winsorize",
    }
    assert not any(
        key in capability.model_dump(mode="json")
        for key in ("earliest_available_date", "coverage", "market_ready", "available_today")
    )


@pytest.mark.parametrize(
    "expression",
    (
        "+close - -open * high / low",
        "close < open",
        "close <= open",
        "close > open",
        "close >= open",
        "ts_mean(close, 2)",
        "ts_std(close, 2)",
        "ts_delta(close, 2)",
        "ts_rank(close, 2)",
        "ref(close, 1)",
        "ts_corr(close, open, 2)",
        "cs_rank(close)",
        "cs_zscore(close)",
        "cs_winsorize(close, 3)",
    ),
)
def test_declared_runnable_operators_are_accepted_by_parser(expression: str) -> None:
    capability = HISTORICAL_DAILY_V1
    assert parse_factor_expression(expression, capability.feature_catalog()).expression


def test_context_functions_can_parse_but_are_not_historical_capabilities() -> None:
    capability = HISTORICAL_DAILY_V1

    assert {operator.name for operator in capability.unavailable_operators} == {
        "industry_neutralize",
        "size_neutralize",
    }
    assert all(operator.reason_zh for operator in capability.unavailable_operators)
    for operator in capability.unavailable_operators:
        assert operator.name not in capability.runnable_operators
        assert parse_factor_expression(
            f"{operator.name}(close)", capability.feature_catalog()
        ).expression


def test_daily_capability_admits_each_runnable_function_and_rejects_missing_source() -> None:
    capability = HISTORICAL_DAILY_V1
    for expression in (
        "ts_mean(close, 2)",
        "ts_std(close, 2)",
        "ts_delta(close, 2)",
        "ts_rank(close, 2)",
        "ref(close, 1)",
        "ts_corr(close, open, 2)",
        "cs_rank(close)",
        "cs_zscore(close)",
        "cs_winsorize(close, 3)",
    ):
        capability.require_runnable_definition(_definition(expression))

    with pytest.raises(ValueError, match="column"):
        capability.require_runnable_definition(
            _definition("pe", catalog=FeatureCatalog(columns=("pe",)))
        )
    for operator in capability.unavailable_operators:
        with pytest.raises(ValueError, match="context"):
            capability.require_runnable_definition(_definition(f"{operator.name}(close)"))
