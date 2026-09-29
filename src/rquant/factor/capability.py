"""Fixed implementation capabilities of the retrospective daily factor source."""

from __future__ import annotations

import ast
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.factor.definition import FactorDefinition
from rquant.factor.expression import FeatureCatalog

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True)


class DailyFactorField(BaseModel):
    model_config = _IMMUTABLE

    column: str = Field(min_length=1, max_length=64)
    name_zh: str = Field(min_length=1, max_length=32)
    description_zh: str = Field(min_length=1, max_length=120)


class UnavailableFactorOperator(BaseModel):
    model_config = _IMMUTABLE

    name: str = Field(min_length=1, max_length=64)
    reason_zh: str = Field(min_length=1, max_length=120)


class DailyFactorCapabilities(BaseModel):
    """Code support only; the snapshot decides dates, coverage, and availability."""

    model_config = _IMMUTABLE

    version: Literal["daily_v1"]
    source_mode: Literal["historical_retrospective"]
    fields: tuple[DailyFactorField, ...] = Field(min_length=1)
    runnable_operators: tuple[str, ...] = Field(min_length=1)
    unavailable_operators: tuple[UnavailableFactorOperator, ...]

    @model_validator(mode="after")
    def _unique_names(self) -> DailyFactorCapabilities:
        columns = tuple(field.column for field in self.fields)
        unavailable = tuple(operator.name for operator in self.unavailable_operators)
        if (
            len(columns) != len(set(columns))
            or len(self.runnable_operators) != len(set(self.runnable_operators))
            or len(unavailable) != len(set(unavailable))
            or set(self.runnable_operators) & set(unavailable)
        ):
            raise ValueError("daily factor capability names must be unique")
        return self

    def feature_catalog(self) -> FeatureCatalog:
        return FeatureCatalog(columns=tuple(field.column for field in self.fields))

    def require_runnable_definition(self, definition: FactorDefinition) -> None:
        checked = FactorDefinition.model_validate(definition)
        columns = set(self.feature_catalog().columns)
        if (
            not set(checked.feature_catalog.columns) <= columns
            or not set(checked.dependency_columns) <= columns
        ):
            raise ValueError(
                "factor definition requires a column without a historical daily contract"
            )
        tree = ast.parse(checked.expression, mode="eval")
        functions = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        unavailable = {operator.name for operator in self.unavailable_operators}
        if functions & unavailable:
            raise ValueError("factor definition requires unavailable industry or size context")
        if not functions <= set(self.runnable_operators):
            raise ValueError("factor definition requires an unsupported historical operator")


HISTORICAL_DAILY_V1 = DailyFactorCapabilities(
    version="daily_v1",
    source_mode="historical_retrospective",
    fields=(
        DailyFactorField(column="open", name_zh="开盘价", description_zh="日线记录的开盘价。"),
        DailyFactorField(column="high", name_zh="最高价", description_zh="日线记录的最高价。"),
        DailyFactorField(column="low", name_zh="最低价", description_zh="日线记录的最低价。"),
        DailyFactorField(column="close", name_zh="收盘价", description_zh="日线记录的收盘价。"),
        DailyFactorField(column="vol", name_zh="成交量", description_zh="日线记录的成交量。"),
        DailyFactorField(column="amount", name_zh="成交额", description_zh="日线记录的成交额。"),
    ),
    runnable_operators=(
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
    ),
    unavailable_operators=(
        UnavailableFactorOperator(
            name="industry_neutralize", reason_zh="历史来源缺少可核验的行业归属。"
        ),
        UnavailableFactorOperator(
            name="size_neutralize", reason_zh="历史来源缺少可核验的市值数据。"
        ),
    ),
)
