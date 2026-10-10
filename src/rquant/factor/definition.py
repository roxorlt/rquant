"""Immutable research factor definition backed by an explicit feature catalog."""

from __future__ import annotations

import re
from datetime import date

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from rquant.factor.evaluate import FactorDirection
from rquant.factor.expression import (
    FactorExpressionError,
    FeatureCatalog,
    parse_factor_expression,
)

_FACTOR_ID_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_HAN_PATTERN = re.compile(r"[\u3400-\u9fff]")


class FactorDefinition(BaseModel):
    """A catalog-bound factor specification without values or serving state."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    factor_id: str
    name_zh: str
    category: str
    direction: FactorDirection
    version: int = Field(ge=1, strict=True)
    earliest_available_date: date | None
    expression: str
    dependency_columns: tuple[str, ...]
    max_history_window: int = Field(ge=1, strict=True)
    feature_catalog: FeatureCatalog

    @field_validator("factor_id")
    @classmethod
    def _validate_factor_id(cls, value: str) -> str:
        if _FACTOR_ID_PATTERN.fullmatch(value) is None:
            raise PydanticCustomError("factor_invalid_id", "factor ID is invalid")
        return value

    @field_validator("name_zh")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        if (
            not value
            or len(value) > 80
            or value.strip() != value
            or _HAN_PATTERN.search(value) is None
            or not value.isprintable()
        ):
            raise PydanticCustomError("factor_invalid_name", "Chinese factor name is invalid")
        return value

    @field_validator("category")
    @classmethod
    def _validate_category(cls, value: str) -> str:
        if not value or len(value) > 64 or value.strip() != value or not value.isprintable():
            raise PydanticCustomError("factor_invalid_category", "factor category is invalid")
        return value

    @model_validator(mode="after")
    def _validate_expression(self) -> FactorDefinition:
        try:
            parsed = parse_factor_expression(self.expression, self.feature_catalog)
        except FactorExpressionError as error:
            raise PydanticCustomError(
                f"factor_{error.reason}", "factor expression is invalid"
            ) from error
        if self.expression != parsed.expression:
            raise PydanticCustomError(
                "factor_expression_not_normalized", "factor expression is not normalized"
            )
        if self.dependency_columns != parsed.dependency_columns:
            raise PydanticCustomError("factor_dependency_mismatch", "factor dependencies differ")
        if self.max_history_window != parsed.max_history_window:
            raise PydanticCustomError("factor_history_mismatch", "factor history differs")
        return self


def build_factor_definition(
    *,
    factor_id: str,
    name_zh: str,
    category: str,
    direction: FactorDirection,
    version: int,
    earliest_available_date: date | None,
    expression: str,
    feature_catalog: FeatureCatalog,
    dependency_columns: tuple[str, ...] | None = None,
) -> FactorDefinition:
    """Bind a definition to an explicit catalog and reject false dependency claims."""
    parsed = parse_factor_expression(expression, feature_catalog)
    if dependency_columns is not None and dependency_columns != parsed.dependency_columns:
        raise FactorExpressionError("dependency_mismatch")
    return FactorDefinition(
        factor_id=factor_id,
        name_zh=name_zh,
        category=category,
        direction=direction,
        version=version,
        earliest_available_date=earliest_available_date,
        expression=parsed.expression,
        dependency_columns=parsed.dependency_columns,
        max_history_window=parsed.max_history_window,
        feature_catalog=feature_catalog,
    )
