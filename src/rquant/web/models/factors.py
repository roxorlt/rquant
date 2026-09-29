"""Public, read-only factor definition catalog response."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.factor.evaluate import FactorDirection


class FactorDefinitionItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    factor_id: str
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    name_zh: str
    category_label: str
    direction: FactorDirection
    direction_label: str
    version: int
    earliest_available_date: date
    archived: bool
    expression: str
    dependency_columns: list[str]
    max_history_window: int


class FactorCatalogData(BaseModel):
    model_config = ConfigDict(frozen=True)

    availability: Literal["unavailable", "empty", "populated"]
    available_at: datetime | None
    definitions: list[FactorDefinitionItem]
