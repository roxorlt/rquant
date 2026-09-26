"""Typed, user-facing contract for manual condition screening."""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ScreenOption(BaseModel):
    model_config = ConfigDict(frozen=True)

    value: str
    label: str


class ScreenParameter(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    label: str
    input: Literal["number", "integer", "choice", "multi_choice", "field", "operand"]
    initial: str | int | float | list[str] | None
    required: bool
    minimum: float | None = None
    maximum: float | None = None
    scale: float = 1
    options: list[ScreenOption] = Field(default_factory=list)
    hint: str | None = None


class ScreenBlock(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    label: str
    hint: str
    category: str
    category_label: str
    parameters: list[ScreenParameter]


class ScreenCatalogData(BaseModel):
    model_config = ConfigDict(frozen=True)

    blocks: list[ScreenBlock]
    dates: list[date]
    available: bool
    ranking_metrics: list[ScreenOption]


class ScreenCondition(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str = Field(min_length=1, max_length=64)
    args: dict[str, Any] = Field(default_factory=dict)


class ScreenRankingCondition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric: str = Field(min_length=1, max_length=64)
    ascending: bool
    weight: float = Field(ge=0, le=100, allow_inf_nan=False)


class ScreenRankingPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conditions: list[ScreenRankingCondition] = Field(min_length=1, max_length=4)
    top_n: int = Field(ge=1, le=100)

    @model_validator(mode="after")
    def validate_weights_and_metrics(self) -> ScreenRankingPlan:
        if sum(condition.weight for condition in self.conditions) <= 0:
            raise ValueError("at least one ranking weight must be positive")
        metrics = [condition.metric for condition in self.conditions]
        if len(metrics) != len(set(metrics)):
            raise ValueError("ranking metrics must be unique")
        return self


class ScreenRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade_date: date
    conditions: list[ScreenCondition] = Field(min_length=1, max_length=26)
    page_size: int = Field(default=20, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=1024)
    ranking: ScreenRankingPlan | None = None


class ScreenStep(BaseModel):
    model_config = ConfigDict(frozen=True)

    label: str
    count: int


class ScreenRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    ts_code: str
    name: str | None
    close: float | None
    pct_chg: float | None
    ranking_score: float | None = None
    rank_position: int | None = None


class ScreenRunData(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_date: date
    status: Literal["ready", "unavailable", "no_date"]
    base_count: int | None
    total: int | None
    ranked_count: int | None = None
    steps: list[ScreenStep]
    rows: list[ScreenRow]
    next_cursor: str | None
