"""Typed, user-facing contract for manual condition screening."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.screen.tdx.ast import ParseResult


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
    custom_ma: bool = False


class ScreenBlock(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    label: str
    hint: str
    category: str
    category_label: str
    parameters: list[ScreenParameter]


class ScreenSourceInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    identity: str
    updated_at: datetime
    mode: Literal["daily", "intraday"] = "daily"
    cutoff: AwareUtcDatetime | None = None
    daily_anchor_date: date | None = None
    intraday_source_identity: str | None = None
    coverage_count: int | None = None
    missing_count: int | None = None


class ScreenCatalogData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_kind: Literal["serving", "replica", "intraday"]
    blocks: list[ScreenBlock]
    dates: list[date]
    available: bool
    ranking_metrics: list[ScreenOption]
    source: ScreenSourceInfo | None
    nl_generate_available: bool = False


class ScreenCondition(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str = Field(min_length=1, max_length=64)
    args: dict[str, Any] = Field(default_factory=dict)


class ScreenNlPreviewRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source_kind: Literal["serving", "replica"]
    source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    trade_date: date
    instruction: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_instruction(self) -> ScreenNlPreviewRequest:
        if not self.instruction.strip():
            raise ValueError("instruction must not be blank")
        return self


class ScreenNlPreviewData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_kind: Literal["serving", "replica"]
    source_identity: str
    trade_date: date
    conditions: list[ScreenCondition]


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
    source_identity: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    ranking: ScreenRankingPlan | None = None
    mode: Literal["daily", "intraday"] = "daily"
    decision_cutoff: AwareUtcDatetime | None = None
    intraday_source_identity: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def require_mode_source(self) -> ScreenRunRequest:
        if self.mode == "intraday" and (
            self.source_identity is None
            or self.decision_cutoff is None
            or self.intraday_source_identity is None
        ):
            raise ValueError("intraday screening requires its source and cutoff")
        if self.mode == "daily" and (
            self.decision_cutoff is not None or self.intraday_source_identity is not None
        ):
            raise ValueError("daily screening cannot contain intraday source context")
        return self


class ScreenStep(BaseModel):
    model_config = ConfigDict(frozen=True)

    label: str
    count: int
    unknown_count: int = Field(default=0, ge=0)


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
    unknown_count: int = Field(default=0, ge=0)
    ranked_count: int | None = None
    steps: list[ScreenStep]
    rows: list[ScreenRow]
    next_cursor: str | None
    source: ScreenSourceInfo | None


class TdxParseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = ""


class TdxParseData(ParseResult):
    capability: Literal["parse_only"] = "parse_only"


class TdxPreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = ""
    stock_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    trade_date: date
    source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")


class TdxPreviewData(BaseModel):
    model_config = ConfigDict(frozen=True)

    stock_code: str
    trade_date: date
    status: Literal["match", "no_match", "unknown"]
    reason: str | None
    source_updated_at: datetime


class TdxPreviewSourceData(BaseModel):
    model_config = ConfigDict(frozen=True)

    available: bool
    dates: list[date]
    source: ScreenSourceInfo | None
