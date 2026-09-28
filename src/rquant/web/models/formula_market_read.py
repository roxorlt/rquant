"""Public, bounded formula market task and result shapes."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.screen.formula_market_jobs import JobStatus


class _PublicModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class FormulaMarketJobItem(_PublicModel):
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    status: JobStatus
    status_label: str
    hint: str
    formula: str
    trade_date: date
    created_at: datetime
    updated_at: datetime
    result_available: bool


class FormulaMarketJobListData(_PublicModel):
    availability: Literal["unavailable", "not_published", "empty", "ready"]
    message: str
    available_at: datetime | None
    total_task_count: int = Field(ge=0)
    has_older_tasks: bool
    jobs: list[FormulaMarketJobItem] = Field(max_length=100)


class FormulaMarketUnknownReason(_PublicModel):
    reason: str
    label: str
    count: int = Field(ge=1)


class FormulaMarketResultSummary(_PublicModel):
    market_total: int = Field(ge=0)
    listed_count: int = Field(ge=0)
    paused_count: int = Field(ge=0)
    match_count: int = Field(ge=0)
    no_match_count: int = Field(ge=0)
    unknown_count: int = Field(ge=0)
    unknown_reasons: list[FormulaMarketUnknownReason]


class FormulaMarketJobDetailData(_PublicModel):
    job: FormulaMarketJobItem
    summary: FormulaMarketResultSummary | None


class FormulaMarketMatchesData(_PublicModel):
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    total: int = Field(ge=0)
    offset: int = Field(ge=0)
    match_codes: list[str] = Field(max_length=100)
    next_cursor: str | None
