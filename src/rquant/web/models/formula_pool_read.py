"""Public formula pool list and member pagination contracts."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _PublicModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class FormulaPoolUnknownReason(_PublicModel):
    reason: str
    label: str
    count: int = Field(ge=1)


class FormulaPoolLatestResult(_PublicModel):
    trade_date: date
    market_total: int = Field(ge=0)
    match_count: int = Field(ge=0)
    no_match_count: int = Field(ge=0)
    unknown_count: int = Field(ge=0)
    unknown_reasons: list[FormulaPoolUnknownReason]


class FormulaPoolItem(_PublicModel):
    pool_name: str
    display_name: str
    formula: str
    syntax_version: str
    created_at: datetime
    status_label: str
    latest_result: FormulaPoolLatestResult | None


class FormulaPoolListData(_PublicModel):
    availability: Literal["unavailable", "not_published", "empty", "ready"]
    message: str
    available_at: datetime | None
    pools: list[FormulaPoolItem] = Field(max_length=512)


class FormulaPoolMembersData(_PublicModel):
    pool_name: str
    trade_date: date
    total: int = Field(ge=0)
    offset: int = Field(ge=0)
    match_codes: list[str] = Field(max_length=100)
    next_cursor: str | None
