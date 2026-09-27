"""Typed financial summary contract for the data center."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

ReasonLabel = Literal[
    "尚无来源记录",
    "披露尚未可见",
    "字段缺值",
    "来源证据不足",
    "日历待核验",
    "候选数量超限",
    "数值不可用",
    "其他原因",
]
_FIELD_KEYS = {"pe_ttm", "pb", "dv_ttm", "roe", "or_yoy", "netprofit_yoy"}


class FinancialReasonCount(BaseModel):
    model_config = ConfigDict(frozen=True)

    label: ReasonLabel
    count: int = Field(ge=1)


class FinancialFieldCount(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: Literal["pe_ttm", "pb", "dv_ttm", "roe", "or_yoy", "netprofit_yoy"]
    label: str
    unit: str
    known_count: int | None = Field(ge=0)
    unknown_count: int | None = Field(ge=0)
    reasons: list[FinancialReasonCount] = Field(max_length=4)


class FinancialSummarySource(BaseModel):
    model_config = ConfigDict(frozen=True)

    identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    updated_at: datetime


class FundamentalSummaryData(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["ready", "not_configured", "calendar_unavailable", "no_records"]
    decision_date: date | None
    waiting_for_today: bool
    source: FinancialSummarySource | None
    record_count: int | None = Field(ge=1)
    fields: list[FinancialFieldCount] = Field(min_length=6, max_length=6)
    coverage_note: Literal["全市场覆盖尚未核验"]

    @model_validator(mode="after")
    def verify_evidence_counts(self) -> Self:
        if {field.key for field in self.fields} != _FIELD_KEYS:
            raise ValueError("financial summary must contain six unique fields")
        if self.status == "ready":
            if self.source is None or self.decision_date is None or self.record_count is None:
                raise ValueError("ready financial summary requires a verified source and records")
            for field in self.fields:
                if (
                    field.known_count is None
                    or field.unknown_count is None
                    or field.known_count + field.unknown_count != self.record_count
                    or sum(reason.count for reason in field.reasons) != field.unknown_count
                    or len({reason.label for reason in field.reasons}) != len(field.reasons)
                ):
                    raise ValueError("financial field counts do not reconcile")
        elif self.record_count is not None or any(
            field.known_count is not None or field.unknown_count is not None or field.reasons
            for field in self.fields
        ):
            raise ValueError("unavailable financial summary cannot report counts")
        if self.status == "not_configured" and (
            self.source is not None or self.decision_date is not None
        ):
            raise ValueError("unconfigured financial summary has no source")
        if self.status == "calendar_unavailable" and (
            self.source is None or self.decision_date is not None
        ):
            raise ValueError("unverified calendar has no decision date")
        if self.status == "no_records" and (self.source is None or self.decision_date is None):
            raise ValueError("empty financial summary needs a decision date")
        return self
