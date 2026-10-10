"""Public, read-only data audit result shapes."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class AuditModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class AuditAttempt(AuditModel):
    status: Literal["running", "completed", "failed"]
    label: str
    observed_at: datetime
    completed_at: datetime | None


class AuditSuccess(AuditModel):
    as_of_date: date
    range_start: date
    range_end: date
    completed_at: datetime
    finding_count: int = Field(ge=0)
    p0_count: int = Field(ge=0)


class DataAuditHealthData(AuditModel):
    source_state: Literal["ready", "not_published", "unavailable"]
    latest_attempt: AuditAttempt | None
    latest_success: AuditSuccess | None


class DataAuditIssueItem(AuditModel):
    number: int = Field(ge=1)
    name: str
    severity: Literal["P0", "P1", "P2", "P3"]
    status: Literal["待处理", "已处理"]


class DataAuditIssuesData(AuditModel):
    source_state: Literal["ready", "not_published", "unavailable"]
    dataset_name: str
    total_count: int = Field(ge=0)
    partial: bool
    issues: list[DataAuditIssueItem]
