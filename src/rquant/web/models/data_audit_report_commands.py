"""Browser input and public admission result for a read-only data audit report task."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_audit_contracts import MAX_AUDIT_DAYS
from rquant.runtime_contracts import AwareUtcDatetime


class AuditReportCommandRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    audit_start: date
    observed_through: date

    @model_validator(mode="after")
    def validate_range(self) -> AuditReportCommandRequest:
        days = (self.observed_through - self.audit_start).days + 1
        if not 1 <= days <= MAX_AUDIT_DAYS:
            raise ValueError("audit report range is outside the supported limit")
        return self


class AuditReportCommandReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: str
    status: Literal["queued", "pending", "processing", "failed", "ambiguous"]
    task_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    message: str
