"""Small shared shape for bounded Lab-owned audit Serving rows."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel

MAX_AUDIT_ISSUES = 256
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class DataAuditStatusProjectionRow(RuntimeContractModel):
    latest_status: Literal["never_run", "running", "completed", "failed"]
    latest_observed_at: AwareUtcDatetime | None = None
    latest_completed_at: AwareUtcDatetime | None = None
    successful_audit_id: Sha256 | None = None
    successful_as_of_date: date | None = None
    successful_range_start: date | None = None
    successful_range_end: date | None = None
    successful_completed_at: AwareUtcDatetime | None = None
    finding_count: int = Field(ge=0, le=MAX_AUDIT_ISSUES)
    p0_count: int = Field(ge=0, le=MAX_AUDIT_ISSUES)

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        successful_fields = (
            self.successful_as_of_date,
            self.successful_range_start,
            self.successful_range_end,
            self.successful_completed_at,
        )
        if (self.successful_audit_id is None) != all(value is None for value in successful_fields):
            raise ValueError("audit success fields are incomplete")
        if self.successful_audit_id is not None and any(
            value is None for value in successful_fields
        ):
            raise ValueError("audit success fields are incomplete")
        if self.latest_status == "never_run" and self.latest_observed_at is not None:
            raise ValueError("never-run audit has an attempt")
        if self.latest_status != "never_run" and self.latest_observed_at is None:
            raise ValueError("audit attempt lacks an observation time")
        if self.latest_status in {"completed", "failed"} and self.latest_completed_at is None:
            raise ValueError("terminal audit lacks a completion time")
        if self.latest_status == "running" and self.latest_completed_at is not None:
            raise ValueError("running audit has a completion time")
        if self.latest_status == "completed" and self.successful_audit_id is None:
            raise ValueError("completed audit lacks successful evidence")
        if self.successful_audit_id is None and (self.finding_count or self.p0_count):
            raise ValueError("audit counts lack successful evidence")
        if self.p0_count > self.finding_count:
            raise ValueError("audit P0 count exceeds finding count")
        return self


class DataAuditIssueProjectionRow(RuntimeContractModel):
    audit_run_id: Sha256
    issue_id: Sha256
    dataset_id: str = Field(min_length=1, max_length=256)
    rule_id: str = Field(min_length=1, max_length=256)
    severity: Literal["P0", "P1", "P2", "P3"]
    status: Literal["open", "resolved"]
