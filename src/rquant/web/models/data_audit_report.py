"""Bounded public shapes for the offline daily-bar audit report."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from rquant.data_audit_contracts import MAX_AUDIT_DAYS, MAX_INDEXED_ISSUES, MAX_REPORT_ISSUES

ReportHash = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
RuleId = Literal["daily_bar.close_limit", "daily_bar.zero_volume", "daily_bar.field_null_ratio"]
IssueRuleId = Literal[
    "daily_bar.close_above_limit",
    "daily_bar.close_below_limit",
    "daily_bar.zero_volume_unsuspended",
    "daily_bar.field_null_ratio",
]
UnassessedReason = Literal[
    "no_daily_bar",
    "close_missing",
    "limits_unavailable",
    "volume_missing",
    "suspension_unknown",
    "no_observations",
]


class _ReportModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)


class ReportOverviewRow(_ReportModel):
    report_hash: ReportHash
    schema_version: Literal[1]
    rule_version: Literal["daily-bar-quality-v1"]
    run_status: Literal["completed"]
    collection_status: Literal["collection_unconfirmed"]
    collection_completed_through: None
    coverage_conclusion: Literal["unconfirmed"]
    quality_conclusion: Literal["not_fully_assessed", "issues_observed", "no_issues_observed"]
    current: Literal[False]
    source_mode: Literal["production_unverified"]
    source_namespace: Literal["production"]
    replica_generation_id: None
    audit_start: date
    observed_through: date
    expected_open_days: int = Field(ge=1, le=MAX_AUDIT_DAYS)
    covered_open_days: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    missing_open_days: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    gap_count: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    longest_gap_open_days: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    closed_day_count: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    monthly_count: int = Field(ge=1, le=122)
    rule_count: int = Field(ge=3, le=11)
    quality_issue_count: int = Field(ge=0, le=MAX_REPORT_ISSUES)
    indexed_issue_count: int = Field(ge=0, le=MAX_INDEXED_ISSUES)
    omitted_issue_count: int = Field(ge=0, le=MAX_REPORT_ISSUES)
    unassessed_rule_days: int = Field(ge=0, le=MAX_AUDIT_DAYS * 11)

    @model_validator(mode="after")
    def validate_counts(self) -> ReportOverviewRow:
        span = (self.observed_through - self.audit_start).days + 1
        if not 1 <= span <= MAX_AUDIT_DAYS:
            raise ValueError("report date range is invalid")
        if self.missing_open_days != self.expected_open_days - self.covered_open_days:
            raise ValueError("report coverage counts disagree")
        if (
            self.gap_count > self.missing_open_days
            or not 0 <= self.longest_gap_open_days <= self.missing_open_days
            or (self.gap_count == 0) != (self.missing_open_days == 0)
            or (self.longest_gap_open_days == 0) != (self.missing_open_days == 0)
        ):
            raise ValueError("report gap counts disagree")
        if self.quality_issue_count != self.indexed_issue_count + self.omitted_issue_count:
            raise ValueError("report issue counts disagree")
        if self.indexed_issue_count != min(self.quality_issue_count, MAX_INDEXED_ISSUES):
            raise ValueError("report issue index is incomplete")
        return self


class ReportMonthRow(_ReportModel):
    report_hash: ReportHash
    month: date
    expected_open_days: int = Field(ge=0, le=31)
    covered_open_days: int = Field(ge=0, le=31)
    coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    status: Literal["measured", "no_expected_sessions"]


class ReportRuleRow(_ReportModel):
    report_hash: ReportHash
    rule_id: RuleId
    field_name: str = Field(max_length=32)
    expected_days: int = Field(ge=1, le=MAX_AUDIT_DAYS)
    checked_days: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    assessed_days: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    unassessed_days: int = Field(ge=0, le=MAX_AUDIT_DAYS)
    first_assessed_date: date | None
    last_assessed_date: date | None
    assessment_complete: bool
    unassessed_reasons_json: str = Field(max_length=16 * 1024)
    issue_count: int = Field(ge=0, le=MAX_REPORT_ISSUES)

    @model_validator(mode="after")
    def validate_counts(self) -> ReportRuleRow:
        if (self.rule_id == "daily_bar.field_null_ratio") != bool(self.field_name):
            raise ValueError("report rule field disagrees with rule type")
        if not self.assessed_days <= self.checked_days <= self.expected_days:
            raise ValueError("report assessed days exceed checked days")
        if self.unassessed_days != self.expected_days - self.assessed_days:
            raise ValueError("report rule assessment counts disagree")
        if self.assessment_complete != (self.assessed_days == self.expected_days):
            raise ValueError("report rule completeness disagrees")
        if (self.first_assessed_date is None) != (self.assessed_days == 0) or (
            self.last_assessed_date is None
        ) != (self.assessed_days == 0):
            raise ValueError("report assessed date bounds disagree")
        if (
            self.first_assessed_date is not None
            and self.last_assessed_date is not None
            and self.first_assessed_date > self.last_assessed_date
        ):
            raise ValueError("report assessed date bounds are reversed")
        return self


class ReportIssueRow(_ReportModel):
    report_hash: ReportHash
    issue_index: int = Field(ge=0, le=MAX_INDEXED_ISSUES - 1)
    trade_date: date
    rule_id: IssueRuleId
    ts_code: str | None = Field(default=None, max_length=32)
    field_name: str | None = Field(default=None, max_length=32)
    observed_value: str | None = Field(default=None, max_length=128)
    reference_value: str | None = Field(default=None, max_length=128)
    null_rows: int | None = Field(default=None, ge=0, le=10_000)
    observed_rows: int | None = Field(default=None, ge=0, le=10_000)


class AuditReportOverview(ReportOverviewRow):
    collection_label: str
    coverage_label: str
    quality_label: str


class AuditReportMonth(_ReportModel):
    month: date
    expected_open_days: int = Field(ge=0)
    covered_open_days: int = Field(ge=0)
    coverage_ratio: float | None
    status: Literal["measured", "no_expected_sessions"]
    status_label: str


class AuditReportUnassessedReason(_ReportModel):
    reason: UnassessedReason
    name: str
    days: int = Field(ge=1, le=MAX_AUDIT_DAYS)


class AuditReportRule(_ReportModel):
    rule_id: RuleId
    name: str
    field_name: str | None
    field_label: str | None
    expected_days: int = Field(ge=1)
    checked_days: int = Field(ge=0)
    assessed_days: int = Field(ge=0)
    unassessed_days: int = Field(ge=0)
    first_assessed_date: date | None
    last_assessed_date: date | None
    assessment_complete: bool
    unassessed_reasons: list[AuditReportUnassessedReason]
    issue_count: int = Field(ge=0)


class AuditReportIssue(_ReportModel):
    number: int = Field(ge=1, le=MAX_INDEXED_ISSUES)
    trade_date: date
    rule_id: IssueRuleId
    name: str
    ts_code: str | None
    field_name: str | None
    field_label: str | None
    observed_value: str | None
    reference_value: str | None
    null_rows: int | None
    observed_rows: int | None


class DataAuditReportData(_ReportModel):
    source_state: Literal["ready", "not_published", "unavailable"]
    overview: AuditReportOverview | None
    months: list[AuditReportMonth]
    rules: list[AuditReportRule]
    issues: list[AuditReportIssue]
