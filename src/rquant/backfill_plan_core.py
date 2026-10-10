"""Build an offline, read-only proposal for whole-day daily-bar gaps."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfo

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.data_audit_coverage import (
    CalendarEvidence,
    DailyBarCountEvidence,
    DailyBarCoverageRequest,
    TradingDayGap,
    audit_daily_bar_coverage,
)
from rquant.data_audit_evidence import (
    MAX_AUDIT_DAYS,
    read_daily_bar_coverage_evidence_from_connection,
)
from rquant.security_status import count_namechange_windows

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CLOSE = time(15)
_SHA256_PATTERN = r"^[0-9a-f]{64}$"


class _PlanModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class BackfillEstimateAssumptions(_PlanModel):
    """Declared elapsed-time assumptions, not provider quota guarantees."""

    status_namechange_start: date
    status_source_as_of: date
    status_window_years: int = Field(ge=1, le=10, strict=True)
    adapter_seconds_per_operation: Decimal = Field(ge=0, le=3600)
    market_throttle_seconds_per_operation: Decimal = Field(ge=0, le=3600)
    status_throttle_seconds_per_operation: Decimal = Field(ge=0, le=3600)
    retry_allowance_seconds_per_operation: Decimal = Field(ge=0, le=3600)

    @model_validator(mode="after")
    def validate_dates(self) -> BackfillEstimateAssumptions:
        if self.status_namechange_start > self.status_source_as_of:
            raise ValueError("namechange start must not exceed source_as_of")
        return self


class BackfillPlanSource(_PlanModel):
    mode: Literal["production_unverified"] = "production_unverified"
    snapshot_label: str = Field(min_length=1, max_length=128)
    claimed_file_sha256: str = Field(pattern=_SHA256_PATTERN)
    identity_verified: Literal[False] = False
    collection_complete_verified: Literal[False] = False

    @model_validator(mode="after")
    def validate_label(self) -> BackfillPlanSource:
        if not self.snapshot_label.strip():
            raise ValueError("snapshot_label must be non-empty")
        return self


class BackfillEvidenceIdentity(_PlanModel):
    version: Literal["daily_bar_coverage.v1"] = "daily_bar_coverage.v1"
    code_revision: str = Field(min_length=1, max_length=80)
    calendar: CalendarEvidence
    daily_bar_counts: DailyBarCountEvidence
    calendar_sha256: str = Field(pattern=_SHA256_PATTERN)
    daily_bar_counts_sha256: str = Field(pattern=_SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_evidence_hashes(self) -> BackfillEvidenceIdentity:
        if not self.code_revision.strip():
            raise ValueError("evidence code revision must be non-empty")
        if (
            len(self.calendar.days) > MAX_AUDIT_DAYS
            or len(self.daily_bar_counts.days) > MAX_AUDIT_DAYS
        ):
            raise ValueError("daily-bar evidence exceeds audit range limit")
        if self.calendar_sha256 != _hash_payload(self.calendar.model_dump(mode="json")):
            raise ValueError("SSE calendar hash mismatch")
        if self.daily_bar_counts_sha256 != _hash_payload(
            self.daily_bar_counts.model_dump(mode="json")
        ):
            raise ValueError("daily-bar row-count hash mismatch")
        return self


class BackfillPlanMonth(_PlanModel):
    month: date
    expected_open_days: int = Field(ge=0, le=31, strict=True)
    covered_open_days: int = Field(ge=0, le=31, strict=True)
    missing_open_days: int = Field(ge=0, le=31, strict=True)

    @model_validator(mode="after")
    def validate_counts(self) -> BackfillPlanMonth:
        if self.covered_open_days + self.missing_open_days != self.expected_open_days:
            raise ValueError("monthly expected days must equal covered plus missing")
        return self


class BackfillLogicalOperations(_PlanModel):
    daily: int = Field(ge=0, le=MAX_AUDIT_DAYS, strict=True)
    daily_basic: int = Field(ge=0, le=MAX_AUDIT_DAYS, strict=True)
    adj_factor: int = Field(ge=0, le=MAX_AUDIT_DAYS, strict=True)
    namechange_context_batches: int = Field(ge=0, le=1, strict=True)
    namechange_windows: int = Field(ge=0, le=100, strict=True)
    stock_st_upper_bound: int = Field(ge=0, le=MAX_AUDIT_DAYS, strict=True)
    trade_cal: Literal[0] = 0
    total: int = Field(ge=0, le=20_000, strict=True)

    @model_validator(mode="after")
    def validate_total(self) -> BackfillLogicalOperations:
        if self.total != (
            self.daily
            + self.daily_basic
            + self.adj_factor
            + self.namechange_windows
            + self.stock_st_upper_bound
        ):
            raise ValueError("total logical operations do not match breakdown")
        return self


class BackfillPlanEstimate(_PlanModel):
    logical_operations: BackfillLogicalOperations
    assumptions: BackfillEstimateAssumptions
    estimated_seconds: Decimal = Field(ge=0, le=100_000_000)
    quota_status: Literal["unverified"] = "unverified"
    actual_http_calls_known: Literal[False] = False


def _validate_cutoff(
    observed_at: datetime,
    completed_through: date,
    assumptions: BackfillEstimateAssumptions,
) -> None:
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("cutoff observed_at must be timezone-aware")
    observed_local = observed_at.astimezone(_SHANGHAI)
    if completed_through > observed_local.date() or (
        completed_through == observed_local.date() and observed_local.time() < _CLOSE
    ):
        raise ValueError("completed_through must be after the Shanghai market close")
    if assumptions.status_source_as_of < completed_through:
        raise ValueError("status_source_as_of must include completed_through")
    if assumptions.status_source_as_of > observed_local.date():
        raise ValueError("status_source_as_of cannot be in the future")


def _estimate_operations(
    count: int,
    audit_start: date,
    assumptions: BackfillEstimateAssumptions,
) -> BackfillPlanEstimate:
    namechange_windows = (
        count_namechange_windows(
            min(assumptions.status_namechange_start, audit_start),
            assumptions.status_source_as_of,
            window_years=assumptions.status_window_years,
        )
        if count
        else 0
    )
    operations = BackfillLogicalOperations(
        daily=count,
        daily_basic=count,
        adj_factor=count,
        namechange_context_batches=int(count > 0),
        namechange_windows=namechange_windows,
        stock_st_upper_bound=count,
        total=4 * count + namechange_windows,
    )
    estimated_seconds = (
        Decimal(operations.total) * assumptions.adapter_seconds_per_operation
        + Decimal(3 * count) * assumptions.market_throttle_seconds_per_operation
        + Decimal(namechange_windows + count) * assumptions.status_throttle_seconds_per_operation
        + Decimal(operations.total) * assumptions.retry_allowance_seconds_per_operation
    )
    return BackfillPlanEstimate(
        logical_operations=operations,
        assumptions=assumptions,
        estimated_seconds=estimated_seconds,
    )


class _BackfillPlanBody(_PlanModel):
    version: Literal["daily_bar_backfill_plan.v1"] = "daily_bar_backfill_plan.v1"
    dataset: Literal["daily_bar"] = "daily_bar"
    exchange: Literal["SSE"] = "SSE"
    audit_start: date
    completed_through: date
    cutoff_observed_at_utc: datetime
    source: BackfillPlanSource
    evidence: BackfillEvidenceIdentity
    missing_dates: tuple[date, ...] = Field(max_length=MAX_AUDIT_DAYS)
    gaps: tuple[TradingDayGap, ...] = Field(max_length=MAX_AUDIT_DAYS)
    monthly: tuple[BackfillPlanMonth, ...] = Field(max_length=MAX_AUDIT_DAYS)
    estimate: BackfillPlanEstimate
    coverage_scope: Literal["whole_day_presence_only"] = "whole_day_presence_only"
    executable: Literal[False] = False

    @model_validator(mode="after")
    def validate_structure(self) -> _BackfillPlanBody:
        if self.audit_start > self.completed_through:
            raise ValueError("audit_start must not exceed completed_through")
        _validate_cutoff(
            self.cutoff_observed_at_utc,
            self.completed_through,
            self.estimate.assumptions,
        )
        if self.source.snapshot_label != self.evidence.calendar.snapshot_id:
            raise ValueError("snapshot label differs from calendar evidence")
        coverage_request = DailyBarCoverageRequest(
            audit_start=self.audit_start,
            completed_through=self.completed_through,
            calendar=self.evidence.calendar,
            daily_bar_counts=self.evidence.daily_bar_counts,
        )
        coverage = audit_daily_bar_coverage(coverage_request)
        row_counts = {item.day: item.row_count for item in self.evidence.daily_bar_counts.days}
        actual_missing = tuple(
            item.day
            for item in self.evidence.calendar.days
            if item.is_open and row_counts.get(item.day, 0) == 0
        )
        if self.missing_dates != actual_missing or self.gaps != coverage.gaps:
            raise ValueError("missing_dates and gaps must match SSE calendar evidence")
        expected_monthly = tuple(
            BackfillPlanMonth(
                month=item.month,
                expected_open_days=item.expected_open_days,
                covered_open_days=item.covered_open_days,
                missing_open_days=item.expected_open_days - item.covered_open_days,
            )
            for item in coverage.monthly
        )
        if self.monthly != expected_monthly:
            raise ValueError("monthly summary must match SSE calendar evidence")
        if any(
            day < self.audit_start or day > self.completed_through for day in self.missing_dates
        ) or any(
            self.missing_dates[index - 1] >= self.missing_dates[index]
            for index in range(1, len(self.missing_dates))
        ):
            raise ValueError("missing_dates must be unique, ordered, and within the range")
        if sum(gap.missing_open_days for gap in self.gaps) != len(self.missing_dates):
            raise ValueError("gap counts must equal the full missing date count")
        if sum(month.missing_open_days for month in self.monthly) != len(self.missing_dates):
            raise ValueError("monthly counts must equal the full missing date count")
        operations = self.estimate.logical_operations
        count = len(self.missing_dates)
        if (
            operations.daily != count
            or operations.daily_basic != count
            or operations.adj_factor != count
            or operations.stock_st_upper_bound != count
            or operations.namechange_context_batches != int(count > 0)
        ):
            raise ValueError("operation breakdown must use one exact missing-date batch")
        if self.estimate != _estimate_operations(
            count, self.audit_start, self.estimate.assumptions
        ):
            raise ValueError("estimate must match the exact missing-date batch and assumptions")
        return self


class DailyBarBackfillPlan(_BackfillPlanBody):
    content_sha256: str = Field(pattern=_SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_hash(self) -> DailyBarBackfillPlan:
        payload = self.model_dump(mode="json", exclude={"content_sha256"})
        if self.content_sha256 != _hash_payload(payload):
            raise ValueError("backfill plan content hash mismatch")
        return self


def _hash_payload(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_daily_bar_backfill_plan(
    connection: duckdb.DuckDBPyConnection,
    *,
    snapshot_label: str,
    snapshot_file_sha256: str,
    evidence_code_revision: str,
    audit_start: date,
    completed_through: date,
    observed_at: datetime,
    assumptions: BackfillEstimateAssumptions,
) -> DailyBarBackfillPlan:
    """Propose exact historical dates from one caller-fixed read-only connection."""
    _validate_cutoff(observed_at, completed_through, assumptions)

    source = BackfillPlanSource(
        snapshot_label=snapshot_label, claimed_file_sha256=snapshot_file_sha256
    )
    evidence = read_daily_bar_coverage_evidence_from_connection(
        connection,
        snapshot_id=source.snapshot_label,
        audit_start=audit_start,
        completed_through=completed_through,
    )
    coverage = audit_daily_bar_coverage(evidence)
    row_counts = {item.day: item.row_count for item in evidence.daily_bar_counts.days}
    missing_dates = tuple(
        item.day
        for item in evidence.calendar.days
        if item.is_open and row_counts.get(item.day, 0) == 0
    )
    body = _BackfillPlanBody(
        audit_start=audit_start,
        completed_through=completed_through,
        cutoff_observed_at_utc=observed_at.astimezone(UTC),
        source=source,
        evidence=BackfillEvidenceIdentity(
            code_revision=evidence_code_revision,
            calendar=evidence.calendar,
            daily_bar_counts=evidence.daily_bar_counts,
            calendar_sha256=_hash_payload(evidence.calendar.model_dump(mode="json")),
            daily_bar_counts_sha256=_hash_payload(
                evidence.daily_bar_counts.model_dump(mode="json")
            ),
        ),
        missing_dates=missing_dates,
        gaps=coverage.gaps,
        monthly=tuple(
            BackfillPlanMonth(
                month=item.month,
                expected_open_days=item.expected_open_days,
                covered_open_days=item.covered_open_days,
                missing_open_days=item.expected_open_days - item.covered_open_days,
            )
            for item in coverage.monthly
        ),
        estimate=_estimate_operations(len(missing_dates), audit_start, assumptions),
    )
    payload = body.model_dump(mode="json")
    return DailyBarBackfillPlan.model_validate(
        {**payload, "content_sha256": _hash_payload(payload)}
    )
