"""Public read-only shapes for sealed daily-bar gap proposals."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

PlanHash = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
PlanSourceState = Literal["ready", "empty", "not_published", "unavailable"]


class _PlanModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)


class BackfillPlanItem(_PlanModel):
    rank: int = Field(ge=0, le=4095)
    plan_hash: PlanHash
    published_at: datetime
    audit_start: date
    completed_through: date
    cutoff_observed_at: datetime
    missing_day_count: int = Field(ge=0, le=3660)
    estimated_seconds: str = Field(pattern=r"^\d+(?:\.\d+)?$")
    source_mode: Literal["production_unverified"]
    snapshot_label: str = Field(min_length=1, max_length=128)
    identity_verified: Literal[False]
    collection_complete_verified: Literal[False]
    quota_status: Literal["unverified"]
    executable: Literal[False]


class BackfillPlanProgress(_PlanModel):
    availability: Literal["unavailable"]
    task_id: None
    message: Literal["任务进度尚未提供"] = "任务进度尚未提供"
    logs: list[str] = Field(default_factory=list, max_length=100)


class BackfillPlanCatalogRow(_PlanModel):
    catalog_key: Literal["current"]
    total_plan_count: int = Field(ge=0, le=4096)
    indexed_plan_count: int = Field(ge=0, le=4096)
    preview_plan_count: int = Field(ge=0, le=8)
    has_older_plans: bool
    oldest_indexed_hash: PlanHash | None


class BackfillPlanProgressRow(_PlanModel):
    status_key: Literal["current"]
    availability: Literal["unavailable"]
    task_id: None


class BackfillPlanSource(_PlanModel):
    mode: Literal["production_unverified"]
    snapshot_label: str = Field(min_length=1, max_length=128)
    claimed_file_sha256: PlanHash
    identity_verified: Literal[False]
    collection_complete_verified: Literal[False]


class BackfillPlanMonth(_PlanModel):
    month: date
    expected_open_days: int = Field(ge=0, le=31)
    covered_open_days: int = Field(ge=0, le=31)
    missing_open_days: int = Field(ge=0, le=31)


class BackfillLogicalOperations(_PlanModel):
    daily: int = Field(ge=0, le=3660)
    daily_basic: int = Field(ge=0, le=3660)
    adj_factor: int = Field(ge=0, le=3660)
    namechange_context_batches: int = Field(ge=0, le=1)
    namechange_windows: int = Field(ge=0, le=100)
    stock_st_upper_bound: int = Field(ge=0, le=3660)
    trade_cal: Literal[0]
    total: int = Field(ge=0, le=20_000)


class BackfillEstimateAssumptions(_PlanModel):
    status_namechange_start: date
    status_source_as_of: date
    status_window_years: int = Field(ge=1, le=10)
    adapter_seconds_per_operation: Decimal = Field(ge=0)
    market_throttle_seconds_per_operation: Decimal = Field(ge=0)
    status_throttle_seconds_per_operation: Decimal = Field(ge=0)
    retry_allowance_seconds_per_operation: Decimal = Field(ge=0)


class BackfillPlanEstimate(_PlanModel):
    logical_operations: BackfillLogicalOperations
    assumptions: BackfillEstimateAssumptions
    estimated_seconds: Decimal = Field(ge=0)
    quota_status: Literal["unverified"]
    actual_http_calls_known: Literal[False]


class BackfillPlanDetail(_PlanModel):
    plan_hash: PlanHash
    published_at: datetime
    audit_start: date
    completed_through: date
    cutoff_observed_at: datetime
    missing_day_count: int = Field(ge=0, le=3660)
    estimated_seconds: str = Field(pattern=r"^\d+(?:\.\d+)?$")
    source_mode: Literal["production_unverified"]
    snapshot_label: str = Field(min_length=1, max_length=128)
    identity_verified: Literal[False]
    collection_complete_verified: Literal[False]
    quota_status: Literal["unverified"]
    executable: Literal[False]
    missing_dates: list[date] = Field(max_length=3660)
    monthly: list[BackfillPlanMonth] = Field(max_length=122)
    estimate: BackfillPlanEstimate
    source: BackfillPlanSource
    gap_count: int = Field(ge=0, le=3660)
    coverage_scope: Literal["whole_day_presence_only"]


class BackfillPlansData(_PlanModel):
    source_state: PlanSourceState
    total: int | None = Field(default=None, ge=0, le=4096)
    page_size: int = Field(ge=1, le=50)
    items: list[BackfillPlanItem] = Field(max_length=50)
    next_cursor: PlanHash | None = None
    progress: BackfillPlanProgress | None = None


class BackfillPlanDetailData(_PlanModel):
    source_state: PlanSourceState
    plan: BackfillPlanDetail | None = None
    progress: BackfillPlanProgress | None = None
