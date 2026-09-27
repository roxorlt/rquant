"""A bounded view of published research jobs."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime
from rquant.web.models.common import StatusInfo

JobSourceState = Literal["ready", "empty", "not_published", "unavailable"]


class JobCounts(BaseModel):
    model_config = ConfigDict(frozen=True)

    queued: int = Field(ge=0)
    running: int = Field(ge=0)
    checkpointed: int = Field(ge=0)
    succeeded: int = Field(ge=0)
    failed: int = Field(ge=0)
    cancelled: int = Field(ge=0)
    other: int = Field(ge=0)


class ResearchJobItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    strategy_name: str
    job_type_label: str
    resource_label: str
    status: StatusInfo
    progress_fraction: float = Field(ge=0, le=1, allow_inf_nan=False)
    terminal_shards: int = Field(ge=0)
    total_shards: int = Field(ge=0)
    eta_at: AwareUtcDatetime | None
    eta_low: AwareUtcDatetime | None
    eta_high: AwareUtcDatetime | None
    eta_label: str
    updated_at: AwareUtcDatetime


class ResearchJobsData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: JobSourceState
    source_label: str
    source_note: str | None
    source_updated_at: AwareUtcDatetime | None
    total: int | None = Field(ge=0)
    counts: JobCounts | None
    page_size: int = Field(ge=1, le=50)
    items: list[ResearchJobItem]
    next_cursor: str | None


OverviewSourceState = Literal["ready", "unavailable"]


def _validate_remaining(state: OverviewSourceState, remaining: float | None) -> None:
    if state == "ready" and (remaining is None or remaining <= 0):
        raise ValueError("ready ops source requires a positive remaining budget")
    if state == "unavailable" and remaining not in (None, 0):
        raise ValueError("unavailable ops source cannot retain a positive budget")


class ScheduledTaskItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(max_length=40)
    status: StatusInfo
    last_trigger_at: AwareUtcDatetime | None
    next_at: AwareUtcDatetime | None
    duration_seconds: float | None = Field(ge=0, allow_inf_nan=False)
    result_label: str
    timer_unit: str = Field(max_length=128)
    service_unit: str = Field(max_length=128)


class ScheduledTasksData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: OverviewSourceState
    source_label: str
    source_note: str | None
    source_updated_at: AwareUtcDatetime | None
    expires_at: AwareUtcDatetime | None
    remaining_seconds: float | None = Field(ge=0, le=120, allow_inf_nan=False)
    items: list[ScheduledTaskItem] = Field(max_length=32)

    @model_validator(mode="after")
    def validate_remaining(self) -> Self:
        _validate_remaining(self.source_state, self.remaining_seconds)
        return self


class RuntimeServiceItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    plane_label: str
    status: StatusInfo
    heartbeat_at: AwareUtcDatetime | None
    service_id: str = Field(max_length=128)


class RuntimeServicesData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: OverviewSourceState
    source_label: str
    source_note: str | None
    source_updated_at: AwareUtcDatetime | None
    items: list[RuntimeServiceItem] = Field(max_length=32)


class ResourceGroupItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    slice_unit: str
    memory_current_bytes: int | None = Field(ge=0)
    memory_peak_bytes: int | None = Field(ge=0)


class ResourcesData(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_state: OverviewSourceState
    source_label: str
    source_note: str | None
    source_updated_at: AwareUtcDatetime | None
    expires_at: AwareUtcDatetime | None
    remaining_seconds: float | None = Field(ge=0, le=120, allow_inf_nan=False)
    host_memory_total_bytes: int | None = Field(ge=0)
    host_memory_available_bytes: int | None = Field(ge=0)
    rquant_memory_current_bytes: int | None = Field(ge=0)
    rquant_memory_peak_bytes: int | None = Field(ge=0)
    groups: list[ResourceGroupItem] = Field(max_length=4)
    cpu_usage_percent: float | None = Field(ge=0, le=100, allow_inf_nan=False)
    cpu_note: str

    @model_validator(mode="after")
    def validate_remaining(self) -> Self:
        _validate_remaining(self.source_state, self.remaining_seconds)
        return self


class TaskOverviewData(BaseModel):
    model_config = ConfigDict(frozen=True)

    scheduled: ScheduledTasksData
    services: RuntimeServicesData
    resources: ResourcesData
    research: ResearchJobsData
