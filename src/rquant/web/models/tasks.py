"""A bounded view of published research jobs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

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
