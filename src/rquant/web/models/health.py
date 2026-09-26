"""``GET /api/v1/health``: runtime services, data freshness and the page data itself."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from rquant.web.models.common import StateCounts, StatusInfo


class ServiceItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    #: Technical id, for the tooltip and the detail drawer only.
    service_id: str
    name: str
    plane: str
    plane_label: str
    status: StatusInfo
    heartbeat_at: datetime | None
    observed_at: datetime
    #: Raw runtime fields, for the detail drawer.
    raw_status: str
    stale: bool
    input_sequence: int
    output_sequence: int
    backlog_count: int
    consecutive_failures: int
    last_error: str | None


class FreshnessItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    #: Dataset id or table name, for the tooltip only.
    key: str
    name: str
    kind: Literal["dataset", "market"]
    latest_at: datetime | None
    latest_date: date | None
    status: StatusInfo


class TableItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    name: str


class PageDataStatus(BaseModel):
    """The Serving generation, in the owner's words (页面数据)."""

    model_config = ConfigDict(frozen=True)

    status: StatusInfo
    built_at: datetime | None
    published_at: datetime | None
    age_seconds: float | None
    #: For the tooltip only.
    generation_id: str | None
    tables_total: int
    #: Page tables with no data source yet (their modules show a placeholder).
    unpublished: list[TableItem]


class ErrorItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    service_id: str
    name: str
    at: datetime | None
    summary: str
    #: The raw error text, for the tooltip only.
    message: str


class HealthData(BaseModel):
    model_config = ConfigDict(frozen=True)

    counts: StateCounts
    services: list[ServiceItem]
    freshness: list[FreshnessItem]
    page_data: PageDataStatus
    errors: list[ErrorItem]
