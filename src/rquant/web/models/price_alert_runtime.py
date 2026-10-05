"""Read-only price runtime DTOs; precise prices remain text."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _ReadModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


PriceRuntimeStatus = Literal["normal", "attention", "error", "not_running"]


class PriceAlertRuntimeItem(_ReadModel):
    rule_id: str
    version: int
    membership_version: int
    status: PriceRuntimeStatus
    status_label: Literal["正常", "注意", "异常", "未运行", "等待开盘", "已收盘", "午间休市"]
    message: str
    evaluated_at: datetime | None
    last_triggered_at: datetime | None
    next_allowed_at: datetime | None
    state: Literal["triggered", "not_triggered", "unavailable"] | None


class PriceAlertRuntimeData(_ReadModel):
    availability: Literal["ready", "unavailable", "not_running"]
    generation_id: str | None
    status: PriceRuntimeStatus
    status_label: Literal["正常", "注意", "异常", "未运行"]
    message: str
    evaluated_at: datetime | None
    quote_updated_at: datetime | None
    applied_at: datetime | None
    mode: Literal["record_only", "notification", "disabled"]
    items: list[PriceAlertRuntimeItem] = Field(max_length=100)


class PriceAlertNotificationFact(_ReadModel):
    channel: Literal["pushdeer", "pushplus"]
    state: Literal[
        "pending",
        "admitted",
        "accepted",
        "recorded",
        "unknown",
        "cancelled",
        "rejected",
        "expired",
        "unavailable",
    ]
    label: str
    message: str
    updated_at: datetime


class PriceAlertRecentEvent(_ReadModel):
    event_id: str
    rule_id: str
    rule_version: int
    membership_version: int
    rule_name: str
    ts_code: str
    comparison: Literal["gte", "lte"]
    threshold: str
    price: str
    triggered_at: datetime
    notifications: list[PriceAlertNotificationFact] = Field(max_length=2)
    route_message: str


class PriceAlertRecentEventsData(_ReadModel):
    availability: Literal["ready", "unavailable", "not_running"]
    generation_id: str | None
    message: str
    items: list[PriceAlertRecentEvent] = Field(max_length=20)
