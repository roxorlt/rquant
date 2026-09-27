"""Shared presentation data contracts for acknowledgment state."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.runtime_contracts import AwareUtcDatetime


class AlertAcknowledgmentView(BaseModel):
    model_config = ConfigDict(frozen=True)

    alert_id: str | None = None
    state: Literal["unavailable", "historical", "unconfirmed", "confirmed"]
    eligible: bool
    confirmation_id: str | None = None
    confirmed_at: datetime | None = None
    label: str
    note: str | None = None


class UnacknowledgedSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: Literal["unavailable", "source_incomplete", "ready"] = "unavailable"
    count: int | None = Field(default=None, ge=0)
    count_as_of: AwareUtcDatetime | None = None
    label: str = "确认状态暂不可用"
    note: str | None = "确认信息尚未发布，请稍后查看。"
