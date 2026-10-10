"""Public, typed acknowledgment state shared by timeline and overview."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.alert_ack_models import (
    AlertAcknowledgmentView as AlertAcknowledgmentView,
)
from rquant.alert_ack_models import UnacknowledgedSummary as UnacknowledgedSummary
from rquant.runtime_contracts import AwareUtcDatetime


class AckCommandRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    alert_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class AckCommandReceipt(BaseModel):
    model_config = ConfigDict(frozen=True)

    command_id: str
    status: Literal["pending", "processing", "succeeded", "failed", "ambiguous"]
    confirmation_id: str | None = None
    message: str


class AckCommandConflict(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    detail: str
    code: Literal["stale_generation_no_effect"] | None = None
