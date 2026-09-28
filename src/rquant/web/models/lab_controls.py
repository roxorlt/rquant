"""Typed browser request and bounded acknowledgement for research job controls."""

from __future__ import annotations

import re
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from rquant.runtime_contracts import AwareUtcDatetime

LabControlAction = Literal["pause", "resume", "cancel", "retry"]
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class LabControlRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: UUID
    requested_at: AwareUtcDatetime
    job_id: UUID
    action: LabControlAction
    expected_version: StrictInt = Field(ge=0)

    @field_validator("command_id", "job_id", mode="before")
    @classmethod
    def canonical_uuid(cls, value: object) -> object:
        if not isinstance(value, str) or _UUID.fullmatch(value) is None or UUID(value).int == 0:
            raise ValueError("zero UUID is not a command or job identity")
        return value


class LabControlReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: UUID
    status: Literal["submitted", "pending", "processing", "unknown", "conflict", "failed"]
    message: str = Field(min_length=1, max_length=80)


class LabControlCapabilities(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    can_control: bool
