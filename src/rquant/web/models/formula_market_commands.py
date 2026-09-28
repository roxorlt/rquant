"""Browser input and public receipt for a formula market task admission."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime


class FormulaMarketCommandRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    formula: str = Field(min_length=1)
    trade_date: date


class FormulaMarketCommandReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: str
    status: Literal["queued", "pending", "processing", "failed", "ambiguous", "conflict"]
    task_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    message: str

    @model_validator(mode="after")
    def validate_task_identity(self) -> FormulaMarketCommandReceipt:
        if (self.status == "queued") != (self.task_id is not None):
            raise ValueError("only queued formula commands name a task")
        return self
