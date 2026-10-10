"""Published experiment attempt facts for the private read-only list."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExperimentItem(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    experiment_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    hypothesis_family: str = Field(min_length=1)
    registered_at: datetime
    status: Literal["registered", "running", "executed", "succeeded", "failed", "cancelled"]
    completed_at: datetime | None
    trade_count: int | None = Field(ge=0)
    net_return_pct: float | None = Field(allow_inf_nan=False)
    max_drawdown_pct: float | None = Field(ge=0, le=100, allow_inf_nan=False)
    win_rate_pct: float | None = Field(ge=0, le=100, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_result(self) -> ExperimentItem:
        values = (self.trade_count, self.net_return_pct, self.max_drawdown_pct, self.win_rate_pct)
        if self.status == "succeeded" and any(value is None for value in values):
            raise ValueError("succeeded experiment lacks published result")
        if self.status != "succeeded" and any(value is not None for value in values):
            raise ValueError("unfinished experiment has published result")
        return self


class ExperimentListData(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    available: bool
    items: list[ExperimentItem] = Field(max_length=50)
    retained_count: int = Field(ge=0, le=500)
    truncated: bool
    oldest_registered_at: datetime | None
    next_cursor: str | None
