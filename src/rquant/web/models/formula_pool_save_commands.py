"""Browser input and public receipt for a formula pool save command."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime


class FormulaPoolSaveCommandRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    command_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._-]+$")
    requested_at: AwareUtcDatetime
    base_name: str = Field(min_length=1, max_length=80, pattern=r"^[\w\u4e00-\u9fff-]+$")
    display_name: str = Field(min_length=1, max_length=80)
    task_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    expected_version: None


class FormulaPoolSaveCommandReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    command_id: str
    status: Literal["succeeded", "pending", "processing", "failed", "ambiguous", "conflict"]
    pool_name: str | None = None
    version: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    message: str

    @model_validator(mode="after")
    def validate_saved_identity(self) -> FormulaPoolSaveCommandReceipt:
        has_identity = self.pool_name is not None and self.version is not None
        if (self.status == "succeeded") != has_identity:
            raise ValueError("only a succeeded save names a pool version")
        return self
