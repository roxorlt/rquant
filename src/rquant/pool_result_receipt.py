"""Durable identity for one successfully materialized pool result."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


def member_set_digest(codes: list[str]) -> str:
    """Hash the sorted exact member set, independent of display row order."""
    if len(codes) != len(set(codes)) or any(not code for code in codes):
        raise ValueError("pool members must have unique nonempty ts_code values")
    return canonical_sha256(sorted(codes))


class ScreenRunReceipt(RuntimeContractModel):
    contract: Literal["screen-run-receipt/v1"] = "screen-run-receipt/v1"
    trade_date: date
    preset_name: str = Field(min_length=1)
    definition_version: Sha256
    parent_trade_date: date | None = None
    parent_result_version: Sha256 | None = None
    hit_count: int = Field(ge=0)
    member_digest: Sha256
    lineage_complete: bool
    completed_at: AwareUtcDatetime
    result_version: Sha256 | None = None

    @model_validator(mode="after")
    def bind_result_version(self) -> Self:
        if (self.parent_trade_date is None) != (self.parent_result_version is None):
            raise ValueError("parent date and result version must be provided together")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"result_version"}))
        if self.result_version is None:
            object.__setattr__(self, "result_version", expected)
        elif self.result_version != expected:
            raise ValueError("screen run result version does not match receipt content")
        return self
