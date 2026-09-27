"""Durable identity for one successfully materialized pool result."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Sequence
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


def member_price_digest(prices: Sequence[tuple[str, float | None]]) -> str:
    """Hash domain + u64 count + sorted (u32 UTF-8 code + NULL/binary64 tag)."""
    seen: set[bytes] = set()
    rows: list[tuple[bytes, float | None]] = []
    for code, value in prices:
        if not isinstance(code, str) or not code:
            raise ValueError("pool price members must have unique nonempty ts_code values")
        code_bytes = code.encode("utf-8")
        if code_bytes in seen:
            raise ValueError("pool price members must have unique nonempty ts_code values")
        seen.add(code_bytes)
        if value is not None and not isinstance(value, float):
            raise ValueError("pool price digest requires persisted DOUBLE or SQL NULL")
        rows.append((code_bytes, value))

    digest = hashlib.sha256()
    digest.update(b"rquant/screen-run-price/v2\x00")
    digest.update(struct.pack(">Q", len(rows)))
    for code_bytes, value in sorted(rows, key=lambda row: row[0]):
        digest.update(struct.pack(">I", len(code_bytes)))
        digest.update(code_bytes)
        if value is None:
            digest.update(b"\x00")
        else:
            digest.update(b"\x01")
            digest.update(struct.pack(">d", value))
    return digest.hexdigest()


class ScreenRunReceiptDraft(RuntimeContractModel):
    """New run facts; only the store can seal v2 after reading its persisted rows."""

    contract: Literal["screen-run-receipt/v2"] = "screen-run-receipt/v2"
    trade_date: date
    preset_name: str = Field(min_length=1)
    definition_version: Sha256
    parent_trade_date: date | None = None
    parent_result_version: Sha256 | None = None
    hit_count: int = Field(ge=0)
    member_digest: Sha256
    lineage_complete: bool
    completed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_parent(self) -> Self:
        if (self.parent_trade_date is None) != (self.parent_result_version is None):
            raise ValueError("parent date and result version must be provided together")
        return self


class ScreenRunReceipt(RuntimeContractModel):
    contract: Literal["screen-run-receipt/v1", "screen-run-receipt/v2"] = "screen-run-receipt/v1"
    trade_date: date
    preset_name: str = Field(min_length=1)
    definition_version: Sha256
    parent_trade_date: date | None = None
    parent_result_version: Sha256 | None = None
    hit_count: int = Field(ge=0)
    member_digest: Sha256
    price_digest: Sha256 | None = None
    lineage_complete: bool
    completed_at: AwareUtcDatetime
    result_version: Sha256 | None = None

    @model_validator(mode="after")
    def bind_result_version(self) -> Self:
        if (self.parent_trade_date is None) != (self.parent_result_version is None):
            raise ValueError("parent date and result version must be provided together")
        if self.contract == "screen-run-receipt/v1" and self.price_digest is not None:
            raise ValueError("v1 receipt cannot claim a price_digest")
        if self.contract == "screen-run-receipt/v2" and self.price_digest is None:
            raise ValueError("v2 receipt requires price_digest")
        exclude = {"result_version"}
        if self.contract == "screen-run-receipt/v1":
            exclude.add("price_digest")
        expected = canonical_sha256(self.model_dump(mode="python", exclude=exclude))
        if self.result_version is None:
            object.__setattr__(self, "result_version", expected)
        elif self.result_version != expected:
            raise ValueError("screen run result version does not match receipt content")
        return self
