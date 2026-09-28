"""Durable identity for one successfully materialized pool result."""

from __future__ import annotations

import hashlib
import math
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


def member_rank_digest(ranks: Sequence[tuple[str, int, float]]) -> str:
    """Hash code-sorted rank positions and persisted binary64 scores for v3."""
    seen: set[bytes] = set()
    positions: set[int] = set()
    rows: list[tuple[bytes, int, float]] = []
    for code, position, score in ranks:
        if not isinstance(code, str) or not code:
            raise ValueError("rank members must have unique nonempty ts_code values")
        code_bytes = code.encode("utf-8")
        if code_bytes in seen:
            raise ValueError("rank members must have unique nonempty ts_code values")
        seen.add(code_bytes)
        if type(position) is not int or position < 1 or position in positions:
            raise ValueError("rank positions must be unique positive integers")
        positions.add(position)
        if not isinstance(score, float) or not math.isfinite(score) or not 0 <= score <= 100:
            raise ValueError("ranking scores must be finite persisted DOUBLE values in 0..100")
        rows.append((code_bytes, position, score))
    if positions != set(range(1, len(rows) + 1)):
        raise ValueError("rank positions must cover 1..N without gaps")

    digest = hashlib.sha256()
    digest.update(b"rquant/screen-run-rank/v3\x00")
    digest.update(struct.pack(">Q", len(rows)))
    for code_bytes, position, score in sorted(rows, key=lambda row: row[0]):
        digest.update(struct.pack(">I", len(code_bytes)))
        digest.update(code_bytes)
        digest.update(struct.pack(">Q", position))
        digest.update(struct.pack(">d", score))
    return digest.hexdigest()


class ScreenRunReceiptDraft(RuntimeContractModel):
    """New run facts; only the store can seal proofs after persisted readback."""

    contract: Literal["screen-run-receipt/v2", "screen-run-receipt/v3"] = "screen-run-receipt/v2"
    trade_date: date
    preset_name: str = Field(min_length=1)
    definition_version: Sha256
    parent_trade_date: date | None = None
    parent_result_version: Sha256 | None = None
    hit_count: int = Field(ge=0)
    member_digest: Sha256
    rank_digest: Sha256 | None = Field(default=None, exclude_if=lambda value: value is None)
    lineage_complete: bool
    completed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_parent(self) -> Self:
        if (self.parent_trade_date is None) != (self.parent_result_version is None):
            raise ValueError("parent date and result version must be provided together")
        if self.contract == "screen-run-receipt/v2" and self.rank_digest is not None:
            raise ValueError("v2 draft cannot claim a rank_digest")
        if self.contract == "screen-run-receipt/v3" and self.rank_digest is None:
            raise ValueError("v3 draft requires rank_digest")
        return self


class ScreenRunReceipt(RuntimeContractModel):
    contract: Literal[
        "screen-run-receipt/v1", "screen-run-receipt/v2", "screen-run-receipt/v3"
    ] = "screen-run-receipt/v1"
    trade_date: date
    preset_name: str = Field(min_length=1)
    definition_version: Sha256
    parent_trade_date: date | None = None
    parent_result_version: Sha256 | None = None
    hit_count: int = Field(ge=0)
    member_digest: Sha256
    price_digest: Sha256 | None = None
    rank_digest: Sha256 | None = Field(default=None, exclude_if=lambda value: value is None)
    lineage_complete: bool
    completed_at: AwareUtcDatetime
    result_version: Sha256 | None = None

    @model_validator(mode="after")
    def bind_result_version(self) -> Self:
        if (self.parent_trade_date is None) != (self.parent_result_version is None):
            raise ValueError("parent date and result version must be provided together")
        if self.contract == "screen-run-receipt/v1" and self.price_digest is not None:
            raise ValueError("v1 receipt cannot claim a price_digest")
        if (
            self.contract in {"screen-run-receipt/v2", "screen-run-receipt/v3"}
            and self.price_digest is None
        ):
            raise ValueError("v2/v3 receipt requires price_digest")
        if self.contract != "screen-run-receipt/v3" and self.rank_digest is not None:
            raise ValueError("v1/v2 receipt cannot claim a rank_digest")
        if self.contract == "screen-run-receipt/v3" and self.rank_digest is None:
            raise ValueError("v3 receipt requires rank_digest")
        exclude = {"result_version"}
        if self.contract == "screen-run-receipt/v1":
            exclude.add("price_digest")
        if self.contract != "screen-run-receipt/v3":
            exclude.add("rank_digest")
        expected = canonical_sha256(self.model_dump(mode="python", exclude=exclude))
        if self.result_version is None:
            object.__setattr__(self, "result_version", expected)
        elif self.result_version != expected:
            raise ValueError("screen run result version does not match receipt content")
        return self
