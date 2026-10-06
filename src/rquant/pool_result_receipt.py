"""Durable identity for one successfully materialized pool result."""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Sequence
from datetime import date
import json
import math
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


class DailyScreenAuthority(RuntimeContractModel):
    contract: Literal["daily-screen-authority/v1"] = "daily-screen-authority/v1"
    trade_date: date
    canonical_receipt_id: Sha256
    canonical_generation_id: Sha256
    source_generation_id: Sha256
    available_at: AwareUtcDatetime


class DailyInputEvidence(RuntimeContractModel):
    method: Literal["daily-screen-inputs/v1"] = "daily-screen-inputs/v1"
    trade_date: date
    decision_at: AwareUtcDatetime
    source_kind: Literal["daily_writer"] = "daily_writer"
    source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    required_columns: tuple[str, ...]
    universe_count: int = Field(ge=0, le=8_000)
    unknown_count: int = Field(ge=0, le=8_000)
    ranking_unknown_count: int = Field(default=0, ge=0, le=8_000)
    scope_missing_count: int = Field(default=0, ge=0, le=8_000)
    parent_scope_result_version: Sha256 | None = None
    unknown_steps: tuple[int, ...]
    rsi_source_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    fundamental_versions: tuple[str, ...] = ()
    writer_contract_fingerprint: Sha256
    canonical_authority: DailyScreenAuthority | None = None

    @model_validator(mode="after")
    def bind_authority_date(self) -> Self:
        if self.canonical_authority is not None and self.canonical_authority.trade_date != self.trade_date:
            raise ValueError("daily screen authority date changed")
        return self


class DailyWriterCapability(RuntimeContractModel):
    contract: Literal["daily-screen-writer/v1"] = "daily-screen-writer/v1"
    serving_generation_id: Sha256
    writer_contract_fingerprint: Sha256
    verified_result_version: Sha256
    verified_evidence_version: Sha256
    completed_at: AwareUtcDatetime
    canonical_receipt_id: Sha256
    canonical_generation_id: Sha256
    source_generation_id: Sha256


class PublishedDailyScreenEvidence(RuntimeContractModel):
    trade_date: date
    preset_name: str
    definition_version: Sha256
    result_version: Sha256
    source_kind: Literal["daily_writer"]
    source_identity: Sha256
    content_digest: Sha256
    decision_at: AwareUtcDatetime
    universe_count: int = Field(ge=0, le=8_000)
    hit_count: int = Field(ge=0, le=8_000)
    unknown_count: int = Field(ge=0, le=8_000)
    ranking_plan_digest: Sha256 | None
    member_rank_digest: Sha256
    persisted_extra_digest: Sha256
    writer_contract_fingerprint: Sha256
    evidence_version: Sha256
    completed_at: AwareUtcDatetime
    canonical_receipt_id: Sha256 | None = None
    canonical_generation_id: Sha256 | None = None
    source_generation_id: Sha256 | None = None

    @model_validator(mode="after")
    def require_complete_authority(self) -> Self:
        values = (self.canonical_receipt_id, self.canonical_generation_id, self.source_generation_id)
        if any(value is not None for value in values) and any(value is None for value in values):
            raise ValueError("published daily authority is incomplete")
        return self


def persisted_result_digests(
    rows: Sequence[tuple[str, str | None]], *, ranked: bool,
) -> tuple[str, str]:
    extras: list[tuple[str, object]] = []
    ranks: list[tuple[str, float | None, int | None]] = []
    codes = [code for code, _ in rows]
    member_set_digest(codes)
    for code, raw in sorted(rows):
        value = json.loads(raw) if raw is not None else None
        if value is not None and not isinstance(value, dict):
            raise ValueError("persisted result extra must be an object")
        extras.append((code, value))
        score = value.get("ranking_score") if value else None
        rank = value.get("rank_position") if value else None
        if ranked:
            if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score):
                raise ValueError("persisted ranking score is unavailable")
            if type(rank) is not int or not 1 <= rank <= len(rows):
                raise ValueError("persisted rank position is unavailable")
        elif score is not None or rank is not None:
            raise ValueError("unranked result contains ranking facts")
        ranks.append((code, float(score) if score is not None else None, rank))
    if ranked and {rank for _, _, rank in ranks} != set(range(1, len(rows) + 1)):
        raise ValueError("persisted ranking positions are ambiguous")
    return canonical_sha256({"contract": "screen-extra/v1", "rows": extras}), canonical_sha256({"contract": "screen-rank/v1", "rows": ranks})


class ScreenRunEvidenceDraft(RuntimeContractModel):
    input: DailyInputEvidence
    definition_version: Sha256
    ranking_plan_digest: Sha256 | None = None


class ScreenRunEvidence(ScreenRunEvidenceDraft):
    contract: Literal["screen-run-evidence/v1"] = "screen-run-evidence/v1"
    preset_name: str = Field(min_length=1)
    result_version: Sha256
    hit_count: int = Field(ge=0, le=8_000)
    persisted_extra_digest: Sha256
    member_rank_digest: Sha256
    completed_at: AwareUtcDatetime
    evidence_version: Sha256 | None = None

    @model_validator(mode="after")
    def bind_evidence(self) -> Self:
        if self.input.unknown_count > self.input.universe_count or self.hit_count > self.input.universe_count:
            raise ValueError("screen evidence counts differ from its inputs")
        if self.input.decision_at > self.completed_at:
            raise ValueError("screen evidence decision is later than completion")
        if self.input.canonical_authority is not None and self.input.canonical_authority.available_at > self.completed_at:
            raise ValueError("daily screen authority is later than completion")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"evidence_version"}))
        if self.evidence_version is None:
            object.__setattr__(self, "evidence_version", expected)
        elif self.evidence_version != expected:
            raise ValueError("screen evidence version differs from persisted content")
        return self
