"""Typed publication service for one built-in strategy candidate authority."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated, Literal, TypeAlias

from pydantic import Field, StringConstraints

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strategy_candidate_producers import (
    AuctionMatchFact,
    GrowthBoardFact,
    NShapePoolFact,
    PublishedCandidateInputAuthority,
    produce_auction_gap_candidates,
    produce_growth_board_surge_candidates,
    produce_n_shape_candidates,
)
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshotSpool

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class NShapeCandidateBatch(RuntimeContractModel):
    authority: PublishedCandidateInputAuthority
    facts: tuple[NShapePoolFact, ...]


class GrowthBoardCandidateBatch(RuntimeContractModel):
    authority: PublishedCandidateInputAuthority
    facts: tuple[GrowthBoardFact, ...]


class AuctionGapCandidateBatch(RuntimeContractModel):
    authority: PublishedCandidateInputAuthority
    facts: tuple[AuctionMatchFact, ...]


CandidatePublishBatch: TypeAlias = (
    NShapeCandidateBatch | GrowthBoardCandidateBatch | AuctionGapCandidateBatch
)


class CandidatePublishSummary(RuntimeContractModel):
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"]
    strategy_version: Literal["1"]
    trade_date: date
    captured_at: AwareUtcDatetime
    authority_snapshot_id: Sha256
    candidate_count: int = Field(ge=0)
    snapshot_sequence: int = Field(ge=0)
    snapshot_content_sha256: Sha256
    published: bool


def _require_expected_commit(
    authority: PublishedCandidateInputAuthority,
    expected_commit: str,
) -> None:
    if authority.producer_commit != expected_commit:
        raise ValueError("candidate authority commit does not match running code")


def publish_candidate_batch(
    *,
    snapshot_root: Path,
    expected_commit: str,
    batch: CandidatePublishBatch,
    definition_fingerprint: str,
    executable_fingerprint: str,
    candidate_schema_fingerprint: str,
    static_feature_schema: Mapping[str, object],
) -> CandidatePublishSummary:
    if _COMMIT_PATTERN.fullmatch(expected_commit) is None:
        raise ValueError("expected commit must be a full lowercase Git SHA")
    if isinstance(batch, NShapeCandidateBatch):
        validated = NShapeCandidateBatch.model_validate(batch)
        _require_expected_commit(validated.authority, expected_commit)
        strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"] = "n_shape"
        rows = produce_n_shape_candidates(
            authority=validated.authority,
            facts=validated.facts,
        )
    elif isinstance(batch, GrowthBoardCandidateBatch):
        validated = GrowthBoardCandidateBatch.model_validate(batch)
        _require_expected_commit(validated.authority, expected_commit)
        strategy_id = "growth_board_surge"
        rows = produce_growth_board_surge_candidates(
            authority=validated.authority,
            facts=validated.facts,
        )
    elif isinstance(batch, AuctionGapCandidateBatch):
        validated = AuctionGapCandidateBatch.model_validate(batch)
        _require_expected_commit(validated.authority, expected_commit)
        strategy_id = "auction_gap"
        rows = produce_auction_gap_candidates(
            authority=validated.authority,
            facts=validated.facts,
        )
    else:
        raise TypeError("batch must be a typed candidate publish batch")

    authority = validated.authority
    result = StrategyCandidateSnapshotSpool(snapshot_root).publish_strategy_records(
        strategy_id=strategy_id,
        strategy_version="1",
        source_snapshot_ids={"candidate_input": authority.authority_snapshot_id},
        trade_date=authority.trade_date,
        captured_at=authority.captured_at,
        producer_commit=expected_commit,
        rows=rows,
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=executable_fingerprint,
        candidate_schema_fingerprint=candidate_schema_fingerprint,
        static_feature_schema=static_feature_schema,
    )
    return CandidatePublishSummary(
        strategy_id=strategy_id,
        strategy_version="1",
        trade_date=result.snapshot.trade_date,
        captured_at=result.snapshot.captured_at,
        authority_snapshot_id=authority.authority_snapshot_id,
        candidate_count=len(result.snapshot.rows),
        snapshot_sequence=result.snapshot.sequence,
        snapshot_content_sha256=result.snapshot.content_sha256,
        published=result.published,
    )


__all__ = (
    "AuctionGapCandidateBatch",
    "CandidatePublishBatch",
    "CandidatePublishSummary",
    "GrowthBoardCandidateBatch",
    "NShapeCandidateBatch",
    "publish_candidate_batch",
)
