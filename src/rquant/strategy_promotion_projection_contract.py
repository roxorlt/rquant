"""Bounded private layouts consumed by the original promotions publication."""

from __future__ import annotations

from types import MappingProxyType
from typing import Self

from pydantic import Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_promotion_contracts import (
    Owner,
    Sha256,
    StrategyPromotionReview,
    StrategyPromotionState,
)

PRIVATE_TABLES = ("strategy_manual_state", "strategy_manual_review", "strategy_manual_window")
MAX_STATES = 4096
MAX_STATE_BYTES = 2 * 1024 * 1024
MAX_PUBLISHED_REVIEWS = 1000
MAX_REVIEW_BYTES = 1024 * 1024
MAX_PRIVATE_BYTES = 3 * 1024 * 1024
MAX_CELL_BYTES = 64 * 1024
STRATEGY_PROMOTION_PROJECTION_LAYOUTS = MappingProxyType(
    {
        "strategy_manual_state": (
            (
                ("owner_id", "string"),
                ("target_key", "string"),
                ("strategy_id", "string"),
                ("version", "int"),
                ("state_json", "string"),
                ("applied_at", "timestamp"),
            ),
            ("owner_id", "target_key"),
            MAX_STATES,
            MAX_STATE_BYTES,
            ("applied_at",),
        ),
        "strategy_manual_review": (
            (
                ("owner_id", "string"),
                ("review_id", "string"),
                ("target_key", "string"),
                ("review_json", "string"),
                ("observed_at", "timestamp"),
            ),
            ("owner_id", "review_id"),
            MAX_PUBLISHED_REVIEWS,
            MAX_REVIEW_BYTES,
            ("observed_at",),
        ),
        "strategy_manual_window": (
            (
                ("owner_id", "string"),
                ("state_count", "int"),
                ("review_count", "int"),
                ("metadata_json", "string"),
            ),
            ("owner_id",),
            4096,
            64 * 1024,
            (),
        ),
    }
)


class StrategyPromotionStateFact(RuntimeContractModel):
    owner_id: Owner
    target_key: Sha256
    state: StrategyPromotionState
    applied_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def original_owner(self) -> Self:
        if (self.owner_id, self.target_key) != (
            self.state.target.owner_id,
            self.state.target.version_key,
        ):
            raise ValueError("private manual state differs from its exact owner/version")
        if (self.applied_at is None) != (self.state.revision == 0):
            raise ValueError("manual state lacks its actual approval time")
        return self


class StrategyPromotionReviewFact(RuntimeContractModel):
    owner_id: Owner
    review_id: Sha256
    review: StrategyPromotionReview

    @model_validator(mode="after")
    def original_actor(self) -> Self:
        if (self.owner_id, self.review_id) != (self.review.actor_id, self.review.review_id):
            raise ValueError("private review differs from its original actor/index")
        return self


class StrategyPromotionPrivateSnapshot(RuntimeContractModel):
    metadata_identity: StrategyAuthoringIdentity
    observed_at: AwareUtcDatetime
    states: tuple[StrategyPromotionStateFact, ...] = Field(max_length=MAX_STATES)
    reviews: tuple[StrategyPromotionReviewFact, ...] = Field(max_length=MAX_PUBLISHED_REVIEWS)

    @model_validator(mode="after")
    def bounds_and_visibility(self) -> Self:
        if len({state.target_key for state in self.states}) != len(self.states) or len(
            {review.review_id for review in self.reviews}
        ) != len(self.reviews):
            raise ValueError("manual private snapshot has repeated exact facts")
        if any(
            f.applied_at is not None and f.applied_at > self.observed_at for f in self.states
        ) or any(f.review.observed_at > self.observed_at for f in self.reviews):
            raise ValueError("manual private snapshot contains future facts")
        if (
            sum(len(f.model_dump_json().encode()) for f in self.states) > MAX_STATE_BYTES
            or sum(len(f.model_dump_json().encode()) for f in self.reviews) > MAX_REVIEW_BYTES
        ):
            raise ValueError("manual private layout exceeds its byte capacity")
        if len(self.model_dump_json().encode()) > MAX_PRIVATE_BYTES:
            raise ValueError("manual private snapshot exceeds original 3 MiB allocation")
        return self
