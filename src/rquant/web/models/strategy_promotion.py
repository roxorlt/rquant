"""Manual stages and exact original commands exposed by the private Web API."""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from rquant.page_control import PageControlReceipt
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strategy_promotion_commands import StrategyPromotionCommand
from rquant.strategy_promotion_contracts import (
    PreparedPromotionApproval,
    StrategyPromotionApproval,
    StrategyPromotionCandidateReference,
    StrategyPromotionPaperReference,
    StrategyPromotionReview,
    StrategyPromotionWalkForwardReference,
)
from rquant.strategy_promotion_projection_contract import StrategyPromotionStateFact
from rquant.strategy_promotion_walk_forward import PromotionWalkForwardSubmission


class StrategyPromotionData(RuntimeContractModel):
    availability: Literal["unavailable", "empty", "populated"]
    source_kind: Literal["template", "builtin"]
    strategy_id: str
    available_at: AwareUtcDatetime | None
    states: tuple[StrategyPromotionStateFact, ...] = Field(default=(), max_length=4096)
    reviews: tuple[StrategyPromotionReview, ...] = Field(default=(), max_length=100)
    next_offset: int | None = Field(default=None, ge=1, le=1000)
    candidates: tuple[StrategyPromotionCandidateReference, ...] = Field(default=(), max_length=64)
    walk_forward: tuple[StrategyPromotionWalkForwardReference, ...] = Field(
        default=(), max_length=64
    )
    paper_accounts: tuple[StrategyPromotionPaperReference, ...] = Field(default=(), max_length=64)
    can_evaluate: bool = False
    can_prepare_approval: bool = False
    can_run_walk_forward: bool = False
    reason: str = Field(default="", max_length=256)


class StrategyPromotionCommandData(RuntimeContractModel):
    original_request: StrategyPromotionCommand
    status: Literal[
        "not_registered",
        "pending",
        "uncertain",
        "rejected",
        "completed",
        "completed_waiting_publication",
        "published",
    ]
    receipt: PageControlReceipt | None = None
    review: StrategyPromotionReview | None = None
    preparation: PreparedPromotionApproval | None = None
    approval: StrategyPromotionApproval | None = None
    walk_forward: PromotionWalkForwardSubmission | None = None
    message: str = Field(max_length=256)
