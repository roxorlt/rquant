"""Exact original UUID bodies for the four manual promotion operations."""

from __future__ import annotations

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, TypeAdapter, field_validator, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_promotion_contracts import (
    PreparedPromotionApproval,
    PromotionEvidenceSelection,
    Sha256,
    StrategyPromotionTarget,
)


class _PromotionCommand(RuntimeContractModel):
    command_id: str
    requested_at: AwareUtcDatetime
    generation_id: str = Field(min_length=1, max_length=128)
    target: StrategyPromotionTarget

    @field_validator("command_id")
    @classmethod
    def original_uuid(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("command ID must be canonical UUID")
        return value

    @model_validator(mode="after")
    def bounded_original_body(self) -> Self:
        if (
            len(
                self.model_dump_json(
                    exclude={"owner_id", "metadata_identity", "accepted_at"}
                ).encode()
            )
            > 32 * 1024
        ):
            raise ValueError("promotion command exceeds original 32 KiB budget")
        return self

    @property
    def request_hash(self) -> str:
        return canonical_sha256(self)


class RequestPromotionReview(_PromotionCommand):
    kind: Literal["request_promotion_review"] = "request_promotion_review"
    expected_revision: int = Field(strict=True, ge=0, le=2)
    selection: PromotionEvidenceSelection


class PreparePromotionApproval(_PromotionCommand):
    kind: Literal["prepare_promotion_approval"] = "prepare_promotion_approval"
    review_id: Sha256


class ApprovePromotion(_PromotionCommand):
    kind: Literal["approve_promotion"] = "approve_promotion"
    preparation: PreparedPromotionApproval
    entered_name: str = Field(min_length=1, max_length=80)

    @model_validator(mode="after")
    def binds_original_preparation(self) -> Self:
        if self.target != self.preparation.review.target:
            raise ValueError("approval differs from prepared target")
        return self


class RunStrategyWalkForward(_PromotionCommand):
    kind: Literal["run_strategy_walk_forward"] = "run_strategy_walk_forward"
    selection: PromotionEvidenceSelection
    fold_count: int = Field(strict=True, ge=1, le=6)


StrategyPromotionCommand = Annotated[
    RequestPromotionReview | PreparePromotionApproval | ApprovePromotion | RunStrategyWalkForward,
    Field(discriminator="kind"),
]

PROMOTION_PUBLIC_TYPES = (
    RequestPromotionReview,
    PreparePromotionApproval,
    ApprovePromotion,
    RunStrategyWalkForward,
)
_PROMOTION_PUBLIC_ADAPTER = TypeAdapter(StrategyPromotionCommand)


class StrategyPromotionRateLimitError(RuntimeError):
    """The original SQLite admission budget rejected a new UUID before enqueue."""


class _OwnedPromotion(RuntimeContractModel):
    owner_id: str
    metadata_identity: StrategyAuthoringIdentity
    accepted_at: AwareUtcDatetime

    @model_validator(mode="after")
    def exact_private_owner(self) -> Self:
        if self.owner_id != self.target.owner_id:
            raise ValueError("original promotion actor differs from target owner")
        return self

    def original(self) -> StrategyPromotionCommand:
        return _PROMOTION_PUBLIC_ADAPTER.validate_python(
            self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted_at"})
        )


class OwnedRequestPromotionReview(_OwnedPromotion, RequestPromotionReview):
    pass


class OwnedPreparePromotionApproval(_OwnedPromotion, PreparePromotionApproval):
    pass


class OwnedApprovePromotion(_OwnedPromotion, ApprovePromotion):
    pass


class OwnedRunStrategyWalkForward(_OwnedPromotion, RunStrategyWalkForward):
    pass


PROMOTION_OWNED_TYPES = (
    OwnedRequestPromotionReview,
    OwnedPreparePromotionApproval,
    OwnedApprovePromotion,
    OwnedRunStrategyWalkForward,
)
OwnedStrategyPromotionCommand = Annotated[
    OwnedRequestPromotionReview
    | OwnedPreparePromotionApproval
    | OwnedApprovePromotion
    | OwnedRunStrategyWalkForward,
    Field(discriminator="kind"),
]


def own_strategy_promotion(
    request: StrategyPromotionCommand,
    *,
    actor_id: str,
    metadata_identity: StrategyAuthoringIdentity,
    accepted_at: AwareUtcDatetime,
) -> OwnedStrategyPromotionCommand:
    if type(request) not in PROMOTION_PUBLIC_TYPES:
        raise TypeError("original ownerless promotion command required")
    model = PROMOTION_OWNED_TYPES[PROMOTION_PUBLIC_TYPES.index(type(request))]
    return model.model_validate(
        request.model_dump(mode="python")
        | {"owner_id": actor_id, "metadata_identity": metadata_identity, "accepted_at": accepted_at}
    )
