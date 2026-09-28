"""Point-in-time promotion governance source for serving publication."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from rquant.experiment_registry import (
    ExperimentServingReadSnapshot,
    PromotionDecisionReadSnapshot,
)
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPointer,
    ServingSourceAuthorityPublisher,
)
from rquant.runtime_serving_snapshot import (
    PROMOTIONS_DATASET_ID,
    PromotionsPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingProjectionPayload

_EMPTY_EVENT_TIME = datetime(1970, 1, 1, tzinfo=UTC)


class PromotionDecisionAuthority(Protocol):
    """The narrow read capability used by serving publication."""

    def read_promotion_decisions(
        self,
        *,
        observed_at: datetime,
        limit: int = 1_000,
    ) -> PromotionDecisionReadSnapshot: ...

    def read_serving_snapshot(
        self,
        *,
        observed_at: datetime,
        decision_limit: int = 1_000,
        attempt_limit: int = 500,
    ) -> ExperimentServingReadSnapshot: ...


class PromotionsSourceReader:
    """Convert the immutable promotion ledger into a bounded serving source result."""

    def __init__(
        self,
        *,
        registry: PromotionDecisionAuthority,
        limit: int = 1_000,
        include_experiments: bool = False,
    ) -> None:
        if not callable(getattr(registry, "read_promotion_decisions", None)):
            raise TypeError("registry must provide promotion decision reads")
        if limit < 1:
            raise ValueError("limit must be positive")
        self.registry = registry
        self.limit = limit
        self.include_experiments = include_experiments

    def __call__(self, observed_at: datetime, /) -> SourceReadResult:
        observed = normalize_aware_utc(observed_at)
        if self.include_experiments:
            combined = self.registry.read_serving_snapshot(
                observed_at=observed,
                decision_limit=self.limit,
            )
            snapshot = combined.promotions
            source_time = combined.event_time or _EMPTY_EVENT_TIME
            rows = tuple(
                {
                    "experiment_id": attempt.spec.experiment_id,
                    "hypothesis_family": attempt.spec.hypothesis_family,
                    "registered_at": attempt.registered_at.isoformat(),
                    "status": attempt.status.value,
                    "completed_at": (
                        attempt.completed_at.isoformat()
                        if attempt.completed_at is not None
                        else None
                    ),
                    "trade_count": (
                        attempt.outcome.trade_count if attempt.outcome is not None else None
                    ),
                    "net_return_pct": (
                        float(attempt.outcome.net_return * 100)
                        if attempt.outcome is not None
                        else None
                    ),
                    "max_drawdown_pct": (
                        float(attempt.outcome.max_drawdown * 100)
                        if attempt.outcome is not None
                        else None
                    ),
                    "win_rate_pct": (
                        float(attempt.outcome.win_rate * 100)
                        if attempt.outcome is not None
                        else None
                    ),
                }
                for attempt in combined.attempts
            )
            projections = (
                ServingProjectionPayload(
                    table_name="experiment_attempt",
                    available_at=source_time,
                    rows=rows,
                ),
                ServingProjectionPayload(
                    table_name="experiment_attempt_window",
                    available_at=source_time,
                    rows=(
                        {
                            "snapshot_key": "current",
                            "retained_count": len(rows),
                            "truncated": combined.truncated,
                            "oldest_registered_at": (
                                combined.oldest_registered_at.isoformat()
                                if combined.oldest_registered_at is not None
                                else None
                            ),
                        },
                    ),
                ),
            )
            sequence = combined.sequence
        else:
            snapshot = self.registry.read_promotion_decisions(
                observed_at=observed,
                limit=self.limit,
            )
            source_time = snapshot.event_time or _EMPTY_EVENT_TIME
            projections = ()
            sequence = snapshot.sequence
        values: dict[str, object] = {
            "dataset_id": PROMOTIONS_DATASET_ID,
            "sequence": sequence,
            "event_time": source_time,
            "published_at": source_time,
            "status": FreshnessStatus.FRESH,
            "reason": None,
            "payload": PromotionsPayload(
                promotions=snapshot.decisions,
                projections=projections,
            ),
        }
        values["generation_id"] = canonical_sha256(values)
        return SourceReadResult.model_validate(values)


class PromotionsAuthorityPublisher:
    """Publish one verified promotion read through its exclusive source authority."""

    def __init__(
        self,
        *,
        reader: PromotionsSourceReader,
        publisher: ServingSourceAuthorityPublisher,
    ) -> None:
        if not isinstance(reader, PromotionsSourceReader):
            raise TypeError("reader must be PromotionsSourceReader")
        if not isinstance(publisher, ServingSourceAuthorityPublisher):
            raise TypeError("publisher must be ServingSourceAuthorityPublisher")
        if publisher.dataset_id != PROMOTIONS_DATASET_ID:
            raise ValueError("publisher must own the promotions dataset")
        if publisher.payload_kind != "promotions":
            raise ValueError("publisher must own the promotions payload kind")
        self.reader = reader
        self.publisher = publisher

    def publish(self, observed_at: datetime) -> ServingSourceAuthorityPointer:
        return self.publisher.publish(self.reader(observed_at))


__all__ = [
    "PromotionsAuthorityPublisher",
    "PromotionsSourceReader",
]
