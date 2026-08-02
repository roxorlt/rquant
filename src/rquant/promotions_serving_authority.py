"""Point-in-time promotion governance source for serving publication."""

from __future__ import annotations

from datetime import UTC, datetime

from rquant.experiment_registry import ExperimentRegistryReadonlyReader
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

_EMPTY_EVENT_TIME = datetime(1970, 1, 1, tzinfo=UTC)


class PromotionsSourceReader:
    """Convert the immutable promotion ledger into a bounded serving source result."""

    def __init__(
        self,
        *,
        registry: ExperimentRegistryReadonlyReader,
        limit: int = 1_000,
    ) -> None:
        if not isinstance(registry, ExperimentRegistryReadonlyReader):
            raise TypeError("registry must be ExperimentRegistryReadonlyReader")
        if limit < 1:
            raise ValueError("limit must be positive")
        self.registry = registry
        self.limit = limit

    def __call__(self, observed_at: datetime, /) -> SourceReadResult:
        observed = normalize_aware_utc(observed_at)
        snapshot = self.registry.read_promotion_decisions(
            observed_at=observed,
            limit=self.limit,
        )
        source_time = snapshot.event_time or _EMPTY_EVENT_TIME
        values: dict[str, object] = {
            "dataset_id": PROMOTIONS_DATASET_ID,
            "sequence": snapshot.sequence,
            "event_time": source_time,
            "published_at": source_time,
            "status": FreshnessStatus.FRESH,
            "reason": None,
            "payload": PromotionsPayload(promotions=snapshot.decisions),
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
