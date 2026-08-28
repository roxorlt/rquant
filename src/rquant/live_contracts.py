"""Immutable contracts shared by live market-data producers and consumers."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import Field, StringConstraints, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]


class LiveChannel(StrEnum):
    AUCTION_MATCH = "auction_match"
    WATCHLIST_QUOTE = "watchlist_quote"
    MARKET_MINUTE = "market_minute"
    DAILY_CLOSE = "daily_close"
    REFERENCE_SLOW = "reference_slow"


class BatchQualityStatus(StrEnum):
    CANDIDATE = "candidate"
    PUBLISHED = "published"
    DEGRADED = "degraded"
    QUARANTINED = "quarantined"
    STALE = "stale"


class BatchEnvelope(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    channel: LiveChannel
    dataset_id: NonEmptyStr
    source: NonEmptyStr
    source_request_id: NonEmptyStr
    batch_id: NonEmptyStr
    sequence: int = Field(ge=0)
    revision: int = Field(ge=1)
    revises_batch_id: NonEmptyStr | None = None
    event_time_start: AwareUtcDatetime
    event_time_end: AwareUtcDatetime
    source_time: AwareUtcDatetime
    received_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    row_count: int = Field(ge=0)
    content_sha256: Sha256Hex
    quality_status: BatchQualityStatus
    degraded_reasons: tuple[NonEmptyStr, ...] = ()
    producer_version: NonEmptyStr
    producer_commit: CommitSha

    @model_validator(mode="after")
    def validate_consistency(self) -> BatchEnvelope:
        if self.event_time_start > self.event_time_end:
            raise ValueError("event_time_start must be before or equal to event_time_end")
        if self.available_at < self.received_at:
            raise ValueError("available_at must be after or equal to received_at")

        has_revised_batch = self.revises_batch_id is not None
        if self.revision == 1 and has_revised_batch:
            raise ValueError("revision=1 forbids revises_batch_id")
        if self.revision > 1 and not has_revised_batch:
            raise ValueError("revision>1 requires revises_batch_id")

        if len(set(self.degraded_reasons)) != len(self.degraded_reasons):
            raise ValueError("degraded_reasons must be unique")
        requires_reasons = self.quality_status in {
            BatchQualityStatus.DEGRADED,
            BatchQualityStatus.STALE,
        }
        if requires_reasons and not self.degraded_reasons:
            raise ValueError("degraded and stale batches require degraded_reasons")
        if not requires_reasons and self.degraded_reasons:
            raise ValueError("non-degraded batch statuses forbid degraded_reasons")
        return self

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ConsumerCursor(RuntimeContractModel):
    consumer_id: NonEmptyStr
    channel: LiveChannel
    source_generation_id: Sha256Hex
    last_sequence: int = Field(ge=-1)
    last_batch_id: NonEmptyStr | None = None
    last_content_sha256: Sha256Hex | None = None
    updated_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_batch_identity_pair(self) -> ConsumerCursor:
        if (self.last_batch_id is None) != (self.last_content_sha256 is None):
            raise ValueError(
                "last_batch_id and last_content_sha256 must be both set or both absent"
            )
        return self

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class BatchPointer(RuntimeContractModel):
    channel: LiveChannel
    source_generation_id: Sha256Hex
    batch_id: NonEmptyStr
    sequence: int = Field(ge=0)
    revision: int = Field(ge=1)
    content_sha256: Sha256Hex
    quality_status: BatchQualityStatus
    published_at: AwareUtcDatetime

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class CurrentPointer(BatchPointer):
    @model_validator(mode="after")
    def validate_current_quality(self) -> CurrentPointer:
        if self.quality_status is not BatchQualityStatus.PUBLISHED:
            raise ValueError(f"{self.quality_status.value} batch cannot be current")
        return self


class LiveSourceDescriptor(RuntimeContractModel):
    channel: LiveChannel
    generation_id: Sha256Hex
    first_sequence: int = 0
    high_watermark: int = Field(ge=-1)
