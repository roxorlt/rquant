"""Immutable contracts for atomically published serving generations."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Self

from pydantic import Field, StringConstraints, field_serializer, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]


class FreshnessStatus(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class ServingDatasetWatermark(RuntimeContractModel):
    dataset_id: str = Field(min_length=1)
    generation_id: str = Field(min_length=1)
    event_time: AwareUtcDatetime
    published_at: AwareUtcDatetime
    sequence: int = Field(ge=0)
    status: FreshnessStatus
    reason: str | None = None

    @model_validator(mode="after")
    def validate_watermark(self) -> Self:
        if self.published_at < self.event_time:
            raise ValueError("published_at cannot precede event_time")
        if self.status is FreshnessStatus.FRESH and self.reason is not None:
            raise ValueError("fresh watermark cannot have reason")
        if self.status is not FreshnessStatus.FRESH and not self.reason:
            raise ValueError("non-fresh watermark requires reason")
        return self


def _validate_sha256(value: str, *, field_name: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


class ServingGenerationManifest(RuntimeContractModel):
    generation_id: str = ""
    schema_version: int = Field(ge=1)
    source_generations: Mapping[str, str] = Field(min_length=1)
    watermarks: tuple[ServingDatasetWatermark, ...]
    content_sha256: str
    row_counts: Mapping[str, int] = Field(min_length=1)
    built_at: AwareUtcDatetime
    producer_commit: CommitSha

    @field_validator("content_sha256")
    @classmethod
    def validate_content_sha256(cls, value: str) -> str:
        return _validate_sha256(value, field_name="content_sha256")

    @field_validator("source_generations")
    @classmethod
    def validate_source_generations(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if any(not dataset_id or not generation_id for dataset_id, generation_id in value.items()):
            raise ValueError("source_generations keys and values cannot be empty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_generations")
    def serialize_source_generations(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @field_validator("watermarks")
    @classmethod
    def canonicalize_watermarks(
        cls,
        value: tuple[ServingDatasetWatermark, ...],
    ) -> tuple[ServingDatasetWatermark, ...]:
        dataset_ids = tuple(item.dataset_id for item in value)
        if len(dataset_ids) != len(set(dataset_ids)):
            raise ValueError("watermarks must be unique by dataset_id")
        return tuple(sorted(value, key=lambda item: item.dataset_id))

    @field_validator("row_counts")
    @classmethod
    def validate_row_counts(cls, value: Mapping[str, int]) -> Mapping[str, int]:
        if any(not dataset_id for dataset_id in value):
            raise ValueError("row_counts dataset ids cannot be empty")
        if any(row_count < 0 for row_count in value.values()):
            raise ValueError("row_counts values must be nonnegative")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("row_counts")
    def serialize_row_counts(self, value: Mapping[str, int]) -> dict[str, int]:
        return dict(value)

    def identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_generations": self.source_generations,
            "watermarks": self.watermarks,
            "content_sha256": self.content_sha256,
            "row_counts": self.row_counts,
            "built_at": self.built_at,
            "producer_commit": self.producer_commit,
        }

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        watermark_ids = {item.dataset_id for item in self.watermarks}
        source_ids = set(self.source_generations)
        if watermark_ids != source_ids:
            raise ValueError("each source generation must have exactly one watermark")
        for watermark in self.watermarks:
            expected_generation = self.source_generations[watermark.dataset_id]
            if watermark.generation_id != expected_generation:
                raise ValueError(
                    f"watermark {watermark.dataset_id} generation does not match source_generations"
                )
            if self.built_at < watermark.published_at:
                raise ValueError("built_at cannot precede watermark published_at")

        expected_id = canonical_sha256(self.identity_payload())
        if self.generation_id and self.generation_id != expected_id:
            raise ValueError("generation_id does not match canonical manifest content")
        object.__setattr__(self, "generation_id", expected_id)
        return self


class ServingCurrentPointer(RuntimeContractModel):
    generation_id: str = Field(min_length=1)
    manifest_sha256: str
    published_at: AwareUtcDatetime
    previous_generation_id: str | None = None

    @field_validator("manifest_sha256")
    @classmethod
    def validate_manifest_sha256(cls, value: str) -> str:
        return _validate_sha256(value, field_name="manifest_sha256")

    @model_validator(mode="after")
    def validate_pointer(self) -> Self:
        if self.previous_generation_id == self.generation_id:
            raise ValueError("previous_generation_id must differ from generation_id")
        return self
