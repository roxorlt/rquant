"""Immutable contracts shared by live and replay feature producers."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)


class FeatureAvailability(StrEnum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    STALE = "stale"


class RequirementLevel(StrEnum):
    REQUIRED = "required"
    OPTIONAL = "optional"


class FeatureDefinition(RuntimeContractModel):
    name: str = Field(min_length=1)
    dtype: str = Field(min_length=1)
    source_datasets: tuple[str, ...] = Field(min_length=1)
    lookback: int = Field(ge=0)
    pit_rule: str = Field(min_length=1)
    price_basis: str = Field(min_length=1)

    @field_validator("source_datasets")
    @classmethod
    def validate_source_datasets(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("source_datasets cannot contain empty values")
        if len(values) != len(set(values)):
            raise ValueError("source_datasets must be unique")
        return values


class FeatureRequirement(RuntimeContractModel):
    name: str = Field(min_length=1)
    level: RequirementLevel
    min_contract_version: int = Field(ge=1)
    allow_degraded: bool = False


class FeatureFieldStatus(RuntimeContractModel):
    name: str = Field(min_length=1)
    status: FeatureAvailability
    available_at: AwareUtcDatetime
    reason: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_reason(self) -> FeatureFieldStatus:
        if self.status is FeatureAvailability.AVAILABLE and self.reason is not None:
            raise ValueError("available feature status forbids a reason")
        if self.status is not FeatureAvailability.AVAILABLE and self.reason is None:
            raise ValueError(f"{self.status.value} feature status requires a reason")
        return self


def _feature_payload(feature: FeatureDefinition) -> dict[str, object]:
    payload = feature.model_dump(mode="python")
    payload["source_datasets"] = tuple(sorted(feature.source_datasets))
    return payload


class FeatureContract(RuntimeContractModel):
    contract_id: str = Field(min_length=1)
    version: int = Field(ge=1)
    features: tuple[FeatureDefinition, ...] = Field(min_length=1)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    @field_validator("features")
    @classmethod
    def validate_unique_features(
        cls,
        values: tuple[FeatureDefinition, ...],
    ) -> tuple[FeatureDefinition, ...]:
        names = tuple(item.name for item in values)
        if len(names) != len(set(names)):
            raise ValueError("feature names must be unique")
        return values

    @property
    def contract_fingerprint(self) -> str:
        payload = {
            "contract_id": self.contract_id,
            "version": self.version,
            "features": tuple(
                _feature_payload(feature)
                for feature in sorted(self.features, key=lambda item: item.name)
            ),
            "producer_commit": self.producer_commit,
        }
        return canonical_sha256(payload)


class FeatureBatchEnvelope(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    batch_id: str = Field(min_length=1)
    contract_id: str = Field(min_length=1)
    contract_version: int = Field(ge=1)
    input_batch_ids: tuple[str, ...] = Field(min_length=1)
    sequence: int = Field(ge=0)
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    row_count: int = Field(ge=0)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    field_statuses: tuple[FeatureFieldStatus, ...]
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    @field_validator("input_batch_ids")
    @classmethod
    def validate_input_batch_ids(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("input_batch_ids cannot contain empty values")
        if len(values) != len(set(values)):
            raise ValueError("input_batch_ids must be unique")
        return values

    @field_validator("field_statuses")
    @classmethod
    def validate_field_statuses(
        cls,
        values: tuple[FeatureFieldStatus, ...],
    ) -> tuple[FeatureFieldStatus, ...]:
        names = tuple(item.name for item in values)
        if len(names) != len(set(names)):
            raise ValueError("field status names must be unique")
        return values

    @model_validator(mode="after")
    def validate_pit_time(self) -> FeatureBatchEnvelope:
        if self.available_at < self.event_time:
            raise ValueError("available_at cannot be earlier than event_time")
        if any(item.available_at > self.available_at for item in self.field_statuses):
            raise ValueError("field status available_at cannot exceed batch available_at")
        return self

    @property
    def input_fingerprint(self) -> str:
        return canonical_sha256(tuple(sorted(self.input_batch_ids)))

    def field_status(self, name: str) -> FeatureFieldStatus | None:
        return next((item for item in self.field_statuses if item.name == name), None)
