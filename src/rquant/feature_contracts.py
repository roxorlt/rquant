"""Immutable contracts shared by live and replay feature producers."""

from __future__ import annotations

import math
from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType

from pydantic import Field, JsonValue, field_serializer, field_validator, model_validator

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


class SourceAvailableAtBasis(StrEnum):
    MAX_SOURCE_AVAILABLE_AT = "max_source_available_at"
    AUTHORITATIVE_SOURCE_AVAILABLE_AT = "authoritative_source_available_at"
    PER_CANDIDATE_SOURCE_AVAILABLE_AT = "per_candidate_source_available_at"


class MissingFeaturePolicy(StrEnum):
    FAIL_CLOSED = "fail_closed"
    MARK_UNAVAILABLE = "mark_unavailable"


class LateFeaturePolicy(StrEnum):
    FAIL_CLOSED = "fail_closed"
    MARK_STALE = "mark_stale"
    MARK_DEGRADED = "mark_degraded"


class DecisionVisibilityGate(StrEnum):
    AVAILABLE_AT_LTE_DECISION_TIME = "available_at_lte_decision_time"


class FeatureAvailabilityContract(RuntimeContractModel):
    source_available_at_basis: SourceAvailableAtBasis
    max_delay_seconds: int = Field(ge=0)
    missing_policy: MissingFeaturePolicy
    late_policy: LateFeaturePolicy
    decision_visibility_gate: DecisionVisibilityGate


class FeatureDefinition(RuntimeContractModel):
    name: str = Field(min_length=1)
    dtype: str = Field(min_length=1)
    source_datasets: tuple[str, ...] = Field(min_length=1)
    lookback: int = Field(ge=0)
    pit_rule: str = Field(min_length=1)
    price_basis: str = Field(min_length=1)
    availability_contract: FeatureAvailabilityContract

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
    candidate_id: str | None = Field(default=None, min_length=1)
    name: str = Field(min_length=1)
    status: FeatureAvailability
    source_event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    decision_cutoff: AwareUtcDatetime
    actual_delay_seconds: float = Field(ge=0, allow_inf_nan=False)
    reason: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_reason(self) -> FeatureFieldStatus:
        if self.status is FeatureAvailability.AVAILABLE and self.reason is not None:
            raise ValueError("available feature status forbids a reason")
        if self.status is not FeatureAvailability.AVAILABLE and self.reason is None:
            raise ValueError(f"{self.status.value} feature status requires a reason")
        if self.available_at < self.source_event_time:
            raise ValueError("available_at cannot precede source_event_time")
        if self.available_at > self.decision_cutoff:
            raise ValueError("available_at cannot exceed decision_cutoff")
        measured_delay = (self.available_at - self.source_event_time).total_seconds()
        if not math.isclose(
            self.actual_delay_seconds,
            measured_delay,
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            raise ValueError("actual_delay_seconds does not match source availability delay")
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
    decision_cutoff: AwareUtcDatetime
    actual_delay_seconds: float = Field(ge=0, allow_inf_nan=False)
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
        keys = tuple((item.candidate_id, item.name) for item in values)
        if len(keys) != len(set(keys)):
            raise ValueError("field status candidate/name pairs must be unique")
        scopes_by_name: dict[str, set[bool]] = {}
        for item in values:
            scopes_by_name.setdefault(item.name, set()).add(item.candidate_id is not None)
        if any(len(scopes) != 1 for scopes in scopes_by_name.values()):
            raise ValueError("field statuses cannot mix scoped and unscoped evidence")
        return values

    @model_validator(mode="after")
    def validate_pit_time(self) -> FeatureBatchEnvelope:
        if self.available_at < self.event_time:
            raise ValueError("available_at cannot be earlier than event_time")
        if self.available_at > self.decision_cutoff:
            raise ValueError("available_at cannot exceed decision_cutoff")
        measured_delay = (self.available_at - self.event_time).total_seconds()
        if not math.isclose(
            self.actual_delay_seconds,
            measured_delay,
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            raise ValueError("actual_delay_seconds does not match batch availability delay")
        if any(item.available_at > self.available_at for item in self.field_statuses):
            raise ValueError("field status available_at cannot exceed batch available_at")
        if any(item.decision_cutoff != self.decision_cutoff for item in self.field_statuses):
            raise ValueError("field status decision_cutoff must match batch decision_cutoff")
        return self

    @property
    def input_fingerprint(self) -> str:
        return canonical_sha256(tuple(sorted(self.input_batch_ids)))

    def field_status(
        self,
        name: str,
        *,
        candidate_id: str | None = None,
    ) -> FeatureFieldStatus | None:
        matches = tuple(item for item in self.field_statuses if item.name == name)
        if candidate_id is not None:
            scoped = next(
                (item for item in matches if item.candidate_id == candidate_id),
                None,
            )
            if scoped is not None:
                return scoped
            if self.row_count == 1 and len(matches) == 1 and matches[0].candidate_id is None:
                return matches[0]
            return None
        if len(matches) == 1 and (matches[0].candidate_id is None or self.row_count == 1):
            return matches[0]
        return None


class FeatureInstanceEnvelope(RuntimeContractModel):
    source_id: str = Field(min_length=1)
    decision_cutoff: AwareUtcDatetime
    values: Mapping[str, JsonValue]
    field_statuses: tuple[FeatureFieldStatus, ...] = Field(min_length=1)

    @field_validator("values")
    @classmethod
    def freeze_values(cls, values: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        if any(not isinstance(name, str) or not name for name in values):
            raise ValueError("feature instance names must be nonempty strings")
        return MappingProxyType(dict(sorted(values.items())))

    @field_serializer("values")
    def serialize_values(self, values: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        return dict(values)

    @model_validator(mode="after")
    def validate_evidence(self) -> FeatureInstanceEnvelope:
        names = tuple(status.name for status in self.field_statuses)
        if len(names) != len(set(names)) or set(names) != set(self.values):
            raise ValueError("feature instance statuses must exactly cover values")
        if any(status.decision_cutoff != self.decision_cutoff for status in self.field_statuses):
            raise ValueError("feature instance decision cutoffs must match")
        return self

    def field_status(self, name: str) -> FeatureFieldStatus | None:
        return next((item for item in self.field_statuses if item.name == name), None)

    @property
    def instance_fingerprint(self) -> str:
        return canonical_sha256(self)
