"""Immutable strategy signal contracts shared across runtime boundaries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Self

from pydantic import (
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in sorted(value.items())}
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_thaw_json(item) for item in value]
    return value


class SignalAction(StrEnum):
    WATCH = "watch"
    B_INTENT = "b_intent"
    REDUCE = "reduce"
    S_INTENT = "s_intent"
    CANCEL = "cancel"


class SignalEnvelope(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    signal_id: Sha256 | None = None
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    parameter_fingerprint: Sha256
    dataset_snapshot_id: Sha256
    feature_snapshot_id: Sha256
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    candidate_id: str = Field(min_length=1)
    action: SignalAction
    reason_codes: tuple[str, ...] = Field(min_length=1)
    evidence: Mapping[str, JsonValue] = Field(default_factory=dict)
    expires_at: AwareUtcDatetime
    producer_commit: CommitSha

    @field_validator("reason_codes")
    @classmethod
    def validate_reason_codes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("reason_codes cannot contain empty values")
        if len(values) != len(set(values)):
            raise ValueError("reason_codes must be unique")
        return tuple(sorted(values))

    @field_validator("evidence", mode="before")
    @classmethod
    def thaw_evidence_for_revalidation(cls, value: object) -> object:
        return _thaw_json(value)

    @field_validator("evidence")
    @classmethod
    def freeze_evidence(cls, value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        frozen = _freeze_json(value)
        if not isinstance(frozen, Mapping):
            raise TypeError("evidence must be a mapping")
        canonical_sha256(frozen)
        return frozen

    @field_serializer("evidence")
    def serialize_evidence(self, value: Mapping[str, JsonValue]) -> dict[str, object]:
        thawed = _thaw_json(value)
        if not isinstance(thawed, dict):
            raise TypeError("evidence must serialize as a mapping")
        return thawed

    def _identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "parameter_fingerprint": self.parameter_fingerprint,
            "dataset_snapshot_id": self.dataset_snapshot_id,
            "feature_snapshot_id": self.feature_snapshot_id,
            "event_time": self.event_time,
            "available_at": self.available_at,
            "candidate_id": self.candidate_id,
            "action": self.action,
            "reason_codes": self.reason_codes,
            "evidence": self.evidence,
            "expires_at": self.expires_at,
            "producer_commit": self.producer_commit,
        }

    @model_validator(mode="after")
    def validate_signal(self) -> Self:
        if self.event_time > self.available_at:
            raise ValueError("event_time must be at or before available_at")
        if self.available_at >= self.expires_at:
            raise ValueError("available_at must be before expires_at")

        expected_signal_id = canonical_sha256(self._identity_payload())
        if self.signal_id is None:
            object.__setattr__(self, "signal_id", expected_signal_id)
        elif self.signal_id != expected_signal_id:
            raise ValueError("signal_id does not match the deterministic signal identity")
        return self
