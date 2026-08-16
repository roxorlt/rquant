"""Immutable strategy signal contracts shared across runtime boundaries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    BeforeValidator,
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
from rquant.strict_json import canonical_json_bytes, strict_json_loads

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

CURRENT_ENVELOPE_SCHEMA = "rquant.signal-envelope/v1"


def _require_exact_string(value: object) -> object:
    if not isinstance(value, str) or value != value.strip():
        raise ValueError("value must be an exact untrimmed string")
    return value


def _require_nonzero_digest(value: str) -> str:
    if not value.strip("0"):
        raise ValueError("digest must not be the all-zero sentinel")
    return value


CommitSha = Annotated[
    str,
    BeforeValidator(_require_exact_string),
    StringConstraints(pattern=r"^[0-9a-f]{40}$"),
]
NonzeroCommitSha = Annotated[
    str,
    BeforeValidator(_require_exact_string),
    StringConstraints(pattern=r"^[0-9a-f]{40}$"),
    AfterValidator(_require_nonzero_digest),
]
NonzeroSha256 = Annotated[
    str,
    BeforeValidator(_require_exact_string),
    StringConstraints(pattern=r"^[0-9a-f]{64}$"),
    AfterValidator(_require_nonzero_digest),
]
GitCommitClaimKind = Literal["git-commit-claim-sha1/v1"]
FullManifestKind = Literal["full-manifest-sha256/v1"]
CurrentEnvelopeSchema = Annotated[
    Literal["rquant.signal-envelope/v1"],
    BeforeValidator(_require_exact_string),
]


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_json(item) for key, item in sorted(value.items())})
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


class LegacySignalReadStatus(StrEnum):
    LEGACY_COMMIT_CLAIM = "legacy_commit_claim"
    LEGACY_ZERO_SENTINEL = "legacy_zero_sentinel"


class _SignalEnvelopeBase(RuntimeContractModel):
    @field_validator("reason_codes", check_fields=False)
    @classmethod
    def validate_reason_codes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("reason_codes cannot contain empty values")
        if len(values) != len(set(values)):
            raise ValueError("reason_codes must be unique")
        return tuple(sorted(values))

    @field_validator("evidence", mode="before", check_fields=False)
    @classmethod
    def thaw_evidence_for_revalidation(cls, value: object) -> object:
        return _thaw_json(value)

    @field_validator("evidence", check_fields=False)
    @classmethod
    def freeze_evidence(cls, value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        frozen = _freeze_json(value)
        if not isinstance(frozen, Mapping):
            raise TypeError("evidence must be a mapping")
        canonical_sha256(frozen)
        return frozen

    @field_serializer("evidence", check_fields=False)
    def serialize_evidence(self, value: Mapping[str, JsonValue]) -> dict[str, object]:
        thawed = _thaw_json(value)
        if not isinstance(thawed, dict):
            raise TypeError("evidence must serialize as a mapping")
        return thawed

    def _common_identity_payload(self) -> dict[str, object]:
        return {
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
        }

    def _identity_payload(self) -> dict[str, object]:
        raise NotImplementedError

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


class LegacySignalEnvelope(_SignalEnvelopeBase):
    schema_version: int = Field(ge=1, strict=True)
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

    @property
    def legacy_read_status(self) -> LegacySignalReadStatus:
        if self.producer_commit == "0" * 40:
            return LegacySignalReadStatus.LEGACY_ZERO_SENTINEL
        return LegacySignalReadStatus.LEGACY_COMMIT_CLAIM

    def _identity_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            **self._common_identity_payload(),
            "producer_commit": self.producer_commit,
        }


class GitCommitClaimIdentity(RuntimeContractModel):
    kind: GitCommitClaimKind
    producer_commit: NonzeroCommitSha


class FullManifestIdentity(RuntimeContractModel):
    kind: FullManifestKind
    producer_generation_id: NonzeroSha256


ProducerIdentity = Annotated[
    GitCommitClaimIdentity | FullManifestIdentity,
    Field(discriminator="kind"),
]


class CurrentSignalEnvelope(_SignalEnvelopeBase):
    envelope_schema: CurrentEnvelopeSchema
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
    producer_identity: ProducerIdentity

    def _identity_payload(self) -> dict[str, object]:
        return {
            "envelope_schema": self.envelope_schema,
            **self._common_identity_payload(),
            "producer_identity": self.producer_identity,
        }


# Deprecated compatibility name for untouched writers during the reader-only rollout.
SignalEnvelope = LegacySignalEnvelope
SignalEnvelopeFamily = LegacySignalEnvelope | CurrentSignalEnvelope


def current_signal_envelope_json_bytes(envelope: CurrentSignalEnvelope) -> bytes:
    if not isinstance(envelope, CurrentSignalEnvelope):
        raise TypeError("current writer requires a CurrentSignalEnvelope object")
    verified = CurrentSignalEnvelope.model_validate(envelope)
    return canonical_json_bytes(verified.model_dump(mode="json"))


def parse_signal_envelope(
    payload: Mapping[str, object] | str | bytes | bytearray,
) -> SignalEnvelopeFamily:
    decoded: object = (
        strict_json_loads(payload) if isinstance(payload, (str, bytes, bytearray)) else payload
    )
    if not isinstance(decoded, Mapping):
        raise TypeError("signal envelope payload must be a mapping or JSON object")
    if "envelope_schema" not in decoded:
        return LegacySignalEnvelope.model_validate(decoded)
    if decoded["envelope_schema"] != CURRENT_ENVELOPE_SCHEMA:
        raise ValueError("unknown signal envelope_schema")
    return CurrentSignalEnvelope.model_validate(decoded)
