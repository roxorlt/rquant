"""Immutable strategy signal contracts shared across runtime boundaries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    BeforeValidator,
    Field,
    JsonValue,
    StringConstraints,
    WithJsonSchema,
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
    WithJsonSchema(
        {"type": "string", "pattern": r"^[0-9a-f]{40}$"},
        mode="validation",
    ),
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


def _detached_snapshot(value: object) -> object:
    if isinstance(value, Mapping):
        return {deepcopy(key): _detached_snapshot(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_detached_snapshot(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_detached_snapshot(item) for item in value)
    return deepcopy(value)


def _require_current_json_representation(value: object, *, path: str) -> None:
    if value is None or isinstance(value, (str, bool, int, float)):
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            _require_current_json_representation(item, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _require_current_json_representation(item, path=f"{path}[{index}]")
        return
    raise ValueError(f"{path} must use exact JSON value types")


def _require_current_representation(value: object) -> object:
    if not isinstance(value, Mapping):
        return value
    if any(not isinstance(key, str) for key in value):
        raise ValueError("current envelope field names must be strings")

    exact_string_fields = (
        "envelope_schema",
        "strategy_id",
        "strategy_version",
        "parameter_fingerprint",
        "dataset_snapshot_id",
        "feature_snapshot_id",
        "candidate_id",
        "action",
    )
    for field in exact_string_fields:
        if field in value:
            _require_exact_string(value[field])
    if value.get("signal_id") is not None:
        _require_exact_string(value["signal_id"])

    for field in ("event_time", "available_at", "expires_at"):
        if field not in value:
            continue
        timestamp = value[field]
        if isinstance(timestamp, str):
            _require_exact_string(timestamp)
        elif not isinstance(timestamp, datetime):
            raise ValueError(f"{field} must be an exact datetime string or datetime object")

    if "reason_codes" in value:
        reasons = value["reason_codes"]
        if not isinstance(reasons, Sequence) or isinstance(
            reasons,
            (str, bytes, bytearray),
        ):
            raise ValueError("reason_codes must be a sequence of exact strings")
        for reason in reasons:
            _require_exact_string(reason)

    if "evidence" in value:
        evidence = value["evidence"]
        if not isinstance(evidence, Mapping):
            raise ValueError("evidence must be a mapping")
        _require_current_json_representation(evidence, path="evidence")

    identity = value.get("producer_identity")
    if isinstance(identity, Mapping):
        if any(not isinstance(key, str) for key in identity):
            raise ValueError("producer_identity field names must be strings")
        for field in ("kind", "producer_commit", "producer_generation_id"):
            if identity.get(field) is not None:
                _require_exact_string(identity[field])
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


class SignalEnvelope(_SignalEnvelopeBase):
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


# Explicit permanent-reader name; SignalEnvelope retains its persisted runtime identity.
LegacySignalEnvelope = SignalEnvelope


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

    @model_validator(mode="before")
    @classmethod
    def validate_exact_representation(cls, value: object) -> object:
        return _require_current_representation(value)

    def _identity_payload(self) -> dict[str, object]:
        return {
            "envelope_schema": self.envelope_schema,
            **self._common_identity_payload(),
            "producer_identity": self.producer_identity,
        }


SignalEnvelopeFamily = LegacySignalEnvelope | CurrentSignalEnvelope


def current_signal_envelope_json_bytes(envelope: CurrentSignalEnvelope) -> bytes:
    if type(envelope) is not CurrentSignalEnvelope:
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
    snapshot = _detached_snapshot(decoded)
    if not isinstance(snapshot, dict):
        raise TypeError("signal envelope payload must be a mapping or JSON object")
    if "envelope_schema" not in snapshot:
        return LegacySignalEnvelope.model_validate(snapshot)
    if snapshot["envelope_schema"] != CURRENT_ENVELOPE_SCHEMA:
        raise ValueError("unknown signal envelope_schema")
    return CurrentSignalEnvelope.model_validate(snapshot)
