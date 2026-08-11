"""Point-in-time immutable strategy candidate snapshots."""

from __future__ import annotations

import fcntl
import json
import math
import os
import re
import stat
from bisect import bisect_right
from collections import OrderedDict
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from threading import RLock
from types import MappingProxyType
from typing import Annotated, Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

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
    normalize_aware_utc,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_GENERATIONS = 4_096
_MAX_AUTHORITY_BYTES = 16 * 1024 * 1024
_GENERATION_CACHE_MAX_ITEMS = 4
_GENERATION_CACHE_MAX_BYTES = 32 * 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_ASIA_SHANGHAI = ZoneInfo("Asia/Shanghai")


class StrategyCandidateSnapshotIntegrityError(RuntimeError):
    """Raised when the immutable candidate snapshot authority is unsafe."""


class StrategyCandidatePriceBasis(StrEnum):
    RAW = "raw"
    QFQ_PIT = "qfq_pit"


def asia_shanghai_trade_date(value: datetime) -> date:
    return normalize_aware_utc(value).astimezone(_ASIA_SHANGHAI).date()


def strategy_candidate_decision_trade_date(
    value: datetime,
    *,
    legacy_utc_date_semantics: bool,
) -> date:
    normalized = normalize_aware_utc(value)
    if legacy_utc_date_semantics:
        return normalized.date()
    return asia_shanghai_trade_date(normalized)


def _freeze_json(value: JsonValue) -> JsonValue:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in sorted(value.items())})  # type: ignore[return-value]
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)  # type: ignore[return-value]
    return value


def _thaw_json(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings")
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_thaw_json(item) for item in value]
    return value  # type: ignore[return-value]


def canonicalize_candidate_static_features(value: object) -> Mapping[str, JsonValue]:
    thawed = _thaw_json(value)
    if not isinstance(thawed, dict):
        raise ValueError("static_features must be a JSON object")
    if any(not isinstance(key, str) or not key for key in thawed):
        raise ValueError("static_features keys must be non-empty strings")
    detached = json.loads(
        json.dumps(thawed, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    )
    return MappingProxyType({key: _freeze_json(item) for key, item in sorted(detached.items())})


def serialize_candidate_static_features(
    value: Mapping[str, JsonValue],
) -> dict[str, JsonValue]:
    return {key: _thaw_json(item) for key, item in value.items()}


def thaw_candidate_static_features(value: object) -> JsonValue:
    return _thaw_json(value)


def candidate_occurrence_id(
    *,
    strategy_id: str,
    strategy_version: str,
    candidate_id: str,
    variant: str,
    effective_trade_date: date,
) -> str:
    return canonical_sha256(
        {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "candidate_id": candidate_id,
            "variant": variant,
            "effective_trade_date": effective_trade_date,
        }
    )


def _snapshot_content_identity(
    *,
    schema_version: Literal[1, 2, 3],
    sequence: int,
    trade_date: date,
    captured_at: datetime,
    producer_commit: str,
    rows: Sequence[StrategyCandidateRecord],
    authority_binding: StrategyCandidateAuthorityBinding | None = None,
    source_snapshot_ids: Mapping[str, str] | None = None,
) -> dict[str, object]:
    if schema_version == 1:
        canonical_rows = tuple(
            sorted(
                rows,
                key=lambda row: (row.strategy_id, row.strategy_version, row.candidate_id),
            )
        )
        row_payloads: list[dict[str, object]] = []
        for row in canonical_rows:
            payload = row.model_dump(mode="python")
            payload.pop("effective_trade_date")
            row_payloads.append(payload)
        return {
            "sequence": sequence,
            "trade_date": trade_date,
            "captured_at": normalize_aware_utc(captured_at),
            "producer_commit": producer_commit,
            "rows": tuple(row_payloads),
        }
    canonical_rows = tuple(sorted(rows, key=lambda row: row.identity))
    identity: dict[str, object] = {
        "schema_version": schema_version,
        "sequence": sequence,
        "trade_date": trade_date,
        "captured_at": normalize_aware_utc(captured_at),
        "producer_commit": producer_commit,
        "rows": canonical_rows,
    }
    if schema_version == 3:
        identity["authority_binding"] = (
            None
            if authority_binding is None
            else _authority_binding_payload(authority_binding, mode="python")
        )
        identity["source_snapshot_ids"] = dict(sorted((source_snapshot_ids or {}).items()))
    return identity


def strategy_candidate_snapshot_content_sha256(
    *,
    schema_version: Literal[1, 2, 3],
    sequence: int,
    trade_date: date,
    captured_at: datetime,
    producer_commit: str,
    rows: Sequence[StrategyCandidateRecord],
    authority_binding: StrategyCandidateAuthorityBinding | None = None,
    source_snapshot_ids: Mapping[str, str] | None = None,
) -> str:
    return canonical_sha256(
        _snapshot_content_identity(
            schema_version=schema_version,
            sequence=sequence,
            trade_date=trade_date,
            captured_at=captured_at,
            producer_commit=producer_commit,
            rows=rows,
            authority_binding=authority_binding,
            source_snapshot_ids=source_snapshot_ids,
        )
    )


class StrategyCandidateRecord(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    candidate_id: str = Field(min_length=1)
    variant: str = Field(min_length=1)
    decision_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    effective_trade_date: date
    reference_trade_date: date
    price_basis: StrategyCandidatePriceBasis
    static_features: Mapping[str, JsonValue]
    reference_snapshot_ids: Mapping[str, Sha256]
    legacy_utc_date_semantics: bool = Field(default=False, exclude=True, repr=False)

    @field_validator("static_features", mode="before")
    @classmethod
    def thaw_static_features_for_validation(cls, value: object) -> JsonValue:
        return thaw_candidate_static_features(value)

    @field_validator("static_features")
    @classmethod
    def freeze_static_features(
        cls,
        value: object,
    ) -> Mapping[str, JsonValue]:
        return canonicalize_candidate_static_features(value)

    @field_validator("reference_snapshot_ids")
    @classmethod
    def freeze_reference_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not isinstance(key, str) or not key for key in value):
            raise ValueError("reference_snapshot_ids keys must be non-empty strings")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("static_features")
    def serialize_static_features(
        self,
        value: Mapping[str, JsonValue],
    ) -> dict[str, JsonValue]:
        return serialize_candidate_static_features(value)

    @field_serializer("reference_snapshot_ids")
    def serialize_reference_snapshot_ids(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_point_in_time(self) -> StrategyCandidateRecord:
        if self.available_at < self.decision_at:
            raise ValueError("available_at must be at or after decision_at")
        decision_trade_date = strategy_candidate_decision_trade_date(
            self.decision_at,
            legacy_utc_date_semantics=self.legacy_utc_date_semantics,
        )
        if self.effective_trade_date < decision_trade_date:
            raise ValueError("effective_trade_date cannot precede decision_at date")
        if self.reference_trade_date > decision_trade_date:
            raise ValueError("reference_trade_date cannot be a future reference")
        return self

    @property
    def identity(self) -> tuple[str, str, str, date]:
        return (
            self.strategy_id,
            self.strategy_version,
            self.candidate_id,
            self.effective_trade_date,
        )

    @property
    def occurrence_id(self) -> str:
        return candidate_occurrence_id(
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            candidate_id=self.candidate_id,
            variant=self.variant,
            effective_trade_date=self.effective_trade_date,
        )


class StrategyCandidateStaticFeatureSemantic(RuntimeContractModel):
    dtype: str = Field(min_length=1)
    semantic: str = Field(min_length=1)

    @field_validator("dtype", mode="before")
    @classmethod
    def require_canonical_dtype(cls, value: object) -> str:
        if not isinstance(value, str) or value not in {
            "array",
            "bool",
            "integer",
            "null",
            "number",
            "object",
            "string",
        }:
            raise ValueError("dtype must be a canonical static feature dtype")
        return value


def validate_candidate_static_feature_value(
    *,
    name: str,
    value: object,
    semantic: StrategyCandidateStaticFeatureSemantic,
) -> None:
    """Validate one detached JSON value against its immutable schema semantic."""

    dtype = semantic.dtype
    valid = False
    if dtype == "number":
        valid = type(value) in {int, float} and (
            not isinstance(value, float) or math.isfinite(value)
        )
    elif dtype == "integer":
        valid = type(value) is int
    elif dtype == "string":
        valid = isinstance(value, str)
    elif dtype == "bool":
        valid = type(value) is bool
    elif dtype == "object":
        valid = isinstance(value, Mapping)
    elif dtype == "array":
        valid = isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
    elif dtype == "null":
        valid = value is None
    if not valid:
        raise ValueError(f"static feature {name!r} does not match declared dtype {dtype!r}")


def validate_candidate_static_features_against_schema(
    *,
    static_features: Mapping[str, object],
    static_feature_schema: Mapping[str, StrategyCandidateStaticFeatureSemantic],
) -> None:
    if tuple(sorted(static_features)) != tuple(sorted(static_feature_schema)):
        raise ValueError("static feature names do not match declared schema")
    for name, semantic in static_feature_schema.items():
        validate_candidate_static_feature_value(
            name=name,
            value=static_features[name],
            semantic=semantic,
        )


def _canonical_strategy_version(value: str) -> int:
    if (
        not isinstance(value, str)
        or not value.isascii()
        or not value.isdecimal()
        or value == "0"
        or str(int(value)) != value
    ):
        raise ValueError("strategy_version must be canonical positive integer text")
    return int(value)


def strategy_candidate_schema_fingerprint(
    *,
    strategy_id: str,
    strategy_version: str,
    static_feature_schema: Mapping[str, object],
) -> str:
    validated_schema = {
        name: StrategyCandidateStaticFeatureSemantic.model_validate(semantic)
        for name, semantic in static_feature_schema.items()
    }
    return canonical_sha256(
        {
            "contract": "strategy-candidate-static-schema/v1",
            "strategy_id": strategy_id,
            "strategy_version": _canonical_strategy_version(strategy_version),
            "static_feature_schema": {
                name: semantic.model_dump(mode="python")
                for name, semantic in sorted(validated_schema.items())
            },
        }
    )


class StrategyCandidateAuthorityBinding(RuntimeContractModel):
    schema_version: Literal[1, 2, 3]
    strategy_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    strategy_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")
    definition_fingerprint: Sha256 | None = None
    executable_fingerprint: Sha256 | None = None
    candidate_schema_fingerprint: Sha256 | None = None
    static_feature_names: tuple[str, ...] = ()
    static_feature_schema: Mapping[str, StrategyCandidateStaticFeatureSemantic] = Field(
        default_factory=dict
    )
    content_sha256: Sha256

    @field_validator("static_feature_names")
    @classmethod
    def canonicalize_static_feature_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("authority static feature names must be sorted and unique")
        return value

    @field_validator("static_feature_schema")
    @classmethod
    def freeze_static_feature_schema(
        cls,
        value: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    ) -> Mapping[str, StrategyCandidateStaticFeatureSemantic]:
        if any(not isinstance(name, str) or not name for name in value):
            raise ValueError("authority static feature schema names must be nonempty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("static_feature_schema")
    def serialize_static_feature_schema(
        self,
        value: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    ) -> dict[str, dict[str, str]]:
        return {name: semantic.model_dump(mode="json") for name, semantic in value.items()}

    @model_validator(mode="after")
    def validate_content_hash(self) -> StrategyCandidateAuthorityBinding:
        static_binding = (
            self.definition_fingerprint,
            self.executable_fingerprint,
            self.candidate_schema_fingerprint,
        )
        if self.schema_version == 1 and any(item is not None for item in static_binding):
            raise ValueError("schema v1 authority cannot contain static semantic fingerprints")
        if self.schema_version == 2 and (
            self.definition_fingerprint is None
            or self.candidate_schema_fingerprint is None
            or self.executable_fingerprint is not None
        ):
            raise ValueError("schema v2 authority requires its two legacy fingerprints")
        if self.schema_version == 3:
            if any(item is None for item in static_binding):
                raise ValueError("schema v3 authority requires all semantic fingerprints")
            if not self.static_feature_schema:
                raise ValueError("schema v3 authority requires a static feature schema")
            expected_names = tuple(self.static_feature_schema)
            if self.static_feature_names != expected_names:
                raise ValueError("authority static feature names must exactly match its schema")
            expected_schema_fingerprint = strategy_candidate_schema_fingerprint(
                strategy_id=self.strategy_id,
                strategy_version=self.strategy_version,
                static_feature_schema=self.static_feature_schema,
            )
            if self.candidate_schema_fingerprint != expected_schema_fingerprint:
                raise ValueError("candidate schema fingerprint does not match static schema")
        elif self.static_feature_names or self.static_feature_schema:
            raise ValueError("legacy authority cannot contain a static feature schema")
        expected = canonical_sha256(
            _authority_binding_payload(self, mode="python", include_content_hash=False)
        )
        if self.content_sha256 != expected:
            raise ValueError("authority binding content_sha256 does not bind its identity")
        return self

    @classmethod
    def create(
        cls,
        *,
        strategy_id: str,
        strategy_version: str,
        definition_fingerprint: str,
        executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        static_feature_schema: Mapping[str, object],
    ) -> StrategyCandidateAuthorityBinding:
        validated_schema = {
            name: StrategyCandidateStaticFeatureSemantic.model_validate(semantic)
            for name, semantic in static_feature_schema.items()
        }
        identity = {
            "schema_version": 3,
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "definition_fingerprint": definition_fingerprint,
            "executable_fingerprint": executable_fingerprint,
            "candidate_schema_fingerprint": candidate_schema_fingerprint,
            "static_feature_names": tuple(sorted(validated_schema)),
            "static_feature_schema": dict(sorted(validated_schema.items())),
        }
        return cls(**identity, content_sha256=canonical_sha256(identity))


def _authority_binding_payload(
    binding: StrategyCandidateAuthorityBinding,
    *,
    mode: Literal["json", "python"],
    include_content_hash: bool = True,
) -> dict[str, object]:
    exclude = set() if include_content_hash else {"content_sha256"}
    payload = binding.model_dump(mode=mode, exclude=exclude)
    if binding.schema_version == 1:
        payload.pop("definition_fingerprint", None)
        payload.pop("executable_fingerprint", None)
        payload.pop("candidate_schema_fingerprint", None)
        payload.pop("static_feature_names", None)
        payload.pop("static_feature_schema", None)
    elif binding.schema_version == 2:
        payload.pop("executable_fingerprint", None)
        payload.pop("static_feature_names", None)
        payload.pop("static_feature_schema", None)
    return payload


class StrategyCandidateSnapshot(RuntimeContractModel):
    schema_version: Literal[1, 2, 3]
    sequence: int = Field(ge=0)
    trade_date: date
    captured_at: AwareUtcDatetime
    producer_commit: CommitSha
    authority_binding: StrategyCandidateAuthorityBinding | None = None
    source_snapshot_ids: Mapping[str, Sha256] = Field(default_factory=dict)
    rows: tuple[StrategyCandidateRecord, ...]
    content_sha256: Sha256

    @field_validator("source_snapshot_ids")
    @classmethod
    def freeze_source_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not isinstance(key, str) or not key for key in value):
            raise ValueError("source_snapshot_ids keys must be non-empty strings")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_snapshot_ids")
    def serialize_source_snapshot_ids(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @field_validator("rows")
    @classmethod
    def canonicalize_rows(
        cls,
        value: tuple[StrategyCandidateRecord, ...],
    ) -> tuple[StrategyCandidateRecord, ...]:
        return tuple(sorted(value, key=lambda row: row.identity))

    @model_validator(mode="after")
    def validate_snapshot(self) -> StrategyCandidateSnapshot:
        if self.schema_version == 1 and any(not row.legacy_utc_date_semantics for row in self.rows):
            raise ValueError("schema v1 rows require legacy UTC date semantics")
        if self.schema_version in {2, 3} and any(
            row.legacy_utc_date_semantics for row in self.rows
        ):
            raise ValueError("schema v2/v3 rows reject legacy UTC date semantics")
        if self.schema_version == 1:
            for row in self.rows:
                decision_trade_date = strategy_candidate_decision_trade_date(
                    row.decision_at,
                    legacy_utc_date_semantics=True,
                )
                if not (decision_trade_date == row.effective_trade_date == self.trade_date):
                    raise ValueError(
                        "schema v1 decision date must equal effective and snapshot trade date"
                    )
        if self.schema_version == 3:
            if self.authority_binding is None or not self.source_snapshot_ids:
                raise ValueError("schema v3 requires authority_binding and source_snapshot_ids")
            expected_identity = (
                self.authority_binding.strategy_id,
                self.authority_binding.strategy_version,
            )
            if any(
                (row.strategy_id, row.strategy_version) != expected_identity for row in self.rows
            ):
                raise ValueError("schema v3 row identity does not match authority binding")
            if any(
                tuple(row.static_features) != self.authority_binding.static_feature_names
                for row in self.rows
            ):
                raise ValueError("schema v3 row static features do not match authority schema")
            for row in self.rows:
                validate_candidate_static_features_against_schema(
                    static_features=row.static_features,
                    static_feature_schema=self.authority_binding.static_feature_schema,
                )
        elif self.authority_binding is not None or self.source_snapshot_ids:
            raise ValueError("schema v1/v2 cannot contain schema v3 authority evidence")
        identities = [row.identity for row in self.rows]
        if len(identities) != len(set(identities)):
            raise ValueError("snapshot contains a duplicate candidate")
        for row in self.rows:
            if row.effective_trade_date != self.trade_date:
                raise ValueError("candidate effective_trade_date must match snapshot trade_date")
            if row.reference_trade_date > self.trade_date:
                raise ValueError("candidate contains a future trade-date reference")
            if row.available_at > self.captured_at:
                raise ValueError("candidate available_at cannot exceed captured_at")
        expected = strategy_candidate_snapshot_content_sha256(
            schema_version=self.schema_version,
            sequence=self.sequence,
            trade_date=self.trade_date,
            captured_at=self.captured_at,
            producer_commit=self.producer_commit,
            rows=self.rows,
            authority_binding=self.authority_binding,
            source_snapshot_ids=self.source_snapshot_ids,
        )
        if self.content_sha256 != expected:
            raise ValueError("content_sha256 does not bind canonical snapshot content")
        return self

    @classmethod
    def build(
        cls,
        *,
        sequence: int,
        trade_date: date,
        captured_at: datetime,
        producer_commit: str,
        rows: Sequence[StrategyCandidateRecord],
    ) -> StrategyCandidateSnapshot:
        normalized_captured_at = normalize_aware_utc(captured_at)
        canonical_rows = tuple(sorted(rows, key=lambda row: row.identity))
        identity = _snapshot_content_identity(
            schema_version=2,
            sequence=sequence,
            trade_date=trade_date,
            captured_at=normalized_captured_at,
            producer_commit=producer_commit,
            rows=canonical_rows,
        )
        return cls(
            **identity,
            content_sha256=canonical_sha256(identity),
        )

    @classmethod
    def build_strategy(
        cls,
        *,
        sequence: int,
        trade_date: date,
        captured_at: datetime,
        producer_commit: str,
        authority_binding: StrategyCandidateAuthorityBinding,
        source_snapshot_ids: Mapping[str, str],
        rows: Sequence[StrategyCandidateRecord],
    ) -> StrategyCandidateSnapshot:
        normalized_captured_at = normalize_aware_utc(captured_at)
        canonical_rows = tuple(sorted(rows, key=lambda row: row.identity))
        for row in canonical_rows:
            validate_candidate_static_features_against_schema(
                static_features=row.static_features,
                static_feature_schema=authority_binding.static_feature_schema,
            )
        identity = _snapshot_content_identity(
            schema_version=3,
            sequence=sequence,
            trade_date=trade_date,
            captured_at=normalized_captured_at,
            producer_commit=producer_commit,
            authority_binding=authority_binding,
            source_snapshot_ids=source_snapshot_ids,
            rows=canonical_rows,
        )
        return cls(
            **identity,
            content_sha256=canonical_sha256(identity),
        )


class StrategyCandidatePublishResult(RuntimeContractModel):
    snapshot: StrategyCandidateSnapshot
    published: bool


class StrategyCandidateSnapshotPointer(RuntimeContractModel):
    generation_sha256: Sha256
    sequence: int = Field(ge=0)
    trade_date: date
    captured_at: AwareUtcDatetime
    producer_commit: CommitSha

    @classmethod
    def from_snapshot(
        cls,
        snapshot: StrategyCandidateSnapshot,
    ) -> StrategyCandidateSnapshotPointer:
        return cls(
            generation_sha256=snapshot.content_sha256,
            sequence=snapshot.sequence,
            trade_date=snapshot.trade_date,
            captured_at=snapshot.captured_at,
            producer_commit=snapshot.producer_commit,
        )


class StrategyCandidateGenerationMetadata(RuntimeContractModel):
    sequence: int = Field(ge=0)
    generation_sha256: Sha256
    schema_version: Literal[1, 2, 3]
    trade_date: date
    captured_at: AwareUtcDatetime
    max_available_at: AwareUtcDatetime
    producer_commit: CommitSha
    authority_binding_sha256: Sha256 | None = None
    size_bytes: int = Field(gt=0, le=_MAX_AUTHORITY_BYTES)

    @model_validator(mode="after")
    def validate_metadata(self) -> StrategyCandidateGenerationMetadata:
        if self.max_available_at > self.captured_at:
            raise ValueError("generation max_available_at cannot exceed captured_at")
        if self.schema_version == 3 and self.authority_binding_sha256 is None:
            raise ValueError("schema v3 generation metadata requires authority binding")
        if self.schema_version != 3 and self.authority_binding_sha256 is not None:
            raise ValueError("legacy generation metadata cannot bind strategy authority")
        return self

    @classmethod
    def from_snapshot(
        cls,
        snapshot: StrategyCandidateSnapshot,
        *,
        size_bytes: int,
    ) -> StrategyCandidateGenerationMetadata:
        binding_sha = (
            snapshot.authority_binding.content_sha256
            if snapshot.authority_binding is not None
            else None
        )
        return cls(
            sequence=snapshot.sequence,
            generation_sha256=snapshot.content_sha256,
            schema_version=snapshot.schema_version,
            trade_date=snapshot.trade_date,
            captured_at=snapshot.captured_at,
            max_available_at=max(
                (row.available_at for row in snapshot.rows),
                default=snapshot.captured_at,
            ),
            producer_commit=snapshot.producer_commit,
            authority_binding_sha256=binding_sha,
            size_bytes=size_bytes,
        )

    @property
    def generation_name(self) -> str:
        return f"{self.generation_sha256}.json"

    @property
    def pointer(self) -> StrategyCandidateSnapshotPointer:
        return StrategyCandidateSnapshotPointer(
            generation_sha256=self.generation_sha256,
            sequence=self.sequence,
            trade_date=self.trade_date,
            captured_at=self.captured_at,
            producer_commit=self.producer_commit,
        )


class StrategyCandidateGenerationIndex(RuntimeContractModel):
    schema_version: Literal[1] = 1
    entries: tuple[StrategyCandidateGenerationMetadata, ...]

    @field_validator("entries")
    @classmethod
    def validate_entries(
        cls,
        value: tuple[StrategyCandidateGenerationMetadata, ...],
    ) -> tuple[StrategyCandidateGenerationMetadata, ...]:
        if len(value) > _MAX_GENERATIONS:
            raise ValueError("strategy candidate generation count exceeds limit")
        if tuple(entry.sequence for entry in value) != tuple(range(len(value))):
            raise ValueError("generation metadata sequences must be contiguous")
        hashes = tuple(entry.generation_sha256 for entry in value)
        if len(hashes) != len(set(hashes)):
            raise ValueError("generation metadata hashes must be unique")
        captured = tuple(entry.captured_at for entry in value)
        if captured != tuple(sorted(captured)):
            raise ValueError("generation metadata captured_at cannot move backwards")
        return value


class StrategyCandidateSnapshotSpool:
    """Publish and resolve immutable point-in-time candidate generations."""

    def __init__(self, root: Path) -> None:
        candidate = Path(root)
        if not candidate.is_absolute():
            raise ValueError("strategy candidate snapshot root must be absolute")
        normalized = Path(os.path.abspath(candidate))
        if candidate != normalized:
            raise ValueError("strategy candidate snapshot root must be normalized")
        probe = candidate
        while True:
            try:
                probe.lstat()
                break
            except FileNotFoundError:
                if probe == Path(probe.anchor):
                    raise
                probe = probe.parent
        descriptor = self._open_directory(probe, private_final=False)
        os.close(descriptor)
        self.root = candidate
        self.generations_root = self.root / "generations"
        self.authority_path = self.root / "authority.json"
        self.current_path = self.root / "current.json"
        self._lock_path = self.root / ".publish.lock"
        self._thread_lock = RLock()
        self._generation_cache: OrderedDict[
            str,
            tuple[tuple[int, ...], StrategyCandidateSnapshot, int],
        ] = OrderedDict()
        self._generation_cache_bytes = 0

    def publish_legacy_for_migration(
        self,
        snapshot: StrategyCandidateSnapshot,
    ) -> StrategyCandidateSnapshot:
        if not isinstance(snapshot, StrategyCandidateSnapshot):
            raise TypeError("snapshot must be a StrategyCandidateSnapshot")
        if snapshot.schema_version != 2:
            raise StrategyCandidateSnapshotIntegrityError(
                "only schema v2 snapshots may be published"
            )
        self._initialize_for_publish()
        with self._locked(exclusive=True) as (root_fd, generations_fd):
            self._cleanup_stale_temporaries(root_fd)
            if self._entry_exists(root_fd, "authority.json"):
                raise StrategyCandidateSnapshotIntegrityError(
                    "bound authority requires strategy-aware publication"
                )
            generations = self._read_generation_index(
                root_fd,
                generations_fd,
                allow_one_orphan=True,
            )
            if any(entry.schema_version == 3 for entry in generations):
                raise StrategyCandidateSnapshotIntegrityError(
                    "schema v3 bound generations require strategy-aware publication"
                )
            try:
                self._validate_current_pointer(root_fd, generations)
            except StrategyCandidateSnapshotIntegrityError:
                if self._finish_interrupted_publish(root_fd, generations, snapshot):
                    return snapshot
                raise
            return self._publish_locked(root_fd, generations_fd, generations, snapshot)

    def publish_legacy_records_for_migration(
        self,
        *,
        trade_date: date,
        captured_at: datetime,
        producer_commit: str,
        rows: Sequence[StrategyCandidateRecord],
    ) -> StrategyCandidatePublishResult:
        validated_request = self._validate_publish_request(
            trade_date=trade_date,
            captured_at=captured_at,
            producer_commit=producer_commit,
            rows=rows,
        )
        return self._publish_records_request(
            validated_request,
            authority_binding=None,
        )

    def publish_strategy_records(
        self,
        *,
        strategy_id: str,
        strategy_version: str,
        definition_fingerprint: str,
        executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        static_feature_schema: Mapping[str, object],
        source_snapshot_ids: Mapping[str, str],
        trade_date: date,
        captured_at: datetime,
        producer_commit: str,
        rows: Sequence[StrategyCandidateRecord],
    ) -> StrategyCandidatePublishResult:
        authority_binding = StrategyCandidateAuthorityBinding.create(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            definition_fingerprint=definition_fingerprint,
            executable_fingerprint=executable_fingerprint,
            candidate_schema_fingerprint=candidate_schema_fingerprint,
            static_feature_schema=static_feature_schema,
        )
        if not isinstance(source_snapshot_ids, Mapping):
            raise TypeError("source_snapshot_ids must be a mapping")
        validated_request = StrategyCandidateSnapshot.build_strategy(
            sequence=0,
            trade_date=trade_date,
            captured_at=captured_at,
            producer_commit=producer_commit,
            authority_binding=authority_binding,
            source_snapshot_ids=source_snapshot_ids,
            rows=rows,
        )
        return self._publish_records_request(
            validated_request,
            authority_binding=authority_binding,
        )

    @staticmethod
    def _validate_publish_request(
        *,
        trade_date: date,
        captured_at: datetime,
        producer_commit: str,
        rows: Sequence[StrategyCandidateRecord],
    ) -> StrategyCandidateSnapshot:
        if (
            not isinstance(rows, Sequence)
            or isinstance(rows, (str, bytes, bytearray))
            or any(not isinstance(row, StrategyCandidateRecord) for row in rows)
        ):
            raise TypeError("rows must be a Sequence[StrategyCandidateRecord]")
        return StrategyCandidateSnapshot.build(
            sequence=0,
            trade_date=trade_date,
            captured_at=captured_at,
            producer_commit=producer_commit,
            rows=rows,
        )

    def _publish_records_request(
        self,
        validated_request: StrategyCandidateSnapshot,
        *,
        authority_binding: StrategyCandidateAuthorityBinding | None,
    ) -> StrategyCandidatePublishResult:
        self._initialize_for_publish()
        with self._locked(exclusive=True) as (root_fd, generations_fd):
            self._cleanup_stale_temporaries(root_fd)
            generations = self._read_generation_index(
                root_fd,
                generations_fd,
                allow_one_orphan=True,
            )
            binding_exists = self._validate_authority_binding(
                root_fd,
                generations,
                expected=authority_binding,
            )
            try:
                self._validate_current_pointer(root_fd, generations)
            except StrategyCandidateSnapshotIntegrityError:
                interrupted = (
                    None
                    if not generations
                    else self._load_generation(generations_fd, generations[-1])
                )
                if (
                    interrupted is not None
                    and validated_request.captured_at >= interrupted.captured_at
                    and self._same_semantics(interrupted, validated_request)
                    and self._finish_interrupted_publish(root_fd, generations, interrupted)
                ):
                    return StrategyCandidatePublishResult(
                        snapshot=interrupted,
                        published=True,
                    )
                raise
            if authority_binding is not None and not binding_exists:
                self._atomic_create_authority_binding(
                    root_fd,
                    self._authority_binding_bytes(authority_binding),
                )
            current = (
                None if not generations else self._load_generation(generations_fd, generations[-1])
            )
            if current is not None and validated_request.captured_at < current.captured_at:
                raise StrategyCandidateSnapshotIntegrityError(
                    "captured_at cannot move backwards across sequences"
                )
            if current is not None and self._same_semantics(current, validated_request):
                return StrategyCandidatePublishResult(snapshot=current, published=False)
            snapshot = self._resequence_snapshot(
                validated_request,
                sequence=0 if current is None else current.sequence + 1,
            )
            published = self._publish_locked(
                root_fd,
                generations_fd,
                generations,
                snapshot,
            )
            return StrategyCandidatePublishResult(snapshot=published, published=True)

    def read_authority_binding(
        self,
        *,
        strategy_id: str,
        strategy_version: str,
        definition_fingerprint: str,
        executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        static_feature_schema: Mapping[str, object],
    ) -> StrategyCandidateAuthorityBinding:
        expected = StrategyCandidateAuthorityBinding.create(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            definition_fingerprint=definition_fingerprint,
            executable_fingerprint=executable_fingerprint,
            candidate_schema_fingerprint=candidate_schema_fingerprint,
            static_feature_schema=static_feature_schema,
        )
        with self._locked(exclusive=False) as (root_fd, generations_fd):
            generations = self._read_generation_index(root_fd, generations_fd)
            self._validate_authority_binding(root_fd, generations, expected=expected)
            return expected

    def read_strategy_as_of(
        self,
        as_of: datetime,
        *,
        strategy_id: str,
        strategy_version: str,
        definition_fingerprint: str,
        executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        static_feature_schema: Mapping[str, object],
    ) -> StrategyCandidateSnapshot | None:
        normalized_as_of = normalize_aware_utc(as_of)
        expected = StrategyCandidateAuthorityBinding.create(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            definition_fingerprint=definition_fingerprint,
            executable_fingerprint=executable_fingerprint,
            candidate_schema_fingerprint=candidate_schema_fingerprint,
            static_feature_schema=static_feature_schema,
        )
        with self._locked(exclusive=False) as (root_fd, generations_fd):
            generations = self._read_generation_index(root_fd, generations_fd)
            self._validate_authority_binding(
                root_fd,
                generations,
                expected=expected,
            )
            self._validate_current_pointer(root_fd, generations)
            return self._visible_snapshot(generations_fd, generations, normalized_as_of)

    def _validate_authority_binding(
        self,
        root_fd: int,
        generations: Sequence[StrategyCandidateGenerationMetadata],
        *,
        expected: StrategyCandidateAuthorityBinding | None,
    ) -> bool:
        exists = self._entry_exists(root_fd, "authority.json")
        if expected is None:
            if exists or any(entry.schema_version == 3 for entry in generations):
                raise StrategyCandidateSnapshotIntegrityError(
                    "bound authority requires strategy-aware publication"
                )
            return False
        if not exists:
            if generations:
                raise StrategyCandidateSnapshotIntegrityError(
                    "unbound legacy generations cannot be claimed by a strategy"
                )
            return False
        observed = self._read_authority_binding(root_fd)
        if observed != expected:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate authority is bound to a different identity"
            )
        if any(
            entry.schema_version != 3 or entry.authority_binding_sha256 != expected.content_sha256
            for entry in generations
        ):
            raise StrategyCandidateSnapshotIntegrityError(
                "bound authority generations do not match root identity"
            )
        return True

    @staticmethod
    def _same_semantics(
        snapshot: StrategyCandidateSnapshot,
        request: StrategyCandidateSnapshot,
    ) -> bool:
        return (
            snapshot.schema_version == request.schema_version
            and snapshot.trade_date == request.trade_date
            and snapshot.producer_commit == request.producer_commit
            and snapshot.authority_binding == request.authority_binding
            and snapshot.source_snapshot_ids == request.source_snapshot_ids
            and snapshot.rows == request.rows
        )

    @staticmethod
    def _resequence_snapshot(
        request: StrategyCandidateSnapshot,
        *,
        sequence: int,
    ) -> StrategyCandidateSnapshot:
        if request.schema_version == 3:
            if request.authority_binding is None:
                raise StrategyCandidateSnapshotIntegrityError(
                    "schema v3 authority binding is missing"
                )
            return StrategyCandidateSnapshot.build_strategy(
                sequence=sequence,
                trade_date=request.trade_date,
                captured_at=request.captured_at,
                producer_commit=request.producer_commit,
                authority_binding=request.authority_binding,
                source_snapshot_ids=request.source_snapshot_ids,
                rows=request.rows,
            )
        return StrategyCandidateSnapshot.build(
            sequence=sequence,
            trade_date=request.trade_date,
            captured_at=request.captured_at,
            producer_commit=request.producer_commit,
            rows=request.rows,
        )

    def _publish_locked(
        self,
        root_fd: int,
        generations_fd: int,
        generations: Sequence[StrategyCandidateGenerationMetadata],
        snapshot: StrategyCandidateSnapshot,
    ) -> StrategyCandidateSnapshot:
        existing = generations[snapshot.sequence] if snapshot.sequence < len(generations) else None
        if existing is not None:
            loaded = self._load_generation(generations_fd, existing)
            if loaded != snapshot:
                raise StrategyCandidateSnapshotIntegrityError(
                    "immutable sequence already contains different content"
                )
            return loaded
        if len(generations) >= _MAX_GENERATIONS:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generation count exceeds limit"
            )
        expected_sequence = len(generations)
        if snapshot.sequence != expected_sequence:
            raise StrategyCandidateSnapshotIntegrityError(
                f"next sequence must be {expected_sequence}, got {snapshot.sequence}"
            )
        if generations and snapshot.captured_at < generations[-1].captured_at:
            raise StrategyCandidateSnapshotIntegrityError(
                "captured_at cannot move backwards across sequences"
            )
        generation_name = self._generation_name(snapshot.content_sha256)
        if self._entry_exists(generations_fd, generation_name):
            raise StrategyCandidateSnapshotIntegrityError(
                "generation hash already exists with conflicting sequence authority"
            )
        snapshot_payload = self._snapshot_bytes(snapshot)
        self._atomic_create_generation(
            root_fd,
            generations_fd,
            generation_name,
            snapshot_payload,
        )
        updated_index = StrategyCandidateGenerationIndex(
            entries=(
                *generations,
                StrategyCandidateGenerationMetadata.from_snapshot(
                    snapshot,
                    size_bytes=len(snapshot_payload),
                ),
            )
        )
        self._atomic_replace_generation_index(
            root_fd,
            self._model_bytes(updated_index),
        )
        self._atomic_replace_pointer(
            root_fd,
            self._model_bytes(StrategyCandidateSnapshotPointer.from_snapshot(snapshot)),
        )
        return snapshot

    def read_as_of(self, as_of: datetime) -> StrategyCandidateSnapshot | None:
        normalize_aware_utc(as_of)
        with self._locked(exclusive=False) as (root_fd, generations_fd):
            if self._entry_exists(root_fd, "authority.json"):
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy-aware authority requires read_strategy_as_of with exact binding"
                )
            generations = self._read_generation_index(root_fd, generations_fd)
            if any(entry.schema_version == 3 for entry in generations):
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy-aware authority requires read_strategy_as_of with exact binding"
                )
            if generations:
                raise StrategyCandidateSnapshotIntegrityError(
                    "legacy authority requires explicit migration or republication"
                )
            return None

    def read_legacy_for_migration(
        self,
        as_of: datetime,
    ) -> StrategyCandidateSnapshot | None:
        normalized_as_of = normalize_aware_utc(as_of)
        with self._locked(exclusive=False) as (root_fd, generations_fd):
            generations = self._read_generation_index(
                root_fd,
                generations_fd,
                allow_unindexed_legacy=True,
            )
            self._validate_authority_binding(root_fd, generations, expected=None)
            self._validate_current_pointer(root_fd, generations)
            return self._visible_snapshot(generations_fd, generations, normalized_as_of)

    def _visible_snapshot(
        self,
        generations_fd: int,
        generations: Sequence[StrategyCandidateGenerationMetadata],
        as_of: datetime,
    ) -> StrategyCandidateSnapshot | None:
        visible_count = bisect_right(
            tuple(entry.captured_at for entry in generations),
            as_of,
        )
        if visible_count == 0:
            return None
        metadata = generations[visible_count - 1]
        if metadata.max_available_at > as_of:
            return None
        return self._load_generation(generations_fd, metadata)

    @staticmethod
    def _same_file(left: os.stat_result, right: os.stat_result) -> bool:
        stable_identity_matches = (
            left.st_dev,
            left.st_ino,
            left.st_mode,
            left.st_uid,
        ) == (
            right.st_dev,
            right.st_ino,
            right.st_mode,
            right.st_uid,
        )
        if not stable_identity_matches:
            return False
        if stat.S_ISDIR(left.st_mode) and stat.S_ISDIR(right.st_mode):
            return True
        return left.st_nlink == right.st_nlink

    @staticmethod
    def _validate_private_directory(observed: os.stat_result, *, label: str) -> None:
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) != _PRIVATE_DIRECTORY_MODE
        ):
            raise StrategyCandidateSnapshotIntegrityError(f"unsafe {label}")

    @classmethod
    def _open_directory(cls, path: Path, *, private_final: bool) -> int:
        descriptor = -1
        child = -1
        try:
            descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
            for component in path.parts[1:]:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                if stat.S_ISLNK(before.st_mode):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot path contains a symlink"
                    )
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
                opened = os.fstat(child)
                active = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                if not cls._same_file(before, opened) or not cls._same_file(opened, active):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot path identity changed"
                    )
                os.close(descriptor)
                descriptor = child
                child = -1
            if private_final:
                cls._validate_private_directory(
                    os.fstat(descriptor), label="strategy candidate snapshot directory"
                )
            return descriptor
        except OSError as exc:
            if child >= 0:
                os.close(child)
            if descriptor >= 0:
                os.close(descriptor)
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate snapshot directory is missing or contains a symlink"
            ) from exc
        except BaseException:
            if child >= 0:
                os.close(child)
            if descriptor >= 0:
                os.close(descriptor)
            raise

    @classmethod
    def _open_or_create_private_directory(cls, path: Path) -> int:
        descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
        child = -1
        try:
            for index, component in enumerate(path.parts[1:], start=1):
                created = False
                try:
                    before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                except FileNotFoundError:
                    with suppress(FileExistsError):
                        os.mkdir(component, _PRIVATE_DIRECTORY_MODE, dir_fd=descriptor)
                    before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                    created = True
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
                opened = os.fstat(child)
                active = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                if not cls._same_file(before, opened) or not cls._same_file(opened, active):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot directory identity changed"
                    )
                if created or index == len(path.parts) - 1:
                    cls._validate_private_directory(
                        opened, label="strategy candidate snapshot directory"
                    )
                os.close(descriptor)
                descriptor = child
                child = -1
            return descriptor
        except OSError as exc:
            if child >= 0:
                os.close(child)
            if descriptor >= 0:
                os.close(descriptor)
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate snapshot directory is unsafe"
            ) from exc
        except BaseException:
            if child >= 0:
                os.close(child)
            if descriptor >= 0:
                os.close(descriptor)
            raise

    @classmethod
    def _open_child_directory(cls, parent_fd: int, name: str) -> int:
        descriptor = -1
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            if descriptor >= 0:
                os.close(descriptor)
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generations directory is missing or unsafe"
            ) from exc
        try:
            if not cls._same_file(before, opened) or not cls._same_file(opened, active):
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy candidate generations directory identity changed"
                )
            cls._validate_private_directory(
                opened, label="strategy candidate generations directory"
            )
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _initialize_for_publish(self) -> None:
        root_fd = self._open_or_create_private_directory(self.root)
        try:
            try:
                generations_fd = self._open_child_directory(root_fd, "generations")
            except StrategyCandidateSnapshotIntegrityError:
                with suppress(FileExistsError):
                    os.mkdir("generations", _PRIVATE_DIRECTORY_MODE, dir_fd=root_fd)
                generations_fd = self._open_child_directory(root_fd, "generations")
            os.close(generations_fd)
            self._ensure_lock_file(root_fd)
        finally:
            os.close(root_fd)

    @staticmethod
    def _validate_private_file(observed: os.stat_result, *, label: str) -> None:
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != _PRIVATE_FILE_MODE
        ):
            raise StrategyCandidateSnapshotIntegrityError(f"{label} is not a private regular file")

    @classmethod
    def _ensure_lock_file(cls, root_fd: int) -> None:
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = -1
        try:
            try:
                descriptor = os.open(".publish.lock", flags, _PRIVATE_FILE_MODE, dir_fd=root_fd)
            except FileExistsError:
                descriptor = os.open(
                    ".publish.lock",
                    os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_fd,
                )
            cls._validate_private_file(os.fstat(descriptor), label="snapshot lock")
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate snapshot lock is unsafe"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @contextmanager
    def _locked(self, *, exclusive: bool) -> Iterator[tuple[int, int]]:
        with self._thread_lock:
            root_fd = self._open_directory(self.root, private_final=True)
            lock_fd = -1
            generations_fd = -1
            try:
                before = os.stat(".publish.lock", dir_fd=root_fd, follow_symlinks=False)
                if stat.S_ISLNK(before.st_mode):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot lock cannot be a symlink"
                    )
                lock_fd = os.open(
                    ".publish.lock",
                    (os.O_RDWR if exclusive else os.O_RDONLY) | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_fd,
                )
                opened = os.fstat(lock_fd)
                active = os.stat(".publish.lock", dir_fd=root_fd, follow_symlinks=False)
                self._validate_private_file(opened, label="snapshot lock")
                if not self._same_file(before, opened) or not self._same_file(opened, active):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot lock identity changed"
                    )
                fcntl.flock(lock_fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                locked_active = os.stat(".publish.lock", dir_fd=root_fd, follow_symlinks=False)
                if not self._same_file(opened, locked_active):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot lock changed while waiting"
                    )
                generations_fd = self._open_child_directory(root_fd, "generations")
                yield root_fd, generations_fd
            except OSError as exc:
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy candidate snapshot lock is missing or unsafe"
                ) from exc
            finally:
                if generations_fd >= 0:
                    os.close(generations_fd)
                if lock_fd >= 0:
                    with suppress(OSError):
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)
                os.close(root_fd)

    def _read_generation_index(
        self,
        root_fd: int,
        generations_fd: int,
        *,
        allow_one_orphan: bool = False,
        allow_unindexed_legacy: bool = False,
    ) -> tuple[StrategyCandidateGenerationMetadata, ...]:
        index_exists = self._entry_exists(root_fd, "generation-index.json")
        if index_exists:
            payload = self._read_regular_file(
                root_fd,
                "generation-index.json",
                label="generation index",
            )
            try:
                index = StrategyCandidateGenerationIndex.model_validate_json(payload)
            except ValueError as exc:
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy candidate generation index is invalid"
                ) from exc
            if self._model_bytes(index) != payload:
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy candidate generation index is not canonical JSON"
                )
            indexed = {entry.generation_name: entry for entry in index.entries}
        else:
            index = StrategyCandidateGenerationIndex(entries=())
            indexed = {}

        observed: dict[str, os.stat_result] = {}
        try:
            with os.scandir(generations_fd) as entries:
                for count, entry in enumerate(entries, start=1):
                    if count > _MAX_GENERATIONS:
                        raise StrategyCandidateSnapshotIntegrityError(
                            "strategy candidate generation count exceeds limit"
                        )
                    if re.fullmatch(r"[0-9a-f]{64}\.json", entry.name) is None:
                        raise StrategyCandidateSnapshotIntegrityError(
                            "strategy candidate generations contain an unexpected entry"
                        )
                    entry_stat = entry.stat(follow_symlinks=False)
                    if stat.S_ISLNK(entry_stat.st_mode):
                        raise StrategyCandidateSnapshotIntegrityError(
                            "generation cannot be a symlink"
                        )
                    self._validate_private_file(entry_stat, label="generation")
                    observed[entry.name] = entry_stat
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generations are unreadable"
            ) from exc

        missing = set(indexed) - set(observed)
        extras = set(observed) - set(indexed)
        if missing:
            raise StrategyCandidateSnapshotIntegrityError(
                "generation sequence is missing an indexed entry"
            )
        for name, metadata in indexed.items():
            if observed[name].st_size != metadata.size_bytes:
                raise StrategyCandidateSnapshotIntegrityError(
                    "immutable generation size changed after indexing"
                )
        if not extras:
            if observed and not index_exists:
                raise StrategyCandidateSnapshotIntegrityError("generation index is missing")
            return index.entries
        if not index_exists and allow_unindexed_legacy:
            migrated: dict[int, StrategyCandidateGenerationMetadata] = {}
            for name in extras:
                snapshot = self._read_snapshot(generations_fd, name)
                if snapshot.schema_version == 3:
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy-bound generation requires indexed republication"
                    )
                metadata = StrategyCandidateGenerationMetadata.from_snapshot(
                    snapshot,
                    size_bytes=observed[name].st_size,
                )
                if metadata.generation_name != name:
                    raise StrategyCandidateSnapshotIntegrityError(
                        "generation filename does not match content_sha256"
                    )
                if metadata.sequence in migrated:
                    raise StrategyCandidateSnapshotIntegrityError(
                        "duplicate generation sequence conflict"
                    )
                migrated[metadata.sequence] = metadata
            if tuple(sorted(migrated)) != tuple(range(len(migrated))):
                raise StrategyCandidateSnapshotIntegrityError(
                    "generation sequence has a missing or conflicting entry"
                )
            return tuple(migrated[index] for index in range(len(migrated)))
        if not allow_one_orphan or len(extras) != 1 or len(index.entries) >= _MAX_GENERATIONS:
            raise StrategyCandidateSnapshotIntegrityError(
                "generation index has a duplicate or unindexed generation"
            )
        orphan_name = next(iter(extras))
        orphan = self._read_snapshot(generations_fd, orphan_name)
        if orphan.sequence != len(index.entries):
            raise StrategyCandidateSnapshotIntegrityError(
                "orphan generation sequence is not the next contiguous sequence"
            )
        if index.entries and orphan.captured_at < index.entries[-1].captured_at:
            raise StrategyCandidateSnapshotIntegrityError(
                "orphan generation captured_at moves backwards"
            )
        orphan_metadata = StrategyCandidateGenerationMetadata.from_snapshot(
            orphan,
            size_bytes=observed[orphan_name].st_size,
        )
        if orphan_metadata.generation_name != orphan_name:
            raise StrategyCandidateSnapshotIntegrityError(
                "orphan generation filename does not match content"
            )
        return (*index.entries, orphan_metadata)

    def _load_generation(
        self,
        generations_fd: int,
        metadata: StrategyCandidateGenerationMetadata,
    ) -> StrategyCandidateSnapshot:
        name = metadata.generation_name
        try:
            observed = os.stat(name, dir_fd=generations_fd, follow_symlinks=False)
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "indexed generation is missing or unsafe"
            ) from exc
        self._validate_private_file(observed, label="generation")
        state = self._cache_state(observed)
        cached = self._generation_cache.get(name)
        if cached is not None:
            cached_state, snapshot, _size = cached
            if cached_state != state:
                raise StrategyCandidateSnapshotIntegrityError(
                    "immutable generation changed after validation"
                )
            self._validate_cached_generation(
                generations_fd,
                name,
                expected_state=state,
            )
            self._generation_cache.move_to_end(name)
            return snapshot

        snapshot = self._read_snapshot(generations_fd, name)
        active = os.stat(name, dir_fd=generations_fd, follow_symlinks=False)
        if self._cache_state(active) != state:
            raise StrategyCandidateSnapshotIntegrityError(
                "generation changed while populating cache"
            )
        expected = StrategyCandidateGenerationMetadata.from_snapshot(
            snapshot,
            size_bytes=active.st_size,
        )
        if expected != metadata:
            raise StrategyCandidateSnapshotIntegrityError(
                "generation content does not match indexed metadata"
            )
        self._generation_cache[name] = (state, snapshot, active.st_size)
        self._generation_cache_bytes += active.st_size
        while (
            len(self._generation_cache) > _GENERATION_CACHE_MAX_ITEMS
            or self._generation_cache_bytes > _GENERATION_CACHE_MAX_BYTES
        ):
            evicted_name, (_evicted_state, _evicted_snapshot, evicted_size) = (
                self._generation_cache.popitem(last=False)
            )
            self._generation_cache_bytes -= evicted_size
            if evicted_name == name and not self._generation_cache:
                break
        return snapshot

    @staticmethod
    def _cache_state(observed: os.stat_result) -> tuple[int, ...]:
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_uid,
            observed.st_nlink,
            observed.st_size,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )

    @classmethod
    def _validate_cached_generation(
        cls,
        parent_fd: int,
        name: str,
        *,
        expected_state: tuple[int, ...],
    ) -> None:
        descriptor = -1
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            cls._validate_private_file(opened, label="generation")
            if (
                cls._cache_state(opened) != expected_state
                or cls._cache_state(active) != expected_state
            ):
                raise StrategyCandidateSnapshotIntegrityError(
                    "immutable generation changed after validation"
                )
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "cached generation is missing or unsafe"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _validate_current_pointer(
        self,
        root_fd: int,
        generations: Sequence[StrategyCandidateGenerationMetadata],
    ) -> None:
        pointer_exists = self._entry_exists(root_fd, "current.json")
        if not generations:
            if pointer_exists:
                raise StrategyCandidateSnapshotIntegrityError(
                    "current pointer generation is missing"
                )
            return
        if not pointer_exists:
            raise StrategyCandidateSnapshotIntegrityError("current pointer is missing")
        pointer = self._read_pointer(root_fd)
        latest = generations[-1]
        expected = latest.pointer
        if pointer != expected:
            if pointer.generation_sha256 not in {entry.generation_sha256 for entry in generations}:
                raise StrategyCandidateSnapshotIntegrityError(
                    "current pointer generation is missing"
                )
            raise StrategyCandidateSnapshotIntegrityError(
                "current pointer does not bind the latest generation"
            )

    def _finish_interrupted_publish(
        self,
        root_fd: int,
        generations: Sequence[StrategyCandidateGenerationMetadata],
        snapshot: StrategyCandidateSnapshot,
    ) -> bool:
        if not generations or generations[-1].generation_sha256 != snapshot.content_sha256:
            return False
        if snapshot.sequence == 0:
            if self._entry_exists(root_fd, "current.json"):
                return False
        else:
            if not self._entry_exists(root_fd, "current.json"):
                return False
            pointer = self._read_pointer(root_fd)
            previous = generations[snapshot.sequence - 1]
            if pointer != previous.pointer:
                return False
        self._atomic_replace_generation_index(
            root_fd,
            self._model_bytes(StrategyCandidateGenerationIndex(entries=tuple(generations))),
        )
        self._atomic_replace_pointer(
            root_fd,
            self._model_bytes(StrategyCandidateSnapshotPointer.from_snapshot(snapshot)),
        )
        return True

    def _read_snapshot(self, parent_fd: int, name: str) -> StrategyCandidateSnapshot:
        payload = self._read_regular_file(parent_fd, name, label="generation")
        try:
            raw = json.loads(payload)
            if not isinstance(raw, dict):
                raise ValueError("snapshot generation must be a JSON object")
            if "schema_version" not in raw:
                rows = raw.get("rows")
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                    raise ValueError("legacy snapshot rows are invalid")
                if any("effective_trade_date" in row for row in rows):
                    raise ValueError("legacy snapshot cannot contain effective_trade_date")
                trade_date = raw.get("trade_date")
                raw["schema_version"] = 1
                for row in rows:
                    row["effective_trade_date"] = trade_date
                    row["legacy_utc_date_semantics"] = True
            snapshot = StrategyCandidateSnapshot.model_validate(raw)
        except (TypeError, ValueError) as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generation is invalid"
            ) from exc
        if self._snapshot_bytes(snapshot) != payload:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generation is not canonical JSON"
            )
        return snapshot

    @classmethod
    def _snapshot_bytes(cls, snapshot: StrategyCandidateSnapshot) -> bytes:
        payload = snapshot.model_dump(mode="json")
        if snapshot.schema_version in {1, 2}:
            payload.pop("authority_binding")
            payload.pop("source_snapshot_ids")
        if snapshot.schema_version == 1:
            payload.pop("schema_version")
            for row in payload["rows"]:
                row.pop("effective_trade_date")
        if snapshot.schema_version == 3 and snapshot.authority_binding is not None:
            payload["authority_binding"] = _authority_binding_payload(
                snapshot.authority_binding,
                mode="json",
            )
        return cls._canonical_json_bytes(payload)

    def _read_pointer(self, root_fd: int) -> StrategyCandidateSnapshotPointer:
        payload = self._read_regular_file(root_fd, "current.json", label="current pointer")
        try:
            pointer = StrategyCandidateSnapshotPointer.model_validate_json(payload)
        except ValueError as exc:
            raise StrategyCandidateSnapshotIntegrityError("current pointer is invalid") from exc
        if self._model_bytes(pointer) != payload:
            raise StrategyCandidateSnapshotIntegrityError("current pointer is not canonical JSON")
        return pointer

    def _read_authority_binding(
        self,
        root_fd: int,
    ) -> StrategyCandidateAuthorityBinding:
        payload = self._read_regular_file(
            root_fd,
            "authority.json",
            label="authority binding",
        )
        try:
            binding = StrategyCandidateAuthorityBinding.model_validate_json(payload)
        except ValueError as exc:
            raise StrategyCandidateSnapshotIntegrityError("authority binding is invalid") from exc
        if self._authority_binding_bytes(binding) != payload:
            raise StrategyCandidateSnapshotIntegrityError("authority binding is not canonical JSON")
        return binding

    @classmethod
    def _authority_binding_bytes(cls, binding: StrategyCandidateAuthorityBinding) -> bytes:
        return cls._canonical_json_bytes(_authority_binding_payload(binding, mode="json"))

    @staticmethod
    def _model_bytes(model: RuntimeContractModel) -> bytes:
        return StrategyCandidateSnapshotSpool._canonical_json_bytes(model.model_dump(mode="json"))

    @staticmethod
    def _canonical_json_bytes(value: object) -> bytes:
        payload = json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(payload) > _MAX_AUTHORITY_BYTES:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate authority payload exceeds size limit"
            )
        return payload

    @classmethod
    def _read_regular_file(cls, parent_fd: int, name: str, *, label: str) -> bytes:
        descriptor = -1
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise StrategyCandidateSnapshotIntegrityError(f"{label} cannot be a symlink")
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            cls._validate_private_file(opened, label=label)
            if not cls._same_file(before, opened) or not cls._same_file(opened, active):
                raise StrategyCandidateSnapshotIntegrityError(f"{label} identity changed")
            if opened.st_size > _MAX_AUTHORITY_BYTES:
                raise StrategyCandidateSnapshotIntegrityError(f"{label} exceeds size limit")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                payload = stream.read(_MAX_AUTHORITY_BYTES + 1)
            after = os.fstat(descriptor)
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if len(payload) > _MAX_AUTHORITY_BYTES:
                raise StrategyCandidateSnapshotIntegrityError(f"{label} exceeds size limit")
            if not cls._same_file(opened, after) or not cls._same_file(after, current):
                raise StrategyCandidateSnapshotIntegrityError(f"{label} changed while being read")
            if (opened.st_size, opened.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise StrategyCandidateSnapshotIntegrityError(f"{label} changed while being read")
            return payload
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(f"{label} is missing or unsafe") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _entry_exists(parent_fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def _generation_name(content_sha256: str) -> str:
        if not _SHA256_PATTERN.fullmatch(content_sha256):
            raise StrategyCandidateSnapshotIntegrityError("generation content hash is invalid")
        return f"{content_sha256}.json"

    @classmethod
    def _write_temporary(cls, parent_fd: int, name: str, payload: bytes) -> None:
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
            dir_fd=parent_fd,
        )
        try:
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @classmethod
    def _atomic_create_generation(
        cls,
        root_fd: int,
        generations_fd: int,
        target_name: str,
        payload: bytes,
    ) -> None:
        temporary_name = f".candidate-generation.{uuid4().hex}.tmp"
        try:
            cls._write_temporary(root_fd, temporary_name, payload)
            try:
                os.link(
                    temporary_name,
                    target_name,
                    src_dir_fd=root_fd,
                    dst_dir_fd=generations_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise StrategyCandidateSnapshotIntegrityError(
                    "immutable generation already exists"
                ) from exc
            os.fsync(generations_fd)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_fd)
            os.fsync(root_fd)

    @classmethod
    def _atomic_create_authority_binding(cls, root_fd: int, payload: bytes) -> None:
        temporary_name = f".authority.{uuid4().hex}.tmp"
        try:
            cls._write_temporary(root_fd, temporary_name, payload)
            try:
                os.link(
                    temporary_name,
                    "authority.json",
                    src_dir_fd=root_fd,
                    dst_dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy candidate authority binding already exists"
                ) from exc
            os.fsync(root_fd)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_fd)
            os.fsync(root_fd)

    @classmethod
    def _atomic_replace_pointer(cls, root_fd: int, payload: bytes) -> None:
        temporary_name = f".current.{uuid4().hex}.tmp"
        try:
            cls._write_temporary(root_fd, temporary_name, payload)
            os.replace(
                temporary_name,
                "current.json",
                src_dir_fd=root_fd,
                dst_dir_fd=root_fd,
            )
            os.fsync(root_fd)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_fd)

    @classmethod
    def _atomic_replace_generation_index(cls, root_fd: int, payload: bytes) -> None:
        temporary_name = f".generation-index.{uuid4().hex}.tmp"
        try:
            cls._write_temporary(root_fd, temporary_name, payload)
            os.replace(
                temporary_name,
                "generation-index.json",
                src_dir_fd=root_fd,
                dst_dir_fd=root_fd,
            )
            os.fsync(root_fd)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_fd)

    @classmethod
    def _cleanup_stale_temporaries(cls, root_fd: int) -> None:
        pattern = re.compile(
            r"^\.(?:authority|candidate-generation|current|generation-index)"
            r"\.[0-9a-f]{32}\.tmp$"
        )
        with os.scandir(root_fd) as entries:
            for entry in entries:
                if pattern.fullmatch(entry.name) is None:
                    continue
                observed = os.stat(entry.name, dir_fd=root_fd, follow_symlinks=False)
                if (
                    not stat.S_ISREG(observed.st_mode)
                    or observed.st_uid != os.getuid()
                    or observed.st_nlink not in {1, 2}
                    or stat.S_IMODE(observed.st_mode) != _PRIVATE_FILE_MODE
                ):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "stale publish temporary is unsafe"
                    )
                os.unlink(entry.name, dir_fd=root_fd)
        os.fsync(root_fd)
