"""Pure read-only assembly of point-in-time strategy candidate universes."""

from __future__ import annotations

import os
import re
from collections import defaultdict
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal

from pydantic import (
    Field,
    JsonValue,
    StrictInt,
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
from rquant.strategy_candidate_snapshot import (
    StrategyCandidateAuthorityBinding,
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
    StrategyCandidateSnapshot,
    StrategyCandidateSnapshotIntegrityError,
    StrategyCandidateSnapshotSpool,
    StrategyCandidateStaticFeatureSemantic,
    candidate_occurrence_id,
    canonicalize_candidate_static_features,
    serialize_candidate_static_features,
    strategy_candidate_decision_trade_date,
    strategy_candidate_schema_fingerprint,
    strategy_candidate_snapshot_content_sha256,
    thaw_candidate_static_features,
    validate_candidate_static_features_against_schema,
)

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_TS_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")


class RuntimeCandidateUniverseIntegrityError(RuntimeError):
    """Raised when a candidate authority cannot be trusted at the requested time."""


class CandidateUniverseAuthority(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    snapshot_root: Path
    required: bool
    max_age_seconds: StrictInt = Field(gt=0)
    definition_fingerprint: Sha256
    executable_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
    static_feature_names: tuple[str, ...] = Field(min_length=1)
    static_feature_schema: Mapping[str, StrategyCandidateStaticFeatureSemantic]

    @field_validator("snapshot_root")
    @classmethod
    def require_absolute_normalized_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("candidate snapshot root must be absolute")
        normalized = Path(os.path.abspath(value))
        if value != normalized:
            raise ValueError("candidate snapshot root must be normalized without traversal")
        return value

    @model_validator(mode="after")
    def validate_static_semantic_binding(self) -> CandidateUniverseAuthority:
        if self.static_feature_names != tuple(sorted(set(self.static_feature_names))):
            raise ValueError("static feature names must be sorted and unique")
        if self.static_feature_names != tuple(self.static_feature_schema):
            raise ValueError("static feature names must exactly match the declared schema")
        if self.candidate_schema_fingerprint != strategy_candidate_schema_fingerprint(
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            static_feature_schema=self.static_feature_schema,
        ):
            raise ValueError("candidate schema fingerprint does not match static schema")
        return self

    @field_validator("static_feature_schema")
    @classmethod
    def freeze_static_feature_schema(
        cls,
        value: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    ) -> Mapping[str, StrategyCandidateStaticFeatureSemantic]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("static_feature_schema")
    def serialize_static_feature_schema(
        self,
        value: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    ) -> dict[str, dict[str, str]]:
        return {name: semantic.model_dump(mode="json") for name, semantic in value.items()}

    @property
    def identity(self) -> tuple[str, str]:
        return self.strategy_id, self.strategy_version


class RuntimeCandidateUniverseConfig(RuntimeContractModel):
    expected_commit: CommitSha
    authorities: tuple[CandidateUniverseAuthority, ...] = Field(min_length=1)

    @field_validator("authorities")
    @classmethod
    def canonicalize_authorities(
        cls,
        value: tuple[CandidateUniverseAuthority, ...],
    ) -> tuple[CandidateUniverseAuthority, ...]:
        identities = [authority.identity for authority in value]
        if len(identities) != len(set(identities)):
            raise ValueError("candidate universe contains a duplicate strategy authority")
        return tuple(sorted(value, key=lambda authority: authority.identity))


class CandidateUniverseAuthorityEvidence(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    schema_version: Literal[1, 2, 3]
    generation_sha256: Sha256
    authority_binding_sha256: Sha256
    definition_fingerprint: Sha256
    executable_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
    static_feature_names: tuple[str, ...] = Field(min_length=1)
    static_feature_schema: Mapping[str, StrategyCandidateStaticFeatureSemantic]
    source_snapshot_ids: Mapping[str, Sha256] = Field(default_factory=dict)
    sequence: int = Field(ge=0)
    row_count: int = Field(ge=0)
    captured_at: AwareUtcDatetime
    codes: tuple[str, ...]

    @field_validator("source_snapshot_ids")
    @classmethod
    def freeze_source_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not isinstance(key, str) or not key for key in value):
            raise ValueError("source_snapshot_ids keys must be non-empty strings")
        return MappingProxyType(dict(sorted(value.items())))

    @field_validator("static_feature_schema")
    @classmethod
    def freeze_static_feature_schema(
        cls,
        value: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    ) -> Mapping[str, StrategyCandidateStaticFeatureSemantic]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_snapshot_ids")
    def serialize_source_snapshot_ids(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @field_serializer("static_feature_schema")
    def serialize_static_feature_schema(
        self,
        value: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    ) -> dict[str, dict[str, str]]:
        return {name: semantic.model_dump(mode="json") for name, semantic in value.items()}

    @model_validator(mode="after")
    def validate_codes(self) -> CandidateUniverseAuthorityEvidence:
        if self.codes != tuple(sorted(set(self.codes))):
            raise ValueError("authority evidence codes must be sorted and unique")
        if self.row_count != len(self.codes):
            raise ValueError("authority row_count must equal its unique candidate codes")
        if self.schema_version != 3:
            raise ValueError("legacy candidate authority requires explicit schema v3 republish")
        if not self.source_snapshot_ids:
            raise ValueError("schema v3 authority evidence is incomplete")
        if self.static_feature_names != tuple(self.static_feature_schema):
            raise ValueError("authority evidence static names must exactly match its schema")
        if self.candidate_schema_fingerprint != strategy_candidate_schema_fingerprint(
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            static_feature_schema=self.static_feature_schema,
        ):
            raise ValueError("authority evidence candidate schema fingerprint does not match")
        return self


class CandidateUniverseHitEvidence(RuntimeContractModel):
    schema_version: Literal[1, 2, 3]
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    generation_sha256: Sha256
    candidate_id: str = Field(min_length=1)
    variant: str = Field(min_length=1)
    decision_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    effective_trade_date: date
    occurrence_id: Sha256
    static_features: Mapping[str, JsonValue]
    reference_trade_date: date
    price_basis: StrategyCandidatePriceBasis
    reference_snapshot_ids: Mapping[str, Sha256]

    @field_validator("static_features", mode="before")
    @classmethod
    def thaw_static_features_for_validation(cls, value: object) -> JsonValue:
        return thaw_candidate_static_features(value)

    @field_validator("static_features")
    @classmethod
    def freeze_static_features(cls, value: object) -> Mapping[str, JsonValue]:
        return canonicalize_candidate_static_features(value)

    @field_serializer("static_features")
    def serialize_static_features(
        self,
        value: Mapping[str, JsonValue],
    ) -> dict[str, JsonValue]:
        return serialize_candidate_static_features(value)

    @field_validator("reference_snapshot_ids", mode="before")
    @classmethod
    def thaw_reference_snapshot_ids(cls, value: object) -> object:
        return dict(value) if isinstance(value, Mapping) else value

    @field_validator("reference_snapshot_ids")
    @classmethod
    def freeze_reference_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not isinstance(key, str) or not key for key in value):
            raise ValueError("reference_snapshot_ids keys must be non-empty strings")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("reference_snapshot_ids")
    def serialize_reference_snapshot_ids(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_pit_order(self) -> CandidateUniverseHitEvidence:
        if self.schema_version != 3:
            raise ValueError("legacy candidate hit requires explicit schema v3 republish")
        if self.available_at < self.decision_at:
            raise ValueError("candidate hit available_at cannot precede decision_at")
        decision_trade_date = strategy_candidate_decision_trade_date(
            self.decision_at,
            legacy_utc_date_semantics=self.schema_version == 1,
        )
        if self.schema_version == 1 and self.effective_trade_date != decision_trade_date:
            raise ValueError("schema v1 decision date must equal effective_trade_date")
        if self.schema_version in {2, 3} and self.effective_trade_date < decision_trade_date:
            raise ValueError("candidate hit effective_trade_date precedes decision date")
        if self.reference_trade_date > decision_trade_date:
            raise ValueError("candidate hit reference_trade_date is a future reference")
        expected_occurrence = candidate_occurrence_id(
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            candidate_id=self.candidate_id,
            variant=self.variant,
            effective_trade_date=self.effective_trade_date,
        )
        if self.occurrence_id != expected_occurrence:
            raise ValueError("candidate hit occurrence_id does not bind semantic identity")
        return self


class CandidateUniverseCodeEvidence(RuntimeContractModel):
    code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    hits: tuple[CandidateUniverseHitEvidence, ...] = Field(min_length=1)

    @field_validator("hits")
    @classmethod
    def canonicalize_hits(
        cls,
        value: tuple[CandidateUniverseHitEvidence, ...],
    ) -> tuple[CandidateUniverseHitEvidence, ...]:
        canonical = tuple(
            sorted(
                value,
                key=lambda hit: (
                    hit.strategy_id,
                    hit.strategy_version,
                    hit.candidate_id,
                    hit.variant,
                    hit.effective_trade_date,
                    hit.occurrence_id,
                ),
            )
        )
        identities = [
            (
                hit.strategy_id,
                hit.strategy_version,
                hit.generation_sha256,
                hit.candidate_id,
                hit.variant,
                hit.effective_trade_date,
                hit.occurrence_id,
            )
            for hit in canonical
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("candidate code evidence contains duplicate hits")
        return canonical

    @model_validator(mode="after")
    def bind_hits_to_code(self) -> CandidateUniverseCodeEvidence:
        if any(hit.candidate_id != self.code for hit in self.hits):
            raise ValueError("candidate hit evidence does not match its code")
        return self


class CandidateUniverseDegradedAuthority(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    reason: Literal["missing", "not_visible"]


class RuntimeCandidateUniverseResult(RuntimeContractModel):
    as_of: AwareUtcDatetime
    required_trade_date: date
    expected_commit: CommitSha
    codes: tuple[str, ...]
    authorities: tuple[CandidateUniverseAuthorityEvidence, ...]
    degraded_optional_authorities: tuple[CandidateUniverseDegradedAuthority, ...]
    code_evidence: tuple[CandidateUniverseCodeEvidence, ...]
    content_fingerprint: Sha256

    @model_validator(mode="after")
    def validate_result(self) -> RuntimeCandidateUniverseResult:
        if not self.authorities:
            raise ValueError("candidate universe requires a successful authority")
        if self.codes != tuple(sorted(set(self.codes))):
            raise ValueError("candidate universe codes must be sorted and unique")
        evidence_codes = tuple(item.code for item in self.code_evidence)
        if evidence_codes != self.codes:
            raise ValueError("candidate universe code evidence must bind every code")
        authority_keys = [(item.strategy_id, item.strategy_version) for item in self.authorities]
        if authority_keys != sorted(set(authority_keys)):
            raise ValueError("successful authority evidence must be sorted and unique")
        degraded_keys = [
            (item.strategy_id, item.strategy_version) for item in self.degraded_optional_authorities
        ]
        if degraded_keys != sorted(set(degraded_keys)):
            raise ValueError("degraded authority evidence must be sorted and unique")
        if set(authority_keys) & set(degraded_keys):
            raise ValueError("an authority cannot be both successful and degraded")
        authority_by_key = {
            (item.strategy_id, item.strategy_version): item for item in self.authorities
        }
        if any(item.captured_at > self.as_of for item in self.authorities):
            raise ValueError("authority evidence cannot be captured after result as_of")
        hits_by_authority: defaultdict[tuple[str, str], list[CandidateUniverseHitEvidence]] = (
            defaultdict(list)
        )
        for code_item in self.code_evidence:
            for hit in code_item.hits:
                if not (hit.decision_at <= hit.available_at <= self.as_of):
                    raise ValueError("candidate hit evidence violates PIT visibility")
                if hit.effective_trade_date != self.required_trade_date:
                    raise ValueError(
                        "candidate hit effective trade date does not match result trade date"
                    )
                key = (hit.strategy_id, hit.strategy_version)
                authority = authority_by_key.get(key)
                if authority is None:
                    raise ValueError("candidate hit has no successful authority evidence")
                if hit.schema_version != authority.schema_version:
                    raise ValueError("candidate hit schema does not match its authority")
                if hit.generation_sha256 != authority.generation_sha256:
                    raise ValueError("candidate hit generation does not match its authority")
                if hit.available_at > authority.captured_at:
                    raise ValueError(
                        "candidate hit cannot become available after authority capture"
                    )
                hits_by_authority[key].append(hit)
        for key, authority in authority_by_key.items():
            hits = hits_by_authority[key]
            if len(hits) != authority.row_count:
                raise ValueError("authority row_count does not match candidate hits")
            if tuple(sorted(hit.candidate_id for hit in hits)) != authority.codes:
                raise ValueError("authority codes do not match candidate hits")
            for hit in hits:
                validate_candidate_static_features_against_schema(
                    static_features=hit.static_features,
                    static_feature_schema=authority.static_feature_schema,
                )
            rows = tuple(
                StrategyCandidateRecord(
                    strategy_id=hit.strategy_id,
                    strategy_version=hit.strategy_version,
                    candidate_id=hit.candidate_id,
                    variant=hit.variant,
                    decision_at=hit.decision_at,
                    available_at=hit.available_at,
                    effective_trade_date=hit.effective_trade_date,
                    reference_trade_date=hit.reference_trade_date,
                    price_basis=hit.price_basis,
                    static_features=hit.static_features,
                    reference_snapshot_ids=hit.reference_snapshot_ids,
                    legacy_utc_date_semantics=authority.schema_version == 1,
                )
                for hit in hits
            )
            authority_binding = None
            if authority.schema_version == 3:
                authority_binding = StrategyCandidateAuthorityBinding.create(
                    strategy_id=authority.strategy_id,
                    strategy_version=authority.strategy_version,
                    definition_fingerprint=authority.definition_fingerprint,
                    executable_fingerprint=authority.executable_fingerprint,
                    candidate_schema_fingerprint=authority.candidate_schema_fingerprint,
                    static_feature_schema=authority.static_feature_schema,
                )
                if authority_binding.content_sha256 != authority.authority_binding_sha256:
                    raise ValueError("candidate authority binding hash does not match identity")
            reconstructed_generation = strategy_candidate_snapshot_content_sha256(
                schema_version=authority.schema_version,
                sequence=authority.sequence,
                trade_date=self.required_trade_date,
                captured_at=authority.captured_at,
                producer_commit=self.expected_commit,
                rows=rows,
                authority_binding=authority_binding,
                source_snapshot_ids=authority.source_snapshot_ids,
            )
            if reconstructed_generation != authority.generation_sha256:
                raise ValueError(
                    "candidate authority generation does not bind reconstructed snapshot"
                )
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_fingerprint"}))
        if self.content_fingerprint != expected:
            raise ValueError("content_fingerprint does not bind the candidate universe")
        return self

    @classmethod
    def build(
        cls,
        *,
        as_of: datetime,
        required_trade_date: date,
        expected_commit: str,
        codes: tuple[str, ...],
        authorities: tuple[CandidateUniverseAuthorityEvidence, ...],
        degraded_optional_authorities: tuple[CandidateUniverseDegradedAuthority, ...],
        code_evidence: tuple[CandidateUniverseCodeEvidence, ...],
    ) -> RuntimeCandidateUniverseResult:
        identity = {
            "as_of": normalize_aware_utc(as_of),
            "required_trade_date": required_trade_date,
            "expected_commit": expected_commit,
            "codes": codes,
            "authorities": authorities,
            "degraded_optional_authorities": degraded_optional_authorities,
            "code_evidence": code_evidence,
        }
        return cls(**identity, content_fingerprint=canonical_sha256(identity))


class RuntimeCandidateUniverseLoader:
    """Resolve every authority afresh without mutating or caching its spool."""

    def __init__(self, config: RuntimeCandidateUniverseConfig) -> None:
        if not isinstance(config, RuntimeCandidateUniverseConfig):
            raise TypeError("config must be a RuntimeCandidateUniverseConfig")
        self._config = config
        self._spools: dict[tuple[str, str], StrategyCandidateSnapshotSpool] = {}

    def load(
        self,
        *,
        as_of: datetime,
        required_trade_date: date,
    ) -> RuntimeCandidateUniverseResult:
        normalized_as_of = normalize_aware_utc(as_of)
        authority_evidence: list[CandidateUniverseAuthorityEvidence] = []
        degraded: list[CandidateUniverseDegradedAuthority] = []
        hits_by_code: defaultdict[str, list[CandidateUniverseHitEvidence]] = defaultdict(list)

        for authority in self._config.authorities:
            snapshot = self._read_authority(
                authority,
                as_of=normalized_as_of,
                degraded=degraded,
            )
            if snapshot is None:
                continue
            self._validate_snapshot(
                authority,
                snapshot,
                as_of=normalized_as_of,
                required_trade_date=required_trade_date,
            )
            codes: list[str] = []
            for row in snapshot.rows:
                code = row.candidate_id
                codes.append(code)
                hits_by_code[code].append(
                    CandidateUniverseHitEvidence(
                        schema_version=snapshot.schema_version,
                        strategy_id=authority.strategy_id,
                        strategy_version=authority.strategy_version,
                        generation_sha256=snapshot.content_sha256,
                        candidate_id=row.candidate_id,
                        variant=row.variant,
                        decision_at=row.decision_at,
                        available_at=row.available_at,
                        effective_trade_date=row.effective_trade_date,
                        occurrence_id=row.occurrence_id,
                        static_features=row.static_features,
                        reference_trade_date=row.reference_trade_date,
                        price_basis=row.price_basis,
                        reference_snapshot_ids=row.reference_snapshot_ids,
                    )
                )
            authority_evidence.append(
                CandidateUniverseAuthorityEvidence(
                    strategy_id=authority.strategy_id,
                    strategy_version=authority.strategy_version,
                    schema_version=snapshot.schema_version,
                    generation_sha256=snapshot.content_sha256,
                    authority_binding_sha256=snapshot.authority_binding.content_sha256,
                    definition_fingerprint=snapshot.authority_binding.definition_fingerprint,
                    executable_fingerprint=snapshot.authority_binding.executable_fingerprint,
                    candidate_schema_fingerprint=(
                        snapshot.authority_binding.candidate_schema_fingerprint
                    ),
                    static_feature_names=snapshot.authority_binding.static_feature_names,
                    static_feature_schema=snapshot.authority_binding.static_feature_schema,
                    source_snapshot_ids=snapshot.source_snapshot_ids,
                    sequence=snapshot.sequence,
                    row_count=len(snapshot.rows),
                    captured_at=snapshot.captured_at,
                    codes=tuple(sorted(codes)),
                )
            )

        codes = tuple(sorted(hits_by_code))
        if not authority_evidence:
            raise RuntimeCandidateUniverseIntegrityError("candidate universe is empty")
        code_evidence = tuple(
            CandidateUniverseCodeEvidence(
                code=code,
                hits=tuple(hits_by_code[code]),
            )
            for code in codes
        )
        return RuntimeCandidateUniverseResult.build(
            as_of=normalized_as_of,
            required_trade_date=required_trade_date,
            expected_commit=self._config.expected_commit,
            codes=codes,
            authorities=tuple(authority_evidence),
            degraded_optional_authorities=tuple(degraded),
            code_evidence=code_evidence,
        )

    def _read_authority(
        self,
        authority: CandidateUniverseAuthority,
        *,
        as_of: datetime,
        degraded: list[CandidateUniverseDegradedAuthority],
    ) -> StrategyCandidateSnapshot | None:
        try:
            spool = self._spools.get(authority.identity)
            if spool is None:
                spool = StrategyCandidateSnapshotSpool(authority.snapshot_root)
                self._spools[authority.identity] = spool
        except (StrategyCandidateSnapshotIntegrityError, OSError, ValueError) as exc:
            raise self._error(authority, f"snapshot authority is damaged: {exc}") from exc
        try:
            authority.snapshot_root.lstat()
        except FileNotFoundError:
            return self._handle_unavailable(authority, reason="missing", degraded=degraded)
        except OSError as exc:
            raise self._error(authority, "snapshot root is unreadable") from exc

        try:
            snapshot = spool.read_strategy_as_of(
                as_of,
                strategy_id=authority.strategy_id,
                strategy_version=authority.strategy_version,
                definition_fingerprint=authority.definition_fingerprint,
                executable_fingerprint=authority.executable_fingerprint,
                candidate_schema_fingerprint=authority.candidate_schema_fingerprint,
                static_feature_schema=authority.static_feature_schema,
            )
        except (StrategyCandidateSnapshotIntegrityError, OSError, ValueError) as exc:
            raise self._error(authority, f"snapshot authority is damaged: {exc}") from exc
        if snapshot is None:
            return self._handle_unavailable(authority, reason="not_visible", degraded=degraded)
        return snapshot

    def _handle_unavailable(
        self,
        authority: CandidateUniverseAuthority,
        *,
        reason: Literal["missing", "not_visible"],
        degraded: list[CandidateUniverseDegradedAuthority],
    ) -> None:
        if authority.required:
            raise self._error(authority, f"required authority has no {reason} snapshot")
        degraded.append(
            CandidateUniverseDegradedAuthority(
                strategy_id=authority.strategy_id,
                strategy_version=authority.strategy_version,
                reason=reason,
            )
        )
        return None

    def _validate_snapshot(
        self,
        authority: CandidateUniverseAuthority,
        snapshot: StrategyCandidateSnapshot,
        *,
        as_of: datetime,
        required_trade_date: date,
    ) -> None:
        if snapshot.producer_commit != self._config.expected_commit:
            raise self._error(authority, "snapshot producer commit does not match expected commit")
        if snapshot.trade_date != required_trade_date:
            raise self._error(authority, "snapshot trade date does not match required trade date")
        age_seconds = (as_of - snapshot.captured_at).total_seconds()
        if age_seconds < 0:
            raise self._error(authority, "snapshot is not yet visible")
        if age_seconds > authority.max_age_seconds:
            raise self._error(authority, "snapshot is stale")
        binding = snapshot.authority_binding
        if snapshot.schema_version != 3 or binding is None or binding.schema_version != 3:
            raise self._error(authority, "legacy candidate authority requires schema v3 republish")
        if binding.definition_fingerprint != authority.definition_fingerprint:
            raise self._error(authority, "candidate definition fingerprint does not match")
        if binding.executable_fingerprint != authority.executable_fingerprint:
            raise self._error(authority, "candidate executable fingerprint does not match")
        if binding.candidate_schema_fingerprint != authority.candidate_schema_fingerprint:
            raise self._error(authority, "candidate schema fingerprint does not match")
        if binding.static_feature_names != authority.static_feature_names:
            raise self._error(authority, "candidate static feature names do not match")
        if binding.static_feature_schema != authority.static_feature_schema:
            raise self._error(authority, "candidate static feature schema does not match")
        for row in snapshot.rows:
            if (row.strategy_id, row.strategy_version) != authority.identity:
                raise self._error(authority, "candidate row authority identity mismatch")
            if row.available_at > as_of:
                raise self._error(authority, "candidate row is not yet available")
            if row.effective_trade_date != required_trade_date:
                raise self._error(
                    authority,
                    "candidate row effective trade date does not match required trade date",
                )
            if not _TS_CODE_PATTERN.fullmatch(row.candidate_id):
                raise self._error(authority, f"invalid A-share candidate code: {row.candidate_id}")
            if tuple(sorted(row.static_features)) != authority.static_feature_names:
                raise self._error(authority, "candidate static feature schema does not match")
            try:
                validate_candidate_static_features_against_schema(
                    static_features=row.static_features,
                    static_feature_schema=authority.static_feature_schema,
                )
            except ValueError as exc:
                raise self._error(
                    authority,
                    f"candidate static feature dtype does not match: {exc}",
                ) from exc

    @staticmethod
    def _error(
        authority: CandidateUniverseAuthority,
        message: str,
    ) -> RuntimeCandidateUniverseIntegrityError:
        return RuntimeCandidateUniverseIntegrityError(
            f"{authority.strategy_id}@{authority.strategy_version}: {message}"
        )


__all__ = [
    "CandidateUniverseAuthority",
    "CandidateUniverseAuthorityEvidence",
    "CandidateUniverseCodeEvidence",
    "CandidateUniverseDegradedAuthority",
    "CandidateUniverseHitEvidence",
    "RuntimeCandidateUniverseConfig",
    "RuntimeCandidateUniverseIntegrityError",
    "RuntimeCandidateUniverseLoader",
    "RuntimeCandidateUniverseResult",
]
