"""Pure point-in-time join between common minute features and one strategy universe."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from types import MappingProxyType
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import (
    Field,
    StringConstraints,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
)
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthorityEvidence,
    CandidateUniverseHitEvidence,
    RuntimeCandidateUniverseResult,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidateStaticFeatureSemantic,
    candidate_occurrence_id,
    serialize_candidate_static_features,
    strategy_candidate_schema_fingerprint,
    validate_candidate_static_features_against_schema,
)
from rquant.strategy_runner import canonical_feature_payload

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_RESERVED_COLUMNS = frozenset(
    {
        "candidate_occurrence_id",
        "candidate_effective_trade_date",
        "candidate_variant",
        "candidate_generation_sha256",
        "candidate_snapshot_schema_version",
    }
)
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class StrategyCandidateFeatureJoinError(RuntimeError):
    """Raised when PIT evidence cannot be joined without weakening its contract."""


class StrategyCandidateFeatureAuthority(RuntimeContractModel):
    """Exact candidate snapshot authority bound into a joined feature payload."""

    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    schema_version: Literal[3]
    generation_sha256: Sha256
    authority_binding_sha256: Sha256
    definition_fingerprint: Sha256
    executable_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
    static_feature_names: tuple[str, ...] = Field(min_length=1)
    static_feature_schema: Mapping[str, StrategyCandidateStaticFeatureSemantic]
    captured_at: AwareUtcDatetime

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

    @model_validator(mode="after")
    def validate_static_schema(self) -> StrategyCandidateFeatureAuthority:
        if self.static_feature_names != tuple(self.static_feature_schema):
            raise ValueError("candidate authority static names must exactly match its schema")
        if self.candidate_schema_fingerprint != strategy_candidate_schema_fingerprint(
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            static_feature_schema=self.static_feature_schema,
        ):
            raise ValueError("candidate authority schema fingerprint does not match")
        return self

    @property
    def input_id(self) -> str:
        return f"candidate-authority:{canonical_sha256(self.model_dump(mode='python'))}"


def _canonical_json(payload: object) -> str:
    return json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _validate_flat_rows(
    rows: object,
    *,
    columns: tuple[str, ...],
    nested_columns: tuple[str, ...] = (),
) -> list[dict[str, object]]:
    if not isinstance(rows, list):
        raise ValueError("payload rows must be a list")
    expected = set(columns)
    validated: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != expected:
            raise ValueError("payload row columns do not match declared columns")
        if any(
            isinstance(value, (dict, list)) and name not in nested_columns
            for name, value in row.items()
        ):
            raise ValueError("only declared static features may contain nested values")
        validated.append(dict(row))
    return validated


class StrategyCandidateFeatureBatch(RuntimeContractModel):
    """Serializable joined feature payload plus its immutable batch envelope."""

    envelope: FeatureBatchEnvelope
    common_batch_id: str = Field(min_length=1)
    candidate_authority: StrategyCandidateFeatureAuthority
    static_feature_names: tuple[str, ...]
    columns: tuple[str, ...] = Field(min_length=1)
    payload_json: str = Field(min_length=1)

    @field_validator("static_feature_names")
    @classmethod
    def validate_static_feature_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not name for name in value):
            raise ValueError("static feature names cannot contain empty values")
        if value != tuple(sorted(set(value))):
            raise ValueError("static feature names must be sorted and unique")
        return value

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not column for column in value):
            raise ValueError("joined feature columns cannot contain empty names")
        if value != tuple(sorted(set(value))):
            raise ValueError("joined feature columns must be sorted and unique")
        if "ts_code" not in value:
            raise ValueError("joined feature columns require ts_code")
        missing_reserved = _RESERVED_COLUMNS - set(value)
        if missing_reserved:
            raise ValueError(
                f"joined feature columns require reserved metadata: {sorted(missing_reserved)!r}"
            )
        return value

    @model_validator(mode="after")
    def validate_payload(self) -> StrategyCandidateFeatureBatch:
        try:
            decoded = json.loads(self.payload_json)
        except json.JSONDecodeError as exc:
            raise ValueError("joined feature payload must be valid JSON") from exc
        if not isinstance(decoded, dict) or set(decoded) != {
            "candidate_authority",
            "columns",
            "common_batch_id",
            "rows",
            "schema_version",
            "static_feature_names",
        }:
            raise ValueError("joined feature payload has an invalid shape")
        if decoded["schema_version"] != self.envelope.schema_version:
            raise ValueError("joined payload schema does not match its envelope")
        if decoded["common_batch_id"] != self.common_batch_id:
            raise ValueError("joined payload common_batch_id does not match its output contract")
        if decoded["static_feature_names"] != list(self.static_feature_names):
            raise ValueError("joined payload static feature names do not match its output contract")
        if self.static_feature_names != self.candidate_authority.static_feature_names:
            raise ValueError("joined static feature names do not match candidate authority schema")
        if not set(self.static_feature_names).issubset(self.columns):
            raise ValueError("joined columns do not contain the complete static feature schema")
        if decoded["columns"] != list(self.columns):
            raise ValueError("joined payload columns do not match its output contract")
        if decoded["candidate_authority"] != self.candidate_authority.model_dump(mode="json"):
            raise ValueError(
                "joined payload candidate authority does not match its output contract"
            )
        rows = _validate_flat_rows(
            decoded["rows"],
            columns=self.columns,
            nested_columns=self.static_feature_names,
        )
        for row in rows:
            validate_candidate_static_features_against_schema(
                static_features={name: row[name] for name in self.static_feature_names},
                static_feature_schema=self.candidate_authority.static_feature_schema,
            )
            if (
                row["candidate_generation_sha256"] != self.candidate_authority.generation_sha256
                or row["candidate_snapshot_schema_version"]
                != self.candidate_authority.schema_version
            ):
                raise ValueError("joined row candidate authority metadata does not match")
            try:
                effective_trade_date = date.fromisoformat(row["candidate_effective_trade_date"])
                expected_occurrence = candidate_occurrence_id(
                    strategy_id=self.candidate_authority.strategy_id,
                    strategy_version=self.candidate_authority.strategy_version,
                    candidate_id=row["ts_code"],
                    variant=row["candidate_variant"],
                    effective_trade_date=effective_trade_date,
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("joined row candidate metadata is invalid") from exc
            if row["candidate_occurrence_id"] != expected_occurrence:
                raise ValueError("joined row candidate occurrence metadata does not match")
        if _canonical_json(decoded) != self.payload_json:
            raise ValueError("joined feature payload must use canonical JSON")
        frame = pd.DataFrame(rows, columns=self.columns, dtype="object")
        canonical_rows = json.loads(_canonical_json(frame.to_dict(orient="records")))
        if canonical_rows != rows:
            raise ValueError("joined feature rows cannot be restored losslessly")
        if self.envelope.row_count != len(rows):
            raise ValueError("joined feature row count does not match its envelope")
        if hashlib.sha256(self.payload_bytes).hexdigest() != self.envelope.content_hash:
            raise ValueError("joined feature payload hash does not match its envelope")
        if self.candidate_authority.captured_at > self.envelope.available_at:
            raise ValueError("candidate authority capture exceeds batch available_at")
        if self.envelope.input_batch_ids != tuple(sorted(self.envelope.input_batch_ids)):
            raise ValueError("joined feature input lineage must be sorted")
        if self.candidate_authority.input_id not in self.envelope.input_batch_ids:
            raise ValueError("candidate authority input is missing from envelope lineage")
        if self.common_batch_id not in self.envelope.input_batch_ids:
            raise ValueError("common parent input is missing from envelope lineage")
        candidate_ids = tuple(row["ts_code"] for row in rows)
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("joined feature rows require unique candidate ids")
        expected_static_statuses = {
            (candidate_id, name)
            for candidate_id in candidate_ids
            for name in self.static_feature_names
        }
        static_statuses = tuple(
            status
            for status in self.envelope.field_statuses
            if status.name in self.static_feature_names
        )
        if {(status.candidate_id, status.name) for status in static_statuses} != (
            expected_static_statuses
        ) or len(static_statuses) != len(expected_static_statuses):
            raise ValueError("static feature statuses must exactly cover every candidate")
        if any(
            status.status is not FeatureAvailability.AVAILABLE
            or status.source_event_time != self.candidate_authority.captured_at
            or status.available_at != self.candidate_authority.captured_at
            or status.actual_delay_seconds != 0.0
            for status in static_statuses
        ):
            raise ValueError("static feature status does not bind authority capture")
        expected_batch_id = _joined_batch_id(
            common_batch_id=self.common_batch_id,
            authority=self.candidate_authority,
            joined_content_hash=self.envelope.content_hash,
            input_batch_ids=self.envelope.input_batch_ids,
        )
        if self.envelope.batch_id != expected_batch_id:
            raise ValueError("joined feature envelope batch_id does not bind its lineage")
        return self

    @property
    def payload_bytes(self) -> bytes:
        return self.payload_json.encode("utf-8")

    @property
    def frame(self) -> pd.DataFrame:
        decoded = json.loads(self.payload_json)
        rows = _validate_flat_rows(
            decoded["rows"],
            columns=self.columns,
            nested_columns=self.static_feature_names,
        )
        return pd.DataFrame(rows, columns=self.columns, dtype="object")


def _joined_batch_id(
    *,
    common_batch_id: str,
    authority: StrategyCandidateFeatureAuthority,
    joined_content_hash: str,
    input_batch_ids: tuple[str, ...],
) -> str:
    return canonical_sha256(
        {
            "common_batch_id": common_batch_id,
            "candidate_authority": authority,
            "joined_content_hash": joined_content_hash,
            "input_batch_ids": input_batch_ids,
        }
    )


def _requested_authority(
    universe: RuntimeCandidateUniverseResult,
    *,
    strategy_id: str,
    strategy_version: str,
) -> CandidateUniverseAuthorityEvidence:
    matches = tuple(
        authority
        for authority in universe.authorities
        if (authority.strategy_id, authority.strategy_version) == (strategy_id, strategy_version)
    )
    if len(matches) != 1:
        raise StrategyCandidateFeatureJoinError(
            f"requested strategy authority must resolve exactly once: "
            f"{strategy_id}@{strategy_version}"
        )
    return matches[0]


def _requested_hits(
    universe: RuntimeCandidateUniverseResult,
    *,
    strategy_id: str,
    strategy_version: str,
    authority: CandidateUniverseAuthorityEvidence,
) -> dict[str, CandidateUniverseHitEvidence]:
    hits: dict[str, CandidateUniverseHitEvidence] = {}
    for code_evidence in universe.code_evidence:
        for hit in code_evidence.hits:
            if (hit.strategy_id, hit.strategy_version) != (strategy_id, strategy_version):
                continue
            if hit.schema_version != authority.schema_version:
                raise StrategyCandidateFeatureJoinError(
                    "candidate hit schema does not match requested authority"
                )
            if hit.generation_sha256 != authority.generation_sha256:
                raise StrategyCandidateFeatureJoinError(
                    "candidate hit generation does not match requested authority"
                )
            if hit.candidate_id in hits:
                raise StrategyCandidateFeatureJoinError(
                    "requested authority contains duplicate candidate hits"
                )
            hits[hit.candidate_id] = hit
    if len(hits) != authority.row_count or tuple(sorted(hits)) != authority.codes:
        raise StrategyCandidateFeatureJoinError(
            "requested authority candidate evidence is incomplete"
        )
    return hits


def _static_feature_keys(
    hits: Mapping[str, CandidateUniverseHitEvidence],
    *,
    expected_keys: tuple[str, ...],
    static_feature_schema: Mapping[str, StrategyCandidateStaticFeatureSemantic],
    common_columns: set[str],
    common_statuses: set[str],
) -> tuple[str, ...]:
    key_sets = {tuple(sorted(hit.static_features)) for hit in hits.values()}
    if key_sets and key_sets != {expected_keys}:
        raise StrategyCandidateFeatureJoinError(
            "candidate hit static features do not match the authority schema"
        )
    keys = expected_keys
    for hit in hits.values():
        try:
            validate_candidate_static_features_against_schema(
                static_features=hit.static_features,
                static_feature_schema=static_feature_schema,
            )
        except ValueError as exc:
            raise StrategyCandidateFeatureJoinError(
                f"candidate static feature dtype validation failed: {exc}"
            ) from exc
    reserved_overlap = set(keys) & _RESERVED_COLUMNS
    if reserved_overlap:
        raise StrategyCandidateFeatureJoinError(
            f"static feature uses a reserved column: {sorted(reserved_overlap)!r}"
        )
    common_overlap = set(keys) & (common_columns | common_statuses)
    if common_overlap:
        raise StrategyCandidateFeatureJoinError(
            f"static feature collides with common columns: {sorted(common_overlap)!r}"
        )
    return keys


def _validate_pit_identity(
    common_envelope: FeatureBatchEnvelope,
    universe: RuntimeCandidateUniverseResult,
) -> None:
    if universe.as_of != common_envelope.available_at:
        raise StrategyCandidateFeatureJoinError(
            "candidate universe as_of must exactly equal common available_at"
        )
    event_trade_date = common_envelope.event_time.astimezone(_SHANGHAI).date()
    if universe.required_trade_date != event_trade_date:
        raise StrategyCandidateFeatureJoinError(
            "candidate universe trade date does not match common event trade date"
        )
    if universe.expected_commit != common_envelope.producer_commit:
        raise StrategyCandidateFeatureJoinError(
            "candidate universe commit does not match common producer commit"
        )


def join_strategy_candidate_features(
    common_envelope: FeatureBatchEnvelope,
    common_frame: pd.DataFrame,
    universe: RuntimeCandidateUniverseResult,
    strategy_id: str,
    strategy_version: str,
) -> StrategyCandidateFeatureBatch:
    """Join one immutable candidate authority to a common minute feature batch."""

    try:
        common_envelope = FeatureBatchEnvelope.model_validate(common_envelope)
        universe = RuntimeCandidateUniverseResult.model_validate(universe)
    except (TypeError, ValidationError) as exc:
        raise StrategyCandidateFeatureJoinError(
            f"input contract revalidation failed: {exc}"
        ) from exc
    if not strategy_id.strip() or not strategy_version.strip():
        raise ValueError("strategy identity cannot be empty")

    _validate_pit_identity(common_envelope, universe)
    try:
        common_payload = canonical_feature_payload(
            common_frame,
            schema_version=common_envelope.schema_version,
        )
    except (TypeError, ValueError) as exc:
        raise StrategyCandidateFeatureJoinError(f"invalid common feature frame: {exc}") from exc
    if common_envelope.row_count != len(common_frame):
        raise StrategyCandidateFeatureJoinError(
            "common frame row count does not match common envelope"
        )
    if hashlib.sha256(common_payload).hexdigest() != common_envelope.content_hash:
        raise StrategyCandidateFeatureJoinError(
            "common frame content_hash does not match common envelope"
        )

    authority = _requested_authority(
        universe,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
    )
    hits = _requested_hits(
        universe,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        authority=authority,
    )
    common_status_names = {status.name for status in common_envelope.field_statuses}
    common_columns = set(common_frame.columns)
    common_reserved_overlap = common_columns & _RESERVED_COLUMNS
    if common_reserved_overlap:
        raise StrategyCandidateFeatureJoinError(
            f"common frame uses reserved metadata columns: {sorted(common_reserved_overlap)!r}"
        )
    static_keys = _static_feature_keys(
        hits,
        expected_keys=authority.static_feature_names,
        static_feature_schema=authority.static_feature_schema,
        common_columns=common_columns,
        common_statuses=common_status_names,
    )

    common_rows = json.loads(common_payload)["rows"]
    joined_rows: list[dict[str, object]] = []
    for common_row in common_rows:
        code = common_row["ts_code"]
        hit = hits.get(code)
        if hit is None:
            continue
        row = dict(common_row)
        row.update(
            {
                "candidate_occurrence_id": hit.occurrence_id,
                "candidate_effective_trade_date": hit.effective_trade_date.isoformat(),
                "candidate_variant": hit.variant,
                "candidate_generation_sha256": hit.generation_sha256,
                "candidate_snapshot_schema_version": hit.schema_version,
            }
        )
        row.update(
            serialize_candidate_static_features(
                {key: hit.static_features[key] for key in static_keys}
            )
        )
        joined_rows.append(dict(sorted(row.items())))

    columns = tuple(sorted(common_columns | set(static_keys) | _RESERVED_COLUMNS))
    joined_frame = pd.DataFrame(joined_rows, columns=columns, dtype="object")
    canonical_joined_rows = json.loads(_canonical_json(joined_frame.to_dict(orient="records")))
    candidate_authority = StrategyCandidateFeatureAuthority(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        schema_version=authority.schema_version,
        generation_sha256=authority.generation_sha256,
        authority_binding_sha256=authority.authority_binding_sha256,
        definition_fingerprint=authority.definition_fingerprint,
        executable_fingerprint=authority.executable_fingerprint,
        candidate_schema_fingerprint=authority.candidate_schema_fingerprint,
        static_feature_names=authority.static_feature_names,
        static_feature_schema=authority.static_feature_schema,
        captured_at=authority.captured_at,
    )
    joined_payload = _canonical_json(
        {
            "schema_version": common_envelope.schema_version,
            "columns": list(columns),
            "common_batch_id": common_envelope.batch_id,
            "candidate_authority": candidate_authority.model_dump(mode="json"),
            "rows": canonical_joined_rows,
            "static_feature_names": list(static_keys),
        }
    ).encode("utf-8")
    joined_content_hash = hashlib.sha256(joined_payload).hexdigest()

    direct_parent_ids = (common_envelope.batch_id, candidate_authority.input_id)
    lineage = (*common_envelope.input_batch_ids, *direct_parent_ids)
    if len(lineage) != len(set(lineage)):
        raise StrategyCandidateFeatureJoinError(
            "joined feature direct parent lineage collides with common ancestors"
        )
    input_batch_ids = tuple(sorted(lineage))
    batch_id = _joined_batch_id(
        common_batch_id=common_envelope.batch_id,
        authority=candidate_authority,
        joined_content_hash=joined_content_hash,
        input_batch_ids=input_batch_ids,
    )

    static_available_at = authority.captured_at
    if static_available_at > common_envelope.available_at:
        raise StrategyCandidateFeatureJoinError(
            "static feature evidence is newer than common batch availability"
        )
    static_statuses = tuple(
        FeatureFieldStatus(
            candidate_id=candidate_id,
            name=key,
            status=FeatureAvailability.AVAILABLE,
            source_event_time=static_available_at,
            available_at=static_available_at,
            decision_cutoff=common_envelope.decision_cutoff,
            actual_delay_seconds=0.0,
        )
        for candidate_id in sorted(row["ts_code"] for row in canonical_joined_rows)
        for key in static_keys
    )
    envelope = FeatureBatchEnvelope(
        schema_version=common_envelope.schema_version,
        batch_id=batch_id,
        contract_id=common_envelope.contract_id,
        contract_version=common_envelope.contract_version,
        input_batch_ids=input_batch_ids,
        sequence=common_envelope.sequence,
        event_time=common_envelope.event_time,
        available_at=common_envelope.available_at,
        decision_cutoff=common_envelope.decision_cutoff,
        actual_delay_seconds=common_envelope.actual_delay_seconds,
        row_count=len(joined_rows),
        content_hash=joined_content_hash,
        field_statuses=(*common_envelope.field_statuses, *static_statuses),
        producer_commit=common_envelope.producer_commit,
    )
    try:
        return StrategyCandidateFeatureBatch(
            envelope=envelope,
            common_batch_id=common_envelope.batch_id,
            candidate_authority=candidate_authority,
            static_feature_names=static_keys,
            columns=columns,
            payload_json=joined_payload.decode("utf-8"),
        )
    except (TypeError, ValueError) as exc:
        raise StrategyCandidateFeatureJoinError(
            f"joined feature batch failed validation: {exc}"
        ) from exc


__all__ = [
    "StrategyCandidateFeatureAuthority",
    "StrategyCandidateFeatureBatch",
    "StrategyCandidateFeatureJoinError",
    "join_strategy_candidate_features",
]
