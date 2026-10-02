"""Verified, bounded Lab-owned Serving rows for retrospective factor results."""

from __future__ import annotations

import base64
import binascii
import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from rquant.factor.display_artifact import (
    FactorDisplayArtifactV1,
    _load_factor_display_artifact_with_identity,
)
from rquant.factor.job_ledger import (
    FactorEvaluationJobLedger,
    FactorJobFailureCode,
    FactorJobRecord,
    FactorJobStatus,
    FactorLedgerIdentity,
)
from rquant.factor.stream_job_artifact import (
    FactorStreamDisplayArtifact,
    _load_factor_stream_display_with_identity,
)
from rquant.factor.stream_job_spec import FactorStreamJobSpec
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    _projection_json_bytes,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

FACTOR_RESULT_PROJECTION_TABLES = frozenset(
    {"factor_result_state", "factor_result_index", "factor_result_display"}
)
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_SHA = r"^[0-9a-f]{64}$"
_JOB = r"^[0-9a-f]{32}$"
_CHUNK_BYTES = 32 * 1024
_RAW_BUDGET = 4 * 1024 * 1024
_OWNER_BUDGET = 7 * 1024 * 1024
_MAX_DISPLAYS = 4
_Display = FactorDisplayArtifactV1 | FactorStreamDisplayArtifact


def _load_display(
    root: Path, digest: str, version: int
) -> tuple[_Display, tuple[int, ...], tuple[int, ...]]:
    if version == 1:
        return _load_factor_display_artifact_with_identity(root, digest)
    if version == 2:
        return _load_factor_stream_display_with_identity(root, digest)
    raise ValueError("unknown factor display version")


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


class FactorResultServingState(BaseModel):
    model_config = _IMMUTABLE

    status: Literal["empty", "populated"]
    job_count: int = Field(ge=0, le=50, strict=True)
    ledger_instance_id: str = Field(pattern=_JOB)
    snapshot_sha256: str = Field(pattern=_SHA)


class FactorResultIndexRow(BaseModel):
    model_config = _IMMUTABLE

    job_id: str = Field(pattern=_JOB)
    spec_sha256: str = Field(pattern=_SHA)
    factor_id: str
    factor_version: int = Field(ge=1, strict=True)
    definition_content_sha256: str = Field(pattern=_SHA)
    status: FactorJobStatus
    failure_code: FactorJobFailureCode | None
    updated_at: AwareDatetime
    as_of_time: AwareDatetime
    code_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_sha256: str | None = Field(default=None, pattern=_SHA)
    result_sha256: str | None = Field(default=None, pattern=_SHA)
    full_artifact_sha256: str | None = Field(default=None, pattern=_SHA)
    display_artifact_sha256: str | None = Field(default=None, pattern=_SHA)
    display_byte_count: int | None = Field(default=None, gt=0, strict=True)
    completion_sha256: str | None = Field(default=None, pattern=_SHA)
    display_status: Literal["not_ready", "display_unavailable", "not_published", "available"]

    @model_validator(mode="after")
    def _valid_status(self) -> FactorResultIndexRow:
        if self.status == "succeeded":
            if self.failure_code is not None or any(
                value is None
                for value in (
                    self.source_sha256,
                    self.result_sha256,
                    self.full_artifact_sha256,
                    self.completion_sha256,
                )
            ):
                raise ValueError("successful factor index lacks its completion identity")
            has_display = self.display_artifact_sha256 is not None
            if has_display != (self.display_byte_count is not None):
                raise ValueError("factor index display receipt is incomplete")
            if (
                self.display_status == "not_ready"
                or (self.display_status == "display_unavailable" and has_display)
                or (self.display_status in {"available", "not_published"} and not has_display)
            ):
                raise ValueError("factor index display state differs from its completion")
        elif (
            self.display_status != "not_ready"
            or self.source_sha256 is not None
            or self.display_artifact_sha256 is not None
            or self.display_byte_count is not None
            or self.result_sha256 is not None
            or self.full_artifact_sha256 is not None
            or self.completion_sha256 is not None
        ):
            raise ValueError("unfinished factor index claims completed research")
        if self.status == "failed" and self.failure_code is None:
            raise ValueError("failed factor index lacks a failure code")
        if self.status in {"queued", "running"} and self.failure_code is not None:
            raise ValueError("unfinished factor index claims a failure code")
        return self


class FactorResultDisplayChunk(BaseModel):
    model_config = _IMMUTABLE

    job_id: str = Field(pattern=_JOB)
    chunk_index: int = Field(ge=0, strict=True)
    chunk_count: int = Field(ge=1, le=128, strict=True)
    file_sha256: str = Field(pattern=_SHA)
    data_b64: str


def _display_binding(row: FactorResultIndexRow, display: _Display) -> None:
    display_sha = (
        display.content_sha256
        if display.schema_version == 1
        else hashlib.sha256(
            canonical_json_bytes(display.model_dump(mode="json", round_trip=True))
        ).hexdigest()
    )
    if (
        row.display_artifact_sha256 != display_sha
        or row.full_artifact_sha256 != display.full_artifact_sha256
        or row.result_sha256 != display.result_sha256
        or row.definition_content_sha256 != display.definition_content_sha256
        or row.factor_id != display.factor_id
        or row.factor_version != display.factor_version
        or row.code_revision != display.code_revision
        or row.source_sha256 != display.source_sha256
        or row.as_of_time != display.as_of
    ):
        raise ValueError("factor result display differs from its indexed research")


class FactorResultServingSnapshot(BaseModel):
    model_config = _IMMUTABLE

    available_at: AwareDatetime
    state: FactorResultServingState
    index: tuple[FactorResultIndexRow, ...] = Field(max_length=50)
    chunks: tuple[FactorResultDisplayChunk, ...] = Field(max_length=512)

    @property
    def displays(self) -> tuple[_Display, ...]:
        grouped: dict[str, list[FactorResultDisplayChunk]] = {}
        for chunk in self.chunks:
            grouped.setdefault(chunk.job_id, []).append(chunk)
        if set(grouped) != {row.job_id for row in self.index if row.display_status == "available"}:
            raise ValueError("factor result display group disagrees with the index")
        displays: list[_Display] = []
        total = 0
        for row in self.index:
            if row.display_status != "available":
                continue
            parts = grouped[row.job_id]
            if (
                len(parts) != parts[0].chunk_count
                or tuple(part.chunk_index for part in parts) != tuple(range(len(parts)))
                or len({part.chunk_count for part in parts}) != 1
            ):
                raise ValueError("factor result display chunks are not contiguous")
            if len({part.file_sha256 for part in parts}) != 1:
                raise ValueError("factor result display chunks mix files")
            decoded: list[bytes] = []
            for part in parts:
                try:
                    raw = base64.b64decode(part.data_b64, validate=True)
                except (binascii.Error, ValueError) as exc:
                    raise ValueError("factor result display chunk encoding is invalid") from exc
                if (
                    not 0 < len(raw) <= _CHUNK_BYTES
                    or base64.b64encode(raw).decode() != part.data_b64
                ):
                    raise ValueError("factor result display chunk encoding is invalid")
                decoded.append(raw)
            data = b"".join(decoded)
            total += len(data)
            if (
                total > _RAW_BUDGET
                or len(data) != row.display_byte_count
                or hashlib.sha256(data).hexdigest() != parts[0].file_sha256
            ):
                raise ValueError("factor result display file bytes differ")
            payload = strict_canonical_json_loads(data)
            if not isinstance(payload, dict) or type(payload.get("schema_version")) is not int:
                raise ValueError("factor display requires an explicit version")
            model = {1: FactorDisplayArtifactV1, 2: FactorStreamDisplayArtifact}.get(
                payload["schema_version"]
            )
            if model is None:
                raise ValueError("unknown factor display version")
            display = model.model_validate_json(data, strict=False)
            if data != canonical_json_bytes(display.model_dump(mode="json", round_trip=True)):
                raise ValueError("factor result display file is not canonical")
            _display_binding(row, display)
            displays.append(display)
        return tuple(displays)

    @model_validator(mode="after")
    def _valid_snapshot(self) -> FactorResultServingSnapshot:
        if self.available_at.utcoffset() != UTC.utcoffset(None):
            raise ValueError("factor result availability must be UTC")
        if self.state.job_count != len(self.index) or self.state.status != (
            "populated" if self.index else "empty"
        ):
            raise ValueError("factor result state count differs from index")
        if tuple(row.job_id for row in self.index) != tuple(
            row.job_id
            for row in sorted(
                self.index, key=lambda item: (item.updated_at, item.job_id), reverse=True
            )
        ) or len({row.job_id for row in self.index}) != len(self.index):
            raise ValueError("factor result index order or identity is invalid")
        if any(row.updated_at > self.available_at for row in self.index):
            raise ValueError("factor result index contains future jobs")
        chunk_keys = tuple((chunk.job_id, chunk.chunk_index) for chunk in self.chunks)
        if chunk_keys != tuple(sorted(set(chunk_keys))):
            raise ValueError("factor result display chunks are unordered or duplicated")
        if len(self.displays) > _MAX_DISPLAYS:
            raise ValueError("factor result display count exceeds four")
        content = {
            "available_at": self.available_at.isoformat(),
            "ledger_instance_id": self.state.ledger_instance_id,
            "status": self.state.status,
            "job_count": self.state.job_count,
            "index": tuple(row.model_dump(mode="json") for row in self.index),
            "chunks": tuple(row.model_dump(mode="json") for row in self.chunks),
        }
        if self.state.snapshot_sha256 != _digest(content):
            raise ValueError("factor result snapshot digest differs")
        return self


def _chunks(job_id: str, display: _Display) -> tuple[FactorResultDisplayChunk, ...]:
    data = canonical_json_bytes(display.model_dump(mode="json", round_trip=True))
    chunks = tuple(
        data[offset : offset + _CHUNK_BYTES] for offset in range(0, len(data), _CHUNK_BYTES)
    )
    file_sha256 = hashlib.sha256(data).hexdigest()
    return tuple(
        FactorResultDisplayChunk(
            job_id=job_id,
            chunk_index=index,
            chunk_count=len(chunks),
            file_sha256=file_sha256,
            data_b64=base64.b64encode(chunk).decode(),
        )
        for index, chunk in enumerate(chunks)
    )


def _index_row(record: FactorJobRecord) -> FactorResultIndexRow:
    spec = record.spec
    adapter = (
        spec.adapter_request.formula
        if isinstance(spec, FactorStreamJobSpec)
        else spec.adapter_request
    )
    completion = record.completion
    return FactorResultIndexRow(
        job_id=record.job_id,
        spec_sha256=record.spec_sha256,
        factor_id=adapter.definition.factor_id,
        factor_version=adapter.definition.version,
        definition_content_sha256=spec.definition_content_sha256,
        status=record.status,
        failure_code=record.failure_code,
        updated_at=record.updated_at,
        as_of_time=adapter.as_of,
        code_revision=spec.code_revision,
        source_sha256=None if completion is None else completion.source_sha256,
        result_sha256=None if completion is None else completion.result_sha256,
        full_artifact_sha256=None if completion is None else completion.artifact_sha256,
        display_artifact_sha256=None if completion is None else completion.display_artifact_sha256,
        display_byte_count=None if completion is None else completion.display_artifact_byte_count,
        completion_sha256=None
        if completion is None
        else _digest(completion.model_dump(mode="json")),
        display_status=(
            "not_ready"
            if completion is None
            else "display_unavailable"
            if completion.display_status == "display_unavailable"
            else "not_published"
        ),
    )


def _snapshot(
    *,
    available_at: datetime,
    ledger_instance_id: str,
    index: tuple[FactorResultIndexRow, ...],
    chunks: tuple[FactorResultDisplayChunk, ...],
) -> FactorResultServingSnapshot:
    status = "populated" if index else "empty"
    content = {
        "available_at": available_at.isoformat(),
        "ledger_instance_id": ledger_instance_id,
        "status": status,
        "job_count": len(index),
        "index": tuple(row.model_dump(mode="json") for row in index),
        "chunks": tuple(row.model_dump(mode="json") for row in chunks),
    }
    return FactorResultServingSnapshot(
        available_at=available_at,
        state=FactorResultServingState(
            status=status,
            job_count=len(index),
            ledger_instance_id=ledger_instance_id,
            snapshot_sha256=_digest(content),
        ),
        index=index,
        chunks=chunks,
    )


def _projections(snapshot: FactorResultServingSnapshot) -> tuple[ServingProjectionPayload, ...]:
    at = snapshot.available_at
    return (
        ServingProjectionPayload(
            table_name="factor_result_state",
            available_at=at,
            rows=({"status_key": "current", **snapshot.state.model_dump(mode="json")},),
        ),
        ServingProjectionPayload(
            table_name="factor_result_index",
            available_at=at,
            rows=tuple(row.model_dump(mode="json") for row in snapshot.index),
        ),
        ServingProjectionPayload(
            table_name="factor_result_display",
            available_at=at,
            rows=tuple(row.model_dump(mode="json") for row in snapshot.chunks),
        ),
    )


def _owner_bytes(projections: tuple[ServingProjectionPayload, ...]) -> int:
    generation = "0" * 64
    return sum(
        _projection_json_bytes(
            ServingProjectionInput.bind(
                projection, owner_dataset_id="lab_jobs", owner_generation_id=generation
            )
        )
        for projection in projections
    )


def project_factor_result_projections(
    identity: FactorLedgerIdentity,
    artifact_root: Path,
    *,
    available_at: datetime,
    other_projections: tuple[ServingProjectionPayload, ...] = (),
) -> tuple[ServingProjectionPayload, ...]:
    """Publish only pinned, twice-read jobs and verified compact files."""
    checked_identity = FactorLedgerIdentity.model_validate(identity)
    if any(item.table_name in FACTOR_RESULT_PROJECTION_TABLES for item in other_projections):
        raise ValueError("factor result projections are already present")
    if available_at.tzinfo is None or available_at.utcoffset() is None:
        raise ValueError("factor result observation requires a timezone")
    observed = available_at.astimezone(UTC)
    ledger = FactorEvaluationJobLedger.open_existing(checked_identity)
    records = ledger.list_recent_updated(limit=50)
    index = tuple(_index_row(record) for record in records)
    loaded: dict[str, tuple[_Display, tuple[int, ...], tuple[int, ...]]] = {}
    for record, row in zip(records, index, strict=True):
        completion = record.completion
        if completion is None or completion.display_status == "display_unavailable":
            continue
        v2 = isinstance(record.spec, FactorStreamJobSpec)
        source = record.spec.adapter_request.source if v2 else record.spec.admission_request
        adapter = record.spec.adapter_request.formula if v2 else record.spec.adapter_request
        display, root_identity, file_identity = _load_display(
            artifact_root, completion.display_artifact_sha256, record.spec.schema_version
        )
        filename = (
            f"factor-stream-display-v2-{completion.display_artifact_sha256}.json"
            if v2
            else f"factor-display-v1-{display.content_sha256}.json"
        )
        if (
            completion.display_artifact_filename != filename
            or completion.display_artifact_byte_count
            != len(canonical_json_bytes(display.model_dump(mode="json", round_trip=True)))
            or completion.spec_sha256 != record.spec_sha256
            or completion.code_revision != record.spec.code_revision
            or completion.snapshot_id != source.snapshot_id
            or completion.binding_hash != source.binding_hash
            or completion.source_mode != ("historical_retrospective" if v2 else source.source_mode)
            or completion.visibility_basis != display.visibility_basis
            or completion.snapshot_as_of_time != display.snapshot_as_of_time
            or display.snapshot_id != completion.snapshot_id
            or display.binding_hash != completion.binding_hash
            or display.source_mode != completion.source_mode
            or display.source_read_boundary != completion.source_read_boundary
            or display.definition_content_sha256 != record.spec.definition_content_sha256
        ):
            raise ValueError("factor display differs from its sealed completion")
        _display_binding(row, display)
        if (
            display.definition != adapter.definition
            or display.holding_sessions != record.spec.adapter_request.holding_sessions
            or completion.completed_at > observed
            or record.updated_at > observed
            or display.snapshot_as_of_time > observed
        ):
            raise ValueError("factor display differs from its exact job or observed time")
        if v2 and (
            display.neutralization != adapter.neutralization
            or completion.neutralization != adapter.neutralization
            or display.context != adapter.sources.context
            or completion.context != adapter.sources.context
        ):
            raise ValueError("factor display context differs from its original spec")
        if v2 and (
            display.mad_multiple != adapter.mad_multiple
            or (display.extended_statistics is not None)
            != record.spec.adapter_request.extended_statistics
            or (
                display.extended_statistics is not None
                and display.extended_statistics.ic_method != record.spec.adapter_request.ic_method
            )
        ):
            raise ValueError("factor display processing differs from its original spec")
        loaded[record.job_id] = display, root_identity, file_identity
    if ledger.list_recent_updated(limit=50) != records:
        raise ValueError("factor ledger changed during projection")
    for record in records:
        if record.job_id not in loaded:
            continue
        completion = record.completion
        repeated = _load_display(
            artifact_root, completion.display_artifact_sha256, record.spec.schema_version
        )
        if repeated != loaded[record.job_id]:
            raise ValueError("factor display file changed during projection")

    chunks: tuple[FactorResultDisplayChunk, ...] = ()
    selected_bytes = 0
    selected_count = 0
    selected_index = list(index)
    base = _projections(
        _snapshot(
            available_at=observed,
            ledger_instance_id=checked_identity.instance_id,
            index=index,
            chunks=(),
        )
    )
    if _owner_bytes((*other_projections, *base)) > _OWNER_BUDGET:
        raise ValueError("lab_jobs mandatory projections exceed 7 MiB")
    for offset, row in enumerate(index):
        if row.job_id not in loaded:
            continue
        display = loaded[row.job_id][0]
        data = canonical_json_bytes(display.model_dump(mode="json", round_trip=True))
        if selected_count >= _MAX_DISPLAYS or selected_bytes + len(data) > _RAW_BUDGET:
            continue
        candidate_index = list(selected_index)
        candidate_index[offset] = row.model_copy(update={"display_status": "available"})
        candidate_chunks = tuple(
            sorted(
                (*chunks, *_chunks(row.job_id, display)),
                key=lambda part: (part.job_id, part.chunk_index),
            )
        )
        candidate = _projections(
            _snapshot(
                available_at=observed,
                ledger_instance_id=checked_identity.instance_id,
                index=tuple(candidate_index),
                chunks=candidate_chunks,
            )
        )
        if _owner_bytes((*other_projections, *candidate)) > _OWNER_BUDGET:
            continue
        selected_index = candidate_index
        chunks = candidate_chunks
        selected_bytes += len(data)
        selected_count += 1
    result = _projections(
        _snapshot(
            available_at=observed,
            ledger_instance_id=checked_identity.instance_id,
            index=tuple(selected_index),
            chunks=chunks,
        )
    )
    validate_factor_result_projections({item.table_name: item for item in result})
    return result


def validate_factor_result_projections(
    projections: Mapping[str, ServingProjectionPayload | ServingProjectionInput],
) -> FactorResultServingSnapshot | None:
    """Reassemble and verify the complete three-table group from any producer."""
    present = FACTOR_RESULT_PROJECTION_TABLES & projections.keys()
    if not present:
        return None
    if present != FACTOR_RESULT_PROJECTION_TABLES:
        raise ValueError("factor result Serving projections are incomplete")
    selected = tuple(projections[name] for name in sorted(FACTOR_RESULT_PROJECTION_TABLES))
    if len({item.available_at for item in selected}) != 1:
        raise ValueError("factor result projections have different availability")
    bound = tuple(item for item in selected if isinstance(item, ServingProjectionInput))
    if bound and (
        len(bound) != 3
        or {item.owner_dataset_id for item in bound} != {"lab_jobs"}
        or len({item.owner_generation_id for item in bound}) != 1
    ):
        raise ValueError("factor result projections mix Serving generations")
    state_rows = projections["factor_result_state"].rows
    if len(state_rows) != 1 or state_rows[0]["status_key"] != "current":
        raise ValueError("factor result state must have one current row")
    state = FactorResultServingState.model_validate(
        {key: value for key, value in state_rows[0].items() if key != "status_key"}
    )
    index = tuple(
        FactorResultIndexRow.model_validate_json(canonical_json_bytes(dict(row)), strict=False)
        for row in projections["factor_result_index"].rows
    )
    chunks = tuple(
        FactorResultDisplayChunk.model_validate(dict(row))
        for row in projections["factor_result_display"].rows
    )
    return FactorResultServingSnapshot(
        available_at=selected[0].available_at,
        state=state,
        index=index,
        chunks=chunks,
    )


class FactorResultProjectionReader:
    """Lab-only adapter holding the external ledger identity and private file root."""

    def __init__(self, identity: FactorLedgerIdentity, artifact_root: Path) -> None:
        self.identity = FactorLedgerIdentity.model_validate(identity)
        self.artifact_root = Path(artifact_root)

    def __call__(
        self,
        observed_at: datetime,
        *,
        other_projections: tuple[ServingProjectionPayload, ...],
    ) -> tuple[ServingProjectionPayload, ...]:
        return project_factor_result_projections(
            self.identity,
            self.artifact_root,
            available_at=observed_at,
            other_projections=other_projections,
        )
