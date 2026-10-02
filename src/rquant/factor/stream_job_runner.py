"""File driven v2 execution; completion is a claim awaiting ledger preparation."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.job_runner import FactorEvaluationCompletion, _before_deadline, _clock_utc
from rquant.factor.member_stream import run_factor_stream_research_from_members
from rquant.factor.neutralization_context import (
    FactorNeutralizationSources,
    require_factor_neutralization_binding,
)
from rquant.factor.run_request import NeutralizationMode
from rquant.factor.stream_job_artifact import (
    FactorStreamArtifactReference,
    FactorStreamFullArtifact,
    FactorStreamJournalWriter,
    _with_digest,
    checked_from_full,
    project_factor_stream_display,
    publish_stream_artifact,
)
from rquant.factor.stream_job_spec import FactorStreamJobSpec
from rquant.factor.universe import Sha256
from rquant.research_snapshot import SnapshotMetadataStore
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads


class FactorStreamCompletion(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )
    schema_version: Literal[2]
    spec_sha256: Sha256
    artifact_sha256: Sha256
    artifact_filename: str
    artifact_byte_count: int = Field(gt=0, strict=True)
    display_artifact_sha256: Sha256
    display_artifact_filename: str
    display_artifact_byte_count: int = Field(gt=0, strict=True)
    result_sha256: Sha256
    source_sha256: Sha256
    snapshot_id: Sha256
    binding_hash: Sha256
    snapshot_as_of_time: AwareDatetime
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    source_read_boundary: Literal["single_snapshot_transaction"] = "single_snapshot_transaction"
    visibility_basis: Literal["retrospective_adapter_assumption"] = (
        "retrospective_adapter_assumption"
    )
    research_status: Literal["exploratory"] = "exploratory"
    result_kind: Literal["research_diagnostic"] = "research_diagnostic"
    code_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    completed_at: AwareDatetime
    neutralization: NeutralizationMode = Field(default="none", exclude_if=lambda v: v == "none")
    context: FactorNeutralizationSources | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @field_validator("snapshot_as_of_time", "completed_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _references(self) -> FactorStreamCompletion:
        _ = self.full_reference, self.display_reference
        require_factor_neutralization_binding(
            self.context,
            mode=self.neutralization,
            snapshot_id=self.snapshot_id,
            binding_hash=self.binding_hash,
            as_of=self.snapshot_as_of_time,
        )
        return self

    @property
    def full_reference(self) -> FactorStreamArtifactReference:
        return FactorStreamArtifactReference(
            kind="full",
            sha256=self.artifact_sha256,
            filename=self.artifact_filename,
            byte_count=self.artifact_byte_count,
        )

    @property
    def display_reference(self) -> FactorStreamArtifactReference:
        return FactorStreamArtifactReference(
            kind="display",
            sha256=self.display_artifact_sha256,
            filename=self.display_artifact_filename,
            byte_count=self.display_artifact_byte_count,
        )

    @property
    def display_status(self) -> Literal["available"]:
        return "available"


def decode_factor_completion_json(data: str) -> FactorEvaluationCompletion | FactorStreamCompletion:
    if not 0 < len(data.encode("utf-8")) <= 64 * 1024:
        raise ValueError("completion exceeds byte budget")
    payload = strict_canonical_json_loads(data)
    if not isinstance(payload, dict):
        raise ValueError("completion requires an object")
    if "schema_version" not in payload:
        # Legacy completion has no version field and remains byte compatible.
        return FactorEvaluationCompletion.model_validate_json(data)
    if type(payload["schema_version"]) is not int or payload["schema_version"] != 2:
        raise ValueError("unknown completion version")
    return FactorStreamCompletion.model_validate_json(canonical_json_bytes(payload))


def run_factor_stream_job(
    spec: FactorStreamJobSpec,
    *,
    metadata_store: SnapshotMetadataStore,
    lake_root: Path,
    member_root: Path,
    artifact_root: Path,
    now: Callable[[], datetime],
) -> FactorStreamCompletion:
    spec = FactorStreamJobSpec.model_validate(spec)
    if spec.daily_feature_lake_root is not None and lake_root != spec.daily_feature_lake_root:
        raise ValueError("stored daily execution lake differs from frozen original")
    _before_deadline(now, spec.deadline)
    journal = FactorStreamJournalWriter(artifact_root, spec)
    try:
        result = run_factor_stream_research_from_members(
            spec.adapter_request,
            member_root=member_root,
            member_archive=spec.member_archive,
            metadata_store=metadata_store,
            lake_root=lake_root,
            batch_observer=journal.consume,
        )
        _before_deadline(now, spec.deadline)
        manifest, reference = journal.finish(result)
        full = _with_digest(
            FactorStreamFullArtifact,
            dict(spec=spec, result=result, journal=manifest, journal_reference=reference),
        )
        full_ref = publish_stream_artifact(artifact_root, "full", full)
        display_ref = publish_stream_artifact(
            artifact_root, "display", project_factor_stream_display(full)
        )
        _before_deadline(now, spec.deadline)
        return checked_from_full(full, full_ref, display_ref, _clock_utc(now))
    finally:
        journal.close()
