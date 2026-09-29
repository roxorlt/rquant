"""Run one admitted retrospective factor evaluation into a verified artifact."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from rquant.factor.historical_adapter import (
    HistoricalFactorResearch,
    assemble_historical_factor_research,
)
from rquant.factor.job_spec import FactorEvaluationJobSpec
from rquant.factor.result_artifact import (
    FactorResearchArtifactReceipt,
    load_factor_research_artifact,
    publish_factor_research_artifact,
)
from rquant.factor_snapshot_admission import (
    FactorSnapshotAdmissionDecision,
    FactorSnapshotMetadataStore,
    open_factor_snapshot_admission,
)

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class FactorEvaluationCompletion(BaseModel):
    """A local completion claim, without a persistent Lab uniqueness guarantee."""

    model_config = _IMMUTABLE

    spec_sha256: Sha256
    artifact_sha256: Sha256
    artifact_filename: str
    artifact_byte_count: int = Field(gt=0, strict=True)
    result_sha256: Sha256
    source_sha256: Sha256
    snapshot_id: Sha256
    binding_hash: Sha256
    snapshot_as_of_time: AwareDatetime
    source_mode: Literal["historical_retrospective"]
    source_read_boundary: Literal["single_snapshot_transaction"]
    visibility_basis: Literal["retrospective_adapter_assumption"]
    research_status: Literal["exploratory"]
    result_kind: Literal["research_diagnostic"]
    code_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    completed_at: AwareDatetime

    @field_validator("snapshot_as_of_time", "completed_at")
    @classmethod
    def _utc_time(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)


def _clock_utc(now: Callable[[], datetime]) -> datetime:
    current = now()
    if not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("factor runner requires an aware UTC clock")
    return current.astimezone(UTC)


def _before_deadline(now: Callable[[], datetime], deadline: datetime) -> None:
    if _clock_utc(now) >= deadline:
        raise TimeoutError("factor evaluation deadline has passed")


def _checked_decision(
    decision: FactorSnapshotAdmissionDecision, spec: FactorEvaluationJobSpec
) -> FactorSnapshotAdmissionDecision:
    checked = FactorSnapshotAdmissionDecision.model_validate(decision)
    admission = spec.admission_request
    if (
        not checked.allowed
        or checked.as_of_time is None
        or checked.snapshot_id != admission.snapshot_id
        or checked.binding_hash != admission.binding_hash
        or checked.source_mode != admission.source_mode
        or checked.source_read_boundary != "single_snapshot_transaction"
        or checked.research_status != spec.research_status
    ):
        raise ValueError("factor admission decision differs from the job spec")
    return checked


def _checked_research(
    research: HistoricalFactorResearch,
    decision: FactorSnapshotAdmissionDecision,
    spec: FactorEvaluationJobSpec,
) -> HistoricalFactorResearch:
    checked = HistoricalFactorResearch.model_validate(research)
    source = checked.receipt
    adapter = spec.adapter_request
    if (
        source.snapshot_id != decision.snapshot_id
        or source.binding_hash != decision.binding_hash
        or source.snapshot_as_of_time != decision.as_of_time
        or source.source_mode != spec.admission_request.source_mode
        or source.source_read_boundary != decision.source_read_boundary
        or source.result_kind != spec.result_kind
        or source.pool_basis != adapter.pool_basis
        or source.stock_codes != adapter.stock_codes
        or source.query_start_date != adapter.query_start_date
        or source.query_end_date != adapter.query_end_date
        or source.evaluation_days != adapter.evaluation_days
        or checked.request.factor_input.definition != adapter.definition
        or checked.request.factor_input.universe != adapter.stock_codes
        or checked.request.evaluation_days != adapter.evaluation_days
        or checked.request.holding_sessions != adapter.holding_sessions
        or checked.request.as_of != adapter.as_of
    ):
        raise ValueError("factor research differs from the admitted job source")
    return checked


def run_factor_evaluation_job(
    spec: FactorEvaluationJobSpec,
    *,
    metadata_store: FactorSnapshotMetadataStore,
    lake_root: Path,
    artifact_root: Path,
    now: Callable[[], datetime],
) -> FactorEvaluationCompletion:
    """Compute within one admitted lease and seal only the verified result."""
    checked = FactorEvaluationJobSpec.model_validate(
        spec.model_dump(mode="python", round_trip=True)
    )
    _before_deadline(now, checked.deadline)
    with open_factor_snapshot_admission(
        checked.admission_request, metadata_store=metadata_store, lake_root=lake_root
    ) as (lease, decision):
        admitted = _checked_decision(decision, checked)
        research = _checked_research(
            assemble_historical_factor_research(lease, admitted, checked.adapter_request),
            admitted,
            checked,
        )
    _before_deadline(now, checked.deadline)
    receipt = FactorResearchArtifactReceipt.model_validate(
        publish_factor_research_artifact(research, checked.code_revision, artifact_root)
    )
    reloaded = load_factor_research_artifact(artifact_root, receipt.sha256)
    if (
        reloaded.content_sha256 != receipt.sha256
        or reloaded.code_revision != checked.code_revision
        or reloaded.research != research
    ):
        raise ValueError("reloaded factor artifact differs from this evaluation")
    source = reloaded.research.receipt
    return FactorEvaluationCompletion(
        spec_sha256=checked.spec_sha256,
        artifact_sha256=receipt.sha256,
        artifact_filename=receipt.filename,
        artifact_byte_count=receipt.byte_count,
        result_sha256=reloaded.research.result.sha256,
        source_sha256=source.source_sha256,
        snapshot_id=source.snapshot_id,
        binding_hash=source.binding_hash,
        snapshot_as_of_time=source.snapshot_as_of_time,
        source_mode=source.source_mode,
        source_read_boundary=source.source_read_boundary,
        visibility_basis=source.visibility_basis,
        research_status=checked.research_status,
        result_kind=checked.result_kind,
        code_revision=checked.code_revision,
        completed_at=_clock_utc(now),
    )
