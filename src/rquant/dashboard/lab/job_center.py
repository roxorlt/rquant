"""Typed, side-effect-free Strategy Lab UI access to the durable Job Center."""

from __future__ import annotations

from datetime import datetime
from typing import Final
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.lab_artifact_export import (
    LabJobZipExportFacade,
    LabJobZipExportReceipt,
)
from rquant.lab_artifact_preview import ArtifactPreview, ArtifactPreviewReader
from rquant.lab_eta import (
    LabEtaEstimate,
    LabEtaInput,
    LabEtaRemainingShard,
    estimate_lab_eta,
)
from rquant.lab_job_center import (
    AuctionGapRunInput,
    CommandSubmissionResult,
    GrowthBoardSurgeRunInput,
    LabCommandSubmissionFacade,
    NShapeComparisonRunInput,
    NShapeOptimizationRunInput,
    ResearchJobSubmission,
    ResearchRunInput,
    build_research_job_submission,
)
from rquant.lab_jobs import (
    LabJobDetail,
    LabJobListFilters,
    LabJobPage,
    LabJobReader,
)
from rquant.research_gate import ResearchGateDecision
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    ResourceClass,
)
from rquant.strategy_job_adapters import (
    build_adapter_execution_contract,
    default_strategy_job_adapter_registry,
)

LAB_UI_JOB_PAGE_SIZES: Final[frozenset[int]] = frozenset({20, 25})
LAB_UI_SHARD_LIMIT: Final = 64
LAB_UI_EVENT_LIMIT: Final = 100
LAB_UI_ARTIFACT_LIMIT: Final = 32
LAB_UI_COMPLETED_TELEMETRY_LIMIT: Final = 64
LAB_UI_PREVIEW_ROW_LIMIT: Final = 20
LAB_UI_PREVIEW_COLUMN_LIMIT: Final = 12
_ADAPTER_VERSION: Final = "1"


class StrategyLabSubmissionContext(BaseModel):
    """Reproducibility and scheduling evidence supplied by one UI form."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
        strict=True,
    )

    gate_decision: ResearchGateDecision
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    dataset_snapshot: DatasetSnapshotIdentity | None
    execution_costs: ExecutionCostSpec
    random_seed: int = Field(strict=True, ge=0, lt=2**63)
    resource_class: ResourceClass
    deadline: datetime
    max_attempts: int = Field(default=1, strict=True, ge=1)

    @field_validator("deadline")
    @classmethod
    def validate_deadline(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("deadline must be timezone-aware")
        return value

    @model_validator(mode="after")
    def validate_formal_snapshot(self) -> StrategyLabSubmissionContext:
        if self.gate_decision.research_status != "exploratory" and self.dataset_snapshot is None:
            raise ValueError("formal research requires an immutable dataset snapshot")
        return self


def _adapter_id(run_input: ResearchRunInput) -> str:
    if isinstance(run_input, NShapeComparisonRunInput):
        return "nshape-compare"
    if isinstance(run_input, NShapeOptimizationRunInput):
        return "nshape-optimize"
    if isinstance(run_input, AuctionGapRunInput):
        return "auction-gap"
    if isinstance(run_input, GrowthBoardSurgeRunInput):
        return "growth-board-surge"
    raise TypeError(f"unsupported research run input: {type(run_input).__name__}")


def _fresh_job_id(interaction_key: str | None) -> UUID:
    if interaction_key is None:
        return uuid4()
    return uuid5(
        NAMESPACE_URL,
        f"rquant.strategy-lab.create-job:{interaction_key}",
    )


class StrategyLabJobCenterController:
    """Narrow UI surface over constructor-bound read and command facades."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        commands: LabCommandSubmissionFacade,
        preview_reader: ArtifactPreviewReader,
        zip_exports: LabJobZipExportFacade,
    ) -> None:
        self._reader = reader
        self._commands = commands
        self._preview_reader = preview_reader
        self._zip_exports = zip_exports

    @staticmethod
    def _build_submission(
        run_input: ResearchRunInput,
        *,
        context: StrategyLabSubmissionContext,
        job_id: UUID,
    ) -> ResearchJobSubmission:
        adapter_id = _adapter_id(run_input)
        feature_contract = build_adapter_execution_contract(
            adapter_id,
            _ADAPTER_VERSION,
            context.code_sha,
        )
        return build_research_job_submission(
            run_input,
            gate_decision=context.gate_decision,
            code_sha=context.code_sha,
            dataset_snapshot=context.dataset_snapshot,
            feature_contract=feature_contract,
            execution_costs=context.execution_costs,
            random_seed=context.random_seed,
            resource_class=context.resource_class,
            deadline=context.deadline,
            job_id=job_id,
            max_attempts=context.max_attempts,
        )

    def estimate_submission(
        self,
        run_input: ResearchRunInput,
        *,
        context: StrategyLabSubmissionContext,
        as_of: datetime,
    ) -> LabEtaEstimate:
        """Estimate the canonical plan without publishing a command."""
        selected_context = StrategyLabSubmissionContext.model_validate(context)
        submission = self._build_submission(
            run_input,
            context=selected_context,
            job_id=UUID(int=0),
        )
        definitions = default_strategy_job_adapter_registry().plan(submission.spec)
        return estimate_lab_eta(
            LabEtaInput(
                job_id=UUID(int=0),
                status="queued",
                as_of=as_of,
                remaining=tuple(
                    LabEtaRemainingShard(
                        shard_id=definition.shard_id,
                        work_plan=definition.work_plan,
                    )
                    for definition in definitions
                ),
            )
        )

    def submit(
        self,
        run_input: ResearchRunInput,
        *,
        context: StrategyLabSubmissionContext,
        interaction_key: str | None = None,
        job_id: UUID | None = None,
    ) -> CommandSubmissionResult:
        selected_context = StrategyLabSubmissionContext.model_validate(context)
        selected_job_id = job_id or _fresh_job_id(interaction_key)
        submission = self._build_submission(
            run_input,
            context=selected_context,
            job_id=selected_job_id,
        )
        return self._commands.submit_create(
            submission.command,
            interaction_key=interaction_key,
        )

    def list_jobs(
        self,
        *,
        filters: LabJobListFilters | None = None,
        page_size: int = 25,
        cursor: str | None = None,
    ) -> LabJobPage:
        if type(page_size) is not int or page_size not in LAB_UI_JOB_PAGE_SIZES:
            raise ValueError("page_size must be 20 or 25")
        return self._reader.list_jobs(
            filters=filters,
            limit=page_size,
            cursor=cursor,
        )

    def get_job_detail(
        self,
        job_id: UUID,
        *,
        as_of: datetime,
    ) -> LabJobDetail | None:
        return self._reader.get_job_detail(
            job_id,
            as_of=as_of,
            shard_limit=LAB_UI_SHARD_LIMIT,
            event_limit=LAB_UI_EVENT_LIMIT,
            artifact_limit=LAB_UI_ARTIFACT_LIMIT,
            completed_telemetry_limit=LAB_UI_COMPLETED_TELEMETRY_LIMIT,
        )

    def pause(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        reason: str,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._commands.submit_pause(
            job_id,
            expected_version=expected_version,
            reason=reason,
            interaction_key=interaction_key,
        )

    def resume(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        reason: str,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._commands.submit_resume(
            job_id,
            expected_version=expected_version,
            reason=reason,
            interaction_key=interaction_key,
        )

    def cancel(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        reason: str,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._commands.submit_cancel(
            job_id,
            expected_version=expected_version,
            reason=reason,
            interaction_key=interaction_key,
        )

    def retry(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        reason: str,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._commands.submit_retry(
            job_id,
            expected_version=expected_version,
            reason=reason,
            interaction_key=interaction_key,
        )

    def rerun(
        self,
        source_job_id: UUID,
        *,
        new_job_id: UUID | None = None,
        max_attempts: int = 1,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._commands.submit_rerun(
            source_job_id,
            new_job_id=new_job_id or _fresh_job_id(interaction_key),
            max_attempts=max_attempts,
            interaction_key=interaction_key,
        )

    def preview_artifact(
        self,
        job_id: UUID,
        table_name: str | None = None,
    ) -> ArtifactPreview:
        return self._preview_reader.preview(
            job_id,
            table_name=table_name,
            row_limit=LAB_UI_PREVIEW_ROW_LIMIT,
            column_limit=LAB_UI_PREVIEW_COLUMN_LIMIT,
        )

    def export_zip(self, job_id: UUID) -> LabJobZipExportReceipt:
        return self._zip_exports.export(job_id)

    def discard_zip(self, receipt: LabJobZipExportReceipt) -> None:
        self._zip_exports.discard(receipt)
