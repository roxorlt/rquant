"""Typed, side-effect-free Strategy Lab UI access to the durable Job Center."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Final, Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.definition_registry import ImmutableDefinitionRegistry, StrategySpecRegistration
from rquant.experiment_registry import FormalExperimentPlan
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
    NShapeComparisonRunInput,
    NShapeOptimizationRunInput,
    ResearchJobSubmission,
    ResearchRunInput,
    build_research_job_submission,
)
from rquant.lab_job_protocol import SubmitJobCommand
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
    FeatureContractIdentity,
    ResearchRunParameters,
    ResourceClass,
)
from rquant.runtime_contracts import canonical_sha256
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


class StrategyLabFormalResolutionRequest(BaseModel):
    """Canonical inputs supplied only to a constructor-bound experiment resolver."""

    model_config = StrategyLabSubmissionContext.model_config

    strategy_name: str = Field(min_length=1)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    parameters: ResearchRunParameters
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    dataset_snapshot: DatasetSnapshotIdentity
    feature_contract: FeatureContractIdentity
    execution_costs: ExecutionCostSpec
    random_seed: int = Field(strict=True, ge=0, lt=2**63)
    requested_at: datetime

    @field_validator("requested_at")
    @classmethod
    def validate_requested_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("requested_at must be timezone-aware")
        return value.astimezone(UTC)


class StrategyLabFormalExperimentBinding(BaseModel):
    """Trusted preregistration evidence returned outside the free-form UI payload."""

    model_config = StrategyLabSubmissionContext.model_config

    formal_plan: FormalExperimentPlan


StrategyLabFormalExperimentResolver = Callable[
    [StrategySpecRegistration, StrategyLabFormalResolutionRequest],
    StrategyLabFormalExperimentBinding,
]


class FormalExperimentPlanReader(Protocol):
    def resolve_formal_plan(
        self,
        *,
        strategy_spec_fingerprint: str,
        strategy_executable_fingerprint: str,
        candidate_schema_fingerprint: str,
        dataset_snapshot_id: str,
        code_commit: str,
        parameter_fingerprint: str,
        cost_model_fingerprint: str,
        execution_model_fingerprint: str,
        seed: int,
        as_of: datetime,
    ) -> FormalExperimentPlan: ...


class LabCommandWriter(Protocol):
    experiment_registry: object | None

    def submit_create(self, command: SubmitJobCommand, **kwargs: object) -> object: ...

    def submit_pause(self, job_id: UUID, **kwargs: object) -> object: ...

    def submit_resume(self, job_id: UUID, **kwargs: object) -> object: ...

    def submit_cancel(self, job_id: UUID, **kwargs: object) -> object: ...

    def submit_retry(self, job_id: UUID, **kwargs: object) -> object: ...

    def submit_rerun(self, source_job_id: UUID, **kwargs: object) -> object: ...


class LabZipWriter(Protocol):
    def export(self, job_id: UUID) -> object: ...

    def discard(self, receipt: object) -> None: ...


class RegistryBackedFormalExperimentResolver:
    """Resolve only exact experiment plans preregistered in the authority ledger."""

    def __init__(self, registry: FormalExperimentPlanReader) -> None:
        if not callable(getattr(registry, "resolve_formal_plan", None)):
            raise TypeError("formal resolver requires a read-only formal plan reader")
        self.registry = registry

    def __call__(
        self,
        registration: StrategySpecRegistration,
        request: StrategyLabFormalResolutionRequest,
    ) -> StrategyLabFormalExperimentBinding:
        selected_registration = StrategySpecRegistration.model_validate(
            registration.model_dump(mode="python")
        )
        selected_request = StrategyLabFormalResolutionRequest.model_validate(request)
        if (
            selected_registration.logical_id != selected_request.strategy_name
            or selected_registration.spec.strategy_id != selected_request.strategy_name
            or selected_registration.producer_commit != selected_request.code_sha
            or selected_registration.spec.producer_commit != selected_request.code_sha
        ):
            raise RuntimeError(
                "formal experiment request conflicts with the trusted strategy registration"
            )
        plan = self.registry.resolve_formal_plan(
            strategy_spec_fingerprint=selected_registration.spec.spec_fingerprint,
            strategy_executable_fingerprint=(selected_registration.executable_fingerprint),
            candidate_schema_fingerprint=(selected_registration.candidate_schema_fingerprint),
            dataset_snapshot_id=selected_request.dataset_snapshot.snapshot_id,
            code_commit=selected_request.code_sha,
            parameter_fingerprint=canonical_sha256(selected_request.parameters),
            cost_model_fingerprint=canonical_sha256(selected_request.execution_costs),
            execution_model_fingerprint=canonical_sha256(
                {
                    "contract": "lab-adapter-execution/v1",
                    "adapter_id": selected_request.adapter_id,
                    "adapter_version": selected_request.adapter_version,
                    "feature_contract": selected_request.feature_contract,
                }
            ),
            seed=selected_request.random_seed,
            as_of=selected_request.requested_at,
        )
        return StrategyLabFormalExperimentBinding(
            formal_plan=plan,
        )


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
        preview_reader: ArtifactPreviewReader,
        commands: LabCommandWriter | None = None,
        zip_exports: LabZipWriter | None = None,
        definition_registry: ImmutableDefinitionRegistry | None = None,
        formal_experiment_resolver: StrategyLabFormalExperimentResolver | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if formal_experiment_resolver is not None and not callable(formal_experiment_resolver):
            raise TypeError("formal_experiment_resolver must be callable")
        self._reader = reader
        self._commands = commands
        self._preview_reader = preview_reader
        self._zip_exports = zip_exports
        self._definition_registry = definition_registry
        self._formal_experiment_resolver = formal_experiment_resolver
        self._clock = clock or (lambda: datetime.now(UTC))

    def build_submission_command(
        self,
        run_input: ResearchRunInput,
        *,
        context: StrategyLabSubmissionContext,
        job_id: UUID,
        as_of: datetime,
    ) -> SubmitJobCommand:
        """Build one immutable command without publishing page-side state."""

        selected_context = StrategyLabSubmissionContext.model_validate(context)
        return self._build_submission(
            run_input,
            context=selected_context,
            job_id=job_id,
            as_of=as_of,
        ).command

    def _build_submission(
        self,
        run_input: ResearchRunInput,
        *,
        context: StrategyLabSubmissionContext,
        job_id: UUID,
        as_of: datetime,
    ) -> ResearchJobSubmission:
        adapter_id = _adapter_id(run_input)
        feature_contract = build_adapter_execution_contract(
            adapter_id,
            _ADAPTER_VERSION,
            context.code_sha,
        )
        if context.gate_decision.research_status == "exploratory":
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
        if self._definition_registry is None or self._formal_experiment_resolver is None:
            raise RuntimeError(
                "formal research requires trusted Definition Registry and formal ownership resolver"
            )
        if context.dataset_snapshot is None:  # pragma: no cover - context validator owns this
            raise RuntimeError("formal research requires an immutable dataset snapshot")
        provisional = build_research_job_submission(
            run_input,
            gate_decision=context.gate_decision.model_copy(
                update={"research_status": "exploratory"}
            ),
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
        registration = self._definition_registry.latest_strategy_spec(
            provisional.spec.parameters.strategy_name,
            as_of=as_of,
        )
        if registration is None:
            raise RuntimeError("formal strategy registration is not visible at submission time")
        request = StrategyLabFormalResolutionRequest(
            strategy_name=provisional.spec.parameters.strategy_name,
            adapter_id=adapter_id,
            adapter_version=_ADAPTER_VERSION,
            parameters=provisional.spec.parameters,
            code_sha=context.code_sha,
            dataset_snapshot=context.dataset_snapshot,
            feature_contract=feature_contract,
            execution_costs=context.execution_costs,
            random_seed=context.random_seed,
            requested_at=as_of,
        )
        binding = StrategyLabFormalExperimentBinding.model_validate(
            self._formal_experiment_resolver(registration, request)
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
            trusted_strategy_registration=registration,
            formal_experiment_plan=binding.formal_plan,
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
            as_of=as_of,
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
        if self._commands is None:
            raise RuntimeError("job command writer is not configured")
        selected_context = StrategyLabSubmissionContext.model_validate(context)
        selected_job_id = job_id or _fresh_job_id(interaction_key)
        submission = self._build_submission(
            run_input,
            context=selected_context,
            job_id=selected_job_id,
            as_of=self._clock(),
        )
        if submission.spec.schema_version == 3 and self._commands.experiment_registry is None:
            raise RuntimeError("formal research submission requires ExperimentRegistry outbox")
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
        if self._commands is None:
            raise RuntimeError("job command writer is not configured")
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
        if self._commands is None:
            raise RuntimeError("job command writer is not configured")
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
        if self._commands is None:
            raise RuntimeError("job command writer is not configured")
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
        if self._commands is None:
            raise RuntimeError("job command writer is not configured")
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
        if self._commands is None:
            raise RuntimeError("job command writer is not configured")
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

    def export_zip(self, job_id: UUID) -> object:
        if self._zip_exports is None:
            raise RuntimeError("ZIP export writer is not configured")
        return self._zip_exports.export(job_id)

    def discard_zip(self, receipt: object) -> None:
        if self._zip_exports is None:
            raise RuntimeError("ZIP export writer is not configured")
        self._zip_exports.discard(receipt)
