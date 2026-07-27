"""Typed Strategy Lab job creation and command-submission boundaries."""

from __future__ import annotations

import re
import stat
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal, TypeAlias
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabAcknowledgedCommand,
    LabCommand,
    LabCommandEnvelope,
    LabCommandSpool,
    LabSpoolEntry,
    PauseJobCommand,
    RequestContentConflictError,
    ResumeJobCommand,
    RetryJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import (
    MAX_JOB_SHARDS,
    JobStatus,
    LabJobReader,
)
from rquant.research_gate import ResearchGateDecision
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ParameterKind,
    ResearchJobType,
    ResearchParameter,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)
from rquant.strategy_job_adapters import (
    AuctionGapParameters,
    GrowthBoardSurgeParameters,
    NShapeCompareParameters,
    NShapeOptimizeParameters,
    build_adapter_execution_contract,
    default_strategy_job_adapter_registry,
)

_CLEAN_CODE_SHA = re.compile(r"^[0-9a-f]{40}$")
_MAX_RESEARCH_DATE_SPAN_DAYS = 5 * 366
_MAX_WALK_FORWARD_FOLDS = 64

ResearchJobSubmissionErrorCode: TypeAlias = Literal[
    "input_bounds",
    "adapter_plan",
    "shard_budget",
    "resource_budget",
]


class ResearchJobSubmissionError(ValueError):
    """Typed deterministic failure before a create command can be published."""

    def __init__(self, code: ResearchJobSubmissionErrorCode, message: str) -> None:
        self.code = code
        super().__init__(f"research submission preflight [{code}]: {message}")


class JobCenterModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
        strict=True,
    )


class _RunInputBase(JobCenterModel):
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_date_range(self) -> _RunInputBase:
        if self.start_date > self.end_date:
            raise ValueError("research start_date cannot be after end_date")
        return self


class NShapeComparisonRunInput(_RunInputBase):
    kind: Literal["n_shape_comparison"] = "n_shape_comparison"
    parameters: NShapeCompareParameters


class NShapeOptimizationRunInput(_RunInputBase):
    kind: Literal["n_shape_optimization"] = "n_shape_optimization"
    parameters: NShapeOptimizeParameters


class AuctionGapRunInput(_RunInputBase):
    kind: Literal["auction_gap"] = "auction_gap"
    parameters: AuctionGapParameters


class GrowthBoardSurgeRunInput(_RunInputBase):
    kind: Literal["growth_board_surge"] = "growth_board_surge"
    parameters: GrowthBoardSurgeParameters


ResearchRunInput: TypeAlias = Annotated[
    NShapeComparisonRunInput
    | NShapeOptimizationRunInput
    | AuctionGapRunInput
    | GrowthBoardSurgeRunInput,
    Field(discriminator="kind"),
]


class ResearchJobSubmission(JobCenterModel):
    spec: ResearchRunSpec
    command: SubmitJobCommand

    @model_validator(mode="after")
    def validate_command_spec(self) -> ResearchJobSubmission:
        if self.command.spec != self.spec:
            raise ValueError("create-job command does not contain the canonical run spec")
        return self


class _ResearchPlanBudget(JobCenterModel):
    max_shards: int = Field(ge=1, le=MAX_JOB_SHARDS)
    max_work_units: int = Field(ge=1)
    max_static_duration_ms: int = Field(ge=1)


_RESEARCH_PLAN_BUDGETS: dict[ResourceClass, _ResearchPlanBudget] = {
    ResourceClass.INTERACTIVE: _ResearchPlanBudget(
        max_shards=16,
        max_work_units=2_000,
        max_static_duration_ms=60 * 60 * 1_000,
    ),
    ResourceClass.STANDARD: _ResearchPlanBudget(
        max_shards=64,
        max_work_units=100_000,
        max_static_duration_ms=24 * 60 * 60 * 1_000,
    ),
    ResourceClass.HEAVY: _ResearchPlanBudget(
        max_shards=MAX_JOB_SHARDS,
        max_work_units=1_000_000,
        max_static_duration_ms=7 * 24 * 60 * 60 * 1_000,
    ),
}


def _research_parameter(name: str, value: object) -> ResearchParameter:
    if type(value) is bool:
        kind = ParameterKind.BOOLEAN
    elif type(value) is int:
        kind = ParameterKind.INTEGER
    elif isinstance(value, Decimal):
        kind = ParameterKind.DECIMAL
    elif type(value) is str:
        kind = ParameterKind.TEXT
    elif isinstance(value, tuple) and value and all(type(item) is int for item in value):
        kind = ParameterKind.INTEGER_LIST
    elif isinstance(value, tuple) and value and all(type(item) is str for item in value):
        kind = ParameterKind.TEXT_LIST
    else:
        raise TypeError(f"unsupported typed strategy parameter {name}: {type(value).__name__}")
    return ResearchParameter(name=name, kind=kind, value=value)


def _run_identity(
    run_input: ResearchRunInput,
) -> tuple[str, ResearchJobType, str, BaseModel]:
    if isinstance(run_input, NShapeComparisonRunInput):
        return (
            "n_shape",
            ResearchJobType.STRATEGY_REPLAY,
            "nshape-compare",
            run_input.parameters,
        )
    if isinstance(run_input, NShapeOptimizationRunInput):
        return (
            "n_shape",
            ResearchJobType.PARAMETER_SEARCH,
            "nshape-optimize",
            run_input.parameters,
        )
    if isinstance(run_input, AuctionGapRunInput):
        return (
            "auction_gap",
            ResearchJobType.STRATEGY_REPLAY,
            "auction-gap",
            run_input.parameters,
        )
    if isinstance(run_input, GrowthBoardSurgeRunInput):
        return (
            "growth_board_surge",
            ResearchJobType.STRATEGY_REPLAY,
            "growth-board-surge",
            run_input.parameters,
        )
    raise TypeError(f"unsupported research run input: {type(run_input).__name__}")


def _validate_gate_snapshot(
    decision: ResearchGateDecision,
    snapshot: DatasetSnapshotIdentity | None,
) -> None:
    if decision.research_status == "exploratory":
        return
    if decision.audit_run_id is None:
        raise ValueError("formal research gate is missing audit evidence")
    if snapshot is None:
        raise ValueError("formal research requires an immutable dataset snapshot")
    if snapshot.audit_run_id is None or snapshot.audit_run_id != decision.audit_run_id:
        raise ValueError("formal snapshot audit identity conflicts with the research gate")
    if snapshot.snapshot_id != decision.dataset_snapshot_id:
        raise ValueError("formal snapshot identity conflicts with the research gate")
    if snapshot.binding_hash != decision.dataset_binding_hash:
        raise ValueError("formal snapshot binding conflicts with the research gate")


def _validate_run_input_bounds(run_input: ResearchRunInput) -> None:
    span_days = (run_input.end_date - run_input.start_date).days + 1
    if span_days > _MAX_RESEARCH_DATE_SPAN_DAYS:
        raise ResearchJobSubmissionError(
            "input_bounds",
            f"research date span exceeds {_MAX_RESEARCH_DATE_SPAN_DAYS} days",
        )
    parameters = run_input.parameters
    if isinstance(parameters, NShapeCompareParameters):
        lengths = (
            ("hold_days", len(parameters.hold_days), 20),
            ("entry_modes", len(parameters.entry_modes), 6),
            ("profile_variants", len(parameters.profile_variants), 3),
        )
    elif isinstance(parameters, NShapeOptimizeParameters):
        lengths = (
            ("hold_days", len(parameters.hold_days), 20),
            ("entry_modes", len(parameters.entry_modes), 6),
            ("profile_variants", len(parameters.profile_variants), 3),
            ("top_n_options", len(parameters.top_n_options), 32),
            ("score_profile_names", len(parameters.score_profile_names), 11),
        )
        if parameters.walk_forward_folds > _MAX_WALK_FORWARD_FOLDS:
            raise ResearchJobSubmissionError(
                "input_bounds",
                f"walk_forward_folds exceeds {_MAX_WALK_FORWARD_FOLDS}",
            )
        if any(value > 1_000 for value in parameters.top_n_options):
            raise ResearchJobSubmissionError(
                "input_bounds",
                "top_n_options values cannot exceed 1000",
            )
    elif isinstance(parameters, GrowthBoardSurgeParameters):
        lengths = (("variants", len(parameters.variants), 5),)
    else:
        lengths = ()
    for field_name, observed, maximum in lengths:
        if observed > maximum:
            raise ResearchJobSubmissionError(
                "input_bounds",
                f"{field_name} cannot contain more than {maximum} values",
            )


def _preflight_research_plan(spec: ResearchRunSpec) -> None:
    try:
        definitions = default_strategy_job_adapter_registry().plan(spec)
    except (OverflowError, TypeError, ValueError, ValidationError) as exc:
        raise ResearchJobSubmissionError("adapter_plan", str(exc)) from exc
    if len(definitions) > MAX_JOB_SHARDS:
        raise ResearchJobSubmissionError(
            "shard_budget",
            f"adapter plan exceeds authoritative {MAX_JOB_SHARDS} shard limit",
        )
    budget = _RESEARCH_PLAN_BUDGETS[spec.resource_class]
    if len(definitions) > budget.max_shards:
        raise ResearchJobSubmissionError(
            "resource_budget",
            f"{spec.resource_class.value} plan exceeds {budget.max_shards} shard budget",
        )
    work_units = 0
    static_duration_ms = 0
    for definition in definitions:
        work_plan = definition.work_plan
        if work_plan is None:
            raise ResearchJobSubmissionError(
                "adapter_plan",
                "adapter preflight requires an explicit work plan for every shard",
            )
        work_units += work_plan.work_units
        static_duration_ms += work_plan.static_duration_ms
        if work_units > budget.max_work_units or static_duration_ms > budget.max_static_duration_ms:
            raise ResearchJobSubmissionError(
                "resource_budget",
                f"{spec.resource_class.value} plan exceeds work-unit or duration budget",
            )


def build_research_job_submission(
    run_input: ResearchRunInput,
    *,
    gate_decision: ResearchGateDecision,
    code_sha: str,
    dataset_snapshot: DatasetSnapshotIdentity | None,
    feature_contract: FeatureContractIdentity,
    execution_costs: ExecutionCostSpec,
    random_seed: int,
    resource_class: ResourceClass,
    deadline: datetime,
    job_id: UUID,
    max_attempts: int = 1,
) -> ResearchJobSubmission:
    decision = ResearchGateDecision.model_validate(gate_decision)
    if not decision.allowed:
        failure_codes = ",".join(item.code for item in decision.failures) or "unspecified"
        raise ValueError(f"research gate rejected the run: {failure_codes}")
    if not isinstance(code_sha, str) or _CLEAN_CODE_SHA.fullmatch(code_sha) is None:
        raise ValueError("code SHA must be an exact clean 40-character lowercase hex commit")
    _validate_gate_snapshot(decision, dataset_snapshot)
    strategy_name, job_type, adapter_id, typed_parameters = _run_identity(run_input)
    _validate_run_input_bounds(run_input)
    expected_contract = build_adapter_execution_contract(adapter_id, "1", code_sha)
    if feature_contract != expected_contract:
        raise ValueError("feature contract does not match the typed adapter and code SHA")
    try:
        arguments = tuple(
            _research_parameter(name, getattr(typed_parameters, name))
            for name in type(typed_parameters).model_fields
        )
        spec = ResearchRunSpec(
            job_type=job_type,
            parameters=ResearchRunParameters(
                strategy_name=strategy_name,
                start_date=run_input.start_date,
                end_date=run_input.end_date,
                arguments=arguments,
            ),
            code_sha=code_sha,
            dataset_snapshot=dataset_snapshot,
            feature_contract=feature_contract,
            execution_costs=execution_costs,
            random_seed=random_seed,
            resource_class=resource_class,
            deadline=deadline,
            research_status=decision.research_status,
        )
    except (TypeError, ValueError, ValidationError) as exc:
        raise ResearchJobSubmissionError("input_bounds", str(exc)) from exc
    _preflight_research_plan(spec)
    command = SubmitJobCommand(
        job_id=job_id,
        spec=spec,
        max_attempts=max_attempts,
    )
    return ResearchJobSubmission(spec=spec, command=command)


class SubmissionSpoolIdentity(JobCenterModel):
    path: Path
    state: Literal["pending", "acknowledged"]
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class CommandSubmissionReceipt(JobCenterModel):
    result: Literal["submitted"] = "submitted"
    request_id: UUID
    command_type: Literal["submit", "pause", "resume", "cancel", "retry"]
    job_id: UUID
    expected_version: int | None = Field(default=None, ge=0)
    spool: SubmissionSpoolIdentity


class CommandSubmissionStale(JobCenterModel):
    result: Literal["stale"] = "stale"
    request_id: UUID
    job_id: UUID
    expected_version: int = Field(ge=0)
    authoritative_version: int = Field(ge=0)
    authoritative_status: JobStatus
    scheduler_reason: str | None = None


class CommandSubmissionConflict(JobCenterModel):
    result: Literal["conflict"] = "conflict"
    request_id: UUID
    job_id: UUID
    reason: Literal[
        "interaction_content_conflict",
        "job_not_found",
        "job_id_exists",
        "scheduler_rejected",
    ]
    scheduler_reason: str | None = None


class CommandSubmissionUnavailable(JobCenterModel):
    result: Literal["unavailable"] = "unavailable"
    request_id: UUID
    job_id: UUID
    command_type: Literal["pause", "resume", "cancel", "retry"]
    authoritative_version: int = Field(ge=0)
    authoritative_status: JobStatus
    scheduler_reason: str | None = None


CommandSubmissionResult: TypeAlias = Annotated[
    CommandSubmissionReceipt
    | CommandSubmissionStale
    | CommandSubmissionConflict
    | CommandSubmissionUnavailable,
    Field(discriminator="result"),
]


class LabCommandSubmissionFacade:
    """Read scheduler state and publish commands without opening a writable ledger."""

    def __init__(self, *, reader: LabJobReader, spool: LabCommandSpool) -> None:
        self.reader = reader
        self.spool = spool

    @staticmethod
    def _request_id(interaction_key: str | None) -> UUID:
        if interaction_key is None:
            return uuid4()
        if (
            not isinstance(interaction_key, str)
            or not interaction_key
            or interaction_key != interaction_key.strip()
            or len(interaction_key) > 256
        ):
            raise ValueError("interaction_key must be 1-256 stable non-whitespace characters")
        return uuid5(NAMESPACE_URL, f"rquant.lab-job-center.interaction:{interaction_key}")

    @staticmethod
    def _spool_identity(
        value: LabSpoolEntry | LabAcknowledgedCommand,
    ) -> SubmissionSpoolIdentity:
        if isinstance(value, LabSpoolEntry):
            return SubmissionSpoolIdentity(
                path=value.path,
                state="pending",
                device=value.device,
                inode=value.inode,
                content_hash=value.envelope.content_hash,
            )
        observed = value.path.lstat()
        if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
            raise RuntimeError("acknowledged command spool identity is unsafe")
        return SubmissionSpoolIdentity(
            path=value.path,
            state="acknowledged",
            device=observed.st_dev,
            inode=observed.st_ino,
            content_hash=value.receipt.content_hash,
        )

    @classmethod
    def _receipt(
        cls,
        envelope: LabCommandEnvelope,
        published: LabSpoolEntry | LabAcknowledgedCommand,
    ) -> CommandSubmissionReceipt:
        command = envelope.command
        return CommandSubmissionReceipt(
            request_id=envelope.request_id,
            command_type=command.command_type,
            job_id=command.job_id,
            expected_version=(
                command.expected_version if not isinstance(command, SubmitJobCommand) else None
            ),
            spool=cls._spool_identity(published),
        )

    def _existing(
        self,
        envelope: LabCommandEnvelope,
    ) -> CommandSubmissionResult | None:
        try:
            existing = self.spool.find(envelope.request_id)
        except RequestContentConflictError:
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=envelope.command.job_id,
                reason="interaction_content_conflict",
            )
        if existing is None:
            return None
        content_hash = (
            existing.envelope.content_hash
            if isinstance(existing, LabSpoolEntry)
            else existing.receipt.content_hash
        )
        existing_job_id = (
            existing.envelope.command.job_id
            if isinstance(existing, LabSpoolEntry)
            else existing.receipt.job_id
        )
        if content_hash != envelope.content_hash or existing_job_id != envelope.command.job_id:
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=envelope.command.job_id,
                reason="interaction_content_conflict",
            )
        if isinstance(existing, LabAcknowledgedCommand) and existing.receipt.status == "rejected":
            return self._scheduler_rejection(envelope, existing)
        return self._receipt(envelope, existing)

    def _scheduler_rejection(
        self,
        envelope: LabCommandEnvelope,
        acknowledged: LabAcknowledgedCommand,
    ) -> CommandSubmissionResult:
        command = envelope.command
        scheduler_reason = acknowledged.receipt.reason
        if scheduler_reason == "job_not_found":
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=command.job_id,
                reason="job_not_found",
                scheduler_reason=scheduler_reason,
            )
        if isinstance(command, SubmitJobCommand):
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=command.job_id,
                reason=(
                    "job_id_exists" if scheduler_reason == "job_id_reused" else "scheduler_rejected"
                ),
                scheduler_reason=scheduler_reason,
            )
        context = self.reader.get_command_context(command.job_id)
        if context is None:
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=command.job_id,
                reason="job_not_found",
                scheduler_reason=scheduler_reason,
            )
        if scheduler_reason.startswith("stale_version:"):
            return CommandSubmissionStale(
                request_id=envelope.request_id,
                job_id=command.job_id,
                expected_version=command.expected_version,
                authoritative_version=context.job.version,
                authoritative_status=context.job.status,
                scheduler_reason=scheduler_reason,
            )
        if scheduler_reason.startswith("invalid_state:"):
            return CommandSubmissionUnavailable(
                request_id=envelope.request_id,
                job_id=command.job_id,
                command_type=command.command_type,
                authoritative_version=context.job.version,
                authoritative_status=context.job.status,
                scheduler_reason=scheduler_reason,
            )
        return CommandSubmissionConflict(
            request_id=envelope.request_id,
            job_id=command.job_id,
            reason="scheduler_rejected",
            scheduler_reason=scheduler_reason,
        )

    def _publish(
        self,
        envelope: LabCommandEnvelope,
    ) -> CommandSubmissionReceipt | CommandSubmissionConflict:
        try:
            published = self.spool.publish(envelope)
        except RequestContentConflictError:
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=envelope.command.job_id,
                reason="interaction_content_conflict",
            )
        return self._receipt(envelope, published)

    def submit_create(
        self,
        command: SubmitJobCommand,
        *,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        validated = SubmitJobCommand.model_validate(command)
        envelope = LabCommandEnvelope(
            request_id=self._request_id(interaction_key),
            command=validated,
        )
        existing = self._existing(envelope)
        if existing is not None:
            return existing
        if self.reader.get_job(validated.job_id) is not None:
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=validated.job_id,
                reason="job_id_exists",
            )
        return self._publish(envelope)

    def _submit_control(
        self,
        command: LabCommand,
        *,
        interaction_key: str | None,
    ) -> CommandSubmissionResult:
        if isinstance(command, SubmitJobCommand):
            raise TypeError("control submission cannot contain a create command")
        envelope = LabCommandEnvelope(
            request_id=self._request_id(interaction_key),
            command=command,
        )
        existing = self._existing(envelope)
        if existing is not None:
            return existing
        context = self.reader.get_command_context(command.job_id)
        if context is None:
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=command.job_id,
                reason="job_not_found",
            )
        job = context.job
        if command.expected_version != job.version:
            return CommandSubmissionStale(
                request_id=envelope.request_id,
                job_id=command.job_id,
                expected_version=command.expected_version,
                authoritative_version=job.version,
                authoritative_status=job.status,
            )
        if not getattr(context.availability, command.command_type):
            return CommandSubmissionUnavailable(
                request_id=envelope.request_id,
                job_id=command.job_id,
                command_type=command.command_type,
                authoritative_version=job.version,
                authoritative_status=job.status,
            )
        return self._publish(envelope)

    def submit_pause(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        reason: str,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._submit_control(
            PauseJobCommand(
                job_id=job_id,
                expected_version=expected_version,
                reason=reason,
            ),
            interaction_key=interaction_key,
        )

    def submit_resume(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        reason: str,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._submit_control(
            ResumeJobCommand(
                job_id=job_id,
                expected_version=expected_version,
                reason=reason,
            ),
            interaction_key=interaction_key,
        )

    def submit_cancel(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        reason: str,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._submit_control(
            CancelJobCommand(
                job_id=job_id,
                expected_version=expected_version,
                reason=reason,
            ),
            interaction_key=interaction_key,
        )

    def submit_retry(
        self,
        job_id: UUID,
        *,
        expected_version: int,
        reason: str,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        return self._submit_control(
            RetryJobCommand(
                job_id=job_id,
                expected_version=expected_version,
                reason=reason,
            ),
            interaction_key=interaction_key,
        )
