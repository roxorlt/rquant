"""Typed Strategy Lab job creation and command-submission boundaries."""

from __future__ import annotations

import re
import stat
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal, TypeAlias
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from rquant.definition_registry import ImmutableDefinitionRegistry, StrategySpecRegistration
from rquant.experiment_registry import (
    ExperimentAttempt,
    ExperimentRegistry,
    ExperimentSubmissionIntent,
    FormalExperimentPlan,
    IncompleteHypothesisFamilyError,
)
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
    FormalSubmissionAuthorityError,
    JobStatus,
    LabJobReader,
)
from rquant.research_gate import ResearchGateDecision
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ParameterKind,
    ResearchExperimentIdentity,
    ResearchJobType,
    ResearchParameter,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
    StrategyExecutionIdentity,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_job_adapters import (
    AuctionGapParameters,
    GrowthBoardSurgeParameters,
    NShapeCompareParameters,
    NShapeOptimizeParameters,
    build_adapter_execution_contract,
    default_strategy_job_adapter_registry,
)
from rquant.strict_json import canonical_json_bytes

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
    trusted_strategy_registration: StrategySpecRegistration | None = None,
    formal_experiment_plan: FormalExperimentPlan | None = None,
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
        parameters = ResearchRunParameters(
            strategy_name=strategy_name,
            start_date=run_input.start_date,
            end_date=run_input.end_date,
            arguments=arguments,
        )
        ownership_values = _validated_research_ownership(
            decision=decision,
            strategy_name=strategy_name,
            adapter_id=adapter_id,
            adapter_version="1",
            code_sha=code_sha,
            deadline=deadline,
            dataset_snapshot=dataset_snapshot,
            feature_contract=feature_contract,
            execution_costs=execution_costs,
            parameters=parameters,
            random_seed=random_seed,
            trusted_strategy_registration=trusted_strategy_registration,
            formal_experiment_plan=formal_experiment_plan,
        )
        spec = ResearchRunSpec(
            schema_version=ownership_values[0],
            job_type=job_type,
            parameters=parameters,
            code_sha=code_sha,
            dataset_snapshot=dataset_snapshot,
            feature_contract=feature_contract,
            execution_costs=execution_costs,
            random_seed=random_seed,
            resource_class=resource_class,
            deadline=deadline,
            research_status=decision.research_status,
            strategy_execution=ownership_values[1],
            experiment=ownership_values[2],
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


def _validated_research_ownership(
    *,
    decision: ResearchGateDecision,
    strategy_name: str,
    adapter_id: str,
    adapter_version: str,
    code_sha: str,
    deadline: datetime,
    dataset_snapshot: DatasetSnapshotIdentity | None,
    feature_contract: FeatureContractIdentity,
    execution_costs: ExecutionCostSpec,
    parameters: ResearchRunParameters,
    random_seed: int,
    trusted_strategy_registration: StrategySpecRegistration | None,
    formal_experiment_plan: FormalExperimentPlan | None,
) -> tuple[
    Literal[2, 3],
    StrategyExecutionIdentity | None,
    ResearchExperimentIdentity | None,
]:
    supplied = (
        trusted_strategy_registration is not None,
        formal_experiment_plan is not None,
    )
    if not any(supplied):
        if decision.research_status != "exploratory":
            raise ValueError(
                "formal research submission requires a trusted strategy registration, "
                "and exact formal experiment plan"
            )
        return 2, None, None
    if not all(supplied):
        raise ValueError(
            "trusted strategy registration and exact formal experiment plan "
            "must be supplied together"
        )
    assert trusted_strategy_registration is not None
    assert formal_experiment_plan is not None
    registration = StrategySpecRegistration.model_validate(
        trusted_strategy_registration.model_dump(mode="python")
    )
    plan = FormalExperimentPlan.model_validate(formal_experiment_plan.model_dump(mode="python"))
    if plan.schema_version != 2:
        raise ValueError("formal research requires a current FormalExperimentPlan")
    experiment = plan.spec
    if registration.logical_id != strategy_name or registration.spec.strategy_id != strategy_name:
        raise ValueError("trusted strategy registration does not match strategy_name")
    if registration.producer_commit != code_sha or registration.spec.producer_commit != code_sha:
        raise ValueError("trusted strategy registration does not match code SHA")
    if registration.available_at > deadline:
        raise ValueError("trusted strategy registration is not visible by the run deadline")
    if plan.preregistered_at > deadline:
        raise ValueError("formal experiment plan is not visible by the run deadline")
    if (
        plan.strategy_definition_fingerprint != registration.fingerprint
        or plan.definition_registration_record_hash != registration.record_hash
    ):
        raise ValueError("formal experiment plan does not match Definition Registry receipts")
    execution = StrategyExecutionIdentity(
        strategy_id=registration.logical_id,
        strategy_version=registration.version,
        adapter_id=adapter_id,
        adapter_version=adapter_version,
        strategy_spec_fingerprint=registration.spec.spec_fingerprint,
        strategy_definition_fingerprint=registration.fingerprint,
        strategy_executable_fingerprint=registration.executable_fingerprint,
        candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
        definition_registration_record_hash=registration.record_hash,
        definition_registered_at=registration.registered_at,
        definition_available_at=registration.available_at,
        producer_code_commit=registration.producer_commit,
    )
    if dataset_snapshot is None:
        raise ValueError("catalog-bound experiment requires an immutable dataset snapshot")
    expected = (
        registration.spec.spec_fingerprint,
        registration.executable_fingerprint,
        registration.candidate_schema_fingerprint,
        dataset_snapshot.snapshot_id,
        code_sha,
        canonical_sha256(parameters),
        canonical_sha256(execution_costs),
        canonical_sha256(
            {
                "contract": "lab-adapter-execution/v1",
                "adapter_id": adapter_id,
                "adapter_version": adapter_version,
                "feature_contract": feature_contract,
            }
        ),
        random_seed,
    )
    actual = (
        experiment.strategy_spec_fingerprint,
        experiment.strategy_executable_fingerprint,
        experiment.candidate_schema_fingerprint,
        experiment.dataset_snapshot_id,
        experiment.code_commit,
        experiment.parameter_fingerprint,
        experiment.cost_model_fingerprint,
        experiment.execution_model_fingerprint,
        experiment.seed,
    )
    if actual != expected:
        raise ValueError("experiment spec does not exactly match the trusted research run")
    return (
        3,
        execution,
        ResearchExperimentIdentity(
            schema_version=2,
            spec=experiment,
            experiment_id=experiment.experiment_id,
            hypothesis_family=experiment.hypothesis_family,
            hypothesis_variant=plan.hypothesis_variant,
            formal_plan_id=plan.plan_id,
        ),
    )


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

    def __init__(
        self,
        *,
        reader: LabJobReader,
        spool: LabCommandSpool,
        experiment_registry: ExperimentRegistry | None = None,
        definition_registry: ImmutableDefinitionRegistry | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.reader = reader
        self.spool = spool
        self.experiment_registry = experiment_registry
        self.definition_registry = definition_registry
        self.clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    def _experiment_submission_intent(
        envelope: LabCommandEnvelope,
    ) -> ExperimentSubmissionIntent | None:
        command = envelope.command
        if not isinstance(command, SubmitJobCommand):
            return None
        spec = command.spec
        if spec.schema_version < 3:
            return None
        if spec.experiment is None or not spec.catalog_owner_eligible:
            raise ValueError("v3 research job is missing experiment ownership")
        envelope_json = canonical_json_bytes(envelope.model_dump(mode="json")).decode("utf-8")
        return ExperimentSubmissionIntent(
            schema_version=2,
            request_id=envelope.request_id,
            job_id=command.job_id,
            experiment_id=spec.experiment.experiment_id,
            attempt_identity=spec.experiment.attempt_identity,
            hypothesis_variant=spec.experiment.hypothesis_variant,
            formal_plan_id=spec.experiment.formal_plan_id,
            strategy_definition_fingerprint=(
                spec.strategy_execution.strategy_definition_fingerprint
            ),
            definition_registration_record_hash=(
                spec.strategy_execution.definition_registration_record_hash
            ),
            command_content_hash=envelope.content_hash,
            envelope_json=envelope_json,
            envelope_sha256=canonical_sha256({"canonical_envelope_json": envelope_json}),
        )

    def _validate_formal_submission_authorities(
        self,
        envelope: LabCommandEnvelope,
        *,
        observed_at: datetime,
    ) -> tuple[ExperimentSubmissionIntent, ResearchExperimentIdentity]:
        intent = self._experiment_submission_intent(envelope)
        if intent is None:
            raise FormalSubmissionAuthorityError("formal v3 submission identity is required")
        if self.experiment_registry is None:
            raise FormalSubmissionAuthorityError(
                "v3 research submission requires an authoritative ExperimentRegistry"
            )
        command = envelope.command
        assert isinstance(command, SubmitJobCommand)
        assert command.spec.experiment is not None
        assert command.spec.strategy_execution is not None
        experiment = command.spec.experiment
        execution = command.spec.strategy_execution
        assert experiment.formal_plan_id is not None
        try:
            plan = self.experiment_registry.resolve_formal_plan_by_id(
                experiment.formal_plan_id,
                as_of=observed_at,
            )
        except IncompleteHypothesisFamilyError as exc:
            raise FormalSubmissionAuthorityError("exact formal plan is unavailable") from exc
        if (
            plan.schema_version != 2
            or plan.spec != experiment.spec
            or plan.hypothesis_variant != experiment.hypothesis_variant
            or plan.strategy_definition_fingerprint != execution.strategy_definition_fingerprint
            or plan.definition_registration_record_hash
            != execution.definition_registration_record_hash
        ):
            raise FormalSubmissionAuthorityError(
                "formal plan receipts do not exactly match the research job"
            )
        if self.definition_registry is None:
            raise FormalSubmissionAuthorityError(
                "v3 research submission requires an authoritative Definition Registry"
            )
        registration = self.definition_registry.read_strategy_spec(
            execution.strategy_definition_fingerprint,
            as_of=observed_at,
        )
        if registration is None:
            raise FormalSubmissionAuthorityError("trusted strategy registration is not visible")
        exact_registration = (
            registration.logical_id,
            registration.version,
            registration.spec.spec_fingerprint,
            registration.fingerprint,
            registration.executable_fingerprint,
            registration.candidate_schema_fingerprint,
            registration.record_hash,
            registration.registered_at,
            registration.available_at,
            registration.producer_commit,
        )
        submitted_registration = (
            execution.strategy_id,
            execution.strategy_version,
            execution.strategy_spec_fingerprint,
            execution.strategy_definition_fingerprint,
            execution.strategy_executable_fingerprint,
            execution.candidate_schema_fingerprint,
            execution.definition_registration_record_hash,
            execution.definition_registered_at,
            execution.definition_available_at,
            execution.producer_code_commit,
        )
        if exact_registration != submitted_registration:
            raise FormalSubmissionAuthorityError(
                "strategy execution identity conflicts with authoritative Definition Registry"
            )
        return intent, experiment

    def _prepare_experiment_submission(self, envelope: LabCommandEnvelope) -> None:
        if self._experiment_submission_intent(envelope) is None:
            return
        observed_at = self.clock()
        intent, experiment = self._validate_formal_submission_authorities(
            envelope,
            observed_at=observed_at,
        )
        assert self.experiment_registry is not None
        self.experiment_registry.register_attempt(
            experiment.spec,
            registered_at=observed_at,
            submission=intent,
        )

    def validate_prepared_experiment_submission(
        self,
        envelope: LabCommandEnvelope,
        *,
        observed_at: datetime,
    ) -> None:
        """Re-read exact immutable ownership before the Job Center transaction writes."""

        intent, experiment = self._validate_formal_submission_authorities(
            envelope,
            observed_at=observed_at,
        )
        assert self.experiment_registry is not None
        stored_intent = self.experiment_registry.get_submission_intent_for_job(
            envelope.command.job_id
        )
        if stored_intent != intent:
            raise FormalSubmissionAuthorityError(
                "formal submission has no exact prepared Experiment ownership intent"
            )
        try:
            attempt = self.experiment_registry.get_attempt(experiment.experiment_id)
        except KeyError as exc:
            raise FormalSubmissionAuthorityError(
                "formal submission has no registered Experiment attempt"
            ) from exc
        if attempt.spec != experiment.spec:
            raise FormalSubmissionAuthorityError(
                "formal submission Experiment attempt identity conflicts with its plan"
            )

    def _mark_experiment_submission_published(self, envelope: LabCommandEnvelope) -> None:
        intent = self._experiment_submission_intent(envelope)
        if intent is None:
            return
        if self.experiment_registry is None:  # pragma: no cover - guarded by prepare
            raise RuntimeError("v3 research submission requires an ExperimentRegistry")
        self.experiment_registry.mark_submission_published(
            envelope.request_id,
            command_content_hash=envelope.content_hash,
            published_at=self.clock(),
        )

    def recover_pending_experiment_submissions(
        self,
        *,
        limit: int = 100,
    ) -> tuple[CommandSubmissionReceipt, ...]:
        if self.experiment_registry is None:
            return ()
        recovered: list[CommandSubmissionReceipt] = []
        for intent in self.experiment_registry.list_pending_submissions(limit=limit):
            envelope = LabCommandEnvelope.model_validate_json(intent.envelope_json)
            if (
                envelope.request_id != intent.request_id
                or envelope.command.job_id != intent.job_id
                or envelope.content_hash != intent.command_content_hash
            ):
                raise RuntimeError("experiment submission outbox conflicts with command envelope")
            published = self._publish(envelope)
            if isinstance(published, CommandSubmissionConflict):
                raise RuntimeError("experiment submission recovery hit a command conflict")
            self._mark_experiment_submission_published(envelope)
            recovered.append(published)
        return tuple(recovered)

    def synchronize_experiment_lifecycle(
        self,
        job_id: UUID,
        *,
        observed_at: datetime,
    ) -> ExperimentAttempt:
        """Recover one experiment attempt from the authoritative job state."""

        if self.experiment_registry is None:
            raise RuntimeError("experiment lifecycle synchronization requires ExperimentRegistry")
        return ExperimentJobLifecycleSynchronizer(
            reader=self.reader,
            registry=self.experiment_registry,
        ).synchronize(job_id, observed_at=observed_at)

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
        if validated.spec.schema_version < 3 and validated.spec.research_status != "exploratory":
            raise ValueError("v2 comparable/formal jobs are not executable; migrate as exploratory")
        envelope = LabCommandEnvelope(
            request_id=self._request_id(interaction_key),
            command=validated,
        )
        existing = self._existing(envelope)
        if existing is not None:
            if isinstance(existing, CommandSubmissionReceipt):
                self._prepare_experiment_submission(envelope)
                self._mark_experiment_submission_published(envelope)
            return existing
        if self.reader.get_job(validated.job_id) is not None:
            return CommandSubmissionConflict(
                request_id=envelope.request_id,
                job_id=validated.job_id,
                reason="job_id_exists",
            )
        self._prepare_experiment_submission(envelope)
        published = self._publish(envelope)
        if isinstance(published, CommandSubmissionReceipt):
            self._mark_experiment_submission_published(envelope)
        return published

    def submit_rerun(
        self,
        source_job_id: UUID,
        *,
        new_job_id: UUID,
        max_attempts: int,
        interaction_key: str | None = None,
    ) -> CommandSubmissionResult:
        source = self.reader.get_job(source_job_id)
        if source is None:
            return CommandSubmissionConflict(
                request_id=self._request_id(interaction_key),
                job_id=new_job_id,
                reason="job_not_found",
            )
        return self.submit_create(
            SubmitJobCommand(
                job_id=new_job_id,
                spec=source.spec,
                max_attempts=max_attempts,
            ),
            interaction_key=interaction_key,
        )

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


class ExperimentJobLifecycleSynchronizer:
    """Map authoritative Job Center states onto one stable experiment attempt."""

    def __init__(self, *, reader: LabJobReader, registry: ExperimentRegistry) -> None:
        self.reader = reader
        self.registry = registry

    def synchronize(self, job_id: UUID, *, observed_at: datetime) -> ExperimentAttempt:
        job = self.reader.get_job(job_id)
        if job is None:
            raise KeyError(f"unknown lab job: {job_id}")
        if job.updated_at > observed_at:
            raise ValueError("job lifecycle evidence is from the future")
        experiment = job.spec.experiment
        if job.spec.schema_version != 3 or experiment is None:
            raise ValueError("legacy lab jobs cannot mutate experiment lifecycle")
        experiment_id = experiment.experiment_id
        if job.status in {JobStatus.RUNNING, JobStatus.CHECKPOINTED, JobStatus.SUCCEEDED}:
            attempt = self.registry.ensure_attempt_started(
                experiment_id,
                started_at=job.updated_at,
            )
            if job.status is JobStatus.SUCCEEDED:
                return self.registry.record_execution_completed(
                    experiment_id,
                    completed_at=job.updated_at,
                )
            return attempt
        if job.status is JobStatus.FAILED and not job.recoverable:
            return self.registry.record_failure(
                experiment_id,
                first_error=(
                    f"lab job failed after {job.attempt_count}/{job.max_attempts} attempts"
                ),
                completed_at=job.updated_at,
            )
        if job.status is JobStatus.CANCELLED:
            return self.registry.cancel_attempt(
                experiment_id,
                first_error="lab job cancelled",
                completed_at=job.updated_at,
            )
        return self.registry.get_attempt(experiment_id)


class ExperimentLifecycleRecoveryResult(JobCenterModel):
    recovered_submission_count: int = Field(ge=0)
    synchronized_job_ids: tuple[UUID, ...]


class ExperimentLifecycleCoordinator:
    """Recover the durable submission outbox and converge every owned job attempt."""

    _MAX_RECOVERY_JOBS = 999

    def __init__(self, facade: LabCommandSubmissionFacade) -> None:
        if facade.experiment_registry is None:
            raise RuntimeError("experiment lifecycle coordinator requires ExperimentRegistry")
        self.facade = facade
        self.registry = facade.experiment_registry

    def validate_submission(
        self,
        envelope: LabCommandEnvelope,
        *,
        observed_at: datetime,
    ) -> None:
        command = envelope.command
        if not isinstance(command, SubmitJobCommand):
            return
        if command.spec.schema_version == 2:
            if command.spec.research_status != "exploratory":
                raise FormalSubmissionAuthorityError(
                    "new v2 comparable submissions require explicit exploratory migration"
                )
            return
        if command.spec.schema_version != 3:
            return
        self.facade.validate_prepared_experiment_submission(
            envelope,
            observed_at=observed_at,
        )

    def synchronize(
        self,
        job_id: UUID,
        *,
        observed_at: datetime,
    ) -> ExperimentAttempt | None:
        intent = self.registry.get_submission_intent_for_job(job_id)
        job = self.facade.reader.get_job(job_id)
        if job is None:
            if intent is None:
                return None
            raise RuntimeError("experiment-owned job is missing from Job Center authority")
        if intent is None:
            if job.spec.schema_version == 3:
                raise RuntimeError("v3 job is missing Experiment Registry submission ownership")
            return None
        experiment = job.spec.experiment
        if (
            job.spec.schema_version != 3
            or experiment is None
            or experiment.experiment_id != intent.experiment_id
            or experiment.attempt_identity != intent.attempt_identity
        ):
            raise RuntimeError("Job Center and Experiment Registry ownership conflict")
        return self.facade.synchronize_experiment_lifecycle(
            job_id,
            observed_at=observed_at,
        )

    def recover(self, *, observed_at: datetime) -> ExperimentLifecycleRecoveryResult:
        recovered = self.facade.recover_pending_experiment_submissions(
            limit=self._MAX_RECOVERY_JOBS
        )
        intents = self.registry.list_recoverable_submission_intents(
            limit=self._MAX_RECOVERY_JOBS + 1
        )
        if len(intents) > self._MAX_RECOVERY_JOBS:
            raise RuntimeError("experiment lifecycle recovery exceeds its bounded job budget")
        synchronized: list[UUID] = []
        for intent in intents:
            if self.facade.reader.get_job(intent.job_id) is None:
                continue
            self.synchronize(intent.job_id, observed_at=observed_at)
            synchronized.append(intent.job_id)
        return ExperimentLifecycleRecoveryResult(
            recovered_submission_count=len(recovered),
            synchronized_job_ids=tuple(synchronized),
        )
