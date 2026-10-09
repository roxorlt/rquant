"""Browser selectors and bounded views of the original native minute results."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, JsonValue, StringConstraints, TypeAdapter, field_validator, model_validator

from rquant.minute_backtest_commands import MinuteParameterRunConfig, MinuteRunConfig
from rquant.minute_backtest_parameters import MinuteFrequency, MinuteParameterSet
from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile, MinuteReplayModel, Sha256
from rquant.minute_backtest_performance import MinuteReplayPerformance
from rquant.minute_backtest_performance import MinutePerformanceDay
from rquant.minute_backtest_formal import MinuteExperimentProtocol
from rquant.minute_backtest_parameter_study import MinuteParameterStudySettings
from rquant.minute_backtest_parameter_study_commands import (
    MinuteParameterStudyExecutionRequest, MinuteParameterStudyWindowSettings, SubmitMinuteParameterStudy,
)
from rquant.minute_backtest_parameter_search import MinuteParameterSearchRequest
from rquant.minute_backtest_parameter_optimizer import MinuteStudyTrainingRank, MinuteStudyTrainingSummary
from rquant.minute_backtest_parameter_heatmap import MinuteStudyHeatmap
from rquant.experiment_registry import DateRange
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strict_json import canonical_json_bytes

MinuteNativeId = Literal["n_shape", "auction_gap", "growth_board_surge"]
MinuteParameterDefinitionId = Annotated[str, StringConstraints(pattern=r"^(?:np|ap|gp)\.[a-z2-7]{52}$")]
MinuteTableName = Literal["signals", "orders", "fills", "paper_queue", "account", "daily_valuations", "execution_profile", "replay_summary"]
MinuteStudyMode = Literal["single", "grid", "random", "ablation", "walk_forward"]
MinuteStudyStatus = Literal["pending", "processing", "unknown", "submitted", "complete", "unavailable", "failed", "conflict"]
MinuteStudyTrialStatus = Literal["not_prepared", "awaiting_submission_receipt", "pending", "queued", "running",
    "checkpointed", "pending_seal", "sealed", "failed", "cancelled", "rejected", "unknown"]


class MinuteStudyCreateRequest(MinuteReplayModel):
    """A browser choice; actor, registration, publication and preparation stay private."""

    command_id: UUID
    requested_at: AwareUtcDatetime
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(strict=True, ge=1)
    full_input_hash: Sha256
    parameters: MinuteParameterSet
    protocol: MinuteExperimentProtocol
    settings: tuple[MinuteParameterStudySettings, ...] = Field(min_length=1, max_length=20_000)
    random_seed: int = Field(strict=True, ge=0, lt=2**63)
    deadline: AwareUtcDatetime
    mode: MinuteStudyMode
    search: MinuteParameterSearchRequest | None = None
    walk_forward: MinuteParameterStudyWindowSettings | None = None

    @field_validator("command_id", mode="before")
    @classmethod
    def original_uuid(cls, value: object) -> object:
        return TypeAdapter(UUID).validate_json(canonical_json_bytes(value), strict=True) if isinstance(value, str) else value

    @field_validator("requested_at", "deadline", mode="before")
    @classmethod
    def original_times(cls, value: object) -> object:
        return TypeAdapter(AwareUtcDatetime).validate_json(canonical_json_bytes(value), strict=True) if isinstance(value, str) else value

    @field_validator("parameters", mode="before")
    @classmethod
    def complete_parameters_json(cls, value: object) -> object:
        return MinuteParameterSet.model_validate_json(canonical_json_bytes(value)) if isinstance(value, dict) else value

    @field_validator("protocol", mode="before")
    @classmethod
    def complete_protocol_json(cls, value: object) -> object:
        return MinuteExperimentProtocol.model_validate_json(canonical_json_bytes(value)) if isinstance(value, dict) else value

    @field_validator("settings", mode="before")
    @classmethod
    def complete_settings_json(cls, value: object) -> object:
        return TypeAdapter(tuple[MinuteParameterStudySettings, ...]).validate_json(canonical_json_bytes(value)) if isinstance(value, list) else value

    @field_validator("search", mode="before")
    @classmethod
    def complete_search_json(cls, value: object) -> object:
        return MinuteParameterSearchRequest.model_validate_json(canonical_json_bytes(value)) if isinstance(value, dict) else value

    @field_validator("walk_forward", mode="before")
    @classmethod
    def complete_walk_forward_json(cls, value: object) -> object:
        return MinuteParameterStudyWindowSettings.model_validate_json(canonical_json_bytes(value)) if isinstance(value, dict) else value

    def to_command(self, *, authenticated_actor_id: str) -> SubmitMinuteParameterStudy:
        request = MinuteParameterStudyExecutionRequest(request_id=self.command_id, owner_id=authenticated_actor_id,
            source_key=self.source_key, source_version=self.source_version, full_input_hash=self.full_input_hash,
            parameters=self.parameters, formal_protocol=self.protocol, settings=self.settings, random_seed=self.random_seed,
            requested_at=self.requested_at, deadline=self.deadline, mode=self.mode, search=self.search,
            walk_forward=self.walk_forward)
        return SubmitMinuteParameterStudy(command_id=str(self.command_id), requested_at=self.requested_at,
            actor_id=authenticated_actor_id, request=request)

    @model_validator(mode="after")
    def original_control_validation(self) -> Self:
        # This pure model validates selection controls, not a role or source authority.
        self.to_command(authenticated_actor_id="unbound-wire-validation")
        return self


class MinuteStudyScoreProfile(RuntimeContractModel):
    name: str = Field(min_length=1, max_length=128)
    label: str = Field(min_length=1, max_length=128)
    available: bool
    missing_features: tuple[str, ...] = Field(default=(), max_length=256)


class MinuteStudySubmittedJob(RuntimeContractModel):
    index: int = Field(ge=0, lt=20_000)
    job_id: UUID


class MinuteStudyCommandReceipt(RuntimeContractModel):
    command_id: UUID
    status: Literal["pending", "processing", "unknown", "submitted", "unavailable", "failed", "conflict"]
    plan_id: Sha256 | None = None
    jobs: tuple[MinuteStudySubmittedJob, ...] = Field(default=(), max_length=20_000)
    unavailable_reasons: tuple[str, ...] = Field(default=(), max_length=256)
    message: str


class MinuteStudyWindowData(RuntimeContractModel):
    window: DateRange
    status: Literal["complete", "unavailable"]
    summary: MinuteStudyTrainingSummary | None
    daily: tuple[MinutePerformanceDay, ...] = Field(max_length=1830)
    cross_window_trades: int = Field(ge=0)
    unavailable_reasons: tuple[str, ...] = Field(default=(), max_length=256)


class MinuteStudyTrialData(RuntimeContractModel):
    index: int = Field(ge=0, lt=20_000)
    fold: int | None = Field(default=None, ge=1)
    label: str
    job_id: UUID
    state: MinuteStudyTrialStatus
    parameters: MinuteParameterSet
    settings: MinuteParameterStudySettings
    protocol: MinuteExperimentProtocol
    request_hash: Sha256
    study_id: Sha256 | None = None
    full_input_hash: Sha256 | None = None
    core_input_hash: Sha256 | None = None
    seed_hash: Sha256 | None = None
    profile_hash: Sha256 | None = None
    spec_hash: Sha256 | None = None
    manifest_hash: Sha256 | None = None
    complete_result_hash: Sha256 | None = None
    result_hash: Sha256 | None = None
    completed_at: AwareUtcDatetime | None = None
    training: MinuteStudyWindowData | None = None
    validation: MinuteStudyWindowData | None = None
    out_of_sample: MinuteStudyWindowData | None = None
    training_rank: MinuteStudyTrainingRank | None = None
    unavailable_reasons: tuple[str, ...] = Field(default=(), max_length=256)


class MinuteStudyListItem(RuntimeContractModel):
    command_id: UUID
    mode: MinuteStudyMode
    family: MinuteNativeId
    display_name: str
    source_key: str
    source_version: int = Field(ge=1)
    full_input_hash: Sha256
    status: Literal["pending", "processing", "unknown", "submitted", "unavailable", "failed"]
    requested_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime | None
    plan_id: Sha256 | None = None
    trial_count: int | None = Field(default=None, ge=0, le=20_000)
    submitted_count: int = Field(default=0, ge=0, le=20_000)


class MinuteStudiesData(RuntimeContractModel):
    available: bool
    studies: tuple[MinuteStudyListItem, ...] = Field(default=(), max_length=50)
    next_cursor: str | None = None
    message: str | None = None


class MinuteStudyResultData(RuntimeContractModel):
    command_id: UUID
    request: MinuteStudyCreateRequest
    status: MinuteStudyStatus
    plan_id: Sha256 | None = None
    trial_count: int | None = Field(default=None, ge=0, le=20_000)
    read_at: AwareUtcDatetime | None = None
    trials: tuple[MinuteStudyTrialData, ...] = Field(default=(), max_length=20_000)
    missing_trial_indices: tuple[int, ...] = Field(default=(), max_length=20_000)
    unavailable_reasons: tuple[str, ...] = Field(default=(), max_length=256)
    message: str | None = None

    @model_validator(mode="after")
    def original_parent(self) -> Self:
        if self.command_id != self.request.command_id:
            raise ValueError("public study result differs from its original parent UUID")
        return self


class MinuteStudyHeatmapData(RuntimeContractModel):
    command_id: UUID
    plan_id: Sha256
    read_at: AwareUtcDatetime
    heatmap: MinuteStudyHeatmap


class MinuteCreateRequest(MinuteReplayModel):
    command_id: UUID
    requested_at: AwareUtcDatetime
    config: MinuteRunConfig | MinuteParameterRunConfig

    @field_validator("command_id", mode="before")
    @classmethod
    def validate_command_json(cls, value: object) -> object:
        return TypeAdapter(UUID).validate_json(canonical_json_bytes(value), strict=True) if isinstance(value, str) else value

    @field_validator("requested_at", mode="before")
    @classmethod
    def validate_time_json(cls, value: object) -> object:
        return TypeAdapter(AwareUtcDatetime).validate_json(canonical_json_bytes(value), strict=True) if isinstance(value, str) else value

    @field_validator("config", mode="before")
    @classmethod
    def validate_config_json(cls, value: object) -> object:
        # FastAPI decodes the body first; the original strict nested contract still owns wire validation.
        return TypeAdapter(MinuteRunConfig | MinuteParameterRunConfig).validate_json(canonical_json_bytes(value)) if isinstance(value, dict) else value


class MinuteExportRequest(MinuteReplayModel):
    command_id: UUID
    requested_at: AwareUtcDatetime
    job_id: UUID
    result_hash: Sha256

    @field_validator("command_id", "job_id", mode="before")
    @classmethod
    def validate_identifier_json(cls, value: object) -> object:
        return TypeAdapter(UUID).validate_json(canonical_json_bytes(value), strict=True) if isinstance(value, str) else value

    @field_validator("requested_at", mode="before")
    @classmethod
    def validate_time_json(cls, value: object) -> object:
        return TypeAdapter(AwareUtcDatetime).validate_json(canonical_json_bytes(value), strict=True) if isinstance(value, str) else value


class MinuteSourceProvenance(RuntimeContractModel):
    source_kind: Literal["captured", "reconstructed"]
    extracted_at: AwareUtcDatetime
    published_at: AwareUtcDatetime
    replay_start: AwareUtcDatetime
    replay_end: AwareUtcDatetime
    acquisition_commits: tuple[str, ...]
    real_capture_times: tuple[AwareUtcDatetime, ...]
    research_code_commit: str
    visibility_policy_id: str | None
    visibility_policy_version: int | None
    visibility_limitations: str | None


class MinuteSourceOption(RuntimeContractModel):
    source_key: str
    source_version: int
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    native_id: MinuteNativeId
    native_name: str = Field(min_length=1, max_length=60)
    native_version: int
    native_registration_hash: Sha256
    native_executable_fingerprint: Sha256
    wrapper_registration_hash: Sha256
    profile_hash: Sha256
    dataset_snapshot_id: Sha256
    start_date: date
    end_date: date
    work_units: int
    provenance: MinuteSourceProvenance


class MinuteParameterResultSource(MinuteSourceOption):
    kind: Literal["minute_parameter_replay"] = "minute_parameter_replay"
    native_id: MinuteParameterDefinitionId
    family: MinuteNativeId
    parameters: MinuteParameterSet
    parameter_hash: Sha256
    evaluator_semantic_version: Literal["2.0.0", "2.1.0"] = Field(
        default_factory=lambda data: data["parameters"].evaluator_semantic_version
    )
    baseline_source_key: str
    baseline_source_version: int = Field(ge=1)
    baseline_full_input_hash: Sha256
    source_nature: Literal["real_retained", "historical_reconstruction", "synthetic_validation"]

    @model_validator(mode="after")
    def exact_complete_parameters(self) -> Self:
        if (self.family, self.native_id, self.native_version, self.parameter_hash, self.evaluator_semantic_version) != (
            self.parameters.parameters.family, self.parameters.definition_id, self.parameters.definition_version,
            self.parameters.fingerprint, self.parameters.evaluator_semantic_version):
            raise ValueError("parameter result source differs from its complete registered recipe")
        return self


class MinuteSourcesData(RuntimeContractModel):
    available: bool
    sources: tuple[MinuteSourceOption, ...] = Field(default=(), max_length=100)
    unavailable_count: int = Field(default=0, ge=0, le=100)
    message: str | None = None


class MinuteParameterCapability(RuntimeContractModel):
    family: MinuteNativeId
    display_name: str = Field(min_length=1, max_length=60)
    default_parameters: MinuteParameterSet
    supported_parameter_names: tuple[str, ...] = Field(max_length=256)
    unavailable_reasons: tuple[str, ...] = Field(default=(), max_length=256)


class MinuteParameterFactSourceOption(RuntimeContractModel):
    source_key: str
    source_version: int = Field(ge=1)
    full_input_hash: Sha256
    display_name: str = Field(min_length=1, max_length=120)
    start_date: date
    end_date: date
    frequency: MinuteFrequency
    source_nature: Literal["real_retained", "historical_reconstruction", "synthetic_validation"]
    provenance: MinuteSourceProvenance
    capabilities: tuple[MinuteParameterCapability, ...] = Field(min_length=1, max_length=3)
    unavailable_reasons: tuple[str, ...] = Field(default=(), max_length=256)


class MinuteParameterSourcesData(RuntimeContractModel):
    available: bool
    sources: tuple[MinuteParameterFactSourceOption, ...] = Field(default=(), max_length=100)
    unavailable_count: int = Field(default=0, ge=0, le=100)
    message: str | None = None


class MinuteStudySourceCapability(RuntimeContractModel):
    source: MinuteParameterFactSourceOption
    modes: tuple[MinuteStudyMode, ...]
    score_profiles: tuple[MinuteStudyScoreProfile, ...] = Field(max_length=11)
    searchable_parameter_names: tuple[str, ...] = Field(max_length=256)
    heatmap_parameter_names: tuple[str, ...] = Field(max_length=256)
    unavailable_reasons: tuple[str, ...] = Field(default=(), max_length=256)


class MinuteStudyCapabilitiesData(RuntimeContractModel):
    available: bool
    can_run: bool
    sources: tuple[MinuteStudySourceCapability, ...] = Field(default=(), max_length=100)
    source_unavailable_count: int = Field(default=0, ge=0, le=100)
    message: str | None = None


class MinuteCapabilities(RuntimeContractModel):
    available: bool
    can_run: bool
    can_export: bool = False
    source_count: int = Field(default=0, ge=0, le=100)
    source_unavailable_count: int = Field(default=0, ge=0, le=100)
    message: str | None = None
    valuation_basis: Literal["pit_asof_15:00"] = "pit_asof_15:00"


class MinuteCommandReceipt(RuntimeContractModel):
    command_id: UUID
    status: Literal["pending", "processing", "unknown", "submitted", "failed", "conflict", "exported"]
    job_id: UUID | None = None
    zip_request_id: UUID | None = None
    result_hash: Sha256 | None = None
    sha256: Sha256 | None = None
    byte_size: int | None = Field(default=None, ge=0, le=32 * 1024 * 1024)
    message: str


class MinuteJob(RuntimeContractModel):
    job_id: UUID
    status: Literal["queued", "running", "paused", "cancelled", "failed", "sealing", "completed"]
    version: int
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    spec_hash: Sha256
    source_key: str
    source_version: int
    full_input_hash: Sha256
    native_id: MinuteNativeId
    native_name: str = Field(min_length=1, max_length=60)
    native_version: int
    start_date: date
    end_date: date
    result_hash: Sha256 | None


class MinuteParameterJob(MinuteJob):
    kind: Literal["minute_parameter_replay"] = "minute_parameter_replay"
    native_id: MinuteParameterDefinitionId
    family: MinuteNativeId
    parameters: MinuteParameterSet
    parameter_hash: Sha256
    evaluator_semantic_version: Literal["2.0.0", "2.1.0"] = Field(
        default_factory=lambda data: data["parameters"].evaluator_semantic_version
    )

    @model_validator(mode="after")
    def exact_complete_parameters(self) -> Self:
        if (self.family, self.native_id, self.native_version, self.parameter_hash, self.evaluator_semantic_version) != (
            self.parameters.parameters.family, self.parameters.definition_id, self.parameters.definition_version,
            self.parameters.fingerprint, self.parameters.evaluator_semantic_version):
            raise ValueError("parameter job differs from its complete registered recipe")
        return self


class MinuteJobsData(RuntimeContractModel):
    available: bool
    jobs: tuple[MinuteJob | MinuteParameterJob, ...] = Field(default=(), max_length=50)
    next_cursor: str | None = None
    message: str | None = None


class MinuteSummaryData(RuntimeContractModel):
    job: MinuteJob | MinuteParameterJob
    source: MinuteSourceOption | MinuteParameterResultSource
    result_hash: Sha256 | None = None
    daily_status: Literal["complete", "unavailable"] | None = None
    signal_count: int | None = None
    order_count: int | None = None
    fill_count: int | None = None
    queue_count: int | None = None
    execution_profile: MinuteReplayExecutionProfile | None = None
    performance: MinuteReplayPerformance | None = None
    can_report: bool = False
    tables: tuple[MinuteTableName, ...] = ()
    message: str | None = None


class MinutePriceTime(RuntimeContractModel):
    code: str
    event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    quote_snapshot_id: Sha256 | None


class MinuteNavPoint(RuntimeContractModel):
    trade_date: date
    as_of: AwareUtcDatetime
    basis: Literal["pit_asof_15:00"]
    status: Literal["complete", "unavailable"]
    nav: Decimal | None
    cash: Decimal | None
    account_snapshot_id: Sha256 | None
    profile_hash: Sha256
    price_times: tuple[MinutePriceTime, ...] = Field(max_length=500)
    unavailable_reasons: tuple[str, ...]


class MinuteNavData(RuntimeContractModel):
    job_id: UUID
    result_hash: Sha256
    daily_status: Literal["complete", "unavailable"]
    basis: Literal["pit_asof_15:00"] = "pit_asof_15:00"
    points: tuple[MinuteNavPoint, ...] = Field(max_length=1830)
    message: str | None = None


class MinuteResultRow(RuntimeContractModel):
    sequence: int = Field(ge=0)
    payload: JsonValue


class MinuteRowsData(RuntimeContractModel):
    job_id: UUID
    result_hash: Sha256
    table: MinuteTableName
    rows: tuple[MinuteResultRow, ...] = Field(max_length=50)
    total: int = Field(ge=0)
    next_offset: int | None
