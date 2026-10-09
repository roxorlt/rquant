"""Strict public values for the private, formally registered experiment path."""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, JsonValue

from rquant.experiment_platform import (
    ExperimentSearchRequest,
    HoldoutPolicy,
    NativeMinuteExperimentRequest,
    Parameter,
)
from rquant.experiment_registry import DateRange
from rquant.research_run_spec import ExecutionCostSpec
from rquant.minute_backtest_formal import MinuteExperimentProtocol
from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile
from rquant.overfit import (
    DeflatedSharpeResult,
    MinimumTrackRecordLengthResult,
    ProbabilisticSharpeResult,
)
from rquant.overfit_pbo import CSCVPBOResult
from rquant.perf import PerformanceSummary
from rquant.portfolio_backtest_models import PortfolioPerformance
from rquant.portfolio_backtest_source import PortfolioExperimentProtocol
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strategy_authoring_commands import StrategyTemplateHead
from rquant.strategy_template import StrategyTemplate
from rquant.strategy_promotion_contracts import NativeMinuteConfiguration, StrategyPromotionTarget
from rquant.web.models.backtests import PortfolioEditableConfig


class ExperimentEditableRequest(ExperimentSearchRequest):
    base_config: PortfolioEditableConfig


class ExperimentSourceOption(RuntimeContractModel):
    key: str
    version: int
    label: str
    start_date: date
    end_date: date
    trading_dates: tuple[date, ...]
    available: bool
    message: str | None = None


class ExperimentCapabilities(RuntimeContractModel):
    available: bool
    can_search: bool
    can_unseal: bool
    can_edit_policy: bool
    message: str | None
    sources: tuple[ExperimentSourceOption, ...] = ()
    default_config: PortfolioEditableConfig | None = None
    policy: HoldoutPolicy | None = None
    can_search_templates: bool = False


class ExperimentMetric(RuntimeContractModel):
    key: str
    label: str
    value: float | None = Field(allow_inf_nan=False)
    unit: Literal["percent", "number", "days", "count"]


class ExperimentAttemptRow(RuntimeContractModel):
    experiment_id: str
    family_id: str
    family_name: str
    phase: Literal["search", "outer"]
    registered_at: datetime
    status: Literal["registered", "running", "executed", "succeeded", "failed", "cancelled"]
    label: str
    index: int
    configuration: PortfolioEditableConfig | NativeMinuteConfiguration
    job_id: UUID
    result_hash: str | None = None
    message: str | None = None
    cancellation_pending: bool = False
    strategy_name: str = "组合回测"
    strategy_version: int = Field(default=1, ge=1)
    rules: StrategyTemplate | None = None
    metrics: tuple[ExperimentMetric, ...] = ()


class ExperimentPreparationFamily(RuntimeContractModel):
    family_id: str
    name: str
    registered_at: datetime
    state: Literal["preparing", "cancelled"]
    planned_count: int
    definition_saved_count: int
    input_prepared_count: int
    failed_count: int
    cancelled_count: int


class ExperimentMineData(RuntimeContractModel):
    available: bool
    items: tuple[ExperimentAttemptRow, ...] = ()
    retained_count: int = Field(ge=0, le=500)
    truncated: bool
    oldest_registered_at: datetime | None = None
    next_cursor: str | None = None
    preparing_families: tuple[ExperimentPreparationFamily, ...] = ()
    preparing_window_truncated: bool = False


class ExperimentPreparationRow(RuntimeContractModel):
    index: int = Field(ge=0, le=63)
    configuration: PortfolioEditableConfig | NativeMinuteConfiguration
    definition_state: Literal["pending", "saved", "failed", "cancelled"]
    input_prepared: bool
    failure: Literal["capacity", "source_changed", "invalid_definition"] | None = None
    strategy_name: str = "组合回测"
    strategy_version: int = Field(default=1, ge=1)
    rules: StrategyTemplate | None = None
    metrics: tuple[ExperimentMetric, ...] = ()


class ExperimentFamilyData(RuntimeContractModel):
    family_id: str
    name: str
    phase: Literal["search", "outer"]
    parent_family_id: str | None = None
    registered_at: datetime
    protocol: PortfolioExperimentProtocol | MinuteExperimentProtocol
    planned_count: int
    potential_count: int
    failed_count: int
    cancelled_count: int
    search_count: int
    parameters: tuple[Parameter, ...]
    items: tuple[ExperimentAttemptRow, ...]
    note: str
    note_version: int
    outer_admitted: bool
    preparation_state: Literal["preparing", "ready", "cancelled"] = "ready"
    preparations: tuple[ExperimentPreparationRow, ...] = ()


class ExperimentCurvePoint(RuntimeContractModel):
    trade_date: date
    nav: float = Field(allow_inf_nan=False)
    daily_return: float = Field(allow_inf_nan=False)
    benchmark_nav: float | None = Field(default=None, allow_inf_nan=False)


class ExperimentPhasePerformance(RuntimeContractModel):
    phase: Literal["training", "validation", "outer"]
    window: DateRange
    summary: PerformanceSummary | None
    metrics: tuple[ExperimentMetric, ...]
    curves: tuple[ExperimentCurvePoint, ...]
    message: str | None = None


class ExperimentTemplateResultIdentity(RuntimeContractModel):
    strategy_id: str
    head: StrategyTemplateHead
    rules: StrategyTemplate
    content_hash: str


class ExperimentNativeParameter(RuntimeContractModel):
    name: str = Field(min_length=1, max_length=128)
    value: JsonValue
    label: str | None = Field(default=None, max_length=80)
    display_value: str | None = Field(default=None, max_length=96)


class ExperimentNativeResultIdentity(RuntimeContractModel):
    target: StrategyPromotionTarget
    profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    core_input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    seed_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_kind: Literal["captured", "reconstructed"]
    execution_costs: ExecutionCostSpec
    parameters: tuple[ExperimentNativeParameter, ...] = ()
    execution_profile: MinuteReplayExecutionProfile | None = None
    execution: Literal["minute_runtime_replay@2"] = "minute_runtime_replay@2"


class ExperimentResultData(RuntimeContractModel):
    experiment_id: str
    family_id: str
    job_id: UUID
    phase: Literal["search", "outer"]
    configuration: PortfolioEditableConfig | NativeMinuteConfiguration
    result_hash: str
    input_hash: str
    spec_hash: str
    manifest_hash: str
    basis_hash: str
    performance: PortfolioPerformance
    phases: tuple[ExperimentPhasePerformance, ...]
    metrics: tuple[ExperimentMetric, ...]
    curves: tuple[ExperimentCurvePoint, ...]
    template: ExperimentTemplateResultIdentity | None = None
    native: ExperimentNativeResultIdentity | None = None


class ExperimentParameterDifference(RuntimeContractModel):
    path: str
    a: str | None
    b: str | None


class ExperimentComparisonData(RuntimeContractModel):
    a: ExperimentResultData
    b: ExperimentResultData
    differences: tuple[ExperimentParameterDifference, ...]
    comparable: bool
    message: str | None
    metric_differences: tuple[ExperimentMetric, ...]


class ExperimentHeatmapCell(RuntimeContractModel):
    x: str
    y: str
    experiment_id: str | None
    status: str
    value: float | None = Field(allow_inf_nan=False)


class ExperimentHeatmapData(RuntimeContractModel):
    family_id: str
    selected_experiment_id: str
    x_parameter: str
    y_parameter: str
    x_values: tuple[str, ...]
    y_values: tuple[str, ...]
    phase: Literal["training", "validation", "outer"]
    metric: str
    fixed_parameters: tuple[ExperimentParameterDifference, ...]
    cells: tuple[ExperimentHeatmapCell, ...]
    neighbor_count: int = Field(ge=0, le=8)
    available_neighbors: int = Field(ge=0, le=8)
    neighbor_minimum: float | None = Field(allow_inf_nan=False)
    complete_neighborhood: bool


class ExperimentStatisticsData(RuntimeContractModel):
    family_id: str
    experiment_id: str
    search_count: int
    failed_count: int
    cancelled_count: int
    evidence_id: str
    psr: ProbabilisticSharpeResult | None = None
    dsr: DeflatedSharpeResult | None = None
    mintrl: MinimumTrackRecordLengthResult | None = None
    pbo: CSCVPBOResult | None = None
    bh_adjusted_p: float | None = Field(default=None, allow_inf_nan=False)
    reasons: tuple[str, ...] = ()


class _WriteRequest(RuntimeContractModel):
    command_id: UUID
    requested_at: datetime


class ExperimentSearchWrite(_WriteRequest):
    kind: Literal["register_experiment_family"] = "register_experiment_family"
    request: ExperimentEditableRequest | NativeMinuteExperimentRequest


class ExperimentCancelWrite(_WriteRequest):
    kind: Literal["cancel_experiment_family"] = "cancel_experiment_family"
    family_id: str = Field(max_length=100)


class ExperimentNoteWrite(_WriteRequest):
    kind: Literal["set_experiment_note"] = "set_experiment_note"
    family_id: str = Field(max_length=100)
    expected_version: int = Field(strict=True, ge=0)
    text: str = Field(max_length=1024)


class ExperimentUnsealWrite(_WriteRequest):
    kind: Literal["unseal_experiment_outer_test"] = "unseal_experiment_outer_test"
    family_id: str = Field(max_length=100)
    experiment_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    confirmed: Literal[True]


class ExperimentPolicyWrite(_WriteRequest):
    kind: Literal["set_experiment_holdout_policy"] = "set_experiment_holdout_policy"
    months: int = Field(strict=True, ge=0, le=36)
    expected_version: int = Field(strict=True, ge=0)


ExperimentWrite = Annotated[
    ExperimentSearchWrite
    | ExperimentCancelWrite
    | ExperimentNoteWrite
    | ExperimentUnsealWrite
    | ExperimentPolicyWrite,
    Field(discriminator="kind"),
]


class ExperimentWriteReceipt(RuntimeContractModel):
    command_id: UUID
    status: Literal[
        "pending",
        "processing",
        "unknown",
        "failed",
        "registered",
        "cancellation_pending",
        "cancelled",
        "already_completed",
        "already_finished",
        "note_saved",
        "policy_saved",
        "outer_admitted",
    ]
    message: str
    family_id: str | None = None
    job_ids: tuple[UUID, ...] = ()
    planned_count: int | None = None
    version: int | None = None
