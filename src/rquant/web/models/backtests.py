"""Published minute replay results; these rows are not a portfolio equity curve."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.perf import PerformanceSummary, RelativeMetrics, ReturnDistribution, StreakSummary
from rquant.perf.trades import RoundTripAnalysis
from rquant.portfolio_backtest_models import PortfolioBacktestConfig, PortfolioRollingMetric
from rquant.research_run_spec import (
    ExecutionCostFeeRule,
    ExecutionCostMoney,
    ExecutionCostSlippage,
    ExecutionCostSpec,
    InstrumentSelector,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel


class BacktestRun(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    computed_at: datetime
    start_date: date
    end_date: date
    max_hold_days: int
    candidates: int
    trades: int
    configurations: int


class BacktestListData(BaseModel):
    model_config = ConfigDict(frozen=True)

    available: bool
    runs: list[BacktestRun]
    total: int
    next_offset: int | None


class BacktestGroup(BaseModel):
    model_config = ConfigDict(frozen=True)

    entry_mode: str
    entry_mode_label: str
    profile_variant: str
    profile_variant_label: str
    candidates: int
    trades: int
    trigger_rate_pct: float | None
    mean_ret_pct: float | None
    median_ret_pct: float | None
    win_rate_pct: float | None
    best_ret_pct: float | None
    worst_ret_pct: float | None
    gap_stop_rate_pct: float | None


class BacktestTrade(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_id: str
    entry_mode: str
    entry_mode_label: str
    profile_variant: str
    profile_variant_label: str
    signal_date: date
    ts_code: str
    name: str | None
    entry_time: datetime | None
    entry_price: float | None
    exit_time: datetime | None
    exit_price: float | None
    exit_reason: str | None
    exit_reason_label: str
    ret_pct: float | None


class BacktestDetailData(BaseModel):
    model_config = ConfigDict(frozen=True)

    summary_available: bool
    trades_available: bool
    run: BacktestRun | None
    groups: list[BacktestGroup]
    trades: list[BacktestTrade]
    total_trades: int
    next_offset: int | None


class PortfolioCostConfig(RuntimeContractModel):
    """Exact v3 JSON; validation remains owned by the shared cost contract."""

    schema_version: Literal[3] = 3
    cost_engine_version: str
    instrument_selectors: tuple[InstrumentSelector, ...]
    commission_rules: tuple[ExecutionCostFeeRule, ...]
    transfer_fee_rules: tuple[ExecutionCostFeeRule, ...]
    stamp_duty_rules: tuple[ExecutionCostFeeRule, ...]
    fee_notional_basis: Literal["EXECUTED_NOTIONAL"]
    assessment_unit: Literal["FILL"]
    slippage: ExecutionCostSlippage
    money: ExecutionCostMoney
    cost_spec_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    research_notional_per_trade: Decimal | None = None

    @model_validator(mode="after")
    def validate_shared_contract(self) -> Self:
        original = ExecutionCostSpec.model_validate(self.model_dump(mode="python"))
        object.__setattr__(self, "cost_spec_id", original.cost_spec_id)
        return self

    @property
    def is_alignment_eligible(self) -> bool:
        return True


class PortfolioEditableConfig(PortfolioBacktestConfig):
    execution_cost_spec: PortfolioCostConfig

    @classmethod
    def from_domain(cls, config: PortfolioBacktestConfig) -> PortfolioEditableConfig:
        return cls.model_validate(config.model_dump(mode="python"))

    def to_domain(self) -> PortfolioBacktestConfig:
        return PortfolioBacktestConfig.model_validate(self.model_dump(mode="python"))


class PortfolioSourceOption(RuntimeContractModel):
    key: str
    version: int = Field(ge=1)
    label: str = Field(min_length=1, max_length=60)
    start_date: date
    end_date: date
    updated_at: AwareUtcDatetime
    ranking_available: bool
    industry_available: bool
    opening_verified: bool


class PortfolioCapabilities(RuntimeContractModel):
    available: bool
    can_run: bool
    can_export: bool
    message: str | None
    sources: tuple[PortfolioSourceOption, ...] = ()
    benchmarks: tuple[str, ...] = ()
    default_config: PortfolioEditableConfig | None = None


class PortfolioCreateRequest(RuntimeContractModel):
    command_id: UUID
    requested_at: AwareUtcDatetime
    config: PortfolioEditableConfig


class PortfolioExportRequest(RuntimeContractModel):
    command_id: UUID
    requested_at: AwareUtcDatetime
    job_id: UUID
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class PortfolioCommandReceipt(RuntimeContractModel):
    command_id: UUID
    status: Literal[
        "submitted", "pending", "processing", "unknown", "failed", "conflict", "exported"
    ]
    message: str
    job_id: UUID | None = None
    zip_request_id: UUID | None = None
    result_hash: str | None = None
    sha256: str | None = None
    byte_size: int | None = None


class PortfolioJob(RuntimeContractModel):
    job_id: UUID
    status: Literal["queued", "running", "paused", "cancelled", "failed", "completed", "sealing"]
    label: str
    version: int
    start_date: date
    end_date: date
    created_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    result_hash: str | None
    progress: float | None
    can_pause: bool = False
    can_resume: bool = False
    can_cancel: bool = False
    can_retry: bool = False


class PortfolioJobsData(RuntimeContractModel):
    available: bool
    jobs: tuple[PortfolioJob, ...]
    next_cursor: str | None


class PortfolioPerformanceData(RuntimeContractModel):
    summary: PerformanceSummary
    benchmark_summary: PerformanceSummary | None
    relative: RelativeMetrics | None
    annualized_turnover: float | None
    rolling: tuple[PortfolioRollingMetric, ...]
    round_trip_analysis: RoundTripAnalysis
    distribution: ReturnDistribution
    streaks: StreakSummary
    overfit_state: Literal["not_evaluated"]


class PortfolioSummaryData(RuntimeContractModel):
    available: bool
    job: PortfolioJob
    result_hash: str | None
    result_status: Literal["complete", "incomplete"] | None
    config: PortfolioEditableConfig | None
    performance: PortfolioPerformanceData | None
    benchmark_available: bool
    benchmark_message: str | None
    source_updated_at: AwareUtcDatetime | None
    completed_days: int
    message: str | None
    can_report: bool


class PortfolioNavRow(RuntimeContractModel):
    trade_date: date
    nav: Decimal | None
    normalized_nav: Decimal | None
    daily_return: Decimal | None
    cash: Decimal | None
    market_value: Decimal | None
    fees: Decimal
    benchmark_nav: float | None
    benchmark_return: float | None
    drawdown: float | None
    rebalanced: bool
    incomplete_reason: Literal["missing_held_close"] | None


class PortfolioTradeRow(RuntimeContractModel):
    trade_date: date
    ts_code: str
    side: Literal["BUY", "SELL"]
    quantity: int
    price: Decimal | None
    amount: Decimal | None
    fees: Decimal | None
    status: str
    reason: str | None


class PortfolioHoldingRow(RuntimeContractModel):
    trade_date: date
    code: str
    quantity: int
    available_quantity: int
    frozen_quantity: int
    average_cost: Decimal
    market_price: Decimal


class PortfolioMonthRow(RuntimeContractModel):
    year: int
    month: int
    return_rate: float | None


class PortfolioLogRow(RuntimeContractModel):
    trade_date: date
    ts_code: str | None
    level: Literal["normal", "note", "error"]
    message: str


class PortfolioNavData(RuntimeContractModel):
    result_hash: str
    rows: tuple[PortfolioNavRow, ...]


class PortfolioRowsData(RuntimeContractModel):
    result_hash: str
    view: Literal["trades", "holdings", "daily", "monthly", "log"]
    trades: tuple[PortfolioTradeRow, ...] = ()
    holdings: tuple[PortfolioHoldingRow, ...] = ()
    daily: tuple[PortfolioNavRow, ...] = ()
    monthly: tuple[PortfolioMonthRow, ...] = ()
    log: tuple[PortfolioLogRow, ...] = ()
    total: int
    next_offset: int | None
