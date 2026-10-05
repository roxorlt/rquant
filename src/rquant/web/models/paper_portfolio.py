"""Published paper facts and bounded ownerless Web commands."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import Field

from rquant.paper_contracts import PaperAccountSnapshot
from rquant.paper_operator_commands import PaperOperatorApplication, SetPaperAccountPaused
from rquant.paper_portfolio_band import PaperBacktestBandResult
from rquant.paper_portfolio_models import PaperPortfolioRules, PaperRiskObservation, Sha256
from rquant.paper_portfolio_projection import PaperDailyNavPoint
from rquant.paper_portfolio_history import PaperHistoryPage
from rquant.paper_portfolio_ledger import PaperPortfolioHistoryRecord
from rquant.paper_portfolio_reductions import PaperReductionStatus
from rquant.paper_research_artifact import PaperResearchSummary
from rquant.portfolio.exposure import ExposureResult, AttributionResult
from rquant.research_run_spec import ExecutionCostSpec
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel


class PaperConfigurationView(PaperPortfolioRules):
    account_id: str
    strategy_id: str
    strategy_version: str
    strategy_name: str
    fingerprint: Sha256
    version: int = Field(ge=1, le=4096)
    configured_at: AwareUtcDatetime
    execution_cost_spec: ExecutionCostSpec


class PaperPortfolioMetrics(RuntimeContractModel):
    status: Literal["complete", "unavailable"]
    running_days: int = Field(ge=0, le=2520)
    verified_days: int = Field(ge=0, le=2520)
    total_return: float | None = Field(default=None, allow_inf_nan=False)
    max_drawdown: float | None = Field(default=None, allow_inf_nan=False)
    reason: str | None = None


class PaperPortfolioItem(RuntimeContractModel):
    configuration: PaperConfigurationView
    status: Literal["complete", "unavailable"]
    reason: str | None = None
    account: PaperAccountSnapshot | None
    operator: PaperOperatorApplication
    metrics: PaperPortfolioMetrics
    can_configure: bool = False
    can_pause: bool = False
    can_reconcile: bool = False
    can_band: bool = False


class PaperPortfolioCatalogData(RuntimeContractModel):
    availability: Literal["unavailable", "empty", "populated"]
    available_at: AwareUtcDatetime | None
    accounts: tuple[PaperPortfolioItem, ...] = Field(max_length=64)


class PaperPeriodAttributionView(RuntimeContractModel):
    start_at: AwareUtcDatetime
    end_at: AwareUtcDatetime
    status: Literal["complete", "unavailable"]
    result: AttributionResult | None
    reason: str | None = None


class PaperPortfolioDetailData(PaperPortfolioItem):
    nav: tuple[PaperDailyNavPoint, ...] = Field(max_length=2520)
    risk: PaperRiskObservation | None = None
    exposure: ExposureResult | None = None
    exposure_reason: str | None = None
    attribution: PaperPeriodAttributionView | None = None
    reduction: PaperReductionStatus | None = None
    band: PaperBacktestBandResult | None = None
    band_position: Literal["inside", "outside", "unavailable"] = "unavailable"
    recent_research: tuple[PaperResearchSummary, ...] = Field(max_length=20)
    history_available: bool = False
    backtests: tuple[PaperBacktestChoice, ...] = ()


class PaperBacktestChoice(RuntimeContractModel):
    job_id: UUID
    completed_at: AwareUtcDatetime
    name: str


class PaperPortfolioHistoryRecordView(PaperPortfolioHistoryRecord):
    side_label: str
    status_label: str
    reject_message: str | None


class PaperPortfolioHistoryPageView(PaperHistoryPage):
    records: tuple[PaperPortfolioHistoryRecordView, ...] = Field(max_length=200)


class PaperPauseConfirmBody(RuntimeContractModel):
    request: SetPaperAccountPaused
    confirmation_id: str = Field(min_length=1, max_length=64)


class PaperPausePreparationData(RuntimeContractModel):
    confirmation_id: str
    command: SetPaperAccountPaused
    expires_at: AwareUtcDatetime


class PaperPortfolioCommandData(RuntimeContractModel):
    command_id: UUID
    account_id: str
    status: Literal["rejected", "pending", "uncertain", "waiting_application", "waiting_publication", "applied", "published", "submitted"]
    configuration_fingerprint: Sha256 | None = None
    configuration_version: int | None = None
    sequence: int | None = None
    job_id: UUID | None = None
    message: str
