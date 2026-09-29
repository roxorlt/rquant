"""Small public views of verified retrospective factor research."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from rquant.factor.display_artifact import (
    FactorDisplayCoverageDay,
    FactorDisplayDecayPeriod,
    FactorDisplayICPoint,
)
from rquant.factor.job_ledger import FactorJobStatus
from rquant.factor.portfolio import FactorPortfolioDay
from rquant.factor.result import (
    HoldingSessions,
    ResearchPortfolioStatus,
    ResearchSummaryStatus,
    ReturnPriceBasis,
)
from rquant.factor.summary import FactorICSummary


class FactorResultItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str
    factor_id: str
    factor_version: int
    factor_name_zh: str | None
    definition_status: Literal["current", "historical_unavailable"]
    status: FactorJobStatus
    status_label: str
    failure_message: str | None
    updated_at: datetime
    as_of_time: datetime
    display_status: Literal["not_ready", "display_unavailable", "not_published", "available"]
    display_message: str


class FactorResultListData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    availability: Literal["unavailable", "empty", "populated"]
    available_at: datetime | None
    results: list[FactorResultItem]


class FactorResearchDisplay(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    basis_label: str
    pool_label: Literal["固定样本"]
    return_price_basis: ReturnPriceBasis
    holding_sessions: HoldingSessions
    summary_status: ResearchSummaryStatus
    ic_summary: FactorICSummary | None
    ic_points: list[FactorDisplayICPoint]
    decay_periods: list[FactorDisplayDecayPeriod]
    portfolio_status: ResearchPortfolioStatus
    portfolio_days: list[FactorPortfolioDay]
    coverage_days: list[FactorDisplayCoverageDay]


class FactorResultDetailData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    availability: Literal["unavailable", "empty", "not_found", "ready"]
    available_at: datetime | None
    result: FactorResultItem | None
    research: FactorResearchDisplay | None
