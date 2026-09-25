"""``GET /api/v1/overview``: one trading day at a glance."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from rquant.web.market import MarketPhase
from rquant.web.models.common import StateCounts

StageState = Literal["done", "running", "waiting", "paused", "late"]
DeliveryState = Literal["delivered", "sending", "failed", "expired", "none"]


class SessionInfo(BaseModel):
    """Which trading day the numbers are for."""

    model_config = ConfigDict(frozen=True)

    today: date
    #: The session shown: today once it has started, else the last trading day before.
    #: None when the trade calendar cannot say.
    trade_date: date | None
    is_today: bool
    phase: MarketPhase
    next_trading_day: date | None


class PipelineStage(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    name: str
    window: str
    state: StageState
    state_label: str
    value: str | None
    hint: str


class CandidateGroup(BaseModel):
    model_config = ConfigDict(frozen=True)

    key: str
    name: str
    count: int
    as_of: date | None
    source: Literal["screen", "signals"]


class CandidateItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str
    name: str | None
    group: str
    group_name: str
    close: float | None
    pct_chg: float | None
    first_seen_at: datetime | None


class CandidatesSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    total: int
    groups: list[CandidateGroup]
    items: list[CandidateItem]


class ActionCount(BaseModel):
    model_config = ConfigDict(frozen=True)

    action: str
    label: str
    count: int


class SignalItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    #: For the tooltip only.
    signal_id: str
    sequence: int
    at: datetime
    code: str
    name: str | None
    strategy_id: str
    strategy_name: str
    action: str
    action_label: str
    delivery: DeliveryState
    delivery_label: str
    reasons: list[str]


class SignalsSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    total: int
    by_action: list[ActionCount]
    items: list[SignalItem]


class DeliveriesSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    total: int
    delivered: int
    sending: int
    failed: int
    expired: int


class HoldingItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    code: str
    name: str | None
    quantity: float
    available_quantity: float
    average_cost: float
    market_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pct: float | None


class PaperSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    account_id: str
    as_of: datetime
    nav: float
    cash: float
    unrealized_pnl: float
    realized_pnl: float
    holdings: list[HoldingItem]
    #: One plain sentence when the valuation has a caveat (tooltip).
    note: str | None


class FreshnessSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    on_time: int
    checked: int
    no_source: int
    late: list[str]


class AttentionItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    level: Literal["crit", "warn"]
    title: str
    reason: str
    to: str
    action: str


class OverviewData(BaseModel):
    model_config = ConfigDict(frozen=True)

    session: SessionInfo
    pipeline: list[PipelineStage]
    candidates: CandidatesSummary
    signals: SignalsSummary
    deliveries: DeliveriesSummary
    paper: PaperSummary | None
    services: StateCounts
    freshness: FreshnessSummary
    attention: list[AttentionItem]
