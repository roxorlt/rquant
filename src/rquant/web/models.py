"""Response shapes. Flat on purpose: the React types are generated from these."""

from __future__ import annotations

from datetime import date, datetime
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")
ServingState = Literal["ready", "stale", "degraded", "unavailable"]


class ServingMeta(BaseModel):
    state: ServingState
    generation_id: str | None = None
    generated_at: datetime | None = None
    message: str | None = None


class Envelope(BaseModel, Generic[T]):
    data: T
    serving: ServingMeta


class MetaData(BaseModel):
    app: str = "rQuant"
    version: str
    generation: ServingMeta
    #: deploy-time banner (env ``RQUANT_WEB_NOTICE``), e.g. "回放数据 2026-09-25"
    notice: str | None = None


class Kpi(BaseModel):
    key: str
    label: str
    value: str
    tone: Literal["ok", "warn", "crit"] | None = None


class SignalItem(BaseModel):
    sequence: int
    at: datetime | None
    code: str
    name: str | None
    strategy_id: str
    action: str


class OverviewData(BaseModel):
    trade_date: date | None
    kpis: list[Kpi]
    signals: list[SignalItem]


class ServiceItem(BaseModel):
    service_id: str
    plane: str
    status: str
    stale: bool
    heartbeat_at: datetime | None
    backlog_count: int
    consecutive_failures: int
    last_error: str | None


class FreshnessItem(BaseModel):
    key: str
    label: str
    value: str | None


class HealthData(BaseModel):
    services: list[ServiceItem]
    freshness: list[FreshnessItem]


class BoardItem(BaseModel):
    system: str
    board_code: str
    board_name: str
    amount: float | None
    main_net_amount: float | None
    pct_chg_median: float | None
    limit_up_count: int | None
    stock_count: int | None
    leading_stock: str | None


class MarketPulse(BaseModel):
    up: int
    down: int
    flat: int
    limit_up: int
    limit_down: int


class PanoramaData(BaseModel):
    as_of: datetime | None
    pulse: MarketPulse
    boards: list[BoardItem]


class ScreenRow(BaseModel):
    trade_date: date
    code: str
    name: str | None
    preset: str
    close: float | None
    pct_chg: float | None


class ScreenData(BaseModel):
    trade_date: date | None
    presets: list[str]
    rows: list[ScreenRow]


class PoolMember(BaseModel):
    code: str
    name: str | None
    preset: str
    detail: dict[str, object] = Field(default_factory=dict)


class PoolItem(BaseModel):
    name: str
    description: str
    pool_refs: list[str]
    updated_at: datetime | None
    members: list[PoolMember]


class PoolsData(BaseModel):
    trade_date: date | None
    pools: list[PoolItem]


class BacktestRun(BaseModel):
    run_id: str
    computed_at: datetime | None
    start_date: date | None
    end_date: date | None
    entry_mode: str
    profile_variant: str
    trades: int | None
    win_rate_pct: float | None
    mean_ret_pct: float | None
    median_ret_pct: float | None
    best_ret_pct: float | None
    worst_ret_pct: float | None


class BacktestTrade(BaseModel):
    trade_id: str
    signal_date: date | None
    code: str
    name: str | None
    entry_time: datetime | None
    entry_price: float | None
    exit_time: datetime | None
    exit_price: float | None
    exit_reason: str | None
    ret_pct: float | None


class BacktestListData(BaseModel):
    runs: list[BacktestRun]


class BacktestDetailData(BaseModel):
    run: BacktestRun
    trades: list[BacktestTrade]


class AlertItem(BaseModel):
    alert_id: str
    trade_date: date
    at: datetime | None
    code: str
    name: str | None
    level: str
    trigger_type: str | None
    trigger_price: float | None
    level_price: float | None
    pool: str | None
    acked_at: datetime | None = None
    acked_by: str | None = None


class AlertsData(BaseModel):
    items: list[AlertItem]


class HoldingItem(BaseModel):
    code: str
    name: str | None
    quantity: float
    average_cost: float
    market_price: float
    market_value: float
    unrealized_pnl: float


class PaperAccount(BaseModel):
    account_id: str
    as_of: datetime | None
    nav: float
    cash: float
    unrealized_pnl: float
    realized_pnl: float
    holdings: list[HoldingItem]


class PaperData(BaseModel):
    accounts: list[PaperAccount]


# ---- the three writes -------------------------------------------------------


class SavePoolRequest(BaseModel):
    name: str = Field(min_length=1, max_length=64)
    description: str = ""
    pool_refs: list[str] = Field(default_factory=list)


class AckAlertRequest(BaseModel):
    alert_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class AddWatchRequest(BaseModel):
    code: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    note: str = ""


class CommandReceipt(BaseModel):
    command_id: str | None = None
    status: str
    detail: str | None = None
