"""Published minute replay results; these rows are not a portfolio equity curve."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


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
