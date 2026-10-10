"""``/api/v1/panorama/*``: 市场全景 — pulse, boards, members, charts and the surge ledger."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

from rquant.web.models.common import StatusInfo


class PulseCounts(BaseModel):
    model_config = ConfigDict(frozen=True)

    limit_up: int
    limit_down: int
    broken: int
    up: int
    down: int
    flat: int
    total: int
    up_ratio_pct: float | None


class PulsePoint(BaseModel):
    model_config = ConfigDict(frozen=True)

    t: str
    limit_up: int | None
    broken: int | None
    limit_down: int | None
    up_ratio_pct: float | None


class PulseAlert(BaseModel):
    model_config = ConfigDict(frozen=True)

    t: str
    kind: str
    kind_label: str
    message: str


class PulseData(BaseModel):
    model_config = ConfigDict(frozen=True)

    #: The trading day the page shows (the snapshot's day, else the last session).
    trade_date: date | None
    #: When the market snapshot was taken; None without a snapshot.
    as_of: datetime | None
    age_seconds: float | None
    #: 行情快照 freshness in plain words (a stale snapshot only matters in session).
    freshness: StatusInfo
    #: None when neither the snapshot nor the minute history has numbers.
    counts: PulseCounts | None
    #: "snapshot" (computed from the full-market snapshot) or "history" (the last minute
    #: of the surge-watch pulse history, when the snapshot is missing).
    source: Literal["snapshot", "history"] | None
    history: list[PulsePoint]
    alerts: list[PulseAlert]
    #: The latest alert when it is at most 30 minutes old (today, in session).
    recent_alert: PulseAlert | None


class BoardRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    board_code: str
    board_name: str
    amount: float | None
    main_net_amount: float | None
    main_net_rate: float | None
    pct_chg_median: float | None
    limit_up_count: int | None
    broken_count: int | None
    limit_up_ratio_pct: float | None
    stock_count: int | None
    leading_stock: str | None


class BoardsData(BaseModel):
    model_config = ConfigDict(frozen=True)

    system: str
    systems: list[str]
    #: 开盘啦题材 has no fund-flow columns.
    has_flow: bool
    as_of: datetime | None
    rows: list[BoardRow]


class MemberRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    ts_code: str
    name: str | None
    price: float | None
    pct_chg: float | None
    amount: float | None
    strength: float | None
    turnover_pct: float | None
    rel_volume_5d: float | None
    is_limit_up: bool
    pools: list[str]


class MembersData(BaseModel):
    model_config = ConfigDict(frozen=True)

    board_code: str
    board_name: str | None
    rows: list[MemberRow]


class MinuteBar(BaseModel):
    model_config = ConfigDict(frozen=True)

    day: date
    #: "HH:MM" in Shanghai.
    t: str
    #: Position on the 241-point session axis: 09:30 → 0, 11:30 / 13:00 → 120, 15:00 → 240.
    slot: int
    price: float
    #: Running average price (Σ close×volume / Σ volume, estimated from minute closes).
    avg_price: float | None
    volume: float | None
    direction: Literal["up", "down", "flat"]


class SurgeMark(BaseModel):
    model_config = ConfigDict(frozen=True)

    day: date
    t: str
    slot: int
    price: float | None
    label: str
    count: int


class IntradayData(BaseModel):
    model_config = ConfigDict(frozen=True)

    ts_code: str
    name: str | None
    days: list[date]
    bars: list[MinuteBar]
    marks: list[SurgeMark]


class DailyBar(BaseModel):
    model_config = ConfigDict(frozen=True)

    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    ma5: float | None
    ma10: float | None
    ma20: float | None
    #: Today's bar made up from the live snapshot (daily data ends yesterday).
    provisional: bool


class DailyData(BaseModel):
    model_config = ConfigDict(frozen=True)

    ts_code: str
    name: str | None
    bars: list[DailyBar]


class SurgeRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_date: date
    confirmed_at: str
    ts_code: str
    name: str | None
    theme: str | None
    price: float | None
    pct_chg: float | None
    rel_cum: float | None
    cum_amount: float | None
    room_to_limit_pct: float | None
    status: str
    status_label: str


class SurgeConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    boards: list[str]
    k_cum: float
    ratio_cap: float
    #: One plain sentence describing the detection rule (footer / tooltip).
    summary: str


class SurgeData(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_date: date | None
    #: Open trading days through the current session, newest first (for the date picker).
    dates: list[date]
    rows: list[SurgeRow]
    config: SurgeConfig | None


class SurgeSearchData(BaseModel):
    model_config = ConfigDict(frozen=True)

    query: str
    rows: list[SurgeRow]
    #: True when more records match than the list holds.
    truncated: bool
