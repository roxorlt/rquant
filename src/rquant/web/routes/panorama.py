"""``/api/v1/panorama/*``: 市场全景, read from the current generation only.

Every endpoint borrows the tracker's generation for the one request; the pulse, the board
tables, the members, the charts and the surge ledger of one page load therefore agree as
long as the page reads them within one generation (the page refetches all of them when
/meta reports a new one).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Annotated, Any, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

from rquant.panorama_data import BOARD_SYSTEMS
from rquant.web import panorama, readers
from rquant.web.calendar import calendar_day, session_date
from rquant.web.envelope import Envelope
from rquant.web.market import MARKET_TIMEZONE, MarketPhase, market_phase, shanghai_trade_date
from rquant.web.models.common import StatusInfo
from rquant.web.models.panorama import (
    BoardsData,
    DailyData,
    IntradayData,
    MembersData,
    PulseData,
    SurgeData,
    SurgeSearchData,
)
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.status import Status, UserState

router = APIRouter(prefix="/panorama")

_DataT = TypeVar("_DataT")
_TS_CODE = r"^[0-9A-Z]{6}\.(SH|SZ|BJ)$"
#: A snapshot older than this during the session is flagged (the Streamlit page's rule).
_SNAPSHOT_STALE = timedelta(seconds=180)
_RECENT_ALERT = timedelta(minutes=30)
_IN_SESSION = frozenset(
    {
        MarketPhase.CALL_AUCTION,
        MarketPhase.CONTINUOUS,
        MarketPhase.CLOSING_AUCTION,
    }
)


class _Context:
    """What one panorama request derives from its borrowed generation."""

    def __init__(self, borrowed: BorrowedGeneration, now: datetime) -> None:
        self.borrowed = borrowed
        self.cursor = borrowed.cursor
        self.now = now
        self.tables = readers.table_states(borrowed.cursor)
        self.calendar = calendar_day(borrowed.cursor, shanghai_trade_date(now))
        self.phase = market_phase(now, self.calendar.is_trading_day)
        self._snapshot: Any = None

    @property
    def snapshot(self) -> Any:
        if self._snapshot is None:
            self._snapshot = panorama.snapshot(self.cursor, self.tables)
        return self._snapshot

    @property
    def day(self) -> date:
        """The trading day on screen: the snapshot's, else the last session, else today."""

        as_of = panorama.snapshot_as_of(self.snapshot)
        if as_of is not None:
            return shanghai_trade_date(as_of)
        return session_date(self.calendar, self.phase) or self.calendar.trade_date


def _serve(
    request: Request,
    response: Response,
    build: Callable[[_Context], _DataT],
    empty: Callable[[], _DataT],
) -> Envelope[_DataT]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
        )
        data = empty() if borrowed is None else build(_Context(borrowed, now))
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[_DataT](data=data, serving=meta)


# ------------------------------------------------------------------ pulse


def _age_text(seconds: float) -> str:
    return f"{int(seconds // 60)} 分钟" if seconds >= 60 else f"{int(seconds)} 秒"


def _freshness(context: _Context, as_of: datetime | None) -> Status:
    if as_of is None:
        return Status(UserState.IDLE, "暂无快照", "全市场快照还没有数据来源")
    local = as_of.astimezone(MARKET_TIMEZONE).strftime("%m-%d %H:%M:%S")
    today = context.calendar.trade_date
    if shanghai_trade_date(as_of) != today:
        return Status(UserState.IDLE, "最近交易日", f"快照时间 {local}")
    if context.phase in _IN_SESSION:
        age = (context.now - as_of).total_seconds()
        if age > _SNAPSHOT_STALE.total_seconds():
            return Status(UserState.WARN, "延迟", f"行情快照已 {_age_text(age)}没有更新（{local}）")
        return Status(UserState.OK, "实时", f"快照时间 {local}")
    if context.phase is MarketPhase.NOON_BREAK:
        return Status(UserState.IDLE, "午休", f"快照时间 {local}")
    return Status(UserState.IDLE, "收盘数据", f"快照时间 {local}")


def _recent_alert(context: _Context, day: date, alerts: list[Any]) -> Any:
    if not alerts or day != context.calendar.trade_date:
        return None
    latest = alerts[-1]
    try:
        hour, minute = (int(part) for part in latest.t.split(":"))
    except ValueError:
        return None
    local_now = context.now.astimezone(MARKET_TIMEZONE)
    at = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return latest if timedelta(0) <= local_now - at <= _RECENT_ALERT else None


def build_pulse(context: _Context) -> PulseData:
    snap = context.snapshot
    as_of = panorama.snapshot_as_of(snap)
    day = context.day
    history, last_minute = panorama.pulse_history(context.cursor, context.tables, day)
    counts = panorama.pulse_counts(snap)
    source = "snapshot" if counts is not None else None
    if counts is None and last_minute is not None:
        counts, source = last_minute, "history"
    alerts = panorama.pulse_alerts(context.cursor, context.tables, day)
    return PulseData(
        trade_date=day,
        as_of=as_of,
        age_seconds=None if as_of is None else max((context.now - as_of).total_seconds(), 0.0),
        freshness=StatusInfo.of(_freshness(context, as_of)),
        counts=counts,
        source=source,
        history=history,
        alerts=alerts,
        recent_alert=_recent_alert(context, day, alerts),
    )


def _empty_pulse() -> PulseData:
    return PulseData(
        trade_date=None,
        as_of=None,
        age_seconds=None,
        freshness=StatusInfo(state=UserState.IDLE, label="暂无快照", reason="读不到页面数据"),
        counts=None,
        source=None,
        history=[],
        alerts=[],
        recent_alert=None,
    )


@router.get("/pulse", response_model=Envelope[PulseData], summary="市场脉搏")
def get_pulse(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[PulseData]:
    return _serve(request, response, build_pulse, _empty_pulse)


# ------------------------------------------------------------------ boards


@router.get("/boards", response_model=Envelope[BoardsData], summary="板块总表")
def get_boards(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    system: Annotated[str, Query(max_length=16)] = panorama.KPL_SYSTEM,
) -> Envelope[BoardsData]:
    if system not in BOARD_SYSTEMS:
        raise HTTPException(status_code=422, detail="未知的板块体系")

    def build(context: _Context) -> BoardsData:
        rows, as_of = panorama.boards(context.cursor, context.tables, system)
        return BoardsData(
            system=system,
            systems=list(BOARD_SYSTEMS),
            has_flow=system != panorama.KPL_SYSTEM,
            as_of=as_of,
            rows=rows,
        )

    def empty() -> BoardsData:
        return BoardsData(
            system=system,
            systems=list(BOARD_SYSTEMS),
            has_flow=system != panorama.KPL_SYSTEM,
            as_of=None,
            rows=[],
        )

    return _serve(request, response, build, empty)


@router.get(
    "/boards/{board_code}/members",
    response_model=Envelope[MembersData],
    summary="板块成分",
)
def get_members(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    board_code: Annotated[str, Path(min_length=1, max_length=64)],
) -> Envelope[MembersData]:
    def build(context: _Context) -> MembersData:
        name, rows = panorama.members(context.cursor, context.tables, board_code, context.snapshot)
        return MembersData(board_code=board_code, board_name=name, rows=rows)

    return _serve(
        request,
        response,
        build,
        lambda: MembersData(board_code=board_code, board_name=None, rows=[]),
    )


# ------------------------------------------------------------------ charts


@router.get(
    "/stocks/{ts_code}/intraday",
    response_model=Envelope[IntradayData],
    summary="个股分时 / 5 日",
)
def get_intraday(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    ts_code: Annotated[str, Path(pattern=_TS_CODE)],
    days: Annotated[int, Query(ge=1, le=5)] = 1,
    day: Annotated[date | None, Query(alias="date")] = None,
) -> Envelope[IntradayData]:
    def build(context: _Context) -> IntradayData:
        trading_days, bars = panorama.minute_bars(
            context.cursor, context.tables, ts_code, days=days, day=day
        )
        marks = panorama.surge_marks(
            context.cursor,
            context.tables,
            ts_code,
            trading_days,
            bars,
            every_event=day is not None,
        )
        return IntradayData(
            ts_code=ts_code,
            name=panorama.stock_name(context.cursor, context.tables, ts_code, context.snapshot),
            days=trading_days,
            bars=bars,
            marks=marks,
        )

    return _serve(
        request,
        response,
        build,
        lambda: IntradayData(ts_code=ts_code, name=None, days=[], bars=[], marks=[]),
    )


@router.get("/stocks/{ts_code}/daily", response_model=Envelope[DailyData], summary="个股日 K")
def get_daily(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    ts_code: Annotated[str, Path(pattern=_TS_CODE)],
    count: Annotated[int, Query(ge=20, le=240)] = 120,
) -> Envelope[DailyData]:
    def build(context: _Context) -> DailyData:
        return DailyData(
            ts_code=ts_code,
            name=panorama.stock_name(context.cursor, context.tables, ts_code, context.snapshot),
            bars=panorama.daily_bars(
                context.cursor, context.tables, ts_code, context.snapshot, count=count
            ),
        )

    return _serve(request, response, build, lambda: DailyData(ts_code=ts_code, name=None, bars=[]))


# ------------------------------------------------------------------ surge ledger


@router.get("/surge", response_model=Envelope[SurgeData], summary="爆量记录（按日）")
def get_surge(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    day: Annotated[date | None, Query(alias="date")] = None,
) -> Envelope[SurgeData]:
    def build(context: _Context) -> SurgeData:
        chosen = day or context.day
        return SurgeData(
            trade_date=chosen,
            dates=panorama.surge_dates(context.cursor, context.tables),
            rows=panorama.surge_day(context.cursor, context.tables, chosen),
            config=panorama.surge_config(context.cursor, context.tables),
        )

    return _serve(
        request,
        response,
        build,
        lambda: SurgeData(trade_date=day, dates=[], rows=[], config=None),
    )


@router.get(
    "/surge/search",
    response_model=Envelope[SurgeSearchData],
    summary="爆量记录（跨日搜索）",
)
def search_surge(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    q: Annotated[str, Query(min_length=1, max_length=32)],
) -> Envelope[SurgeSearchData]:
    def build(context: _Context) -> SurgeSearchData:
        rows, truncated = panorama.surge_search(context.cursor, context.tables, q)
        return SurgeSearchData(query=q.strip(), rows=rows, truncated=truncated)

    return _serve(
        request,
        response,
        build,
        lambda: SurgeSearchData(query=q.strip(), rows=[], truncated=False),
    )


__all__ = ["router"]
