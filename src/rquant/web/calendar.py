"""Trading-day questions answered only from a complete daily SSE Serving projection."""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS
from rquant.web.market import MarketPhase

CALENDAR_EXCHANGE = "SSE"
_MAX_ROWS = PAGE_PROJECTION_CONTRACTS["trade_calendar"].max_rows


@dataclass(frozen=True)
class CalendarDay:
    trade_date: date
    #: None when the calendar is unpublished or does not cover ``trade_date``.
    is_trading_day: bool | None
    previous_trading_day: date | None
    next_trading_day: date | None


def calendar_rows(cursor: Any) -> tuple[tuple[date, bool], ...] | None:
    """Return explicit daily facts, or refuse a legacy/incomplete projection."""

    row = cursor.execute(
        "SELECT available, row_count FROM projection_status WHERE table_name = 'trade_calendar'"
    ).fetchone()
    if row is None or row[0] is False:
        return None
    expected = row[1]
    if row[0] is not True or type(expected) is not int or not 1 <= expected <= _MAX_ROWS:
        raise ValueError("calendar projection status is invalid")
    rows = cursor.execute(
        "SELECT trade_date, exchange, is_open FROM trade_calendar "
        "ORDER BY trade_date, exchange LIMIT ?",
        (_MAX_ROWS + 1,),
    ).fetchall()
    if len(rows) != expected or any(
        type(day) is not date or exchange != CALENDAR_EXCHANGE or type(open_flag) is not bool
        for day, exchange, open_flag in rows
    ):
        raise ValueError("calendar projection rows are invalid")
    first = rows[0][0]
    if not any(open_flag is False for _, _, open_flag in rows) or any(
        day != first + timedelta(days=index) for index, (day, _, _) in enumerate(rows)
    ):
        raise ValueError("calendar projection is not a complete daily schedule")
    return tuple((day, open_flag) for day, _, open_flag in rows)


def calendar_available(cursor: Any) -> bool:
    try:
        return calendar_rows(cursor) is not None
    except ValueError:
        return False


def calendar_day(cursor: Any, trade_date: date) -> CalendarDay:
    """Whether ``trade_date`` is open, and the open days either side of it."""

    try:
        rows = calendar_rows(cursor)
    except ValueError:
        return CalendarDay(trade_date, None, None, None)
    if rows is None:
        return CalendarDay(trade_date, None, None, None)
    return calendar_day_from_rows(rows, trade_date)


def calendar_day_from_rows(rows: tuple[tuple[date, bool], ...], trade_date: date) -> CalendarDay:
    """Answer one date from an already validated daily projection."""

    unknown = CalendarDay(trade_date, None, None, None)
    dates = [day for day, _ in rows]
    if not dates[0] <= trade_date <= dates[-1]:
        return unknown
    index = bisect_left(dates, trade_date)
    is_open = rows[index][1]
    previous = next((day for day, open_flag in reversed(rows[:index]) if open_flag), None)
    following = next((day for day, open_flag in rows[index + 1 :] if open_flag), None)
    return CalendarDay(
        trade_date=trade_date,
        is_trading_day=is_open,
        previous_trading_day=previous,
        next_trading_day=following,
    )


def session_date(day: CalendarDay, phase: MarketPhase) -> date | None:
    """The trading day whose numbers the overview shows.

    Today once today's session has any data to show (from the call auction on), otherwise
    the last trading day before today: on a holiday, a weekend or before 09:15 the most
    recent complete session is what the owner wants to read.
    """

    if day.is_trading_day is None:
        return None
    if day.is_trading_day and phase not in {MarketPhase.PRE_OPEN, MarketPhase.UNKNOWN}:
        return day.trade_date
    return day.previous_trading_day


def last_closed_trading_day(day: CalendarDay, phase: MarketPhase) -> date | None:
    """The latest trading day whose session has closed (daily data should reach it)."""

    if day.is_trading_day is None:
        return None
    if day.is_trading_day and phase is MarketPhase.AFTER_CLOSE:
        return day.trade_date
    return day.previous_trading_day


__all__ = [
    "CALENDAR_EXCHANGE",
    "CalendarDay",
    "calendar_available",
    "calendar_day",
    "calendar_day_from_rows",
    "calendar_rows",
    "last_closed_trading_day",
    "session_date",
]
