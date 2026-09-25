"""Trading-day questions answered from the Serving ``trade_calendar`` projection.

The projection lists open SSE dates only; a date inside its coverage that is not listed is
closed (weekends and exchange holidays such as 2026-09-25 中秋). Nothing here guesses from
the weekday: when the projection is unpublished or does not cover a date, the answer is
``None`` and the page says it does not know.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from rquant.web.market import MarketPhase

CALENDAR_EXCHANGE = "SSE"


@dataclass(frozen=True)
class CalendarDay:
    trade_date: date
    #: None when the calendar is unpublished or does not cover ``trade_date``.
    is_trading_day: bool | None
    previous_trading_day: date | None
    next_trading_day: date | None


def calendar_available(cursor: Any) -> bool:
    row = cursor.execute(
        "SELECT available FROM projection_status WHERE table_name = 'trade_calendar'"
    ).fetchone()
    return bool(row is not None and row[0])


def calendar_day(cursor: Any, trade_date: date) -> CalendarDay:
    """Whether ``trade_date`` is open, and the open days either side of it."""

    unknown = CalendarDay(trade_date, None, None, None)
    if not calendar_available(cursor):
        return unknown
    row = cursor.execute(
        "SELECT min(trade_date), max(trade_date), "
        "coalesce(bool_or(trade_date = ? AND is_open), false), "
        "max(trade_date) FILTER (WHERE is_open AND trade_date < ?), "
        "min(trade_date) FILTER (WHERE is_open AND trade_date > ?) "
        "FROM trade_calendar WHERE exchange = ?",
        (trade_date, trade_date, trade_date, CALENDAR_EXCHANGE),
    ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        return unknown
    first, last, is_open, previous, following = row
    if not first <= trade_date <= last:
        return unknown
    return CalendarDay(
        trade_date=trade_date,
        is_trading_day=bool(is_open),
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
    "last_closed_trading_day",
    "session_date",
]
