"""Market phase for the top bar, in the Shanghai market clock.

Display only: the runtime's fetch gate (``runtime_market_session``) has coarser phases
and fails closed; this one names the auctions too and says "unknown" when the Serving
trade calendar cannot answer.
"""

from __future__ import annotations

from datetime import date, datetime, time
from enum import StrEnum
from zoneinfo import ZoneInfo

MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")


class MarketPhase(StrEnum):
    PRE_OPEN = "pre_open"
    CALL_AUCTION = "call_auction"
    CONTINUOUS = "continuous"
    NOON_BREAK = "noon_break"
    CLOSING_AUCTION = "closing_auction"
    AFTER_CLOSE = "after_close"
    NON_TRADING_DAY = "non_trading_day"
    UNKNOWN = "unknown"


PHASE_LABELS: dict[MarketPhase, str] = {
    MarketPhase.PRE_OPEN: "盘前",
    MarketPhase.CALL_AUCTION: "集合竞价",
    MarketPhase.CONTINUOUS: "连续竞价",
    MarketPhase.NOON_BREAK: "午休",
    MarketPhase.CLOSING_AUCTION: "尾盘集合竞价",
    MarketPhase.AFTER_CLOSE: "收盘",
    MarketPhase.NON_TRADING_DAY: "休市",
    MarketPhase.UNKNOWN: "未知",
}


def shanghai_trade_date(observed_at: datetime) -> date:
    return observed_at.astimezone(MARKET_TIMEZONE).date()


def market_phase(observed_at: datetime, is_trading_day: bool | None) -> MarketPhase:
    """Phase at ``observed_at``; ``is_trading_day`` None means the calendar cannot say."""

    if is_trading_day is None:
        return MarketPhase.UNKNOWN
    if not is_trading_day:
        return MarketPhase.NON_TRADING_DAY
    local = observed_at.astimezone(MARKET_TIMEZONE).time()
    if local < time(9, 15):
        return MarketPhase.PRE_OPEN
    if local < time(9, 30):
        return MarketPhase.CALL_AUCTION
    if local < time(11, 30):
        return MarketPhase.CONTINUOUS
    if local < time(13, 0):
        return MarketPhase.NOON_BREAK
    if local < time(14, 57):
        return MarketPhase.CONTINUOUS
    if local < time(15, 0):
        return MarketPhase.CLOSING_AUCTION
    return MarketPhase.AFTER_CLOSE
