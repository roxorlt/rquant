"""Long-only FIFO round trips from executed fills, with partial fee allocation."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Literal


@dataclass(frozen=True)
class Fill:
    trade_date: date
    ts_code: str
    industry: str
    side: Literal["buy", "sell"]
    quantity: int
    price: float
    fee: float


@dataclass(frozen=True)
class RoundTrip:
    ts_code: str
    industry: str
    entry_date: date
    exit_date: date
    quantity: int
    entry_notional: float
    exit_notional: float
    entry_fee: float
    exit_fee: float
    net_pnl: float
    return_rate: float
    holding_days: int


@dataclass(frozen=True)
class RoundTripLedger:
    closed: tuple[RoundTrip, ...]
    open_quantity: dict[str, int]


@dataclass(frozen=True)
class RoundTripStats:
    count: int
    net_pnl: float
    win_rate: float | None
    payoff_ratio: float | None
    average_holding_days: float | None


@dataclass(frozen=True)
class RoundTripAnalysis:
    overall: RoundTripStats
    by_symbol: dict[str, RoundTripStats]
    by_industry: dict[str, RoundTripStats]
    by_holding_days: dict[int, RoundTripStats]


@dataclass
class _Lot:
    trade_date: date
    industry: str
    price: float
    quantity: int
    fee: float


def _validate_fill(fill: Fill) -> None:
    if not isinstance(fill.trade_date, date) or not fill.ts_code or not fill.industry:
        raise ValueError("fill needs a date, symbol and industry")
    if fill.side not in ("buy", "sell"):
        raise ValueError("fill side must be buy or sell")
    if isinstance(fill.quantity, bool) or not isinstance(fill.quantity, int):
        raise ValueError("fill quantity must be whole shares")
    if not all(math.isfinite(value) for value in (fill.quantity, fill.price, fill.fee)):
        raise ValueError("fill quantity, price and fee must be finite")
    if fill.quantity <= 0 or fill.price <= 0 or fill.fee < 0:
        raise ValueError("fill quantity and price must be positive; fee cannot be negative")


def build_round_trips(fills: Sequence[Fill]) -> RoundTripLedger:
    """Match executed long fills FIFO; unsettled buys remain explicit open quantity."""
    lots: dict[str, deque[_Lot]] = defaultdict(deque)
    closed: list[RoundTrip] = []
    previous_date: date | None = None
    for fill in fills:
        _validate_fill(fill)
        if previous_date is not None and fill.trade_date < previous_date:
            raise ValueError("fills must be in trade-date order")
        previous_date = fill.trade_date
        if fill.side == "buy":
            lots[fill.ts_code].append(
                _Lot(fill.trade_date, fill.industry, fill.price, fill.quantity, fill.fee)
            )
            continue
        open_quantity = sum(lot.quantity for lot in lots[fill.ts_code])
        if fill.quantity > open_quantity:
            raise ValueError(f"sell exceeds open quantity for {fill.ts_code}")
        remaining = fill.quantity
        while remaining:
            lot = lots[fill.ts_code][0]
            matched = min(remaining, lot.quantity)
            buy_fee = lot.fee * matched / lot.quantity
            sell_fee = fill.fee * matched / fill.quantity
            entry_notional = matched * lot.price
            exit_notional = matched * fill.price
            pnl = exit_notional - entry_notional - buy_fee - sell_fee
            closed.append(
                RoundTrip(
                    fill.ts_code,
                    lot.industry,
                    lot.trade_date,
                    fill.trade_date,
                    matched,
                    entry_notional,
                    exit_notional,
                    buy_fee,
                    sell_fee,
                    pnl,
                    pnl / (entry_notional + buy_fee),
                    (fill.trade_date - lot.trade_date).days,
                )
            )
            remaining -= matched
            lot.quantity -= matched
            lot.fee -= buy_fee
            if lot.quantity == 0:
                lots[fill.ts_code].popleft()
    open_positions = {
        symbol: sum(lot.quantity for lot in pending) for symbol, pending in lots.items() if pending
    }
    return RoundTripLedger(tuple(closed), open_positions)


def _stats(trips: Sequence[RoundTrip]) -> RoundTripStats:
    if not trips:
        return RoundTripStats(0, 0.0, None, None, None)
    gains = [trip.net_pnl for trip in trips if trip.net_pnl > 0]
    losses = [trip.net_pnl for trip in trips if trip.net_pnl < 0]
    decisive = len(gains) + len(losses)
    win_rate = len(gains) / decisive if decisive else None
    payoff = (
        (sum(gains) / len(gains)) / abs(sum(losses) / len(losses)) if gains and losses else None
    )
    return RoundTripStats(
        len(trips),
        sum(trip.net_pnl for trip in trips),
        win_rate,
        payoff,
        sum(trip.holding_days for trip in trips) / len(trips),
    )


def summarize_round_trips(trips: Sequence[RoundTrip]) -> RoundTripAnalysis:
    """Group closed trips by entry industry and calendar holding days."""
    by_symbol: dict[str, list[RoundTrip]] = defaultdict(list)
    by_industry: dict[str, list[RoundTrip]] = defaultdict(list)
    by_holding_days: dict[int, list[RoundTrip]] = defaultdict(list)
    for trip in trips:
        by_symbol[trip.ts_code].append(trip)
        by_industry[trip.industry].append(trip)
        by_holding_days[trip.holding_days].append(trip)
    return RoundTripAnalysis(
        _stats(trips),
        {key: _stats(group) for key, group in by_symbol.items()},
        {key: _stats(group) for key, group in by_industry.items()},
        {key: _stats(group) for key, group in by_holding_days.items()},
    )
