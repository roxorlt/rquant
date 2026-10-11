"""Intraday (分时) bars for the stock page.

Sources, best first: 1-minute then 5-minute ``minute_bar`` in the research ``market.duckdb``;
a ``minute_bar`` table in ``intraday.duckdb`` (live intraday store) when present. Prices are
as traded (not adjusted); the prior close comes from ``daily_bar.pre_close``.

Any day with minute bars can be requested; ``span=5`` returns the five trading days (with bars)
ending on that day, stitched in order, for the 5日 view.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
from pydantic import BaseModel


class IntradayBar(BaseModel):
    t: str  # bar end time HH:MM
    open: float
    high: float
    low: float
    close: float
    vol: float
    amount: float | None


class IntradaySession(BaseModel):
    date: date
    freq: str  # "1min" | "5min"
    source: str  # "research" | "intraday"
    pre_close: float | None
    bars: list[IntradayBar]


class IntradayData(BaseModel):
    code: str
    date: date  # the selected (last) day
    days: list[date]  # every day with minute bars, oldest first
    freq: str  # freq of the selected day
    source: str
    pre_close: float | None  # prior close of the selected day
    bars: list[IntradayBar]  # bars of the selected day
    sessions: list[IntradaySession]  # the window (1 or 5 days), oldest first; last = selected


def _index(con: duckdb.DuckDBPyConnection, code: str) -> tuple[str, list[date]] | None:
    for freq in ("1min", "5min"):
        days = [r[0] for r in con.execute(
            "SELECT DISTINCT trade_date FROM minute_bar WHERE ts_code = ? AND freq = ? "
            "ORDER BY 1", [code, freq]).fetchall()]
        if days:
            return freq, days
    return None


def _bars(con: duckdb.DuckDBPyConnection, code: str, freq: str, d: date) -> list[IntradayBar]:
    rows = con.execute(
        "SELECT trade_time, open, high, low, close, vol, amount FROM minute_bar "
        "WHERE ts_code = ? AND freq = ? AND trade_date = ? ORDER BY trade_time",
        [code, freq, d]).fetchall()
    return [IntradayBar(t=f"{t:%H:%M}", open=o, high=h, low=lo, close=c, vol=v or 0.0, amount=a)
            for t, o, h, lo, c, v, a in rows if None not in (o, h, lo, c)]


def _pre_close(market: duckdb.DuckDBPyConnection | None, code: str, d: date) -> float | None:
    if market is None:
        return None
    try:
        r = market.execute("SELECT pre_close FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
                           [code, d]).fetchone()
        return r[0] if r else None
    except duckdb.Error:
        return None


def load_intraday(market: duckdb.DuckDBPyConnection | None, root: Path, code: str,
                  day: date | None = None, span: int = 1) -> IntradayData | None:
    srcs: list[tuple[str, duckdb.DuckDBPyConnection, str, set[date]]] = []
    live_con = None
    live = root / "intraday.duckdb"
    try:
        if market is not None:
            try:
                ix = _index(market, code)
                if ix:
                    srcs.append(("research", market, ix[0], set(ix[1])))
            except duckdb.Error:
                pass
        if live.exists():
            try:
                live_con = duckdb.connect(str(live), read_only=True)
                ix = _index(live_con, code)
                if ix:
                    srcs.append(("intraday", live_con, ix[0], set(ix[1])))
            except duckdb.Error:  # no minute_bar table there yet, or the writer holds the lock
                pass
        all_days = sorted(set().union(*(s[3] for s in srcs)))
        if not all_days:
            return None
        if day is None or day > all_days[-1]:
            target = all_days[-1]
        else:  # requested day, or the nearest earlier day with bars (else the first)
            target = max((d for d in all_days if d <= day), default=all_days[0])
        i = all_days.index(target)
        window = all_days[max(0, i - max(1, span) + 1): i + 1]
        sessions = []
        for d in window:
            name, con, freq, _ = next(s for s in srcs if d in s[3])  # research preferred
            bars = _bars(con, code, freq, d)
            pc = _pre_close(market, code, d)
            if pc is None and bars:
                pc = bars[0].open
            sessions.append(IntradaySession(date=d, freq=freq, source=name, pre_close=pc,
                                            bars=bars))
    finally:
        if live_con is not None:
            live_con.close()
    last = sessions[-1]
    return IntradayData(code=code, date=last.date, days=all_days, freq=last.freq,
                        source=last.source, pre_close=last.pre_close, bars=last.bars,
                        sessions=sessions)
