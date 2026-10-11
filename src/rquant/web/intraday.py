"""Intraday (分时) bars for the stock page.

Sources, best first: 1-minute then 5-minute ``minute_bar`` in the research ``market.duckdb``;
a ``minute_bar`` table in ``intraday.duckdb`` (live intraday store) when present. Prices are
as traded (not adjusted); the prior close comes from ``daily_bar.pre_close``.
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


class IntradayData(BaseModel):
    code: str
    date: date
    days: list[date]  # recent days with minute bars, newest last
    freq: str  # "1min" | "5min"
    source: str  # "research" | "intraday"
    pre_close: float | None
    bars: list[IntradayBar]


def _from(con: duckdb.DuckDBPyConnection, code: str, day: date | None,
          source: str) -> IntradayData | None:
    for freq in ("1min", "5min"):
        days = [r[0] for r in con.execute(
            "SELECT DISTINCT trade_date FROM minute_bar WHERE ts_code = ? AND freq = ? "
            "ORDER BY 1 DESC LIMIT 10", [code, freq]).fetchall()][::-1]
        if not days:
            continue
        d = day if day in days else days[-1]
        rows = con.execute(
            "SELECT trade_time, open, high, low, close, vol, amount FROM minute_bar "
            "WHERE ts_code = ? AND freq = ? AND trade_date = ? ORDER BY trade_time",
            [code, freq, d]).fetchall()
        return IntradayData(
            code=code, date=d, days=days, freq=freq, source=source, pre_close=None,
            bars=[IntradayBar(t=f"{t:%H:%M}", open=o, high=h, low=lo, close=c, vol=v or 0.0,
                              amount=a)
                  for t, o, h, lo, c, v, a in rows if None not in (o, h, lo, c)])
    return None


def load_intraday(market: duckdb.DuckDBPyConnection | None, root: Path, code: str,
                  day: date | None = None) -> IntradayData | None:
    out = None
    live = root / "intraday.duckdb"
    if live.exists():
        try:
            con = duckdb.connect(str(live), read_only=True)
            try:
                out = _from(con, code, day, "intraday")
            finally:
                con.close()
        except duckdb.Error:  # no minute_bar table there yet, or the writer holds the lock
            out = None
    if market is not None:
        try:
            research = _from(market, code, day, "research")
        except duckdb.Error:
            research = None
        if research is not None and (out is None or research.date > out.date
                                     or (day is not None and research.date == day)):
            out = research
    if out is None:
        return None
    if market is not None:
        try:
            r = market.execute(
                "SELECT pre_close FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
                [code, out.date]).fetchone()
            out.pre_close = r[0] if r else None
        except duckdb.Error:
            pass
    if out.pre_close is None and out.bars:
        out.pre_close = out.bars[0].open
    return out
