"""Intraday (分时) bars for the stock page.

Sources, best first: 1-minute then 5-minute ``minute_bar`` in the research ``market.duckdb``;
a ``minute_bar`` table in ``intraday.duckdb`` (live intraday store) when present. Prices are
as traded (not adjusted); the prior close comes from ``daily_bar.pre_close``.

During the session, today's bars are built on the fly from the research 3-second snapshots
(``snapshots/<today>/quotes/part-*.parquet`` and/or ``quotes.parquet``) when no store has
minute bars for today yet; such a session has ``source="live"`` and ``freq="1min"``.

Any day with minute bars can be requested; ``span=5`` returns the five trading days (with bars)
ending on that day, stitched in order, for the 5日 view.
"""

from __future__ import annotations

import threading
import time as _time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

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
    source: str  # "research" | "intraday" | "live"
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


_SH = ZoneInfo("Asia/Shanghai")
_LIVE_TTL = 3.0
_LIVE_LOCK = threading.Lock()
# code -> (day, newest part mtime/count signature, built-at monotonic, result)
_LIVE_CACHE: dict[str, tuple[date, tuple, float, tuple[list[IntradayBar], float | None]]] = {}


def _today() -> date:
    return datetime.now(_SH).date()


def _snapshot_files(root: Path, d: date) -> list[Path]:
    base = root / "snapshots" / d.isoformat()
    files = [base / "quotes.parquet"] if (base / "quotes.parquet").is_file() else []
    q = base / "quotes"
    if q.is_dir():
        files += sorted(q.glob("part-*.parquet"))
    return files


def _bar_label(hhmmss: str) -> str | None:
    """Minute bar END label for a snapshot time (09:30:05 -> 09:31; auction folds into 09:31)."""
    hh, mm = int(hhmmss[0:2]), int(hhmmss[3:5])
    m = hh * 60 + mm
    if m < 9 * 60 + 25 or (11 * 60 + 30 < m < 13 * 60) or m > 15 * 60:
        return None
    if m < 9 * 60 + 30:
        m = 9 * 60 + 30
    end = m if m in (11 * 60 + 30, 15 * 60) else m + 1  # closing prints join the last bar
    return f"{end // 60:02d}:{end % 60:02d}"


def _live_bars(root: Path, code: str, d: date) -> tuple[list[IntradayBar], float | None]:
    """1-minute bars for ``code`` on ``d`` from the 3s snapshot files (cached ~3s)."""
    files = _snapshot_files(root, d)
    if not files:
        return [], None
    try:
        sig = (len(files), str(files[-1]), files[-1].stat().st_mtime)
    except OSError:
        sig = (len(files), str(files[-1]), 0.0)
    now = _time.monotonic()
    with _LIVE_LOCK:
        hit = _LIVE_CACHE.get(code)
        if hit and hit[0] == d and (hit[1] == sig or now - hit[2] < _LIVE_TTL):
            return hit[3]
    paths = ", ".join("'" + str(f).replace("'", "''") + "'" for f in files)
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT time, price, volume, amount, pre_close FROM read_parquet([{paths}], "
            "union_by_name = true) WHERE ts_code = ? AND price > 0 ORDER BY time",
            [code]).fetchall()
    except duckdb.Error:
        rows = []
    finally:
        con.close()
    bars: list[IntradayBar] = []
    pre_close = None
    prev_vol = prev_amt = 0.0
    cur: dict | None = None
    for t, px, vol, amt, pc in rows:
        if pc:
            pre_close = float(pc)
        lab = _bar_label(str(t))
        if lab is None:
            continue
        vol = float(vol or 0.0)
        amt = float(amt or 0.0)
        if cur is None or cur["t"] != lab:
            if cur is not None:
                bars.append(IntradayBar(**cur))
            cur = {"t": lab, "open": px, "high": px, "low": px, "close": px, "vol": 0.0,
                   "amount": 0.0}
        cur["high"] = max(cur["high"], px)
        cur["low"] = min(cur["low"], px)
        cur["close"] = px
        if vol >= prev_vol:  # cumulative counters; ignore glitches that go backwards
            cur["vol"] += vol - prev_vol
            cur["amount"] += max(0.0, amt - prev_amt)
            prev_vol, prev_amt = vol, amt
    if cur is not None:
        bars.append(IntradayBar(**cur))
    out = (bars, pre_close)
    with _LIVE_LOCK:
        _LIVE_CACHE[code] = (d, sig, now, out)
    return out


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
        today = _today()
        live_today: tuple[list[IntradayBar], float | None] | None = None
        if today not in all_days:
            live_today = _live_bars(root, code, today)
            if live_today[0]:
                all_days.append(today)
            else:
                live_today = None
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
            if live_today is not None and d == today:
                bars, pc = live_today
                if pc is None:
                    pc = bars[0].open
                sessions.append(IntradaySession(date=d, freq="1min", source="live",
                                                pre_close=pc, bars=bars))
                continue
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
