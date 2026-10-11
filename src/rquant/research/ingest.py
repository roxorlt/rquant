# ruff: noqa: E501 - SQL and preset tables read better unwrapped
"""Tushare -> $RQUANT_RESEARCH_ROOT/market.duckdb (incremental by trade date).

Tables (catalog names where they exist): trade_calendar, stock_basic, daily_bar (raw),
adj_factor, daily_basic, daily_state (limit_pct from stk_limit), index_daily,
minute_bar (5min, subset). View daily_bar_hfq = back-adjusted prices for research.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

log = logging.getLogger(__name__)

BENCHMARKS = ("000300.SH", "000905.SH", "000852.SH", "000001.SH", "399006.SZ", "000016.SH")

DDL = """
CREATE TABLE IF NOT EXISTS trade_calendar(cal_date DATE PRIMARY KEY, is_open BOOLEAN);
CREATE TABLE IF NOT EXISTS stock_basic(ts_code VARCHAR PRIMARY KEY, name VARCHAR,
  industry VARCHAR, market VARCHAR, list_date DATE);
CREATE TABLE IF NOT EXISTS daily_bar(ts_code VARCHAR, trade_date DATE, open DOUBLE,
  high DOUBLE, low DOUBLE, close DOUBLE, pre_close DOUBLE, pct_chg DOUBLE, vol DOUBLE,
  amount DOUBLE, PRIMARY KEY (ts_code, trade_date));
CREATE TABLE IF NOT EXISTS adj_factor(ts_code VARCHAR, trade_date DATE, adj_factor DOUBLE,
  PRIMARY KEY (ts_code, trade_date));
CREATE TABLE IF NOT EXISTS daily_basic(ts_code VARCHAR, trade_date DATE,
  turnover_rate DOUBLE, pe_ttm DOUBLE, pb DOUBLE, total_mv DOUBLE, circ_mv DOUBLE,
  PRIMARY KEY (ts_code, trade_date));
CREATE TABLE IF NOT EXISTS daily_state(ts_code VARCHAR, trade_date DATE, limit_pct DOUBLE,
  PRIMARY KEY (ts_code, trade_date));
CREATE TABLE IF NOT EXISTS index_daily(ts_code VARCHAR, trade_date DATE, close DOUBLE,
  pct_chg DOUBLE, PRIMARY KEY (ts_code, trade_date));
CREATE TABLE IF NOT EXISTS minute_bar(ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR,
  trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE,
  amount DOUBLE, PRIMARY KEY (ts_code, trade_time, freq));
CREATE OR REPLACE VIEW daily_bar_hfq AS
  SELECT b.ts_code, b.trade_date, b.open * a.adj_factor AS open, b.high * a.adj_factor AS high,
         b.low * a.adj_factor AS low, b.close * a.adj_factor AS close,
         b.pre_close * a.adj_factor AS pre_close, b.pct_chg, b.vol, b.amount
  FROM daily_bar b JOIN adj_factor a USING (ts_code, trade_date);
CREATE OR REPLACE VIEW daily_bar_qfq AS
  SELECT b.ts_code, b.trade_date, b.open * a.adj_factor / l.adj AS open,
         b.high * a.adj_factor / l.adj AS high, b.low * a.adj_factor / l.adj AS low,
         b.close * a.adj_factor / l.adj AS close, b.pre_close * a.adj_factor / l.adj AS pre_close,
         b.pct_chg, b.vol, b.amount
  FROM daily_bar b JOIN adj_factor a USING (ts_code, trade_date)
  JOIN (SELECT ts_code, arg_max(adj_factor, trade_date) AS adj FROM adj_factor GROUP BY ts_code) l
  USING (ts_code);
"""


def db_path(root: Path) -> Path:
    return root / "market.duckdb"


def connect(root: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    root.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path(root)), read_only=read_only)
    if not read_only:
        con.execute(DDL)
    return con


class Pro:
    """Thin rate-limited Tushare wrapper with retries."""

    def __init__(self, token: str, pause: float = 0.13) -> None:
        import tushare as ts

        self._pro = ts.pro_api(token)
        self._pause = pause

    def __call__(self, api: str, **kw: Any) -> pd.DataFrame:
        for attempt in range(5):
            try:
                time.sleep(self._pause)
                if api == "stk_mins":
                    return self._pro.query(api, **kw)
                return getattr(self._pro, api)(**kw)
            except Exception as exc:  # noqa: BLE001 - network / rate limit: back off
                log.warning("%s %s failed (%s), retry %d", api, kw, exc, attempt)
                time.sleep(5 * (attempt + 1))
        raise RuntimeError(f"tushare {api} failed after retries")


def _d(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format="%Y%m%d").dt.date


def _upsert(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    con.register("_up", df)
    cols = ", ".join(df.columns)
    con.execute(f"INSERT OR REPLACE INTO {table} ({cols}) SELECT {cols} FROM _up")
    con.unregister("_up")
    return len(df)


def _limit_pct(code: str, name: str) -> float:
    if "ST" in (name or ""):
        return 0.05
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    if code.endswith(".BJ"):
        return 0.30
    return 0.10


def refresh_reference(con: duckdb.DuckDBPyConnection, pro: Pro, start: date, end: date) -> None:
    cal = pro("trade_cal", exchange="SSE", start_date=f"{start:%Y%m%d}",
              end_date=f"{end + timedelta(days=30):%Y%m%d}")
    _upsert(con, "trade_calendar", pd.DataFrame(
        {"cal_date": _d(cal["cal_date"]), "is_open": cal["is_open"].astype(int) == 1}))
    frames = [pro("stock_basic", list_status=s,
                  fields="ts_code,name,industry,market,list_date") for s in ("L", "D", "P")]
    sb = pd.concat([f for f in frames if f is not None], ignore_index=True)
    sb["list_date"] = pd.to_datetime(sb["list_date"], format="%Y%m%d", errors="coerce").dt.date
    _upsert(con, "stock_basic", sb[["ts_code", "name", "industry", "market", "list_date"]])


def missing_days(con: duckdb.DuckDBPyConnection, table: str, start: date, end: date) -> list[date]:
    return [r[0] for r in con.execute(
        f"SELECT cal_date FROM trade_calendar WHERE is_open AND cal_date BETWEEN ? AND ? "
        f"AND cal_date NOT IN (SELECT DISTINCT trade_date FROM {table}) ORDER BY 1",
        [start, end]).fetchall()]


def ingest_day(con: duckdb.DuckDBPyConnection, pro: Pro, day: date,
               names: dict[str, str]) -> int:
    ds = f"{day:%Y%m%d}"
    bar = pro("daily", trade_date=ds)
    if bar is None or bar.empty:
        return 0
    bar = bar.assign(trade_date=_d(bar["trade_date"]))
    n = _upsert(con, "daily_bar", bar[["ts_code", "trade_date", "open", "high", "low", "close",
                                       "pre_close", "pct_chg", "vol", "amount"]])
    adj = pro("adj_factor", trade_date=ds)
    if adj is not None and not adj.empty:
        _upsert(con, "adj_factor", adj.assign(trade_date=_d(adj["trade_date"]))[
            ["ts_code", "trade_date", "adj_factor"]])
    basic = pro("daily_basic", trade_date=ds,
                fields="ts_code,trade_date,turnover_rate,pe_ttm,pb,total_mv,circ_mv")
    if basic is not None and not basic.empty:
        _upsert(con, "daily_basic", basic.assign(trade_date=_d(basic["trade_date"])))
    lim = pro("stk_limit", trade_date=ds)
    if lim is not None and not lim.empty:
        m = bar.set_index("ts_code")["pre_close"]
        lim = lim[lim["ts_code"].isin(m.index)]
        pct = (lim["up_limit"].to_numpy() / m.loc[lim["ts_code"]].to_numpy() - 1).round(2)
        state = pd.DataFrame({"ts_code": lim["ts_code"].to_numpy(), "trade_date": day,
                              "limit_pct": pct})
    else:
        state = pd.DataFrame({"ts_code": bar["ts_code"], "trade_date": day,
                              "limit_pct": [_limit_pct(c, names.get(c, "")) for c in bar["ts_code"]]})
    _upsert(con, "daily_state", state)
    return n


def ingest_indexes(con: duckdb.DuckDBPyConnection, pro: Pro, start: date, end: date) -> None:
    for code in BENCHMARKS:
        df = pro("index_daily", ts_code=code, start_date=f"{start:%Y%m%d}", end_date=f"{end:%Y%m%d}")
        if df is not None and not df.empty:
            _upsert(con, "index_daily", df.assign(trade_date=_d(df["trade_date"]))[
                ["ts_code", "trade_date", "close", "pct_chg"]])


def ingest_minutes(con: duckdb.DuckDBPyConnection, pro: Pro, codes: list[str], start: date,
                   end: date, freq: str = "5min") -> int:
    total = 0
    for code in codes:
        have = con.execute("SELECT MAX(trade_date) FROM minute_bar WHERE ts_code = ? AND freq = ?",
                           [code, freq]).fetchone()[0]
        s = max(start, have + timedelta(days=1)) if have else start
        while s <= end:
            e = min(end, s + timedelta(days=120))
            df = pro("stk_mins", ts_code=code, freq=freq, start_date=f"{s} 09:00:00",
                     end_date=f"{e} 15:30:00")
            if df is not None and not df.empty:
                df["trade_time"] = pd.to_datetime(df["trade_time"])
                df = df.assign(freq=freq, trade_date=df["trade_time"].dt.date)
                total += _upsert(con, "minute_bar", df[["ts_code", "trade_time", "freq",
                                                        "trade_date", "open", "high", "low",
                                                        "close", "vol", "amount"]])
            s = e + timedelta(days=1)
    return total


def minute_universe(con: duckdb.DuckDBPyConnection, n: int = 300) -> list[str]:
    """Largest-cap names on the latest day (a CSI300-like subset)."""
    return [r[0] for r in con.execute(
        "SELECT ts_code FROM daily_basic WHERE trade_date = (SELECT MAX(trade_date) FROM daily_basic) "
        "ORDER BY total_mv DESC NULLS LAST LIMIT ?", [n]).fetchall()]


def run(root: Path, years: int = 5, end: date | None = None, minutes: int = 300,
        minute_days: int = 365, token: str | None = None) -> dict[str, int]:
    end = end or date.today()
    start = end - timedelta(days=365 * years)
    pro = Pro(token or os.environ.get("TUSHARE_TOKEN_MAIN") or os.environ["TUSHARE_TOKEN"])
    con = connect(root)
    try:
        refresh_reference(con, pro, start, end)
        names = dict(con.execute("SELECT ts_code, name FROM stock_basic").fetchall())
        days = [d for d in missing_days(con, "daily_bar", start, end) if d <= end]
        rows = 0
        for i, day in enumerate(days):
            rows += ingest_day(con, pro, day, names)
            if i % 50 == 0:
                log.info("daily %s (%d/%d)", day, i + 1, len(days))
        ingest_indexes(con, pro, start, end)
        mins = ingest_minutes(con, pro, minute_universe(con, minutes),
                              end - timedelta(days=minute_days), end) if minutes else 0
        con.execute("CHECKPOINT")
        return {"days": len(days), "daily_rows": rows, "minute_rows": mins}
    finally:
        con.close()


#: research tables and their date column, for the coverage audit
RESEARCH_TABLES = {"daily_bar": "trade_date", "adj_factor": "trade_date",
                   "daily_basic": "trade_date", "daily_state": "trade_date",
                   "index_daily": "trade_date", "minute_bar": "trade_date",
                   "screen_result": "trade_date"}
