"""Intraday research inputs in a separate DuckDB (``intraday.duckdb``, so long backfills
never hold the market.duckdb write lock):

* ``bar_1450`` - per stock/day aggregates of 5-min bars up to and including the 14:50 bar
  (price at 14:50, high/low/volume/amount so far) plus full-day volume, from stk_mins.
  Raw (unadjusted) prices.
* ``auction`` - 09:25 opening auction (stk_auction).
* ``limit_list`` - daily limit-up/down list (limit_list_d: first/last seal time, opens,
  consecutive boards, seal amount).

    python -m rquant.research.intraday [--years 2] [--what minutes,auction,limits]
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from rquant.research.ingest import Pro, _d, _upsert, db_path

log = logging.getLogger("rquant.research")

DDL = """
CREATE TABLE IF NOT EXISTS bar_1450(ts_code VARCHAR, trade_date DATE, p1450 DOUBLE,
  h1450 DOUBLE, l1450 DOUBLE, v1450 DOUBLE, a1450 DOUBLE, vday DOUBLE,
  PRIMARY KEY (ts_code, trade_date));
CREATE TABLE IF NOT EXISTS auction(ts_code VARCHAR, trade_date DATE, vol DOUBLE, price DOUBLE,
  amount DOUBLE, pre_close DOUBLE, turnover_rate DOUBLE, volume_ratio DOUBLE,
  float_share DOUBLE, PRIMARY KEY (ts_code, trade_date));
CREATE TABLE IF NOT EXISTS limit_list(ts_code VARCHAR, trade_date DATE, lim VARCHAR,
  first_time VARCHAR, last_time VARCHAR, open_times INTEGER, limit_times INTEGER,
  fd_amount DOUBLE, float_mv DOUBLE, turnover_ratio DOUBLE, industry VARCHAR,
  PRIMARY KEY (ts_code, trade_date));
CREATE TABLE IF NOT EXISTS minute_done(ts_code VARCHAR PRIMARY KEY, start_date DATE,
  end_date DATE);
"""

CHUNK_DAYS = 150  # stk_mins returns at most 8000 rows (~166 days of 5-min bars)


def intraday_path(root: Path) -> Path:
    return root / "intraday.duckdb"


def connect(root: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(intraday_path(root)), read_only=read_only)
    if not read_only:
        con.execute(DDL)
    return con


def aggregate_1450(df: pd.DataFrame) -> pd.DataFrame:
    """5-min bars (trade_time = bar end) -> one row per day, cumulative through 14:50."""
    if df is None or df.empty:
        return pd.DataFrame()
    t = pd.to_datetime(df["trade_time"])
    df = df.assign(trade_date=t.dt.date, hm=t.dt.strftime("%H:%M"))
    day = df.groupby("trade_date")["vol"].sum().rename("vday")
    upto = df[df["hm"] <= "14:50"].sort_values("trade_time")
    g = upto.groupby("trade_date")
    out = pd.DataFrame({"p1450": g["close"].last(), "h1450": g["high"].max(),
                        "l1450": g["low"].min(), "v1450": g["vol"].sum(),
                        "a1450": g["amount"].sum(), "last": g["hm"].last()})
    out = out[out["last"] == "14:50"].drop(columns="last").join(day)  # need the 14:50 bar
    return out.reset_index().assign(ts_code=df["ts_code"].iloc[0])[
        ["ts_code", "trade_date", "p1450", "h1450", "l1450", "v1450", "a1450", "vday"]]


def ingest_minutes(con, pro: Pro, codes: list[str], start: date, end: date) -> int:
    done = {c: (s, e) for c, s, e in con.execute("SELECT * FROM minute_done").fetchall()}
    total = 0
    for i, code in enumerate(codes):
        if code in done and done[code][0] <= start and done[code][1] >= end:
            continue
        frames = []
        a = start
        while a <= end:
            b = min(a + timedelta(days=CHUNK_DAYS), end)
            df = pro("stk_mins", ts_code=code, freq="5min",
                     start_date=f"{a:%Y-%m-%d} 09:00:00", end_date=f"{b:%Y-%m-%d} 15:00:00")
            if df is not None and len(df):
                frames.append(df)
            a = b + timedelta(days=1)
        if frames:
            agg = aggregate_1450(pd.concat(frames, ignore_index=True)
                                 .drop_duplicates(["trade_time"]))
            total += _upsert(con, "bar_1450", agg)
        _upsert(con, "minute_done", pd.DataFrame(
            {"ts_code": [code], "start_date": [start], "end_date": [end]}))
        if i % 200 == 0:
            log.info("minutes %d/%d rows=%d", i, len(codes), total)
    return total


def ingest_daily_api(con, pro: Pro, api: str, table: str, days: list[date],
                     cols: list[str]) -> int:
    have = {r[0] for r in con.execute(f"SELECT DISTINCT trade_date FROM {table}").fetchall()}
    total = 0
    for d in days:
        if d in have:
            continue
        df = pro(api, trade_date=f"{d:%Y%m%d}")
        if df is None or df.empty:
            continue
        df = df.assign(trade_date=_d(df["trade_date"])).rename(columns={"limit": "lim"})
        for c in cols:
            if c not in df:
                df[c] = None
        total += _upsert(con, table, df[cols])
    return total


def run(root: Path, years: float = 2, what: tuple[str, ...] = ("auction", "limits", "minutes"),
        end: date | None = None) -> dict[str, int]:
    mcon = duckdb.connect(str(db_path(root)), read_only=True)
    try:
        end = end or mcon.execute("SELECT MAX(trade_date) FROM daily_bar").fetchone()[0]
        start = end - timedelta(days=int(365 * years))
        days = [r[0] for r in mcon.execute(
            "SELECT cal_date FROM trade_calendar WHERE is_open AND cal_date BETWEEN ? AND ? "
            "ORDER BY 1", [start, end]).fetchall()]
        codes = [r[0] for r in mcon.execute(
            "SELECT DISTINCT ts_code FROM daily_bar WHERE trade_date BETWEEN ? AND ? "
            "AND ts_code NOT LIKE '%.BJ' ORDER BY 1", [start, end]).fetchall()]
    finally:
        mcon.close()
    pro = Pro(os.environ.get("TUSHARE_TOKEN_MAIN") or os.environ["TUSHARE_TOKEN"], pause=0.07)
    con = connect(root)
    out: dict[str, int] = {}
    try:
        if "auction" in what:
            out["auction"] = ingest_daily_api(con, pro, "stk_auction", "auction", days, [
                "ts_code", "trade_date", "vol", "price", "amount", "pre_close", "turnover_rate",
                "volume_ratio", "float_share"])
        if "limits" in what:
            out["limits"] = ingest_daily_api(con, pro, "limit_list_d", "limit_list", days, [
                "ts_code", "trade_date", "lim", "first_time", "last_time", "open_times",
                "limit_times", "fd_amount", "float_mv", "turnover_ratio", "industry"])
        if "minutes" in what:
            out["minutes"] = ingest_minutes(con, pro, codes, start, end)
    finally:
        con.close()
    return out


def main(argv: list[str] | None = None) -> int:
    from rquant.backtest.store import research_root

    p = argparse.ArgumentParser(prog="python -m rquant.research.intraday")
    p.add_argument("--years", type=float, default=2)
    p.add_argument("--what", default="auction,limits,minutes")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log.info("intraday: %s", run(research_root(), a.years, tuple(a.what.split(","))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
