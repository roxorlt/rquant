"""Stock profile (F9-lite) from the research market DuckDB, read-only.

Daily bars come from ``daily_bar`` (unadjusted, as traded), valuation from
``daily_basic`` and industry from ``stock_basic``. Every read tolerates a missing
database or table (fixture mode, a replica mid-write): the profile simply has less in it.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import duckdb
from pydantic import BaseModel

CODE_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")


class Bar(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    pct_chg: float | None
    amount: float | None


class Valuation(BaseModel):
    trade_date: date
    pe_ttm: float | None
    pb: float | None
    total_mv: float | None  # 万元 (Tushare)
    circ_mv: float | None
    turnover_rate: float | None


class StockProfile(BaseModel):
    code: str
    name: str | None
    industry: str | None
    market: str | None
    list_date: date | None
    bars: list[Bar]
    valuation: Valuation | None
    returns: dict[str, float | None]  # 5d / 20d / 60d / 250d / ytd, fractions
    fundamentals: dict[str, float | None]
    fundamentals_period: str | None
    fundamentals_fields: dict[str, str]
    pools: list[str]
    alerts: list[dict[str, object]]


@contextmanager
def market(root: Path) -> Iterator[duckdb.DuckDBPyConnection | None]:
    db = root / "market.duckdb"
    con = None
    if db.exists():
        try:
            con = duckdb.connect(str(db), read_only=True)
        except duckdb.Error:  # the daily job holds the write lock for a few minutes
            con = None
    try:
        yield con
    finally:
        if con is not None:
            con.close()


def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: list) -> list[dict]:
    try:
        cur = con.execute(sql, params)
    except duckdb.Error:
        return []
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _ret(bars: list[Bar], n: int) -> float | None:
    if len(bars) <= n:
        return None
    return bars[-1].close / bars[-1 - n].close - 1


def load_profile(con: duckdb.DuckDBPyConnection | None, code: str, days: int = 250) -> dict:
    """Market part of the profile; the caller adds fundamentals, pools and alerts."""
    out: dict = {"name": None, "industry": None, "market": None, "list_date": None,
                 "bars": [], "valuation": None, "returns": {}}
    if con is None:
        return out
    basic = _rows(con, "SELECT name, industry, market, list_date FROM stock_basic "
                       "WHERE ts_code = ?", [code])
    if basic:
        out.update(basic[0])
    raw = _rows(con, "SELECT trade_date, open, high, low, close, pct_chg, amount FROM daily_bar "
                     "WHERE ts_code = ? ORDER BY trade_date DESC LIMIT ?", [code, days])
    bars = [Bar(date=r["trade_date"], open=r["open"], high=r["high"], low=r["low"],
                close=r["close"], pct_chg=r["pct_chg"], amount=r["amount"])
            for r in reversed(raw) if r["close"] is not None]
    out["bars"] = bars
    val = _rows(con, "SELECT trade_date, pe_ttm, pb, total_mv, circ_mv, turnover_rate "
                     "FROM daily_basic WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 1", [code])
    out["valuation"] = Valuation(**val[0]) if val else None
    ytd = None
    if bars:
        prior = [b for b in bars if b.date.year < bars[-1].date.year]
        ytd = bars[-1].close / prior[-1].close - 1 if prior else None
    out["returns"] = {"5日": _ret(bars, 5), "20日": _ret(bars, 20), "60日": _ret(bars, 60),
                      "250日": _ret(bars, 249), "年初至今": ytd}
    return out


def write_demo_market(root: Path, today: date) -> None:
    """A small market.duckdb for fixture mode: a few stocks, ~300 days of bars and valuation."""
    import numpy as np
    import pandas as pd

    stocks = [("600519.SH", "贵州茅台", "白酒", 1500.0), ("000001.SZ", "平安银行", "银行", 11.0),
              ("300750.SZ", "宁德时代", "电池", 200.0), ("601318.SH", "中国平安", "保险", 45.0),
              ("600036.SH", "招商银行", "银行", 35.0), ("000858.SZ", "五粮液", "白酒", 140.0)]
    days = pd.bdate_range(end=pd.Timestamp(today), periods=300)
    rng = np.random.default_rng(11)
    bars, basic = [], []
    for k, (code, _, _, p0) in enumerate(stocks):
        rets = rng.normal(0.0004, 0.018, len(days))
        close = p0 * np.exp(np.cumsum(rets))
        prev = np.concatenate([[p0], close[:-1]])
        for d, c, pc in zip(days, close, prev, strict=True):
            o = pc * (1 + rng.normal(0, 0.004))
            hi = max(o, c) * (1 + abs(rng.normal(0, 0.006)))
            lo = min(o, c) * (1 - abs(rng.normal(0, 0.006)))
            bars.append((code, d.date(), o, hi, lo, c, pc, (c / pc - 1) * 100, 1e5, c * 1e5))
            basic.append((code, d.date(), 0.5 + k * 0.2, 10 + 5 * k + c / p0, 1 + k * 0.5,
                          c * 1e6 * (k + 1), c * 8e5 * (k + 1)))
    con = duckdb.connect(str(root / "market.duckdb"))
    try:
        con.execute("CREATE TABLE stock_basic(ts_code VARCHAR, name VARCHAR, industry VARCHAR, "
                    "market VARCHAR, list_date DATE, delist_date DATE)")
        con.executemany("INSERT INTO stock_basic VALUES (?, ?, ?, '主板', DATE '2001-08-27', NULL)",
                        [(c, n, i) for c, n, i, _ in stocks])
        con.execute("CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, open DOUBLE, "
                    "high DOUBLE, low DOUBLE, close DOUBLE, pre_close DOUBLE, pct_chg DOUBLE, "
                    "vol DOUBLE, amount DOUBLE)")
        bar_df = pd.DataFrame(bars)  # noqa: F841 - read by DuckDB's replacement scan
        con.execute("INSERT INTO daily_bar SELECT * FROM bar_df")
        con.execute("CREATE TABLE daily_basic(ts_code VARCHAR, trade_date DATE, "
                    "turnover_rate DOUBLE, pe_ttm DOUBLE, pb DOUBLE, total_mv DOUBLE, "
                    "circ_mv DOUBLE)")
        basic_df = pd.DataFrame(basic)  # noqa: F841
        con.execute("INSERT INTO daily_basic SELECT * FROM basic_df")
    finally:
        con.close()
