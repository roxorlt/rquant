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
    vol: float | None = None  # 手
    amount: float | None


class Valuation(BaseModel):
    trade_date: date
    pe_ttm: float | None
    pb: float | None
    total_mv: float | None  # 万元 (Tushare)
    circ_mv: float | None
    turnover_rate: float | None


class FlowPoint(BaseModel):
    date: date
    main_net: float  # 大单+特大单 主动买 - 主动卖, 万元
    net: float | None  # Tushare net_mf_amount, 万元


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
    flow: list[FlowPoint] = []


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
                 "bars": [], "valuation": None, "returns": {}, "flow": []}
    if con is None:
        return out
    basic = _rows(con, "SELECT name, industry, market, list_date FROM stock_basic "
                       "WHERE ts_code = ?", [code])
    if basic:
        out.update(basic[0])
    raw = _rows(con, "SELECT trade_date, open, high, low, close, pct_chg, vol, amount "
                     "FROM daily_bar WHERE ts_code = ? ORDER BY trade_date DESC LIMIT ?",
                [code, days])
    adj = {r["trade_date"]: r["adj_factor"] for r in _rows(
        con, "SELECT trade_date, adj_factor FROM adj_factor WHERE ts_code = ?", [code])}
    last_adj = (adj.get(raw[0]["trade_date"]) if raw else None) or 1.0

    def k(d: date) -> float:  # forward-adjust (前复权) so ex-rights gaps don't show
        return (adj.get(d) or last_adj) / last_adj

    bars = [Bar(date=r["trade_date"], open=r["open"] * k(r["trade_date"]),
                high=r["high"] * k(r["trade_date"]), low=r["low"] * k(r["trade_date"]),
                close=r["close"] * k(r["trade_date"]), pct_chg=r["pct_chg"], vol=r["vol"],
                amount=r["amount"])
            for r in reversed(raw) if None not in (r["close"], r["open"], r["high"], r["low"])]
    out["bars"] = bars
    val = _rows(con, "SELECT trade_date, pe_ttm, pb, total_mv, circ_mv, turnover_rate "
                     "FROM daily_basic WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 1", [code])
    out["valuation"] = Valuation(**val[0]) if val else None
    out["flow"] = [FlowPoint(date=r["trade_date"], main_net=r["main"], net=r["net_mf_amount"])
                   for r in reversed(_rows(con, """
        SELECT trade_date, buy_lg_amount + buy_elg_amount - sell_lg_amount - sell_elg_amount
               AS main, net_mf_amount
        FROM moneyflow WHERE ts_code = ? ORDER BY trade_date DESC LIMIT ?""", [code, days]))
                   if r["main"] is not None]
    ytd = None
    if bars:
        prior = [b for b in bars if b.date.year < bars[-1].date.year]
        ytd = bars[-1].close / prior[-1].close - 1 if prior else None
    out["returns"] = {"5日": _ret(bars, 5), "20日": _ret(bars, 20), "60日": _ret(bars, 60),
                      "250日": _ret(bars, 249), "年初至今": ytd}
    return out


def enrich(con: duckdb.DuckDBPyConnection | None, codes: list[str], day: date) -> dict[str, dict]:
    """Industry and latest-on-or-before-``day`` valuation per code (total_mv in 亿元)."""
    if con is None or not codes:
        return {}
    marks = ",".join("?" * len(codes))
    out: dict[str, dict] = {c: {} for c in codes}
    for r in _rows(con, f"SELECT ts_code, industry FROM stock_basic WHERE ts_code IN ({marks})",
                   codes):
        out[r["ts_code"]]["industry"] = r["industry"]
    for r in _rows(con, f"""
            SELECT ts_code, pe_ttm, pb, total_mv, turnover_rate FROM daily_basic
            WHERE ts_code IN ({marks}) AND trade_date = (
              SELECT max(trade_date) FROM daily_basic WHERE trade_date <= ?)""", [*codes, day]):
        out[r["ts_code"]].update(
            pe_ttm=r["pe_ttm"], pb=r["pb"], turnover_rate=r["turnover_rate"],
            total_mv=None if r["total_mv"] is None else round(r["total_mv"] / 1e4, 2))
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
    bars, basic, flows, mins = [], [], [], []
    slots = ([pd.Timedelta(hours=9, minutes=30 + 5 * i) for i in range(1, 25)]
             + [pd.Timedelta(hours=13, minutes=5 * i) for i in range(1, 25)])
    for k, (code, _, _, p0) in enumerate(stocks):
        rets = rng.normal(0.0004, 0.018, len(days))
        close = p0 * np.exp(np.cumsum(rets))
        prev = np.concatenate([[p0], close[:-1]])
        for d, c, pc in zip(days, close, prev, strict=True):
            o = pc * (1 + rng.normal(0, 0.004))
            hi = max(o, c) * (1 + abs(rng.normal(0, 0.006)))
            lo = min(o, c) * (1 - abs(rng.normal(0, 0.006)))
            vol = 1e5 * (1 + abs(rng.normal(0, 0.6)))
            bars.append((code, d.date(), o, hi, lo, c, pc, (c / pc - 1) * 100, vol, c * vol / 10))
            if d >= days[-5]:  # a week of 5-minute bars (a bridge from open to close)
                walk = np.cumsum(rng.normal(0, 1, len(slots)))
                path = o + (c - o) * np.linspace(0, 1, len(slots)) + (walk - walk[-1] * np.linspace(
                    0, 1, len(slots))) * (hi - lo) / 12
                path = np.clip(path, lo, hi)
                prev_p = o
                for t, p in zip(slots, path, strict=True):
                    mins.append((code, d + t, "5min", d.date(), prev_p, max(prev_p, p),
                                 min(prev_p, p), p, vol / 48, p * vol / 480))
                    prev_p = p
            basic.append((code, d.date(), 0.5 + k * 0.2, 10 + 5 * k + c / p0, 1 + k * 0.5,
                          c * 1e6 * (k + 1), c * 8e5 * (k + 1), 1.5 + k * 0.5))
            main = (c / pc - 1) * 5e4 + rng.normal(0, 800)
            flows.append((code, d.date(), 100.0, 100.0, 200.0, 200.0, 300 + max(main, 0),
                           300 + max(-main, 0), 100.0, 100.0, main * 1.2))
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
                    "circ_mv DOUBLE, turnover_rate_f DOUBLE)")
        basic_df = pd.DataFrame(basic)  # noqa: F841
        con.execute("INSERT INTO daily_basic SELECT * FROM basic_df")
        con.execute("CREATE TABLE moneyflow(ts_code VARCHAR, trade_date DATE, "
                    "buy_sm_amount DOUBLE, sell_sm_amount DOUBLE, buy_md_amount DOUBLE, "
                    "sell_md_amount DOUBLE, buy_lg_amount DOUBLE, sell_lg_amount DOUBLE, "
                    "buy_elg_amount DOUBLE, sell_elg_amount DOUBLE, net_mf_amount DOUBLE)")
        flow_df = pd.DataFrame(flows)  # noqa: F841
        con.execute("INSERT INTO moneyflow SELECT * FROM flow_df")
        con.execute("CREATE TABLE minute_bar(ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR, "
                    "trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
                    "vol DOUBLE, amount DOUBLE)")
        min_df = pd.DataFrame(mins)  # noqa: F841
        con.execute("INSERT INTO minute_bar SELECT * FROM min_df")
    finally:
        con.close()
