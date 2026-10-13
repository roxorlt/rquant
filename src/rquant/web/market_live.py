"""Live 市场全景 from the research 3-second snapshots (today's day dir).

Rebuilds the retired Streamlit market_panorama page on data that is live during the session:
breadth pulse (涨跌/涨停/跌停/炸板), a per-minute breadth curve, industry boards
(median change, turnover, limit-ups, leader) and per-board members. Every snapshot chunk
(part file, or row group of the compacted quotes.parquet) is reduced once to the last quote
per code per minute and cached by (path, mtime, chunk), so a refresh only reads new parts.
"""

from __future__ import annotations

import threading
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pydantic import BaseModel

_COLS = ["ts_code", "name", "time", "price", "high", "pre_close", "amount"]
_CACHE: dict[tuple[str, float, int], pd.DataFrame] = {}
_IND: dict[str, dict[str, str]] = {}
_LOCK = threading.Lock()


class LivePulse(BaseModel):
    total: int
    up: int
    down: int
    flat: int
    limit_up: int
    limit_down: int
    broken: int  # 炸板: touched limit-up today, not sealed now
    up_ratio: float | None
    amount: float  # CNY, whole market turnover so far
    median_pct: float | None


class LiveCurve(BaseModel):
    t: list[str]
    up_ratio: list[float]
    limit_up: list[int]
    broken: list[int]


class LiveBoard(BaseModel):
    name: str
    n: int
    pct_median: float | None
    up_ratio: float | None
    amount: float
    limit_up: int
    broken: int
    leader: str | None
    leader_pct: float | None


class LiveMember(BaseModel):
    code: str
    name: str | None
    pct: float | None
    amount: float | None
    state: str | None  # 涨停 / 炸板 / 跌停


class MarketLiveData(BaseModel):
    date: str | None
    time: str | None
    pulse: LivePulse | None
    curve: LiveCurve | None
    boards: list[LiveBoard]
    members: list[LiveMember]
    board: str | None


def _chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df["price"] > 0) & (df["time"] >= "09:30:00")]
    df = df.assign(m=df["time"].str.slice(0, 5)).sort_values("time", kind="stable")
    hi = df.groupby(["ts_code", "m"])["high"].max()
    last = df.groupby(["ts_code", "m"]).last()
    last["high"] = hi
    return last.reset_index()


_RESULT: dict[tuple, MarketLiveData] = {}


def _chunks(d: Path) -> list[pd.DataFrame]:
    out = []
    files = ([d / "quotes.parquet"] if (d / "quotes.parquet").is_file() else [])
    files += sorted((d / "quotes").glob("part-*.parquet")) if (d / "quotes").is_dir() else []
    for f in files:
        try:
            mt = f.stat().st_mtime
            pf = pq.ParquetFile(f)
            for i in range(pf.num_row_groups):
                key = (str(f), mt, i)
                with _LOCK:
                    hit = _CACHE.get(key)
                if hit is None:
                    hit = _chunk(pf.read_row_group(i, columns=_COLS).to_pandas())
                    with _LOCK:
                        _CACHE[key] = hit
                out.append(hit)
        except (OSError, ValueError):
            continue  # part renamed / being compacted; next refresh picks it up
    with _LOCK:  # drop entries of files that no longer exist (compaction)
        live = {str(f) for f in files}
        for k in [k for k in _CACHE if k[0] not in live]:
            del _CACHE[k]
    return out


def _limit_pct(codes: pd.Series, names: pd.Series) -> np.ndarray:
    pct = np.where(codes.str.startswith(("30", "68")), 0.2, 0.1)
    pct = np.where(codes.str.endswith(".BJ"), 0.3, pct)
    return np.where(names.fillna("").str.contains("ST"), 0.05, pct)


def _industries(root: Path) -> dict[str, str]:
    day = "ind"
    if day in _IND:
        return _IND[day]
    try:
        con = duckdb.connect(str(root / "market.duckdb"), read_only=True)
        try:
            rows = con.execute("SELECT ts_code, industry FROM stock_basic").fetchall()
        finally:
            con.close()
    except duckdb.Error:
        return {}  # daily job holds the lock; retry next call
    _IND[day] = {c: i for c, i in rows if i}
    return _IND[day]


def _state(price: pd.Series, high: pd.Series, up: pd.Series, down: pd.Series) -> pd.Series:
    s = pd.Series(None, index=price.index, dtype=object)
    s[high >= up - 1e-6] = "炸板"
    s[price >= up - 1e-6] = "涨停"
    s[price <= down + 1e-6] = "跌停"
    return s


def load_market_live(root: Path, day: str, board: str | None = None) -> MarketLiveData:
    d = root / "snapshots" / day
    chunks = _chunks(d)
    with _LOCK:
        keys = sorted(k for k in _CACHE if k[0].startswith(str(d)))
        sig = (day, board, len(chunks), tuple(keys[-1:]))
        hit = _RESULT.get(sig)
    if hit is not None:
        return hit
    out = _compute(root, day, chunks, board)
    with _LOCK:
        if len(_RESULT) > 64:
            _RESULT.clear()
        _RESULT[sig] = out
    return out


def _compute(root: Path, day: str, chunks: list[pd.DataFrame], board: str | None,
             ) -> MarketLiveData:
    if not chunks:
        return MarketLiveData(date=None, time=None, pulse=None, curve=None, boards=[],
                              members=[], board=None)
    df = pd.concat(chunks, ignore_index=True).sort_values(["m", "time"], kind="stable")
    last = df.groupby("ts_code").last()
    last["high"] = df.groupby("ts_code")["high"].max()
    last = last[last["pre_close"] > 0]
    lp = _limit_pct(last.index.to_series(), last["name"])
    up_px = np.round(last["pre_close"] * (1 + lp) + 1e-9, 2)
    dn_px = np.round(last["pre_close"] * (1 - lp) + 1e-9, 2)
    last["pct"] = (last["price"] / last["pre_close"] - 1) * 100
    last["state"] = _state(last["price"], last["high"], up_px, dn_px)
    n = len(last)
    up, dn = int((last["pct"] > 0).sum()), int((last["pct"] < 0).sum())
    pulse = LivePulse(total=n, up=up, down=dn, flat=n - up - dn,
                      limit_up=int((last["state"] == "涨停").sum()),
                      limit_down=int((last["state"] == "跌停").sum()),
                      broken=int((last["state"] == "炸板").sum()),
                      up_ratio=round(up / n, 4) if n else None,
                      amount=float(last["amount"].fillna(0).sum()),
                      median_pct=round(float(last["pct"].median()), 2) if n else None)
    # per-minute breadth: forward-filled price / running high per code
    px = df.pivot_table(index="m", columns="ts_code", values="price", aggfunc="last").ffill()
    hi = df.pivot_table(index="m", columns="ts_code", values="high", aggfunc="max").cummax()
    cols = [c for c in px.columns if c in last.index]
    px, hi = px[cols], hi.reindex(columns=cols).ffill()
    pre = last.loc[cols, "pre_close"]
    upl = up_px.loc[cols]
    valid = px.notna().sum(axis=1).replace(0, np.nan)
    curve = LiveCurve(
        t=list(px.index),
        up_ratio=((px.gt(pre, axis=1)).sum(axis=1) / valid).fillna(0).round(4).tolist(),
        limit_up=px.ge(upl - 1e-6, axis=1).sum(axis=1).astype(int).tolist(),
        broken=(hi.ge(upl - 1e-6, axis=1) & px.lt(upl - 1e-6, axis=1)).sum(axis=1)
        .astype(int).tolist())
    ind = _industries(root)
    last["industry"] = [ind.get(c) for c in last.index]
    boards = []
    for name, g in last.dropna(subset=["industry"]).groupby("industry"):
        lead = g["pct"].idxmax()
        boards.append(LiveBoard(
            name=str(name), n=len(g), pct_median=round(float(g["pct"].median()), 2),
            up_ratio=round(float((g["pct"] > 0).mean()), 3),
            amount=float(g["amount"].fillna(0).sum()),
            limit_up=int((g["state"] == "涨停").sum()), broken=int((g["state"] == "炸板").sum()),
            leader=g.at[lead, "name"], leader_pct=round(float(g.at[lead, "pct"]), 2)))
    boards.sort(key=lambda b: -b.amount)
    members = []
    board = board or (boards[0].name if boards else None)
    if board:
        g = last[last["industry"] == board].sort_values("pct", ascending=False)
        members = [LiveMember(code=str(c), name=r["name"], pct=round(float(r["pct"]), 2),
                              amount=float(r["amount"]) if pd.notna(r["amount"]) else None,
                              state=r["state"] if isinstance(r["state"], str) else None)
                   for c, r in g.head(200).iterrows()]
    return MarketLiveData(date=day, time=str(df["time"].max()), pulse=pulse, curve=curve,
                          boards=boards, members=members, board=board)
