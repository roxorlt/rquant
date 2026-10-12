"""Live pct change + intraday sparkline for the 「竞价选股」 pool, from 3-second snapshots.

Reads ``snapshots/YYYY-MM-DD/quotes/part-*.parquet`` (plus ``quotes.parquet`` after compaction)
for the pool's codes only. Each file is filtered once and cached by (path, mtime), so a refresh
only scans the parts written since the previous call.
"""

from __future__ import annotations

import threading
from pathlib import Path

import duckdb
import pandas as pd
from pydantic import BaseModel


class PoolLiveRow(BaseModel):
    code: str
    price: float | None
    pre_close: float | None
    pct: float | None  # percent vs prior close, e.g. 3.21
    open: float | None
    intraday_pct: float | None  # percent vs today's open
    time: str | None  # HH:MM:SS of the latest quote
    spark: list[float]  # 1-minute last prices, 09:30 -> latest
    spark_t: list[str]  # HH:MM of each spark point


class PoolLiveData(BaseModel):
    date: str | None
    rows: list[PoolLiveRow]


_CACHE: dict[tuple[str, float, tuple[str, ...]], pd.DataFrame] = {}
_LOCK = threading.Lock()
_COLS = ["ts_code", "time", "price", "pre_close", "open"]


def _read(path: Path, codes: tuple[str, ...]) -> pd.DataFrame:
    key = (str(path), path.stat().st_mtime, codes)
    with _LOCK:
        hit = _CACHE.get(key)
    if hit is not None:
        return hit
    ph = ",".join("?" * len(codes))
    con = duckdb.connect()
    try:
        df = con.execute(
            f"SELECT ts_code, time, price, pre_close, open FROM read_parquet(?) "
            f"WHERE ts_code IN ({ph}) AND price > 0", [str(path), *codes]).df()
    finally:
        con.close()
    with _LOCK:
        if len(_CACHE) > 4000:
            _CACHE.clear()
        _CACHE[key] = df
    return df


def load_pool_live(root: Path, day: str, codes: list[str]) -> PoolLiveData:
    codes_t = tuple(sorted(set(codes)))[:80]
    d = root / "snapshots" / day
    files = ([d / "quotes.parquet"] if (d / "quotes.parquet").is_file() else [])
    files += sorted((d / "quotes").glob("part-*.parquet")) if (d / "quotes").is_dir() else []
    if not codes_t or not files:
        return PoolLiveData(date=day if files else None, rows=[])
    frames = []
    for f in files:
        try:
            frames.append(_read(f, codes_t))
        except (OSError, duckdb.Error):
            continue  # part being renamed/compacted; next refresh picks it up
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=_COLS)
    df = df[(df["time"] >= "09:30:00") & (df["time"] <= "15:00:59")]
    rows = []
    for code in codes:
        g = df[df["ts_code"] == code].sort_values("time", kind="stable")
        if g.empty:
            rows.append(PoolLiveRow(code=code, price=None, pre_close=None, pct=None, open=None,
                                    intraday_pct=None, time=None,
                                    spark=[], spark_t=[]))
            continue
        last = g.iloc[-1]
        pc = last["pre_close"]
        pre = float(pc) if pd.notna(pc) and pc else None
        price = float(last["price"])
        op = last["open"]
        opn = float(op) if pd.notna(op) and op else None
        spark = g.groupby(g["time"].str.slice(0, 5))["price"].last().astype(float).round(3)
        rows.append(PoolLiveRow(
            code=code, price=price, pre_close=pre,
            pct=round((price / pre - 1) * 100, 2) if pre else None, open=opn,
            intraday_pct=round((price / opn - 1) * 100, 2) if opn else None,
            time=str(last["time"]), spark=spark.tolist(),
            spark_t=[str(t) for t in spark.index]))
    return PoolLiveData(date=day, rows=rows)
