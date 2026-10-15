"""Live 市场全景 from the research 3-second snapshots (today's day dir).

Rebuilds the retired Streamlit market_panorama page on data that is live during the session:
breadth pulse (涨跌/涨停/跌停/炸板), a per-minute breadth curve, industry boards
(median change, turnover, limit-ups, leader) and per-board members. Every snapshot chunk
(part file, or row group of the compacted quotes.parquet) is reduced once to the last quote
per code per minute and cached by (path, mtime, chunk), so a refresh only reads new parts.
"""

from __future__ import annotations

import contextlib
import threading
import time
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
                    hit.attrs["rq_key"] = key
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


def _limit_pct(codes: pd.Series, names: pd.Series, day: str | None = None) -> np.ndarray:
    """Date-aware exchange limit pct (主板 ST 5% → 10% from 2026-07-06), see rquant.limit_rules."""
    from datetime import date as _date

    from rquant.limit_rules import limit_pct_vec

    names = pd.Series(names, dtype=object).fillna("")
    return limit_pct_vec(np.asarray(codes, dtype=object), names.to_numpy(),
                         _date.fromisoformat(day) if day else None)


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


_LATEST: dict[tuple[str, str | None], tuple[float, MarketLiveData]] = {}
_INFLIGHT: set[tuple] = set()
#: serve the previous result while a newer one is computed in the background (a full
#: recompute takes several seconds once the day has many snapshot parts)
_STALE_OK_S = 90.0


def _refresh(root: Path, day: str, chunks: list[pd.DataFrame], board: str | None,
             sig: tuple) -> MarketLiveData:
    try:
        out = _compute(root, day, chunks, board)
        with _LOCK:
            if len(_RESULT) > 64:
                _RESULT.clear()
            _RESULT[sig] = out
            _LATEST[(day, board)] = (time.monotonic(), out)
        return out
    finally:
        with _LOCK:
            _INFLIGHT.discard(sig)


def load_market_live(root: Path, day: str, board: str | None = None) -> MarketLiveData:
    d = root / "snapshots" / day
    chunks = _chunks(d)
    with _LOCK:
        keys = sorted(k for k in _CACHE if k[0].startswith(str(d)))
        sig = (day, board, len(chunks), tuple(keys[-1:]))
        hit = _RESULT.get(sig)
        prev = _LATEST.get((day, board))
        busy = sig in _INFLIGHT
        stale = prev is not None and time.monotonic() - prev[0] < _STALE_OK_S
        if hit is None and (stale or busy):
            if not busy:
                _INFLIGHT.add(sig)
                threading.Thread(target=_refresh, args=(root, day, chunks, board, sig),
                                 daemon=True, name="market-live-refresh").start()
            if prev is not None:
                return prev[1]
        elif hit is None:
            _INFLIGHT.add(sig)
    if hit is not None:
        return hit
    return _refresh(root, day, chunks, board, sig)


def warm_market_live(root: Path) -> None:
    """Fill the snapshot/result caches for the latest day (run once at startup, off-thread)."""
    snaps = root / "snapshots"
    days = sorted(p.name for p in snaps.glob("20??-??-??") if p.is_dir()) if snaps.is_dir() else []
    if days:
        with contextlib.suppress(Exception):  # best effort; first request computes it instead
            load_market_live(root, days[-1], None)


def _finish(root: Path, day: str, last: pd.DataFrame, curve_fn, tmax: str,
            board: str | None) -> MarketLiveData:
    """Shared tail: ``last`` = one row per code (sorted index) with name/price/high/
    pre_close/amount. ``curve_fn(last, up_px)`` builds the per-minute breadth curve."""
    last = last[last["pre_close"] > 0].copy()
    lp = _limit_pct(last.index.to_series(), last["name"], day)
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
    curve = curve_fn(last, up_px)
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
    return MarketLiveData(date=day, time=tmax, pulse=pulse, curve=curve,
                          boards=boards, members=members, board=board)


_EMPTY = dict(date=None, time=None, pulse=None, curve=None, boards=[], members=[], board=None)


def _compute_ref(root: Path, day: str, chunks: list[pd.DataFrame], board: str | None,
                 ) -> MarketLiveData:
    """Reference pandas implementation (full recompute); kept for parity tests/fallback."""
    if not chunks:
        return MarketLiveData(**_EMPTY)
    df = pd.concat(chunks, ignore_index=True).sort_values(["m", "time"], kind="stable")
    last = df.groupby("ts_code").last()
    last["high"] = df.groupby("ts_code")["high"].max()

    def curve_fn(last: pd.DataFrame, up_px: pd.Series) -> LiveCurve:
        # per-minute breadth: forward-filled price / running high per code
        px = df.pivot_table(index="m", columns="ts_code", values="price", aggfunc="last").ffill()
        hi = df.pivot_table(index="m", columns="ts_code", values="high", aggfunc="max").cummax()
        cols = [c for c in px.columns if c in last.index]
        px, hi = px[cols], hi.reindex(columns=cols).ffill()
        pre = last.loc[cols, "pre_close"]
        upl = up_px.loc[cols]
        valid = px.notna().sum(axis=1).replace(0, np.nan)
        return LiveCurve(
            t=list(px.index),
            up_ratio=((px.gt(pre, axis=1)).sum(axis=1) / valid).fillna(0).round(4).tolist(),
            limit_up=px.ge(upl - 1e-6, axis=1).sum(axis=1).astype(int).tolist(),
            broken=(hi.ge(upl - 1e-6, axis=1) & px.lt(upl - 1e-6, axis=1)).sum(axis=1)
            .astype(int).tolist())

    return _finish(root, day, last, curve_fn, str(df["time"].max()), board)


# ── numpy grid path (incremental) ────────────────────────────────────────────────────
# Chunks arrive in chronological order (row groups of quotes.parquet, then part files), and
# each reduced chunk holds one row per (code, minute), sorted by (code, minute). So "last
# per code/cell over the (minute, time)-sorted concat" == "last write wins, chunk by chunk":
# no global sort, no groupby, no pivot. State per day is kept and only new chunks are
# folded in; the curve is recomputed from the earliest touched minute onward.

_AXIS = [f"{h:02d}:{m:02d}" for h in range(9, 16) for m in range(60) if (h, m) >= (9, 30)]
_AXIS_IX = pd.Index(_AXIS)
_GRID_LOCK = threading.Lock()


class _OutOfAxisError(Exception):
    pass


class _DayGrid:
    _VCOLS = ("name", "time", "price", "pre_close", "amount")

    def __init__(self) -> None:
        self.keys: list = []
        self.codes: list[str] = []
        self.code_ix: dict[str, int] = {}
        nm = len(_AXIS)
        self.cap = 0
        self.px = np.empty((nm, 0))
        self.hg = np.empty((nm, 0))
        self.seen = np.zeros(nm, dtype=bool)
        self.vals: dict[str, np.ndarray] = {}
        self.hmax = np.empty(0)
        self.tmax = ""
        self.dirty_from = 0  # earliest minute row whose curve needs recompute
        self.curve_cache: dict | None = None

    def _grow(self, n: int) -> None:
        if n <= self.cap:
            return
        cap = max(n, int(self.cap * 1.5) + 256)
        nm = len(_AXIS)
        px = np.full((nm, cap), np.nan)
        px[:, :self.cap] = self.px
        hg = np.full((nm, cap), np.nan)
        hg[:, :self.cap] = self.hg
        self.px, self.hg = px, hg
        hm = np.full(cap, np.nan)
        hm[:self.cap] = self.hmax
        self.hmax = hm
        for c in self._VCOLS:
            old = self.vals.get(c)
            obj = c in ("name", "time")
            new = np.full(cap, None, dtype=object) if obj else np.full(cap, np.nan)
            if old is not None:
                new[:self.cap] = old
            self.vals[c] = new
        self.cap = cap

    def add(self, ch: pd.DataFrame) -> None:
        if ch.empty:
            return
        mf, muniq = pd.factorize(ch["m"])
        mmap = _AXIS_IX.get_indexer(pd.Index(muniq).astype(str))
        if (mmap < 0).any():
            raise _OutOfAxisError
        mi = mmap[mf].astype(np.int64)
        cf, cuniq = pd.factorize(ch["ts_code"])
        for c in cuniq:
            c = str(c)
            if c not in self.code_ix:
                self.code_ix[c] = len(self.codes)
                self.codes.append(c)
        self._grow(len(self.codes))
        cmap = np.fromiter((self.code_ix[str(c)] for c in cuniq), dtype=np.int64,
                           count=len(cuniq))
        ci = cmap[cf]
        n = len(ci)
        rows = np.arange(n)
        price = ch["price"].to_numpy(float)
        high = ch["high"].to_numpy(float)
        # rows are chronological per code (chunks concatenated in order); a (code, minute)
        # cell may repeat across chunks -> keep its last row explicitly, then scatter
        cell = mi * self.cap + ci
        lastrow = np.full(len(_AXIS) * self.cap, -1, dtype=np.int64)
        np.maximum.at(lastrow, cell, rows)
        keep = lastrow[lastrow >= 0]
        self.px[mi[keep], ci[keep]] = price[keep]
        np.fmax.at(self.hg, (mi, ci), high)
        np.fmax.at(self.hmax, ci, high)
        for col in self._VCOLS:  # last non-null per code (groupby.last semantics)
            ok = ch[col].notna().to_numpy()
            if not ok.any():
                continue
            last = np.full(self.cap, -1, dtype=np.int64)
            np.maximum.at(last, ci[ok], rows[ok])
            tgt = np.flatnonzero(last >= 0)
            vals = ch[col].iloc[last[tgt]]
            self.vals[col][tgt] = vals.to_numpy(dtype=object if col in ("name", "time")
                                                else float)
        self.seen[mi] = True
        self.tmax = max(self.tmax, str(ch["time"].max()))
        self.dirty_from = min(self.dirty_from, int(mi.min()))

    def last_frame(self) -> pd.DataFrame:
        n = len(self.codes)
        df = pd.DataFrame({c: self.vals[c][:n] for c in self._VCOLS},
                          index=pd.Index(self.codes, name="ts_code"))
        df["high"] = self.hmax[:n]
        for c in ("price", "pre_close", "amount", "high"):
            df[c] = df[c].astype(float)
        return df.sort_index()

    def curve(self, last: pd.DataFrame, up_px: pd.Series) -> LiveCurve:
        n = len(self.codes)
        cols = np.fromiter((self.code_ix[c] for c in last.index), dtype=np.int64, count=len(last))
        pre = last["pre_close"].to_numpy(float)
        upl = up_px.to_numpy(float) - 1e-6
        sig = (len(cols), hash(cols.tobytes()), hash(pre.tobytes()), hash(upl.tobytes()))
        cc = self.curve_cache
        start = self.dirty_from if (cc is not None and cc["sig"] == sig) else 0
        nm = len(_AXIS)
        if start == 0 or cc is None:
            pxf = np.full((nm, len(cols)), np.nan)
            hif = np.full((nm, len(cols)), np.nan)
            ur = np.zeros(nm)
            lu = np.zeros(nm, dtype=np.int64)
            br = np.zeros(nm, dtype=np.int64)
        else:
            pxf, hif, ur, lu, br = cc["pxf"], cc["hif"], cc["ur"], cc["lu"], cc["br"]
        prev_p = pxf[start - 1].copy() if start > 0 else np.full(len(cols), np.nan)
        prev_h = hif[start - 1].copy() if start > 0 else np.full(len(cols), np.nan)
        rows_p = self.px[start:, :n][:, cols]
        rows_h = self.hg[start:, :n][:, cols]
        for k in range(rows_p.shape[0]):  # ffill price, running max high (minute loop, not stock)
            r = rows_p[k]
            prev_p = np.where(np.isnan(r), prev_p, r)
            prev_h = np.fmax(prev_h, rows_h[k])
            pxf[start + k] = prev_p
            hif[start + k] = prev_h
        pm, hm = pxf[start:], hif[start:]
        with np.errstate(invalid="ignore"):
            valid = (~np.isnan(pm)).sum(1)
            gt = (pm > pre).sum(1)
            lim = pm >= upl
            ur[start:] = np.where(valid > 0, gt / np.maximum(valid, 1), 0.0)
            lu[start:] = lim.sum(1)
            br[start:] = ((hm >= upl) & (pm < upl)).sum(1)
        self.curve_cache = dict(sig=sig, pxf=pxf, hif=hif, ur=ur, lu=lu, br=br)
        self.dirty_from = nm
        rows = np.flatnonzero(self.seen)
        return LiveCurve(t=[_AXIS[i] for i in rows],
                         up_ratio=np.round(ur[rows], 4).tolist(),
                         limit_up=lu[rows].astype(int).tolist(),
                         broken=br[rows].astype(int).tolist())


_GRIDS: dict[str, _DayGrid] = {}


def _grid_for(day: str, chunks: list[pd.DataFrame]) -> _DayGrid:
    keys = [ch.attrs.get("rq_key", id(ch)) for ch in chunks]
    g = _GRIDS.get(day)
    if g is None or g.keys != keys[:len(g.keys)]:
        g = _DayGrid()  # first call, or files compacted / rewritten: rebuild
    new = chunks[len(g.keys):]
    if new:  # fold all new chunks in one vectorized pass (order preserved by concat)
        g.add(pd.concat(new, ignore_index=True) if len(new) > 1 else new[0])
    g.keys = keys
    _GRIDS[day] = g
    for k in [k for k in _GRIDS if k != day][:-1]:  # keep at most two days
        del _GRIDS[k]
    return g


def _compute(root: Path, day: str, chunks: list[pd.DataFrame], board: str | None,
             ) -> MarketLiveData:
    if not chunks:
        return MarketLiveData(**_EMPTY)
    try:
        with _GRID_LOCK:
            g = _grid_for(day, chunks)
            last = g.last_frame()
            return _finish(root, day, last, g.curve, g.tmax, board)
    except _OutOfAxisError:
        with _GRID_LOCK:
            _GRIDS.pop(day, None)
        return _compute_ref(root, day, chunks, board)
