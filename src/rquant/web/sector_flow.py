"""板块资金流 (estimated active net buy per industry) from the research 3-second snapshots.

Per stock, each snapshot-to-snapshot turnover increment Δamount is signed by a tick rule:
buy if price >= previous ask1, sell if price <= previous bid1, otherwise by up/down tick
(unchanged -> 0, i.e. split evenly). The first traded snapshot of the day (normally the 09:25
auction print) is signed by auction price vs pre_close. Sectors = stock_basic.industry (same
as 市场全景). This is a Lee-Ready style *estimate*, not exchange-reported order flow.

Incremental: one state per day; each new parquet chunk is folded in once (carry = last row per
code), keeping only dense per-sector-minute sums, per-code cumulative net and a float32
per-code-minute price matrix (~6 MB). Nothing re-reads the whole day per request.

3-second updates: the collector also writes each ~3s round as a delta file under
``snapshots/<day>/stream/`` (``rquant.research.snapshots.DeltaStream``). The official state is
folded only from the 30s shards (source of truth). On refresh, the pending deltas (rounds whose
seq is above the latest *folded* shard's recorded ``{"run","seq"}``) are folded onto a throwaway
clone of the official state, so the served result moves every round; when the covering shard
lands those deltas are dropped by seq and the same rows enter the official state exactly once.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections.abc import Callable
from datetime import date as _date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pydantic import BaseModel

_COLS = ["ts_code", "name", "time", "price", "pre_close", "amount", "b1_p", "a1_p"]


def _minutes() -> list[str]:
    out = ["09:25"]
    for h0, m0, h1, m1 in ((9, 30, 11, 30), (13, 0, 15, 0)):
        t = h0 * 60 + m0
        while t <= h1 * 60 + m1:
            out.append(f"{t // 60:02d}:{t % 60:02d}")
            t += 1
    return out


MINUTES = _minutes()
_MIDX = {m: i for i, m in enumerate(MINUTES)}


def _slot(hhmm: pd.Series) -> np.ndarray:
    h = hhmm.str.slice(0, 2).astype(int).to_numpy()
    m = hhmm.str.slice(3, 5).astype(int).to_numpy()
    t = h * 60 + m
    out = np.where(t < 570, 0, 0)
    am = (t >= 570) & (t < 13 * 60)
    out = np.where(am, 1 + np.minimum(t, 690) - 570, out)
    pm = t >= 13 * 60
    out = np.where(pm, 1 + 121 + np.minimum(t, 900) - 780, out)
    return out


class _Day:
    def __init__(self, sectors: dict[str, str]):
        self.lock = threading.Lock()
        self.keys: list[tuple] = []
        self.code_ix: dict[str, int] = {}
        self.codes: list[str] = []
        self.names: list[str | None] = []
        self.pre = np.zeros(0)
        self.net = np.zeros(0)
        self.amt = np.zeros(0)
        self.px = np.full((0, len(MINUTES)), np.nan, dtype=np.float32)
        self.cnm = np.zeros((0, len(MINUTES)), dtype=np.float32)  # net per code-minute
        self.am = np.full((0, len(MINUTES)), np.nan, dtype=np.float32)  # cum turnover
        self.sec_of = np.zeros(0, dtype=np.int32)
        self.sector_names = sorted(set(sectors.values()))
        self.sec_ix = {s: i for i, s in enumerate(self.sector_names)}
        self.sectors = sectors
        self.sec_min = np.zeros((len(self.sector_names) + 1, len(MINUTES)))  # last row = 未分类
        self.sec_amt = np.zeros_like(self.sec_min)  # turnover per sector-minute
        self.carry: pd.DataFrame | None = None
        self.last_time: str | None = None
        self.max_slot = -1
        self.landed: dict[str, int] = {}  # collector run -> max delta seq inside folded shards
        self.late_dropped = 0  # rows not newer than the code's carry row (see fold)

    def clone(self) -> _Day:
        """A copy for folding provisional deltas (the official state is never touched)."""
        c = _Day.__new__(_Day)
        c.lock = threading.Lock()
        c.keys = list(self.keys)
        c.code_ix, c.codes, c.names = dict(self.code_ix), list(self.codes), list(self.names)
        for a in ("pre", "net", "amt", "px", "cnm", "am", "sec_of", "sec_min", "sec_amt"):
            setattr(c, a, getattr(self, a).copy())
        c.sector_names, c.sec_ix, c.sectors = self.sector_names, self.sec_ix, self.sectors
        c.carry, c.last_time, c.max_slot = self.carry, self.last_time, self.max_slot
        c.landed = dict(self.landed)
        c.late_dropped = self.late_dropped
        return c

    def _ensure(self, codes: np.ndarray, names: np.ndarray) -> np.ndarray:
        new = [c for c in dict.fromkeys(codes) if c not in self.code_ix]
        if new:
            nm = dict(zip(codes, names, strict=True))
            for c in new:
                self.code_ix[c] = len(self.codes)
                self.codes.append(c)
                self.names.append(nm.get(c))
            k = len(new)
            self.pre = np.concatenate([self.pre, np.zeros(k)])
            self.net = np.concatenate([self.net, np.zeros(k)])
            self.amt = np.concatenate([self.amt, np.zeros(k)])
            self.px = np.vstack([self.px, np.full((k, len(MINUTES)), np.nan, np.float32)])
            self.cnm = np.vstack([self.cnm, np.zeros((k, len(MINUTES)), np.float32)])
            self.am = np.vstack([self.am, np.full((k, len(MINUTES)), np.nan, np.float32)])
            unk = len(self.sector_names)
            self.sec_of = np.concatenate([self.sec_of, np.array(
                [self.sec_ix.get(self.sectors.get(c, ""), unk) for c in new], np.int32)])
        return np.array([self.code_ix[c] for c in codes], dtype=np.int64)

    def fold(self, df: pd.DataFrame) -> None:
        df = df[(df["price"] > 0) & (df["time"] >= "09:25:00")]
        if df.empty:
            return
        df = df.assign(_new=True)
        if self.carry is not None:
            # Late rows (flowfix 2026-10-19): the collector's sources (ts_k/sina/ts_tick) can put a
            # row for a code into a LATER chunk than a newer row already folded (e.g. sina 11:29:59
            # after ts_k 11:30:00). Sorted ahead of the carry row it used to become the code's
            # "first row of the day" and re-count all turnover to date. amount is cumulative, so a
            # row not newer than the carry adds nothing: drop it.
            last_t = dict(zip(self.carry["ts_code"], self.carry["time"], strict=True))
            ct = df["ts_code"].map(last_t)
            df = df[~(ct.notna() & (df["time"] <= ct)).to_numpy()]
            self.late_dropped += int(len(ct) - len(df))
            if df.empty:
                return
            df = pd.concat([self.carry.assign(_new=False), df], ignore_index=True)
        df = df.sort_values(["ts_code", "time"], kind="stable").reset_index(drop=True)
        same = df["ts_code"].eq(df["ts_code"].shift())
        # cumulative turnover never goes back: a stale source within a chunk must not make the
        # next row re-count the difference
        df["amount"] = df.groupby("ts_code", sort=False)["amount"].cummax()
        p_amt = df["amount"].shift().where(same)
        p_px = df["price"].shift().where(same)
        p_a1 = df["a1_p"].shift().where(same)
        p_b1 = df["b1_p"].shift().where(same)
        px = df["price"]
        tick = np.sign(px - p_px)
        sign = np.where((p_a1 > 0) & (px >= p_a1 - 1e-9), 1.0,
                        np.where((p_b1 > 0) & (px <= p_b1 + 1e-9), -1.0, tick))
        first = ~same
        sign = np.where(first, np.sign(px - df["pre_close"]), sign)
        d = (df["amount"] - p_amt.fillna(0)).clip(lower=0).fillna(0).to_numpy()
        # ignore re-seen carry rows themselves (their delta was counted before)
        newm = df["_new"].to_numpy()
        flow = np.where(newm, d * np.nan_to_num(sign), 0.0)
        new = df[newm]
        ix_all = self._ensure(df["ts_code"].to_numpy(), df["name"].to_numpy())
        slots = _slot(df["time"].str.slice(0, 5))
        np.add.at(self.net, ix_all, flow)
        np.add.at(self.cnm, (ix_all, slots), flow.astype(np.float32))
        np.add.at(self.sec_min, (self.sec_of[ix_all], slots), flow)
        np.add.at(self.sec_amt, (self.sec_of[ix_all], slots), np.where(newm, d, 0.0))
        # last price per code-minute (rows sorted by time within code)
        ixn, sn = ix_all[newm], slots[newm]
        self.px[ixn, sn] = new["price"].to_numpy(np.float32)
        self.pre[ixn] = new["pre_close"].to_numpy()
        self.amt[ixn] = new["amount"].to_numpy()
        self.am[ixn, sn] = new["amount"].to_numpy(np.float32)
        self.max_slot = max(self.max_slot, int(sn.max()))
        t = str(new["time"].max())
        self.last_time = max(self.last_time or t, t)
        self.carry = df.groupby("ts_code", sort=False).tail(1)[_COLS].reset_index(drop=True)


_DAYS: dict[str, _Day] = {}
_CIRC: dict[str, dict[str, float]] = {}
_SECT: dict[str, dict[str, str]] = {}
_RESULT: dict[str, tuple] = {}
_GLOCK = threading.Lock()


class FlowSeries(BaseModel):
    name: str
    n: int
    net: float  # CNY, cumulative since 09:25
    amount: float  # CNY turnover since 09:25
    pct: float | None  # mean member % change now
    limit_up: int
    series: list[float]  # 亿 CNY cumulative net per minute (aligned to t)
    ratio: list[float]  # cumulative net / cumulative turnover, %
    pct_series: list[float | None]  # mean member % change per minute


class FlowStock(BaseModel):
    code: str
    name: str | None
    pct: float | None
    net: float  # CNY
    net_ratio: float | None  # net / float market cap
    amount: float
    sealed: bool  # sitting on limit-up now
    spark: list[float | None]  # price per minute (aligned to t), forward-filled
    pre_close: float | None


class FlowLeader(BaseModel):
    name: str
    net: float
    pct: float | None
    limit_up: int
    stocks: list[FlowStock]


class FlowSummary(BaseModel):
    """Top cards. No market-wide net (2026-10-20, roxor): the sum of the tick-rule estimate over all
    stocks is "active buy - active sell since the open", not a market net inflow, so it is no longer
    served. See docs/数据口径核实-20261019.md §6."""

    amount: float  # CNY, Σ snapshot cumulative turnover (last row per code)
    up: int
    down: int
    limit_up: int
    limit_down: int
    # 两市成交额 (沪深京全 A, 含北交所, same as docs/flowfix/turnover_daily.csv), 亿 CNY
    turnover: float | None = None
    turnover_source: str | None = None  # "tushare_daily" | "snapshot_last" | None
    prev_date: str | None = None  # previous open day per trade_calendar
    prev_turnover: float | None = None  # 亿 CNY, Tushare daily (daily_bar) of prev_date
    turnover_chg: float | None = None  # 亿 CNY, turnover - prev_turnover
    turnover_chg_pct: float | None = None  # %, turnover_chg / prev_turnover * 100


class SectorFlowData(BaseModel):
    date: str | None
    time: str | None
    version: str | None = None
    unchanged: bool = False
    warming: bool = False
    t: list[str] = []
    summary: FlowSummary | None = None
    sectors: list[FlowSeries] = []  # sorted by net desc
    leaders: list[FlowLeader] = []  # top-3 inflow sectors x top-3 stocks by net / float mv
    days: list[str] = []


def _industries(root: Path) -> dict[str, str] | None:
    if "ind" in _SECT:
        return _SECT["ind"]
    try:
        con = duckdb.connect(str(root / "market.duckdb"), read_only=True)
        try:
            rows = con.execute("SELECT ts_code, industry FROM stock_basic").fetchall()
        finally:
            con.close()
    except duckdb.Error:
        return None
    _SECT["ind"] = {c: i for c, i in rows if i}
    return _SECT["ind"]


def _circ(root: Path, day: str) -> dict[str, float]:
    if day in _CIRC:
        return _CIRC[day]
    try:
        con = duckdb.connect(str(root / "market.duckdb"), read_only=True)
        try:
            rows = con.execute(
                "SELECT ts_code, circ_mv FROM daily_basic WHERE trade_date = "
                "(SELECT max(trade_date) FROM daily_basic WHERE trade_date < ?)",
                [_date.fromisoformat(day)]).fetchall()
        finally:
            con.close()
    except duckdb.Error:
        return {}
    if len(_CIRC) > 4:
        _CIRC.clear()
    _CIRC[day] = {c: float(v) * 1e4 for c, v in rows if v}  # 万元 -> CNY
    return _CIRC[day]


def _files(d: Path) -> list[Path]:
    files = [d / "quotes.parquet"] if (d / "quotes.parquet").is_file() else []
    if (d / "quotes").is_dir():
        files += sorted((d / "quotes").glob("part-*.parquet"))
    return files


_FOOTER: dict[str, tuple[float, int, tuple[str, int] | None]] = {}
_STREAM_KEY = b"rquant_stream"


def _footer(f: Path) -> tuple[float, int, tuple[str, int] | None]:
    """(mtime, row groups, (run, seq) the shard covers). Parts are immutable: cached by mtime."""
    mt = f.stat().st_mtime
    hit = _FOOTER.get(str(f))
    if hit and hit[0] == mt:
        return hit
    md = pq.read_metadata(f)
    cover = None
    raw = (md.metadata or {}).get(_STREAM_KEY)
    if raw:
        try:
            j = json.loads(raw)
            cover = (str(j["run"]), int(j["seq"]))
        except (ValueError, KeyError, TypeError):
            cover = None
    if len(_FOOTER) > 5000:
        _FOOTER.clear()
    _FOOTER[str(f)] = out = (mt, md.num_row_groups, cover)
    return out


def _update(root: Path, day: str) -> _Day | None:
    sectors = _industries(root)
    if sectors is None:
        return None
    with _GLOCK:
        st = _DAYS.get(day)
        if st is None:
            if len(_DAYS) >= 2:  # today + one browsed history day
                _DAYS.pop(next(iter(_DAYS)))
            st = _DAYS[day] = _Day(sectors)
    with st.lock:
        keys: list[tuple] = []
        covers: dict[str, tuple[str, int] | None] = {}
        files = _files(root / "snapshots" / day)
        for f in files:
            try:
                mt, n, covers[str(f)] = _footer(f)
            except (OSError, ValueError):
                break
            keys += [(str(f), mt, i) for i in range(n)]
        if keys[: len(st.keys)] != st.keys:  # compaction rewrote the day: rebuild
            with _GLOCK:
                st = _DAYS[day] = _Day(sectors)
            st.lock.acquire()
            release = True
        else:
            release = False
        try:
            for k in keys[len(st.keys):]:
                try:
                    pf = pq.ParquetFile(k[0])
                    df = pf.read_row_group(k[2], columns=_COLS).to_pandas()
                except (OSError, ValueError):
                    break  # part being written/renamed; retry next call
                st.fold(df)
                st.keys.append(k)
                cov = covers.get(k[0])
                if cov and cov[1] > st.landed.get(cov[0], 0):
                    st.landed[cov[0]] = cov[1]
        finally:
            if release:
                st.lock.release()
    return st


_TURN: dict[tuple[str, str], tuple[float, float | None]] = {}  # (root, day) -> (checked, Σ亿)
_PREV: dict[tuple[str, str], str | None] = {}  # (root, day) -> previous open day
TURN_RETRY = 300.0  # s: re-check daily_bar for a day not ingested yet at most this often


def _db_turnover(root: Path, day: str) -> float | None:
    """Tushare daily Σamount for the day, 亿 CNY (daily_bar.amount is 千元). All A shares in
    daily_bar: 沪 + 深 + 北交所 (the same total as docs/flowfix/turnover_daily.csv
    tushare_daily_yi). None when the day is not ingested yet."""
    key = (str(root), day)
    hit = _TURN.get(key)
    if hit and (hit[1] is not None or time.time() - hit[0] < TURN_RETRY):
        return hit[1]
    try:
        con = duckdb.connect(str(root / "market.duckdb"), read_only=True)
        try:
            n, s = con.execute("SELECT count(*), sum(amount) FROM daily_bar WHERE trade_date = ?",
                               [_date.fromisoformat(day)]).fetchone()
        finally:
            con.close()
    except duckdb.Error:
        n, s = 0, None
    v = round(float(s) * 1e3 / 1e8, 3) if n and s else None
    if len(_TURN) > 64:
        _TURN.clear()
    _TURN[key] = (time.time(), v)
    return v


def _prev_open(root: Path, day: str) -> str | None:
    """Previous trading day from trade_calendar (the authoritative exchange calendar)."""
    key = (str(root), day)
    if key in _PREV:
        return _PREV[key]
    try:
        con = duckdb.connect(str(root / "market.duckdb"), read_only=True)
        try:
            row = con.execute("SELECT max(cal_date) FROM trade_calendar WHERE is_open AND cal_date < ?",
                              [_date.fromisoformat(day)]).fetchone()
        finally:
            con.close()
    except duckdb.Error:
        return None  # not cached: retry next call
    out = row[0].isoformat() if row and row[0] else None
    if len(_PREV) > 64:
        _PREV.clear()
    _PREV[key] = out
    return out


def turnover_fields(root: Path, day: str, snapshot_amount: float) -> dict:
    """两市成交额 card. turnover: Tushare daily total once ingested, else the snapshot total
    (Σ last cumulative amount per code; intraday = turnover so far); prev_turnover: Tushare daily
    total of the previous open day (trade_calendar). All 亿 CNY; missing -> None."""
    tu = _db_turnover(root, day)
    src = "tushare_daily"
    if tu is None and snapshot_amount > 0:
        tu, src = round(snapshot_amount / 1e8, 3), "snapshot_last"
    elif tu is None:
        src = None
    prev = _prev_open(root, day)
    pt = _db_turnover(root, prev) if prev else None
    chg = round(tu - pt, 3) if tu is not None and pt else None
    pct = round(chg / pt * 100, 2) if chg is not None and pt else None
    return dict(turnover=tu, turnover_source=src, prev_date=prev, prev_turnover=pt,
                turnover_chg=chg, turnover_chg_pct=pct)


MIN_CIRC = 30e8  # leader floor: float cap >= 30亿
MIN_AMT = 1e8  # and turnover >= 1亿


def _limit_up(codes: list[str], names: list, pre: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    from rquant.limit_rules import limit_pct_vec

    lp = limit_pct_vec(codes, pd.Series(names, dtype=object).fillna("").to_numpy())
    return np.round(pre * (1 + lp) + 1e-9, 2), np.round(pre * (1 - lp) + 1e-9, 2)


def _compute(st: _Day, root: Path, day: str, version: str | None = None) -> SectorFlowData:
    upto = st.max_slot
    nsec = len(st.sector_names)
    cum = np.cumsum(st.sec_min[:, : upto + 1], axis=1)
    camt = np.cumsum(st.sec_amt[:, : upto + 1], axis=1)
    counts = np.bincount(st.sec_of, minlength=nsec + 1)
    px = pd.DataFrame(st.px[:, : upto + 1]).ffill(axis=1).to_numpy()
    pre = np.where(st.pre > 0, st.pre, np.nan)
    pctm = (px / pre[:, None] - 1) * 100
    ok = np.isfinite(pctm)
    psum = np.zeros((nsec + 1, upto + 1))
    pcnt = np.zeros((nsec + 1, upto + 1))
    np.add.at(psum, st.sec_of, np.where(ok, pctm, 0))
    np.add.at(pcnt, st.sec_of, ok)
    with np.errstate(invalid="ignore", divide="ignore"):
        smean = psum / pcnt
        ratio = np.where(camt > 0, cum / camt * 100, 0.0)
    last = px[:, -1]
    upx, dpx = _limit_up(st.codes, st.names, st.pre)
    sealed = np.isfinite(last) & (last >= upx - 1e-6) & (st.pre > 0)
    floored = np.isfinite(last) & (last <= dpx + 1e-6) & (st.pre > 0)
    lu_sec = np.bincount(st.sec_of, weights=sealed, minlength=nsec + 1)
    nowp = pctm[:, -1]
    summary = FlowSummary(amount=float(st.amt.sum()),
                          up=int((nowp > 0).sum()), down=int((nowp < 0).sum()),
                          limit_up=int(sealed.sum()), limit_down=int(floored.sum()),
                          **turnover_fields(root, day, float(st.amt.sum())))

    def r(v: float) -> float | None:
        return None if not np.isfinite(v) else round(float(v), 2)

    order = [int(i) for i in np.argsort(-cum[:nsec, -1])]
    sectors = [FlowSeries(
        name=st.sector_names[i], n=int(counts[i]), net=float(cum[i, -1]),
        amount=float(camt[i, -1]), pct=r(smean[i, -1]), limit_up=int(lu_sec[i]),
        series=np.round(cum[i] / 1e8, 2).tolist(), ratio=np.round(ratio[i], 2).tolist(),
        pct_series=[r(v) for v in smean[i]]) for i in order if counts[i]]
    leaders = leaders_at(st, root, day, upto)
    return SectorFlowData(date=day, time=st.last_time,
                          version=version or f"{len(st.keys)}-{st.last_time}",
                          t=MINUTES[: upto + 1], summary=summary, sectors=sectors,
                          leaders=leaders)


def leaders_at(st: _Day, root: Path, day: str, m: int) -> list[FlowLeader]:
    """资金领先股 as of minute slot m (same rule as the replay video export): top-3 sectors by
    cumulative net at m; in each, top-3 by cumulative net / float cap among stocks with float
    cap >= 30亿 and cumulative turnover >= 1亿 at m."""
    m = max(0, min(m, st.max_slot))
    nsec = len(st.sector_names)
    cnet = st.cnm[:, : m + 1].sum(axis=1, dtype=np.float64)
    scum = np.bincount(st.sec_of, weights=cnet, minlength=nsec + 1)[:nsec]
    px = pd.DataFrame(st.px[:, : m + 1]).ffill(axis=1).to_numpy()
    amt = pd.DataFrame(st.am[:, : m + 1]).ffill(axis=1).to_numpy()[:, -1]
    amt = np.nan_to_num(amt)
    pre = np.where(st.pre > 0, st.pre, np.nan)
    pct = (px[:, -1] / pre - 1) * 100
    upx, _ = _limit_up(st.codes, st.names, st.pre)
    sealed = np.isfinite(px[:, -1]) & (px[:, -1] >= upx - 1e-6) & (st.pre > 0)
    circ = _circ(root, day)
    codes = np.array(st.codes)
    cm_all = np.array([circ.get(c, np.nan) for c in st.codes])

    def r(v: float) -> float | None:
        return None if not np.isfinite(v) else round(float(v), 2)

    out = []
    for i in [int(i) for i in np.argsort(-scum)][:3]:
        if scum[i] <= 0:
            break
        mem = np.nonzero(st.sec_of == i)[0]
        cm = cm_all[mem]
        good = (cm >= MIN_CIRC) & (amt[mem] >= MIN_AMT)
        rt = cnet[mem] / cm
        pick = mem[good][np.argsort(-rt[good])][:3]
        stocks = [FlowStock(
            code=str(codes[j]), name=(st.names[j] or "").strip() or None, pct=r(pct[j]),
            net=float(cnet[j]), net_ratio=float(cnet[j] / cm_all[j]), amount=float(amt[j]),
            sealed=bool(sealed[j]),
            spark=[None if not np.isfinite(v) else round(float(v), 3) for v in px[j]],
            pre_close=float(st.pre[j]) if st.pre[j] > 0 else None) for j in pick]
        sp = pct[mem]
        sp = sp[np.isfinite(sp)]
        out.append(FlowLeader(name=st.sector_names[i], net=float(scum[i]),
                              pct=r(float(sp.mean())) if len(sp) else None,
                              limit_up=int(sealed[mem].sum()), stocks=stocks))
    return out


def load_leaders_at(root: Path, day: str, m: int) -> list[FlowLeader]:
    st = _DAYS.get(day)
    if st is None or st.max_slot < 0:
        return []
    with st.lock:
        return leaders_at(st, root, day, m)


DELTA_RE = re.compile(r"^d-([0-9A-Za-z]+)-(\d{1,12})\.parquet$")
DELTA_MAX_AGE = 300.0  # s: never show provisional rounds older than this (collector stalled)


def _deltas(root: Path, day: str) -> list[tuple[str, int, Path]]:
    """Delta files present now: (run, seq, path), oldest first."""
    sd = root / "snapshots" / day / "stream"
    if not sd.is_dir():
        return []
    out = []
    cutoff = time.time() - DELTA_MAX_AGE
    try:
        entries = list(os.scandir(sd))
    except OSError:
        return []
    for e in entries:
        m = DELTA_RE.match(e.name)
        if not m:
            continue
        try:
            if e.stat().st_mtime < cutoff:
                continue
        except OSError:
            continue
        out.append((m.group(1), int(m.group(2)), Path(e.path)))
    out.sort(key=lambda r: (r[0], r[1]))
    return out


def _pending(st: _Day, found: list[tuple[str, int, Path]]) -> list[tuple[str, int, Path]]:
    """Reconciliation: drop rounds already inside a folded shard (seq <= landed[run])."""
    return [r for r in found if r[1] > st.landed.get(r[0], 0)]


def _read_deltas(pend: list[tuple[str, int, Path]]) -> pd.DataFrame | None:
    """None when a delta vanished mid-read (its shard just landed -> refold first)."""
    frames = []
    for _, _, p in pend:
        try:
            frames.append(pq.read_table(p, columns=_COLS).to_pandas())
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            continue  # unreadable round: the shard will carry it
    frames = [f for f in frames if len(f)]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=_COLS)


def _refresh(root: Path, day: str) -> SectorFlowData | None:
    found = _deltas(root, day)  # listed before folding: a shard landing in between is caught
    for _ in range(2):
        st = _update(root, day)
        if st is None:
            return None
        pend = _pending(st, found)
        extra = (pend[-1][0], pend[-1][1], len(pend)) if pend else None
        # the Tushare daily total lands in the evening: a new sig/version swaps the turnover card
        # from the snapshot total to it without a restart (lookup is cached, see TURN_RETRY)
        td = _db_turnover(root, day) is not None
        sig = (len(st.keys), st.last_time, extra, td)
        hit = _RESULT.get(day)
        if hit and hit[0] == sig:
            return hit[1]
        df = _read_deltas(pend) if pend else None
        if pend and df is None:  # a delta vanished: its shard landed after _update -> refold
            found = _deltas(root, day)
            continue
        break
    else:
        pend, extra, df = [], None, None
        td = _db_turnover(root, day) is not None
        sig = (len(st.keys), st.last_time, None, td)
    tv = "-td" if td else ""
    version = f"{len(st.keys)}-{st.last_time}{tv}"
    if pend and df is not None:
        with st.lock:
            view = st.clone()
        view.fold(df)
        if view.max_slot < 0:
            return None
        version = f"{len(st.keys)}+{extra[0]}.{extra[1]}-{view.last_time}{tv}"
        out = _compute(view, root, day, version)
    else:
        if st.max_slot < 0:
            return None
        with st.lock:
            out = _compute(st, root, day, version)
    with _GLOCK:
        if len(_RESULT) > 4:
            _RESULT.clear()
        _RESULT[day] = (sig, out)
    return out


_BUSY: set[str] = set()
_WARMER: dict[str, threading.Thread] = {}


def _bg(root: Path, day: str) -> None:
    with _GLOCK:
        if day in _BUSY:
            return
        _BUSY.add(day)

    def run() -> None:
        try:
            _refresh(root, day)
        finally:
            with _GLOCK:
                _BUSY.discard(day)

    threading.Thread(target=run, name=f"sector-flow-{day}", daemon=True).start()


def _in_session() -> bool:
    now = pd.Timestamp.now(tz="Asia/Shanghai")
    m = now.hour * 60 + now.minute
    return now.weekday() < 5 and 9 * 60 + 24 <= m <= 15 * 60 + 2


def start_warmer(root: Path, latest: Callable[[], str | None]) -> None:
    """Keep today's state (shards + pending 3s deltas) folded every 1.5 s in session so requests
    never block on I/O and each ~3s collector round shows up within one poll."""
    if "w" in _WARMER and _WARMER["w"].is_alive():
        return

    def loop() -> None:
        while True:
            try:
                d = latest()
                if d and (_in_session() or d not in _RESULT):
                    _refresh(root, d)
            except Exception:  # noqa: BLE001 - never let the warmer die
                pass
            time.sleep(1.5 if _in_session() else 60)

    _WARMER["w"] = threading.Thread(target=loop, name="sector-flow-warmer", daemon=True)
    _WARMER["w"].start()


def load_sector_flow(root: Path, day: str, days: list[str], known: str | None = None,
                     wait: bool = False) -> SectorFlowData:
    """Never blocks on folding: serves the last computed result and refreshes in background."""
    hit = _RESULT.get(day)
    if wait and hit is None:
        _refresh(root, day)
        hit = _RESULT.get(day)
    else:
        _bg(root, day)
    if hit is None:
        return SectorFlowData(date=day, time=None, warming=True, days=days)
    out = hit[1]
    if known and known == out.version:
        return SectorFlowData(date=day, time=out.time, version=out.version, unchanged=True,
                              days=days)
    return out.model_copy(update={"days": days})
