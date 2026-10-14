"""板块资金流 (estimated active net buy per industry) from the research 3-second snapshots.

Per stock, each snapshot-to-snapshot turnover increment Δamount is signed by a tick rule:
buy if price >= previous ask1, sell if price <= previous bid1, otherwise by up/down tick
(unchanged -> 0, i.e. split evenly). The first traded snapshot of the day (normally the 09:25
auction print) is signed by auction price vs pre_close. Sectors = stock_basic.industry (same
as 市场全景). This is a Lee-Ready style *estimate*, not exchange-reported order flow.

Incremental: one state per day; each new parquet chunk is folded in once (carry = last row per
code), keeping only dense per-sector-minute sums, per-code cumulative net and a float32
per-code-minute price matrix (~6 MB). Nothing re-reads the whole day per request.
"""

from __future__ import annotations

import threading
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
        self.px = np.full((0, len(MINUTES)), np.nan, dtype=np.float32)
        self.sec_of = np.zeros(0, dtype=np.int32)
        self.sector_names = sorted(set(sectors.values()))
        self.sec_ix = {s: i for i, s in enumerate(self.sector_names)}
        self.sectors = sectors
        self.sec_min = np.zeros((len(self.sector_names) + 1, len(MINUTES)))  # last row = 未分类
        self.carry: pd.DataFrame | None = None
        self.last_time: str | None = None
        self.max_slot = -1

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
            self.px = np.vstack([self.px, np.full((k, len(MINUTES)), np.nan, np.float32)])
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
            df = pd.concat([self.carry.assign(_new=False), df], ignore_index=True)
        df = df.sort_values(["ts_code", "time"], kind="stable").reset_index(drop=True)
        same = df["ts_code"].eq(df["ts_code"].shift())
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
        np.add.at(self.sec_min, (self.sec_of[ix_all], slots), flow)
        # last price per code-minute (rows sorted by time within code)
        ixn, sn = ix_all[newm], slots[newm]
        self.px[ixn, sn] = new["price"].to_numpy(np.float32)
        self.pre[ixn] = new["pre_close"].to_numpy()
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
    series: list[float]  # 亿 CNY cumulative per minute (aligned to t)


class FlowStock(BaseModel):
    code: str
    name: str | None
    pct: float | None
    net: float  # CNY
    net_ratio: float | None  # net / float market cap
    spark: list[float | None]  # price per minute (aligned to t), forward-filled


class FlowLeader(BaseModel):
    name: str
    net: float
    stocks: list[FlowStock]


class SectorFlowData(BaseModel):
    date: str | None
    time: str | None
    t: list[str]
    sectors: list[FlowSeries]  # sorted by net desc
    leaders: list[FlowLeader]  # top 3 inflow sectors x top 3 stocks by net / float mv
    days: list[str]


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
        files = _files(root / "snapshots" / day)
        for f in files:
            try:
                mt = f.stat().st_mtime
                n = pq.ParquetFile(f).num_row_groups
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
        finally:
            if release:
                st.lock.release()
    return st


def _leaders(st: _Day, root: Path, day: str, order: list[int], upto: int) -> list[FlowLeader]:
    circ = _circ(root, day)
    out = []
    codes = np.array(st.codes)
    for si in order[:3]:
        if st.sec_min[si, : upto + 1].sum() <= 0:
            break
        mem = np.nonzero(st.sec_of == si)[0]
        cm = np.array([circ.get(c, np.nan) for c in codes[mem]])
        ratio = st.net[mem] / cm
        ok = np.isfinite(ratio)
        pick = mem[ok][np.argsort(-ratio[ok])][:3]
        stocks = []
        for i in pick:
            sp = pd.Series(st.px[i, : upto + 1]).ffill()
            last = sp.dropna()
            pct = (float(last.iloc[-1]) / st.pre[i] - 1) * 100 if len(last) and st.pre[i] else None
            stocks.append(FlowStock(
                code=str(codes[i]), name=st.names[i],
                pct=round(pct, 2) if pct is not None else None, net=float(st.net[i]),
                net_ratio=float(st.net[i] / circ[codes[i]]),
                spark=[None if np.isnan(v) else round(float(v), 3) for v in sp]))
        out.append(FlowLeader(name=st.sector_names[si], net=float(st.sec_min[si].sum()),
                              stocks=stocks))
    return out


def load_sector_flow(root: Path, day: str, days: list[str]) -> SectorFlowData:
    st = _update(root, day)
    if st is None or st.max_slot < 0:
        return SectorFlowData(date=day, time=None, t=[], sectors=[], leaders=[], days=days)
    sig = (len(st.keys), st.last_time)
    hit = _RESULT.get(day)
    if hit and hit[0] == sig:
        return hit[1].model_copy(update={"days": days})
    with st.lock:
        upto = st.max_slot
        cum = np.cumsum(st.sec_min[:, : upto + 1], axis=1)
        nsec = len(st.sector_names)
        counts = np.bincount(st.sec_of, minlength=nsec + 1)
        order = [int(i) for i in np.argsort(-cum[:nsec, -1])]
        sectors = [FlowSeries(name=st.sector_names[i], n=int(counts[i]), net=float(cum[i, -1]),
                              series=np.round(cum[i] / 1e8, 3).tolist())
                   for i in order if counts[i]]
        out = SectorFlowData(date=day, time=st.last_time, t=MINUTES[: upto + 1],
                             sectors=sectors, leaders=_leaders(st, root, day, order, upto),
                             days=days)
    if len(_RESULT) > 4:
        _RESULT.clear()
    _RESULT[day] = (sig, out)
    return out
