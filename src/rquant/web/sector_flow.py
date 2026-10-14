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
        self.sec_of = np.zeros(0, dtype=np.int32)
        self.sector_names = sorted(set(sectors.values()))
        self.sec_ix = {s: i for i, s in enumerate(self.sector_names)}
        self.sectors = sectors
        self.sec_min = np.zeros((len(self.sector_names) + 1, len(MINUTES)))  # last row = 未分类
        self.sec_amt = np.zeros_like(self.sec_min)  # turnover per sector-minute
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
            self.amt = np.concatenate([self.amt, np.zeros(k)])
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
        np.add.at(self.sec_amt, (self.sec_of[ix_all], slots), np.where(newm, d, 0.0))
        # last price per code-minute (rows sorted by time within code)
        ixn, sn = ix_all[newm], slots[newm]
        self.px[ixn, sn] = new["price"].to_numpy(np.float32)
        self.pre[ixn] = new["pre_close"].to_numpy()
        self.amt[ixn] = new["amount"].to_numpy()
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
    amount: float
    net: float
    up: int
    down: int
    limit_up: int
    limit_down: int


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


MIN_CIRC = 30e8  # leader floor: float cap >= 30亿
MIN_AMT = 1e8  # and turnover >= 1亿


def _limit_up(codes: list[str], names: list, pre: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    c = pd.Series(codes)
    lp = np.where(c.str.startswith(("30", "68")), 0.2, 0.1)
    lp = np.where(c.str.endswith(".BJ"), 0.3, lp)
    lp = np.where(pd.Series(names).fillna("").str.contains("ST"), 0.05, lp)
    return np.round(pre * (1 + lp) + 1e-9, 2), np.round(pre * (1 - lp) + 1e-9, 2)


def _compute(st: _Day, root: Path, day: str) -> SectorFlowData:
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
    summary = FlowSummary(amount=float(st.amt.sum()), net=float(cum[:nsec, -1].sum()),
                          up=int((nowp > 0).sum()), down=int((nowp < 0).sum()),
                          limit_up=int(sealed.sum()), limit_down=int(floored.sum()))

    def r(v: float) -> float | None:
        return None if not np.isfinite(v) else round(float(v), 2)

    order = [int(i) for i in np.argsort(-cum[:nsec, -1])]
    sectors = [FlowSeries(
        name=st.sector_names[i], n=int(counts[i]), net=float(cum[i, -1]),
        amount=float(camt[i, -1]), pct=r(smean[i, -1]), limit_up=int(lu_sec[i]),
        series=np.round(cum[i] / 1e8, 2).tolist(), ratio=np.round(ratio[i], 2).tolist(),
        pct_series=[r(v) for v in smean[i]]) for i in order if counts[i]]
    circ = _circ(root, day)
    codes = np.array(st.codes)
    leaders = []
    for i in order[:3]:
        if cum[i, -1] <= 0:
            break
        mem = np.nonzero(st.sec_of == i)[0]
        cm = np.array([circ.get(c, np.nan) for c in codes[mem]])
        rt = st.net[mem] / cm
        good = np.isfinite(rt) & (cm >= MIN_CIRC) & (st.amt[mem] >= MIN_AMT)
        pick = mem[good][np.argsort(-rt[good])][:3]
        stocks = [FlowStock(
            code=str(codes[j]), name=(st.names[j] or "").strip() or None, pct=r(nowp[j]),
            net=float(st.net[j]), net_ratio=float(st.net[j] / circ[codes[j]]),
            amount=float(st.amt[j]), sealed=bool(sealed[j]),
            spark=[None if not np.isfinite(v) else round(float(v), 3) for v in px[j]],
            pre_close=float(st.pre[j]) if st.pre[j] > 0 else None) for j in pick]
        leaders.append(FlowLeader(name=st.sector_names[i], net=float(cum[i, -1]),
                                  pct=r(smean[i, -1]), limit_up=int(lu_sec[i]), stocks=stocks))
    return SectorFlowData(date=day, time=st.last_time, version=f"{len(st.keys)}-{st.last_time}",
                          t=MINUTES[: upto + 1], summary=summary, sectors=sectors,
                          leaders=leaders)


def _refresh(root: Path, day: str) -> SectorFlowData | None:
    st = _update(root, day)
    if st is None or st.max_slot < 0:
        return None
    sig = (len(st.keys), st.last_time)
    hit = _RESULT.get(day)
    if hit and hit[0] == sig:
        return hit[1]
    with st.lock:
        out = _compute(st, root, day)
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
    """Keep today's state folded every 3 s in session so requests never block on I/O."""
    if "w" in _WARMER and _WARMER["w"].is_alive():
        return

    def loop() -> None:
        import time
        while True:
            try:
                d = latest()
                if d and (_in_session() or d not in _RESULT):
                    _refresh(root, d)
            except Exception:  # noqa: BLE001 - never let the warmer die
                pass
            time.sleep(3 if _in_session() else 60)

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
