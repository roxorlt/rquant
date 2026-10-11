"""Chip distribution (筹码分布) and volume profile approximated from bars, per stock on demand.

A-share data here has no tick / order book, so both are approximations:

* Chips (通达信 CYQ-style turnover decay): each day the existing chips shrink by the day's
  turnover t (fraction) and t of new chips are spread over [low, high] as a triangle peaked
  at the typical price (H+L+C)/3. Prices are forward-adjusted so ex-rights days don't tear
  the distribution. Turnover is ``daily_basic.turnover_rate`` (total shares, not free float),
  so decay is somewhat slower than vendors that use free-float turnover.
* Volume profile: each bar's volume spread over its [low, high] the same way; 5-minute bars
  when the research DB has them for the stock, daily bars otherwise. POC = densest bin,
  value area = smallest band around POC holding 70% of volume.
"""

from __future__ import annotations

from datetime import date

import duckdb
import numpy as np
from pydantic import BaseModel

BINS = 120


class ChipDay(BaseModel):
    date: date
    close: float
    dist: list[int]  # per bin, scaled so the day's max = 1000
    avg_cost: float
    profit_ratio: float  # share of chips below the close
    conc90: float  # (p95 - p5) / (p95 + p5)
    conc70: float  # (p85 - p15) / (p85 + p15)
    cost90: list[float]
    cost70: list[float]


class VolumeProfile(BaseModel):
    source: str  # "5min" | "daily"
    days: int
    dist: list[int]  # scaled max = 1000
    poc: float
    val: float
    vah: float


class ChipsData(BaseModel):
    lo: float
    hi: float
    bins: int
    days: list[ChipDay]
    profile: VolumeProfile | None


def _tri(lo: float, hi: float, peak: float, edges: np.ndarray) -> np.ndarray:
    """Mass of a triangular density on [lo, hi] (mode ``peak``) falling in each bin."""
    if hi <= lo:
        out = np.zeros(len(edges) - 1)
        k = int(np.clip(np.searchsorted(edges, lo, side="right") - 1, 0, len(out) - 1))
        out[k] = 1.0
        return out
    peak = min(max(peak, lo), hi)

    def cdf(x: np.ndarray) -> np.ndarray:
        x = np.clip(x, lo, hi)
        left = (x - lo) ** 2 / ((hi - lo) * (peak - lo)) if peak > lo else np.zeros_like(x)
        right = (1 - (hi - x) ** 2 / ((hi - lo) * (hi - peak))) if hi > peak else np.ones_like(x)
        return np.where(x <= peak, left if peak > lo else 0.0, right)

    return np.diff(cdf(edges))


def _quantile(dist: np.ndarray, mids: np.ndarray, q: float) -> float:
    c = np.cumsum(dist)
    return float(mids[min(int(np.searchsorted(c, q * c[-1])), len(mids) - 1)])


def chip_series(bars: list[dict], show: int, edges: np.ndarray) -> list[ChipDay]:
    """``bars``: oldest first with high/low/close/turnover (%); the last ``show`` are returned."""
    mids = (edges[:-1] + edges[1:]) / 2
    dist = np.zeros(len(mids))
    out: list[ChipDay] = []
    for i, b in enumerate(bars):
        new = _tri(b["low"], b["high"], (b["high"] + b["low"] + b["close"]) / 3, edges)
        t = 1.0 if i == 0 else min(max((b.get("turnover") or 0) / 100, 0.0), 1.0)
        dist = dist * (1 - t) + new * t
        if i < len(bars) - show or dist.sum() <= 0:
            continue
        total = dist.sum()
        p = [_quantile(dist, mids, q) for q in (0.05, 0.15, 0.85, 0.95)]
        out.append(ChipDay(
            date=b["date"], close=b["close"],
            dist=np.rint(dist / dist.max() * 1000).astype(int).tolist(),
            avg_cost=round(float((dist * mids).sum() / total), 3),
            profit_ratio=round(float(dist[mids <= b["close"]].sum() / total), 4),
            conc90=round((p[3] - p[0]) / (p[3] + p[0]), 4) if p[3] + p[0] else 0.0,
            conc70=round((p[2] - p[1]) / (p[2] + p[1]), 4) if p[2] + p[1] else 0.0,
            cost90=[round(p[0], 3), round(p[3], 3)], cost70=[round(p[1], 3), round(p[2], 3)]))
    return out


def volume_profile(bars: list[tuple[float, float, float, float]], edges: np.ndarray,
                   source: str, days: int) -> VolumeProfile | None:
    """``bars``: (high, low, close, volume)."""
    mids = (edges[:-1] + edges[1:]) / 2
    vp = np.zeros(len(mids))
    for h, lo, c, v in bars:
        if v and v > 0:
            vp += _tri(lo, h, (h + lo + c) / 3, edges) * v
    if vp.sum() <= 0:
        return None
    poc = int(vp.argmax())
    a = b = poc
    need = 0.7 * vp.sum()
    got = vp[poc]
    while got < need and (a > 0 or b < len(vp) - 1):
        down = vp[a - 1] if a > 0 else -1
        up = vp[b + 1] if b < len(vp) - 1 else -1
        if up >= down:
            b += 1
            got += vp[b]
        else:
            a -= 1
            got += vp[a]
    return VolumeProfile(source=source, days=days,
                         dist=np.rint(vp / vp.max() * 1000).astype(int).tolist(),
                         poc=round(float(mids[poc]), 3), val=round(float(edges[a]), 3),
                         vah=round(float(edges[b + 1]), 3))


def load_chips(con: duckdb.DuckDBPyConnection, code: str, show: int = 250, warmup: int = 250,
               profile_days: int = 20) -> ChipsData | None:
    """Forward-adjusted daily bars (+turnover) → chips for the last ``show`` days and a volume
    profile of the last ``profile_days`` days on the same price grid."""
    try:
        adj = dict(con.execute("SELECT trade_date, adj_factor FROM adj_factor WHERE ts_code = ?",
                               [code]).fetchall())
    except duckdb.Error:
        adj = {}
    rows = con.execute(
        "SELECT b.trade_date, b.high, b.low, b.close, b.vol, d.turnover_rate FROM daily_bar b "
        "LEFT JOIN daily_basic d USING (ts_code, trade_date) WHERE b.ts_code = ? "
        "ORDER BY b.trade_date DESC LIMIT ?", [code, show + warmup]).fetchall()[::-1]
    if not rows:
        return None
    last_adj = adj.get(rows[-1][0]) or 1.0
    k = {r[0]: (adj.get(r[0]) or last_adj) / last_adj for r in rows}
    bars = [{"date": d, "high": h * k[d], "low": lo * k[d], "close": c * k[d], "vol": v,
             "turnover": t} for d, h, lo, c, v, t in rows if None not in (h, lo, c)]
    shown = bars[-show:]
    lo, hi = min(b["low"] for b in shown), max(b["high"] for b in shown)
    pad = (hi - lo) * 0.02 or hi * 0.01 or 1.0
    edges = np.linspace(lo - pad, hi + pad, BINS + 1)
    clipped = [{**b, "high": min(max(b["high"], edges[0]), edges[-1]),
                "low": min(max(b["low"], edges[0]), edges[-1])} for b in bars]
    days = chip_series(clipped, show, edges)

    recent = {b["date"] for b in shown[-profile_days:]}
    profile = None
    try:
        mins = con.execute(
            "SELECT trade_date, high, low, close, vol FROM minute_bar WHERE ts_code = ? "
            "AND freq = '5min' AND trade_date >= ?", [code, min(recent)]).fetchall()
    except duckdb.Error:
        mins = []
    if mins:
        profile = volume_profile([(h * k.get(d, 1), lo_ * k.get(d, 1), c * k.get(d, 1), v)
                                  for d, h, lo_, c, v in mins], edges, "5min",
                                 len({m[0] for m in mins}))
    if profile is None:
        profile = volume_profile([(b["high"], b["low"], b["close"], b["vol"])
                                  for b in clipped if b["date"] in recent], edges, "daily",
                                 len(recent))
    return ChipsData(lo=float(edges[0]), hi=float(edges[-1]), bins=BINS, days=days,
                     profile=profile)
