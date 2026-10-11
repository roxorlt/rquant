"""Cross-sectional chip-distribution (筹码) fields for factor research.

Vectorised version of :mod:`rquant.web.chips` over the whole panel: per stock a log-price grid
spanning its whole range, each day the distribution decays by the day's free-float turnover
and the new chips are a triangle over [low, high] peaked at (H+L+C)/3 (on log price).

Fields (one value per stock per day):

* ``chip_profit``   profit ratio, share of chips at or below the close (获利比例)
* ``chip_cost_dev`` close / average cost − 1 (成本乖离)
* ``chip_conc90``   90% chip concentration, (p95 − p5) / (p95 + p5) (lower = more concentrated)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

CHIP_FIELDS = ("chip_profit", "chip_cost_dev", "chip_conc90")
BINS = 160


def _tri_cdf(x: np.ndarray, a: np.ndarray, m: np.ndarray, b: np.ndarray) -> np.ndarray:
    a, m, b = a[:, None], m[:, None], b[:, None]
    w = np.maximum(b - a, 1e-12)
    left = (x - a) ** 2 / (w * np.maximum(m - a, 1e-12))
    right = 1 - (b - x) ** 2 / (w * np.maximum(b - m, 1e-12))
    out = np.where(x <= m, left, right)
    out = np.where(x <= a, 0.0, np.where(x >= b, 1.0, out))
    return out


def chip_fields(panel: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    high, low, close = panel["high"], panel["low"], panel["close"]
    turn = panel.get("turnover")
    if turn is None:
        return {}
    turn = turn.reindex_like(close)
    with np.errstate(divide="ignore", invalid="ignore"):
        lh, ll, lc = np.log(high.to_numpy()), np.log(low.to_numpy()), np.log(close.to_numpy())
    lo = np.nanmin(ll, axis=0)
    hi = np.nanmax(lh, axis=0)
    ok_cols = np.isfinite(lo) & np.isfinite(hi)
    lo, hi = np.where(ok_cols, lo, 0.0), np.where(ok_cols, hi, 1.0)
    hi = np.maximum(hi, lo + 1e-6)
    k = np.linspace(0.0, 1.0, BINS + 1)
    edges = lo[:, None] + (hi - lo)[:, None] * k[None, :]          # N × (B+1)
    centers = (edges[:, 1:] + edges[:, :-1]) / 2                    # N × B (log)
    n_days, n = lc.shape
    dist = np.zeros((n, BINS))
    started = np.zeros(n, dtype=bool)
    out = {f: np.full((n_days, n), np.nan) for f in CHIP_FIELDS}
    tv = turn.to_numpy() / 100.0
    for t in range(n_days):
        h, lw, c, r = lh[t], ll[t], lc[t], tv[t]
        valid = np.isfinite(h) & np.isfinite(lw) & np.isfinite(c) & ok_cols
        if not valid.any():
            continue
        rate = np.clip(np.where(np.isfinite(r), r, 0.0), 0.0, 1.0)
        peak = np.log((np.exp(h) + np.exp(lw) + np.exp(c)) / 3)
        a = np.where(valid, lw, 0.0)
        b = np.where(valid, np.maximum(h, lw + 1e-9), 1.0)
        m = np.clip(np.where(valid, peak, 0.5), a, b)
        new = np.diff(_tri_cdf(edges, a, m, b), axis=1)
        first = valid & ~started
        step = valid & started
        dist[first] = new[first]
        dist[step] = dist[step] * (1 - rate[step, None]) + new[step] * rate[step, None]
        started |= valid
        tot = dist.sum(axis=1)
        live = valid & (tot > 0)
        if not live.any():
            continue
        d, cen, cl = dist[live], centers[live], c[live]
        s = tot[live]
        out["chip_profit"][t, live] = (d * (cen <= cl[:, None])).sum(axis=1) / s
        avg = (d * np.exp(cen)).sum(axis=1) / s
        out["chip_cost_dev"][t, live] = np.exp(cl) / avg - 1
        cum = np.cumsum(d, axis=1) / s[:, None]
        p5 = np.exp(np.take_along_axis(cen, (cum < 0.05).sum(axis=1)[:, None], 1)[:, 0])
        p95 = np.exp(np.take_along_axis(
            cen, np.minimum((cum < 0.95).sum(axis=1), BINS - 1)[:, None], 1)[:, 0])
        out["chip_conc90"][t, live] = (p95 - p5) / (p95 + p5)
    return {f: pd.DataFrame(v, index=close.index, columns=close.columns) for f, v in out.items()}
