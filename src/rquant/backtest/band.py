"""Bootstrap band of cumulative return from a backtest's daily returns (item 14).

Resample daily returns with replacement (i.i.d. — ignores autocorrelation, stated
on the page) to get the 5/50/95% path of cumulative return over N days, so a
paper account's return after N days can be placed inside or outside it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pydantic import BaseModel


class BandPoint(BaseModel):
    day: int
    p5: float
    p50: float
    p95: float


def bootstrap_band(returns: pd.Series, days: int, paths: int = 2000,
                   seed: int = 0) -> list[BandPoint]:
    values = returns.dropna().to_numpy()
    if len(values) < 5 or days < 1:
        return []
    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(paths, days), replace=True)
    cumulative = np.cumprod(1 + draws, axis=1) - 1
    p5, p50, p95 = np.percentile(cumulative, [5, 50, 95], axis=0)
    return [BandPoint(day=i + 1, p5=float(p5[i]), p50=float(p50[i]), p95=float(p95[i]))
            for i in range(days)]
