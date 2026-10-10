"""Single-factor test: rank IC, quantile returns, IC decay.

Signal at the close of t, forward return close(t) → close(t+h). Close-to-close
and no costs: a screening statistic, not a tradable backtest (module 4 is that).
"""

from __future__ import annotations

import math
from datetime import date

import pandas as pd
from pydantic import BaseModel

HORIZONS = (1, 2, 3, 5, 10, 20)


class IcPoint(BaseModel):
    date: date
    ic: float


class QuantileReturn(BaseModel):
    quantile: int
    mean_return: float


class DecayPoint(BaseModel):
    horizon: int
    mean_ic: float | None


class FactorTestResult(BaseModel):
    horizon: int
    days: int
    mean_ic: float | None
    ic_ir: float | None
    t_stat: float | None
    positive_ratio: float | None
    long_short: float | None
    coverage: float
    ic_series: list[IcPoint]
    quantiles: list[QuantileReturn]
    decay: list[DecayPoint]


def forward_returns(close: pd.DataFrame, horizon: int) -> pd.DataFrame:
    return close.shift(-horizon) / close - 1


def daily_ic(factor: pd.DataFrame, fwd: pd.DataFrame, min_names: int = 10) -> pd.Series:
    f = factor.rank(axis=1)
    r = fwd.rank(axis=1)
    both = f.notna() & r.notna()
    f, r = f.where(both), r.where(both)
    count = both.sum(axis=1)
    ic = f.corrwith(r, axis=1, method="pearson")  # Pearson on ranks = Spearman
    return ic[count >= min_names].dropna()


def factor_test(factor: pd.DataFrame, close: pd.DataFrame, horizon: int = 5,
                quantiles: int = 5) -> FactorTestResult:
    fwd = forward_returns(close, horizon)
    ic = daily_ic(factor, fwd)
    n = len(ic)
    std = float(ic.std(ddof=1)) if n > 1 else 0.0
    mean = float(ic.mean()) if n else None
    groups: dict[int, list[float]] = {q: [] for q in range(1, quantiles + 1)}
    for day in ic.index:
        f, r = factor.loc[day], fwd.loc[day]
        ok = f.notna() & r.notna()
        if ok.sum() < quantiles * 2:
            continue
        labels = pd.qcut(f[ok].rank(method="first"), quantiles, labels=False) + 1
        for q, value in r[ok].groupby(labels).mean().items():
            groups[int(q)].append(float(value))
    quantile_rows = [QuantileReturn(quantile=q, mean_return=sum(v) / len(v))
                     for q, v in groups.items() if v]
    long_short = (quantile_rows[-1].mean_return - quantile_rows[0].mean_return
                  if len(quantile_rows) == quantiles else None)
    decay = []
    for h in HORIZONS:
        series = ic if h == horizon else daily_ic(factor, forward_returns(close, h))
        decay.append(DecayPoint(horizon=h, mean_ic=float(series.mean()) if len(series) else None))
    total = factor.size
    return FactorTestResult(
        horizon=horizon, days=n, mean_ic=mean,
        ic_ir=mean / std if mean is not None and std > 0 else None,
        t_stat=mean / std * math.sqrt(n) if mean is not None and std > 0 else None,
        positive_ratio=float((ic > 0).mean()) if n else None,
        long_short=long_short,
        coverage=float(factor.notna().sum().sum() / total) if total else 0.0,
        ic_series=[IcPoint(date=d.date() if hasattr(d, "date") else d, ic=float(v))
                   for d, v in ic.items()],
        quantiles=quantile_rows, decay=decay,
    )
