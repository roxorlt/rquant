"""Industry exposure of a stored portfolio run vs. the pool it picked from.

Benchmark = equal weight over the last signal day's candidates (the pool), which
we always have; index-constituent benchmarks need index member data (not yet
collected), so no Brinson here.
"""

from __future__ import annotations

from collections import defaultdict

from pydantic import BaseModel

from rquant.backtest.store import StoredRun


class IndustryExposure(BaseModel):
    industry: str
    weight: float          # last day, share of NAV
    avg_weight: float      # mean over invested days
    pool_weight: float     # equal weight across the last candidate pool
    deviation: float       # weight - pool_weight


def industry_exposure(run: StoredRun) -> list[IndustryExposure]:
    def industry(code: str) -> str:
        return run.industries.get(code, "未分类")

    totals: dict[str, float] = defaultdict(float)
    invested = [d for d in run.result.days if d.values]
    for day in invested:
        equity = day.cash + day.market_value
        for code, value in day.values.items():
            totals[industry(code)] += value / equity / len(invested)
    last: dict[str, float] = defaultdict(float)
    if run.result.days:
        day = run.result.days[-1]
        equity = day.cash + day.market_value
        for code, value in day.values.items():
            last[industry(code)] += value / equity
    pool: dict[str, float] = defaultdict(float)
    for code in run.pool_last:
        pool[industry(code)] += 1 / len(run.pool_last)
    names = sorted(set(totals) | set(last) | set(pool),
                   key=lambda n: (-last.get(n, 0.0), -pool.get(n, 0.0), n))
    return [
        IndustryExposure(industry=n, weight=round(last.get(n, 0.0), 6),
                         avg_weight=round(totals.get(n, 0.0), 6),
                         pool_weight=round(pool.get(n, 0.0), 6),
                         deviation=round(last.get(n, 0.0) - pool.get(n, 0.0), 6))
        for n in names
    ]
