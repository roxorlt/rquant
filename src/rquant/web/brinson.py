"""Industry Brinson attribution (Brinson-Fachler) of a stored portfolio backtest.

Benchmark: all A-shares in the research DB, weighted by previous-day total market cap,
grouped by ``stock_basic.industry``. Portfolio: previous-close weights from the run's
daily records (cash is its own segment with zero return); stock returns are the day's
``daily_bar.pct_chg``. Daily effects are summed arithmetically, so they add up to the
arithmetic excess return, not the compounded one.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date

import duckdb
from pydantic import BaseModel

CASH = "现金"


class BrinsonRow(BaseModel):
    industry: str
    weight_portfolio: float  # average over days
    weight_benchmark: float
    allocation: float
    selection: float
    interaction: float


class BrinsonData(BaseModel):
    benchmark: str
    days: int
    portfolio_return: float  # arithmetic sum of daily returns
    benchmark_return: float
    allocation: float
    selection: float
    interaction: float
    rows: list[BrinsonRow]


def _bench(con: duckdb.DuckDBPyConnection, start: date, end: date):
    rows = con.execute(
        """
        WITH v AS (
          SELECT ts_code, trade_date,
                 lag(total_mv) OVER (PARTITION BY ts_code ORDER BY trade_date) AS prev_mv
          FROM daily_basic WHERE trade_date BETWEEN ? - INTERVAL 10 DAY AND ?)
        SELECT b.trade_date, COALESCE(NULLIF(s.industry, ''), '未分类') AS industry,
               sum(v.prev_mv * b.pct_chg / 100) / sum(v.prev_mv) AS ret, sum(v.prev_mv) AS mv
        FROM daily_bar b JOIN stock_basic s USING (ts_code)
        JOIN v USING (ts_code, trade_date)
        WHERE b.trade_date BETWEEN ? AND ? AND v.prev_mv > 0 AND b.pct_chg IS NOT NULL
        GROUP BY 1, 2
        """, [start, end, start, end]).fetchall()
    out: dict[date, dict[str, tuple[float, float]]] = defaultdict(dict)
    for d, ind, ret, mv in rows:
        out[d][ind] = (float(ret), float(mv))
    return out


def brinson(con: duckdb.DuckDBPyConnection, days: list, fallback_industry: dict[str, str]
            ) -> BrinsonData | None:
    """``days``: the run's DayRecord list (trade_date, cash, positions, values)."""
    if len(days) < 2:
        return None
    start, end = days[1].trade_date, days[-1].trade_date
    bench = _bench(con, start, end)
    codes = sorted({c for d in days for c in d.values})
    if not codes or not bench:
        return None
    marks = ",".join("?" * len(codes))
    pct = {(c, d): float(p) / 100 for c, d, p in con.execute(
        f"SELECT ts_code, trade_date, pct_chg FROM daily_bar WHERE ts_code IN ({marks}) "
        "AND trade_date BETWEEN ? AND ? AND pct_chg IS NOT NULL", [*codes, start, end]).fetchall()}
    industry = dict(fallback_industry)
    industry.update({c: i for c, i in con.execute(
        f"SELECT ts_code, COALESCE(NULLIF(industry, ''), '未分类') FROM stock_basic "
        f"WHERE ts_code IN ({marks})", codes).fetchall()})
    acc: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0.0])  # wp wb a s i
    n = 0
    rp_sum = rb_sum = 0.0
    for prev, cur in zip(days, days[1:], strict=False):
        b = bench.get(cur.trade_date)
        equity = prev.cash + sum(prev.values.values())
        if not b or equity <= 0:
            continue
        mv_total = sum(mv for _, mv in b.values())
        wb = {i: mv / mv_total for i, (_, mv) in b.items()}
        rb = {i: r for i, (r, _) in b.items()}
        rb_total = sum(wb[i] * rb[i] for i in wb)
        wp: dict[str, float] = defaultdict(float)
        contrib: dict[str, float] = defaultdict(float)
        for code, value in prev.values.items():
            ind = industry.get(code, "未分类")
            wp[ind] += value / equity
            contrib[ind] += value / equity * pct.get((code, cur.trade_date), 0.0)
        wp[CASH] += prev.cash / equity
        n += 1
        rb_sum += rb_total
        rp_sum += sum(contrib.values())
        for ind in set(wp) | set(wb):
            p, q = wp.get(ind, 0.0), wb.get(ind, 0.0)
            r_b = rb.get(ind, 0.0)
            r_p = contrib[ind] / p if p > 0 else r_b
            a = acc[ind]
            a[0] += p
            a[1] += q
            a[2] += (p - q) * (r_b - rb_total)
            a[3] += q * (r_p - r_b)
            a[4] += (p - q) * (r_p - r_b)
    if n == 0:
        return None
    rows = [BrinsonRow(industry=k, weight_portfolio=v[0] / n, weight_benchmark=v[1] / n,
                       allocation=v[2], selection=v[3], interaction=v[4])
            for k, v in acc.items()]
    rows.sort(key=lambda r: -abs(r.allocation + r.selection + r.interaction))
    return BrinsonData(
        benchmark="全A（前日总市值加权）", days=n, portfolio_return=rp_sum,
        benchmark_return=rb_sum, allocation=sum(r.allocation for r in rows),
        selection=sum(r.selection for r in rows), interaction=sum(r.interaction for r in rows),
        rows=rows)
