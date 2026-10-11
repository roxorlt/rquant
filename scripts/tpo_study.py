"""Which TPO period (15/30/60 min) gives the most informative prior-day levels for A-shares?

For every stock with 5-minute bars and every pair of consecutive days, the prior day's
POC / VAH / VAL (70% value area, same 28-row grid as the stock page) are tested on the next day:

* level reaction: touch rate and reject rate (touched, but closed back on the open side) at
  VAH, VAL, POC, against a random level drawn uniformly in the prior day's range;
* open inside value: rate of trading through the POC, against the mirrored level at the same
  distance on the other side of the open;
* open outside value: breakout continuation = close further from value than the open.

Usage: python scripts/tpo_study.py <market.duckdb> [max_stocks]
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict

import duckdb
import numpy as np

from rquant.web.chips import TPO_PERIODS, tpo_days


def study(db: str, limit: int = 2000) -> dict:
    con = duckdb.connect(db, read_only=True)
    codes = [r[0] for r in con.execute(
        "SELECT ts_code FROM minute_bar WHERE freq = '5min' GROUP BY 1 ORDER BY count(*) DESC "
        "LIMIT ?", [limit]).fetchall()]
    rng = np.random.default_rng(7)
    acc: dict[int, dict[str, list[float]]] = {p: defaultdict(list) for p in TPO_PERIODS}
    for i, code in enumerate(codes):
        rows = con.execute(
            "SELECT trade_time, trade_date, high, low, open, close FROM minute_bar "
            "WHERE ts_code = ? AND freq = '5min' ORDER BY trade_time", [code]).fetchall()
        adj = dict(con.execute("SELECT trade_date, adj_factor FROM adj_factor WHERE ts_code = ?",
                               [code]).fetchall())
        by: dict = defaultdict(list)
        for r in rows:
            by[r[1]].append(r)
        days = sorted(d for d, b in by.items() if len(b) >= 40)
        for p in TPO_PERIODS:
            prof = {t.date: t for t in tpo_days(rows, {}, days=10**6, period=p)}
            for d0, d1 in zip(days, days[1:], strict=False):
                if adj.get(d0) != adj.get(d1) or d0 not in prof:
                    continue  # ex-rights between the days: levels are not comparable
                t, nxt = prof[d0], by[d1]
                o, c = nxt[0][4], nxt[-1][5]
                hi, lo = max(b[2] for b in nxt), min(b[3] for b in nxt)
                prev = by[d0]
                rand = rng.uniform(min(b[3] for b in prev), max(b[2] for b in prev))
                a = acc[p]
                for name, lvl in (("VAH", t.vah), ("VAL", t.val), ("POC", t.poc), ("RAND", rand)):
                    if o == lvl:
                        continue
                    touched = hi >= lvl if o < lvl else lo <= lvl
                    a[f"{name}_touch"].append(touched)
                    if touched:
                        a[f"{name}_reject"].append(c < lvl if o < lvl else c > lvl)
                if t.val <= o <= t.vah:
                    a["in_poc"].append(lo <= t.poc <= hi)
                    mirror = 2 * o - t.poc
                    a["in_mirror"].append(lo <= mirror <= hi)
                elif o > t.vah:
                    a["out_cont"].append(c > o)
                    a["out_accept"].append(c > t.vah)
                else:
                    a["out_cont"].append(c < o)
                    a["out_accept"].append(c < t.val)
        if i % 50 == 0:
            print(f"{i + 1}/{len(codes)} {code}", file=sys.stderr, flush=True)
    return {p: {k: [round(float(np.mean(v)), 4), len(v)] for k, v in a.items()}
            for p, a in acc.items()}


if __name__ == "__main__":
    print(json.dumps(study(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 2000),
                     ensure_ascii=False, indent=1))
