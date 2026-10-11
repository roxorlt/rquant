from __future__ import annotations

import numpy as np
import pandas as pd

from rquant.research.intraday import aggregate_1450
from rquant.research.studies import _apply, search_rules


def test_aggregate_1450_is_cumulative_through_the_1450_bar() -> None:
    rows = [("A", "2024-01-02 14:45:00", 10, 10, 9, 100, 1),
            ("A", "2024-01-02 14:50:00", 11, 12, 10, 50, 1),
            ("A", "2024-01-02 15:00:00", 12, 13, 12, 30, 1),
            ("A", "2024-01-03 10:00:00", 5, 5, 5, 1, 1)]  # no 14:50 bar: dropped
    df = pd.DataFrame(rows, columns=["ts_code", "trade_time", "close", "high", "low", "vol",
                                     "amount"])
    (r,) = aggregate_1450(df).to_dict("records")
    assert (r["p1450"], r["h1450"], r["l1450"], r["v1450"], r["vday"]) == (11, 12, 9, 150, 180)


def test_rule_search_reports_out_of_sample() -> None:
    rng = np.random.default_rng(0)
    n = 3000
    days = pd.date_range("2024-01-01", periods=300, freq="B")
    df = pd.DataFrame({"trade_date": rng.choice(days, n), "ts_code": [f"S{i}" for i in range(n)],
                       "vol_x": rng.random(n), "auc_vol_ratio": rng.random(n)})
    for f in ("auc_turnover", "float_mv", "boards", "first_min", "open_times", "seal_ratio",
              "sector_ups", "market_ups", "gap"):
        df[f] = rng.random(n)
    df["r_n_open"] = rng.normal(0, 0.03, n) + (df["auc_vol_ratio"] > 0.66) * 0.02
    df["r_n_close"] = df["r_close"] = df["r_n_open"]
    df["hit_up"] = df["close_up"] = df["one_word"] = False
    rules = search_rules(df, days[180], min_n=50)
    assert rules and any(f == "auc_vol_ratio" for f, _, _ in rules[0]["rule"])
    assert rules[0]["test"]["n"] > 0 and len(_apply(df, rules[0]["rule"])) > 0
