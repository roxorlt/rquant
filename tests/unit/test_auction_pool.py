from datetime import date

import pandas as pd

from rquant.research import auction_pool as ap
from rquant.research.surge import format_push


def test_filter_candidates():
    df = pd.DataFrame({"ts_code": ["A.SZ", "B.SZ", "C.SZ", "D.BJ", "E.SH"],
                       "high": [11, 11, 11, 11, 11], "low": [10, 11, 10, 10, 10],
                       "fd_amount": [5e6, 1e6, 5e8, 1e6, 1e6], "circ_mv": [1e5] * 5,
                       "is_st": [False, False, False, False, True]})
    assert list(ap.filter_candidates(df)["ts_code"]) == ["A.SZ"]


def test_rank_picks_and_store(tmp_path):
    cands = [{"ts_code": c, "name": c, "y_close": 10.0} for c in "ABCDE"]
    q = pd.DataFrame({"ts_code": list("ABCDE"), "price": [10.6, 10.7, 10.3, 10.8, 11.0],
                      "pre_close": [10.0] * 5, "volume": [100, 300, 900, 200, 50]})
    picks = ap.rank_picks(cands, q)
    assert [(p["rank"], p["ts_code"]) for p in picks] == [(1, "B"), (2, "D"), (3, "A")]
    day = date(2026, 10, 12)
    ap._write(tmp_path, day, {"date": "2026-10-12", "candidates": cands, "picks": picks})
    assert ap.priority_codes(tmp_path, day)[:3] == ["B", "D", "A"]
    assert ap.read_pool(tmp_path)["date"] == "2026-10-12"


def test_push_priority():
    evs = [{"ts_code": c, "name": c, "ratio": r, "time": "09:40:00", "acc5": 1.0, "pct": 0.01,
            "amount": 1e8} for c, r in (("X", 9.0), ("P", 3.0))]
    _, body = format_push(evs, {"P"})
    assert body.index("★P") < body.index("X")
