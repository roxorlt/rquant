import json

import pandas as pd

from rquant.research import auction_bt as bt
from rquant.research import auction_paper as paper
from rquant.research import promotion_gate as gate
from rquant.research import studies


def test_select_top_and_net_trades():
    df = pd.DataFrame({"trade_date": ["d1"] * 3 + ["d2"], "v": [1, 3, 2, 5],
                       "one_word": [False, True, False, False],
                       "r_n_open": [0.01, 0.05, 0.0, 0.02], "r_n_close": [0.0] * 4})
    top = bt.select_top(df, "v", top_n=2)
    assert list(top["v"]) == [3, 2, 5]
    net = bt.net_trades(top)
    assert list(net["v"]) == [2, 5]  # 一字 unbuyable
    assert abs(net["r_n_open_net"].iloc[1] - (0.02 - 0.0031)) < 1e-12


def test_gate_rules():
    days = pd.bdate_range("2025-07-01", "2026-06-30")
    good = pd.DataFrame({"trade_date": days, "r": 0.01 + 0.001 * (days.day % 3)})
    res = gate.evaluate(good, "r", studies.SPLIT_DATE, paper_days=25)
    assert res["passed"] and res["months_positive"] == 1.0
    assert not gate.evaluate(good, "r", studies.SPLIT_DATE, paper_days=5)["passed"]
    bad = good.assign(r=-good["r"])
    assert not gate.evaluate(bad, "r", studies.SPLIT_DATE, paper_days=25)["passed"]


def test_gate_file(tmp_path):
    assert not gate.gate_passed(tmp_path, "auction-ths")
    gate.gate_path(tmp_path, "auction-ths").parent.mkdir(parents=True)
    gate.gate_path(tmp_path, "auction-ths").write_text(json.dumps({"passed": True}))
    assert gate.gate_passed(tmp_path, "auction-ths")


def test_paper_trades_skip_limit_open():
    pool = {"date": "2026-10-14", "picks": [
        {"ts_code": "A", "name": "A", "rank": 1, "auction_price": 11.0, "limit_open": True},
        {"ts_code": "B", "name": "B", "rank": 2, "auction_price": 10.5, "limit_open": False}]}
    bars = pd.DataFrame({"ts_code": ["A", "B", "A", "B"],
                         "trade_date": ["2026-10-14"] * 2 + ["2026-10-15"] * 2,
                         "open": [11.0, 10.5, 12.0, 10.71], "close": [11, 10.6, 12, 10.5]})
    assert paper.trades_for_pool(pool, bars[bars.trade_date == "2026-10-14"]) is None
    t = paper.trades_for_pool(pool, bars)
    assert [x["ts_code"] for x in t] == ["B"]
    assert abs(t[0]["r_n_open"] - (10.71 / 10.5 - 1 - bt.COST)) < 1e-4


def test_registry_and_luck(tmp_path):
    import numpy as np

    rng = np.random.default_rng(1)
    n = 400
    df = pd.DataFrame({"trade_date": pd.to_datetime("2025-01-01")
                       + pd.to_timedelta(rng.integers(0, 300, n), "D"),
                       "vol_x": rng.random(n), "one_word": False, "hit_up": False,
                       "close_up": False, "r_close": 0.0,
                       "r_n_open": rng.normal(0, 0.03, n), "r_n_close": rng.normal(0, 0.03, n)})
    for f in studies.FEATURES:
        df[f] = rng.random(n)
    studies.search_rules(df, pd.Timestamp("2025-09-01"), min_n=20, root=tmp_path, shuffles=3)
    rec = json.loads((tmp_path / "registry.jsonl").read_text().splitlines()[-1])
    assert rec["n_scored"] == len(rec["trials"]) > 0 and rec["luck_check"]["runs"] == 3
