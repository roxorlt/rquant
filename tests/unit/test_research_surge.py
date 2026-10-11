from pathlib import Path

import numpy as np
import pandas as pd

from rquant.research import surge
from rquant.research.auction import compute
from rquant.research.surge import PriorCurve, Pusher, SurgeScanner, format_push, trading_minute


def _prior(codes):
    cum = np.tile(np.linspace(0, 240_000, 241), (len(codes), 1))  # 1000 shares/min
    return PriorCurve(codes, cum, "2026-10-09", "test")


def _row(code, t, vol, price=10.5, amount=None, name="测试"):
    return {"ts_code": code, "name": name, "time": t, "price": price, "pre_close": 10.0,
            "volume": vol, "amount": amount if amount is not None else vol * price,
            "a1_v": 100.0, "b1_v": 100.0}


def test_trading_minute():
    assert trading_minute("09:30:00") == 0
    assert trading_minute("11:30:00") == 120
    assert trading_minute("12:00:00") == 120
    assert trading_minute("13:30:00") == 150
    assert trading_minute("15:00:05") == 240


def test_ratio_event_cooldown_and_filters(tmp_path: Path):
    sent = []
    pusher = Pusher(lambda t, b: sent.append((t, b)))
    sc = SurgeScanner(_prior(["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]),
                      tmp_path / "surge", pusher)
    rows = [_row("000001.SZ", "09:40:00", 10_000 * 4 * 1000, amount=5e8),     # ratio 4000? big
            _row("000002.SZ", "09:40:00", 5_000),                             # ratio 0.5
            _row("000003.SZ", "09:40:00", 40_000_000, name="*ST测", amount=5e8),
            _row("000004.SZ", "09:40:00", 40_000_000, price=11.0, amount=5e8)]  # locked up
    rows[3]["a1_v"] = 0.0
    ev = sc.update(pd.DataFrame(rows), wall=1000.0)
    assert set(ev["ts_code"]) == {"000001.SZ"}
    assert (tmp_path / "surge" / "events.jsonl").is_file()
    ev2 = sc.update(pd.DataFrame([_row("000001.SZ", "09:41:00", 5e7, amount=6e8)]), wall=1100.0)
    assert len(ev2) == 0  # cooldown
    sc.close()
    assert (tmp_path / "surge" / "events.parquet").is_file()
    assert (tmp_path / "surge" / "factors.parquet").is_file()
    day, rows = surge.read_events(tmp_path.parent if False else tmp_path, None)  # no snapshots dir
    assert rows == []


def test_accel(tmp_path: Path):
    sc = SurgeScanner(_prior(["000001.SZ"]), tmp_path)
    sc.update(pd.DataFrame([_row("000001.SZ", "09:50:00", 20_000, amount=1e8)]), wall=0.0)
    ev = sc.update(pd.DataFrame([_row("000001.SZ", "09:55:30", 20_000 + 60_000, amount=2e8)]),
                   wall=10.0)
    assert "accel" in set(ev["kind"])
    assert ev.iloc[0]["acc5"] > 5


def test_format_push():
    t, b = format_push([{"ts_code": "000001.SZ", "name": "平安", "time": "09:40:00", "ratio": 4.0,
                         "acc5": 2.0, "pct": 0.03, "amount": 1e8}])
    assert "异动 1只" in t and "平安" in b


def test_auction_compute(tmp_path: Path):
    d = tmp_path / "snapshots" / "2026-10-12"
    (d / "quotes").mkdir(parents=True)
    rows = []
    for t, p, v, a5 in [("09:16:00", 10.2, 1000, None), ("09:20:30", 10.3, 3000, None),
                        ("09:25:03", 10.4, 5000, 8000), ("09:30:03", 10.5, 7000, 6000),
                        ("09:31:00", 10.45, 8000, 5000)]:
        r = {"ts_code": "000001.SZ", "name": "x", "time": t, "price": p, "pre_close": 10.0,
             "volume": float(v), "amount": v * p}
        for i in range(1, 6):
            r[f"a{i}_v"] = a5 / 5 if a5 else None
            r[f"a{i}_p"] = p + i * 0.01 if a5 else None
        rows.append(r)
    pd.DataFrame(rows).to_parquet(d / "quotes" / "part-1.parquet")
    df = compute(d)
    r = df.iloc[0]
    assert r.auc_price == 10.4 and r.auc_vol == 5000
    assert abs(r.gap - 0.04) < 1e-9
    assert r.ask5_0925 == 8000
    assert r.buy_delta_5 == (2000 - 1000) / 3000
