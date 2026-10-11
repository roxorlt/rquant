import numpy as np
import pandas as pd

from rquant.research import report_factors as rf


def _daily(n_days: int = 160, n_codes: int = 60, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2025-01-01", periods=n_days)
    codes = [f"{i:06d}.SZ" for i in range(n_codes)]
    rows = []
    for c in codes:
        px = 10 * np.cumprod(1 + rng.normal(0, 0.02, n_days))
        for d, p in zip(dates, px, strict=True):
            rows.append((c, d, p * 1.02, p * 0.98, p, 1.0, 1e5, 1e6, 10.0, rng.normal(0, 100)))
    return pd.DataFrame(rows, columns=["ts_code", "trade_date", "high", "low", "close",
                                       "pct_chg", "amount", "circ_mv", "limit_pct", "big_net"])


def test_ideal_amp_shape_and_sign():
    close = pd.DataFrame({"A": np.arange(25.0) + 10})
    high = close + (close - 10) * 0.01  # amplitude grows with price
    amp = rf._ideal_amp(high, close, close)
    assert amp.iloc[:19].isna().all().all()
    assert (amp.iloc[19:] > 0).all().all()


def test_evaluate_detects_planted_signal():
    d = _daily()
    basic = pd.DataFrame({"ts_code": d.ts_code.unique(), "name": "x", "list_date": "20100101"})
    close, tradable, size = rf.universe(d, basic)
    fwd = close.shift(-rf.HOLD) / close - 1
    noise = np.random.default_rng(1).normal(0, 0.02, fwd.shape)
    res = rf.evaluate(fwd + noise, close, tradable, size)
    assert res["sign"] == 1
    assert res["test"]["rank_ic"]["mean"] > 0.3
    assert res["series"]["q_nav"][4][-1] > res["series"]["q_nav"][0][-1]
