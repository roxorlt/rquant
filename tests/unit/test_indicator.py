"""技术指标计算单测。"""

import numpy as np
import pandas as pd
import pytest

from rquant.indicator import compute_indicators


def _make_df(n: int = 100) -> pd.DataFrame:
    """生成 n 天递增的 qfq 数据，方便手算核对。"""
    rng = np.random.default_rng(42)
    close = np.linspace(10.0, 20.0, n) + rng.normal(0, 0.1, n)
    high = close + 0.2
    low = close - 0.2
    return pd.DataFrame(
        {
            "ts_code": ["TEST.SZ"] * n,
            "trade_date": pd.date_range("2024-01-01", periods=n, freq="D").date,
            "qfq_open": close,
            "qfq_high": high,
            "qfq_low": low,
            "qfq_close": close,
        }
    )


class TestComputeIndicators:
    def test_empty_input_returns_empty(self) -> None:
        out = compute_indicators(pd.DataFrame())
        assert out.empty
        assert "ma5" in out.columns

    def test_output_columns(self) -> None:
        out = compute_indicators(_make_df())
        expected = {
            "ts_code", "trade_date",
            "ma5", "ma10", "ma20", "ma60",
            "rsi6", "rsi14",
            "macd", "macd_signal", "macd_hist",
            "kdj_k", "kdj_d", "kdj_j",
        }
        assert expected.issubset(set(out.columns))

    def test_ma5_matches_manual_mean(self) -> None:
        df = _make_df(20)
        out = compute_indicators(df)
        # 最后一天 MA5 = 最后 5 个 close 的均值
        manual = df["qfq_close"].iloc[-5:].mean()
        assert out["ma5"].iloc[-1] == pytest.approx(manual, rel=1e-9)

    def test_ma5_early_rows_are_nan(self) -> None:
        out = compute_indicators(_make_df(20))
        # MA5 前 4 行不足 5 天，应该是 NaN
        assert out["ma5"].iloc[:4].isna().all()
        assert not pd.isna(out["ma5"].iloc[4])

    def test_rsi_in_valid_range(self) -> None:
        out = compute_indicators(_make_df(50))
        rsi = out["rsi14"].dropna()
        assert (rsi >= 0).all() and (rsi <= 100).all()

    def test_kdj_k_d_in_valid_range(self) -> None:
        out = compute_indicators(_make_df(50))
        # K/D 理论上在 0-100 之间（极端情况下 J 可能为负或 >100，所以只测 K/D）
        assert (out["kdj_k"] >= 0).all() and (out["kdj_k"] <= 100).all()
        assert (out["kdj_d"] >= 0).all() and (out["kdj_d"] <= 100).all()

    def test_macd_hist_equals_macd_minus_signal(self) -> None:
        out = compute_indicators(_make_df(100))
        diff = out["macd"] - out["macd_signal"]
        pd.testing.assert_series_equal(
            out["macd_hist"].rename(None), diff.rename(None), check_names=False
        )

    def test_output_length_matches_input(self) -> None:
        df = _make_df(77)
        out = compute_indicators(df)
        assert len(out) == len(df)
