"""技术指标计算：MA / RSI / MACD / KDJ。

输入必须是按 trade_date 升序、单只股票、前复权价的 DataFrame。
列要求：ts_code, trade_date, qfq_open, qfq_high, qfq_low, qfq_close
（可由 DuckDBStore.get_daily_qfq() 直接提供）

所有指标基于前复权价计算，避免分红除权导致的指标跳变。
"""

from __future__ import annotations

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator


def _kdj(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    n: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """KDJ 指标（中国 A 股常用口径）。

    RSV = (close - LLV(low,n)) / (HHV(high,n) - LLV(low,n)) * 100
    K = SMA(RSV, 3, 1)  — α=1/3 的指数平滑（初值 50）
    D = SMA(K, 3, 1)    — α=1/3 的指数平滑（初值 50）
    J = 3K - 2D
    """
    llv = low.rolling(window=n, min_periods=1).min()
    hhv = high.rolling(window=n, min_periods=1).max()
    spread = hhv - llv
    rsv = (
        (close - llv) / spread.mask(spread.eq(0)) * 100
    ).astype("float64")

    k = pd.Series(index=close.index, dtype="float64")
    d = pd.Series(index=close.index, dtype="float64")
    prev_k, prev_d = 50.0, 50.0
    for i, rv in enumerate(rsv):
        if pd.isna(rv):
            k.iloc[i] = prev_k
            d.iloc[i] = prev_d
            continue
        cur_k = (2 / 3) * prev_k + (1 / 3) * rv
        cur_d = (2 / 3) * prev_d + (1 / 3) * cur_k
        k.iloc[i] = cur_k
        d.iloc[i] = cur_d
        prev_k, prev_d = cur_k, cur_d
    j = 3 * k - 2 * d
    return k, d, j


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """基于前复权 OHLCV 计算技术指标。

    Args:
        df: 单只股票、按 trade_date 升序，至少含
            ts_code / trade_date / qfq_high / qfq_low / qfq_close 列。

    Returns:
        含 ts_code, trade_date, ma5/10/20/60, rsi6/14,
        macd/macd_signal/macd_hist, kdj_k/kdj_d/kdj_j 的 DataFrame。
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "ts_code", "trade_date",
                "ma5", "ma10", "ma20", "ma60",
                "rsi6", "rsi14",
                "macd", "macd_signal", "macd_hist",
                "kdj_k", "kdj_d", "kdj_j",
            ]
        )

    close = df["qfq_close"].astype("float64")
    high = df["qfq_high"].astype("float64")
    low = df["qfq_low"].astype("float64")

    out = pd.DataFrame(
        {
            "ts_code": df["ts_code"].values,
            "trade_date": df["trade_date"].values,
            "ma5": SMAIndicator(close, window=5).sma_indicator(),
            "ma10": SMAIndicator(close, window=10).sma_indicator(),
            "ma20": SMAIndicator(close, window=20).sma_indicator(),
            "ma60": SMAIndicator(close, window=60).sma_indicator(),
            "rsi6": RSIIndicator(close, window=6).rsi(),
            "rsi14": RSIIndicator(close, window=14).rsi(),
        }
    )

    macd_calc = MACD(close, window_slow=26, window_fast=12, window_sign=9)
    out["macd"] = macd_calc.macd()
    out["macd_signal"] = macd_calc.macd_signal()
    out["macd_hist"] = macd_calc.macd_diff()

    k, d, j = _kdj(high, low, close, n=9)
    out["kdj_k"] = k
    out["kdj_d"] = d
    out["kdj_j"] = j

    return out
