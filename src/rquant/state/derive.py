"""派生状态字段计算。

用 MyTT（通达信风格函数）组装以下字段：
- ST / 北交所 / 板块分类
- 涨停价 / 跌停价（分档：ST 5% / 主板 10% / 创业板科创板 20% / 北交所 30%）
- is_limit_up / is_limit_down（容差 1 分）
- is_first_limit_up（今涨停 且 昨未涨停）
- is_yiziban（4 价相等 且 涨停）
- consecutive_limit_ups（连板数，BARSLASTCOUNT 直接对应通达信语义）
- body_upper / body_lower（实体上下沿 = max/min(open, close)）

涨跌停规则历史沿革比较复杂（创业板 2020-08-24 从 10% 改 20%、主板 2023-05-01
起全面注册制等），MVP 阶段按**当前规则**算，不回溯历史规则差异。
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from MyTT import BARSLASTCOUNT, REF


def _classify_board(ts_code: str) -> str:
    """按 ts_code 判定板块。返回 main/gem/star/bj。"""
    if ts_code.endswith(".BJ"):
        return "bj"
    if ts_code.startswith(("688", "689")):
        return "star"
    if ts_code.startswith(("300", "301")):
        return "gem"
    return "main"


def _detect_st(name: str | None) -> bool:
    """从股票名称判断是否 ST。覆盖 ST / *ST / SST 前缀（含空格）。"""
    if not name:
        return False
    n = name.upper().replace(" ", "").replace("\u3000", "")
    return n.startswith(("ST", "*ST", "SST"))


def _limit_pct(is_st: bool, board_type: str) -> float:
    """涨跌停幅度。"""
    if is_st:
        return 0.05
    if board_type == "bj":
        return 0.30
    if board_type in ("gem", "star"):
        return 0.20
    return 0.10


def derive_state(
    df_daily: pd.DataFrame,
    ts_code: str,
    name: str | None = None,
    price_tol: float = 0.01,
) -> pd.DataFrame:
    """计算一只股票的派生状态字段。

    Args:
        df_daily: 日线 DataFrame，必须含 trade_date, open, high, low, close, pre_close，
                  按 trade_date 升序。**用原始价（不复权）**，涨停判断基于真实 pre_close。
        ts_code: 股票代码（带后缀，如 600519.SH）
        name: 股票名称（用于 ST 判断）
        price_tol: 价格比较容差（默认 0.01 元，即 1 分）

    Returns:
        DataFrame（14 列），列顺序与 schema.DAILY_STATE_DDL 对齐
    """
    if df_daily.empty:
        return pd.DataFrame()

    df = df_daily.sort_values("trade_date").reset_index(drop=True)

    # 固定字段（不随日期变）
    is_st = _detect_st(name)
    is_bj = ts_code.endswith(".BJ")
    board_type = _classify_board(ts_code)
    limit_pct = _limit_pct(is_st, board_type)

    # 逐日字段
    open_ = df["open"].astype("float64")
    close = df["close"].astype("float64")
    high = df["high"].astype("float64")
    low = df["low"].astype("float64")
    pre_close = df["pre_close"].astype("float64")

    # 涨停价 / 跌停价（四舍五入到分）
    limit_up_price = (pre_close * (1 + limit_pct)).round(2)
    limit_down_price = (pre_close * (1 - limit_pct)).round(2)

    # 涨停 / 跌停（容差 1 分，pre_close 缺失时为 False）
    valid = close.notna() & pre_close.notna() & (pre_close > 0)
    is_limit_up = valid & (close >= limit_up_price - price_tol)
    is_limit_down = valid & (close <= limit_down_price + price_tol)

    # 首板：今涨停 且 昨未涨停（REF = 昨日值；MyTT 返回 ndarray，包回 Series）
    prev_is_lu = pd.Series(
        REF(is_limit_up.astype(float), 1), index=is_limit_up.index
    ).fillna(0).astype(bool)
    is_first_limit_up = is_limit_up & ~prev_is_lu

    # 一字板：4 价极差 ≤ 容差 且 涨停
    ohlc = pd.concat([high, low, open_, close], axis=1)
    rng = ohlc.max(axis=1) - ohlc.min(axis=1)
    is_yiziban = is_limit_up & (rng.abs() <= price_tol)

    # 连板数：MyTT BARSLASTCOUNT 直接对应通达信语义（包回 Series）
    consecutive = pd.Series(
        BARSLASTCOUNT(is_limit_up), index=is_limit_up.index
    ).astype("Int64")

    # 实体上下沿
    body_upper = pd.concat([open_, close], axis=1).max(axis=1)
    body_lower = pd.concat([open_, close], axis=1).min(axis=1)

    result = pd.DataFrame(
        {
            "ts_code": ts_code,
            "trade_date": df["trade_date"],
            "is_st": is_st,
            "is_bj": is_bj,
            "board_type": board_type,
            "limit_pct": limit_pct,
            "limit_up_price": limit_up_price,
            "limit_down_price": limit_down_price,
            "is_limit_up": is_limit_up,
            "is_limit_down": is_limit_down,
            "is_first_limit_up": is_first_limit_up,
            "is_yiziban": is_yiziban,
            "consecutive_limit_ups": consecutive,
            "body_upper": body_upper,
            "body_lower": body_lower,
        }
    )

    logger.debug(f"{ts_code} 派生状态 {len(result)} 行")
    return result
