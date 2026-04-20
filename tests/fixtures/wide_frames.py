"""宽表 DataFrame 构造器，用于积木单测。"""

from __future__ import annotations

import pandas as pd

# 默认 5 只股票、lookback=3 的迷你宽表
DEFAULT_CODES = ["000001.SZ", "300001.SZ", "688001.SH", "833001.BJ", "600001.SH"]


def make_wide_frame(
    codes: list[str] | None = None,
    lookback: int = 3,
    overrides: dict | None = None,
) -> pd.DataFrame:
    """构造一个宽表 DataFrame。

    默认每只股票每字段全填 0.0（或 False），lookback+1 天。
    overrides 按 {(ts_code, 列名): value} 局部覆盖。

    - `CLOSE[n]`, `OPEN[n]`, `HIGH[n]`, `LOW[n]`, `VOL[n]`, `PCT_CHG[n]`, `PRE_CLOSE[n]`
    - `MA5[n]`, `MA20[n]`, `MA60[n]`, `RSI14[n]`, `MACD[n]`
    - `IS_LIMIT_UP[n]`, `IS_LIMIT_DOWN[n]`, `IS_FIRST_LIMIT_UP[n]`, `IS_YIZIBAN[n]`,
      `CONSECUTIVE_LIMIT_UPS[n]`
    - `BODY_UPPER[n]`, `BODY_LOWER[n]`
    - `CIRC_MV[n]`, `TOTAL_MV[n]`, `TURNOVER_RATE[n]`
    - `is_st`, `is_bj`, `board_type`, `ts_code`, `name`
    """
    codes = codes or DEFAULT_CODES
    price_cols = ["CLOSE", "OPEN", "HIGH", "LOW", "VOL", "PCT_CHG", "PRE_CLOSE", "AMOUNT"]
    ind_cols = [
        "MA5", "MA10", "MA20", "MA60",
        "RSI6", "RSI14",
        "MACD", "MACD_SIGNAL", "MACD_HIST",
        "KDJ_K", "KDJ_D", "KDJ_J",
    ]
    bool_state_cols = [
        "IS_LIMIT_UP", "IS_LIMIT_DOWN", "IS_FIRST_LIMIT_UP", "IS_YIZIBAN",
    ]
    int_state_cols = ["CONSECUTIVE_LIMIT_UPS"]
    float_state_cols = ["BODY_UPPER", "BODY_LOWER"]
    basic_mkt_cols = ["CIRC_MV", "TOTAL_MV", "TURNOVER_RATE"]

    rows = []
    for code in codes:
        row = {"ts_code": code, "name": code.split(".")[0]}
        row["is_st"] = False
        row["is_bj"] = code.endswith(".BJ")
        if code.startswith("300") or code.startswith("301"):
            row["board_type"] = "gem"
        elif code.startswith("688") or code.startswith("689"):
            row["board_type"] = "star"
        elif code.endswith(".BJ"):
            row["board_type"] = "bj"
        else:
            row["board_type"] = "main"

        for n in range(lookback + 1):
            for c in price_cols:
                row[f"{c}[{n}]"] = 0.0
            for c in ind_cols:
                row[f"{c}[{n}]"] = 0.0
            for c in bool_state_cols:
                row[f"{c}[{n}]"] = False
            for c in int_state_cols:
                row[f"{c}[{n}]"] = 0
            for c in float_state_cols:
                row[f"{c}[{n}]"] = 0.0
            for c in basic_mkt_cols:
                row[f"{c}[{n}]"] = 0.0
        rows.append(row)

    df = pd.DataFrame(rows)

    if overrides:
        for (code, col), value in overrides.items():
            df.loc[df["ts_code"] == code, col] = value

    return df
