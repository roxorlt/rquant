"""宽表加载：把 daily_bar/daily_indicator/daily_state/stock_basic 合并成
每行 1 只股票、价量字段带 [n] 后缀的宽表。"""

from __future__ import annotations

import pandas as pd

from rquant.storage.duckdb import DuckDBStore

PRICE_COLS_MAP = {
    "open": "OPEN",
    "high": "HIGH",
    "low": "LOW",
    "close": "CLOSE",
    "pre_close": "PRE_CLOSE",
    "vol": "VOL",
    "amount": "AMOUNT",
    "pct_chg": "PCT_CHG",
}

IND_COLS_MAP = {
    "ma5": "MA5", "ma10": "MA10", "ma20": "MA20", "ma60": "MA60",
    "rsi6": "RSI6", "rsi14": "RSI14",
    "macd": "MACD", "macd_signal": "MACD_SIGNAL", "macd_hist": "MACD_HIST",
    "kdj_k": "KDJ_K", "kdj_d": "KDJ_D", "kdj_j": "KDJ_J",
}

STATE_COLS_MAP = {
    "is_limit_up": "IS_LIMIT_UP",
    "is_limit_down": "IS_LIMIT_DOWN",
    "is_first_limit_up": "IS_FIRST_LIMIT_UP",
    "is_yiziban": "IS_YIZIBAN",
    "consecutive_limit_ups": "CONSECUTIVE_LIMIT_UPS",
    "body_upper": "BODY_UPPER",
    "body_lower": "BODY_LOWER",
}

BASIC_COLS_MAP = {
    "circ_mv": "CIRC_MV",
    "total_mv": "TOTAL_MV",
    "turnover_rate": "TURNOVER_RATE",
}


def _resolve_trading_dates(
    store: DuckDBStore, trade_date: str, lookback: int
) -> list[str]:
    """返回 [T 日, T-1 日, ..., T-lookback 日] 的字符串日期列表。"""
    sql = """
    SELECT strftime(trade_date, '%Y-%m-%d') AS d
    FROM (
        SELECT DISTINCT trade_date FROM daily_bar
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT ?
    )
    ORDER BY d DESC
    """
    rows = store._conn.execute(sql, [trade_date, lookback + 1]).fetchall()
    return [r[0] for r in rows]


def _wide_from_long(
    long_df: pd.DataFrame, rename_map: dict[str, str], date_to_offset: dict[str, int]
) -> pd.DataFrame:
    """把 (ts_code, trade_date_str, <field>...) 长表 pivot 成
    ts_code × <FIELD>[offset] 宽表。"""
    if long_df.empty:
        return pd.DataFrame(columns=["ts_code"])

    long_df = long_df.copy()
    long_df["offset"] = long_df["trade_date_str"].map(date_to_offset)
    long_df = long_df.dropna(subset=["offset"])
    long_df["offset"] = long_df["offset"].astype(int)

    frames: list[pd.DataFrame] = []
    for src, dst in rename_map.items():
        if src not in long_df.columns:
            continue
        p = long_df.pivot(index="ts_code", columns="offset", values=src)
        p.columns = [f"{dst}[{c}]" for c in p.columns]
        frames.append(p)

    if not frames:
        return pd.DataFrame({"ts_code": long_df["ts_code"].unique()})

    wide = pd.concat(frames, axis=1).reset_index()
    return wide


def load_universe(
    trade_date: str,
    lookback: int = 5,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    owns_store = store is None
    store = store or DuckDBStore()

    try:
        dates = _resolve_trading_dates(store, trade_date, lookback)
        if not dates:
            return pd.DataFrame()
        date_to_offset = {d: i for i, d in enumerate(dates)}
        t0_date = dates[0]

        # universe：T 日有日线数据的所有股票
        universe_sql = """
        SELECT DISTINCT ts_code
        FROM daily_bar
        WHERE trade_date = ?
        """
        universe = store._conn.execute(universe_sql, [t0_date]).fetchdf()
        if universe.empty:
            return pd.DataFrame()

        in_universe = universe["ts_code"].tolist()
        placeholders = ",".join(["?"] * len(in_universe))

        # 日线 + 指标长表
        bar_sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {", ".join(PRICE_COLS_MAP.keys())}
        FROM daily_bar
        WHERE ts_code IN ({placeholders})
          AND trade_date IN ({",".join(["?"] * len(dates))})
        """
        bar_long = store._conn.execute(bar_sql, in_universe + dates).fetchdf()
        bar_wide = _wide_from_long(bar_long, PRICE_COLS_MAP, date_to_offset)

        ind_sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {", ".join(IND_COLS_MAP.keys())}
        FROM daily_indicator
        WHERE ts_code IN ({placeholders})
          AND trade_date IN ({",".join(["?"] * len(dates))})
        """
        ind_long = store._conn.execute(ind_sql, in_universe + dates).fetchdf()
        ind_wide = _wide_from_long(ind_long, IND_COLS_MAP, date_to_offset)

        state_sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {", ".join(STATE_COLS_MAP.keys())},
               is_st, is_bj, board_type
        FROM daily_state
        WHERE ts_code IN ({placeholders})
          AND trade_date IN ({",".join(["?"] * len(dates))})
        """
        state_long = store._conn.execute(state_sql, in_universe + dates).fetchdf()
        state_wide = _wide_from_long(state_long, STATE_COLS_MAP, date_to_offset)

        # daily_basic: circ_mv / total_mv / turnover_rate
        basic_mkt_sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {", ".join(BASIC_COLS_MAP.keys())}
        FROM daily_basic
        WHERE ts_code IN ({placeholders})
          AND trade_date IN ({",".join(["?"] * len(dates))})
        """
        basic_mkt_long = store._conn.execute(basic_mkt_sql, in_universe + dates).fetchdf()
        basic_mkt_wide = _wide_from_long(basic_mkt_long, BASIC_COLS_MAP, date_to_offset)

        # 不分日属性：取 T 日的 is_st / is_bj / board_type
        state_t0 = state_long[state_long["trade_date_str"] == t0_date][
            ["ts_code", "is_st", "is_bj", "board_type"]
        ].drop_duplicates(subset=["ts_code"])

        # stock_basic 拿 name
        basic = store._conn.execute(
            f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
            in_universe,
        ).fetchdf()

        # 合并所有
        out = universe.merge(basic, on="ts_code", how="left")
        out = out.merge(state_t0, on="ts_code", how="left")
        for wide in (bar_wide, ind_wide, state_wide, basic_mkt_wide):
            if not wide.empty:
                out = out.merge(wide, on="ts_code", how="left")

        # 默认值填充
        if "is_st" in out.columns:
            out["is_st"] = out["is_st"].fillna(False).astype(bool)
        if "is_bj" in out.columns:
            out["is_bj"] = out["is_bj"].fillna(False).astype(bool)

        return out
    finally:
        if owns_store:
            store.close()
