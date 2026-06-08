"""宽表加载：把 daily_bar/daily_indicator/daily_state/stock_basic 合并成
每行 1 只股票、价量字段带 [n] 后缀的宽表。"""

from __future__ import annotations

import pandas as pd

from rquant.screen.rules import AggregateRequest
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


def _compute_aggregate(
    store: DuckDBStore,
    req: AggregateRequest,
    t0_date: str,
    ts_codes: list[str],
) -> pd.DataFrame:
    """根据 AggregateRequest 生成 DuckDB SQL，返回 (ts_code, <agg_col>) DataFrame。"""
    placeholders = ",".join(["?"] * len(ts_codes))

    # 找到 T 日前 window 个交易日的日期范围
    date_sql = """
    SELECT strftime(trade_date, '%Y-%m-%d') AS d
    FROM (
        SELECT DISTINCT trade_date FROM daily_bar
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT ?
    )
    ORDER BY d
    """
    date_rows = store._conn.execute(date_sql, [t0_date, req.window]).fetchall()
    window_dates = [r[0] for r in date_rows]

    if not window_dates:
        return pd.DataFrame(columns=["ts_code", req.name])

    # 排除 exclude_offset 对应的日期
    if req.exclude_offset is not None:
        # 获取完整日期列表（倒序），找到 offset 对应的日期
        all_dates_sql = """
        SELECT strftime(trade_date, '%Y-%m-%d') AS d
        FROM (
            SELECT DISTINCT trade_date FROM daily_bar
            WHERE trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT ?
        )
        ORDER BY d DESC
        """
        all_date_rows = store._conn.execute(
            all_dates_sql, [t0_date, req.window]
        ).fetchall()
        all_dates_desc = [r[0] for r in all_date_rows]
        if req.exclude_offset < len(all_dates_desc):
            exclude_date = all_dates_desc[req.exclude_offset]
            window_dates = [d for d in window_dates if d != exclude_date]

    if not window_dates:
        return pd.DataFrame(columns=["ts_code", req.name])

    date_placeholders = ",".join(["?"] * len(window_dates))

    # 根据 agg_func 生成 SQL
    if req.agg_func == "max":
        agg_expr = f"MAX({req.source_col})"
    elif req.agg_func == "sum":
        agg_expr = f"SUM({req.source_col})"
    elif req.agg_func == "any":
        agg_expr = f"BOOL_OR(CAST({req.source_col} AS BOOLEAN))"
    elif req.agg_func == "count_nonzero":
        agg_expr = f"SUM(CASE WHEN CAST({req.source_col} AS BOOLEAN) THEN 1 ELSE 0 END)"
    else:
        raise ValueError(f"Unsupported agg_func: {req.agg_func}")

    sql = f"""
    SELECT ts_code, {agg_expr} AS {req.name}
    FROM {req.source_table}
    WHERE ts_code IN ({placeholders})
      AND strftime(trade_date, '%Y-%m-%d') IN ({date_placeholders})
    GROUP BY ts_code
    """
    params = ts_codes + window_dates
    result = store._conn.execute(sql, params).fetchdf()
    return result


def load_universe(
    trade_date: str,
    lookback: int = 5,
    store: DuckDBStore | None = None,
    aggregate_requests: list[AggregateRequest] | None = None,
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

        # 数据源临时缺失（如 5/29 daily_basic 延迟、daily_state/daily_indicator 整表缺）
        # 时对应宽表为空 → 列消失 → screen 规则引用 CIRC_MV[0]/BODY_UPPER[1] 等崩
        # KeyError，整条 pipeline 挂掉。这里补全 IND/BASIC/STATE 标准列在 lookback 内
        # **各 offset**（不只当日 [0]）为 NaN（float）：
        # - 数值规则（circ_mv_lt 内部 .fillna(inf)）拿 NaN 得 False（该股不入选）
        # - bool 状态规则 _bool_state_rule 内部 .fillna(False)，NaN 安全
        # PRICE 与 universe 同源（daily_bar 非空则必有 [0..lookback]），无需补。
        max_offset = len(dates) - 1
        for cmap in (IND_COLS_MAP, BASIC_COLS_MAP, STATE_COLS_MAP):
            for dst in cmap.values():
                for off in range(max_offset + 1):
                    col = f"{dst}[{off}]"
                    if col not in out.columns:
                        out[col] = float("nan")

        # 聚合列：根据 AggregateRequest 动态生成 SQL
        if aggregate_requests:
            t0_date_val = dates[0]  # T 日
            for req in aggregate_requests:
                agg_col = _compute_aggregate(store, req, t0_date_val, in_universe)
                if not agg_col.empty:
                    out = out.merge(agg_col, on="ts_code", how="left")

        # 标量属性列默认值填充。daily_state 整表缺失时 state_t0 为空，is_st/is_bj/
        # board_type 这三列会不存在 → not_st / not_bj / board_in 引用崩 KeyError，
        # 所以先无条件补默认列再 fillna。
        if "is_st" not in out.columns:
            out["is_st"] = False
        if "is_bj" not in out.columns:
            out["is_bj"] = False
        if "board_type" not in out.columns:
            out["board_type"] = ""
        out["is_st"] = out["is_st"].fillna(False).astype(bool)
        out["is_bj"] = out["is_bj"].fillna(False).astype(bool)

        return out
    finally:
        if owns_store:
            store.close()
