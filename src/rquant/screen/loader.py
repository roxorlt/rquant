"""宽表加载：把 daily_bar/daily_indicator/daily_state/stock_basic 合并成
每行 1 只股票、价量字段带 [n] 后缀的宽表。"""

from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

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

PIT_STATE_COLS = (
    "is_limit_up",
    "is_limit_down",
    "is_first_limit_up",
    "is_yiziban",
    "consecutive_limit_ups",
)

BASIC_COLS_MAP = {
    "circ_mv": "CIRC_MV",
    "total_mv": "TOTAL_MV",
    "turnover_rate": "TURNOVER_RATE",
}

SHANGHAI = ZoneInfo("Asia/Shanghai")
DAILY_SCREEN_TIME = time(17, 0)
CALENDAR_EXCHANGE = "SSE"
_AGGREGATE_SOURCE_COLUMNS = {
    "daily_state": frozenset(
        {
            "is_limit_up",
            "is_limit_down",
            "is_first_limit_up",
            "is_yiziban",
            "consecutive_limit_ups",
        }
    ),
    "daily_bar": frozenset(PRICE_COLS_MAP),
    "daily_basic": frozenset(BASIC_COLS_MAP),
}


class ScreeningCalendarError(RuntimeError):
    """Authoritative trade-calendar coverage cannot support the screen."""


def _parse_trade_date(trade_date: str) -> date:
    return date.fromisoformat(trade_date[:10])


def _resolve_decision_at(
    trade_date: str, decision_at: datetime | None
) -> datetime:
    daily_screen_at = datetime.combine(
        _parse_trade_date(trade_date),
        DAILY_SCREEN_TIME,
        SHANGHAI,
    )
    if decision_at is None:
        return daily_screen_at
    if decision_at.tzinfo is None or decision_at.utcoffset() is None:
        raise ValueError("decision_at must be timezone-aware")
    if decision_at < daily_screen_at:
        raise ValueError(
            "decision_at must be at or after "
            f"{daily_screen_at:%Y-%m-%d %H:%M} Asia/Shanghai "
            "for close-only screening"
        )
    return decision_at


def _calendar_window(
    store: DuckDBStore,
    trade_date: str,
    count: int,
) -> tuple[list[str], bool]:
    """Return known open dates newest-first plus authoritative coverage state."""
    if count < 1:
        return [], False
    anchor = _parse_trade_date(trade_date)
    anchor_row = store._conn.execute(
        """
        SELECT is_open
        FROM trade_calendar
        WHERE exchange = ? AND cal_date = ?
        """,
        [CALENDAR_EXCHANGE, anchor],
    ).fetchone()
    if anchor_row is None or not bool(anchor_row[0]):
        return [], False

    rows = store._conn.execute(
        """
        SELECT strftime(cal_date, '%Y-%m-%d') AS trade_date
        FROM trade_calendar
        WHERE exchange = ?
          AND cal_date <= ?
          AND is_open
        ORDER BY cal_date DESC
        LIMIT ?
        """,
        [CALENDAR_EXCHANGE, anchor, count],
    ).fetchall()
    dates = [str(row[0]) for row in rows]
    if not dates:
        return [], False

    earliest = _parse_trade_date(dates[-1])
    present_count = int(
        store._conn.execute(
            """
            SELECT COUNT(*)
            FROM trade_calendar
            WHERE exchange = ? AND cal_date BETWEEN ? AND ?
            """,
            [CALENDAR_EXCHANGE, earliest, anchor],
        ).fetchone()[0]
    )
    civil_day_count = (anchor - earliest).days + 1
    complete = len(dates) == count and present_count == civil_day_count
    return dates, complete


def _resolve_trading_dates(
    store: DuckDBStore, trade_date: str, lookback: int
) -> list[str]:
    """返回 [T 日, T-1 日, ..., T-lookback 日] 的字符串日期列表。"""
    anchor = _parse_trade_date(trade_date)
    anchor_row = store._conn.execute(
        """
        SELECT is_open
        FROM trade_calendar
        WHERE exchange = ? AND cal_date = ?
        """,
        [CALENDAR_EXCHANGE, anchor],
    ).fetchone()
    if anchor_row is None:
        raise ScreeningCalendarError(
            f"authoritative {CALENDAR_EXCHANGE} trade calendar is missing anchor "
            f"{anchor.isoformat()}"
        )
    if not bool(anchor_row[0]):
        raise ScreeningCalendarError(
            f"screen trade date {anchor.isoformat()} is closed in authoritative "
            f"{CALENDAR_EXCHANGE} trade calendar"
        )

    dates, complete = _calendar_window(store, trade_date, lookback + 1)
    if complete:
        return dates

    if dates:
        earliest = _parse_trade_date(dates[-1])
        present_rows = store._conn.execute(
            """
            SELECT cal_date
            FROM trade_calendar
            WHERE exchange = ? AND cal_date BETWEEN ? AND ?
            ORDER BY cal_date
            """,
            [CALENDAR_EXCHANGE, earliest, anchor],
        ).fetchall()
        present_dates = {row[0] for row in present_rows}
        missing_dates = [
            date.fromordinal(ordinal)
            for ordinal in range(earliest.toordinal(), anchor.toordinal() + 1)
            if date.fromordinal(ordinal) not in present_dates
        ]
        if missing_dates:
            missing = ", ".join(day.isoformat() for day in missing_dates)
            raise ScreeningCalendarError(
                f"authoritative {CALENDAR_EXCHANGE} trade calendar is incomplete; "
                f"missing civil dates: {missing}"
            )

    raise ScreeningCalendarError(
        f"authoritative {CALENDAR_EXCHANGE} trade calendar does not contain "
        f"{lookback + 1} open days through {anchor.isoformat()}"
    )


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
    decision_at: datetime,
) -> pd.DataFrame:
    """根据 AggregateRequest 生成 DuckDB SQL，返回 (ts_code, <agg_col>) DataFrame。"""
    allowed_columns = _AGGREGATE_SOURCE_COLUMNS.get(req.source_table)
    if allowed_columns is None or req.source_col not in allowed_columns:
        raise ValueError(
            f"Unsupported aggregate source: {req.source_table}.{req.source_col}"
        )

    window_dates, calendar_complete = _calendar_window(
        store, t0_date, req.window
    )
    if req.exclude_offset is not None:
        if 0 <= req.exclude_offset < len(window_dates):
            exclude_date = window_dates[req.exclude_offset]
            window_dates = [d for d in window_dates if d != exclude_date]
        else:
            calendar_complete = False

    if not window_dates or not ts_codes:
        return pd.DataFrame(
            {
                "ts_code": pd.Series(ts_codes, dtype="string"),
                req.name: pd.Series([pd.NA] * len(ts_codes), dtype="object"),
            }
        )

    complete_predicate = "? AND COUNT(*) FILTER (WHERE fact_known) = ?"
    if req.agg_func == "max":
        agg_expr = (
            f"CASE WHEN {complete_predicate} "
            "THEN MAX(source_value) ELSE NULL END"
        )
    elif req.agg_func == "sum":
        agg_expr = (
            f"CASE WHEN {complete_predicate} "
            "THEN SUM(source_value) ELSE NULL END"
        )
    elif req.agg_func == "any":
        agg_expr = """
        CASE
            WHEN NOT ? THEN NULL
            WHEN COUNT(*) FILTER (
                WHERE fact_known AND CAST(source_value AS BOOLEAN)
            ) > 0 THEN TRUE
            WHEN COUNT(*) FILTER (WHERE fact_known) = ? THEN FALSE
            ELSE NULL
        END
        """
    elif req.agg_func == "count_nonzero":
        agg_expr = """
        CASE
            WHEN ? AND COUNT(*) FILTER (WHERE fact_known) = ?
            THEN COUNT(*) FILTER (
                WHERE fact_known AND CAST(source_value AS BOOLEAN)
            )
            ELSE NULL
        END
        """
    else:
        raise ValueError(f"Unsupported agg_func: {req.agg_func}")

    state_matches_status = (
        "AND source.is_st IS NOT DISTINCT FROM status.is_st"
        if req.source_table == "daily_state"
        else ""
    )
    sql = f"""
    WITH expected AS (
        SELECT codes.ts_code, dates.trade_date
        FROM UNNEST(?::VARCHAR[]) AS codes(ts_code)
        CROSS JOIN UNNEST(?::DATE[]) AS dates(trade_date)
    ),
    facts AS (
        SELECT
            expected.ts_code,
            source.{req.source_col} AS source_value,
            source.ts_code IS NOT NULL
                AND source.{req.source_col} IS NOT NULL
                AND status.ts_code IS NOT NULL
                AND status.conflict_reason IS NULL
                AND status.is_st IS NOT NULL
                AND status.available_at IS NOT NULL
                AND status.available_at <= ?
                {state_matches_status}
                AS fact_known
        FROM expected
        LEFT JOIN {req.source_table} AS source
            ON source.ts_code = expected.ts_code
           AND source.trade_date = expected.trade_date
        LEFT JOIN stock_status_daily AS status
            ON status.ts_code = expected.ts_code
           AND status.trade_date = expected.trade_date
    )
    SELECT ts_code, {agg_expr} AS aggregate_value
    FROM facts
    GROUP BY ts_code
    ORDER BY ts_code
    """
    expected_count = len(window_dates)
    params: list[object] = [ts_codes, window_dates, decision_at]
    params.extend([calendar_complete, expected_count])
    result = store._conn.execute(sql, params).fetchdf()
    result = result.rename(columns={"aggregate_value": req.name})
    return result


def load_universe(
    trade_date: str,
    lookback: int = 5,
    store: DuckDBStore | None = None,
    aggregate_requests: list[AggregateRequest] | None = None,
    decision_at: datetime | None = None,
) -> pd.DataFrame:
    """Load one post-close universe with PIT security status at ``decision_at``.

    The ordinary operational screen defaults to 17:00 Asia/Shanghai on the
    requested trading date. ``decision_at`` gates ``stock_status_daily``; it
    does not by itself prove that every panel dataset was historically visible.
    Research callers must enforce those dataset contracts before calling.
    """
    owns_store = store is None
    store = store or DuckDBStore()

    try:
        resolved_decision_at = _resolve_decision_at(trade_date, decision_at)
        dates = _resolve_trading_dates(store, trade_date, lookback)
        if not dates:
            return pd.DataFrame()
        date_to_offset = {d: i for i, d in enumerate(dates)}
        t0_date = dates[0]

        # universe：T 日有日线数据的所有股票
        universe_sql = """
        SELECT
            daily.ts_code,
            CASE
                WHEN status.conflict_reason IS NULL
                 AND status.name IS NOT NULL
                 AND length(trim(status.name)) > 0
                 AND status.available_at IS NOT NULL
                 AND status.available_at <= ?
                THEN trim(status.name)
                ELSE NULL
            END AS name,
            CASE
                WHEN status.conflict_reason IS NULL
                 AND status.is_st IS NOT NULL
                 AND status.available_at IS NOT NULL
                 AND status.available_at <= ?
                THEN status.is_st
                ELSE NULL
            END AS is_st
        FROM daily_bar AS daily
        LEFT JOIN stock_status_daily AS status
            ON status.ts_code = daily.ts_code
           AND status.trade_date = daily.trade_date
        WHERE daily.trade_date = ?
        ORDER BY daily.ts_code
        """
        universe = store._conn.execute(
            universe_sql,
            [resolved_decision_at, resolved_decision_at, t0_date],
        ).fetchdf()
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

        visible_state_columns = ", ".join(
            f"CASE WHEN status_visible THEN {column} ELSE NULL END AS {column}"
            for column in PIT_STATE_COLS
        )
        state_sql = f"""
        WITH state_facts AS (
            SELECT
                state.*,
                status.ts_code IS NOT NULL
                    AND status.conflict_reason IS NULL
                    AND status.is_st IS NOT NULL
                    AND status.available_at IS NOT NULL
                    AND status.available_at <= ?
                    AND state.is_st IS NOT DISTINCT FROM status.is_st
                    AS status_visible
            FROM daily_state AS state
            LEFT JOIN stock_status_daily AS status
                ON status.ts_code = state.ts_code
               AND status.trade_date = state.trade_date
            WHERE state.ts_code IN ({placeholders})
              AND state.trade_date IN ({",".join(["?"] * len(dates))})
        )
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date_str,
               {visible_state_columns}, body_upper, body_lower,
               is_bj, board_type
        FROM state_facts
        """
        state_long = store._conn.execute(
            state_sql,
            [resolved_decision_at, *in_universe, *dates],
        ).fetchdf()
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

        # 不分日属性：is_st/name 只来自 PIT status；板块属性取 T 日派生值。
        state_t0 = state_long[state_long["trade_date_str"] == t0_date][
            ["ts_code", "is_bj", "board_type"]
        ].drop_duplicates(subset=["ts_code"])

        # 合并所有
        out = universe.merge(state_t0, on="ts_code", how="left")
        for wide in (bar_wide, ind_wide, state_wide, basic_mkt_wide):
            if not wide.empty:
                out = out.merge(wide, on="ts_code", how="left")

        # 数据源临时缺失（如 5/29 daily_basic 延迟、daily_state/daily_indicator 整表缺）
        # 时对应宽表为空 → 列消失 → screen 规则引用 CIRC_MV[0]/BODY_UPPER[1] 等崩
        # KeyError，整条 pipeline 挂掉。这里补全所有标准列在 lookback 内
        # **各 offset**（不只当日 [0]）为 NaN（float）：
        # - 数值规则（circ_mv_lt 内部 .fillna(inf)）拿 NaN 得 False（该股不入选）
        # - bool 状态规则只接受显式 True/False，NaN 会 fail closed
        max_offset = len(dates) - 1
        for cmap in (PRICE_COLS_MAP, IND_COLS_MAP, BASIC_COLS_MAP, STATE_COLS_MAP):
            for dst in cmap.values():
                for off in range(max_offset + 1):
                    col = f"{dst}[{off}]"
                    if col not in out.columns:
                        out[col] = float("nan")

        # 聚合列：根据 AggregateRequest 动态生成 SQL
        if aggregate_requests:
            t0_date_val = dates[0]  # T 日
            for req in aggregate_requests:
                agg_col = _compute_aggregate(
                    store,
                    req,
                    t0_date_val,
                    in_universe,
                    resolved_decision_at,
                )
                if not agg_col.empty:
                    out = out.merge(agg_col, on="ts_code", how="left")

        # 缺失属性列保留 nullable unknown；规则负责 fail closed。
        if "is_bj" not in out.columns:
            out["is_bj"] = pd.Series(pd.NA, index=out.index, dtype="boolean")
        if "board_type" not in out.columns:
            out["board_type"] = pd.Series(pd.NA, index=out.index, dtype="string")
        out["name"] = out["name"].astype("string")
        out["is_st"] = out["is_st"].astype("boolean")
        out["is_bj"] = out["is_bj"].astype("boolean")
        out["board_type"] = out["board_type"].astype("string")

        return out
    finally:
        if owns_store:
            store.close()
