"""rQuant Health Dashboard 监控看板（独立 Streamlit 入口）。

启动方式：
    streamlit run src/rquant/dashboard/app.py --server.port 8501 \\
        --server.address 0.0.0.0 --server.headless true

此页面包含 Sections 1-9（systemd / 数据 / Watchlist / 事件 / K 线 / 黑名单等），
30 秒 meta refresh。NL 选股是独立的 Streamlit 应用，跑在另一个端口，
启动方式见 `src/rquant/dashboard/nl_screen.py` 文件头。
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import altair as alt
import pandas as pd
import streamlit as st

from rquant.dashboard.runtime_console_data import (
    ServingFrameState,
)
from rquant.dashboard.serving_page_data import ServingPageRenderContext
from rquant.dashboard.serving_page_ui import render_serving_state_banner
from rquant.serving_paths import serving_root_from_env

REFRESH_SECONDS = 30
CST = timezone(timedelta(hours=8))
_dashboard_serving_context: ServingPageRenderContext | None = None
_dashboard_serving_banners: set[tuple[str, str]] = set()

st.set_page_config(
    page_title="rQuant Health",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(
    f'<meta http-equiv="refresh" content="{REFRESH_SECONDS}">',
    unsafe_allow_html=True,
)

# 紧凑型 CSS 美化（参考 Linear / Vercel 风格）
st.markdown(
    """
    <style>
    /* 全局缩小基础字号 */
    html, body, [class*="st-"], .stMarkdown, .stText {
        font-size: 13px;
    }

    /* Block 容器：限宽 + 紧凑 padding */
    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 1rem !important;
        max-width: 1400px;
    }

    /* H1 顶部标题 */
    h1, .stMarkdown h1 {
        font-size: 1.5rem !important;
        margin-bottom: 0.25rem !important;
        font-weight: 700 !important;
    }

    /* H2 章节标题：小号大写 + 上方细线分隔 */
    h2, .stMarkdown h2 {
        font-size: 0.78rem !important;
        margin-top: 1.5rem !important;
        margin-bottom: 0.6rem !important;
        padding-top: 0.6rem !important;
        color: #6b7280 !important;
        border-top: 1px solid #e5e7eb !important;
        text-transform: uppercase !important;
        letter-spacing: 0.08em !important;
        font-weight: 600 !important;
    }

    /* H3 sub-section（容器内标题）*/
    h3, .stMarkdown h3 {
        font-size: 0.85rem !important;
        margin-bottom: 0.5rem !important;
        margin-top: 0 !important;
        color: #374151 !important;
        font-weight: 600 !important;
    }

    /* Metric 卡 */
    [data-testid="stMetric"] {
        padding: 0.3rem 0 !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.15rem !important;
        font-weight: 600 !important;
        line-height: 1.2 !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.7rem !important;
        color: #6b7280 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.04em !important;
        font-weight: 500 !important;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.75rem !important;
        font-weight: 500 !important;
    }

    /* DataFrame */
    [data-testid="stDataFrame"] {
        font-size: 0.78rem !important;
    }
    [data-testid="stDataFrame"] th {
        font-size: 0.72rem !important;
        font-weight: 600 !important;
        color: #6b7280 !important;
    }

    /* Container border：细灰 + 圆角 */
    [data-testid="stVerticalBlockBorderWrapper"] {
        border: 1px solid #e5e7eb !important;
        border-radius: 6px !important;
        padding: 0.75rem 1rem !important;
        background: #fafafa !important;
    }

    /* Caption / 副文 */
    [data-testid="stCaptionContainer"], .stCaption {
        font-size: 0.72rem !important;
        color: #9ca3af !important;
    }

    /* Divider 极细 */
    hr {
        margin: 0.75rem 0 !important;
        border-color: #e5e7eb !important;
    }

    /* Alert（info / success / warning / error）紧凑 */
    [data-testid="stAlert"] {
        padding: 0.5rem 0.75rem !important;
        font-size: 0.8rem !important;
    }

    /* Expander */
    [data-testid="stExpander"] {
        border: 1px solid #e5e7eb !important;
        border-radius: 6px !important;
    }
    [data-testid="stExpander"] summary {
        font-size: 0.8rem !important;
        padding: 0.4rem 0.75rem !important;
    }

    /* 收紧 columns 之间间距 */
    [data-testid="column"] {
        padding: 0 0.4rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── 工具函数 ──


def query_duckdb(
    sql: str,
    params: list | None = None,
    *,
    required_projections: tuple[str, ...] = (),
) -> pd.DataFrame | None:
    """查询独立 serving generation；不可用时返回页面空态。"""

    if _dashboard_serving_context is None:
        return None
    result = _dashboard_serving_context.query(
        sql, params or (), required_projections=required_projections
    )
    banner_key = (result.state.value, result.detail)
    if result.state is not ServingFrameState.READY and banner_key not in _dashboard_serving_banners:
        render_serving_state_banner(st, result, label="Serving 数据")
        _dashboard_serving_banners.add(banner_key)
    if result.state is ServingFrameState.UNAVAILABLE:
        return None
    frame = result.dataframe()
    assert isinstance(frame, pd.DataFrame)
    return frame


def screen_result_date_bounds(
    preset_name: str = "n-shape-pool1",
) -> tuple[date | None, date | None]:
    if _dashboard_serving_context is None:
        return None, None
    result = _dashboard_serving_context.screen_bounds(preset_name)
    render_serving_state_banner(st, result, label="候选区间")
    minimum, maximum, _candidate_count = result.value
    return minimum, maximum


def minute_data_overview() -> pd.DataFrame | None:
    return query_duckdb(
        """
        SELECT rows_count, codes_count, min_time, max_time
        FROM minute_coverage
        WHERE is_total
        ORDER BY rows_count DESC, source
        LIMIT 1
        """,
        required_projections=("minute_coverage",),
    )


def strategy_compare_cached(
    start_date: str,
    end_date: str,
    entry_modes: tuple[str, ...],
    profile_variants: tuple[str, ...],
    max_hold_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    if _dashboard_serving_context is None:
        raise RuntimeError("serving generation unavailable")
    acquired = _dashboard_serving_context
    mode_marks = ",".join("?" for _ in entry_modes)
    variant_marks = ",".join("?" for _ in profile_variants)
    parameters = [
        start_date,
        end_date,
        max_hold_days,
        *entry_modes,
        *profile_variants,
    ]
    summary_result = acquired.query(
        f"""
        WITH eligible AS (
          SELECT * FROM strategy_summary
          WHERE start_date = CAST(? AS DATE)
            AND end_date = CAST(? AS DATE)
            AND max_hold_days = ?
            AND entry_mode IN ({mode_marks})
            AND profile_variant IN ({variant_marks})
        ), selected_run AS (
          SELECT run_id
          FROM eligible
          GROUP BY run_id
          ORDER BY MAX(computed_at) DESC, run_id DESC
          LIMIT 1
        )
        SELECT e.* EXCLUDE (run_id, start_date, end_date, max_hold_days, computed_at)
        FROM eligible e JOIN selected_run r USING (run_id)
        ORDER BY entry_mode, profile_variant
        """,
        parameters,
        max_rows=2_000,
        max_result_bytes=1024 * 1024,
        max_query_seconds=1.0,
        required_projections=("strategy_summary", "strategy_trade"),
    )
    if summary_result.state is ServingFrameState.UNAVAILABLE:
        raise RuntimeError(summary_result.detail)
    summary = summary_result.dataframe()
    assert isinstance(summary, pd.DataFrame)
    if summary.empty:
        return summary, pd.DataFrame(), 0
    selected_result = acquired.query(
        f"""
        SELECT run_id
        FROM strategy_summary
        WHERE start_date = CAST(? AS DATE)
          AND end_date = CAST(? AS DATE)
          AND max_hold_days = ?
          AND entry_mode IN ({mode_marks})
          AND profile_variant IN ({variant_marks})
        GROUP BY run_id
        ORDER BY MAX(computed_at) DESC, run_id DESC
        LIMIT 1
        """,
        parameters,
        max_rows=1,
        max_result_bytes=4096,
        max_query_seconds=1.0,
    )
    if selected_result.state is ServingFrameState.UNAVAILABLE or not selected_result.rows:
        raise RuntimeError(selected_result.detail)
    selected_run = selected_result.rows[0][0]
    trades_result = acquired.query(
        """
        SELECT * EXCLUDE (run_id)
        FROM strategy_trade
        WHERE run_id = ?
        ORDER BY signal_date, ts_code, trade_id
        LIMIT 10000
        """,
        [selected_run],
        max_rows=10_000,
        max_result_bytes=5 * 1024 * 1024,
        max_query_seconds=2.0,
    )
    if trades_result.state is ServingFrameState.UNAVAILABLE:
        raise RuntimeError(trades_result.detail)
    trades = trades_result.dataframe()
    assert isinstance(trades, pd.DataFrame)
    return summary, trades, int(summary["candidates"].max())


def db_lock_holder() -> str | None:
    """探测 serving generation 是否可读；绝不探测或回退生产主库。"""
    try:
        return None if _dashboard_serving_context is not None else "serving-unavailable"
    except Exception:
        return "serving-unavailable"


def find_timer(timers: list[dict], unit_name: str) -> dict | None:
    for t in timers:
        if t.get("unit") == unit_name:
            return t
    return None


def fmt_us_timestamp(us: int | str | datetime | pd.Timestamp | None) -> str:
    """systemctl 输出的微秒时间戳转 UTC+8 字符串。"""
    if not us:
        return "—"
    try:
        if isinstance(us, (datetime, pd.Timestamp)):
            dt = pd.Timestamp(us).to_pydatetime()
            dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt
            dt = dt.astimezone(CST)
        else:
            us_int = int(us)
            if us_int <= 0:
                return "—"
            dt = datetime.fromtimestamp(us_int / 1_000_000, tz=CST)
        return dt.strftime("%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return str(us)


def time_diff_human(target_us: int | str | datetime | pd.Timestamp) -> str:
    """目标微秒时间戳距现在多久（人类可读）。"""
    if not target_us:
        return ""
    try:
        if isinstance(target_us, (datetime, pd.Timestamp)):
            target_dt = pd.Timestamp(target_us).to_pydatetime()
            target_dt = target_dt.replace(tzinfo=UTC) if target_dt.tzinfo is None else target_dt
            target_dt = target_dt.astimezone(CST)
        else:
            target_dt = datetime.fromtimestamp(int(target_us) / 1_000_000, tz=CST)
        now_dt = datetime.now(CST)
        delta = target_dt - now_dt
        total_min = int(delta.total_seconds() / 60)
        if total_min < 0:
            total_min = -total_min
            prefix = ""
            suffix = "前"
        else:
            prefix = ""
            suffix = "后"
        if total_min < 60:
            return f"{prefix}{total_min} 分钟{suffix}"
        hours = total_min / 60
        if hours < 24:
            return f"{prefix}{hours:.1f} 小时{suffix}"
        return f"{prefix}{hours / 24:.1f} 天{suffix}"
    except Exception:
        return ""


def get_realtime_quotes_batch(ts_codes: tuple[str, ...]) -> pd.DataFrame:
    """从同一 serving generation 的最新市场快照读取 watchlist 价格。"""
    if not ts_codes:
        return pd.DataFrame(columns=["代码", "code_short", "名称", "最新价", "最低", "最高"])
    marks = ",".join("?" for _ in ts_codes)
    frame = query_duckdb(
        f"""
        SELECT ts_code AS 代码,
               split_part(ts_code, '.', 1) AS code_short,
               name AS 名称, price AS 最新价, low AS 最低, high AS 最高
        FROM market_snapshot
        WHERE as_of = (SELECT MAX(as_of) FROM market_snapshot)
          AND ts_code IN ({marks})
        ORDER BY ts_code
        """,
        list(ts_codes),
        required_projections=("market_snapshot",),
    )
    if frame is None:
        return pd.DataFrame()
    return frame


def get_daily_kline(ts_code: str, days: int = 30) -> pd.DataFrame:
    """从有界 serving 日线投影读取最近 ``days`` 根。"""
    frame = query_duckdb(
        """
        SELECT trade_date AS 日期, open AS 开盘, close AS 收盘,
               high AS 最高, low AS 最低, vol AS 成交量,
               close - open AS 涨跌
        FROM daily_bar
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        [ts_code, days],
        required_projections=("daily_bar",),
    )
    if frame is None or frame.empty:
        return pd.DataFrame()
    frame = frame.sort_values("日期").reset_index(drop=True)
    frame["日期"] = frame["日期"].astype(str)
    return frame


def get_minute_kline(ts_code: str) -> pd.DataFrame:
    """从有界 serving 分时投影读取当日 1 分钟线。"""
    frame = query_duckdb(
        """
        SELECT trade_time AS 时间, open AS 开盘, close AS 收盘,
               high AS 最高, low AS 最低, vol AS 成交量
        FROM intraday_kline
        WHERE ts_code = ?
          AND CAST(trade_time AT TIME ZONE 'Asia/Shanghai' AS DATE) = CURRENT_DATE
        ORDER BY trade_time
        LIMIT 300
        """,
        [ts_code],
        required_projections=("intraday_kline",),
    )
    if frame is None or frame.empty:
        return pd.DataFrame()
    frame["时间"] = pd.to_datetime(frame["时间"])
    return frame.reset_index(drop=True)


try:
    _dashboard_serving_context = ServingPageRenderContext.open(
        serving_root_from_env()
    )
except Exception:
    _dashboard_serving_context = None

try:
    dashboard_health = query_duckdb(
        """
        SELECT * FROM dashboard_summary
        WHERE snapshot_key = 'current'
        """,
        required_projections=("dashboard_summary",),
    )
    health_row = (
        None if dashboard_health is None or dashboard_health.empty else dashboard_health.iloc[0]
    )

    # ── Header ──

    hostname = (
        str(health_row.get("host_name") or "serving-unavailable")
        if health_row is not None
        else "serving-unavailable"
    )
    col_title, col_meta = st.columns([3, 2])
    with col_title:
        st.markdown(
            "<div style='display:flex;align-items:baseline;gap:10px;'>"
            "<span style='font-size:1.5rem;font-weight:700;letter-spacing:-0.02em;'>"
            "📈 rQuant Health</span>"
            "<span style='color:#9ca3af;font-size:0.78rem;'>云端业务健康看板</span>"
            "</div>",
            unsafe_allow_html=True,
        )
    with col_meta:
        st.markdown(
            f"<div style='text-align:right;color:#9ca3af;font-size:0.72rem;line-height:1.4;'>"
            f"<span style='color:#6b7280;font-weight:500;'>{hostname}</span>"
            f" · 渲染 {datetime.now(CST).strftime('%H:%M:%S')}"
            f" · 每 {REFRESH_SECONDS}s 自动刷新"
            f"</div>",
            unsafe_allow_html=True,
        )

    today_iso = date.today().isoformat()

    # ── 总览健康条 ──

    if health_row is None:
        timers: list[dict] = []
        monitor_status = {"ActiveState": "unknown", "SubState": ""}
        daily_status = {"ActiveState": "unknown", "ExecMainStatus": "?"}
        dashboard_status = {"ActiveState": "unknown"}
    else:
        timers = [
            {
                "unit": "rquant-monitor.timer",
                "next": health_row.get("monitor_next_at"),
                "last": health_row.get("monitor_last_at"),
            },
            {
                "unit": "rquant-daily.timer",
                "next": health_row.get("daily_next_at"),
                "last": health_row.get("daily_last_at"),
            },
        ]
        monitor_status = {
            "ActiveState": str(health_row.get("monitor_state") or "unknown"),
            "SubState": str(health_row.get("monitor_substate") or ""),
        }
        daily_status = {
            "ActiveState": str(health_row.get("daily_state") or "unknown"),
            "ExecMainStatus": str(health_row.get("daily_exec_status") or "?"),
        }
        dashboard_status = {"ActiveState": str(health_row.get("dashboard_state") or "unknown")}

    monitor_active = monitor_status.get("ActiveState") == "active"
    daily_active = daily_status.get("ActiveState") in ("active", "inactive")  # idle 也算正常
    dashboard_active = dashboard_status.get("ActiveState") == "active"

    def _market_phase_now() -> str:
        """09:30-11:30 morning / 11:30-13:00 lunch / 13:00-15:00 afternoon / 其他 closed/pre"""
        now = datetime.now(CST)
        t = now.hour * 100 + now.minute
        if t < 930:
            return "pre"
        if t < 1130:
            return "morning"
        if t < 1300:
            return "lunch"
        if t < 1500:
            return "afternoon"
        return "closed"

    _MONITOR_EXPECTED_ACTIVE = {"morning", "afternoon"}

    def _badge(label: str, status: str, sub: str = "") -> str:
        """status: 'ok' (green) | 'bad' (red) | 'idle' (gray)。"""
        color = {"ok": "#16a34a", "bad": "#dc2626", "idle": "#9ca3af"}[status]
        dot = "●"
        return (
            f"<span style='display:inline-flex;align-items:center;gap:6px;"
            f"padding:4px 10px;margin-right:8px;border-radius:5px;"
            f"background:{color}14;color:{color};font-size:0.72rem;"
            f"font-weight:500;border:1px solid {color}33;'>"
            f"<span style='font-size:0.7rem;'>{dot}</span>"
            f"<b style='letter-spacing:0.02em;'>{label}</b>"
            f"<span style='color:{color}aa;'>{sub}</span></span>"
        )

    # monitor: 交易时段必须 active；非交易时段 idle 也算正常（盘前 / 午休 / 收盘后）
    _phase = _market_phase_now()
    if _phase in _MONITOR_EXPECTED_ACTIVE:
        monitor_badge_status = "ok" if monitor_active else "bad"
    else:
        monitor_badge_status = "ok" if monitor_active else "idle"

    # daily: failed 才算 bad；inactive(idle) 是常态
    daily_badge_status = "bad" if daily_status.get("ActiveState") == "failed" else "ok"

    health_html = (
        _badge(
            "monitor",
            monitor_badge_status,
            f"({monitor_status.get('SubState', '')} · {_phase})",
        )
        + _badge(
            "daily",
            daily_badge_status,
            f"({daily_status.get('SubState', '')})",
        )
        + _badge("dashboard", "ok" if dashboard_active else "bad", "")
    )
    st.markdown(health_html, unsafe_allow_html=True)

    # DuckDB 写锁检测（daily 流水线 ingest 时会持有写锁，dashboard 让位）
    db_locked_pid = db_lock_holder()
    if db_locked_pid:
        st.warning(
            f"⏳ **数据库被流水线写锁占用**（PID `{db_locked_pid}`）— "
            f"大概率是 daily 17:00 触发后 ingest 还在跑（最长 ~45 分钟，含 3 次重试）。"
            f"业务数据相关 section 暂时降级，流水线完成后自动恢复。"
        )

    st.divider()

    # ── Section 1: systemd 服务详情 ──

    st.markdown("## 🟢 服务调度")
    col_m, col_d = st.columns(2)

    with col_m, st.container(border=True):
        st.markdown("### rquant-monitor")
        state = monitor_status.get("ActiveState", "unknown")
        sub = monitor_status.get("SubState", "")

        timer = find_timer(timers, "rquant-monitor.timer")
        next_str = fmt_us_timestamp(timer.get("next") if timer else None)
        last_str = fmt_us_timestamp(timer.get("last") if timer else None)

        c1, c2 = st.columns(2)
        c1.metric(
            "当前状态",
            f"{state}",
            delta=sub if state == "active" else None,
        )
        next_delta = time_diff_human(int(timer.get("next") or 0)) if timer else None
        c2.metric("下次触发", next_str, delta=next_delta)
        st.caption(f"上次触发: {last_str}")

    with col_d, st.container(border=True):
        st.markdown("### rquant-daily")
        state = daily_status.get("ActiveState", "unknown")

        timer = find_timer(timers, "rquant-daily.timer")
        next_str = fmt_us_timestamp(timer.get("next") if timer else None)
        last_str = fmt_us_timestamp(timer.get("last") if timer else None)
        exec_status = daily_status.get("ExecMainStatus", "?")

        c1, c2 = st.columns(2)
        c1.metric(
            "当前状态",
            "Idle" if state == "inactive" else state,
            delta=f"上次 status={exec_status}",
        )
        next_delta = time_diff_human(int(timer.get("next") or 0)) if timer else None
        c2.metric("下次触发", next_str, delta=next_delta)
        st.caption(f"上次触发: {last_str}")

    # ── Section 2: 数据新鲜度 ──

    st.markdown("## 🗓️ 数据新鲜度")
    if db_locked_pid:
        st.info("⏳ 等流水线完成")
    else:
        freshness = (
            None
            if dashboard_health is None
            else dashboard_health.rename(columns={"monitor_event_rows": "event_rows"})
        )
        if freshness is None:
            st.info("⏳ 等流水线完成")
        else:
            row = freshness.iloc[0]
            fcols = st.columns(4)
            fcols[0].metric("最新 daily_bar", row["latest_daily_bar"] or "—")
            fcols[1].metric("最新 screen_result", row["latest_screen"] or "—")
            fcols[2].metric("daily_bar 总行数", f"{int(row['daily_bar_rows']):,}")
            fcols[3].metric("monitor_event 总行数", f"{int(row['event_rows']):,}")

    # ── Section 3: Watchlist ──

    st.markdown("## 📋 当前 Watchlist")
    if db_locked_pid:
        st.info("⏳ 等流水线完成")
    else:
        col_p2, col_p1 = st.columns([1, 1])

        with col_p2, st.container(border=True):
            p2 = query_duckdb(
                """
                    SELECT pw.ts_code AS 代码, sb.name AS 名称,
                           strftime(pw.entry_date, '%m-%d') AS 入池,
                           pw.body_lower AS bodyBtm, pw.body_upper AS bodyTop,
                           pw.stop_strong AS 强止, pw.stop_weak AS 弱止,
                           CASE WHEN rb.list_label IS NOT NULL
                                THEN '⚠️ ' || rb.list_label
                                ELSE '' END AS 黑名单
                    FROM pool2_watch pw
                    LEFT JOIN stock_basic sb ON pw.ts_code = sb.ts_code
                    LEFT JOIN risk_blacklist rb
                      ON pw.ts_code = rb.ts_code
                     AND rb.expires_at >= CURRENT_DATE
                    WHERE pw.status = 'active'
                    ORDER BY pw.entry_date DESC
                    """,
                required_projections=("pool2_watch", "stock_basic", "risk_blacklist"),
            )
            count = 0 if p2 is None else len(p2)
            bl_count = 0 if p2 is None else int((p2["黑名单"] != "").sum())
            bl_suffix = f" · ⚠️ {bl_count}" if bl_count else ""
            st.markdown(f"### Pool 2 active ({count} 只{bl_suffix})")
            if p2 is None:
                st.info("⏳ 数据暂不可读")
            elif p2.empty:
                st.info("Pool 2 暂无 active 标的")
            else:
                st.dataframe(p2, hide_index=True, use_container_width=True)

        with col_p1, st.container(border=True):
            p1 = query_duckdb(
                """
                    SELECT ts_code AS 代码, name AS 名称, close AS 收盘, pct_chg AS 涨跌
                    FROM screen_result
                    WHERE strftime(trade_date, '%Y-%m-%d') = ?
                      AND preset_name = 'n-shape-pool1'
                    ORDER BY ts_code
                    """,
                [today_iso],
                required_projections=("screen_result",),
            )
            count = 0 if p1 is None else len(p1)
            st.markdown(f"### Pool 1 候选 today ({count} 只)")
            if p1 is None:
                st.info("⏳ 数据暂不可读")
            elif p1.empty:
                st.info("当日暂无 Pool 1 命中")
            else:
                st.dataframe(p1, hide_index=True, use_container_width=True)

    # ── Section 4: 今日触发事件 ──

    st.markdown("## 📍 今日触发事件")
    if db_locked_pid:
        st.info("⏳ 等流水线完成")
    else:
        events = query_duckdb(
            """
            SELECT strftime(trigger_time, '%H:%M:%S') AS 时间,
                   ts_code AS 代码, level AS 档位,
                   trigger_price AS 触发价, level_price AS 档位价,
                   trigger_type AS 类型, pool
            FROM monitor_event
            WHERE strftime(trade_date, '%Y-%m-%d') = ?
            ORDER BY trigger_time DESC
            """,
            [today_iso],
            required_projections=("monitor_event",),
        )
        if events is None:
            st.info("⏳ 数据暂不可读")
        elif events.empty:
            st.info("当日暂无触发事件")
        else:
            by_level = events["档位"].value_counts().to_dict()
            cols = st.columns(6)
            cols[0].metric("总触发", len(events))
            for i, (k, v) in enumerate(
                [("40", "40%"), ("30", "30%"), ("20", "20%"), ("strong", "强止"), ("weak", "弱止")]
            ):
                cols[i + 1].metric(v, by_level.get(k, 0))
            st.dataframe(events, hide_index=True, use_container_width=True)

    # ── Section 5: 7 日 Pool 1 趋势 ──

    st.markdown("## 📊 最近 7 日 Pool 1 命中数")
    if db_locked_pid:
        st.info("⏳ 等流水线完成")
        trend = None
    else:
        trend = query_duckdb(
            """
            SELECT strftime(trade_date, '%Y-%m-%d') AS date, COUNT(*) AS hits
            FROM screen_result
            WHERE preset_name = 'n-shape-pool1'
              AND trade_date >= (CAST(? AS DATE) - INTERVAL '7 days')
            GROUP BY trade_date
            ORDER BY trade_date
            """,
            [today_iso],
            required_projections=("screen_result",),
        )
    if trend is None:
        pass  # 已显示等待提示
    elif trend.empty:
        st.info("最近 7 日无 Pool 1 数据")
    else:
        max_hits = max(int(trend["hits"].max()), 1)
        y_max = int(max_hits * 1.2) + 1
        chart = (
            alt.Chart(trend)
            .mark_line(
                point=alt.OverlayMarkDef(filled=True, size=80, color="#3b82f6"),
                color="#3b82f6",
                strokeWidth=2.5,
            )
            .encode(
                x=alt.X("date:O", title=None, axis=alt.Axis(labelAngle=-30)),
                y=alt.Y(
                    "hits:Q",
                    title="命中数",
                    scale=alt.Scale(domain=[0, y_max]),
                    axis=alt.Axis(grid=True, gridDash=[2, 4]),
                ),
                tooltip=[
                    alt.Tooltip("date:O", title="日期"),
                    alt.Tooltip("hits:Q", title="命中数"),
                ],
            )
            .properties(height=200)
        )
        text = (
            alt.Chart(trend)
            .mark_text(dy=-12, fontSize=11, color="#3b82f6")
            .encode(x=alt.X("date:O"), y=alt.Y("hits:Q"), text=alt.Text("hits:Q"))
        )
        st.altair_chart(chart + text, use_container_width=True)

    # ── Section 6: 分钟策略实验室 ──

    st.markdown("## 🧪 分钟策略实验室")
    if db_locked_pid:
        st.info("⏳ 等流水线完成")
    else:
        bounds_min, bounds_max = screen_result_date_bounds()
        overview = minute_data_overview()
        if bounds_min is None or bounds_max is None:
            st.info("暂无 Pool 1 日期范围")
        else:
            default_end = min(bounds_max, date(2026, 6, 22))
            default_start = max(bounds_min, default_end - timedelta(days=21))
            with st.container(border=True):
                c1, c2, c3, c4, c5 = st.columns([1.0, 1.0, 0.9, 1.8, 2.2])
                replay_start = c1.date_input(
                    "开始日期",
                    value=default_start,
                    min_value=bounds_min,
                    max_value=bounds_max,
                    key="replay_start",
                )
                replay_end = c2.date_input(
                    "结束日期",
                    value=default_end,
                    min_value=bounds_min,
                    max_value=bounds_max,
                    key="replay_end",
                )
                replay_hold = c3.number_input(
                    "持有交易日",
                    min_value=1,
                    max_value=10,
                    value=1,
                    step=1,
                    key="replay_hold",
                )
                mode_labels = {
                    "first_break": "第一次突破",
                    "break_retest": "突破回踩确认",
                    "late_confirm": "10:30后确认",
                    "vwap_confirm": "VWAP确认",
                    "amount_surge": "成交额突增",
                }
                selected_labels = c4.multiselect(
                    "入场模式",
                    list(mode_labels.values()),
                    default=list(mode_labels.values()),
                    key="replay_modes",
                )
                selected_modes = tuple(
                    key for key, label in mode_labels.items() if label in selected_labels
                )
                variant_labels = {
                    "baseline": "Baseline",
                    "vp_risk_only": "90日价量动态风控",
                    "vp_90": "90日价量过滤+风控",
                }
                selected_variant_labels = c5.multiselect(
                    "风控版本",
                    list(variant_labels.values()),
                    default=list(variant_labels.values()),
                    key="replay_profile_variants",
                )
                selected_variants = tuple(
                    key for key, label in variant_labels.items() if label in selected_variant_labels
                )

                if not selected_modes or not selected_variants:
                    st.warning("至少选择一个入场模式和一个风控版本")
                else:
                    try:
                        summary, trades, candidates_count = strategy_compare_cached(
                            replay_start.isoformat(),
                            replay_end.isoformat(),
                            selected_modes,
                            selected_variants,
                            int(replay_hold),
                        )
                        m1, m2, m3, m4 = st.columns(4)
                        total_trades = int(summary["trades"].sum()) if not summary.empty else 0
                        m1.metric("候选数", candidates_count)
                        m2.metric("触发交易", total_trades)
                        if not trades.empty:
                            m3.metric("平均收益", f"{trades['ret_pct'].mean():.2f}%")
                            m4.metric("胜率", f"{(trades['ret_pct'] > 0).mean() * 100:.1f}%")
                        else:
                            m3.metric("平均收益", "—")
                            m4.metric("胜率", "—")

                        summary_display = summary.copy()
                        summary_display["entry_mode"] = summary_display["entry_mode"].map(
                            mode_labels
                        )
                        summary_display["profile_variant"] = summary_display["profile_variant"].map(
                            variant_labels
                        )
                        summary_display = summary_display.rename(
                            columns={
                                "entry_mode": "入场模式",
                                "profile_variant": "风控版本",
                                "candidates": "候选",
                                "trades": "交易",
                                "trigger_rate_pct": "触发率%",
                                "mean_ret_pct": "平均收益%",
                                "median_ret_pct": "中位收益%",
                                "win_rate_pct": "胜率%",
                                "best_ret_pct": "最佳%",
                                "worst_ret_pct": "最差%",
                                "gap_stop_rate_pct": "跳空止损%",
                            }
                        )
                        st.dataframe(summary_display, hide_index=True, use_container_width=True)

                        if not summary.empty:
                            chart_df = summary.copy()
                            chart_df["entry_mode"] = chart_df["entry_mode"].map(mode_labels)
                            chart_df["profile_variant"] = chart_df["profile_variant"].map(
                                variant_labels
                            )
                            chart_df["strategy_label"] = (
                                chart_df["entry_mode"] + " / " + chart_df["profile_variant"]
                            )
                            compare_chart = (
                                alt.Chart(chart_df)
                                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                                .encode(
                                    x=alt.X("strategy_label:N", title=None, sort=None),
                                    y=alt.Y("mean_ret_pct:Q", title="平均收益%"),
                                    color=alt.condition(
                                        alt.datum.mean_ret_pct >= 0,
                                        alt.value("#0f766e"),
                                        alt.value("#b91c1c"),
                                    ),
                                    tooltip=[
                                        alt.Tooltip("entry_mode:N", title="模式"),
                                        alt.Tooltip("profile_variant:N", title="风控"),
                                        alt.Tooltip("trades:Q", title="交易"),
                                        alt.Tooltip("mean_ret_pct:Q", title="平均收益%"),
                                        alt.Tooltip("win_rate_pct:Q", title="胜率%"),
                                        alt.Tooltip("gap_stop_rate_pct:Q", title="跳空止损%"),
                                    ],
                                )
                                .properties(height=180)
                            )
                            st.altair_chart(compare_chart, use_container_width=True)

                        tab_trades, tab_exits, tab_data = st.tabs(
                            ["交易明细", "退出原因", "分钟数据"]
                        )
                        with tab_trades:
                            if trades.empty:
                                st.info("所选区间没有触发交易")
                            else:
                                display = trades.copy()
                                display["entry_mode"] = display["entry_mode"].map(mode_labels)
                                display["profile_variant"] = display["profile_variant"].map(
                                    variant_labels
                                )
                                cols = [
                                    "entry_mode",
                                    "profile_variant",
                                    "signal_date",
                                    "ts_code",
                                    "name",
                                    "entry_time",
                                    "entry_price_raw",
                                    "entry_price",
                                    "stop_loss_basis",
                                    "take_profit_basis",
                                    "volume_profile_lookbacks",
                                    "volume_profile_rr",
                                    "exit_time",
                                    "exit_price",
                                    "exit_reason",
                                    "ret_pct",
                                ]
                                st.dataframe(
                                    display[[c for c in cols if c in display.columns]],
                                    hide_index=True,
                                    use_container_width=True,
                                )
                        with tab_exits:
                            if trades.empty:
                                st.info("暂无退出分布")
                            else:
                                exits = (
                                    trades.groupby(["entry_mode", "profile_variant", "exit_reason"])
                                    .agg(n=("ts_code", "count"), mean_ret=("ret_pct", "mean"))
                                    .reset_index()
                                )
                                exits["entry_mode"] = exits["entry_mode"].map(mode_labels)
                                exits["profile_variant"] = exits["profile_variant"].map(
                                    variant_labels
                                )
                                st.dataframe(exits, hide_index=True, use_container_width=True)
                        with tab_data:
                            if overview is None or overview.empty:
                                st.info("暂无分钟线数据")
                            else:
                                row = overview.iloc[0]
                                d1, d2, d3 = st.columns(3)
                                d1.metric("分钟线行数", int(row["rows_count"]))
                                d2.metric("覆盖股票", int(row["codes_count"]))
                                d3.metric(
                                    "时间范围",
                                    f"{row['min_time']:%m-%d} → {row['max_time']:%m-%d}"
                                    if pd.notna(row["min_time"])
                                    else "—",
                                )
                                st.caption(
                                    "90 日价量分布已接入入场过滤、止损、止盈和移动止盈参数；"
                                    "更完整的参数对比建议使用独立策略实验室页面。"
                                )
                    except Exception as e:
                        st.error(f"分钟策略对比失败: {e}")

    # ── Section 7: 通知通道健康 ──

    st.markdown("## 📨 通知通道")
    last24h = query_duckdb(
        """
        SELECT channel, status
        FROM deliveries
        WHERE updated_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        ORDER BY updated_at DESC
        LIMIT 500
        """
    )
    if last24h is None:
        st.info("通知 serving 数据暂不可用")
    elif last24h.empty:
        st.info("最近 24h 无推送记录")
    else:
        last24h = last24h.assign(success=last24h["status"].eq("delivered"))
        rate = (
            last24h.groupby("channel")
            .agg(
                total=("success", "size"),
                ok=("success", "sum"),
            )
            .reset_index()
        )
        ncols = st.columns(max(len(rate), 2))
        for i, r in rate.iterrows():
            success_rate = r["ok"] / r["total"] * 100 if r["total"] else 0
            color = "normal" if success_rate >= 95 else "inverse"
            ncols[i].metric(
                f"{r['channel']} (24h)",
                f"{int(r['ok'])}/{int(r['total'])}",
                delta=f"{success_rate:.0f}% 成功",
                delta_color=color,
            )

    recent = query_duckdb(
        """
        SELECT updated_at, channel, recipient_id, status, attempt_count, last_error
        FROM deliveries
        ORDER BY updated_at DESC, outbox_id
        LIMIT 30
        """
    )
    if recent is not None and not recent.empty:
        display = recent.copy()
        display.columns = ["时间", "通道", "目标", "状态", "尝试次数", "最后错误"]
        with st.expander(f"📜 最近 {len(display)} 条推送", expanded=False):
            st.dataframe(display, hide_index=True, use_container_width=True)

    # ── Section 7: 本地 sync ──

    st.markdown("## ☁️ 云端 Backup Snapshot")
    if health_row is not None and pd.notna(health_row.get("backup_snapshot_at")):
        snap_dt = pd.Timestamp(health_row["backup_snapshot_at"]).to_pydatetime()
        snap_dt = snap_dt.replace(tzinfo=UTC) if snap_dt.tzinfo is None else snap_dt
        src_mb = float(health_row.get("backup_source_bytes") or 0) / 1024 / 1024
        gz_mb = float(health_row.get("backup_compressed_bytes") or 0) / 1024 / 1024
        delta = datetime.now(UTC) - snap_dt.astimezone(UTC)
        bcols = st.columns(3)
        bcols[0].metric("snapshot 大小", f"{src_mb:.1f} MB")
        bcols[1].metric("压缩后", f"{gz_mb:.1f} MB")
        bcols[2].metric(
            "最后 snapshot",
            snap_dt.astimezone(CST).strftime("%m-%d %H:%M:%S"),
            delta=f"{int(delta.total_seconds() / 60)} 分钟前",
            delta_color="inverse" if delta > timedelta(hours=1) else "normal",
        )
    else:
        st.info("snapshot serving 摘要暂不可用")

    st.markdown("## 💾 客户端 API 拉取")
    if health_row is not None and pd.notna(health_row.get("backup_last_download_at")):
        download_at = pd.Timestamp(health_row["backup_last_download_at"]).to_pydatetime()
        download_at = download_at.replace(tzinfo=UTC) if download_at.tzinfo is None else download_at
        delta = datetime.now(UTC) - download_at.astimezone(UTC)
        scols = st.columns(3)
        scols[0].metric("客户端 IP", str(health_row.get("backup_last_download_ip") or "—"))
        download_mb = float(health_row.get("backup_last_download_bytes") or 0) / 1024 / 1024
        scols[1].metric("拉取大小", f"{download_mb:.1f} MB")
        scols[2].metric(
            "上次拉取",
            download_at.astimezone(CST).strftime("%m-%d %H:%M:%S"),
            delta=f"{int(delta.total_seconds() / 60)} 分钟前",
            delta_color="inverse" if delta > timedelta(hours=1) else "normal",
        )
    else:
        st.info("客户端拉取 serving 摘要暂不可用")

    # ── Section 8: Pool 2 实时价位 ──

    st.markdown("## ⚡ Pool 2 实时价位 vs 档位")
    if db_locked_pid:
        st.info("⏳ 等流水线完成")
    else:
        try:
            p2_active = query_duckdb(
                """
                SELECT pw.ts_code, sb.name,
                       pw.body_upper, pw.body_lower,
                       pw.level_40, pw.level_30, pw.level_20,
                       pw.stop_strong, pw.stop_weak,
                       rb.list_label AS blacklist_label
                FROM pool2_watch pw
                LEFT JOIN stock_basic sb ON pw.ts_code = sb.ts_code
                LEFT JOIN risk_blacklist rb
                  ON pw.ts_code = rb.ts_code
                 AND rb.expires_at >= CURRENT_DATE
                WHERE pw.status = 'active'
                """,
                required_projections=("pool2_watch", "stock_basic", "risk_blacklist"),
            )
            if p2_active is None:
                st.info("⏳ 数据暂不可读")
            elif p2_active.empty:
                st.info("Pool 2 无 active 标的")
            else:
                ts_codes_tuple = tuple(p2_active["ts_code"].tolist())
                prices = get_realtime_quotes_batch(ts_codes_tuple)

                rows = []
                for _, p2 in p2_active.iterrows():
                    code_short = p2["ts_code"].split(".")[0]
                    price_row = prices[prices["code_short"] == code_short]
                    if price_row.empty:
                        continue
                    price = float(price_row.iloc[0]["最新价"])
                    bl_label = p2.get("blacklist_label")
                    rows.append(
                        {
                            "代码": p2["ts_code"],
                            "名称": p2.get("name") or "",
                            "黑名单": f"⚠️ {bl_label}" if bl_label else "",
                            "现价": round(price, 2),
                            "bodyTop": round(p2["body_upper"], 2),
                            "40档": round(p2["level_40"], 2),
                            "30档": round(p2["level_30"], 2),
                            "20档": round(p2["level_20"], 2),
                            "bodyBtm": round(p2["body_lower"], 2),
                            "强止": round(p2["stop_strong"], 2),
                            "弱止": round(p2["stop_weak"], 2),
                            "距40": round(price - p2["level_40"], 2),
                            "距强止": round(price - p2["stop_strong"], 2),
                        }
                    )
                if rows:
                    df = pd.DataFrame(rows)
                    num_cols = [
                        "现价",
                        "bodyTop",
                        "40档",
                        "30档",
                        "20档",
                        "bodyBtm",
                        "强止",
                        "弱止",
                        "距40",
                        "距强止",
                    ]
                    col_config = {c: st.column_config.NumberColumn(format="%.2f") for c in num_cols}

                    event = st.dataframe(
                        df,
                        hide_index=True,
                        use_container_width=True,
                        column_config=col_config,
                        on_select="rerun",
                        selection_mode="single-row",
                    )
                    st.caption(
                        "💡 点击表格中某行查看该标的的日 K + 分时 / "
                        "「距档位」红=已穿过、黄=接近(<0.5)、绿=安全"
                    )

                    # 选中行 → 显示详情
                    if event.selection.rows:
                        sel_idx = event.selection.rows[0]
                        sel_code = df.iloc[sel_idx]["代码"]
                        sel_name = df.iloc[sel_idx]["名称"] or ""
                        sel_levels = df.iloc[sel_idx]

                        st.markdown(f"### 📈 {sel_code} {sel_name} 详情")

                        detail_col_k, detail_col_m = st.columns([1, 1])

                        # 日 K（最近 30 天）
                        with detail_col_k, st.container(border=True):
                            st.markdown("**最近 30 日 K 线**")
                            try:
                                kline = get_daily_kline(sel_code, days=30)
                                if kline.empty:
                                    st.info("暂无日 K 数据")
                                else:
                                    base = alt.Chart(kline).encode(
                                        x=alt.X(
                                            "日期:O",
                                            axis=alt.Axis(labelAngle=-30, labelFontSize=9),
                                            title=None,
                                        )
                                    )
                                    rule = base.mark_rule().encode(
                                        y=alt.Y(
                                            "最低:Q",
                                            scale=alt.Scale(zero=False),
                                            title="价格",
                                        ),
                                        y2="最高:Q",
                                        color=alt.condition(
                                            "datum.涨跌 >= 0",
                                            alt.value("#dc2626"),
                                            alt.value("#16a34a"),
                                        ),
                                    )
                                    bar = base.mark_bar(size=6).encode(
                                        y="开盘:Q",
                                        y2="收盘:Q",
                                        color=alt.condition(
                                            "datum.涨跌 >= 0",
                                            alt.value("#dc2626"),
                                            alt.value("#16a34a"),
                                        ),
                                        tooltip=[
                                            "日期",
                                            "开盘",
                                            "收盘",
                                            "最高",
                                            "最低",
                                            "成交量",
                                        ],
                                    )

                                    # 各档位横线
                                    levels_data = pd.DataFrame(
                                        [
                                            {"label": "40档", "value": float(sel_levels["40档"])},
                                            {"label": "30档", "value": float(sel_levels["30档"])},
                                            {"label": "20档", "value": float(sel_levels["20档"])},
                                            {"label": "强止", "value": float(sel_levels["强止"])},
                                            {"label": "弱止", "value": float(sel_levels["弱止"])},
                                        ]
                                    )
                                    level_lines = (
                                        alt.Chart(levels_data)
                                        .mark_rule(strokeDash=[3, 3], strokeWidth=1)
                                        .encode(
                                            y="value:Q",
                                            color=alt.Color(
                                                "label:N",
                                                scale=alt.Scale(
                                                    domain=["40档", "30档", "20档", "强止", "弱止"],
                                                    range=[
                                                        "#fbbf24",
                                                        "#f59e0b",
                                                        "#dc2626",
                                                        "#7c3aed",
                                                        "#1e40af",
                                                    ],
                                                ),
                                                legend=alt.Legend(orient="top", title=None),
                                            ),
                                        )
                                    )
                                    chart_k = (rule + bar + level_lines).properties(height=260)
                                    st.altair_chart(chart_k, use_container_width=True)
                            except Exception as e:
                                st.error(f"日 K 加载失败: {e}")

                        # 当日分时
                        with detail_col_m, st.container(border=True):
                            st.markdown("**当日分时**")
                            try:
                                minute = get_minute_kline(sel_code)
                                if minute.empty:
                                    st.info("当日分时暂无数据")
                                else:
                                    # ordinal x 跳过午休 11:30-13:00 空段
                                    minute = minute.copy()
                                    minute["时间_str"] = minute["时间"].dt.strftime("%H:%M")

                                    # 11:30 之后第一个数据点位置 → 画虚线分隔上下午
                                    morning_end = minute[minute["时间"].dt.hour < 12]
                                    afternoon_start_label = (
                                        minute.iloc[len(morning_end)]["时间_str"]
                                        if len(morning_end) < len(minute)
                                        else None
                                    )

                                    base_m = alt.Chart(minute).encode(
                                        x=alt.X(
                                            "时间_str:O",
                                            sort=None,
                                            title=None,
                                            axis=alt.Axis(
                                                labelOverlap="greedy",
                                                labelFontSize=9,
                                                tickCount=6,
                                            ),
                                        ),
                                        y=alt.Y(
                                            "收盘:Q",
                                            title="价格",
                                            scale=alt.Scale(zero=False),
                                        ),
                                    )
                                    line_m = base_m.mark_line(
                                        color="#3b82f6", strokeWidth=1.5
                                    ).encode(
                                        tooltip=[
                                            alt.Tooltip("时间_str:N", title="时间"),
                                            "开盘",
                                            "收盘",
                                            "最高",
                                            "最低",
                                        ],
                                    )

                                    layers = [line_m]
                                    if afternoon_start_label:
                                        sep = (
                                            alt.Chart(
                                                pd.DataFrame({"时间_str": [afternoon_start_label]})
                                            )
                                            .mark_rule(
                                                strokeDash=[3, 3],
                                                stroke="#9ca3af",
                                                strokeWidth=1,
                                            )
                                            .encode(x="时间_str:O")
                                        )
                                        layers.append(sep)

                                    chart_m = alt.layer(*layers).properties(height=260)
                                    st.altair_chart(chart_m, use_container_width=True)
                                    if afternoon_start_label:
                                        st.caption("💡 虚线 = 午休分隔（11:30 → 13:00 跳过）")
                            except Exception as e:
                                st.error(f"分时加载失败: {e}")
                else:
                    st.warning("未匹配到任何 Pool 2 标的的实时价格")
        except Exception as e:
            st.error(f"实时价位获取失败: {e}")

    # ── Section 9: 风险黑名单状态 ──

    st.markdown("## 🛑 风险黑名单状态")
    if db_locked_pid:
        st.info("⏳ 等流水线完成")
    else:
        bl_summary = query_duckdb(
            """
            SELECT list_label,
                   COUNT(*) AS n_total,
                   MAX(imported_at) AS imported_at,
                   MAX(expires_at)  AS expires_at
            FROM risk_blacklist
            GROUP BY list_label
            ORDER BY MAX(expires_at) DESC
            """,
            required_projections=("risk_blacklist",),
        )
        if bl_summary is None:
            st.info("⏳ 数据暂不可读")
        elif bl_summary.empty:
            st.info("暂无黑名单。导入命令：`rquant blacklist import ~/Downloads/风险控制名单.pdf`")
        else:
            today_d = date.today()
            cols = st.columns(min(len(bl_summary), 3) or 1)
            for i, (_, r) in enumerate(bl_summary.iterrows()):
                with cols[i % len(cols)], st.container(border=True):
                    label = r["list_label"]
                    expires = r["expires_at"]
                    if hasattr(expires, "date"):
                        expires = expires.date()
                    days_left = (expires - today_d).days
                    expired = days_left < 0
                    near = 0 <= days_left <= 30

                    badge_color = "#dc2626" if expired else ("#f59e0b" if near else "#16a34a")
                    badge_text = (
                        f"已过期 {-days_left}d"
                        if expired
                        else (f"剩 {days_left}d" if near else f"剩 {days_left}d")
                    )

                    st.markdown(
                        f"<div style='display:flex;align-items:baseline;"
                        f"justify-content:space-between;'>"
                        f"<span style='font-size:0.95rem;font-weight:600;'>"
                        f"{label}</span>"
                        f"<span style='padding:2px 8px;border-radius:4px;"
                        f"background:{badge_color}1a;color:{badge_color};"
                        f"font-size:0.7rem;font-weight:500;'>{badge_text}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                    st.markdown(
                        f"<div style='color:#6b7280;font-size:0.78rem;"
                        f"margin-top:4px;'>"
                        f"{r['n_total']} 只 · 导入 {r['imported_at']} → "
                        f"失效 {expires}"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                    if expired:
                        st.markdown(
                            f"<div style='color:#dc2626;font-size:0.75rem;"
                            f"margin-top:6px;'>"
                            f"⚠️ 名单已过期，过滤仍在生效。请上传新版后用 "
                            f"<code>rquant blacklist import &lt;pdf&gt; --label "
                            f"{label}</code> 刷新。"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
                    elif near:
                        st.markdown(
                            "<div style='color:#f59e0b;font-size:0.75rem;"
                            "margin-top:6px;'>"
                            "⏰ 即将到期，建议尽快上传新版"
                            "</div>",
                            unsafe_allow_html=True,
                        )

    st.divider()
    st.caption(
        f"Serving: {serving_root_from_env()}  ·  "
        f"Refresh: {REFRESH_SECONDS}s  ·  "
        f"Last render: {datetime.now(CST).strftime('%Y-%m-%d %H:%M:%S')}"
    )

finally:
    if _dashboard_serving_context is not None:
        _dashboard_serving_context.close()
