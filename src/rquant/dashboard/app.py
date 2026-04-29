"""rQuant Health Dashboard。

启动方式：
    streamlit run src/rquant/dashboard/app.py --server.port 8501 \\
        --server.address 0.0.0.0 --server.headless true
"""

from __future__ import annotations

import json
import socket
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from rquant.config import settings

REFRESH_SECONDS = 30
CST = timezone(timedelta(hours=8))

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

# 简单 CSS 美化
st.markdown(
    """
    <style>
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    [data-testid="stMetricValue"] { font-size: 1.5rem; }
    [data-testid="stMetricLabel"] { font-size: 0.85rem; color: #666; }
    h2 { margin-top: 1rem; padding-top: 0.5rem; border-top: 1px solid #eee; }
    h3 { font-size: 1.1rem; margin-bottom: 0.5rem; }
    [data-testid="stDataFrame"] { font-size: 0.85rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── 工具函数 ──


@st.cache_data(ttl=20)
def query_duckdb(sql: str, params: list | None = None) -> pd.DataFrame:
    conn = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        if params:
            return conn.execute(sql, params).fetchdf()
        return conn.execute(sql).fetchdf()
    finally:
        conn.close()


@st.cache_data(ttl=10)
def systemd_show(unit: str) -> dict[str, str]:
    try:
        result = subprocess.run(
            [
                "systemctl",
                "show",
                unit,
                "--property=ActiveState,SubState,ActiveEnterTimestamp,"
                "ExecMainStartTimestamp,ExecMainStatus",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return dict(
            line.split("=", 1)
            for line in result.stdout.strip().split("\n")
            if "=" in line
        )
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=10)
def systemd_list_timers() -> list[dict]:
    try:
        result = subprocess.run(
            ["systemctl", "list-timers", "--all", "--output=json", "--no-pager"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return json.loads(result.stdout) if result.stdout.strip() else []
    except Exception:
        return []


def find_timer(timers: list[dict], unit_name: str) -> dict | None:
    for t in timers:
        if t.get("unit") == unit_name:
            return t
    return None


def fmt_us_timestamp(us: int | str | None) -> str:
    """systemctl 输出的微秒时间戳转 UTC+8 字符串。"""
    if not us:
        return "—"
    try:
        us_int = int(us)
        if us_int <= 0:
            return "—"
        dt = datetime.fromtimestamp(us_int / 1_000_000, tz=CST)
        return dt.strftime("%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return str(us)


def time_diff_human(target_us: int) -> str:
    """目标微秒时间戳距现在多久（人类可读）。"""
    if not target_us:
        return ""
    try:
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


@st.cache_data(ttl=30)
def get_realtime_prices_sina() -> pd.DataFrame:
    import akshare as ak

    df = ak.stock_zh_a_spot()
    df = df[["代码", "名称", "最新价", "最低", "最高"]].copy()
    df["code_short"] = df["代码"].str[-6:]
    return df


# ── Header ──


hostname = socket.gethostname()
col_title, col_meta = st.columns([3, 1])
with col_title:
    st.markdown("# 📈 rQuant Health")
with col_meta:
    st.markdown(
        f"<div style='text-align:right;color:#888;font-size:0.85rem;'>"
        f"<b>{hostname}</b><br/>"
        f"刷新于 {datetime.now(CST).strftime('%H:%M:%S')} · 每 {REFRESH_SECONDS}s 自动刷新"
        f"</div>",
        unsafe_allow_html=True,
    )

today_iso = date.today().isoformat()


# ── 总览健康条 ──


timers = systemd_list_timers()
monitor_status = systemd_show("rquant-monitor.service")
daily_status = systemd_show("rquant-daily.service")
dashboard_status = systemd_show("rquant-dashboard.service")

monitor_active = monitor_status.get("ActiveState") == "active"
daily_active = daily_status.get("ActiveState") in ("active", "inactive")  # idle 也算正常
dashboard_active = dashboard_status.get("ActiveState") == "active"


def _badge(label: str, ok: bool, sub: str = "") -> str:
    color = "#16a34a" if ok else "#dc2626"
    icon = "✅" if ok else "❌"
    return (
        f"<span style='display:inline-block;padding:6px 14px;margin-right:10px;"
        f"border-radius:8px;background:{color}1a;color:{color};font-size:0.9rem;"
        f"border:1px solid {color}40;'>"
        f"{icon} <b>{label}</b> {sub}</span>"
    )


health_html = (
    _badge(
        "monitor",
        monitor_active,
        f"({monitor_status.get('SubState', '')})",
    )
    + _badge(
        "daily",
        daily_status.get("ActiveState") != "failed",
        f"(上次 status={daily_status.get('ExecMainStatus', '?')})",
    )
    + _badge("dashboard", dashboard_active, "")
)
st.markdown(health_html, unsafe_allow_html=True)
st.divider()


# ── Section 1: systemd 服务详情 ──


st.markdown("## 🟢 服务调度")
col_m, col_d = st.columns(2)

with col_m:
    with st.container(border=True):
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
        c2.metric("下次触发", next_str, delta=time_diff_human(int(timer.get("next") or 0)) if timer else None)
        st.caption(f"上次触发: {last_str}")

with col_d:
    with st.container(border=True):
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
        c2.metric("下次触发", next_str, delta=time_diff_human(int(timer.get("next") or 0)) if timer else None)
        st.caption(f"上次触发: {last_str}")


# ── Section 2: 数据新鲜度 ──


st.markdown("## 🗓️ 数据新鲜度")
freshness = query_duckdb(
    """
    SELECT
        (SELECT strftime(MAX(trade_date), '%Y-%m-%d') FROM daily_bar) AS latest_daily_bar,
        (SELECT strftime(MAX(trade_date), '%Y-%m-%d') FROM screen_result) AS latest_screen,
        (SELECT COUNT(*) FROM daily_bar) AS daily_bar_rows,
        (SELECT COUNT(*) FROM monitor_event) AS event_rows
    """
)
row = freshness.iloc[0]
fcols = st.columns(4)
fcols[0].metric("最新 daily_bar", row["latest_daily_bar"] or "—")
fcols[1].metric("最新 screen_result", row["latest_screen"] or "—")
fcols[2].metric("daily_bar 总行数", f"{int(row['daily_bar_rows']):,}")
fcols[3].metric("monitor_event 总行数", f"{int(row['event_rows']):,}")


# ── Section 3: Watchlist ──


st.markdown("## 📋 当前 Watchlist")
col_p2, col_p1 = st.columns([1, 1])

with col_p2:
    with st.container(border=True):
        p2 = query_duckdb(
            """
            SELECT pw.ts_code AS 代码, sb.name AS 名称,
                   strftime(pw.entry_date, '%m-%d') AS 入池,
                   pw.body_lower AS bodyBtm, pw.body_upper AS bodyTop,
                   pw.stop_strong AS 强止, pw.stop_weak AS 弱止
            FROM pool2_watch pw
            LEFT JOIN stock_basic sb ON pw.ts_code = sb.ts_code
            WHERE pw.status = 'active'
            ORDER BY pw.entry_date DESC
            """
        )
        st.markdown(f"### Pool 2 active ({len(p2)} 只)")
        if p2.empty:
            st.info("Pool 2 暂无 active 标的")
        else:
            st.dataframe(p2, hide_index=True, use_container_width=True)

with col_p1:
    with st.container(border=True):
        p1 = query_duckdb(
            """
            SELECT ts_code AS 代码, name AS 名称, close AS 收盘, pct_chg AS 涨跌
            FROM screen_result
            WHERE strftime(trade_date, '%Y-%m-%d') = ?
              AND preset_name = 'n-shape-pool1'
            ORDER BY ts_code
            """,
            [today_iso],
        )
        st.markdown(f"### Pool 1 候选 today ({len(p1)} 只)")
        if p1.empty:
            st.info("当日暂无 Pool 1 命中")
        else:
            st.dataframe(p1, hide_index=True, use_container_width=True)


# ── Section 4: 今日触发事件 ──


st.markdown("## 📍 今日触发事件")
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
)

if events.empty:
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
)
if trend.empty:
    st.info("最近 7 日无 Pool 1 数据")
else:
    max_hits = max(int(trend["hits"].max()), 1)
    y_max = int(max_hits * 1.2) + 1
    chart = (
        alt.Chart(trend)
        .mark_line(point=alt.OverlayMarkDef(filled=True, size=80, color="#3b82f6"), color="#3b82f6", strokeWidth=2.5)
        .encode(
            x=alt.X("date:O", title=None, axis=alt.Axis(labelAngle=-30)),
            y=alt.Y(
                "hits:Q",
                title="命中数",
                scale=alt.Scale(domain=[0, y_max]),
                axis=alt.Axis(grid=True, gridDash=[2, 4]),
            ),
            tooltip=[alt.Tooltip("date:O", title="日期"), alt.Tooltip("hits:Q", title="命中数")],
        )
        .properties(height=260)
    )
    text = (
        alt.Chart(trend)
        .mark_text(dy=-12, fontSize=11, color="#3b82f6")
        .encode(x=alt.X("date:O"), y=alt.Y("hits:Q"), text=alt.Text("hits:Q"))
    )
    st.altair_chart(chart + text, use_container_width=True)


# ── Section 6: 通知通道健康 ──


st.markdown("## 📨 通知通道")
try:
    rate = query_duckdb(
        """
        SELECT channel,
               COUNT(*) AS total,
               SUM(CASE WHEN success THEN 1 ELSE 0 END) AS ok
        FROM notification_log
        WHERE sent_at >= (CURRENT_TIMESTAMP - INTERVAL '24 hours')
        GROUP BY channel
        """
    )
    if rate.empty:
        st.info("最近 24h 无推送记录")
    else:
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

    notif = query_duckdb(
        """
        SELECT strftime(sent_at, '%m-%d %H:%M') AS 时间,
               scene AS 场景, channel AS 通道, target AS 目标,
               success AS 成功, title AS 标题
        FROM notification_log
        ORDER BY sent_at DESC
        LIMIT 30
        """
    )
    if not notif.empty:
        with st.expander(f"📜 最近 {len(notif)} 条推送", expanded=False):
            st.dataframe(notif, hide_index=True, use_container_width=True)
except duckdb.Error:
    st.info("notification_log 表暂无数据（首次推送后会自动生成）")


# ── Section 7: 本地 sync ──


st.markdown("## 💾 本地热备 sync")
sync_marker = settings.data_dir / ".last-local-sync.json"
if sync_marker.exists():
    try:
        info = json.loads(sync_marker.read_text())
        sync_at_str = info.get("sync_at", "")
        size_mb = info.get("local_size_bytes", 0) / 1024 / 1024
        host = info.get("host", "?")

        scols = st.columns(3)
        scols[0].metric("本地主机", host)
        scols[1].metric("数据大小", f"{size_mb:.1f} MB")

        if sync_at_str:
            sync_dt = datetime.fromisoformat(sync_at_str.replace("Z", "+00:00"))
            now_utc = datetime.now(timezone.utc)
            delta = now_utc - sync_dt
            sync_local = sync_dt.astimezone(CST).strftime("%m-%d %H:%M:%S")

            if delta > timedelta(hours=2):
                scols[2].metric(
                    "最后 sync",
                    sync_local,
                    delta=f"{delta.total_seconds() / 3600:.1f} 小时前",
                    delta_color="inverse",
                )
            else:
                scols[2].metric(
                    "最后 sync",
                    sync_local,
                    delta=f"{int(delta.total_seconds() / 60)} 分钟前",
                )
    except Exception as e:
        st.error(f"sync marker 解析失败: {e}")
else:
    st.info("等待本地首次 sync（marker 文件未生成）")


# ── Section 8: Pool 2 实时价位 ──


st.markdown("## ⚡ Pool 2 实时价位 vs 档位")
try:
    p2_active = query_duckdb(
        """
        SELECT pw.ts_code, sb.name,
               pw.body_upper, pw.body_lower,
               pw.level_40, pw.level_30, pw.level_20,
               pw.stop_strong, pw.stop_weak
        FROM pool2_watch pw
        LEFT JOIN stock_basic sb ON pw.ts_code = sb.ts_code
        WHERE pw.status = 'active'
        """
    )
    if p2_active.empty:
        st.info("Pool 2 无 active 标的")
    else:
        prices = get_realtime_prices_sina()

        rows = []
        for _, p2 in p2_active.iterrows():
            code_short = p2["ts_code"].split(".")[0]
            price_row = prices[prices["code_short"] == code_short]
            if price_row.empty:
                continue
            price = float(price_row.iloc[0]["最新价"])
            rows.append(
                {
                    "代码": p2["ts_code"],
                    "名称": p2.get("name") or "",
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

            def _color_dist(v):
                if v < 0:
                    return "color:#dc2626;font-weight:bold"
                if v < 0.5:
                    return "color:#f59e0b"
                return "color:#16a34a"

            styled = df.style.map(_color_dist, subset=["距40", "距强止"])
            st.dataframe(styled, hide_index=True, use_container_width=True)
            st.caption(
                "💡 距档位列：红色 = 已穿过（应已触发）/ "
                "黄色 = 接近（< 0.5 元）/ 绿色 = 安全"
            )
        else:
            st.warning("未匹配到任何 Pool 2 标的的实时价格")
except Exception as e:
    st.error(f"实时价位获取失败: {e}")


st.divider()
st.caption(
    f"DB: {settings.duckdb_path.name}  ·  "
    f"Refresh: {REFRESH_SECONDS}s  ·  "
    f"Last render: {datetime.now(CST).strftime('%Y-%m-%d %H:%M:%S')}"
)
