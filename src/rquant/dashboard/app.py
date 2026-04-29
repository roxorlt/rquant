"""rQuant Health Dashboard。

启动方式（本地或云端）：
    streamlit run src/rquant/dashboard/app.py --server.port 8501 \\
        --server.address 0.0.0.0 --server.headless true

显示 9 个核心指标，自动刷新 30 秒。读 DuckDB read-only 不阻塞业务写入。
"""

from __future__ import annotations

import json
import socket
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from rquant.config import settings

REFRESH_SECONDS = 30

st.set_page_config(
    page_title="rQuant Health",
    page_icon="📈",
    layout="wide",
)
st.markdown(
    f'<meta http-equiv="refresh" content="{REFRESH_SECONDS}">',
    unsafe_allow_html=True,
)


# ── DuckDB 查询封装 ──


@st.cache_data(ttl=20)
def query_duckdb(sql: str, params: list | None = None) -> pd.DataFrame:
    conn = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        if params:
            return conn.execute(sql, params).fetchdf()
        return conn.execute(sql).fetchdf()
    finally:
        conn.close()


# ── systemctl 查询 ──


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


# ── Real-time 行情（指标 #9） ──


@st.cache_data(ttl=30)
def get_realtime_prices_sina() -> pd.DataFrame:
    """sina 源拉全市场实时行情，30s 缓存。"""
    import akshare as ak

    df = ak.stock_zh_a_spot()
    df = df[["代码", "名称", "最新价", "最低", "最高"]].copy()
    df["code_short"] = df["代码"].str[-6:]
    return df


# ── Header ──


hostname = socket.gethostname()
st.title("📈 rQuant Health Dashboard")
st.caption(
    f"自动刷新 {REFRESH_SECONDS}s | 服务器: {hostname} | "
    f"渲染时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)

today_iso = date.today().isoformat()


# ── #1 #2: systemd 服务状态 ──


st.header("🟢 systemd 服务状态")
col_m, col_d = st.columns(2)
timers = systemd_list_timers()

with col_m:
    st.subheader("rquant-monitor")
    monitor_status = systemd_show("rquant-monitor.service")
    state = monitor_status.get("ActiveState", "unknown")
    sub = monitor_status.get("SubState", "")
    if state == "active":
        enter = monitor_status.get("ActiveEnterTimestamp", "")
        st.success(f"✅ Running ({sub})  启动: {enter}")
    elif state == "inactive":
        st.info(f"⚪ Inactive ({sub})  上次执行: status={monitor_status.get('ExecMainStatus', '?')}")
    else:
        st.error(f"❌ {state} ({sub})")

    timer = find_timer(timers, "rquant-monitor.timer")
    if timer:
        st.metric("下次触发", timer.get("next", "?"))
        if timer.get("last") and timer["last"] != "n/a":
            st.caption(f"上次触发: {timer.get('last')}")

with col_d:
    st.subheader("rquant-daily")
    daily_status = systemd_show("rquant-daily.service")
    state = daily_status.get("ActiveState", "unknown")
    if state == "active":
        st.warning("🔄 Running (流水线执行中)")
    elif state == "inactive":
        st.success(
            f"⚪ Idle  上次执行: status={daily_status.get('ExecMainStatus', '?')}"
        )
    else:
        st.error(f"❌ {state}")

    timer = find_timer(timers, "rquant-daily.timer")
    if timer:
        st.metric("下次触发", timer.get("next", "?"))


# ── #4: Watchlist ──


st.header("📋 当前 Watchlist")
col_p2, col_p1 = st.columns(2)

with col_p2:
    st.subheader("Pool 2 (active)")
    p2 = query_duckdb(
        """
        SELECT pw.ts_code, sb.name,
               pw.entry_date, pw.body_lower, pw.body_upper,
               pw.stop_strong, pw.stop_weak
        FROM pool2_watch pw
        LEFT JOIN stock_basic sb ON pw.ts_code = sb.ts_code
        WHERE pw.status = 'active'
        ORDER BY pw.entry_date DESC
        """
    )
    if p2.empty:
        st.info("Pool 2 暂无 active 标的")
    else:
        st.dataframe(p2, hide_index=True, use_container_width=True)

with col_p1:
    st.subheader(f"Pool 1 ({today_iso} 命中)")
    p1 = query_duckdb(
        """
        SELECT ts_code, name, close, pct_chg
        FROM screen_result
        WHERE strftime(trade_date, '%Y-%m-%d') = ?
          AND preset_name = 'n-shape-pool1'
        ORDER BY ts_code
        """,
        [today_iso],
    )
    if p1.empty:
        st.info("当日暂无 Pool 1 命中")
    else:
        st.dataframe(p1, hide_index=True, use_container_width=True)


# ── #3: 今日触发事件 ──


st.header("📍 今日触发事件")
events = query_duckdb(
    """
    SELECT trigger_time, ts_code, level, trigger_price, level_price,
           trigger_type, pool
    FROM monitor_event
    WHERE strftime(trade_date, '%Y-%m-%d') = ?
    ORDER BY trigger_time DESC
    """,
    [today_iso],
)

if events.empty:
    st.info("当日暂无触发事件")
else:
    col_count, col_table = st.columns([1, 4])
    col_count.metric("今日总触发", len(events))
    by_level = events["level"].value_counts().to_dict()
    breakdown = " / ".join(f"{k} {v}" for k, v in by_level.items())
    col_count.caption(breakdown)
    col_table.dataframe(events, hide_index=True, use_container_width=True)


# ── #5: 数据新鲜度 ──


st.header("🗓️ 数据新鲜度")
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
fcols[2].metric("daily_bar 行", f"{int(row['daily_bar_rows']):,}")
fcols[3].metric("monitor_event 行", f"{int(row['event_rows']):,}")


# ── #6: 最近 7 日 Pool 1 命中数趋势 ──


st.header("📊 最近 7 日 Pool 1 命中数")
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
    st.line_chart(trend.set_index("date")["hits"])


# ── #7: 通知通道健康 ──


st.header("📨 通知通道健康")
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
        ncols = st.columns(len(rate))
        for i, r in rate.iterrows():
            success_rate = r["ok"] / r["total"] * 100 if r["total"] else 0
            ncols[i].metric(
                f"{r['channel']} 24h",
                f"{int(r['ok'])}/{int(r['total'])}",
                delta=f"{success_rate:.0f}% 成功",
                delta_color="normal" if success_rate == 100 else "inverse",
            )

    notif = query_duckdb(
        """
        SELECT sent_at, scene, channel, target, success, title, error_msg
        FROM notification_log
        ORDER BY sent_at DESC
        LIMIT 20
        """
    )
    if not notif.empty:
        with st.expander("📜 最近 20 条推送日志"):
            st.dataframe(notif, hide_index=True, use_container_width=True)
except duckdb.Error:
    st.info("notification_log 表暂无数据（首次推送后会自动生成）")


# ── #8: 本地数据 sync 状态 ──


st.header("💾 本地数据 sync 状态")
sync_marker = settings.data_dir / ".last-local-sync.json"
if sync_marker.exists():
    try:
        info = json.loads(sync_marker.read_text())
        sync_at_str = info.get("sync_at", "")
        size_mb = info.get("local_size_bytes", 0) / 1024 / 1024
        host = info.get("host", "?")

        scols = st.columns(3)
        scols[0].metric("最后 sync", sync_at_str)
        scols[1].metric("本地数据大小", f"{size_mb:.1f} MB")
        scols[2].metric("本地主机", host)

        if sync_at_str:
            sync_dt = datetime.fromisoformat(sync_at_str.replace("Z", "+00:00"))
            now_utc = datetime.now(timezone.utc)
            delta = now_utc - sync_dt
            if delta > timedelta(hours=2):
                st.warning(
                    f"⚠️ 上次 sync 在 {delta.total_seconds() / 3600:.1f} 小时前"
                )
            else:
                st.success(
                    f"✅ {int(delta.total_seconds() / 60)} 分钟前同步成功"
                )
    except Exception as e:
        st.error(f"sync marker 解析失败: {e}")
else:
    st.info("等待本地首次 sync 完成（marker 文件未生成）")


# ── #9: Pool 2 实时价位 ──


st.header("⚡ Pool 2 实时价位 vs 档位")
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
            st.dataframe(df, hide_index=True, use_container_width=True)
            st.caption(
                "距档位 < 0 表示已穿过该档位（应已触发推送）；"
                "可对比 monitor_event 表确认"
            )
        else:
            st.warning("未匹配到任何 Pool 2 标的的实时价格")
except Exception as e:
    st.error(f"实时价位获取失败: {e}")


st.divider()
st.caption(
    f"rQuant Health Dashboard | 数据库: {settings.duckdb_path} | "
    f"刷新间隔: {REFRESH_SECONDS}s"
)
