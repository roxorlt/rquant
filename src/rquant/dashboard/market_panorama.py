"""rQuant 盘中市场全景 v2 — 独立 Streamlit 应用（P0）。

启动方式（仅本地，东财源云端被屏蔽，不进 deploy/systemd）：
    PYTHONPATH=src streamlit run src/rquant/dashboard/market_panorama.py \\
        --server.port 8506 --server.headless true

端口约定：8501 健康 / 8502 nl_screen / 8503 nl_canvas / 8504 Lab / 8505 预留 / 8506 本页。

v2 单屏两栏布局（消灭 tab / 消灭滑动，1440×900 基准）：
- 脉搏行：涨停/跌停/炸板/上涨占比/涨跌家数 5 个紧凑 metric，当日 sparkline 收进 st.popover；
- 左栏（52%）板块总表：体系 segmented_control（东财行业/东财概念/开盘啦题材）切一张合并
  总表（成交额 + 资金流 + 涨停炸板一表），列头点击排序、行选择联动下钻；
- 右栏（48%）上半下钻成分表（行选择联动图表），下半个股图表
  （分时 / 5日 / 日K，segmented_control 切周期，altair 绘制）。

取数架构：每轮渲染只 acquire 一次不可变 serving generation，快照、板块总览、
成分、分时、日 K 与爆量台账全部复用该 lease。页面不发外部行情请求、不读取生产
主库或副本；缺少投影或 generation 损坏时明确显示数据不可用。
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from rquant.dashboard.serving_page_ui import render_serving_state_banner
from rquant.panorama_data import (
    BOARD_SYSTEMS,
    ROUTE_LABELS,
    MarketPulse,
    ServingRuntimeConfigRead,
    board_constituents,
    compute_market_pulse,
    industry_fallback_members,
    load_board_members,
    load_daily_kline,
    load_historical_intraday_trend,
    load_intraday_kline_projection,
    load_kpl_concept_members,
    load_liquidity_baseline,
    load_market_overview_projection,
    load_market_snapshot_projection,
    load_pool_flags,
    load_pulse_alerts,
    load_pulse_history,
    load_surge_event_marks,
    load_surge_log,
    load_surge_marks,
    load_surge_runtime_config,
    open_panorama_serving_generation,
    search_surge_history,
    surge_mark_positions,
    volume_directions,
)

CST = timezone(timedelta(hours=8))
REFRESH_SECONDS = 60
_KPL_SYSTEM = "开盘啦题材"

# 红涨绿跌（A 股口径），日K 蜡烛与量柱同色
_UP_COLOR = "#ef4444"
_DOWN_COLOR = "#10b981"
_CANDLE_COLOR = alt.condition(
    "datum.close >= datum.open", alt.value(_UP_COLOR), alt.value(_DOWN_COLOR)
)

# 全局压缩样式一次性注入（消 divider 后主要靠 padding/字号收紧屏效）
_PANORAMA_CSS = """
<style>
/* 整个隐藏 Streamlit sticky 顶栏（Deploy/菜单无用，且会遮住页面标题） */
header[data-testid="stHeader"] {display: none;}
.block-container {padding-top: 0.8rem; padding-bottom: 0.4rem; max-width: 100%;}
[data-testid="stMetric"] {padding: 0.1rem 0;}
[data-testid="stMetricValue"] {font-size: 1.35rem; line-height: 1.15;}
[data-testid="stMetricLabel"], [data-testid="stMetricLabel"] p {font-size: 0.78rem;}
[data-testid="stVerticalBlock"] {gap: 0.5rem;}
[data-testid="stElementToolbar"] {display: none;}

/* ── 移动端（窄屏 ≤768px）响应式屏效优化 ──────────────────────────
   纯 CSS 媒体查询：PC（>768px）不受任何影响、逐像素保持原样；窄屏下把 st.columns
   渲染的 flexbox 横排改造成竖排堆叠 + 换行 + 收紧高度。不改任何 Python 布局逻辑。
   用 :has() 精准区分三类横排块（标题行 / 脉搏 metric 行 / 含 dataframe 的主两栏），
   避免一刀切 flex-direction 误伤脉搏行。表格高度靠覆盖 stDataFrameResizable —
   glide-grid 用 ResizeObserver 观察容器尺寸，改高后 canvas 自动 reflow、行仍可
   滚动/触摸选择（已 Playwright 实测：client 238 / scroll 245 → 可滚）。
   CSS 对选择器/声明间空白不敏感，长 :has() 规则跨行折行仅为过 E501，渲染等价。 */
@media (max-width: 768px) {
  .block-container {padding-top: 0.5rem; padding-left: 0.55rem; padding-right: 0.55rem;}
  h3 {font-size: 1.25rem !important;}
  [data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p {font-size: 0.66rem;}

  /* 标题 + 立即刷新：竖排，刷新按钮变全宽触摸目标 */
  [data-testid="stHorizontalBlock"]:has(h3) {flex-direction: column !important;}
  [data-testid="stHorizontalBlock"]:has(h3) > [data-testid="stColumn"] {
    width: 100% !important;
    flex: 1 1 100% !important;
  }

  /* 主两栏（含板块总表/下钻的 [52,48]）→ 竖排堆叠：左总表在上、右下钻+个股图表在下 */
  [data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] [data-testid="stDataFrame"]) {
    flex-direction: column !important;
  }
  [data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] [data-testid="stDataFrame"])
    > [data-testid="stColumn"] {
    width: 100% !important;
    flex: 1 1 100% !important;
    min-width: 0 !important;
  }

  /* 脉搏 5 metric + sparkline popover：保持横排但换行（约 3 个/行）+ 缩字号，防挤压溢出 */
  [data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) {
    flex-wrap: wrap !important;
    gap: 0.2rem 0.4rem !important;
  }
  [data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]) > [data-testid="stColumn"] {
    flex: 1 0 30% !important;
    min-width: 30% !important;
  }
  [data-testid="stMetricValue"] {font-size: 1.0rem; line-height: 1.1;}
  [data-testid="stMetricLabel"], [data-testid="stMetricLabel"] p {font-size: 0.66rem;}

  /* 体系/周期 segmented_control 按钮组换行不溢出 */
  [data-testid="stButtonGroup"] {flex-wrap: wrap !important;}

  /* 表格高度移动端收紧：板块总表（左列）560→360、下钻成分（右列）250→240 */
  [data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] [data-testid="stDataFrame"])
    > [data-testid="stColumn"]:first-child [data-testid="stDataFrameResizable"] {
    height: 360px !important;
    max-height: 360px !important;
  }
  [data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] [data-testid="stDataFrame"])
    > [data-testid="stColumn"]:last-child [data-testid="stDataFrameResizable"] {
    height: 240px !important;
    max-height: 240px !important;
  }
}
</style>
"""

st.set_page_config(
    page_title="rQuant 市场全景",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── 取数层（整轮页面复用一个 serving generation） ─────────────────────────────


def serving_members(store: Any) -> tuple[pd.DataFrame, str]:
    """东财成分优先，空时使用同代 stock universe 的行业粗分。"""

    members = load_board_members(store)
    if not members.empty:
        return members, "dc"
    return industry_fallback_members(store), "industry"


def serving_constituents(
    board_code: str,
    snapshot: pd.DataFrame,
    store: Any,
) -> pd.DataFrame:
    """Build one board drill-down entirely from the acquired generation."""

    members, _ = serving_members(store)
    kpl_members = load_kpl_concept_members(store)
    combined = _combined_members(members, kpl_members)
    return board_constituents(
        board_code,
        combined,
        snapshot,
        pool_flags=load_pool_flags(store),
        liquidity=load_liquidity_baseline(store),
    )


@st.cache_data(ttl=30, show_spinner=False)
def cached_surge_history(generation_id: str, query: str, _store: Any) -> pd.DataFrame:
    """Cross-day search cache is scoped to one immutable Serving generation."""
    return search_surge_history(query.strip(), store=_store)


@st.cache_data(ttl=600, show_spinner=False)
def cached_historical_intraday_trend(
    generation_id: str, ts_code: str, day_key: str, _store: Any
) -> pd.DataFrame:
    """Historical minute data cache is scoped to one immutable Serving generation."""
    return load_historical_intraday_trend(ts_code, date.fromisoformat(day_key), store=_store)


@st.cache_data(ttl=30, show_spinner=False)
def cached_surge_event_marks(
    generation_id: str, ts_code: str, day_key: str, _store: Any
) -> pd.DataFrame:
    """Historical event marks cache is scoped to one immutable Serving generation."""
    return load_surge_event_marks(ts_code, date.fromisoformat(day_key), store=_store)


@st.cache_data(ttl=60, show_spinner=False)
def cached_surge_marks(
    generation_id: str, ts_code: str, dates_key: str, _store: Any
) -> pd.DataFrame:
    """Chart event marks cache is scoped to one immutable Serving generation."""
    dates = [date.fromisoformat(s) for s in dates_key.split(",") if s]
    return load_surge_marks(ts_code, dates, store=_store)


@st.cache_data(ttl=300, show_spinner=False)
def cached_runtime_config(
    generation_id: str,
    _store: Any,
) -> ServingRuntimeConfigRead | dict[str, object] | None:
    return load_surge_runtime_config(store=_store)


@st.cache_data(ttl=60, show_spinner=False)
def cached_pulse_history(generation_id: str, day_key: str, _store: Any) -> pd.DataFrame:
    return load_pulse_history(date.fromisoformat(day_key), store=_store)


@st.cache_data(ttl=60, show_spinner=False)
def cached_pulse_alerts(generation_id: str, day_key: str, _store: Any) -> pd.DataFrame:
    return load_pulse_alerts(date.fromisoformat(day_key), store=_store)


# ── 数据整形 helpers ──────────────────────────────────────────────────────────


def _combined_members(members: pd.DataFrame, kpl_members: pd.DataFrame) -> pd.DataFrame:
    """东财成分 + 开盘啦成分按 board_code 合一（下钻只用 board_code/con_code，丢 idx_type）。"""
    cols = ["board_code", "board_name", "con_code"]
    frames = [
        df[cols] for df in (members, kpl_members) if not df.empty and set(cols).issubset(df.columns)
    ]
    if not frames:
        return pd.DataFrame(columns=cols)
    return pd.concat(frames, ignore_index=True)


def _first_selected_row(event: object) -> int | None:
    """从 st.dataframe(on_select) 返回值取单选行的位置索引；空选/异常 → None。

    位置索引是「传入 df 的行序」，客户端列头排序不改变该语义，用 df.iloc[idx] 反查。
    兼容 selection 的属性访问与 dict 访问两种形态。
    """
    selection = getattr(event, "selection", None)
    if selection is None and isinstance(event, dict):
        selection = event.get("selection")
    rows = getattr(selection, "rows", None)
    if rows is None and isinstance(selection, dict):
        rows = selection.get("rows")
    if rows:
        return int(rows[0])
    return None


def _overview_display(overview: pd.DataFrame, is_kpl: bool) -> pd.DataFrame:
    """总表 → 展示列（数值列保持数值 dtype，列头排序才是数值序而非字符序）。"""
    disp = pd.DataFrame(
        {
            "板块": overview["board_name"],
            "成交额(亿)": (overview["amount"] / 1e8).round(2),
            "净流入(亿)": (overview["main_net_amount"] / 1e8).round(2),
            "净流率%": overview["main_net_rate"].round(2),
            "涨跌中位%": overview["pct_chg_median"].round(2),
            "涨停": overview["limit_up_count"],
            "炸板": overview["broken_count"],
            "占比%": overview["limit_up_ratio_pct"],
            "成分数": overview["stock_count"],
            "主力流入最大股": overview["leading_stock"],
        }
    )
    if is_kpl:
        disp = disp.drop(columns=["净流入(亿)", "净流率%", "主力流入最大股"])
    return disp


def _constituents_display(cons: pd.DataFrame) -> pd.DataFrame:
    """下钻成分 → 展示列（强度分默认序已在 board_constituents 内完成）。"""
    disp = pd.DataFrame(
        {
            "代码": cons["ts_code"],
            "名称": cons["name"],
            "现价": cons["price"].round(2),
            "涨幅%": cons["pct_chg"].round(2),
            "成交额(亿)": (cons["amount"] / 1e8).round(2),
        }
    )
    if "strength" in cons.columns:
        disp["强度分"] = cons["strength"]
    if "turnover_pct" in cons.columns:
        disp["换手强度"] = cons["turnover_pct"]
    if "rel_volume_5d" in cons.columns:
        disp["相对放量"] = cons["rel_volume_5d"]
    disp["涨停"] = cons["is_limit_up"].map({True: "🔴", False: ""})
    disp["池内"] = cons["pools"]
    return disp


def _append_intraday_bar(kline: pd.DataFrame, snapshot: pd.DataFrame, ts_code: str) -> pd.DataFrame:
    """副本日K缺今日 bar 时，用实时快照拼一根临时当日 bar（volume 用快照量，MA 不重算）。

    仅当副本最新日 < 今天且快照该票 open/high/low/price 均非空时追加。
    """
    if kline.empty:
        return kline
    today = datetime.now(CST).date()
    last_date = pd.to_datetime(kline["trade_date"].iloc[-1]).date()
    if last_date >= today:
        return kline
    row = snapshot[snapshot["ts_code"] == ts_code]
    if row.empty:
        return kline
    r = row.iloc[0]
    o, h, low_, p = r.get("open"), r.get("high"), r.get("low"), r.get("price")
    if pd.isna(o) or pd.isna(h) or pd.isna(low_) or pd.isna(p):
        return kline
    # 量纲约束：快照 volume 是股（sina/东财归一后口径），daily_bar.vol 是手 → ÷100 对齐
    vol = r.get("volume", float("nan"))
    bar = {
        "trade_date": pd.Timestamp(today),
        "open": o,
        "high": h,
        "low": low_,
        "close": p,
        "volume": vol / 100.0 if pd.notna(vol) else float("nan"),
        "ma5": float("nan"),
        "ma10": float("nan"),
        "ma20": float("nan"),
    }
    return pd.concat([kline, pd.DataFrame([bar])], ignore_index=True)


# ── 脉搏行 ────────────────────────────────────────────────────────────────────


def _record_pulse_history(pulse: MarketPulse) -> pd.DataFrame:
    """把每轮脉搏计数按分钟记进 session_state，供当日 sparkline。"""
    history: dict[str, dict[str, int]] = st.session_state.setdefault("pulse_history", {})
    if pulse.total_count > 0:
        minute = datetime.now(CST).strftime("%H:%M")
        history[minute] = {
            "涨停": pulse.limit_up_count,
            "跌停": pulse.limit_down_count,
            "炸板": pulse.broken_count,
        }
    if not history:
        return pd.DataFrame()
    df = pd.DataFrame.from_dict(history, orient="index").rename_axis("time").reset_index()
    return df.melt("time", var_name="指标", value_name="家数")


def _snapshot_status_line(as_of: str, snap_route: str, status: dict) -> str:
    """动态状态行：数据时间 + 新鲜度 + 快照路由；age>180s 加 ⚠️ 与错误摘要。"""
    snap = status.get("snapshot") or {}
    age = snap.get("age_seconds")
    age_txt = f"{int(age)} 秒前" if age is not None else "—"
    line = f"数据 {as_of}（{age_txt}）· 快照路由 {ROUTE_LABELS.get(snap_route, snap_route)}"
    if age is not None and age > 180:
        err = str(snap.get("last_error") or "").strip()
        line = f"⚠️ {line} · 数据陈旧" + (f" · 最近错误：{err[:80]}" if err else "")
    return line


_PULSE_FACETS: list[tuple[str, str, str]] = [
    ("limit_up", "涨停", _UP_COLOR),
    ("broken", "炸板", "#f97316"),
    ("limit_down", "跌停", _DOWN_COLOR),
    ("up_ratio_pct", "上涨占比%", "#2563eb"),
]


def _pulse_facet_chart(hist: pd.DataFrame) -> alt.VConcatChart:
    """四指标分面小图：独立 y 轴且不从 0 起，x 轴共享、约 6 个稀疏刻度。"""
    ticks = hist["t"].tolist()[:: max(1, len(hist) // 6)]
    x = alt.X("t:O", title=None, axis=alt.Axis(values=ticks, labelAngle=0))
    rows = [
        alt.Chart(hist)
        .mark_line(color=color)
        .encode(
            x=x,
            y=alt.Y(f"{col}:Q", title=title, scale=alt.Scale(zero=False)),
            tooltip=["t", alt.Tooltip(f"{col}:Q", title=title)],
        )
        .properties(height=64)
        for col, title, color in _PULSE_FACETS
    ]
    return alt.vconcat(*rows).resolve_scale(x="shared", y="independent")


def _render_pulse_alert_line(now: datetime, store: Any) -> None:
    """最近 30 分钟内的异动：常驻 warning + 新异动一次性 toast（会话内去重）。"""
    alerts = cached_pulse_alerts(str(store.generation_id), now.date().isoformat(), store)
    if alerts.empty:
        state = str(alerts.attrs.get("serving_state") or "")
        if state in {"unavailable", "degraded"}:
            detail = str(alerts.attrs.get("serving_detail") or "未提供状态详情")
            st.warning(f"脉搏异动该代未发布/降级：{detail[:140]}")
        return
    latest = alerts.iloc[-1]
    try:
        hh, mm = str(latest["t"]).split(":")
        alert_dt = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    except ValueError:
        return
    if not (timedelta(0) <= now - alert_dt <= timedelta(minutes=30)):
        return
    extra = f"（今日共 {len(alerts)} 次异动）" if len(alerts) > 1 else ""
    st.warning(f"⚡ {latest['t']} {latest['kind_label']}：{latest['message']}{extra}")
    seen: set[str] = st.session_state.setdefault("seen_pulse_alerts", set())
    key = f"{latest['t']}-{latest['kind']}"
    if key not in seen:
        seen.add(key)
        st.toast(f"⚡ {latest['t']} {latest['kind_label']}：{latest['message']}")


def render_pulse(
    snapshot: pd.DataFrame, as_of: str, snap_route: str, status: dict, store: Any
) -> None:
    pulse = compute_market_pulse(snapshot)
    if pulse.total_count == 0:
        detail = str(snapshot.attrs.get("serving_detail") or "市场快照投影没有可展示数据")
        st.warning(f"Serving 数据不可用：{detail[:160]}")
        err = str((status.get("snapshot") or {}).get("last_error") or "").strip()
        if err:
            st.caption(f"最近错误：{err[:120]}")
        return
    spark = _record_pulse_history(pulse)  # 每轮都记，与 popover 是否展开无关

    c1, c2, c3, c4, c5, c6 = st.columns([1, 1, 1, 1, 1.3, 0.7])
    c1.metric("涨停", pulse.limit_up_count)
    c2.metric("跌停", pulse.limit_down_count)
    c3.metric("炸板", pulse.broken_count)
    c4.metric("上涨占比", f"{pulse.up_ratio_pct:.1f}%")
    c5.metric("涨/跌家数", f"{pulse.up_count} / {pulse.down_count}")
    st.caption(_snapshot_status_line(as_of, snap_route, status))
    _render_pulse_alert_line(datetime.now(CST), store)
    with c6, st.popover("📈", width="stretch"):
        st.caption(f"快照 {as_of} · 有效样本 {pulse.total_count} 只（停牌除外）")
        hist = cached_pulse_history(
            str(store.generation_id), datetime.now(CST).date().isoformat(), store
        )
        history_state = str(hist.attrs.get("serving_state") or "")
        if history_state in {"unavailable", "degraded"}:
            detail = str(hist.attrs.get("serving_detail") or "未提供状态详情")
            st.warning(f"脉搏历史该代未发布/降级：{detail[:140]}")
        if len(hist) >= 2:
            st.altair_chart(_pulse_facet_chart(hist), width="stretch")
            st.caption("数据来源：服务端全天历史（surge-watch 每分钟落盘）")
        elif not spark.empty and spark["time"].nunique() >= 2:
            chart = (
                alt.Chart(spark)
                .mark_line(point=True)
                .encode(
                    x=alt.X("time:O", title=None),
                    y=alt.Y("家数:Q", title=None, scale=alt.Scale(zero=False)),
                    color=alt.Color("指标:N", legend=alt.Legend(orient="top", title=None)),
                    tooltip=["time", "指标", "家数"],
                )
                .properties(height=160)
            )
            st.altair_chart(chart, width="stretch")
            st.caption("数据来源：本会话临时累积（不替代服务端历史）")
        else:
            st.caption("脉搏曲线累积中（需 ≥2 分钟样本）")


# ── 左栏：板块合并总表 ────────────────────────────────────────────────────────


def render_overview(store: Any, snap_route: str) -> tuple[str | None, str | None]:
    """渲染体系切换 + 合并总表，返回选中板块 (board_code, board_name)。

    体系 segmented_control 变更 → dataframe key 随体系切换 → 旧选择自然失效回退默认第一行
    （streamlit 无法程序化清空行选择，改 key 是绕过该限制的既定手法）。
    """
    system = (
        st.segmented_control(
            "体系",
            list(BOARD_SYSTEMS),
            default=_KPL_SYSTEM,
            key="sys_seg",
            label_visibility="collapsed",
        )
        or _KPL_SYSTEM
    )
    is_kpl = system == _KPL_SYSTEM

    overview = load_market_overview_projection(system, store)
    route = str(overview.attrs.get("route") or "serving")
    if overview.empty:
        detail = str(overview.attrs.get("serving_detail") or "该体系暂无板块总览")
        st.warning(f"{system} 数据不可用：{detail[:140]}")
        return None, None
    # 展示层默认涨停数降序（次键成交额防并列）——数据层契约（amount 降序）不动；
    # 默认选中第一行随之变成涨停最多的板块，列头点击排序仍是客户端零 rerun
    overview = overview.sort_values(["limit_up_count", "amount"], ascending=False).reset_index(
        drop=True
    )

    event = st.dataframe(
        _overview_display(overview, is_kpl),
        key=f"board_tbl_{system}",
        on_select="rerun",
        selection_mode="single-row",
        hide_index=True,
        width="stretch",
        height=560,
    )
    idx = _first_selected_row(event)
    # 缓存刷新可能缩短表长，越界（含跨体系残留）一律回退默认第一行（涨停数最多）
    if idx is None or idx >= len(overview):
        idx = 0
    sel = overview.iloc[idx]
    board_code = str(sel["board_code"])
    board_name = str(sel["board_name"])

    snap_label = ROUTE_LABELS.get(snap_route, snap_route)
    if is_kpl:
        st.caption(
            f"快照路由：{snap_label} · 开盘啦题材无资金流口径（隐藏净流入三列）；"
            "列头点击排序，行选择联动右侧下钻"
        )
    else:
        st.caption(
            f"快照路由：{snap_label} · 资金流路由：{ROUTE_LABELS.get(route or 'none', route)}"
            " · 列头点击排序，行选择联动右侧下钻"
        )
    return board_code, board_name


# ── 右栏上：板块下钻成分表 ────────────────────────────────────────────────────


def render_drilldown(
    board_code: str | None,
    board_name: str | None,
    snapshot: pd.DataFrame,
    store: Any,
) -> tuple[str | None, str | None]:
    """渲染选中板块的下钻成分表，返回选中个股 (ts_code, name)。"""
    title = board_name or "—"
    st.markdown(f"**「{title}」成分 · 强度分默认序**")
    if board_code is None:
        st.info("左侧选板块后查看成分")
        return None, None

    cons = serving_constituents(board_code, snapshot, store)
    if cons.empty:
        st.info("该板块成分与快照无交集")
        return None, None

    # key 随 board_code 切换 → 换板块自动清空个股选择 → 回到「点选个股查看图表」提示
    event = st.dataframe(
        _constituents_display(cons),
        key=f"cons_tbl_{board_code}",
        on_select="rerun",
        selection_mode="single-row",
        hide_index=True,
        width="stretch",
        height=250,
    )
    idx = _first_selected_row(event)
    if idx is None or idx >= len(cons):
        return None, None
    row = cons.iloc[idx]
    return str(row["ts_code"]), str(row["name"])


# ── 右栏下：个股图表（分时 / 5日 / 日K） ──────────────────────────────────────


# A 股 1min 交易日固定 240 根:上午 120(9:30-11:29)+ 下午 120(13:00-14:59),
# 末根(idx 239)=15:00。bar 序号天然跳过午休,故 idx == 全天时段位置。
_SESSION_BARS = 240


def _trend_axis(trend: pd.DataFrame) -> tuple[list[int], str, list[int]]:
    """分时/5日 x 轴（bar 序号 idx）刻度定位 + 标签映射 + x 轴定域。

    单日 → 刻度钉死在**全天真实时段位置** 0/60/120/180/239（标签 09:30/10:30/
    11:30·13:00/14:00/15:00），x 轴定域 [0, 239]——盘中数据不足整天时线只填左边一截、
    停在当前时刻、右边留空,而非把半天数据拉伸铺满(2026-07-07 盘中 bug:99 根被按
    条数等分刻度铺成整天,看着像 9:30-15:00 实际只到 11:11)。多日 → 每日首根打日期
    标签,定域按数据实际范围。labelExpr 用 Vega indexof 把刻度值查回标签。
    """
    day = pd.to_datetime(trend["dt"]).dt.normalize()
    day_starts = day.drop_duplicates()
    if len(day_starts) <= 1:
        values = [0, 60, 120, 180, _SESSION_BARS - 1]
        labels = ["09:30", "10:30", "11:30/13:00", "14:00", "15:00"]
        domain = [0, _SESSION_BARS - 1]
    else:
        values = [int(pos) for pos in day_starts.index]
        labels = [d.strftime("%m-%d") for d in day_starts]
        domain = [0, max(len(trend) - 1, 0)]
    vals_arr = "[" + ",".join(str(v) for v in values) + "]"
    labels_arr = "[" + ",".join(f"'{lbl}'" for lbl in labels) + "]"
    label_expr = f"{labels_arr}[indexof({vals_arr}, datum.value)]"
    return values, label_expr, domain


def _trend_chart(trend: pd.DataFrame, marks: pd.DataFrame | None = None) -> alt.VConcatChart:
    """分时/5日：价格线 + 均价虚线（有则画）+ 底部量柱（按分钟涨跌近似 tick-rule 红涨绿跌上色），
    x 轴共享。marks 非空时叠加爆量标记竖线 + 标记点（爆量确认时点，悬停显示时间与倍数）。

    x 轴用 bar 序号 idx（quantitative）而非真实时间 dt——非交易时段（午休/隔夜）
    不占轴距，线天然连续无空档；轴刻度经 labelExpr 映射回时间/日期，tooltip 保留真实 dt。
    """
    trend = trend.reset_index(drop=True).assign(idx=lambda d: range(len(d)))
    trend["vol_color"] = volume_directions(trend["price"]).map(
        {"up": _UP_COLOR, "down": _DOWN_COLOR, "flat": "#94a3b8"}
    )
    has_avg = trend["avg_price"].notna().any()
    values, label_expr, domain = _trend_axis(trend)
    # 定域到全天(单日)或数据范围(多日):盘中数据不足整天时线停在当前、右边留空
    x_scale = alt.Scale(nice=False, zero=False, domain=domain)
    dt_tip = alt.Tooltip("dt:T", title="时间", format="%m-%d %H:%M")
    x_price = alt.X("idx:Q", title=None, scale=x_scale, axis=alt.Axis(labels=False, ticks=False))
    price_line = (
        alt.Chart(trend)
        .mark_line(color="#2563eb")
        .encode(
            x=x_price,
            y=alt.Y("price:Q", title=None, scale=alt.Scale(zero=False)),
            tooltip=[dt_tip, alt.Tooltip("price:Q", title="价")],
        )
    )
    layers = [price_line]
    if has_avg:
        avg_line = (
            alt.Chart(trend)
            .mark_line(color="#f59e0b", strokeDash=[4, 3])
            .encode(x=x_price, y=alt.Y("avg_price:Q"))
        )
        layers.append(avg_line)
    mark_pos = (
        surge_mark_positions(trend, marks)
        if marks is not None and not marks.empty
        else pd.DataFrame()
    )
    if not mark_pos.empty:
        mark_tip = [
            alt.Tooltip("label:N", title="爆量"),
            alt.Tooltip("trigger_count:Q", title="次数"),
            alt.Tooltip("rel_cum_values:N", title="累计倍数"),
        ]
        layers.append(
            alt.Chart(mark_pos)
            .mark_rule(color="#f97316", strokeDash=[6, 4], size=2)
            .encode(x=alt.X("idx:Q", scale=x_scale), tooltip=mark_tip)
        )
        layers.append(
            alt.Chart(mark_pos)
            .mark_point(color="#f97316", filled=True, size=80)
            .encode(x=alt.X("idx:Q", scale=x_scale), y="price:Q", tooltip=mark_tip)
        )
    price = alt.layer(*layers).properties(height=220)
    x_vol = alt.X(
        "idx:Q",
        title=None,
        scale=x_scale,
        axis=alt.Axis(values=values, labelExpr=label_expr, labelAngle=0, labelOverlap=False),
    )
    vol = (
        alt.Chart(trend)
        .mark_bar()
        .encode(
            x=x_vol,
            y=alt.Y("volume:Q", title=None),
            color=alt.Color("vol_color:N", scale=None),
            tooltip=[dt_tip, alt.Tooltip("volume:Q", title="量")],
        )
        .properties(height=70)
    )
    return alt.vconcat(price, vol).resolve_scale(x="shared")


def _kline_chart(kline: pd.DataFrame) -> alt.VConcatChart:
    """日K：rule(high-low)+bar(open-close) 红涨绿跌 + MA5/10/20 + 底部量柱。"""
    df = kline.copy()
    # 序数轴防周末空隙；label 稀疏化（约 8 个刻度）
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
    dates = df["trade_date"].tolist()
    step = max(1, len(dates) // 8)
    ticks = dates[::step]
    x_price = alt.X("trade_date:O", sort=None, title=None, axis=alt.Axis(labels=False, ticks=False))
    x_vol = alt.X(
        "trade_date:O",
        sort=None,
        title=None,
        axis=alt.Axis(values=ticks, labelAngle=-45, labelOverlap=True),
    )

    rule = (
        alt.Chart(df)
        .mark_rule()
        .encode(
            x=x_price,
            y=alt.Y("low:Q", title=None, scale=alt.Scale(zero=False)),
            y2="high:Q",
            color=_CANDLE_COLOR,
        )
    )
    bar = alt.Chart(df).mark_bar().encode(x=x_price, y="open:Q", y2="close:Q", color=_CANDLE_COLOR)

    ma_cols = [c for c in ("ma5", "ma10", "ma20") if c in df.columns]
    price_layers = [rule, bar]
    if ma_cols:
        ma_long = df.melt(
            id_vars=["trade_date"], value_vars=ma_cols, var_name="MA", value_name="ma_val"
        ).dropna(subset=["ma_val"])
        ma_line = (
            alt.Chart(ma_long)
            .mark_line(size=1)
            .encode(
                x=x_price,
                y=alt.Y("ma_val:Q"),
                color=alt.Color(
                    "MA:N",
                    scale=alt.Scale(
                        domain=["ma5", "ma10", "ma20"],
                        range=["#f59e0b", "#3b82f6", "#a855f7"],
                    ),
                    legend=alt.Legend(orient="top", title=None),
                ),
            )
        )
        price_layers.append(ma_line)
    # 蜡烛用条件值色、MA 用字段刻度色，两套色域独立解析避免图例/刻度合并冲突
    price = alt.layer(*price_layers).resolve_scale(color="independent").properties(height=220)
    vol = (
        alt.Chart(df)
        .mark_bar()
        .encode(x=x_vol, y=alt.Y("volume:Q", title=None), color=_CANDLE_COLOR)
        .properties(height=70)
    )
    return alt.vconcat(price, vol).resolve_scale(x="shared")


def render_stock_chart(
    ts_code: str | None,
    name: str | None,
    snapshot: pd.DataFrame,
    store: Any,
    *,
    key_prefix: str = "pano",
) -> None:
    st.markdown(f"**个股图表 · {name or '—'}**")
    period = (
        st.segmented_control(
            "周期",
            ["分时", "5日", "日K"],
            default="分时",
            key=f"chart_period_{key_prefix}",
            label_visibility="collapsed",
        )
        or "分时"
    )

    if ts_code is None:
        st.info("点选个股查看图表")
        return

    if period in ("分时", "5日"):
        ndays = 1 if period == "分时" else 5
        trend = load_intraday_kline_projection(ts_code, ndays, store)
        route = str(trend.attrs.get("route") or "serving")
        if trend.empty:
            detail = str(trend.attrs.get("serving_detail") or "该标的没有分钟投影")
            st.warning(f"{period}数据不可用：{detail[:140]}")
            return
        days = sorted(pd.to_datetime(trend["dt"]).dt.date.unique())
        marks = cached_surge_marks(
            str(store.generation_id), ts_code, ",".join(d.isoformat() for d in days), store
        )
        st.altair_chart(_trend_chart(trend, marks), width="stretch")
        st.caption(
            f"数据路由：{ROUTE_LABELS.get(route, route)}"
            " · 量柱色=分钟涨跌近似（tick-rule），非真实内外盘"
            " · 橙线=爆量确认"
        )
    else:
        kline = _append_intraday_bar(load_daily_kline(ts_code, store=store), snapshot, ts_code)
        if kline.empty:
            detail = str(kline.attrs.get("serving_detail") or "该标的没有日 K 投影")
            st.warning(f"日 K 数据不可用：{detail[:140]}")
            return
        st.altair_chart(_kline_chart(kline), width="stretch")


# ── 爆量记录 tab（当日 surge-watch 识别台账） ─────────────────────────────────

_SURGE_STATUS_LABEL = {"confirmed": "🟢可买", "unbuyable": "🔴已涨停"}


def _surge_log_display(df: pd.DataFrame) -> pd.DataFrame:
    """台账 → 展示列（中文列名、数值列合理 round、状态映射图标）。"""
    n = len(df)

    def col(name: str, default: object = "") -> pd.Series:
        return df[name] if name in df.columns else pd.Series([default] * n)

    return pd.DataFrame(
        {
            "时间": col("confirmed_at").astype(str),
            "代码": col("ts_code").astype(str),
            "名称": col("name").astype(str),
            "题材": col("theme").astype(str),
            "推送价": pd.to_numeric(col("price", float("nan")), errors="coerce").round(2),
            "涨幅%": pd.to_numeric(col("pct_chg", float("nan")), errors="coerce").round(2),
            "累计爆量倍数": pd.to_numeric(col("rel_cum", float("nan")), errors="coerce").round(2),
            "累计额(亿)": (
                pd.to_numeric(col("cum_amount", float("nan")), errors="coerce") / 1e8
            ).round(2),
            "距涨停%": (
                pd.to_numeric(col("room_to_limit_pct", float("nan")), errors="coerce").round(1)
            ),
            "状态": col("status", "confirmed").map(
                lambda s: _SURGE_STATUS_LABEL.get(str(s), str(s))
            ),
        }
    )


def _surge_history_display(df: pd.DataFrame) -> pd.DataFrame:
    """跨日台账展示：日期单列格式化，余列严格复用日台账口径。"""
    dates = pd.to_datetime(df.get("trade_date"), errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    display = _surge_log_display(df.drop(columns=["trade_date"], errors="ignore"))
    display.insert(0, "日期", dates.reset_index(drop=True))
    return display


def _surge_history_table_key(query: str, df: pd.DataFrame) -> str:
    """跨日结果表的选择态键：查询与有序结果集变化时均重置。"""
    normalized_query = query.strip().casefold()
    query_digest = sha256(normalized_query.encode("utf-8")).hexdigest()[:12]
    identity_columns = ["trade_date", "ts_code", "confirmed_at"]
    identity_rows = []
    for row in df.reindex(columns=identity_columns).itertuples(index=False, name=None):
        identity_rows.append("\x1f".join("" if pd.isna(value) else str(value) for value in row))
    result_digest = sha256("\x1e".join(identity_rows).encode("utf-8")).hexdigest()[:16]
    return f"surge_history_tbl_{query_digest}_{result_digest}"


_BOARD_LABELS = {"main": "主板", "gem": "创业", "star": "科创", "bj": "北交"}


def _surge_caption(n_rows: int, store: Any) -> str:
    """页脚口径：展示同代 runtime config 或明确的未发布状态。"""
    cfg = cached_runtime_config(str(store.generation_id), store)
    if isinstance(cfg, ServingRuntimeConfigRead):
        if cfg.config is None:
            return (
                f"检测口径该代未发布/降级：{cfg.detail[:100]}"
                " · 以下台账仍来自同代 Serving 投影"
                f" · 共 {n_rows} 条"
            )
        config = cfg.config
        boards = "/".join(_BOARD_LABELS.get(board, board) for board in config.boards)
        return (
            f"检测范围：{boards or '—'}"
            f" · 口径 v4：累计放量 {config.k_cum:g}-{config.ratio_cap:g}×"
            " + 当前分钟上涨 + 外盘占优（tick-rule 近似）"
            " · 每标的取当日最早识别时刻"
            f" · 观察提示非买入信号 · 共 {n_rows} 条"
        )
    if cfg:
        boards = "/".join(_BOARD_LABELS.get(b, str(b)) for b in cfg.get("boards", []))
        return (
            f"检测范围：{boards or '—'}"
            f" · 口径 v4：累计放量 {cfg.get('k_cum', '—')}-{cfg.get('ratio_cap', '—')}×"
            " + 当前分钟上涨 + 外盘占优（tick-rule 近似）"
            " · 每标的取当日最早识别时刻"
            f" · 观察提示非买入信号 · 共 {n_rows} 条"
        )
    return (
        "口径 v4：累计放量 + 当前分钟上涨 + 外盘占优（tick-rule 近似）"
        " · 每标的取当日最早识别时刻"
        f" · 观察提示非买入信号 · 共 {n_rows} 条"
    )


def render_historical_surge_detail(ts_code: str, name: str, day: date, store: Any) -> None:
    """指定爆量日复盘：完整分钟趋势与该标的当天每个确认时刻。"""
    day_key = day.isoformat()
    st.markdown(f"**{ts_code} {name or '—'} · {day_key}**")
    trend = cached_historical_intraday_trend(str(store.generation_id), ts_code, day_key, store)
    if trend.empty:
        st.info(f"{day_key} 该日分钟数据未入库/暂不可用（Serving 分钟投影缺该日记录）")
        return
    marks = cached_surge_event_marks(str(store.generation_id), ts_code, day_key, store)
    st.altair_chart(_trend_chart(trend, marks), width="stretch")
    st.caption("数据来源：Serving 投影 · 量柱色=分钟涨跌近似（tick-rule） · 橙线=爆量确认")


def _render_surge_table(df: pd.DataFrame, *, table_key: str, historical: bool) -> int | None:
    display = _surge_history_display(df) if historical else _surge_log_display(df)
    event = st.dataframe(
        display,
        key=table_key,
        on_select="rerun",
        selection_mode="single-row",
        hide_index=True,
        width="stretch",
        height=300,
    )
    return _first_selected_row(event)


def render_surge_log(store: Any) -> None:
    """爆量台账：空搜索按日查看；搜索时跨日检索，并可复盘任一爆量日。"""
    today = datetime.now(CST).date()
    runtime_config = cached_runtime_config(str(store.generation_id), store)
    if isinstance(runtime_config, ServingRuntimeConfigRead) and runtime_config.config is None:
        st.warning(f"检测口径该代未发布/降级：{runtime_config.detail[:140]}")
    query = st.text_input(
        "搜索标的",
        placeholder="输入股票代码或名称，跨天检索全部爆量记录",
        key="surge_search",
    )
    normalized_query = query.strip()
    if normalized_query:
        df = cached_surge_history(str(store.generation_id), normalized_query, store)
        st.caption(f"跨日检索 · 命中 {len(df)} 条（每标的每日保留首次确认）")
        if df.empty:
            st.info(f"未找到“{normalized_query}”的爆量记录")
            return
        idx = _render_surge_table(
            df,
            table_key=_surge_history_table_key(normalized_query, df),
            historical=True,
        )
        if idx is None or idx >= len(df):
            st.info("点选记录查看对应交易日的完整分钟趋势与全部爆量确认点")
            return
        row = df.iloc[idx]
        selected_day = pd.to_datetime(row["trade_date"], errors="coerce")
        if pd.isna(selected_day):
            st.info("该记录日期无效，暂不能加载历史趋势")
            return
        render_historical_surge_detail(
            str(row["ts_code"]), str(row.get("name", "")), selected_day.date(), store
        )
        return

    sel = st.date_input(
        "爆量日期", value=today, max_value=today, key="surge_day", format="YYYY-MM-DD"
    )
    if sel is None:  # 被清空 → 回退今日
        sel = today
    df = load_surge_log(sel, store=store)
    if df.empty:
        detail = str(df.attrs.get("serving_detail") or "")
        if detail:
            st.warning(detail[:160])
            return
        if sel == today:
            st.info("今日暂无爆量记录（surge-watch 尚未识别到，或未到盘中）")
        else:
            st.info(f"{sel.isoformat()} 无爆量记录")
        return
    idx = _render_surge_table(df, table_key=f"surge_tbl_{sel.isoformat()}", historical=False)
    caption = _surge_caption(len(df), store)
    st.caption(caption)
    if idx is None or idx >= len(df):
        st.info("点选记录查看对应交易日的完整分钟趋势与全部爆量确认点")
        return
    row = df.iloc[idx]
    render_historical_surge_detail(str(row["ts_code"]), str(row.get("name", "")), sel, store)


# ── 页面主体 ──────────────────────────────────────────────────────────────────

st.markdown(_PANORAMA_CSS, unsafe_allow_html=True)

head_l, head_r = st.columns([5, 1])
with head_l:
    st.markdown("### 盘中市场全景")
with head_r:
    if st.button("🔄 立即刷新", width="stretch", help="读取最新发布的 Serving 代"):
        st.rerun()
st.caption(
    f"页面每 {REFRESH_SECONDS}s 读取一次最新不可变 Serving 代 · "
    "快照、板块、分时、日 K 与爆量台账同代一致 · 不连接生产主库"
)


@st.fragment(run_every=REFRESH_SECONDS)
def render_body() -> None:
    tab_panorama, tab_surge = st.tabs(["市场全景", "爆量记录"])
    try:
        with open_panorama_serving_generation() as store:
            snapshot = load_market_snapshot_projection(store)
            raw_as_of = snapshot.attrs.get("as_of")
            parsed_as_of = pd.to_datetime(raw_as_of, utc=True, errors="coerce")
            if pd.isna(parsed_as_of):
                as_of = "—"
                age_seconds = None
            else:
                as_of = parsed_as_of.tz_convert(CST).strftime("%Y-%m-%d %H:%M:%S")
                age_seconds = max(
                    0.0,
                    (datetime.now(UTC) - parsed_as_of.to_pydatetime()).total_seconds(),
                )
            status = {
                "snapshot": {
                    "age_seconds": age_seconds,
                    "last_error": snapshot.attrs.get("serving_detail"),
                }
            }
            snap_route = str(snapshot.attrs.get("route") or "serving")

            with tab_panorama:
                render_pulse(snapshot, as_of, snap_route, status, store)
                left, right = st.columns([52, 48])
                with left:
                    board_code, board_name = render_overview(store, snap_route)
                with right:
                    ts_code, stock_name = render_drilldown(
                        board_code,
                        board_name,
                        snapshot,
                        store,
                    )
                    render_stock_chart(ts_code, stock_name, snapshot, store)

            with tab_surge:
                render_surge_log(store)
            serving_health = store.serving_health()
            if serving_health is not None:
                render_serving_state_banner(st, serving_health, label="市场全景数据")
    except Exception as error:
        detail = f"{type(error).__name__}: {error}"
        with tab_panorama:
            st.warning(f"Serving 数据不可用：{detail[:180]}")
        with tab_surge:
            st.warning(f"Serving 数据不可用：{detail[:180]}")


if __name__ == "__main__":
    render_body()
