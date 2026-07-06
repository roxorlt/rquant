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

取数架构（2026-07-06 盘中事故后重构）：快照 + 行业/概念资金流由 SourcePoller
后台 daemon 线程 60s 循环拉取（每源独立熔断，sina 兜底级单独熔断），UI 只读
last-known-good slot——渲染永不等取数。fragment run_every=60 只做纯渲染零网络；
「立即刷新」触发 poller 立即开跑一轮（不阻塞渲染）。合表/下钻 st.cache_data 120s
（键含 poller 给的 as_of，as_of 不变即命中）、本地副本 300s、分时 60s、日K 600s。
所有 DuckDB 读只经 panorama_data（只读副本），UI 层绝不直连主库。
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import streamlit as st
from streamlit.errors import StreamlitAPIException

from rquant.panorama_data import (
    BOARD_SYSTEMS,
    ROUTE_LABELS,
    MarketPulse,
    board_constituents,
    build_board_overview,
    compute_market_pulse,
    fetch_intraday_trend,
    industry_fallback_members,
    load_board_members,
    load_daily_kline,
    load_kpl_concept_members,
    load_liquidity_baseline,
    load_pool_flags,
)
from rquant.panorama_poller import SourcePoller

CST = timezone(timedelta(hours=8))
REFRESH_SECONDS = 60
_KPL_SYSTEM = "开盘啦题材"

# 体系 → 资金流 sector_type；开盘啦题材不拉资金流（传空表，build_board_overview 补 NaN 列）
_SYSTEM_FLOW_TYPE: dict[str, str | None] = {
    "东财行业": "行业资金流",
    "东财概念": "概念资金流",
    _KPL_SYSTEM: None,
}

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
</style>
"""

st.set_page_config(
    page_title="rQuant 市场全景",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── 取数层（快照/资金流走后台 poller，本地副本走 st.cache_data） ───────────────


@st.cache_resource(show_spinner=False)
def get_poller() -> SourcePoller:
    """进程级单例后台拉取器（所有浏览器会话共享，消掉会话数×源数的请求放大）。"""
    poller = SourcePoller(interval=float(REFRESH_SECONDS))
    poller.start()
    return poller


@st.cache_data(ttl=300, show_spinner=False)
def cached_members() -> tuple[pd.DataFrame, str]:
    """东财成分：dc_board_member 优先，空则降级 stock_basic.industry 粗分。"""
    members = load_board_members()
    if not members.empty:
        return members, "dc"
    return industry_fallback_members(), "industry"


@st.cache_data(ttl=300, show_spinner=False)
def cached_kpl_members() -> pd.DataFrame:
    return load_kpl_concept_members()


@st.cache_data(ttl=300, show_spinner=False)
def cached_pool_flags() -> dict[str, str]:
    return load_pool_flags()


@st.cache_data(ttl=300, show_spinner=False)
def cached_liquidity() -> pd.DataFrame:
    return load_liquidity_baseline()


@st.cache_data(ttl=120, show_spinner=False)
def cached_overview(system: str, as_of: str) -> tuple[pd.DataFrame, str | None]:
    """合并板块总表（键=体系+快照时间戳；内部自取上游缓存，大表不入参免哈希开销）。

    返回 (总表, 资金流路由)；开盘啦题材不拉资金流 → 路由 None、资金流三列全 NaN。
    """
    poller = get_poller()
    snapshot, _, _ = poller.snapshot()
    members, _ = cached_members()
    kpl_members = cached_kpl_members()
    flow_type = _SYSTEM_FLOW_TYPE.get(system)
    if flow_type is not None:
        flow, route = poller.flow(flow_type)
    else:
        flow, route = pd.DataFrame(), None
    overview = build_board_overview(snapshot, members, kpl_members, flow, system)
    return overview, route


@st.cache_data(ttl=120, show_spinner=False)
def cached_constituents(board_code: str, as_of: str) -> pd.DataFrame:
    """板块下钻成分表（键=板块码+快照时间戳）。

    board_code 在东财（BKxxxx.DC）/ 开盘啦（xxxxxx.KP）/ industry 兜底三套命名空间内
    互不重叠，故东财 + 开盘啦成分按 board_code 合一即可唯一定位，无需再传体系。
    """
    snapshot, _, _ = get_poller().snapshot()
    members, _ = cached_members()
    kpl_members = cached_kpl_members()
    combined = _combined_members(members, kpl_members)
    return board_constituents(
        board_code,
        combined,
        snapshot,
        pool_flags=cached_pool_flags(),
        liquidity=cached_liquidity(),
    )


@st.cache_data(ttl=60, show_spinner=False)
def cached_trend(ts_code: str, ndays: int) -> tuple[pd.DataFrame, str]:
    """个股分时（ndays=1）/ 5日线（ndays=5）+ 路由标签。"""
    trend = fetch_intraday_trend(ts_code, ndays)
    return trend, trend.attrs.get("route", "none")


@st.cache_data(ttl=600, show_spinner=False)
def cached_kline(ts_code: str) -> pd.DataFrame:
    """日K 基础序列（只读副本 daily_bar）；当日临时 bar 拼接在 UI 层做（依赖实时快照）。"""
    return load_daily_kline(ts_code)


# ── 数据整形 helpers ──────────────────────────────────────────────────────────


def _combined_members(members: pd.DataFrame, kpl_members: pd.DataFrame) -> pd.DataFrame:
    """东财成分 + 开盘啦成分按 board_code 合一（下钻只用 board_code/con_code，丢 idx_type）。"""
    cols = ["board_code", "board_name", "con_code"]
    frames = [
        df[cols]
        for df in (members, kpl_members)
        if not df.empty and set(cols).issubset(df.columns)
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


def _append_intraday_bar(
    kline: pd.DataFrame, snapshot: pd.DataFrame, ts_code: str
) -> pd.DataFrame:
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


def render_pulse(snapshot: pd.DataFrame, as_of: str, snap_route: str, status: dict) -> None:
    pulse = compute_market_pulse(snapshot)
    if pulse.total_count == 0:
        # slot 保留 last-known-good：空快照 ⇔ 后台从未成功过（首轮或全路由熔断）
        st.info("首轮拉取进行中（东财直连 → SOCKS 出口 → 新浪 依次尝试），页面不阻塞，稍候自动出数")
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
    with c6, st.popover("📈", width="stretch"):
        st.caption(f"快照 {as_of} · 有效样本 {pulse.total_count} 只（停牌除外）")
        if not spark.empty and spark["time"].nunique() >= 2:
            chart = (
                alt.Chart(spark)
                .mark_line(point=True)
                .encode(
                    x=alt.X("time:O", title=None),
                    y=alt.Y("家数:Q", title=None),
                    color=alt.Color("指标:N", legend=alt.Legend(orient="top", title=None)),
                    tooltip=["time", "指标", "家数"],
                )
                .properties(height=160)
            )
            st.altair_chart(chart, width="stretch")
        else:
            st.caption("脉搏曲线累积中（需 ≥2 分钟样本）")


# ── 左栏：板块合并总表 ────────────────────────────────────────────────────────


def render_overview(as_of: str, snap_route: str) -> tuple[str | None, str | None]:
    """渲染体系切换 + 合并总表，返回选中板块 (board_code, board_name)。

    体系 segmented_control 变更 → dataframe key 随体系切换 → 旧选择自然失效回退默认第一行
    （streamlit 无法程序化清空行选择，改 key 是绕过该限制的既定手法）。
    """
    system = st.segmented_control(
        "体系",
        list(BOARD_SYSTEMS),
        default=_KPL_SYSTEM,
        key="sys_seg",
        label_visibility="collapsed",
    ) or _KPL_SYSTEM
    is_kpl = system == _KPL_SYSTEM

    overview, route = cached_overview(system, as_of)
    if overview.empty:
        st.info(f"{system}：快照或板块成分不可用，暂无总表")
        return None, None
    # 展示层默认涨停数降序（次键成交额防并列）——数据层契约（amount 降序）不动；
    # 默认选中第一行随之变成涨停最多的板块，列头点击排序仍是客户端零 rerun
    overview = overview.sort_values(
        ["limit_up_count", "amount"], ascending=False
    ).reset_index(drop=True)

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
    board_code: str | None, board_name: str | None, as_of: str
) -> tuple[str | None, str | None]:
    """渲染选中板块的下钻成分表，返回选中个股 (ts_code, name)。"""
    title = board_name or "—"
    st.markdown(f"**「{title}」成分 · 强度分默认序**")
    if board_code is None:
        st.info("左侧选板块后查看成分")
        return None, None

    cons = cached_constituents(board_code, as_of)
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


def _trend_axis(trend: pd.DataFrame) -> tuple[list[int], str]:
    """分时/5日 x 轴（bar 序号 idx）刻度定位 + 标签映射表达式。

    单日 → 5 个时段刻度（idx 0/1/4·1/2·3/4·末，标签 09:30/10:30/11:30·13:00/14:00/15:00，
    午休折点用「11:30/13:00」双标）；多日 → 每日首根 idx 打日期标签（MM-DD）。
    labelExpr 用 Vega indexof 把刻度值查回标签，axis values 限定刻度只落在这些 idx，
    故 datum.value 必命中数组、无 -1 越界。
    """
    day = pd.to_datetime(trend["dt"]).dt.normalize()
    day_starts = day.drop_duplicates()
    if len(day_starts) <= 1:
        n = len(trend)
        values = sorted({0, n // 4, n // 2, 3 * n // 4, n - 1})
        labels = ["09:30", "10:30", "11:30/13:00", "14:00", "15:00"][: len(values)]
    else:
        values = [int(pos) for pos in day_starts.index]
        labels = [d.strftime("%m-%d") for d in day_starts]
    vals_arr = "[" + ",".join(str(v) for v in values) + "]"
    labels_arr = "[" + ",".join(f"'{lbl}'" for lbl in labels) + "]"
    label_expr = f"{labels_arr}[indexof({vals_arr}, datum.value)]"
    return values, label_expr


def _trend_chart(trend: pd.DataFrame) -> alt.VConcatChart:
    """分时/5日：价格线 + 均价虚线（有则画）+ 底部量柱，x 轴共享。

    x 轴用 bar 序号 idx（quantitative）而非真实时间 dt——非交易时段（午休/隔夜）
    不占轴距，线天然连续无空档；轴刻度经 labelExpr 映射回时间/日期，tooltip 保留真实 dt。
    """
    trend = trend.reset_index(drop=True).assign(idx=lambda d: range(len(d)))
    has_avg = trend["avg_price"].notna().any()
    values, label_expr = _trend_axis(trend)
    x_scale = alt.Scale(nice=False, zero=False)
    dt_tip = alt.Tooltip("dt:T", title="时间", format="%m-%d %H:%M")
    x_price = alt.X(
        "idx:Q", title=None, scale=x_scale, axis=alt.Axis(labels=False, ticks=False)
    )
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
    price = alt.layer(*layers).properties(height=220)
    x_vol = alt.X(
        "idx:Q",
        title=None,
        scale=x_scale,
        axis=alt.Axis(values=values, labelExpr=label_expr, labelAngle=0, labelOverlap=False),
    )
    vol = (
        alt.Chart(trend)
        .mark_bar(color="#94a3b8")
        .encode(
            x=x_vol,
            y=alt.Y("volume:Q", title=None),
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

    rule = alt.Chart(df).mark_rule().encode(
        x=x_price,
        y=alt.Y("low:Q", title=None, scale=alt.Scale(zero=False)),
        y2="high:Q",
        color=_CANDLE_COLOR,
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
    ts_code: str | None, name: str | None, snapshot: pd.DataFrame
) -> None:
    st.markdown(f"**个股图表 · {name or '—'}**")
    period = st.segmented_control(
        "周期",
        ["分时", "5日", "日K"],
        default="分时",
        key="chart_period",
        label_visibility="collapsed",
    ) or "分时"

    if ts_code is None:
        st.info("点选个股查看图表")
        return

    if period in ("分时", "5日"):
        ndays = 1 if period == "分时" else 5
        trend, route = cached_trend(ts_code, ndays)
        if trend.empty:
            st.info(f"{period}数据暂不可用")
            return
        st.altair_chart(_trend_chart(trend), width="stretch")
        st.caption(f"数据路由：{ROUTE_LABELS.get(route, route)}")
    else:
        kline = _append_intraday_bar(cached_kline(ts_code), snapshot, ts_code)
        if kline.empty:
            st.info("日K 数据暂不可用（本地副本 daily_bar 缺该票）")
            return
        st.altair_chart(_kline_chart(kline), width="stretch")


# ── 页面主体 ──────────────────────────────────────────────────────────────────

st.markdown(_PANORAMA_CSS, unsafe_allow_html=True)

head_l, head_r = st.columns([5, 1])
with head_l:
    st.markdown("### 盘中市场全景")
with head_r:
    if st.button("🔄 立即刷新", width="stretch", help="触发后台立即拉取一轮（不阻塞渲染）"):
        get_poller().refresh_now()
st.caption(
    f"仅本地运行 · 后台拉取器 {REFRESH_SECONDS}s（快照/资金流，每源独立熔断）· "
    "本地副本 300s / 合表下钻 120s / 分时 60s / 日K 600s 缓存 · "
    f"页面 {REFRESH_SECONDS}s 自动刷新（纯渲染零等待）· 只读副本，不写主库"
)


@st.fragment(run_every=REFRESH_SECONDS)
def render_body() -> None:
    poller = get_poller()
    snapshot, as_of, snap_route = poller.snapshot()
    render_pulse(snapshot, as_of, snap_route, poller.status())

    if not as_of:
        # 首拉未完成：提示已经画上（render_pulse 首轮 info），1s 后重跑再看 slot
        # ——只重读内存 slot 不碰网络，避免冷启动空页面干等 60s fragment 周期。
        # scope="fragment" 仅在 fragment rerun 中合法，初始整页运行时降级整页重跑
        time.sleep(1.0)
        try:
            st.rerun(scope="fragment")
        except StreamlitAPIException:
            st.rerun()

    left, right = st.columns([52, 48])
    with left:
        board_code, board_name = render_overview(as_of, snap_route)
    with right:
        ts_code, stock_name = render_drilldown(board_code, board_name, as_of)
        render_stock_chart(ts_code, stock_name, snapshot)


render_body()
