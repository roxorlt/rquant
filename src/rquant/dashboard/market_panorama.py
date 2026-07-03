"""rQuant 盘中市场全景 — 独立 Streamlit 应用（P0）。

启动方式（仅本地，东财源云端被屏蔽，不进 deploy/systemd）：
    PYTHONPATH=src streamlit run src/rquant/dashboard/market_panorama.py \\
        --server.port 8506 --server.headless true

端口约定：8501 健康 / 8502 nl_screen / 8503 nl_canvas / 8504 Lab / 8505 预留 / 8506 本页。

区块：市场脉搏条（涨停/跌停/炸板 + 上涨占比 + 当日 sparkline）、板块资金流排行、
板块成交额排行（dc_board_member 聚合，industry 兜底）、板块下钻成分表。
B 信号因子矩阵为 P2 占位。

刷新：st.fragment 60s 自动重跑，缓存分层 TTL——S1 快照 30s / S2 东财 60s / 本地副本 300s。
所有 DuckDB 读只走只读副本，绝不写主库（monitor 是唯一常驻写者）。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import streamlit as st

from rquant.panorama_data import (
    MarketPulse,
    add_limit_prices,
    aggregate_board_amount,
    board_constituents,
    compute_market_pulse,
    fetch_market_snapshot,
    fetch_sector_fund_flow,
    industry_fallback_members,
    load_board_members,
    load_pool_flags,
)

CST = timezone(timedelta(hours=8))
REFRESH_SECONDS = 60

st.set_page_config(
    page_title="rQuant 市场全景",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── 缓存层（TTL 矩阵见设计文档 §1） ────────────────────────────────────────────


@st.cache_data(ttl=30, show_spinner="拉取全市场快照…")
def cached_snapshot() -> tuple[pd.DataFrame, str]:
    df = add_limit_prices(fetch_market_snapshot())
    return df, datetime.now(CST).strftime("%H:%M:%S")


@st.cache_data(ttl=60, show_spinner=False)
def cached_fund_flow(sector_type: str) -> pd.DataFrame:
    return fetch_sector_fund_flow(sector_type)


@st.cache_data(ttl=300, show_spinner=False)
def cached_members() -> tuple[pd.DataFrame, str]:
    """成分映射：dc_board_member 优先，空则降级 stock_basic.industry。"""
    members = load_board_members()
    if not members.empty:
        return members, "dc"
    fallback = industry_fallback_members()
    return fallback, "industry"


@st.cache_data(ttl=300, show_spinner=False)
def cached_pool_flags() -> dict[str, str]:
    return load_pool_flags()


# ── 渲染helpers ───────────────────────────────────────────────────────────────


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


def render_pulse(snapshot: pd.DataFrame, as_of: str) -> None:
    st.subheader("市场脉搏")
    pulse = compute_market_pulse(snapshot)
    if pulse.total_count == 0:
        st.info("快照源暂不可用（sina），等下一轮刷新")
        return

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("涨停", pulse.limit_up_count)
    c2.metric("跌停", pulse.limit_down_count)
    c3.metric("炸板", pulse.broken_count)
    c4.metric("上涨占比", f"{pulse.up_ratio_pct:.1f}%")
    c5.metric("涨/跌家数", f"{pulse.up_count} / {pulse.down_count}")
    st.caption(f"快照时间 {as_of} · 有效样本 {pulse.total_count} 只（停牌除外）")

    spark = _record_pulse_history(pulse)
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
        st.altair_chart(chart, use_container_width=True)


def render_fund_flow() -> None:
    st.subheader("板块资金流排行（东财·今日）")
    sort_key = st.radio(
        "排序", ["净流入额", "净流入率"], horizontal=True, key="flow_sort",
        label_visibility="collapsed",
    )
    tabs = st.tabs(["行业", "概念"])
    for tab, sector_type in zip(tabs, ["行业资金流", "概念资金流"], strict=True):
        with tab:
            flow = cached_fund_flow(sector_type)
            if flow.empty:
                st.info("东财资金流源暂不可用（限频/反爬/网络），稍后自动重试")
                continue
            df = flow.copy()
            df["净流入额(亿)"] = (df["main_net_amount"] / 1e8).round(2)
            df = df.rename(
                columns={
                    "board_name": "板块",
                    "pct_chg": "涨跌幅%",
                    "main_net_rate": "净流入率%",
                    "leading_stock": "主力流入最大股",
                }
            )
            by = "净流入额(亿)" if sort_key == "净流入额" else "净流入率%"
            df = df.sort_values(by, ascending=False)
            st.dataframe(
                df[["板块", "净流入额(亿)", "净流入率%", "涨跌幅%", "主力流入最大股"]].head(20),
                use_container_width=True,
                hide_index=True,
            )


def render_amount_rank(snapshot: pd.DataFrame, members: pd.DataFrame, source: str) -> str | None:
    """成交额排行 + 返回当前选中板块体系的 idx_type（供下钻区共用）。"""
    st.subheader("板块成交额排行")
    if source == "industry":
        st.caption("dc_board_member 不可用，已降级 stock_basic.industry 粗分")
    if snapshot.empty or members.empty:
        st.info("快照或板块成分不可用，暂无排行")
        return None

    idx_type = st.radio(
        "板块体系", ["行业板块", "概念板块"], horizontal=True, key="amount_idx_type",
        label_visibility="collapsed",
    )
    agg = aggregate_board_amount(snapshot, members, idx_type=idx_type)
    if agg.empty:
        st.info(f"{idx_type}聚合为空")
        return idx_type
    df = agg.copy()
    df["成交额(亿)"] = (df["amount"] / 1e8).round(1)
    df["涨跌幅中位%"] = df["pct_chg_median"].round(2)
    df = df.rename(
        columns={"board_name": "板块", "limit_up_count": "涨停家数", "stock_count": "成分数"}
    )
    st.dataframe(
        df[["板块", "成交额(亿)", "涨跌幅中位%", "涨停家数", "成分数"]].head(20),
        use_container_width=True,
        hide_index=True,
    )
    return idx_type


def render_drilldown(
    snapshot: pd.DataFrame, members: pd.DataFrame, idx_type: str | None
) -> None:
    st.subheader("板块下钻")
    if snapshot.empty or members.empty or idx_type is None:
        st.info("快照或板块成分不可用，暂无下钻")
        return
    agg = aggregate_board_amount(snapshot, members, idx_type=idx_type)
    if agg.empty:
        st.info("暂无可下钻板块")
        return

    options = agg["board_code"].tolist()
    labels = dict(zip(agg["board_code"], agg["board_name"], strict=True))
    amounts = dict(zip(agg["board_code"], agg["amount"], strict=True))
    board_code = st.selectbox(
        "选择板块（按成交额降序）",
        options,
        format_func=lambda c: f"{labels[c]}（{amounts[c] / 1e8:.1f} 亿）",
        key="drill_board",
    )
    cons = board_constituents(board_code, members, snapshot, cached_pool_flags())
    if cons.empty:
        st.info("该板块成分与快照无交集")
        return
    df = cons.copy()
    df["成交额(亿)"] = (df["amount"] / 1e8).round(2)
    df["现价"] = df["price"].round(2)
    df["涨幅%"] = df["pct_chg"].round(2)
    df["涨停"] = df["is_limit_up"].map({True: "🔴", False: ""})
    df = df.rename(columns={"ts_code": "代码", "name": "名称", "pools": "池内"})
    st.dataframe(
        df[["代码", "名称", "现价", "涨幅%", "成交额(亿)", "涨停", "池内"]],
        use_container_width=True,
        hide_index=True,
        height=420,
    )
    st.caption("B 信号多因子矩阵（勾选成分股 → 分钟档因子命中灯）为 P2 交付，此处占位。")


# ── 页面主体 ──────────────────────────────────────────────────────────────────

st.title("盘中市场全景")
st.caption(
    "仅本地运行（东财源云端被屏蔽）· 快照 30s / 东财 60s / 本地副本 300s 缓存 · "
    f"页面 {REFRESH_SECONDS}s 自动刷新 · 只读副本，不写主库"
)

if st.button("立即刷新（清缓存）"):
    st.cache_data.clear()


@st.fragment(run_every=REFRESH_SECONDS)
def render_body() -> None:
    snapshot, as_of = cached_snapshot()
    members, member_source = cached_members()

    render_pulse(snapshot, as_of)
    st.divider()

    left, right = st.columns(2)
    with left:
        render_fund_flow()
    with right:
        idx_type = render_amount_rank(snapshot, members, member_source)
    st.divider()
    render_drilldown(snapshot, members, idx_type)


render_body()
