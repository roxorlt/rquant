"""rQuant 运行控制台（只读）——独立 Streamlit 入口，也可挂载进 ``preview_app.py`` 多页导航。

独立启动方式::

    RQUANT_SERVING_ROOT=data/runtime/serving uv run streamlit run \\
        src/rquant/dashboard/runtime_console.py --server.port 8507

本页面只读取 ``runtime_console_data.load_runtime_console`` 返回的一份 serving
generation 快照，不做任何写入；不可用 / 缺失的字段一律渲染为「—」，不让页面崩溃。

不用 meta refresh 或 ``st.fragment(run_every=...)`` 自动刷新：本页跟健康看板
（``app.py``）挂在同一个 ``preview_app.py`` 多页 session 里，meta refresh 是整页
文档级刷新，会把用户从这一页拉回默认页（见 ``app.py`` 里的说明）；一个只读快照
页也不值得为了自动刷新去做 fragment 化重构。改用页头一个「🔄 刷新」按钮
+ ``st.rerun()``，跟 ``app.py`` 挂进 preview 时的做法一致。
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import streamlit as st

from rquant.dashboard.runtime_console_data import (
    ConsoleFreshness,
    ConsoleLoadState,
    DeliveryRow,
    LabJobRow,
    PaperAccountRow,
    PaperHoldingRow,
    PromotionRow,
    RuntimeConsoleSnapshot,
    RuntimeServiceRow,
    SignalRow,
    load_runtime_console,
)
from rquant.serving_paths import serving_root_from_env

CST = timezone(timedelta(hours=8))
_DASH = "—"
_PLANE_LABELS = {"live": "盘中链路", "serving": "服务层", "research": "研究任务"}
_FRESHNESS_LABELS = {
    ConsoleFreshness.FRESH: "新鲜",
    ConsoleFreshness.STALE: "已过期",
    ConsoleFreshness.DEGRADED: "降级",
    ConsoleFreshness.UNAVAILABLE: "不可用",
}
_STATUS_LABELS = {
    "missing": "缺失",
    "starting": "启动中",
    "running": "运行中",
    "degraded": "降级",
    "stopped": "已停止",
    "pending": "待运行",
    "queued": "排队中",
    "paused": "已暂停",
    "succeeded": "成功",
    "failed": "失败",
    "cancelled": "已取消",
    "delivered": "已送达",
}

_CSS = """
<style>
header[data-testid="stHeader"] {display: none;}
.block-container {max-width: 1500px; padding-top: 1rem; padding-bottom: 2rem;}
[data-testid="stMetric"] {padding: 0.1rem 0;}
[data-testid="stMetricValue"] {font-size: 1.35rem;}
[data-testid="stVerticalBlock"] {gap: 0.65rem;}
.rq-head {display:flex; align-items:baseline; justify-content:space-between; gap:1rem;}
.rq-head h1 {font-size:1.55rem; margin:0; letter-spacing:0;}
.rq-meta {color:#6b7280; font-size:0.78rem; overflow-wrap:anywhere;}
.rq-status {font-weight:650;}
.rq-fresh {color:#087f5b;}
.rq-stale {color:#9a6700;}
.rq-bad {color:#b42318;}
@media (max-width: 768px) {
  .block-container {padding:0.65rem 0.55rem 1.5rem;}
  .rq-head {align-items:flex-start; flex-direction:column; gap:0.2rem;}
  .rq-head h1 {font-size:1.32rem;}
  [data-testid="stHorizontalBlock"] {flex-wrap:wrap; gap:0.45rem !important;}
  [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {
    min-width:100% !important; flex:1 1 100% !important;
  }
  .st-key-runtime_metrics [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {
    min-width:calc(50% - 0.45rem) !important; flex:1 1 calc(50% - 0.45rem) !important;
  }
  [data-testid="stDataFrame"] {overflow-x:auto;}
}
</style>
"""


def _time(value: datetime | None) -> str:
    """serving 行里的时间戳可能是 NULL（字段尚未接入）；空值一律显示占位符，不抛异常。"""
    if value is None:
        return _DASH
    return value.astimezone(CST).strftime("%m-%d %H:%M:%S")


def _number(value: Decimal | None) -> str:
    if value is None:
        return _DASH
    return f"{value:,.2f}"


def _text(value: str | None) -> str:
    if not value:
        return _DASH
    return value


def _json_text(value: str | None) -> str:
    if not value:
        return _DASH
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return value
    if isinstance(parsed, list):
        return "、".join(str(item) for item in parsed) or _DASH
    return str(parsed)


def _table_height(row_count: int) -> int:
    return min(430, max(120, 38 + row_count * 35))


def _show_table(rows: list[dict[str, object]], *, empty: str) -> None:
    """空数据集（含"当前 generation 无该表数据"和"该表暂不可用"两种情况）都落到这一占位分支。"""
    if not rows:
        st.caption(empty)
        return
    st.dataframe(
        rows,
        hide_index=True,
        width="stretch",
        height=_table_height(len(rows)),
    )


def _service_rows(records: Iterable[RuntimeServiceRow]) -> list[dict[str, object]]:
    return [
        {
            "服务": item.service_id,
            "状态": "心跳过期" if item.stale else _STATUS_LABELS.get(item.status, item.status),
            "心跳": _time(item.heartbeat_at),
            "输入": item.input_sequence,
            "输出": item.output_sequence,
            "积压": item.backlog_count,
            "连续失败": item.consecutive_failures,
            "错误": _text(item.last_error),
        }
        for item in records
    ]


def _signal_rows(records: Iterable[SignalRow]) -> list[dict[str, object]]:
    return [
        {
            "序号": item.global_sequence,
            "时间": _time(item.available_at),
            "策略": item.strategy_id,
            "版本": item.strategy_version,
            "标的": item.candidate_id,
            "动作": item.action,
            "原因": _json_text(item.reason_codes_json),
            "过期": _time(item.expires_at),
        }
        for item in records
    ]


def _delivery_rows(records: Iterable[DeliveryRow]) -> list[dict[str, object]]:
    return [
        {
            "时间": _time(item.updated_at),
            "渠道": item.channel,
            "接收者": item.recipient_id,
            "状态": _STATUS_LABELS.get(item.status, item.status),
            "尝试": item.attempt_count,
            "信号": item.signal_id,
            "错误": _text(item.last_error),
        }
        for item in records
    ]


def _account_rows(records: Iterable[PaperAccountRow]) -> list[dict[str, object]]:
    return [
        {
            "账户": item.account_id,
            "净值": _number(item.nav),
            "现金": _number(item.cash),
            "可用现金": _number(item.available_cash),
            "冻结现金": _number(item.frozen_cash),
            "浮动盈亏": _number(item.unrealized_pnl),
            "已实现盈亏": _number(item.realized_pnl),
            "时间": _time(item.as_of_time),
        }
        for item in records
    ]


def _holding_rows(records: Iterable[PaperHoldingRow]) -> list[dict[str, object]]:
    return [
        {
            "账户": item.account_id,
            "标的": item.ts_code,
            "数量": _number(item.quantity),
            "可卖": _number(item.available_quantity),
            "成本": _number(item.average_cost),
            "现价": _number(item.market_price),
            "市值": _number(item.market_value),
            "浮动盈亏": _number(item.unrealized_pnl),
            "时间": _time(item.as_of_time),
        }
        for item in records
    ]


def _job_rows(records: Iterable[LabJobRow]) -> list[dict[str, object]]:
    return [
        {
            "策略": item.strategy_name,
            "状态": _STATUS_LABELS.get(item.status, item.status),
            "进度": f"{item.progress_fraction:.0%}",
            "阶段": item.phase,
            "分片": f"{item.terminal_shards}/{item.total_shards}",
            "ETA": _time(item.eta_finish_center),
            "ETA 区间": f"{_time(item.eta_finish_low)} 至 {_time(item.eta_finish_high)}",
            "资源": item.resource_class,
            "更新时间": _time(item.updated_at),
            "任务": item.job_id,
        }
        for item in records
    ]


def _promotion_rows(records: Iterable[PromotionRow]) -> list[dict[str, object]]:
    return [
        {
            "时间": _time(item.decided_at),
            "阶段": item.stage,
            "结论": "通过" if item.approved else "未通过",
            "实验": _json_text(item.experiment_ids_json),
            "未过门槛": _json_text(item.gate_failures_json),
            "决策": item.decision_id,
        }
        for item in records
    ]


def _render_header(snapshot: RuntimeConsoleSnapshot) -> None:
    freshness_class = {
        ConsoleFreshness.FRESH: "rq-fresh",
        ConsoleFreshness.STALE: "rq-stale",
        ConsoleFreshness.DEGRADED: "rq-bad",
        ConsoleFreshness.UNAVAILABLE: "rq-bad",
    }[snapshot.freshness]
    generation = snapshot.generation_id[:12] if snapshot.generation_id else "无可用 generation"
    commit = (snapshot.producer_commit or "")[:12] or _DASH
    col_head, col_refresh = st.columns([6, 1])
    with col_head:
        st.markdown(
            f"""
            <div class="rq-head">
              <h1>🖥️ rQuant 运行控制台</h1>
              <div class="rq-meta">generation {generation}</div>
            </div>
            <div class="rq-meta">
              生成 {_time(snapshot.generated_at)} ·
              <span class="rq-status {freshness_class}">
                {_FRESHNESS_LABELS[snapshot.freshness]}
              </span>
              · commit {commit}
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col_refresh:
        # 本页不做自动刷新（无 meta refresh，也没有用 fragment 轮询）——只读快照，
        # 手动点一下比后台定时拉更省事，也不会有任何页面被自动拉走的风险。
        if st.button("🔄 刷新", key="runtime_console_manual_refresh"):
            st.rerun()
    if snapshot.state is ConsoleLoadState.DEGRADED:
        st.error(f"Serving 降级：{snapshot.detail}")


def _render_services(snapshot: RuntimeConsoleSnapshot) -> None:
    st.subheader("服务健康")
    columns = st.columns(3)
    for column, plane in zip(columns, ("live", "serving", "research"), strict=True):
        with column:
            st.markdown(f"**{_PLANE_LABELS[plane]}**")
            records = [item for item in snapshot.services if item.plane == plane]
            _show_table(_service_rows(records), empty="暂无服务心跳")


def render_runtime_console(snapshot: RuntimeConsoleSnapshot) -> None:
    """渲染一份已加载的运行控制台快照；不读取 serving，便于测试与复用。"""

    _render_header(snapshot)
    if snapshot.freshness is ConsoleFreshness.UNAVAILABLE:
        return

    running_jobs = sum(item.status == "running" for item in snapshot.lab_jobs)
    failed_deliveries = sum(item.status == "failed" for item in snapshot.deliveries)
    with st.container(key="runtime_metrics"):
        metrics = st.columns(4)
        metrics[0].metric("Generation 年龄", f"{snapshot.age_seconds or 0} 秒")
        metrics[1].metric("近期信号", len(snapshot.signals))
        metrics[2].metric("推送失败", failed_deliveries)
        metrics[3].metric("运行中 Lab", running_jobs)

    _render_services(snapshot)

    left, right = st.columns(2)
    with left:
        st.subheader("信号")
        _show_table(_signal_rows(snapshot.signals), empty="当前 generation 无信号")
    with right:
        st.subheader("推送")
        _show_table(_delivery_rows(snapshot.deliveries), empty="当前 generation 无推送记录")

    st.subheader("模拟账户")
    _show_table(_account_rows(snapshot.paper_accounts), empty="当前 generation 无模拟账户")
    st.markdown("**持仓**")
    _show_table(_holding_rows(snapshot.paper_holdings), empty="当前 generation 无模拟持仓")

    st.subheader("Lab Jobs")
    st.caption("研究任务队列；serving 尚未接入该 projection 时显示占位，不代表出错。")
    _show_table(
        _job_rows(snapshot.lab_jobs),
        empty="当前 generation 无 Lab 任务，或该 projection 暂不可用",
    )

    st.subheader("策略晋级")
    st.caption("Paper → 实盘候选的晋级决策；serving 尚未接入该 projection 时显示占位，不代表出错。")
    _show_table(
        _promotion_rows(snapshot.promotions),
        empty="当前 generation 无策略晋级决策，或该 projection 暂不可用",
    )


def main() -> None:
    st.set_page_config(
        page_title="rQuant 运行控制台",
        page_icon="🖥️",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.markdown(_CSS, unsafe_allow_html=True)
    render_runtime_console(load_runtime_console(serving_root_from_env()))


if __name__ == "__main__":
    main()


__all__ = ["main", "render_runtime_console"]
