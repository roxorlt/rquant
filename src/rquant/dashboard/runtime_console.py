"""rQuant read-only runtime console.

Run locally with::

    RQUANT_SERVING_ROOT=data/serving PYTHONPATH=src streamlit run \
        src/rquant/dashboard/runtime_console.py --server.port 8507
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

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

SHANGHAI = ZoneInfo("Asia/Shanghai")
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
    min-width:calc(50% - 0.45rem) !important; flex:1 1 calc(50% - 0.45rem) !important;
  }
  [data-testid="stDataFrame"] {overflow-x:auto;}
}
</style>
"""


def _time(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.astimezone(SHANGHAI).strftime("%m-%d %H:%M:%S")


def _number(value: Decimal) -> str:
    return f"{value:,.2f}"


def _json_text(value: str) -> str:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return value
    if isinstance(parsed, list):
        return "、".join(str(item) for item in parsed) or "-"
    return str(parsed)


def _table_height(row_count: int) -> int:
    return min(430, max(120, 38 + row_count * 35))


def _show_table(rows: list[dict[str, object]], *, empty: str) -> None:
    if not rows:
        st.caption(empty)
        return
    st.dataframe(
        rows,
        hide_index=True,
        use_container_width=True,
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
            "错误": item.last_error or "-",
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
            "错误": item.last_error or "-",
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
    st.markdown(
        f"""
        <div class="rq-head">
          <h1>rQuant 运行控制台</h1>
          <div class="rq-meta">generation {generation}</div>
        </div>
        <div class="rq-meta">
          生成 {_time(snapshot.generated_at)} ·
          <span class="rq-status {freshness_class}">
            {_FRESHNESS_LABELS[snapshot.freshness]}
          </span>
          · commit {(snapshot.producer_commit or "-")[:12]}
        </div>
        """,
        unsafe_allow_html=True,
    )
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


def _render_snapshot(snapshot: RuntimeConsoleSnapshot) -> None:
    _render_header(snapshot)
    if snapshot.freshness is ConsoleFreshness.UNAVAILABLE:
        return

    running_jobs = sum(item.status == "running" for item in snapshot.lab_jobs)
    failed_deliveries = sum(item.status == "failed" for item in snapshot.deliveries)
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
    _show_table(_job_rows(snapshot.lab_jobs), empty="当前 generation 无 Lab 任务")

    st.subheader("策略晋级")
    _show_table(_promotion_rows(snapshot.promotions), empty="当前 generation 无策略晋级决策")


def _serving_root() -> Path:
    return Path(os.environ.get("RQUANT_SERVING_ROOT", "data/serving"))


def main() -> None:
    st.set_page_config(
        page_title="rQuant 运行控制台",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.markdown(_CSS, unsafe_allow_html=True)

    @st.fragment(run_every="15s")
    def render_current() -> None:
        _render_snapshot(load_runtime_console(_serving_root()))

    render_current()


if __name__ == "__main__":
    main()


__all__ = ["main"]
