"""Plain Chinese names for what the pages show; technical ids stay out of the main text.

Pages show these names; the technical id (``notifier.admin.shadow.v1``, ``lab_jobs``,
``pulse_alert``) goes into a tooltip or a detail drawer only (``web/CLAUDE.md``
「界面与文案原则」). Unknown ids fall back to a generic name rather than the raw id.
"""

from __future__ import annotations

import re

#: Strategy / candidate instance → name (instances of ``strategy.*`` and ``candidate.*``).
STRATEGY_LABELS: dict[str, str] = {
    "auction_gap": "竞价跳空",
    "n_shape": "N 字",
    "growth_board_surge": "创业科创放量",
}

#: Daily screen presets (``canvas_hit.preset_name``).
PRESET_LABELS: dict[str, str] = {
    "n-shape-pool1": "N 字一池",
    "n-shape-pool2": "N 字二池",
}

_ROLE_LABELS: dict[str, str] = {
    "artifact-catalog": "研究产物目录",
    "artifact-retention": "研究产物清理",
    "auction-match": "竞价撮合数据",
    "auction-universe": "竞价股票池",
    "candidate": "候选生成",
    "daily": "日终编排",
    "daily-close": "收盘数据",
    "daily-orchestrator": "日终编排",
    "feature": "分钟特征",
    "lab-jobs": "研究任务",
    "market-minute": "分钟行情",
    "notifier": "通知推送",
    "paper-broker": "模拟撮合",
    "paper-constraint": "模拟成交约束",
    "promotions": "策略晋级",
    "recovery": "故障恢复",
    "recovery-rehearsal": "恢复演练",
    "reference-slow": "参考数据",
    "runtime-health": "健康汇总",
    "serving": "页面数据发布",
    "serving-publisher": "页面数据发布",
    "shadow": "新旧对照",
    "signal-router": "信号路由",
    "strategy": "策略评估",
    "watchlist-quote": "盯盘报价",
}

_INSTANCE_OVERRIDES: dict[tuple[str, str], str] = {
    ("reference-slow", "publisher"): "参考数据发布",
    ("reference-slow", "source"): "参考数据采集",
}

PLANE_LABELS: dict[str, str] = {
    "live": "盘中",
    "serving": "页面数据",
    "research": "研究",
}

ACTION_LABELS: dict[str, str] = {
    "watch": "观察",
    "b_intent": "买入意向",
    "reduce": "减仓",
    "s_intent": "卖出意向",
    "cancel": "撤销",
}

#: Dataset watermarks (``manifest.watermarks[].dataset_id``).
DATASET_LABELS: dict[str, str] = {
    "signals": "盘中信号",
    "paper_accounts": "模拟账户",
    "runtime_health": "服务心跳",
    "reference_slow": "参考数据",
    "reference_slow_authority": "参考数据签发",
    "reference_slow_contract": "参考数据校验",
    "lab_jobs": "研究任务",
    "promotions": "策略晋级",
}

#: Page tables (``projection_status.table_name``), named after what they feed.
TABLE_LABELS: dict[str, str] = {
    "canvas_definition": "池子画布定义",
    "canvas_diagnostic": "池子逐条命中",
    "canvas_hit": "池子成员",
    "canvas_latest_trade_date": "池子日期",
    "daily_bar": "日 K",
    "dashboard_summary": "运维摘要",
    "dc_board": "东财板块",
    "dc_board_member": "东财板块成分",
    "intraday_kline": "分时",
    "kpl_concept_member": "开盘啦题材成分",
    "market_liquidity": "流动性基准",
    "market_overview": "板块总表",
    "market_snapshot": "全市场快照",
    "minute_coverage": "分钟线覆盖",
    "monitor_event": "盯盘事件",
    "nl_screen_universe": "选股股票池",
    "pool2_watch": "二池盯盘",
    "pulse_alert": "脉搏异动提醒",
    "pulse_history": "脉搏历史",
    "research_gate_metadata": "研究门槛",
    "risk_blacklist": "风险黑名单",
    "screen_bounds": "选股结果范围",
    "screen_result": "选股结果",
    "stock_basic": "股票列表",
    "strategy_summary": "回测汇总",
    "strategy_trade": "回测交易",
    "surge_event": "爆量记录",
    "surge_runtime_config": "爆量参数",
    "trade_calendar": "交易日历",
}

DELIVERY_LABELS: dict[str, str] = {
    "pending": "待发送",
    "leased": "发送中",
    "retry": "重试中",
    "succeeded": "已送达",
    "expired": "已过期",
    "dead_letter": "失败",
}

CHANNEL_LABELS: dict[str, str] = {"pushdeer": "PushDeer", "pushplus": "PushPlus"}

_VERSION_SUFFIX = re.compile(r"\.v\d+$")


def split_service_id(service_id: str) -> tuple[str, str]:
    """``notifier.admin.shadow.v1`` → (``notifier``, ``admin.shadow``)."""

    body = _VERSION_SUFFIX.sub("", service_id)
    role, _, instance = body.partition(".")
    return role, instance


def service_label(service_id: str) -> str:
    role, instance = split_service_id(service_id)
    override = _INSTANCE_OVERRIDES.get((role, instance.split(".", 1)[0]))
    if override is not None:
        return override
    if role in {"strategy", "candidate"} and instance in STRATEGY_LABELS:
        suffix = "策略" if role == "strategy" else "候选"
        return f"{STRATEGY_LABELS[instance]}{suffix}"
    return _ROLE_LABELS.get(role, "其他服务")


def strategy_label(strategy_id: str) -> str:
    return STRATEGY_LABELS.get(strategy_id, "其他策略")


def dataset_label(dataset_id: str) -> str:
    return DATASET_LABELS.get(dataset_id, "其他数据")


def table_label(table_name: str) -> str:
    return TABLE_LABELS.get(table_name, "其他数据表")


__all__ = [
    "ACTION_LABELS",
    "CHANNEL_LABELS",
    "DATASET_LABELS",
    "DELIVERY_LABELS",
    "PLANE_LABELS",
    "PRESET_LABELS",
    "STRATEGY_LABELS",
    "TABLE_LABELS",
    "dataset_label",
    "service_label",
    "split_service_id",
    "strategy_label",
    "table_label",
]
