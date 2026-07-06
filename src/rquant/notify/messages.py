"""5 类场景的消息构造器。

每个 build_<scene>(**kwargs) -> (title, body)。
"""

from __future__ import annotations

import traceback
from datetime import date, datetime


def build_message(scene: str, **kwargs) -> tuple[str, str]:
    """根据 scene 路由到对应构造器。"""
    builders = {
        "price_level": _build_price_level,
        "pool2_exit": _build_pool2_exit,
        "daily_summary": _build_daily_summary,
        "error": _build_error,
        "heartbeat": _build_heartbeat,
        "morning_pulse": _build_prerendered,
        "midday_report": _build_prerendered,
    }
    builder = builders.get(scene)
    if builder is None:
        raise ValueError(f"未知 scene: {scene}")
    return builder(**kwargs)


_LEVEL_LABELS = {
    "40": "40%",
    "30": "30%",
    "20": "20%",
    "strong": "强止",
    "weak": "弱止",
    "attack_open_strength": "开盘强",
    "attack_strong_carry": "强承接",
    "attack_break_high": "突破T高",
    "attack_near_limit": "临近涨停",
}


def _build_prerendered(*, title: str, body: str) -> tuple[str, str]:
    """F/G. 盘中脉搏 / 午间战报：报文已在 midday_briefing 渲染好，此处直通。"""
    return title, body


def _build_price_level(
    *,
    ts_code: str,
    name: str,
    level: str,
    trigger_price: float,
    body_upper: float,
    body_lower: float,
    level_40: float,
    level_30: float,
    level_20: float,
    stop_strong: float,
    stop_weak: float,
    pool: str,
    entry_date: date | str,
    days_in_pool: int,
    blacklist_label: str | None = None,
    blacklist_categories: list[str] | None = None,
) -> tuple[str, str]:
    """A. 档位触发。"""
    label = _LEVEL_LABELS.get(level, level)
    kind = "信号" if level.startswith("attack_") else "档"
    prefix = f"[{blacklist_label}] " if blacklist_label else ""
    title = f"{prefix}{ts_code} {name} {label}{kind} ¥{trigger_price:.2f}"

    entry_str = str(entry_date)[:10]
    date_label = "入池" if pool == "pool2" else "涨停"
    body_lines = [
        f"# {ts_code} | {label}{kind}触发 | 现价 ¥{trigger_price:.2f}",
    ]
    if blacklist_label:
        cat_text = "、".join(blacklist_categories or []) if blacklist_categories else ""
        body_lines.append(
            f"> ⚠️ 在 **{blacklist_label}** 中"
            + (f"：{cat_text}" if cat_text else "")
        )
    body_lines.extend([
        f"- bodyTop: ¥{body_upper:.2f}",
        f"- 40档:    ¥{level_40:.2f}",
        f"- 30档:    ¥{level_30:.2f}",
        f"- 20档:    ¥{level_20:.2f}",
        f"- bodyBtm: ¥{body_lower:.2f}",
        f"- 强止：¥{stop_strong:.2f} | 弱止：¥{stop_weak:.2f}",
        f"- {pool}，{date_label} {entry_str} 第 {days_in_pool} 日",
    ])
    return title, "\n".join(body_lines)


def _build_pool2_exit(
    *,
    trade_date: date | str,
    auto_kicked: list[dict],
    expired_held: list[dict],
) -> tuple[str, str]:
    """B. Pool 2 退出汇总。

    auto_kicked: kind=breakdown {ts_code, name, close, threshold, reason_label}
                 kind=aged_out  {ts_code, name, entry_date, days_in_pool, threshold_days}
                 缺省 kind 视为 breakdown（向后兼容）
    expired_held: [{ts_code, name, entry_date, days_in_pool}]
    """
    date_str = str(trade_date)[5:10]  # MM-DD
    title = (
        f"Pool 2 退出 {date_str}: "
        f"踢出 {len(auto_kicked)} / 待决策 {len(expired_held)}"
    )

    breakdown_items = [it for it in auto_kicked if it.get("kind", "breakdown") == "breakdown"]
    aged_items = [it for it in auto_kicked if it.get("kind") == "aged_out"]

    parts: list[str] = []
    if breakdown_items:
        parts.append("## 自动踢出（跌破止损）")
        for it in breakdown_items:
            tag = f" [{it['blacklist_label']}]" if it.get("blacklist_label") else ""
            parts.append(
                f"- {it['ts_code']} {it.get('name', '')}{tag} "
                f"收盘 ¥{it['close']:.2f} < {it['reason_label']} ¥{it['threshold']:.2f}"
            )
    if aged_items:
        if parts:
            parts.append("")
        parts.append("## 自动踢出（超期）")
        for it in aged_items:
            entry_str = str(it["entry_date"])[5:10]
            tag = f" [{it['blacklist_label']}]" if it.get("blacklist_label") else ""
            parts.append(
                f"- {it['ts_code']} {it.get('name', '')}{tag} "
                f"入池 {entry_str} 第 {it['days_in_pool']} 日"
                f"（阈值 {it['threshold_days']} 日）"
            )
    if expired_held:
        if parts:
            parts.append("")
        parts.append("## 待决策（超期已保留）")
        for it in expired_held:
            entry_str = str(it["entry_date"])[5:10]
            tag = f" [{it['blacklist_label']}]" if it.get("blacklist_label") else ""
            parts.append(
                f"- {it['ts_code']} {it.get('name', '')}{tag} "
                f"入池 {entry_str} 第 {it['days_in_pool']} 日"
            )
            parts.append(f"  → `rquant pool2 remove {it['ts_code']}`")

    body = "\n".join(parts) if parts else "（今日无退出事件）"
    return title, body


def _build_daily_summary(
    *,
    trade_date: date | str,
    pool1_hits: list[dict],
    pool2_active: list[dict],
    duration_seconds: float,
) -> tuple[str, str]:
    """C. 每日筛选汇总。

    pool1_hits: [{ts_code, name, close}]
    pool2_active: [{ts_code, entry_date, days_in_pool}]
    """
    date_str = str(trade_date)[5:10]
    title = (
        f"每日选股 {date_str}: "
        f"P1 命中 {len(pool1_hits)}, P2 持仓 {len(pool2_active)}"
    )

    parts: list[str] = ["## Pool 1 候选"]
    if pool1_hits:
        for it in pool1_hits:
            tag = f" [{it['blacklist_label']}]" if it.get("blacklist_label") else ""
            parts.append(
                f"- {it['ts_code']} {it.get('name', '')}{tag} 收 ¥{it['close']:.2f}"
            )
    else:
        parts.append("- （无）")

    parts.append("")
    parts.append("## Pool 2 持仓")
    if pool2_active:
        for it in pool2_active:
            entry_str = str(it["entry_date"])[5:10]
            tag = f" [{it['blacklist_label']}]" if it.get("blacklist_label") else ""
            parts.append(
                f"- {it['ts_code']} {it.get('name', '')}{tag} "
                f"入池 {entry_str} 第 {it['days_in_pool']} 日"
            )
    else:
        parts.append("- （无）")

    parts.append("")
    parts.append(f"耗时 {duration_seconds:.0f}s")

    return title, "\n".join(parts)


def _build_error(
    *,
    component: str,
    exc: BaseException,
) -> tuple[str, str]:
    """D. 系统异常。"""
    title = f"❌ rQuant 异常: {component} 失败"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tb_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    tb = "".join(tb_lines).splitlines()[:15]
    tb_text = "\n".join(tb)

    body = (
        f"组件：{component}\n"
        f"时间：{now_str}\n"
        f"异常：{type(exc).__name__}: {exc}\n\n"
        f"```\n{tb_text}\n```"
    )
    return title, body


def _build_heartbeat(
    *,
    event: str,  # "start" | "stop"
    watchlist_count: int = 0,
    pool1_count: int = 0,
    pool2_count: int = 0,
    triggers_summary: dict | None = None,
    auto_kicked_count: int = 0,
) -> tuple[str, str]:
    """E. 启停心跳。"""
    if event == "start":
        title = f"▶ Monitor 启动 {watchlist_count} 只"
        body = f"pool2={pool2_count}, pool1={pool1_count}"
        return title, body

    if event == "stop":
        ts = triggers_summary or {}
        total = sum(ts.values())
        title = f"⏹ Monitor 结束: 触发 {total} 次"
        breakdown = " / ".join(
            f"{_LEVEL_LABELS.get(k, k)} {v}"
            for k, v in ts.items()
            if v > 0
        ) or "无"
        body = (
            f"{breakdown}\n"
            f"Pool 2 自动踢出 {auto_kicked_count} 只"
        )
        return title, body

    raise ValueError(f"未知 event: {event}")
