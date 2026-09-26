"""User-level status: five states, a short word and a one-line reason.

States (``web/CLAUDE.md`` 「界面与文案原则」): 正常 / 注意 / 异常 / 未运行, plus 等待开盘
(or 已收盘) for a market-hours service that is idle outside the session, which is expected
and must not look like a fault. Reasons are one plain sentence for a tooltip; technical
detail (ids, error text) travels separately and is shown only in tooltips or drawers.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import StrEnum

from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.web.labels import dataset_label, split_service_id
from rquant.web.market import MARKET_TIMEZONE, MarketPhase

#: The paper broker's watermark reason when holdings are marked at the last fill.
PAPER_VALUATION_REASON = "last execution price"
PAPER_VALUATION_NOTE = "持仓按最近成交价估值，不是实时价"
#: The reference publisher's deadline (#297 / #298, #301 S-1): today's reference
#: generation must be visible by 09:25; every round after that refuses by design.
REFERENCE_DEADLINE = time(9, 25)
#: Consecutive failures from which a degraded service counts as 异常, not 注意.
CRIT_FAILURES = 3
#: Backlog from which a running service needs attention.
BACKLOG_WARN = 1_000

_OFF_SESSION = frozenset(
    {MarketPhase.NON_TRADING_DAY, MarketPhase.PRE_OPEN, MarketPhase.AFTER_CLOSE}
)


class UserState(StrEnum):
    OK = "ok"
    WARN = "warn"
    CRIT = "crit"
    IDLE = "idle"
    WAITING = "waiting"


STATE_ORDER: dict[UserState, int] = {
    UserState.CRIT: 0,
    UserState.WARN: 1,
    UserState.IDLE: 2,
    UserState.WAITING: 3,
    UserState.OK: 4,
}


@dataclass(frozen=True)
class Status:
    state: UserState
    label: str
    reason: str


def _minutes(seconds: float) -> str:
    if seconds < 90:
        return f"{max(int(seconds), 1)} 秒"
    if seconds < 5400:
        return f"{int(seconds // 60)} 分钟"
    if seconds < 172800:
        return f"{int(seconds // 3600)} 小时"
    return f"{int(seconds // 86400)} 天"


def _waiting(phase: MarketPhase) -> Status:
    if phase is MarketPhase.AFTER_CLOSE:
        return Status(UserState.WAITING, "已收盘", "盘中服务，收盘后停止是正常的")
    if phase is MarketPhase.PRE_OPEN:
        return Status(UserState.WAITING, "等待开盘", "盘中服务，09:15 开盘前没有心跳是正常的")
    return Status(UserState.WAITING, "等待开盘", "盘中服务，休市日不运行")


def _known_degraded_reason(service_id: str) -> str | None:
    role, _instance = split_service_id(service_id)
    if role == "notifier":
        # See delivery_mode(): a degraded notifier without failures is the shadow mode.
        return "影子模式：" + SHADOW_NOTE
    return None


def service_status(
    *,
    service_id: str,
    plane: str,
    status: str,
    stale: bool,
    heartbeat_at: datetime | None,
    consecutive_failures: int,
    backlog_count: int,
    phase: MarketPhase,
    now: datetime,
) -> Status:
    """Map one ``runtime_services`` row to what the owner needs to know."""

    market_hours = plane == "live"
    off_session = market_hours and phase in _OFF_SESSION
    if status == "stopped":
        return Status(UserState.IDLE, "未运行", "服务已停止")
    if status == "missing":
        if off_session:
            return _waiting(phase)
        return Status(UserState.IDLE, "未运行", "没有收到这个服务的心跳")
    if stale:
        if off_session:
            return _waiting(phase)
        if heartbeat_at is None:
            return Status(UserState.CRIT, "异常", "心跳中断")
        age = max((now - heartbeat_at).total_seconds(), 0.0)
        return Status(UserState.CRIT, "异常", f"心跳已中断 {_minutes(age)}")
    if status == "starting":
        return Status(UserState.WARN, "注意", "正在启动")
    if status == "degraded":
        if consecutive_failures >= CRIT_FAILURES:
            return Status(UserState.CRIT, "异常", f"连续失败 {consecutive_failures} 次")
        if consecutive_failures > 0:
            return Status(UserState.WARN, "注意", f"最近失败 {consecutive_failures} 次")
        known = _known_degraded_reason(service_id)
        if known is not None:
            return Status(UserState.WARN, "注意", known)
        return Status(UserState.WARN, "注意", "功能降级运行")
    if status == "running":
        if consecutive_failures > 0:
            return Status(UserState.WARN, "注意", f"最近失败 {consecutive_failures} 次")
        if backlog_count >= BACKLOG_WARN:
            return Status(UserState.WARN, "注意", f"积压 {backlog_count:,} 条待处理")
        return Status(UserState.OK, "正常", "运行中")
    return Status(UserState.WARN, "注意", "状态未知")


def is_reference_publisher(service_id: str) -> bool:
    role, instance = split_service_id(service_id)
    return role == "reference-slow" and instance.split(".", 1)[0] == "publisher"


def reference_publisher_status(
    base: Status,
    *,
    is_trading_day: bool | None,
    today: date,
    now: datetime,
    published_on: date | None,
    last_error: str | None,
) -> Status:
    """The reference publisher, judged by whether today's reference data exists.

    After 09:25 the publisher refuses every round ("reference slow publisher started after
    09:25", #301 S-1): that is the design once today's generation is out, not a fault. So
    on a trading day after 09:25 it is 已完成 when today's reference data is published and
    异常 only when it is not; before 09:25 the ordinary rules apply; on a closed day it
    waits. The raw refusal stays in the reason (tooltip) and the detail drawer.
    """

    raw = f"。原始信息：{last_error}" if last_error else ""
    if is_trading_day is None:
        return base
    if not is_trading_day:
        return Status(UserState.WAITING, "等待开盘", "休市日不发布参考数据")
    if now.astimezone(MARKET_TIMEZONE).time() < REFERENCE_DEADLINE:
        return base
    if published_on is not None and published_on >= today:
        return Status(
            UserState.OK,
            "已完成",
            f"今天的参考数据已发布；09:25 之后按设计不再重复发布{raw}",
        )
    return Status(UserState.CRIT, "异常", f"09:25 已过，今天的参考数据还没有发布{raw}")


@dataclass(frozen=True)
class DeliveryMode:
    mode: str  # "live" | "shadow" | "unknown"
    label: str
    note: str | None


SHADOW_NOTE = "正式推送开通前只记录不发送"


def delivery_mode(
    notifiers: Sequence[tuple[str, bool, int, str | None]],
) -> DeliveryMode:
    """Whether a finished delivery reached the phone, from the notifiers' heartbeats.

    ``notifiers`` is ``(status, stale, consecutive_failures, last_error)`` per notifier.
    A shadow notifier's heartbeat never reads as a clean live one: with
    ``suppress_delivery`` it always reports ``notifier:shadow_transport`` and so is
    ``degraded`` (``runtime_builder_signal``), and its receipts are ``shadow:<outbox_id>``.
    Serving publishes neither the receipts nor the degraded reasons, so: every notifier
    ``running`` → live (已送达); every notifier degraded with no failure and no error →
    shadow (仅记录), the standing production state; anything else → unknown (未确认).
    Publishing the degraded reasons (next serving batch) makes shadow exact.
    """

    if not notifiers:
        return DeliveryMode("unknown", "未确认", "看不到推送服务的状态，无法确认手机是否收到")
    if all(status == "running" and not stale for status, stale, _f, _e in notifiers):
        return DeliveryMode("live", "已送达", None)
    if all(
        status == "degraded" and not stale and failures == 0 and not error
        for status, stale, failures, error in notifiers
    ):
        return DeliveryMode("shadow", "仅记录", SHADOW_NOTE)
    return DeliveryMode("unknown", "未确认", "推送服务状态异常，无法确认手机是否收到")


# ------------------------------------------------------------------ data freshness


def watermark_status(watermark: ServingDatasetWatermark) -> Status:
    """A dataset watermark in the owner's words."""

    status = watermark.status
    reason = (watermark.reason or "").lower()
    if status is FreshnessStatus.FRESH:
        return Status(UserState.OK, "按时", "按时更新")
    if status is FreshnessStatus.STALE:
        return Status(UserState.WARN, "延迟", "超过预期时间没有更新")
    if status is FreshnessStatus.DEGRADED:
        if watermark.dataset_id == "runtime_health":
            # The dataset is on time; "degraded" means some service is, and the
            # services table already says which.
            return Status(UserState.OK, "按时", "按时更新；个别服务的状态见运行服务表")
        if PAPER_VALUATION_REASON in reason:
            # Nothing to act on: the account is on time, only valued at the last fill.
            return Status(UserState.OK, "按时", f"按时更新；{PAPER_VALUATION_NOTE}")
        return Status(UserState.WARN, "注意", "数据不完整")
    name = dataset_label(watermark.dataset_id)
    return Status(UserState.IDLE, "未发布", f"{name}服务没有运行，暂时没有数据")


def expected_daily_date(
    *,
    today_is_trading_day: bool | None,
    today: date,
    previous_trading_day: date | None,
    now: datetime,
    ready_at: time,
) -> date | None:
    """The trading day a once-a-day dataset should have reached by ``now``."""

    if today_is_trading_day is None:
        return None
    local = now.astimezone(MARKET_TIMEZONE)
    if today_is_trading_day and local.time() >= ready_at:
        return today
    return previous_trading_day


def daily_status(
    latest: date | None,
    expected: date | None,
    *,
    behind_days: int | None,
    ready_note: str,
) -> Status:
    if latest is None:
        return Status(UserState.IDLE, "未发布", "暂时没有数据")
    if expected is None:
        return Status(UserState.IDLE, "无法判断", "交易日历缺失，无法判断是否按时")
    if latest >= expected:
        return Status(UserState.OK, "按时", ready_note)
    days = behind_days if behind_days is not None and behind_days > 0 else None
    return Status(
        UserState.WARN,
        "延迟",
        f"落后 {days} 个交易日" if days else "没有更新到最近交易日",
    )


def generation_status(age_seconds: float | None, stale_after: timedelta) -> Status:
    if age_seconds is None:
        return Status(UserState.CRIT, "异常", "读不到页面数据")
    if age_seconds > stale_after.total_seconds():
        return Status(UserState.CRIT, "异常", f"已 {_minutes(age_seconds)}没有更新")
    return Status(UserState.OK, "正常", "约每分钟更新一次")


__all__ = [
    "BACKLOG_WARN",
    "CRIT_FAILURES",
    "SHADOW_NOTE",
    "DeliveryMode",
    "PAPER_VALUATION_NOTE",
    "PAPER_VALUATION_REASON",
    "REFERENCE_DEADLINE",
    "STATE_ORDER",
    "Status",
    "UserState",
    "daily_status",
    "delivery_mode",
    "expected_daily_date",
    "generation_status",
    "is_reference_publisher",
    "reference_publisher_status",
    "service_status",
    "watermark_status",
]
