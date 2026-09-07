"""notify(scene, **kwargs) 统一入口：路由到消息构造器 + 多通道推送 + 开关 + 日志。"""

from __future__ import annotations

from typing import Any, Literal

from loguru import logger

from rquant.notify.client import PushDeerClient, PushPlusClient
from rquant.notify.gate import NotificationGate, NotificationLease, error_event_key
from rquant.notify.log import append as _log_notification
from rquant.notify.messages import build_message


def _settings() -> Any:
    """The process-wide settings, built on first use rather than at import (#215, #189).

    `from rquant.config import settings` at module level runs `rquant.config.__getattr__`,
    which constructs `Settings` during the import and so makes five environment variables a
    precondition of importing this module. The runtime-exec wrapper builds a role child from
    an empty environment and copies only `LANG` / `LC_ALL` / `TZ`, so under that regime the
    import died with `5 validation errors for Settings` before any role code ran. This is
    TP9's seam, verbatim: a `settings` a test has bound onto this module still wins, exactly
    as the old module-level name did.
    """

    bound = globals().get("settings")
    if bound is not None:
        return bound
    from rquant.config import get_settings

    return get_settings()


def __getattr__(name: str) -> object:
    """`rquant.notify.api.settings` stays readable — built on first use, like the source."""

    if name == "settings":
        from rquant.config import get_settings

        return get_settings()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


Scene = Literal[
    "price_level",
    "pool2_exit",
    "daily_summary",
    "error",
    "heartbeat",
    "morning_pulse",
    "midday_report",
    "surge_watch",
    "pulse_alert",
]

# 只推 admin（PushDeer）不推 PushPlus 的场景：盘中高频/个人盯盘向，只发刘彤
_PUSHDEER_ONLY_SCENES: frozenset[str] = frozenset({"surge_watch", "pulse_alert"})


def _scene_enabled(scene: str) -> bool:
    return getattr(_settings(), f"notify_{scene}", True)


def notify(scene: Scene, **kwargs) -> None:
    """发送通知到所有配置的通道（PushDeer + PushPlus）。

    失败写日志，不抛异常，不阻塞业务。各通道独立失败。
    每个 target 的成败记录到 notification_log.jsonl 文件，供 dashboard 显示。
    """
    settings = _settings()
    if not settings.notify_enabled:
        return
    if not _scene_enabled(scene):
        return

    try:
        title, body = build_message(scene, **kwargs)
    except Exception as e:
        logger.error(f"通知 [{scene}] 消息构造失败: {e}")
        return

    gate: NotificationGate | None = None
    lease: NotificationLease | None = None
    if scene == "error":
        try:
            gate = NotificationGate(
                settings.notification_state_path_resolved,
                busy_timeout_ms=settings.notification_state_busy_timeout_ms,
            )
            lease = gate.claim(
                error_event_key(kwargs["component"], kwargs["exc"]),
                settings.notify_error_cooldown_seconds,
            )
        except Exception as e:
            logger.error(f"通知 [{scene}] 去重状态完全不可用，已抑制 Push 并保留本地日志: {e}")
            return
        else:
            if lease is None:
                logger.warning(f"通知 [{scene}] 同类故障仍在冷却期，已抑制: {title}")
                return

    delivered = False
    pushdeer = PushDeerClient(
        keys=settings.pushdeer_key_list,
        endpoint=settings.pushdeer_endpoint,
    )
    try:
        results = pushdeer.push(title, body)
        for key, (success, err) in zip(settings.pushdeer_key_list, results, strict=False):
            delivered = delivered or success
            _log_notification(scene, "pushdeer", key[:8], success, err, title)
    except Exception as e:
        logger.error(f"通知 [{scene}] PushDeer 推送失败: {e}")

    if scene in _PUSHDEER_ONLY_SCENES:
        return  # 只 admin，跳过 PushPlus

    pushplus = PushPlusClient(
        tokens=settings.pushplus_token_list,
        endpoint=settings.pushplus_endpoint,
    )
    try:
        results = pushplus.push(title, body)
        for token, (success, err) in zip(settings.pushplus_token_list, results, strict=False):
            delivered = delivered or success
            _log_notification(scene, "pushplus", token[:8], success, err, title)
    except Exception as e:
        logger.error(f"通知 [{scene}] PushPlus 推送失败: {e}")

    if gate is not None and lease is not None:
        try:
            if delivered:
                gate.complete(lease, settings.notify_error_cooldown_seconds)
            else:
                gate.release(lease)
        except Exception as e:
            logger.error(f"通知 [{scene}] 更新投递去重状态失败: {e}")
