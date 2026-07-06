"""notify(scene, **kwargs) 统一入口：路由到消息构造器 + 多通道推送 + 开关 + 日志。"""

from __future__ import annotations

from typing import Literal

from loguru import logger

from rquant.config import settings
from rquant.notify.client import PushDeerClient, PushPlusClient
from rquant.notify.log import append as _log_notification
from rquant.notify.messages import build_message

Scene = Literal[
    "price_level",
    "pool2_exit",
    "daily_summary",
    "error",
    "heartbeat",
    "morning_pulse",
    "midday_report",
    "surge_watch",
]

# 只推 admin（PushDeer）不推 PushPlus 的场景：爆量确认每分钟批次高频，只发刘彤
_PUSHDEER_ONLY_SCENES: frozenset[str] = frozenset({"surge_watch"})


def _scene_enabled(scene: str) -> bool:
    return getattr(settings, f"notify_{scene}", True)


def notify(scene: Scene, **kwargs) -> None:
    """发送通知到所有配置的通道（PushDeer + PushPlus）。

    失败写日志，不抛异常，不阻塞业务。各通道独立失败。
    每个 target 的成败记录到 notification_log.jsonl 文件，供 dashboard 显示。
    """
    if not settings.notify_enabled:
        return
    if not _scene_enabled(scene):
        return

    try:
        title, body = build_message(scene, **kwargs)
    except Exception as e:
        logger.error(f"通知 [{scene}] 消息构造失败: {e}")
        return

    pushdeer = PushDeerClient(
        keys=settings.pushdeer_key_list,
        endpoint=settings.pushdeer_endpoint,
    )
    try:
        results = pushdeer.push(title, body)
        for key, (success, err) in zip(settings.pushdeer_key_list, results, strict=False):
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
            _log_notification(scene, "pushplus", token[:8], success, err, title)
    except Exception as e:
        logger.error(f"通知 [{scene}] PushPlus 推送失败: {e}")
