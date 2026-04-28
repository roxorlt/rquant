"""notify(scene, **kwargs) 统一入口：路由到消息构造器 + 推送 + 开关。"""

from __future__ import annotations

from typing import Literal

from loguru import logger

from rquant.config import settings
from rquant.notify.client import PushDeerClient
from rquant.notify.messages import build_message

Scene = Literal[
    "price_level",
    "pool2_exit",
    "daily_summary",
    "error",
    "heartbeat",
]


def _scene_enabled(scene: str) -> bool:
    return getattr(settings, f"notify_{scene}", True)


def notify(scene: Scene, **kwargs) -> None:
    """发送通知。失败写日志，不抛异常，不阻塞业务。"""
    if not settings.notify_enabled:
        return
    if not _scene_enabled(scene):
        return

    try:
        title, body = build_message(scene, **kwargs)
    except Exception as e:
        logger.error(f"通知 [{scene}] 消息构造失败: {e}")
        return

    client = PushDeerClient(
        keys=settings.pushdeer_key_list,
        endpoint=settings.pushdeer_endpoint,
    )
    try:
        client.push(title, body)
    except Exception as e:
        logger.error(f"通知 [{scene}] 推送失败: {e}")
