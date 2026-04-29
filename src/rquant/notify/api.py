"""notify(scene, **kwargs) 统一入口：路由到消息构造器 + 多通道推送 + 开关。"""

from __future__ import annotations

from typing import Literal

from loguru import logger

from rquant.config import settings
from rquant.notify.client import PushDeerClient, PushPlusClient
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
    """发送通知到所有配置的通道（PushDeer + PushPlus）。

    失败写日志，不抛异常，不阻塞业务。各通道独立失败。
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
        pushdeer.push(title, body)
    except Exception as e:
        logger.error(f"通知 [{scene}] PushDeer 推送失败: {e}")

    pushplus = PushPlusClient(
        tokens=settings.pushplus_token_list,
        endpoint=settings.pushplus_endpoint,
    )
    try:
        pushplus.push(title, body)
    except Exception as e:
        logger.error(f"通知 [{scene}] PushPlus 推送失败: {e}")
