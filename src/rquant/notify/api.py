"""notify(scene, **kwargs) 统一入口：路由到消息构造器 + 多通道推送 + 开关 + 日志。"""

from __future__ import annotations

from datetime import datetime
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


def _log_notification(
    scene: str,
    channel: str,
    target: str,
    success: bool,
    error_msg: str | None,
    title: str,
) -> None:
    """写一条推送日志到 DuckDB notification_log 表。失败仅 log 不抛。"""
    try:
        import duckdb

        from rquant.storage.schema import NOTIFICATION_LOG_DDL

        conn = duckdb.connect(str(settings.duckdb_path))
        try:
            conn.execute(NOTIFICATION_LOG_DDL)  # 幂等，首次会建表
            conn.execute(
                "INSERT INTO notification_log VALUES (?, ?, ?, ?, ?, ?, ?)",
                [datetime.now(), scene, channel, target, success, error_msg, title],
            )
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"写 notification_log 失败: {e}")


def notify(scene: Scene, **kwargs) -> None:
    """发送通知到所有配置的通道（PushDeer + PushPlus）。

    失败写日志，不抛异常，不阻塞业务。各通道独立失败。
    每个 target 的成败记录到 notification_log 表，供 dashboard 显示。
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
