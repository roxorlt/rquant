"""推送日志：append-only JSONL 文件，多进程并发安全。

替代旧的 DuckDB `notification_log` 表。
旧实现的问题：手动跑 push（命令行 / inline python）在盘中（monitor 持写锁）
写日志会撞 `IOError: Could not set lock on file ...`，3 条 channel log 全丢
（5/22 真实事故）。

新实现用 JSONL 追加：
- 文件追加是 OS 层 O_APPEND 短写原子，多进程并发无锁
- 没有数据库锁，monitor / dashboard / cli 任意进程随时能写
- 体量小（每天 ~10 条），全读再 filter / sort 完全够用
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger

from rquant.config import settings


def _log_path() -> Path:
    return settings.log_dir / "notification_log.jsonl"


def append(
    scene: str,
    channel: str,
    target: str,
    success: bool,
    error_msg: str | None,
    title: str,
) -> None:
    """追加一条推送日志。失败仅 logger.error 不抛，不阻塞业务。"""
    entry = {
        # microseconds 精度：同一秒内多条 append 也能稳定按时间倒序排列
        "sent_at": datetime.now().isoformat(timespec="microseconds"),
        "scene": scene,
        "channel": channel,
        "target": target,
        "success": success,
        "error_msg": error_msg,
        "title": title,
    }
    path = _log_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error(f"写 notification_log.jsonl 失败: {e}")


def _read_all() -> list[dict]:
    path = _log_path()
    if not path.exists():
        return []
    entries: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            logger.warning(f"notification_log.jsonl 跳过损坏行: {line[:80]}")
    return entries


def read_recent(limit: int = 30) -> pd.DataFrame:
    """读最近 limit 条推送日志，按时间倒序返回。

    返回 DataFrame 列：sent_at, scene, channel, target, success, error_msg, title
    """
    entries = _read_all()
    if not entries:
        return pd.DataFrame()
    df = pd.DataFrame(entries)
    return df.sort_values("sent_at", ascending=False).head(limit).reset_index(drop=True)


def read_since(hours: int = 24) -> pd.DataFrame:
    """读最近 N 小时内的推送日志。"""
    entries = _read_all()
    if not entries:
        return pd.DataFrame()
    cutoff = datetime.now() - timedelta(hours=hours)
    cutoff_iso = cutoff.isoformat(timespec="microseconds")
    df = pd.DataFrame(entries)
    return df[df["sent_at"] >= cutoff_iso].reset_index(drop=True)
