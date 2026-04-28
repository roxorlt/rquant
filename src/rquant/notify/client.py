"""PushDeer HTTP 客户端：多 key 并发推，失败不抛。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import requests
from loguru import logger


class PushDeerClient:
    def __init__(self, keys: list[str], endpoint: str) -> None:
        self.keys = keys
        self.endpoint = endpoint

    def push(self, title: str, body: str) -> list[tuple[bool, str | None]]:
        """对所有 keys 并发推送。

        Returns:
            list of (success, error_msg)；error_msg 在 success=True 时为 None
        """
        if not self.keys:
            return []

        def _push_one(key: str) -> tuple[bool, str | None]:
            try:
                resp = requests.post(
                    self.endpoint,
                    data={
                        "pushkey": key,
                        "text": title,
                        "desp": body,
                        "type": "markdown",
                    },
                    timeout=10,
                )
                data = resp.json()
                if data.get("code") == 0:
                    return (True, None)
                err = data.get("error", str(data))
                logger.error(f"PushDeer 推送失败 ({key[:8]}…): {err}")
                return (False, err)
            except Exception as e:
                logger.error(f"PushDeer 异常 ({key[:8]}…): {e}")
                return (False, str(e))

        with ThreadPoolExecutor(max_workers=len(self.keys)) as pool:
            return list(pool.map(_push_one, self.keys))
