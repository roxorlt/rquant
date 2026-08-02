"""推送 HTTP 客户端：PushDeer + PushPlus，多 key 并发，失败不抛。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol
from urllib.parse import urlsplit

import requests
from loguru import logger


class HttpResponse(Protocol):
    def json(self) -> object: ...


HttpTransport = Callable[..., HttpResponse]


def require_https_endpoint(endpoint: str) -> str:
    normalized = endpoint.strip()
    parsed = urlsplit(normalized)
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("notification endpoint must use HTTPS")
    return normalized


def _response_code(response: HttpResponse, *, success_code: int) -> tuple[bool, str | None]:
    try:
        payload = response.json()
    except Exception:
        return (False, "invalid_response")
    if not isinstance(payload, Mapping):
        return (False, "invalid_response")
    if payload.get("code") == success_code:
        return (True, None)
    return (False, "provider_rejected")


class PushDeerClient:
    def __init__(
        self,
        keys: list[str],
        endpoint: str,
        *,
        transport: HttpTransport | None = None,
    ) -> None:
        self.keys = keys
        self.endpoint = (
            endpoint.strip() if transport is not None else require_https_endpoint(endpoint)
        )
        if not self.endpoint:
            raise ValueError("notification endpoint must be nonempty")
        self._transport = transport or requests.post

    def push(self, title: str, body: str) -> list[tuple[bool, str | None]]:
        """对所有 keys 并发推送。

        Returns:
            list of (success, error_msg)；error_msg 在 success=True 时为 None
        """
        if not self.keys:
            return []

        def _push_one(key: str) -> tuple[bool, str | None]:
            try:
                resp = self._transport(
                    self.endpoint,
                    data={
                        "pushkey": key,
                        "text": title,
                        "desp": body,
                        "type": "markdown",
                    },
                    timeout=10,
                )
                result = _response_code(resp, success_code=0)
                if not result[0]:
                    logger.error(f"PushDeer 推送失败: {result[1]}")
                return result
            except Exception:
                logger.error("PushDeer 推送失败: transport_error")
                return (False, "transport_error")

        with ThreadPoolExecutor(max_workers=len(self.keys)) as pool:
            return list(pool.map(_push_one, self.keys))


class PushPlusClient:
    """PushPlus 微信公众号推送（用于 PushDeer 不在的设备，例如美丞）。"""

    def __init__(
        self,
        tokens: list[str],
        endpoint: str,
        *,
        transport: HttpTransport | None = None,
    ) -> None:
        self.tokens = tokens
        self.endpoint = (
            endpoint.strip() if transport is not None else require_https_endpoint(endpoint)
        )
        if not self.endpoint:
            raise ValueError("notification endpoint must be nonempty")
        self._transport = transport or requests.post

    def push(self, title: str, body: str) -> list[tuple[bool, str | None]]:
        if not self.tokens:
            return []

        def _push_one(token: str) -> tuple[bool, str | None]:
            try:
                resp = self._transport(
                    self.endpoint,
                    json={
                        "token": token,
                        "title": title,
                        "content": body,
                        "template": "markdown",
                    },
                    timeout=10,
                )
                result = _response_code(resp, success_code=200)
                if not result[0]:
                    logger.error(f"PushPlus 推送失败: {result[1]}")
                return result
            except Exception:
                logger.error("PushPlus 推送失败: transport_error")
                return (False, "transport_error")

        with ThreadPoolExecutor(max_workers=len(self.tokens)) as pool:
            return list(pool.map(_push_one, self.tokens))
