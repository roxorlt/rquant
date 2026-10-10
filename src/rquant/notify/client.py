"""推送 HTTP 客户端：PushDeer + PushPlus，多 key 并发，失败不抛。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable
from datetime import UTC, datetime

import requests
from loguru import logger

from rquant.delivery_contracts import (
    DeliveryChannel, PhysicalPostBinding, PhysicalPostObservation,
)
from rquant.runtime_contracts import canonical_sha256


def _validate_observation(
    binding: PhysicalPostBinding | None, sink: Callable[[PhysicalPostObservation], None] | None,
    *, channel: DeliveryChannel, key_count: int, title: str, body: str,
) -> None:
    if (binding is None) != (sink is None):
        raise ValueError("POST observation requires both binding and sink")
    if binding is None:
        return
    if type(binding) is not PhysicalPostBinding or binding.target.channel is not channel:
        raise ValueError("POST observation channel binding differs")
    if key_count > 1:
        raise ValueError("observed POST requires a single recipient credential")
    if (binding.request_sha256 != canonical_sha256({"title": title, "body": body})
            or binding.request_utf8_bytes != len((title + body).encode())):
        raise ValueError("POST observation request differs from its admitted intent")


def _observe(
    binding: PhysicalPostBinding | None, sink: Callable[[PhysicalPostObservation], None] | None,
    *, called_at: datetime | None, clock: Callable[[], datetime],
    disposition: str, reason: str,
) -> None:
    if binding is None or sink is None or called_at is None:
        return
    try:
        sink(PhysicalPostObservation(
            binding=binding, key_slot=0, called_at=called_at, completed_at=clock(),
            disposition=disposition, reason=reason,
        ))
    except Exception:
        # Observation failure cannot change the legacy client result. The durable
        # intent remains unresolved; the observed runtime transport checks readback.
        logger.error("通知调用观察未写入")


def _utc_now() -> datetime:
    return datetime.now(UTC)


class PushDeerClient:
    def __init__(self, keys: list[str], endpoint: str) -> None:
        self.keys = keys
        self.endpoint = endpoint

    def push(
        self, title: str, body: str, *, observation_binding: PhysicalPostBinding | None = None,
        observation_sink: Callable[[PhysicalPostObservation], None] | None = None,
        observation_clock: Callable[[], datetime] = _utc_now,
    ) -> list[tuple[bool, str | None]]:
        """对所有 keys 并发推送。

        Returns:
            list of (success, error_msg)；error_msg 在 success=True 时为 None
        """
        _validate_observation(observation_binding, observation_sink, channel=DeliveryChannel.PUSHDEER,
                              key_count=len(self.keys), title=title, body=body)
        if not self.keys:
            return []

        def _push_one(key: str) -> tuple[bool, str | None]:
            called_at: datetime | None = None
            called = False
            try:
                called_at = observation_clock() if observation_binding is not None else None
                called = True
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
                    _observe(observation_binding, observation_sink, called_at=called_at,
                             clock=observation_clock, disposition="accepted" if type(data.get("code")) is int else "unknown",
                             reason="channel_accepted" if type(data.get("code")) is int else "invalid_reply")
                    return (True, None)
                err = data.get("error", str(data))
                _observe(observation_binding, observation_sink, called_at=called_at,
                         clock=observation_clock, disposition="rejected" if type(data.get("code")) is int else "unknown",
                         reason="channel_rejected" if type(data.get("code")) is int else "invalid_reply")
                logger.error(f"PushDeer 推送失败 ({key[:8]}…): {err}")
                return (False, err)
            except Exception as e:
                if called:
                    _observe(observation_binding, observation_sink, called_at=called_at,
                             clock=observation_clock, disposition="unknown", reason="post_exception")
                logger.error(f"PushDeer 异常 ({key[:8]}…): {e}")
                return (False, str(e))

        with ThreadPoolExecutor(max_workers=len(self.keys)) as pool:
            return list(pool.map(_push_one, self.keys))


class PushPlusClient:
    """PushPlus 微信公众号推送（用于 PushDeer 不在的设备，例如美丞）。"""

    def __init__(self, tokens: list[str], endpoint: str) -> None:
        self.tokens = tokens
        self.endpoint = endpoint

    def push(
        self, title: str, body: str, *, observation_binding: PhysicalPostBinding | None = None,
        observation_sink: Callable[[PhysicalPostObservation], None] | None = None,
        observation_clock: Callable[[], datetime] = _utc_now,
    ) -> list[tuple[bool, str | None]]:
        _validate_observation(observation_binding, observation_sink, channel=DeliveryChannel.PUSHPLUS,
                              key_count=len(self.tokens), title=title, body=body)
        if not self.tokens:
            return []

        def _push_one(token: str) -> tuple[bool, str | None]:
            called_at: datetime | None = None
            called = False
            try:
                called_at = observation_clock() if observation_binding is not None else None
                called = True
                resp = requests.post(
                    self.endpoint,
                    json={
                        "token": token,
                        "title": title,
                        "content": body,
                        "template": "markdown",
                    },
                    timeout=10,
                )
                data = resp.json()
                if data.get("code") == 200:
                    _observe(observation_binding, observation_sink, called_at=called_at,
                             clock=observation_clock, disposition="accepted" if type(data.get("code")) is int else "unknown",
                             reason="channel_accepted" if type(data.get("code")) is int else "invalid_reply")
                    return (True, None)
                err = data.get("msg", str(data))
                _observe(observation_binding, observation_sink, called_at=called_at,
                         clock=observation_clock, disposition="rejected" if type(data.get("code")) is int else "unknown",
                         reason="channel_rejected" if type(data.get("code")) is int else "invalid_reply")
                logger.error(f"PushPlus 推送失败 ({token[:8]}…): {err}")
                return (False, err)
            except Exception as e:
                if called:
                    _observe(observation_binding, observation_sink, called_at=called_at,
                             clock=observation_clock, disposition="unknown", reason="post_exception")
                logger.error(f"PushPlus 异常 ({token[:8]}…): {e}")
                return (False, str(e))

        with ThreadPoolExecutor(max_workers=len(self.tokens)) as pool:
            return list(pool.map(_push_one, self.tokens))
