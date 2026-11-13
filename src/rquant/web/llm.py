"""DeepSeek chat completion client for Web natural-language condition translation."""

from __future__ import annotations

import os
from collections.abc import Callable

Complete = Callable[[list[dict[str, str]]], str]


def deepseek_complete() -> tuple[Complete, str] | None:
    """DeepSeek chat completion from DEEPSEEK_* env vars, or None when no key is set."""
    key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not key:
        return None
    from openai import OpenAI

    base = os.environ.get("DEEPSEEK_BASE_URL") or "https://api.deepseek.com"
    model = os.environ.get("DEEPSEEK_MODEL") or "deepseek-chat"
    client = OpenAI(api_key=key, base_url=base, timeout=60)

    def complete(messages: list[dict[str, str]]) -> str:
        reply = client.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
            temperature=0.2,
            max_tokens=600,
        )
        return reply.choices[0].message.content or ""

    return complete, model
