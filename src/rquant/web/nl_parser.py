"""Bounded model adapter for structured screening drafts.

The parser has no knowledge of pools, HTTP, or saving. Callers validate every returned
tool argument against their own authoritative data before showing a draft.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from collections.abc import Callable
from typing import Protocol

import httpx
from pydantic import SecretStr

from rquant.ai_assistance import (
    AIModelPrompt, AIModelReply, AIResponseUnknown, MAX_MODEL_RESPONSE_BYTES,
    MAX_TOOL_ARGUMENT_BYTES, measured_provider_usage,
)
from rquant.llm.prompts import build_edit_system_prompt, build_system_prompt
from rquant.llm.schema_export import to_openai_tools


class NlParserUnavailableError(Exception):
    """A model request failed without exposing SDK or credential details."""


class NlClarificationNeededError(Exception):
    """The user needs to give a more specific instruction."""


class RuleContext(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def args(self) -> Mapping[str, object]: ...


class ScreenPlanParser(Protocol):
    def parse_edit(
        self, instruction: str, current_rules: Sequence[RuleContext]
    ) -> Mapping[str, object]: ...

    def parse_new(self, instruction: str, trade_date: str) -> Mapping[str, object]: ...


class OpenAiScreenPlanParser:
    """The only network-capable implementation; fixed official endpoint and bounded call."""

    def __init__(self, *, api_key: SecretStr, model: str,
                 transport: httpx.BaseTransport | None = None,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self._client = httpx.Client(
            headers={"Authorization": "Bearer " + api_key.get_secret_value()},
            timeout=12.0,
            follow_redirects=False,
            trust_env=False,
            transport=transport or httpx.HTTPTransport(retries=0),
        )
        self._model = model
        self._monotonic = monotonic

    def close(self) -> None:
        self._client.close()

    def prepare(self, prompt: AIModelPrompt) -> bytes:
        if prompt.model_id != self._model:
            raise ValueError("model differs from the owner configuration")
        return prompt.encoded_request()

    def generate(self, prompt: AIModelPrompt) -> AIModelReply:
        body = self.prepare(prompt)
        started = self._monotonic()
        try:
            with self._client.stream("POST", "https://api.openai.com/v1/chat/completions",
                                     content=body, headers={"Content-Type": "application/json"}) as response:
                if response.status_code != 200 or self._monotonic() - started >= 12.0:
                    raise AIResponseUnknown("provider response unavailable")
                announced = response.headers.get("content-length")
                if announced is not None and (not announced.isdecimal() or int(announced) > MAX_MODEL_RESPONSE_BYTES):
                    raise AIResponseUnknown("provider response exceeds transport limit")
                chunks = bytearray()
                for chunk in response.iter_bytes():
                    if self._monotonic() - started >= 12.0 or len(chunks) + len(chunk) > MAX_MODEL_RESPONSE_BYTES:
                        raise AIResponseUnknown("provider response exceeds elapsed or byte limit")
                    chunks.extend(chunk)
            payload = json.loads(chunks)
        except AIResponseUnknown:
            raise
        except Exception:
            raise AIResponseUnknown("provider response unavailable") from None
        usage = measured_provider_usage(payload.get("usage") if isinstance(payload, dict) else None)
        try:
            calls = payload["choices"][0]["message"]["tool_calls"]
            if not isinstance(calls, list) or len(calls) != 1 or calls[0]["type"] != "function":
                raise ValueError
            function = calls[0]["function"]
            arguments = function["arguments"]
            if function["name"] != prompt.tool_name or not isinstance(arguments, str) or len(arguments.encode()) > MAX_TOOL_ARGUMENT_BYTES:
                raise ValueError
            result = json.loads(arguments)
            if not isinstance(result, dict):
                raise ValueError
            return AIModelReply(draft=result, usage=usage)
        except Exception:
            return AIModelReply(usage=usage, error_code="invalid_model_output")

    def parse_edit(
        self, instruction: str, current_rules: Sequence[RuleContext]
    ) -> Mapping[str, object]:
        system = (
            build_edit_system_prompt(list(current_rules))
            + "\n只修改选股条件；不要输出展示列、池子名称、父池或画布。"
        )
        return self._request(instruction, system)

    def parse_new(self, instruction: str, trade_date: str) -> Mapping[str, object]:
        system = (
            build_system_prompt()
            + f"\n当前页面选择的日期是 {trade_date}；只生成条件。"
            + "不要输出展示列、排名、池子名称或执行指令。"
        )
        return self._request(instruction, system)

    def _request(self, instruction: str, system: str) -> Mapping[str, object]:
        try:
            tools = to_openai_tools()
            tools[0]["function"]["parameters"]["properties"].pop("include_columns")
            response = self.generate(AIModelPrompt(model_id=self._model, system=system,
                instruction=instruction, tool_name="build_screen", tool_schema=tools[0]["function"]["parameters"]))
            if response.draft is None:
                raise NlParserUnavailableError
            return response.draft
        except (NlClarificationNeededError, NlParserUnavailableError):
            raise
        except Exception:
            raise NlParserUnavailableError from None
