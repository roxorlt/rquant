"""Bounded model adapter for structured screening drafts.

The parser has no knowledge of pools, HTTP, or saving. Callers validate every returned
tool argument against their own authoritative data before showing a draft.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Protocol

from openai import OpenAI
from pydantic import SecretStr

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

    def __init__(self, *, api_key: SecretStr, model: str) -> None:
        self._client = OpenAI(
            api_key=api_key.get_secret_value(),
            base_url="https://api.openai.com/v1",
            timeout=12.0,
            max_retries=0,
        )
        self._model = model

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
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": instruction},
                ],
                tools=tools,
                tool_choice="auto",
                temperature=0.0,
                max_completion_tokens=4096,
            )
            if not response.choices:
                raise NlParserUnavailableError
            tool_calls = response.choices[0].message.tool_calls
            if not tool_calls:
                raise NlClarificationNeededError
            if len(tool_calls) != 1 or tool_calls[0].function.name != "build_screen":
                raise NlParserUnavailableError
            arguments = tool_calls[0].function.arguments
            if len(arguments.encode("utf-8")) > 16_384:
                raise NlParserUnavailableError
            result = json.loads(arguments)
            if not isinstance(result, dict):
                raise NlParserUnavailableError
            return result
        except (NlClarificationNeededError, NlParserUnavailableError):
            raise
        except Exception:
            raise NlParserUnavailableError from None
