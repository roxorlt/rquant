"""DeepSeek-V4-Flash 客户端：NL → ScreenPlan，OpenAI-compatible。"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger
from openai import OpenAI
from pydantic import ValidationError

from rquant.llm.prompts import FEW_SHOT_EXAMPLES, build_system_prompt
from rquant.llm.schema_export import to_openai_tools
from rquant.llm.schemas import ScreenPlan


class LLMClarificationNeeded(Exception):
    """LLM 没调 tool，返回了澄清问题。message 即 LLM 原文。"""


class LLMError(Exception):
    """LLM 调用或返回解析失败。"""


class DeepSeekClient:
    """DeepSeek API 包装。线程安全（内部 OpenAI client 是线程安全的）。"""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-v4-flash",
        openai_client: OpenAI | None = None,
        log_path: Path | None = None,
        max_retries: int = 3,
    ) -> None:
        if not api_key and openai_client is None:
            raise ValueError("DeepSeek API key required")
        self._client = openai_client or OpenAI(api_key=api_key, base_url=base_url)
        self._model = model
        self._log_path = log_path
        self._max_retries = max_retries

    def nl_to_screen_plan(
        self,
        query: str,
        *,
        today: str,
        current_rule_calls: list | None = None,
    ) -> ScreenPlan:
        """解析中文 query 为 ScreenPlan。

        Args:
            query: 中文选股意图
            today: YYYY-MM-DD，用于填充 LLM 留空的 trade_date
            current_rule_calls: 编辑场景下传当前 pool 的规则列表（list[RuleCall]）。
                None 表示新建场景（用 few-shot system prompt）；非 None 时用
                build_edit_system_prompt 注入当前规则上下文，LLM 在此基础上做修改。

        Raises:
            LLMClarificationNeeded: LLM 返回纯文本而非 tool_call（请用户澄清）
            LLMError: API 失败或返回不可解析
            ValidationError: tool_call 参数不通过 Pydantic 校验
        """
        if current_rule_calls is not None:
            from rquant.llm.prompts import build_edit_system_prompt
            messages = [
                {"role": "system", "content": build_edit_system_prompt(current_rule_calls)},
                {"role": "user", "content": query},
            ]
        else:
            messages = self._build_messages(query)
        tools = to_openai_tools()

        t0 = time.monotonic()
        last_err: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._client.chat.completions.create(
                    model=self._model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",  # 允许 LLM 不调 tool（澄清场景）
                    temperature=0.0,
                )
                break
            except Exception as e:
                last_err = e
                logger.warning(f"DeepSeek attempt {attempt}/{self._max_retries} failed: {e}")
                if attempt < self._max_retries:
                    time.sleep(2 ** (attempt - 1))  # max_retries=3 → sleeps 1s, 2s
        else:
            raise LLMError(f"DeepSeek 调用失败（{self._max_retries} 次重试均失败）: {last_err}")

        latency_ms = int((time.monotonic() - t0) * 1000)
        msg = resp.choices[0].message
        usage = getattr(resp, "usage", None)
        tokens_in = getattr(usage, "prompt_tokens", 0) if usage else 0
        tokens_out = getattr(usage, "completion_tokens", 0) if usage else 0

        # 澄清场景：LLM 没调 tool
        if not msg.tool_calls:
            text = msg.content or ""
            self._log(query, None, tokens_in, tokens_out, latency_ms, error="clarification")
            raise LLMClarificationNeeded(text)

        tc = msg.tool_calls[0]
        if tc.function.name != "build_screen":
            raise LLMError(f"LLM 调用了未知工具: {tc.function.name}")

        try:
            args = json.loads(tc.function.arguments)
        except json.JSONDecodeError as e:
            raise LLMError(f"tool_call 参数不是合法 JSON: {e}") from e

        # 填充空 trade_date
        if not args.get("trade_date"):
            args["trade_date"] = today

        try:
            plan = ScreenPlan.model_validate(args)
        except ValidationError as e:
            self._log(query, args, tokens_in, tokens_out, latency_ms, error=str(e))
            raise

        self._log(query, plan.model_dump(), tokens_in, tokens_out, latency_ms)
        return plan

    def _build_messages(self, query: str) -> list[dict[str, Any]]:
        """system + few-shot + user。"""
        messages: list[dict[str, Any]] = [{"role": "system", "content": build_system_prompt()}]
        for ex in FEW_SHOT_EXAMPLES:
            messages.append({"role": "user", "content": ex["user"]})
            tool_call_id = f"fs_{hash(ex['user']) & 0xffff:04x}"
            messages.append({
                "role": "assistant",
                "content": None,
                # deepseek-v4-flash is a thinking model: multi-turn messages that
                # include tool_calls in an assistant turn must carry reasoning_content
                # (even empty string) or the API returns HTTP 400.
                "reasoning_content": "",
                "tool_calls": [{
                    "id": tool_call_id,
                    "type": "function",
                    "function": {
                        "name": ex["assistant_tool_call"]["name"],
                        "arguments": json.dumps(
                            ex["assistant_tool_call"]["arguments"],
                            ensure_ascii=False,
                        ),
                    },
                }],
            })
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": "ok",
            })
        messages.append({"role": "user", "content": query})
        return messages

    def _log(
        self,
        query: str,
        plan: dict | None,
        tokens_in: int,
        tokens_out: int,
        latency_ms: int,
        *,
        error: str | None = None,
    ) -> None:
        if self._log_path is None:
            return
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            record = {
                "ts": datetime.now().isoformat(timespec="seconds"),
                "query": query,
                "plan": plan,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "latency_ms": latency_ms,
                "model": self._model,
                "error": error,
            }
            with self._log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"JSONL log write failed: {e}")
