"""DeepSeekClient 单元测试（OpenAI SDK mock）。"""

import json
from unittest.mock import MagicMock

import pytest

from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded
from rquant.llm.schemas import ScreenPlan


def _make_fake_openai_with_tool_call(arguments: dict) -> MagicMock:
    """构造一个返回 build_screen tool_call 的 mock OpenAI client。"""
    fake = MagicMock()
    fake_tool_call = MagicMock()
    fake_tool_call.function.name = "build_screen"
    fake_tool_call.function.arguments = json.dumps(arguments)
    fake.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(
            tool_calls=[fake_tool_call], content=None
        ))],
        usage=MagicMock(prompt_tokens=100, completion_tokens=50),
    )
    return fake


def _make_fake_openai_with_text(text: str) -> MagicMock:
    """构造一个只返回文字（无 tool_call）的 mock client（澄清场景）。"""
    fake = MagicMock()
    fake.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(tool_calls=None, content=text))],
        usage=MagicMock(prompt_tokens=100, completion_tokens=20),
    )
    return fake


class TestNlToScreenPlan:
    def test_basic_tool_call(self) -> None:
        fake = _make_fake_openai_with_tool_call({
            "trade_date": "2026-04-30",
            "stages": [{"label": "x", "rules": [{"name": "not_st", "args": {}}]}],
            "rationale": "test",
        })
        client = DeepSeekClient(api_key="fake", openai_client=fake)
        plan = client.nl_to_screen_plan("test query", today="2026-04-30")
        assert isinstance(plan, ScreenPlan)
        assert plan.trade_date == "2026-04-30"
        assert plan.rationale == "test"

    def test_empty_trade_date_filled_with_today(self) -> None:
        """LLM 返回 trade_date='' 时，client 用 today 填充。"""
        fake = _make_fake_openai_with_tool_call({
            "trade_date": "",
            "stages": [{"label": "x", "rules": [{"name": "not_st", "args": {}}]}],
        })
        client = DeepSeekClient(api_key="fake", openai_client=fake)
        plan = client.nl_to_screen_plan("test", today="2026-04-30")
        assert plan.trade_date == "2026-04-30"

    def test_clarification_response_raises(self) -> None:
        """LLM 返回纯文本 → 抛 LLMClarificationNeeded。"""
        fake = _make_fake_openai_with_text("请问你想筛选哪个板块？")
        client = DeepSeekClient(api_key="fake", openai_client=fake)
        with pytest.raises(LLMClarificationNeeded) as exc:
            client.nl_to_screen_plan("不清楚的需求", today="2026-04-30")
        assert "板块" in str(exc.value)

    def test_no_api_key_raises(self) -> None:
        with pytest.raises(ValueError, match="API key"):
            DeepSeekClient(api_key="")

    def test_writes_jsonl_log(self, tmp_path) -> None:
        fake = _make_fake_openai_with_tool_call({
            "trade_date": "",
            "stages": [{"label": "x", "rules": [{"name": "not_st", "args": {}}]}],
        })
        log_path = tmp_path / "nl_queries.jsonl"
        client = DeepSeekClient(api_key="fake", openai_client=fake, log_path=log_path)
        client.nl_to_screen_plan("test query", today="2026-04-30")
        assert log_path.exists()
        line = log_path.read_text().strip().splitlines()[-1]
        log = json.loads(line)
        assert log["query"] == "test query"
        assert log["tokens_in"] == 100
        assert log["tokens_out"] == 50
