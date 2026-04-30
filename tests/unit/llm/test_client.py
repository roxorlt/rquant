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

    def test_unknown_tool_name_raises_llm_error(self) -> None:
        """LLM 调用未注册工具 → LLMError。"""
        from rquant.llm.client import LLMError
        fake = MagicMock()
        fake_tool_call = MagicMock()
        fake_tool_call.function.name = "evil_tool"
        fake_tool_call.function.arguments = "{}"
        fake.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(
                tool_calls=[fake_tool_call], content=None,
            ))],
            usage=MagicMock(prompt_tokens=0, completion_tokens=0),
        )
        client = DeepSeekClient(api_key="fake", openai_client=fake)
        with pytest.raises(LLMError, match="未知工具"):
            client.nl_to_screen_plan("test", today="2026-04-30")

    def test_malformed_tool_arguments_raises_llm_error(self) -> None:
        """LLM tool call args 不是合法 JSON → LLMError。"""
        from rquant.llm.client import LLMError
        fake = MagicMock()
        fake_tool_call = MagicMock()
        fake_tool_call.function.name = "build_screen"
        fake_tool_call.function.arguments = "{not valid json"
        fake.chat.completions.create.return_value = MagicMock(
            choices=[MagicMock(message=MagicMock(
                tool_calls=[fake_tool_call], content=None,
            ))],
            usage=MagicMock(prompt_tokens=0, completion_tokens=0),
        )
        client = DeepSeekClient(api_key="fake", openai_client=fake)
        with pytest.raises(LLMError, match="不是合法 JSON"):
            client.nl_to_screen_plan("test", today="2026-04-30")

    def test_jsonl_write_failure_does_not_mask_clarification(self, tmp_path) -> None:
        """日志写入失败不应当压制 LLMClarificationNeeded。"""
        # Create a directory where the log file path would collide with a dir
        bad_log_path = tmp_path / "logs" / "should_be_file"
        bad_log_path.mkdir(parents=True)  # 把 log 路径占成目录，写入会失败

        fake = _make_fake_openai_with_text("请澄清一下你想选什么板块？")
        client = DeepSeekClient(api_key="fake", openai_client=fake, log_path=bad_log_path)
        # _log 应该静默失败而不是抛 IsADirectoryError
        with pytest.raises(LLMClarificationNeeded):
            client.nl_to_screen_plan("test", today="2026-04-30")
