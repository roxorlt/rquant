"""System prompt 渲染测试。"""

from rquant.llm.prompts import FEW_SHOT_EXAMPLES, build_system_prompt


class TestSystemPrompt:
    def test_includes_rule_catalog(self) -> None:
        prompt = build_system_prompt()
        assert "not_st" in prompt
        assert "circ_mv_lt" in prompt
        assert "cross_above" in prompt

    def test_includes_args_schema(self) -> None:
        prompt = build_system_prompt()
        # CircMvLtArgs.threshold_yi 出现
        assert "threshold_yi" in prompt

    def test_mentions_offset_semantics(self) -> None:
        prompt = build_system_prompt()
        assert "0=今天" in prompt or "offset" in prompt.lower()


class TestFewShot:
    def test_at_least_4_examples(self) -> None:
        assert len(FEW_SHOT_EXAMPLES) >= 4

    def test_each_example_has_user_and_assistant(self) -> None:
        for ex in FEW_SHOT_EXAMPLES:
            assert "user" in ex
            assert "assistant_tool_call" in ex
            assert ex["assistant_tool_call"]["name"] == "build_screen"

    def test_examples_use_registered_rules(self) -> None:
        from rquant.llm.registry import REGISTRY_BY_NAME
        for ex in FEW_SHOT_EXAMPLES:
            args = ex["assistant_tool_call"]["arguments"]
            for stage in args["stages"]:
                for rule in stage["rules"]:
                    assert rule["name"] in REGISTRY_BY_NAME
