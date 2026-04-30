"""to_openai_tools() 输出格式校验。"""

from rquant.llm.registry import REGISTRY
from rquant.llm.schema_export import build_rule_catalog_md, to_openai_tools


class TestOpenAITools:
    def test_returns_single_tool(self) -> None:
        tools = to_openai_tools()
        assert len(tools) == 1

    def test_tool_has_function_type(self) -> None:
        tool = to_openai_tools()[0]
        assert tool["type"] == "function"
        assert tool["function"]["name"] == "build_screen"

    def test_rule_name_is_enum(self) -> None:
        tool = to_openai_tools()[0]
        params = tool["function"]["parameters"]
        rule_name_schema = (
            params["properties"]["stages"]["items"]["properties"]["rules"]
            ["items"]["properties"]["name"]
        )
        assert rule_name_schema["type"] == "string"
        enum_names = set(rule_name_schema["enum"])
        registry_names = {s.name for s in REGISTRY}
        assert enum_names == registry_names

    def test_required_fields(self) -> None:
        tool = to_openai_tools()[0]
        params = tool["function"]["parameters"]
        assert "trade_date" in params["required"]
        assert "stages" in params["required"]


class TestRuleCatalog:
    def test_catalog_lists_all_rules(self) -> None:
        md = build_rule_catalog_md()
        for spec in REGISTRY:
            assert spec.name in md, f"{spec.name} not in catalog"

    def test_catalog_includes_args_schema(self) -> None:
        md = build_rule_catalog_md()
        # CircMvLtArgs.threshold_yi 必须出现
        assert "threshold_yi" in md

    def test_catalog_groups_by_category(self) -> None:
        md = build_rule_catalog_md()
        for cat in ("filter", "state", "shape", "indicator", "aggregate", "compare"):
            assert cat.upper() in md or cat in md.lower()
