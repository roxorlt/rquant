"""ScreenPlan → screen() 端到端测试（不调 LLM）。"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.llm.dispatch import build_rules
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage


class TestBuildRules:
    def test_empty_plan(self) -> None:
        plan = ScreenPlan(trade_date="2026-04-30", stages=[])
        rules = build_rules(plan)
        assert rules == []

    def test_single_no_args_rule(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[Stage(label="过滤", rules=[RuleCall(name="not_st")])],
        )
        rules = build_rules(plan)
        assert len(rules) == 1
        assert callable(rules[0])

    def test_rule_with_args(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[Stage(label="过滤", rules=[
                RuleCall(name="circ_mv_lt", args={"threshold_yi": 100}),
            ])],
        )
        rules = build_rules(plan)
        assert len(rules) == 1

    def test_unknown_rule_raises(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[Stage(label="x", rules=[RuleCall(name="bogus_rule")])],
        )
        with pytest.raises(ValueError, match="Unknown rule"):
            build_rules(plan)

    def test_invalid_args_raises(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[Stage(label="x", rules=[
                RuleCall(name="circ_mv_lt", args={"threshold_yi": -10}),
            ])],
        )
        with pytest.raises(ValidationError):
            build_rules(plan)

    def test_multiple_stages_flatten(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[
                Stage(label="A", rules=[RuleCall(name="not_st"), RuleCall(name="not_bj")]),
                Stage(label="B", rules=[RuleCall(name="first_limit_up", args={"offset": 1})]),
            ],
        )
        rules = build_rules(plan)
        assert len(rules) == 3


class TestScreenWithPlanDiagnostic:
    def test_empty_plan_returns_empty_diagnostics(self, tmp_path: Path) -> None:
        from rquant.llm.dispatch import screen_with_plan_diagnostic
        from rquant.llm.schemas import ScreenPlan
        from rquant.storage.duckdb import DuckDBStore

        plan = ScreenPlan(trade_date="2026-04-30", stages=[])
        store = DuckDBStore(tmp_path / "dispatch.duckdb")
        try:
            df, diag = screen_with_plan_diagnostic(plan, store=store)
        finally:
            store.close()
        assert diag == []
        assert df is not None  # screen() 总是返回 DataFrame
