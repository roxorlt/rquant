"""ScreenPlan / Stage / RuleCall Pydantic 模型测试。"""

import pytest
from pydantic import ValidationError

from rquant.llm.schemas import RuleCall, ScreenPlan, Stage


class TestRuleCall:
    def test_minimal(self) -> None:
        rc = RuleCall(name="not_st")
        assert rc.name == "not_st"
        assert rc.args == {}

    def test_with_args(self) -> None:
        rc = RuleCall(name="circ_mv_lt", args={"threshold_yi": 100})
        assert rc.args["threshold_yi"] == 100

    def test_name_required(self) -> None:
        with pytest.raises(ValidationError):
            RuleCall()  # type: ignore[call-arg]


class TestStage:
    def test_minimal(self) -> None:
        s = Stage(label="基础过滤", rules=[])
        assert s.label == "基础过滤"
        assert s.rules == []

    def test_with_rules(self) -> None:
        s = Stage(
            label="形态",
            rules=[RuleCall(name="first_limit_up", args={"offset": 1})],
        )
        assert len(s.rules) == 1

    def test_empty_label_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Stage(label="", rules=[])


class TestScreenPlan:
    def test_minimal(self) -> None:
        plan = ScreenPlan(trade_date="2026-04-30", stages=[])
        assert plan.trade_date == "2026-04-30"
        assert plan.stages == []
        assert plan.include_columns == []
        assert plan.rationale == ""

    def test_full(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[
                Stage(label="过滤", rules=[RuleCall(name="not_st")]),
                Stage(label="形态", rules=[
                    RuleCall(name="first_limit_up", args={"offset": 1}),
                ]),
            ],
            rationale="测试",
            include_columns=["CIRC_MV[0]"],
        )
        assert len(plan.stages) == 2
        assert plan.rationale == "测试"
        assert plan.include_columns == ["CIRC_MV[0]"]

    def test_invalid_date_format_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ScreenPlan(trade_date="not-a-date", stages=[])

    def test_short_date_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ScreenPlan(trade_date="2026-4-30", stages=[])

    def test_flatten_rules(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[
                Stage(label="A", rules=[RuleCall(name="r1"), RuleCall(name="r2")]),
                Stage(label="B", rules=[RuleCall(name="r3")]),
            ],
        )
        flat = plan.flatten_rules()
        assert [rc.name for rc in flat] == ["r1", "r2", "r3"]
