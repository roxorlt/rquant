"""Actual broker replay and frozen source binding for controlled templates."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import SaveStrategyTemplate
from rquant.strategy_authoring_source import StrategySourceCatalog, produce_template_entry, template_source_code_identity
from rquant.strategy_template import StrategyTemplate
from rquant.strategy_template_execution import TemplateEntryEvidence
from rquant.strategy_template_run import FrozenStrategyTemplateInput, TemplateDayEvidence, execute_strategy_template_input
from tests.unit.test_portfolio_backtest import _CODES, _at, _request
from tests.unit.test_strategy_authoring import NOW, draft


def frozen(tmp_path, *, exit_rules: dict[str, object] | None = None, rebalance: dict[str, object] | None = None, prices: tuple[str, ...] = ("10", "8", "8")) -> FrozenStrategyTemplateInput:
    request = _request((_CODES[0],) * len(prices))
    rules = StrategyTemplate.model_validate({"entry": {"kind": "conditions", "conditions": [{"key": "not_st"}]}, "exit": exit_rules or {}, "weight_rule": request.weight_rule.model_dump(mode="python"), "rebalance_rule": rebalance or {"kind": "daily"}})
    request = request.model_copy(update={"rebalance_rule": rules.rebalance_rule})
    days = tuple(day.model_copy(update={"instruments": tuple(instrument.model_copy(update={"decision_price": Decimal(price), "open_price": Decimal(price), "close_price": Decimal(price)}) for instrument in day.instruments)}) for day, price in zip(request.days, prices, strict=True))
    request = type(request).model_validate(request.model_copy(update={"days": days}).model_dump(mode="python"))
    target = StrategyAuthoringStore(tmp_path / "metadata.sqlite", definition_root=tmp_path / "definitions", producer_commit=request.producer_commit)
    target.initialize()
    command = draft().model_copy(update={"rules": rules})
    saved = target.save(command, owner_id="alice", catalog=StrategySourceCatalog(owner_id="alice", generation_id="generation-a", pools=(), signals=()))
    definition = target.definition_registry(saved.strategy_id).read_strategy_spec(saved.head.registration_fingerprint)
    evidence = tuple(TemplateDayEvidence(trade_date=day.trade_date, entry=produce_template_entry(rules, TemplateEntryEvidence(observed_at=day.ranking.observed_at, source_hash=day.ranking.source_identity, rows=({"ts_code": _CODES[0], "is_st": False},)), decision_time=_at(day.trade_date, 9, 25))) for day in days)
    return FrozenStrategyTemplateInput(owner_id="alice", rules=rules, definition=definition, request=request, source_code_identity=template_source_code_identity(), days=evidence)


def test_template_stop_loss_uses_real_broker_costs_and_exits_before_reentry(tmp_path) -> None:
    value = frozen(tmp_path, exit_rules={"stop_loss": "0.1"})
    research = tmp_path / "research"
    research.mkdir(mode=0o700)
    result = execute_strategy_template_input(value, research_root=research)
    assert result.status == "complete" and result.strategy_id == value.definition.logical_id
    assert result.definition_record_hash == value.definition.record_hash
    assert result.input_hash == value.input_hash
    assert [(row.trade_date, row.reason) for row in result.exit_decisions] == [(value.request.days[1].trade_date, "stop_loss")]
    assert [order.intent.side.value for order in result.days[1].orders] == ["SELL"]
    assert result.days[0].account.holdings[0].available_quantity == 0
    assert result.days[1].account.cash == Decimal("2789.20")
    assert result.days[1].account.holdings == ()
    assert result.days[0].fees == Decimal("5") and result.days[1].fees == Decimal("5.80")
    assert result.days[2].orders[0].intent.side.value == "BUY"
    assert tuple(research.iterdir()) == ()


def test_full_exit_parameter_change_changes_actual_decisions(tmp_path) -> None:
    one = tmp_path / "one"
    two = tmp_path / "two"
    one.mkdir(mode=0o700)
    two.mkdir(mode=0o700)
    tight = frozen(one, exit_rules={"stop_loss": "0.1"})
    loose = frozen(two, exit_rules={"stop_loss": "0.3"})
    result_tight = execute_strategy_template_input(tight, research_root=one)
    result_loose = execute_strategy_template_input(loose, research_root=two)
    assert len(result_tight.exit_decisions) == 1 and result_loose.exit_decisions == ()
    assert result_tight.input_hash != result_loose.input_hash


def test_projection_swap_rules_swap_and_future_raw_fact_are_rejected(tmp_path) -> None:
    value = frozen(tmp_path)
    projection = value.days[0].entry
    forged = projection.model_copy(update={"eligible_codes": (_CODES[1],)})
    with pytest.raises(ValueError, match="projection"):
        FrozenStrategyTemplateInput.model_validate({**value.model_dump(mode="python"), "input_hash": None, "days": (value.days[0].model_copy(update={"entry": forged}), *value.days[1:])})
    with pytest.raises(ValueError, match="rules"):
        FrozenStrategyTemplateInput.model_validate({**value.model_dump(mode="python"), "input_hash": None, "rules": value.rules.model_copy(update={"exit": type(value.rules.exit)(stop_loss=Decimal("0.1"))})})
    future = projection.evidence.model_copy(update={"observed_at": _at(value.days[0].trade_date, 9, 25) + timedelta(seconds=1)})
    with pytest.raises(ValueError, match="future"):
        produce_template_entry(value.rules, future, decision_time=_at(value.days[0].trade_date, 9, 25))


def test_timed_exit_without_actual_minute_price_and_status_cannot_run(tmp_path) -> None:
    with pytest.raises(ValueError, match="minute"):
        frozen(tmp_path, exit_rules={"exit_time": "14:30"})


def test_max_holding_days_follows_trading_calendar_on_non_rebalance_day(tmp_path) -> None:
    value = frozen(tmp_path, exit_rules={"max_holding_days": 1}, rebalance={"kind": "every_n", "every_n_days": 5}, prices=("10", "10", "10"))
    result = execute_strategy_template_input(value, research_root=tmp_path)
    assert result.days[1].rebalanced is False
    assert [row.reason for row in result.exit_decisions] == ["max_holding_days"]
    assert result.days[1].account.holdings == ()
