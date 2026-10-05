"""Independent cash references for all controlled rule choices on the real broker."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_source import StrategySourceCatalog, TemplatePoolReference, TemplateSignalReference, produce_template_entry, template_source_code_identity
from rquant.strategy_template import StrategyTemplate
from rquant.strategy_template_execution import TemplateEntryEvidence, TemplatePrice, TemplateSignal
from rquant.strategy_template_run import FrozenStrategyTemplateInput, TemplateDayEvidence, TemplateIndexClose, TemplateMinuteExecution, execute_strategy_template_input
from tests.unit.test_portfolio_backtest import _CODES, _at
from tests.unit.test_strategy_authoring import draft
from tests.unit.test_strategy_template_run import frozen

HASH = "c" * 64


def rebind(tmp_path, value: FrozenStrategyTemplateInput, *, rules: StrategyTemplate, evidence: tuple[TemplateDayEvidence, ...], sources: StrategySourceCatalog | None = None) -> FrozenStrategyTemplateInput:
    target = StrategyAuthoringStore(tmp_path / "new-metadata.sqlite", definition_root=tmp_path / "new-definitions", producer_commit=value.request.producer_commit)
    target.initialize()
    command = draft().model_copy(update={"rules": rules})
    saved = target.save(command, owner_id="alice", catalog=sources or StrategySourceCatalog(owner_id="alice", generation_id="generation-a", pools=(), signals=()))
    definition = target.definition_registry(saved.strategy_id).read_strategy_spec(saved.head.registration_fingerprint)
    return FrozenStrategyTemplateInput(owner_id="alice", rules=rules, definition=definition, request=value.request, source_code_identity=template_source_code_identity(), days=evidence)


@pytest.mark.parametrize("kind", ["conditions", "pool", "signal"])
def test_three_entry_sources_consume_original_facts_and_share_manual_cash(tmp_path, kind: str) -> None:
    value = frozen(tmp_path, rebalance={"kind": "every_n", "every_n_days": 5}, prices=("10", "10", "10"))
    if kind == "conditions":
        exact = value
    else:
        entry = {"kind": "pool", "pool_key": "private/pool", "version": 2, "body_hash": HASH} if kind == "pool" else {"kind": "signal", "strategy_id": "n_shape", "version": 1, "action": "b_intent", "source_hash": HASH}
        rules = StrategyTemplate.model_validate({**value.rules.model_dump(mode="python"), "entry": entry})
        sources = StrategySourceCatalog(owner_id="alice", generation_id="generation-a", pools=(TemplatePoolReference(pool_key="private/pool", version=2, body_hash=HASH, owner_id="alice", name="候选池"),) if kind == "pool" else (), signals=(TemplateSignalReference(strategy_id="n_shape", version=1, source_hash=HASH, actions=("b_intent",), owner_id=None, name="N形"),) if kind == "signal" else ())
        days = []
        for day in value.request.days:
            raw = TemplateEntryEvidence(observed_at=day.ranking.observed_at, source_hash=HASH, pool_key="private/pool" if kind == "pool" else None, pool_version=2 if kind == "pool" else None, pool_codes=(_CODES[0],) if kind == "pool" else (), signals=(TemplateSignal(strategy_id="n_shape", version=1, action="b_intent", ts_code=_CODES[0], observed_at=day.ranking.observed_at, source_hash=HASH), TemplateSignal(strategy_id="n_shape", version=1, action="watch", ts_code=_CODES[1], observed_at=day.ranking.observed_at, source_hash=HASH)) if kind == "signal" else ())
            days.append(TemplateDayEvidence(trade_date=day.trade_date, entry=produce_template_entry(rules, raw, decision_time=_at(day.trade_date, 9, 25))))
        exact = rebind(tmp_path, value, rules=rules, evidence=tuple(days), sources=sources)
    result = execute_strategy_template_input(exact, research_root=tmp_path)
    # 3,000 capital, 50% target: 100 shares at 10 + 5 commission; 1,995 cash.
    assert len(result.days[0].orders) == 1 and result.days[0].orders[0].receipt.fill.quantity == 100
    assert result.days[0].account.cash == Decimal("1995.00")
    assert result.days[0].account.nav == Decimal("2995.00")
    assert all(day.account.holdings[0].code == _CODES[0] for day in result.days)
    assert result.days[1].orders == () and result.days[2].orders == ()


@pytest.mark.parametrize("exit_rules,prices,exit_index,reason,cash", [
    ({"stop_loss": "0.1"}, ("10", "8", "8"), 1, "stop_loss", "2789.20"),
    ({"take_profit": "0.2"}, ("10", "12", "12"), 1, "take_profit", "3188.80"),
    ({"trailing_profit": "0.1"}, ("10", "12", "10.8"), 2, "trailing_profit", "3068.92"),
    ({"max_holding_days": 1}, ("10", "10", "10"), 1, "max_holding_days", "2989.00"),
])
def test_four_daily_exits_change_original_fills_and_manual_cash(tmp_path, exit_rules, prices, exit_index, reason, cash) -> None:
    value = frozen(tmp_path, exit_rules=exit_rules, rebalance={"kind": "every_n", "every_n_days": 5}, prices=prices)
    result = execute_strategy_template_input(value, research_root=tmp_path)
    assert [(row.trade_date, row.reason) for row in result.exit_decisions] == [(value.days[exit_index].trade_date, reason)]
    assert result.days[exit_index].account.holdings == ()
    assert result.days[exit_index].account.cash == Decimal(cash)
    assert result.days[exit_index].orders[0].receipt.fill.quantity == 100


def timed_input(tmp_path) -> FrozenStrategyTemplateInput:
    value = frozen(tmp_path, rebalance={"kind": "every_n", "every_n_days": 5}, prices=("10", "10", "10"))
    rules = StrategyTemplate.model_validate({**value.rules.model_dump(mode="python"), "exit": {"exit_time": "14:30"}})
    days = []
    for day, raw in zip(value.request.days, value.days, strict=True):
        minutes = tuple(TemplateMinuteExecution(quote=TemplatePrice(ts_code=instrument.ts_code, price=Decimal("9"), event_time=_at(day.trade_date, 14, 30), observed_at=_at(day.trade_date, 14, 30), source_hash=HASH, suspended=False, sell_limit_locked=False, basis="minute_close"), execution_at=_at(day.trade_date, 14, 31), execution_price=Decimal("9"), execution_source_hash="b" * 64, conditions=instrument.conditions.model_copy(update={"observed_at": _at(day.trade_date, 14, 31)})) for instrument in day.instruments)
        days.append(TemplateDayEvidence(trade_date=day.trade_date, entry=produce_template_entry(rules, raw.entry.evidence, decision_time=_at(day.trade_date, 9, 25)), minutes=minutes))
    return rebind(tmp_path, value, rules=rules, evidence=tuple(days))


def test_timed_exit_uses_actual_minute_price_status_and_original_t_plus_one(tmp_path) -> None:
    value = timed_input(tmp_path)
    result = execute_strategy_template_input(value, research_root=tmp_path)
    assert result.days[0].orders[0].intent.side.value == "BUY" and len(result.days[0].orders) == 1
    assert [(row.trade_date, row.reason, row.decided_at) for row in result.exit_decisions] == [(value.days[1].trade_date, "exit_time", _at(value.days[1].trade_date, 14, 30))]
    order = result.days[1].orders[0]
    assert order.receipt.fill.price == Decimal("9") and order.intent.earliest_execution_at == _at(value.days[1].trade_date, 14, 31)
    assert result.days[1].account.cash == Decimal("2889.10")
    assert result.days[1].account.holdings == ()


def test_minute_execution_on_future_date_cannot_enter_current_day_valuation(tmp_path) -> None:
    value = timed_input(tmp_path)
    payload = value.model_dump(mode="python")
    minute = payload["days"][0]["minutes"][0]
    minute["execution_at"] += timedelta(days=1)
    minute["conditions"]["observed_at"] = minute["execution_at"]
    payload["input_hash"] = None
    with pytest.raises(ValueError, match="minute.*(date|valuation)"):
        FrozenStrategyTemplateInput.model_validate(payload)


def test_index_filter_consumes_exact_prior_closes_but_does_not_block_exit(tmp_path) -> None:
    value = frozen(tmp_path, exit_rules={"max_holding_days": 1}, rebalance={"kind": "every_n", "every_n_days": 5}, prices=("10", "10", "10"))
    dates = (date(2026, 8, 6), *value.request.calendar.dates)
    calendar = value.request.calendar.model_copy(update={"coverage_start": dates[0], "dates": dates})
    value = value.model_copy(update={"request": value.request.model_copy(update={"calendar": calendar})})
    rules = StrategyTemplate.model_validate({**value.rules.model_dump(mode="python"), "index_filter": {"benchmark_code": "000300.SH", "ma_days": 2, "direction": "above"}})
    days = []
    for index, (day, raw) in enumerate(zip(value.request.days, value.days, strict=True)):
        position = dates.index(day.trade_date)
        prices = ("10", "12") if index == 0 else ("12", "10")
        window = tuple(TemplateIndexClose(trade_date=trade_date, price=Decimal(price), observed_at=_at(trade_date, 15), source_hash=HASH, benchmark_code="000300.SH") for trade_date, price in zip(dates[position-2:position], prices, strict=True))
        days.append(TemplateDayEvidence(trade_date=day.trade_date, entry=produce_template_entry(rules, raw.entry.evidence, decision_time=_at(day.trade_date, 9, 25)), index_closes=window))
    exact = rebind(tmp_path, value, rules=rules, evidence=tuple(days))
    result = execute_strategy_template_input(exact, research_root=tmp_path)
    assert result.days[0].orders[0].intent.side.value == "BUY"
    assert result.days[1].rebalanced is False and result.days[1].orders[0].intent.side.value == "SELL"
    assert result.days[1].account.cash == Decimal("2989.00") and result.days[2].orders == ()
