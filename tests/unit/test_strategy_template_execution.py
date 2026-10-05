"""Decision tests use raw inputs, rather than browser-supplied eligible flags."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from rquant.strategy_authoring_source import produce_template_entry
from rquant.strategy_template import StrategyTemplate
from rquant.strategy_template_execution import (
    TemplateEntryEvidence,
    TemplatePosition,
    TemplatePrice,
    TemplateSignal,
    strategy_template_entry,
    strategy_template_exit,
)

NOW = datetime(2026, 10, 5, 6, 30, tzinfo=UTC)
HASH = "1" * 64


def rules(entry: dict[str, object], exits: dict[str, object] | None = None) -> StrategyTemplate:
    return StrategyTemplate.model_validate(
        {
            "entry": entry,
            "exit": exits or {},
            "weight_rule": {"max_positions": 2},
            "rebalance_rule": {"kind": "daily"},
        }
    )


def test_conditions_actually_run_original_and_rules() -> None:
    template = rules(
        {
            "kind": "conditions",
            "conditions": [
                {"key": "not_st"},
                {"key": "gt", "args": {"left": "CLOSE[0]", "right": 10}},
            ],
        }
    )
    evidence = TemplateEntryEvidence(
        observed_at=NOW,
        source_hash=HASH,
        rows=(
            {"ts_code": "000001.SZ", "is_st": False, "CLOSE[0]": 11},
            {"ts_code": "000002.SZ", "is_st": True, "CLOSE[0]": 20},
            {"ts_code": "000003.SZ", "is_st": False, "CLOSE[0]": 9},
        ),
    )
    assert strategy_template_entry(
        template, produce_template_entry(template, evidence, decision_time=NOW), decision_time=NOW
    ) == ("000001.SZ",)
    with pytest.raises(ValueError, match="future"):
        produce_template_entry(template, evidence, decision_time=NOW - timedelta(seconds=1))


def test_pool_uses_exact_reference_and_signal_uses_exact_action_version() -> None:
    template = rules({"kind": "pool", "pool_key": "user/pool-a", "version": 3, "body_hash": HASH})
    evidence = TemplateEntryEvidence(
        observed_at=NOW,
        source_hash=HASH,
        pool_key="user/pool-a",
        pool_version=3,
        pool_codes=("000001.SZ",),
    )
    assert strategy_template_entry(
        template, produce_template_entry(template, evidence, decision_time=NOW), decision_time=NOW
    ) == ("000001.SZ",)
    with pytest.raises(ValueError, match="reference"):
        produce_template_entry(
            template, evidence.model_copy(update={"pool_version": 2}), decision_time=NOW
        )
    template = rules(
        {
            "kind": "signal",
            "strategy_id": "n_shape",
            "version": 1,
            "action": "b_intent",
            "source_hash": HASH,
        }
    )
    evidence = TemplateEntryEvidence(
        observed_at=NOW,
        source_hash=HASH,
        signals=(
            TemplateSignal(
                strategy_id="n_shape",
                version=1,
                action="b_intent",
                ts_code="000001.SZ",
                observed_at=NOW,
                source_hash=HASH,
            ),
            TemplateSignal(
                strategy_id="n_shape",
                version=2,
                action="b_intent",
                ts_code="000002.SZ",
                observed_at=NOW,
                source_hash=HASH,
            ),
        ),
    )
    assert strategy_template_entry(
        template, produce_template_entry(template, evidence, decision_time=NOW), decision_time=NOW
    ) == ("000001.SZ",)


@pytest.mark.parametrize(
    "exits,price,peak,days,reason",
    [
        ({"stop_loss": "0.1"}, "9", "10", 1, "stop_loss"),
        ({"take_profit": "0.2"}, "12", "12", 1, "take_profit"),
        ({"trailing_profit": "0.1"}, "10.8", "12", 1, "trailing_profit"),
        ({"max_holding_days": 3}, "10", "10", 3, "max_holding_days"),
        ({"exit_time": "14:30"}, "10", "10", 1, "exit_time"),
    ],
)
def test_each_exit_rule_changes_the_decision(
    exits: dict[str, object], price: str, peak: str, days: int, reason: str
) -> None:
    template = rules({"kind": "conditions", "conditions": [{"key": "not_st"}]}, exits)
    position = TemplatePosition(
        ts_code="000001.SZ",
        entry_price=Decimal("10"),
        eligible_high=Decimal(peak),
        holding_days=days,
        sellable_quantity=100,
    )
    quote = TemplatePrice(
        ts_code="000001.SZ",
        price=Decimal(price),
        observed_at=NOW,
        event_time=NOW,
        source_hash=HASH,
        suspended=False,
        sell_limit_locked=False,
        basis="minute_close",
    )
    assert strategy_template_exit(template, position, quote, decision_time=NOW) == reason
    assert (
        strategy_template_exit(
            template, position.model_copy(update={"sellable_quantity": 0}), quote, decision_time=NOW
        )
        is None
    )
    assert (
        strategy_template_exit(
            template,
            position,
            quote.model_copy(update={"sell_limit_locked": True}),
            decision_time=NOW,
        )
        is None
    )


def test_exit_priority_and_timed_price_evidence_are_fail_closed() -> None:
    template = rules(
        {"kind": "conditions", "conditions": [{"key": "not_st"}]},
        {"stop_loss": "0.1", "trailing_profit": "0.1", "max_holding_days": 1, "exit_time": "14:30"},
    )
    position = TemplatePosition(
        ts_code="000001.SZ",
        entry_price=Decimal("10"),
        eligible_high=Decimal("12"),
        holding_days=3,
        sellable_quantity=100,
    )
    quote = TemplatePrice(
        ts_code="000001.SZ",
        price=Decimal("9"),
        observed_at=NOW,
        event_time=NOW,
        source_hash=HASH,
        suspended=False,
        sell_limit_locked=False,
        basis="minute_close",
    )
    assert strategy_template_exit(template, position, quote, decision_time=NOW) == "stop_loss"
    with pytest.raises(ValueError, match="future"):
        strategy_template_exit(
            template,
            position,
            quote.model_copy(update={"observed_at": NOW + timedelta(seconds=1)}),
            decision_time=NOW,
        )
    timed = rules({"kind": "conditions", "conditions": [{"key": "not_st"}]}, {"exit_time": "14:30"})
    with pytest.raises(ValueError, match="minute"):
        strategy_template_exit(
            timed, position, quote.model_copy(update={"basis": "daily_close"}), decision_time=NOW
        )
