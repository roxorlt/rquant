from __future__ import annotations

import importlib
from datetime import time

import pytest
from pydantic import ValidationError


def test_full_condition_rule_uses_original_registry_and_typed_scope_frequency() -> None:
    module = importlib.import_module("rquant.alert_rule_contracts")
    rule = module.ConditionAlertRuleDefinition(
        rule_id="rule-1",
        name="条件提醒",
        priority="P2",
        enabled=True,
        conditions=[{"name": "gt", "args": {"left": "INTRADAY_PRICE[0]", "right": 10}}],
        scope={"kind": "market"},
        frequency={"kind": "per_symbol_minutes", "minutes": 5},
        governance={"dedup_window_seconds": 60, "notify_recovery": True, "channels": ["pushdeer"]},
    )
    assert (
        rule.conditions[0].name == "gt"
        and rule.source_policy.daily_anchor == "previous_closed_session"
    )
    assert rule.rule_body_hash == rule.rule_body_hash
    for update in (
        {"conditions": [{"name": "nonexistent", "args": {}}]},
        {"owner_id": "alice"},
        {"frequency": {"kind": "per_symbol_minutes", "minutes": 0}},
        {"governance": {"channels": []}},
        {"scope": {"kind": "watchlist"}},
        {"conditions": [{"name": "gt", "args": {"left": "UNREGISTERED[0]", "right": 10}}]},
        {
            "conditions": [
                {"name": "between", "args": {"field": "INTRADAY_PRICE[1]", "low": 1, "high": 20}}
            ]
        },
    ):
        with pytest.raises((ValidationError, ValueError)):
            module.ConditionAlertRuleDefinition.model_validate({**rule.model_dump(), **update})


def test_condition_sessions_reject_lunch_midnight_and_aware_clock() -> None:
    module = importlib.import_module("rquant.alert_rule_contracts")
    for start, end in ((time(11), time(13, 30)), (time(15), time(9, 30)), (time(9, 29), time(10))):
        with pytest.raises(ValidationError):
            module.ConditionAlertTimeWindow(start=start, end=end)
    assert module.ConditionAlertBarCloseFrequency().bar_size == "1min"


def test_condition_scope_evidence_is_owner_bound() -> None:
    module = importlib.import_module("rquant.alert_rule_contracts")
    from rquant.runtime_contracts import canonical_sha256
    from tests.unit.test_screen_query_history import NOW

    body = {
        "scope": {"kind": "watchlist", "membership_version": "a" * 64},
        "scope_version": "b" * 64,
        "member_codes": ["000001.SZ"],
        "member_digest": canonical_sha256(("000001.SZ",)),
        "available_at": NOW,
    }
    with pytest.raises(ValidationError):
        module.ConditionAlertScopeEvidence.model_validate(body)
    evidence = module.ConditionAlertScopeEvidence.model_validate({**body, "owner_id": "alice"})
    assert evidence.owner_id == "alice"
