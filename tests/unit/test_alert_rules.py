from __future__ import annotations

from datetime import datetime, timedelta

from rquant.alert_rules import AlertGate, rules_from_rows

T0 = datetime(2026, 10, 12, 10, 0)


def test_no_rules_allows_everything() -> None:
    assert AlertGate(None).allow("pool1", "L3", "A", T0)
    assert AlertGate([]).allow("pool1", "L3", "A", T0)


def test_scope_level_and_cooldown() -> None:
    gate = AlertGate(rules_from_rows([
        {"rule_id": "p2", "enabled": True, "pools": "pool2", "levels": "L1,L2",
         "cooldown_minutes": 30},
        {"rule_id": "off", "enabled": False, "pools": "", "levels": "", "cooldown_minutes": 0},
    ]))
    assert not gate.allow("pool1", "L1", "A", T0)          # pool out of scope
    assert not gate.allow("pool2", "L3", "A", T0)          # level out of scope
    assert gate.allow("pool2", "L1", "A", T0)
    assert not gate.allow("pool2", "L2", "A", T0 + timedelta(minutes=10))  # cooldown per code
    assert gate.allow("pool2", "L2", "B", T0 + timedelta(minutes=10))
    assert gate.allow("pool2", "L1", "A", T0 + timedelta(minutes=31))
