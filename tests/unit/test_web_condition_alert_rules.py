from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def test_condition_private_routes_require_identity_and_hide_owner_query(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    app = create_app(
        WebSettings(serving_root=root, ingress_socket_path=tmp_path / "ingress.sock"),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=1),
    )
    with TestClient(app) as client:
        assert client.get("/api/v1/monitor/condition-rules").status_code == 401
        assert (
            client.get(
                "/api/v1/monitor/condition-rules", headers={"X-Rquant-User": "alice"}
            ).status_code
            == 401
        )


def test_full_condition_http_contract_rejects_owner_and_extra_intent() -> None:
    import pytest

    from rquant.web.models.condition_alert_rules import ConditionAlertRuleCommandRequest
    from tests.unit.test_condition_alert_rule_store import AT, rule_definition

    payload = {
        "command_id": "private-original",
        "requested_at": AT,
        "generation_id": "a" * 64,
        "rule_id": "full-rule",
        "action": "save",
        "rule": rule_definition(),
        "owner_id": "alice",
    }
    with pytest.raises(ValueError):
        ConditionAlertRuleCommandRequest.model_validate(payload)
    payload.pop("owner_id")
    value = ConditionAlertRuleCommandRequest.model_validate(payload)
    assert value.rule.conditions[0].name == "gt"
    with pytest.raises(ValueError):
        ConditionAlertRuleCommandRequest.model_validate({**payload, "enabled": True})


def test_actual_serving_private_condition_heads_zero_unknown_and_tombstone(tmp_path: Path) -> None:
    from rquant.condition_alert_rule_store import ConditionAlertRuleEntry
    from rquant.condition_alert_runtime_projection import (
        ConditionRuleAuthoritySnapshot,
        condition_rule_projections,
    )
    from tests.unit.test_condition_alert_rule_store import rule_definition
    from tests.unit.test_web_price_alert_rules import client_for

    root = tmp_path / "serving"
    at = FIXTURE_BUILT_AT - timedelta(seconds=1)
    definition = rule_definition()
    heads = (
        ConditionAlertRuleEntry(
            owner_id="alice",
            rule_id=definition.rule_id,
            version=2,
            deleted=False,
            rule=definition,
            updated_at=at,
        ),
        ConditionAlertRuleEntry(
            owner_id="bob",
            rule_id=definition.rule_id,
            version=3,
            deleted=False,
            rule=definition,
            updated_at=at,
        ),
        ConditionAlertRuleEntry(
            owner_id="alice", rule_id="removed", version=4, deleted=True, rule=None, updated_at=at
        ),
    )
    build_web_fixture(
        root,
        "baseline",
        signal_projections=condition_rule_projections(
            ConditionRuleAuthoritySnapshot.create(activated_at=at, rows=heads),
            observed_at=FIXTURE_BUILT_AT,
        ),
    )
    with client_for(root, clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=1)) as client:
        data = client.get(
            "/api/v1/monitor/condition-rules", headers={"X-Rquant-User": "alice"}
        ).json()["data"]
        assert data["availability"] == "ready"
        assert data["can_enable"] is False
        assert len(data["blocks"]) == 26 and len(data["items"]) == 1
        assert data["items"][0]["version"] == 2
        assert (
            data["items"][0]["matched_count"] is None and data["items"][0]["unknown_count"] is None
        )
        deleted = client.get(
            "/api/v1/monitor/condition-rules/head?rule_id=removed",
            headers={"X-Rquant-User": "alice"},
        ).json()["data"]
        assert (
            deleted["status"] == "deleted" and deleted["version"] == 4 and deleted["item"] is None
        )
        foreign = client.get(
            "/api/v1/monitor/condition-rules/head?rule_id=removed", headers={"X-Rquant-User": "bob"}
        ).json()["data"]
        assert foreign["status"] == "absent" and foreign["version"] is None
        assert (
            client.get(
                "/api/v1/monitor/condition-rules?owner_id=bob", headers={"X-Rquant-User": "alice"}
            ).status_code
            == 422
        )
