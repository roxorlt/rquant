from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

AT = datetime(2026, 10, 5, 2, tzinfo=UTC)


def rule_definition():
    from rquant.alert_rule_contracts import ConditionAlertRuleDefinition

    return ConditionAlertRuleDefinition(
        rule_id="full-rule",
        name="完整条件",
        priority="P1",
        enabled=True,
        conditions=[{"name": "gt", "args": {"left": "INTRADAY_PRICE[0]", "right": 10}}],
        scope={"kind": "market"},
        governance={"channels": ["pushdeer"], "notify_recovery": True},
    )


def scope_evidence(*, owner: str = "alice"):
    from rquant.alert_rule_contracts import ConditionAlertScopeEvidence
    from rquant.runtime_contracts import canonical_sha256

    return ConditionAlertScopeEvidence(
        owner_id=owner,
        scope=rule_definition().scope,
        scope_version="b" * 64,
        member_codes=("600000.SH",),
        member_digest=canonical_sha256(("600000.SH",)),
        available_at=AT,
    )


def test_condition_rules_share_original_borrowed_transaction_and_owner_cas(tmp_path: Path) -> None:
    from rquant.condition_alert_rule_store import (
        ConditionAlertRuleRepository,
        ConditionAlertRuleUpsert,
    )
    from rquant.price_alert_rule_store import (
        PriceAlertRuleKey,
        PriceAlertRuleRepository,
        PriceAlertRuleTransactionError,
        PriceAlertRuleVersionConflictError,
    )

    connection = sqlite3.connect(tmp_path / "page-control.sqlite3", isolation_level=None)
    old = PriceAlertRuleRepository(connection)
    connection.execute("BEGIN IMMEDIATE")
    old.install_schema()
    old.install_condition_schema()
    connection.execute("COMMIT")
    repo = ConditionAlertRuleRepository(connection)
    command = ConditionAlertRuleUpsert(
        owner_id="alice", expected_version=None, rule=rule_definition()
    )
    with pytest.raises(PriceAlertRuleTransactionError):
        repo.upsert(command, now=AT, scope=scope_evidence())
    connection.execute("BEGIN IMMEDIATE")
    first = repo.upsert(command, now=AT, scope=scope_evidence())
    assert first.version == 1
    assert repo.list_current("bob") == ()
    assert old.get(PriceAlertRuleKey(owner_id="alice", rule_id="full-rule")) is None
    with pytest.raises(PriceAlertRuleVersionConflictError):
        repo.upsert(command, now=AT, scope=scope_evidence())
    second = repo.upsert(
        command.model_copy(update={"expected_version": 1}), now=AT, scope=scope_evidence()
    )
    assert second.version == 2
    connection.execute("ROLLBACK")
    assert repo.list_current("alice") == ()
    connection.close()


@pytest.mark.parametrize("scope", ["missing", "foreign", "future"])
def test_condition_rule_rejects_untrusted_scope_before_any_head_change(
    tmp_path: Path, scope: str
) -> None:
    from rquant.condition_alert_rule_store import (
        ConditionAlertRuleRepository,
        ConditionAlertRuleUpsert,
    )

    connection = sqlite3.connect(tmp_path / "page-control.sqlite3", isolation_level=None)
    connection.execute("BEGIN IMMEDIATE")
    repo = ConditionAlertRuleRepository(connection)
    repo.install_schema()
    evidence = (
        None
        if scope == "missing"
        else scope_evidence(owner="bob" if scope == "foreign" else "alice")
    )
    if scope == "future":
        evidence = evidence.model_copy(update={"available_at": AT.replace(hour=3)})
    with pytest.raises(ValueError):
        repo.upsert(
            ConditionAlertRuleUpsert(
                owner_id="alice", expected_version=None, rule=rule_definition()
            ),
            now=AT,
            scope=evidence,
        )
    assert repo.list_current("alice") == ()
    connection.execute("ROLLBACK")
    connection.close()


def test_condition_commands_use_original_owner_queue_effect_and_exact_recovery(
    tmp_path: Path,
) -> None:
    import json

    from rquant.page_control import (
        DeleteAlertRule,
        PageControlConsumer,
        PageControlOutbox,
        PageControlService,
        SaveAlertRule,
        SetAlertRuleEnabled,
        parse_page_control_command,
    )
    from rquant.price_alert_admission import PriceAlertAdmission, _decode_request
    from rquant.web.condition_alert_commands import ConditionRuleScopeResolution

    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    outbox.activate_condition_alert_rules(AT)
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        clock=lambda: AT,
        condition_rule_scope=lambda owner, rule, now: ConditionRuleScopeResolution(
            evidence=scope_evidence(owner=owner), is_current=lambda: True, consumer_ready=True
        ),
    )
    service = PageControlService(outbox=outbox, consumer=consumer)
    admission = PriceAlertAdmission(service)
    request = SaveAlertRule(
        command_id="condition-save", requested_at=AT, expected_version=None, rule=rule_definition()
    )
    with pytest.raises(ValueError):
        parse_page_control_command(request.model_dump())
    with pytest.raises(ValueError):
        service.submit(request)
    first = admission.submit(request, authenticated_owner_id="alice")
    assert first.status.value == "succeeded" and first.result["version"] == 1
    assert admission.resume(request, authenticated_owner_id="alice") == first
    assert admission.lookup(request, authenticated_owner_id="bob") is None
    decoded, owner = _decode_request(
        json.dumps(
            {"authenticated_owner_id": "alice", "command": request.model_dump(mode="json")}
        ).encode()
    )
    assert decoded == request and owner == "alice"
    disabled = admission.submit(
        SetAlertRuleEnabled(
            command_id="condition-disable",
            requested_at=AT,
            rule_id="full-rule",
            expected_version=1,
            enabled=False,
        ),
        authenticated_owner_id="alice",
    )
    assert disabled.result["version"] == 2 and disabled.result["enabled"] is False
    deleted = admission.submit(
        DeleteAlertRule(
            command_id="condition-delete", requested_at=AT, rule_id="full-rule", expected_version=2
        ),
        authenticated_owner_id="alice",
    )
    assert deleted.result["version"] == 3 and deleted.result["deleted"] is True
    assert outbox.effect(request.command_id).result == first.result


def test_condition_rule_projection_full_heads_validate_exact_owner_digest() -> None:
    from rquant.condition_alert_rule_store import ConditionAlertRuleEntry
    from rquant.condition_alert_runtime_projection import (
        ConditionRuleAuthoritySnapshot,
        condition_rule_projections,
        validate_condition_rule_projections,
    )

    entry = ConditionAlertRuleEntry(
        owner_id="alice",
        rule_id="full-rule",
        version=1,
        deleted=False,
        rule=rule_definition(),
        updated_at=AT,
    )
    snapshot = ConditionRuleAuthoritySnapshot.create(activated_at=AT, rows=(entry,))
    actual = condition_rule_projections(snapshot, observed_at=AT)
    validate_condition_rule_projections({item.table_name: item for item in actual})
    assert len(actual[1].rows) == 1
    assert "alice" not in actual[0].rows[0]["body_json"]


@pytest.mark.parametrize("point", ["head", "effect", "receipt", "before_commit", "generation"])
def test_condition_command_all_effects_and_terminal_receipt_roll_back_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    from rquant.page_control import (
        PageControlConsumer,
        PageControlOutbox,
        PageControlService,
        SaveAlertRule,
    )
    from rquant.price_alert_admission import PriceAlertAdmission
    from rquant.web.condition_alert_commands import ConditionRuleScopeResolution

    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    outbox.activate_condition_alert_rules(AT)

    def fault(stage: str) -> None:
        if stage == point:
            raise OSError("synthetic condition transaction fault")

    monkeypatch.setattr(outbox, "_condition_rule_failpoint", fault)
    current = [point != "generation"]
    clock = [AT]
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        clock=lambda: clock[0],
        condition_rule_scope=lambda owner, rule, now: ConditionRuleScopeResolution(
            evidence=scope_evidence(), is_current=lambda: current[0], consumer_ready=True
        ),
    )
    admission = PriceAlertAdmission(PageControlService(outbox=outbox, consumer=consumer))
    original = SaveAlertRule(
        command_id=f"failure-{point}",
        requested_at=AT,
        expected_version=None,
        rule=rule_definition(),
    )
    with pytest.raises((OSError, RuntimeError)):
        admission.submit(original, authenticated_owner_id="alice")
    receipt = admission.lookup(original, authenticated_owner_id="alice")
    assert receipt.status.value == "processing"
    assert outbox.effect(original.command_id) is None
    with sqlite3.connect(tmp_path / "page-control.sqlite3") as connection:
        assert connection.execute("SELECT count(*) FROM condition_alert_rule").fetchone()[0] == 0
    assert admission.lookup(original, authenticated_owner_id="alice") == receipt
    monkeypatch.setattr(outbox, "_condition_rule_failpoint", lambda stage: None)
    current[0] = True
    clock[0] = AT.replace(hour=3)
    recovered = admission.resume(original, authenticated_owner_id="alice")
    assert recovered.status.value == "succeeded" and recovered.result["version"] == 1
