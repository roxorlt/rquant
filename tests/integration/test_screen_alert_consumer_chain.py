from __future__ import annotations

import json
from datetime import timedelta
from hashlib import sha256
from pathlib import Path

import pytest

from rquant.runtime_contracts import canonical_sha256
from tests.unit.test_condition_alert_route import AT, condition_route_fixture
from tests.unit.test_condition_alert_rule_store import rule_definition


def condition_delivery_fixture(tmp_path: Path, *, actual_round=None):
    from rquant.alert_rule_contracts import ConditionAlertScopeEvidence, OwnedConditionAlertRule
    from rquant.condition_alert_rule_store import ConditionAlertRuleEntry
    from rquant.condition_alert_runtime_contracts import (
        ConditionAlertEventEnvelope,
        ConditionAlertProducerEventRecord,
        verify_condition_alert_activation,
    )
    from rquant.condition_alert_runtime_projection import (
        ConditionAlertDeliveryAuthorityInput,
        ConditionConsumerProof,
        ConditionDeliveryScope,
        ConditionRuleAuthoritySnapshot,
    )
    from rquant.notification_state import NotificationStateStore
    from rquant.runtime_service_entrypoint import RuntimeServiceKind
    from rquant.signal_route_spool import (
        ReadonlyNotificationEventRouteSpool,
        SignalRouteSpool,
        publish_mixed_notification_bus_prefix,
    )

    bus, router, policy, source, original = condition_route_fixture(
        tmp_path, produced_source=None if actual_round is None else actual_round[0]
    )
    if actual_round is None:
        definition = rule_definition()
        codes = ("600000.SH",)
        scope = ConditionAlertScopeEvidence(
            owner_id="alice",
            scope=definition.scope,
            scope_version="2" * 64,
            member_codes=codes,
            member_digest=canonical_sha256(codes),
            available_at=AT,
        )
        event = ConditionAlertEventEnvelope.create(
            **{
                **original.event.model_dump(mode="python", exclude={"event_id"}),
                "rule_body_hash": definition.rule_body_hash,
                "member_digest": scope.member_digest,
            }
        )
        item = ConditionAlertProducerEventRecord(
            sequence=1,
            event=event,
            payload_json=event.wire_bytes().decode(),
            payload_sha256=event.sha256,
        )
    else:
        _, item, definition, scope = actual_round
        event = item.event
    routed = bus.commit_condition_alert_route(
        activation=router,
        policy=policy,
        source=source,
        record=item,
        source_inspected_at=AT,
        routed_at=AT,
    )
    settings = json.loads((tmp_path / "condition-router.json").read_text())["settings"][
        "condition_alert_runtime"
    ]
    settings.update(routing_enabled=False, delivery_enabled=True)
    path = tmp_path / "notifier.json"
    path.write_text(
        json.dumps(
            {
                "service_id": "condition.notifier",
                "service_kind": "notifier",
                "plane": "live",
                "interval_seconds": 2,
                "stale_after_seconds": 10,
                "producer_commit": "a" * 40,
                "settings": {"condition_alert_runtime": settings},
            }
        )
    )
    path.chmod(0o600)
    notifier_hash = sha256(path.read_bytes()).hexdigest()
    activation = verify_condition_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=notifier_hash,
        expected_commit="a" * 40,
        expected_kind=RuntimeServiceKind.NOTIFIER,
    )
    spool = SignalRouteSpool(tmp_path / "spool")
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=AT)
    reader = ReadonlyNotificationEventRouteSpool(tmp_path / "spool")
    state = NotificationStateStore(tmp_path / "notifier.sqlite3")
    state.install_condition_alert_delivery_v1(activation)
    state.replicate_mixed_notification_events(
        reader.source_descriptor(),
        reader.routed_after_global_sequence(
            after_sequence=0, through_sequence=1, limit=100, observed_at=AT
        ),
        observed_at=AT,
        source_inspected_at=AT,
    )
    head = ConditionAlertRuleEntry(
        owner_id="alice",
        rule_id=definition.rule_id,
        version=event.rule_version,
        deleted=False,
        rule=definition,
        updated_at=AT,
    )
    authority = ConditionAlertDeliveryAuthorityInput(
        rules=ConditionRuleAuthoritySnapshot.create(activated_at=AT, rows=(head,)),
        scopes=(
            ConditionDeliveryScope(
                rule=OwnedConditionAlertRule(
                    owner_id="alice", version=event.rule_version, rule=definition, updated_at=AT
                ),
                scope=scope,
            ),
        ),
        policy=policy,
        producer=ConditionConsumerProof(
            producer_manifest_sha256=source.producer_manifest_sha256,
            notifier_manifest_sha256=notifier_hash,
            evaluation_contract_sha256=source.evaluation_contract_sha256,
            routing_contract_sha256=source.routing_policy_sha256,
            frequency_policy_sha256=source.frequency_policy_sha256,
            source_epoch=source.source_epoch,
            producer_generation_id=source.generation_id,
            source_identity=event.source_identity,
            serving_generation_id="1" * 64,
            inspected_at=AT,
            source_cutoff=AT,
            full_source_ready=True,
            feature_contract_version=4,
        ),
        notifier_manifest_sha256=notifier_hash,
        delivery_enabled=True,
        inspected_at=AT,
    )
    state.apply_condition_alert_delivery_authority(
        authority, activation=activation, expected_revision=0, applied_at=AT
    )
    return state, activation, authority, routed


def test_condition_original_mixed_spool_to_recipient_provider_and_actual_attempt(
    tmp_path: Path,
) -> None:
    from rquant.delivery_contracts import DeliveryChannel
    from rquant.notification_worker import run_notification_batch
    from rquant.runtime_notification_providers import (
        NotificationTransportDisposition,
        NotificationTransportResult,
        RecipientNotificationCapabilities,
        RecipientScopedNotificationProvider,
    )

    state, activation, authority, routed = condition_delivery_fixture(tmp_path)
    sent = []

    class Transport:
        def send(self, **message):
            sent.append(message)
            return NotificationTransportResult(
                disposition=NotificationTransportDisposition.ACCEPTED
            )

    provider = RecipientScopedNotificationProvider(
        channel=DeliveryChannel.PUSHDEER,
        endpoint="https://example.invalid/unit",
        capabilities=RecipientNotificationCapabilities(
            {DeliveryChannel.PUSHDEER: {"admin": "synthetic-offline-unit"}}
        ),
        transport=Transport(),
    )
    run = run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: provider},
        worker_id="condition-worker",
        now=AT,
        clock=lambda: AT,
        lease_for=timedelta(seconds=10),
        limit=10,
        condition_activation=activation,
    )
    assert run.succeeded_count == 1 and len(sent) == 1
    assert "完整条件" in sent[0]["body"] and "600000.SH" in sent[0]["body"]
    row = state.outbox_records()[0]
    assert (
        row.status.value == "succeeded"
        and state.attempts(row.outbox_id)[0].provider_receipt is not None
    )
    assert state.condition_alert_send_admission(row.outbox_id, 1).event_id == routed.event_id
    again = run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: provider},
        worker_id="condition-worker",
        now=AT,
        clock=lambda: AT,
        lease_for=timedelta(seconds=10),
        limit=10,
        condition_activation=activation,
    )
    assert again.claimed_count == 0 and len(sent) == 1


@pytest.mark.parametrize("fault", ["disable", "scope", "recipient", "unknown_source"])
def test_condition_actual_authority_change_cancels_or_defers_before_send(
    tmp_path: Path, fault: str
) -> None:
    from rquant.alert_rule_contracts import ConditionAlertRuleDefinition, OwnedConditionAlertRule
    from rquant.condition_alert_rule_store import ConditionAlertRuleEntry
    from rquant.condition_alert_runtime_projection import (
        ConditionAlertDeliveryAuthorityInput,
        ConditionDeliveryScope,
        ConditionRuleAuthoritySnapshot,
    )
    from rquant.delivery_contracts import DeliveryChannel
    from rquant.notification_worker import run_notification_batch
    from rquant.runtime_notification_providers import SuppressedNotificationProvider

    state, activation, authority, routed = condition_delivery_fixture(tmp_path)
    payload = authority.model_dump(mode="python")
    if fault == "disable":
        old = authority.rules.rows[0]
        rule = ConditionAlertRuleDefinition.model_validate(
            {**old.rule.model_dump(mode="python"), "enabled": False}
        )
        payload["rules"] = ConditionRuleAuthoritySnapshot.create(
            activated_at=AT,
            rows=(
                ConditionAlertRuleEntry(
                    owner_id="alice",
                    rule_id=rule.rule_id,
                    version=3,
                    deleted=False,
                    rule=rule,
                    updated_at=AT,
                ),
            ),
        )
        payload["scopes"] = (
            ConditionDeliveryScope(
                rule=OwnedConditionAlertRule(owner_id="alice", version=3, rule=rule, updated_at=AT),
                scope=authority.scopes[0].scope,
            ),
        )
    elif fault == "scope":
        payload["scopes"] = (
            authority.scopes[0].model_copy(
                update={
                    "scope": authority.scopes[0].scope.model_copy(
                        update={"scope_version": "8" * 64}
                    )
                }
            ),
        )
    elif fault == "recipient":
        # Owner policy changes need a new manifest; this case revokes the
        # current rule channel before the original provider call.
        old = authority.rules.rows[0]
        rule = ConditionAlertRuleDefinition.model_validate(
            {**old.rule.model_dump(mode="python"), "governance": {"channels": ["pushplus"]}}
        )
        payload["rules"] = ConditionRuleAuthoritySnapshot.create(
            activated_at=AT,
            rows=(
                ConditionAlertRuleEntry(
                    owner_id="alice",
                    rule_id=rule.rule_id,
                    version=3,
                    deleted=False,
                    rule=rule,
                    updated_at=AT,
                ),
            ),
        )
        payload["scopes"] = (
            ConditionDeliveryScope(
                rule=OwnedConditionAlertRule(owner_id="alice", version=3, rule=rule, updated_at=AT),
                scope=authority.scopes[0].scope,
            ),
        )
    else:
        payload["producer"] = None
    state.apply_condition_alert_delivery_authority(
        ConditionAlertDeliveryAuthorityInput.model_validate(payload),
        activation=activation,
        expected_revision=1,
        applied_at=AT,
    )
    run = run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: SuppressedNotificationProvider()},
        worker_id="worker",
        now=AT,
        clock=lambda: AT,
        lease_for=timedelta(seconds=10),
        limit=10,
        condition_activation=activation,
    )
    assert run.succeeded_count == 0
    row = state.outbox_records()[0]
    assert state.attempts(row.outbox_id) == ()
    if fault == "unknown_source":
        assert row.status.value == "pending" and row.attempt_count == 0
    else:
        assert row.status.value == "dead_letter" and row.attempt_count == 0


def test_condition_projection_rejects_future_attempt_and_keeps_unknown_lease_honest(
    tmp_path: Path,
) -> None:
    from rquant.condition_alert_runtime import ConditionProducerRuntimeSnapshot
    from rquant.condition_alert_runtime_projection import condition_runtime_projections
    from rquant.delivery_contracts import DeliveryChannel
    from rquant.notification_worker import run_notification_batch
    from rquant.runtime_notification_providers import SuppressedNotificationProvider

    state, activation, authority, routed = condition_delivery_fixture(tmp_path)
    run_notification_batch(
        state,
        {DeliveryChannel.PUSHDEER: SuppressedNotificationProvider()},
        worker_id="projection",
        now=AT,
        clock=lambda: AT + timedelta(seconds=1),
        lease_for=timedelta(seconds=10),
        limit=1,
        condition_activation=activation,
    )
    facts = ConditionProducerRuntimeSnapshot(
        source=routed.source,
        inspected_at=AT,
        evaluated_at=None,
        input_sha256=None,
        source_facts=None,
        rules=(),
    )
    with state._read_snapshot() as connection, pytest.raises(ValueError, match="future"):
        condition_runtime_projections(connection, producer=facts, observed_at=AT, history_limit=100)


def test_original_private_command_actual_minute_math_ledger_spool_and_provider_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace
    from datetime import date

    from rquant.alert_rule_contracts import ConditionAlertScopeEvidence, OwnedConditionAlertRule
    from rquant.condition_alert_runtime import evaluate_condition_alert_round
    from rquant.delivery_contracts import DeliveryChannel
    from rquant.notification_worker import run_notification_batch
    from rquant.page_control import (
        PageControlConsumer,
        PageControlOutbox,
        PageControlService,
        SaveAlertRule,
    )
    from rquant.price_alert_admission import PriceAlertAdmission
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_notification_providers import (
        NotificationTransportDisposition,
        NotificationTransportResult,
        RecipientNotificationCapabilities,
        RecipientScopedNotificationProvider,
    )
    from rquant.screen.intraday_reference import IntradayReferenceSnapshot
    from rquant.serving_contracts import ServingCurrentPointer
    from rquant.web.condition_alert_commands import ConditionRuleScopeResolution
    from tests.unit.test_condition_alert_runtime import runtime_world
    from tests.unit.test_serving_screen_intraday import _source_world
    from tests.unit.test_web_screen_intraday import _borrow

    # Synthetic universe has one security. Spool bytes and all minute calculations are real;
    # the existing fixture's installed-schema seam remains an explicit test double.
    reader, at = _source_world(tmp_path, monkeypatch)
    original_reference = reader.config.reference_snapshot_path.read_bytes()
    reference = IntradayReferenceSnapshot.model_validate_json(original_reference)
    reference = IntradayReferenceSnapshot.model_validate(
        {**reference.model_dump(mode="python"), "universe_codes": ("600000.SH",), "identity": None}
    )
    path = tmp_path / "one-security-test-reference.json"
    path.write_bytes(reference.model_dump_json().encode())
    path.chmod(0o600)
    reader.config = reader.config.model_copy(
        update={
            "reference_snapshot_path": path,
            "reference_snapshot_sha256": sha256(path.read_bytes()).hexdigest(),
        }
    )
    snapshot = reader(at)
    assert snapshot.source.missing_codes == ()
    assert (tmp_path / "reference.json").read_bytes() == original_reference
    borrowed = _borrow(snapshot)
    borrowed = replace(
        borrowed,
        pointer=ServingCurrentPointer(
            generation_id=borrowed.manifest.generation_id, manifest_sha256="4" * 64, published_at=at
        ),
    )
    definition = rule_definition()
    scope = ConditionAlertScopeEvidence(
        owner_id="alice",
        scope=definition.scope,
        scope_version="2" * 64,
        member_codes=("600000.SH",),
        member_digest=canonical_sha256(("600000.SH",)),
        available_at=at,
    )
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    outbox.activate_condition_alert_rules(at)
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        clock=lambda: at,
        condition_rule_scope=lambda owner, rule, now: ConditionRuleScopeResolution(
            evidence=scope, is_current=lambda: True, consumer_ready=True
        ),
    )
    admission = PriceAlertAdmission(PageControlService(outbox=outbox, consumer=consumer))
    request = SaveAlertRule(
        command_id="original-full-chain", requested_at=at, expected_version=None, rule=definition
    )
    receipt = admission.submit(request, authenticated_owner_id="alice")
    assert receipt.status.value == "succeeded" and receipt.result["version"] == 1
    assert admission.resume(request, authenticated_owner_id="alice") == receipt
    store, ledger, _, _, activation = runtime_world(tmp_path)
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=date(2026, 7, 30),
        coverage_end=date(2026, 7, 31),
        open_dates=(date(2026, 7, 30), date(2026, 7, 31)),
        generated_at=at - timedelta(days=1),
    )
    owned = OwnedConditionAlertRule(
        owner_id="alice", version=receipt.result["version"], rule=definition, updated_at=at
    )
    try:
        value = evaluate_condition_alert_round(
            activation=activation,
            borrowed=borrowed,
            rules=(owned,),
            scopes=(scope,),
            calendar=calendar,
            evaluated_at=at,
        )
        assert [(row.ts_code, row.truth) for row in value.records] == [("600000.SH", "true")]
        assert value.source.source_identity == snapshot.source.source_identity
        actual = store.commit_round(value)
        assert actual.unknown_count == 0 and len(actual.events) == 1
        record = store.events_after(0, inspected_at=at)[0]
        state, notifier, _, routed = condition_delivery_fixture(
            tmp_path, actual_round=(store.source_descriptor(), record, definition, scope)
        )
        sent = []

        class Transport:
            def send(self, **message):
                sent.append(message)
                return NotificationTransportResult(
                    disposition=NotificationTransportDisposition.ACCEPTED
                )

        provider = RecipientScopedNotificationProvider(
            channel=DeliveryChannel.PUSHDEER,
            endpoint="https://example.invalid/unit",
            capabilities=RecipientNotificationCapabilities(
                {DeliveryChannel.PUSHDEER: {"admin": "synthetic-offline-unit"}}
            ),
            transport=Transport(),
        )
        run = run_notification_batch(
            state,
            {DeliveryChannel.PUSHDEER: provider},
            worker_id="full-chain",
            now=at,
            clock=lambda: at,
            lease_for=timedelta(seconds=10),
            limit=10,
            condition_activation=notifier,
        )
        assert run.succeeded_count == 1 and len(sent) == 1
        delivered = state.outbox_records()[0]
        assert state.attempts(delivered.outbox_id)[0].provider_receipt is not None
        assert (
            state.condition_alert_send_admission(delivered.outbox_id, 1).event_id
            == actual.events[0].event.event_id
            == routed.event_id
        )
    finally:
        borrowed.cursor.close()
        ledger.close()


@pytest.mark.parametrize("point", ["before_admission_commit", "after_admission_commit"])
def test_condition_send_admission_commit_loss_cannot_mint_another_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    state, activation, _, _ = condition_delivery_fixture(tmp_path)
    leased = state.claim_due_with_condition_activation(
        "fault-worker", activation=activation, now=AT, lease_for=timedelta(seconds=10), limit=1
    )[0]

    def fault(stage: str) -> None:
        if stage == point:
            raise OSError("synthetic acknowledgement loss")

    monkeypatch.setattr(state, "_condition_alert_failpoint", fault)
    with pytest.raises(OSError):
        state.admit_condition_alert_delivery(
            leased,
            activation=activation,
            worker_id="fault-worker",
            expected_revision=1,
            admitted_at=AT,
        )
    stored = state.condition_alert_send_admission(leased.outbox_id, leased.attempt_count)
    assert (stored is None) == (point == "before_admission_commit")
    monkeypatch.setattr(state, "_condition_alert_failpoint", lambda stage: None)
    retried = state.admit_condition_alert_delivery(
        leased, activation=activation, worker_id="fault-worker", expected_revision=1, admitted_at=AT
    )
    assert (retried is None) == (point == "after_admission_commit")
    assert state.attempts(leased.outbox_id) == ()


def test_condition_original_admission_right_is_single_use_and_persisted_receipt_cannot_send(
    tmp_path: Path,
) -> None:
    from rquant.condition_alert_runtime_projection import consume_condition_alert_admitted_delivery

    state, activation, _, _ = condition_delivery_fixture(tmp_path)
    leased = state.claim_due_with_condition_activation(
        "single-use", activation=activation, now=AT, lease_for=timedelta(seconds=10), limit=1
    )[0]
    right = state.admit_condition_alert_delivery(
        leased, activation=activation, worker_id="single-use", expected_revision=1, admitted_at=AT
    )
    receipt = consume_condition_alert_admitted_delivery(right, store=state, record=leased, now=AT)
    with pytest.raises(TypeError):
        consume_condition_alert_admitted_delivery(right, store=state, record=leased, now=AT)
    with pytest.raises(TypeError):
        consume_condition_alert_admitted_delivery(receipt, store=state, record=leased, now=AT)
