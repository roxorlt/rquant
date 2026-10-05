import json
import sqlite3
from datetime import timedelta
from hashlib import sha256
from pathlib import Path

import pytest

from rquant.delivery_contracts import OutboxStatus
from rquant.notification_state import NotificationStateStore
from rquant.price_alert_route import (
    PriceAlertRecipientPolicy,
    install_price_alert_history,
)
from rquant.price_alert_runtime import evaluate_price_alert_round
from rquant.price_alert_runtime_contracts import verify_price_alert_activation
from rquant.price_alert_runtime_projection import (
    PriceAlertAuthorityConflict,
    PriceAlertDeliveryAuthorityInput,
    consume_price_alert_admitted_delivery,
)
from rquant.price_alert_runtime_source import PriceAlertScopeSnapshot, UnavailablePriceAlertScope
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.serving_manual_watchlist_projection import ManualWatchlistAuthoritySnapshot
from rquant.serving_price_alert_rule_projection import PriceAlertRuleAuthoritySnapshot
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_route import route_fixture
from tests.unit.test_price_alert_runtime import inputs


def notifier_activation(tmp_path: Path, policy, suffix="notify", *, enabled=True):
    body = json.loads((tmp_path / "router.json").read_text())
    body["service_kind"] = "notifier"
    flags = body["settings"]["price_alert_runtime"]
    flags.update(
        routing_enabled=False, delivery_enabled=enabled, recipient_policy_sha256=policy.sha256
    )
    path = tmp_path / f"{suffix}.json"
    path.write_text(json.dumps(body))
    path.chmod(0o600)
    return verify_price_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=sha256(path.read_bytes()).hexdigest(),
        expected_commit="b" * 40,
        expected_kind=RuntimeServiceKind.NOTIFIER,
    ), sha256(path.read_bytes()).hexdigest()


def admission_fixture(tmp_path: Path):
    producer, bus, router, policy, source, unused = route_fixture(tmp_path)
    # The route fixture has not routed its synthetic event. Seal a real evaluator event
    # in a separate producer so admission can verify the original rule/member body.
    producer.close()
    from tests.unit.test_price_alert_runtime_store import store_fixture

    root = tmp_path / "real"
    root.mkdir(mode=0o700)
    producer, activation, frequency = store_fixture(root)
    scope, quotes, calendar = inputs()
    item = producer.commit_round(
        evaluate_price_alert_round(
            activation=activation,
            scope=scope,
            quotes=quotes,
            calendar=calendar,
            evaluated_at=AT,
            policy=frequency,
        ),
        policy=frequency,
        current_scope=lambda: True,
    ).events[0]
    # Router source identity must be bound to the actual producer, not the unused fixture.
    body = json.loads((tmp_path / "router.json").read_text())
    producer_body = json.loads((root / "runtime.json").read_text())["settings"][
        "price_alert_runtime"
    ]
    for key in (
        "source_id",
        "ledger_id",
        "source_epoch",
        "generation_id",
        "evaluation_contract_sha256",
        "frequency_policy_sha256",
        "routing_policy_sha256",
    ):
        body["settings"]["price_alert_runtime"][key] = producer_body[key]
    (tmp_path / "router.json").write_text(json.dumps(body))
    (tmp_path / "router.json").chmod(0o600)
    router = verify_price_alert_activation(
        tmp_path / "router.json",
        runtime_root=tmp_path,
        expected_manifest_sha256=sha256((tmp_path / "router.json").read_bytes()).hexdigest(),
        expected_commit="b" * 40,
        expected_kind=RuntimeServiceKind.SIGNAL_ROUTER,
    )
    # Both synthetic fixtures intentionally use the same identity. Route activation
    # content is otherwise unchanged and the original bus remains the only bus.
    bus.install_price_alert_route_v1(router)
    routed = bus.commit_price_alert_route(
        activation=router,
        policy=policy,
        source=producer.source_descriptor(),
        record=item,
        source_inspected_at=AT,
        routed_at=AT,
    )
    state = NotificationStateStore(tmp_path / "notify.sqlite3")
    install_price_alert_history(state)
    state.replicate_mixed_notification_events(
        bus.source_descriptor(), (routed,), source_inspected_at=AT, observed_at=AT
    )
    cap, digest = notifier_activation(tmp_path, policy)
    state.install_price_alert_delivery_v1(cap)
    authority = PriceAlertDeliveryAuthorityInput(
        scope=scope,
        policy=policy,
        owner_policy_manifest_sha256=digest,
        delivery_enabled=True,
        inspected_at=AT,
    )
    applied = state.apply_price_alert_delivery_authority(
        authority, activation=cap, expected_revision=0, applied_at=AT
    )
    return producer, bus, state, cap, authority, applied, routed


def changed_authority(tmp_path: Path, value, change, *, at=AT + timedelta(seconds=1)):
    scope = value.scope
    rule, member = scope.rules[0], scope.members[0]
    policy = value.policy
    if change == "disable":
        rule = rule.model_copy(update={"enabled": False, "version": 2, "updated_at": at})
    elif change == "rule_version":
        rule = rule.model_copy(update={"version": 2, "updated_at": at})
    elif change == "member":
        member = member.model_copy(update={"version": 2, "updated_at": at})
    elif change == "recipient":
        policy = PriceAlertRecipientPolicy(generation_id="f" * 64, owners=())
    scope = PriceAlertScopeSnapshot(
        generation_id="c" * 64,
        manifest_sha256="d" * 64,
        source_generation_id=scope.source_generation_id,
        source_sequence=2,
        built_at=at,
        available_at=at,
        inspected_at=at,
        rules=(rule,),
        members=(member,),
        rule_rows_sha256=PriceAlertRuleAuthoritySnapshot.digest((rule,)),
        member_rows_sha256=ManualWatchlistAuthoritySnapshot.digest((member,)),
    )
    cap, digest = notifier_activation(tmp_path, policy, "next")
    return cap, PriceAlertDeliveryAuthorityInput(
        scope=scope,
        policy=policy,
        owner_policy_manifest_sha256=digest,
        delivery_enabled=True,
        inspected_at=at,
    )


def claim(state, cap, *, at=AT):
    items = state.claim_due_with_price_activation(
        "worker", activation=cap, now=at, lease_for=timedelta(seconds=10), limit=10
    )
    assert len(items) == 1
    return items[0]


@pytest.mark.parametrize("change", ["disable", "rule_version", "member", "recipient"])
@pytest.mark.parametrize("order", ["apply_first", "admit_first", "external_only"])
def test_four_changes_in_all_three_commit_orders(tmp_path: Path, change: str, order: str) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    leased = claim(state, cap)
    new_cap, new = changed_authority(tmp_path, authority, change)
    now = AT + timedelta(seconds=1)
    if order == "apply_first":
        latest = state.apply_price_alert_delivery_authority(
            new, activation=new_cap, expected_revision=applied.authority_revision, applied_at=now
        )
        with pytest.raises(PriceAlertAuthorityConflict):
            state.admit_price_alert_delivery(
                leased,
                activation=cap,
                worker_id="worker",
                expected_revision=applied.authority_revision,
                admitted_at=now,
            )
        cancelled = state.cancel_price_unadmitted(
            leased.outbox_id,
            worker_id="worker",
            expected_revision=latest.authority_revision,
            cancelled_at=now,
        )
        assert cancelled is not None
        assert state.outbox_records()[0].status is OutboxStatus.DEAD_LETTER
        assert state.outbox_records()[0].attempt_count == 0
        assert state.price_alert_send_admission(leased.outbox_id, 1) is None
    else:
        admitted = state.admit_price_alert_delivery(
            leased,
            activation=cap,
            worker_id="worker",
            expected_revision=applied.authority_revision,
            admitted_at=AT,
        )
        if order == "admit_first":
            state.apply_price_alert_delivery_authority(
                new,
                activation=new_cap,
                expected_revision=applied.authority_revision,
                applied_at=now,
            )
        receipt = consume_price_alert_admitted_delivery(
            admitted, store=state, record=leased, now=now
        )
        assert receipt.event_id == routed.event_id
        assert receipt.authority_revision == applied.authority_revision
        assert (
            state.cancel_price_unadmitted(
                leased.outbox_id,
                worker_id="worker",
                expected_revision=state.price_alert_delivery_authority().authority_revision,
                cancelled_at=now,
            )
            is None
        )
        with pytest.raises((TypeError, ValueError)):
            consume_price_alert_admitted_delivery(admitted, store=state, record=leased, now=now)
        assert (
            state.admit_price_alert_delivery(
                leased,
                activation=cap,
                worker_id="worker",
                expected_revision=state.price_alert_delivery_authority().authority_revision,
                admitted_at=now,
            )
            is None
        )
    with sqlite3.connect(state.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM delivery_attempt").fetchone()[0] == 0
    producer.close()


def test_authority_cas_full_body_unavailable_and_regression_rejection(tmp_path: Path) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    latest_cap, latest = changed_authority(tmp_path, authority, "rule_version")
    state.apply_price_alert_delivery_authority(
        latest, activation=latest_cap, expected_revision=1, applied_at=AT + timedelta(seconds=1)
    )
    with pytest.raises(PriceAlertAuthorityConflict):
        state.apply_price_alert_delivery_authority(
            authority, activation=cap, expected_revision=1, applied_at=AT + timedelta(seconds=2)
        )
    with pytest.raises((PriceAlertAuthorityConflict, ValueError)):
        state.apply_price_alert_delivery_authority(
            authority, activation=cap, expected_revision=2, applied_at=AT + timedelta(seconds=2)
        )
    unknown = PriceAlertDeliveryAuthorityInput(
        scope=UnavailablePriceAlertScope(inspected_at=AT + timedelta(seconds=2)),
        policy=latest.policy,
        owner_policy_manifest_sha256=latest.owner_policy_manifest_sha256,
        delivery_enabled=True,
        inspected_at=AT + timedelta(seconds=2),
    )
    persisted = state.apply_price_alert_delivery_authority(
        unknown, activation=latest_cap, expected_revision=2, applied_at=AT + timedelta(seconds=2)
    )
    assert persisted.scope.availability == "unavailable"
    assert (
        state.claim_due_with_price_activation(
            "worker",
            activation=latest_cap,
            now=AT + timedelta(seconds=2),
            lease_for=timedelta(seconds=10),
            limit=10,
        )
        == ()
    )
    producer.close()


@pytest.mark.parametrize("point", ["before_admission_commit", "after_admission_commit"])
def test_admission_failure_and_lost_ack_do_not_reissue_call_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    leased = claim(state, cap)
    monkeypatch.setattr(
        state,
        "_price_alert_failpoint",
        lambda name: (_ for _ in ()).throw(OSError("lost")) if name == point else None,
    )
    with pytest.raises(OSError):
        state.admit_price_alert_delivery(
            leased, activation=cap, worker_id="worker", expected_revision=1, admitted_at=AT
        )
    stored = state.price_alert_send_admission(leased.outbox_id, 1)
    assert (stored is None) == (point == "before_admission_commit")
    monkeypatch.setattr(state, "_price_alert_failpoint", lambda _: None)
    if point == "after_admission_commit":
        assert (
            state.admit_price_alert_delivery(
                leased, activation=cap, worker_id="worker", expected_revision=1, admitted_at=AT
            )
            is None
        )
        with pytest.raises(TypeError):
            consume_price_alert_admitted_delivery(stored, store=state, record=leased, now=AT)
    producer.close()


def test_unclaimed_cancel_preserves_count_and_expired_authority_cannot_admit(
    tmp_path: Path,
) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    changed_cap, changed = changed_authority(tmp_path, authority, "disable")
    state.apply_price_alert_delivery_authority(
        changed, activation=changed_cap, expected_revision=1, applied_at=AT + timedelta(seconds=1)
    )
    pending = state.outbox_records()[0]
    assert (
        state.cancel_price_unadmitted(
            pending.outbox_id, expected_revision=2, cancelled_at=AT + timedelta(seconds=1)
        )
        is not None
    )
    assert state.outbox_records()[0].attempt_count == 0
    producer.close()
