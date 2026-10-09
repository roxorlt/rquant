from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from rquant import delivery_contracts as contracts
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget, OutboxStatus
from rquant.notification_state import NotificationStateStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.notification_worker import run_notification_batch
from rquant.runtime_notification_providers import (
    ExistingClientNotificationTransport, RecipientNotificationCapabilities,
    RecipientScopedNotificationProvider,
    SuppressedNotificationProvider,
    prepare_merged_notification,
    deliver_merged_notification,
)

NOW = datetime(2026, 7, 31, 2, 0, tzinfo=UTC)


def _store(path: Path, *, enabled: bool = True, owner: str = "admin", mode: str = "live") -> NotificationStateStore:
    assert hasattr(contracts, "NotificationMergeBinding"), "same-ledger merge binding missing"
    binding: Any = contracts.NotificationMergeBinding(
        owner_id=owner, source_id="signal-route-spool/v1",
        installation_sha256="a" * 64, role_revision="b" * 64,
        generation_id="generation-1", mode=mode,
    ) if enabled else None
    return NotificationStateStore(path, merge_binding=binding)


def _route(
    store: NotificationStateStore, stock: str,
    *, at: datetime = NOW, expires: datetime | None = None, recipient: str = "admin",
) -> str:
    signal = SignalEnvelope(
        schema_version=1, strategy_id="original", strategy_version="1.0.0",
        parameter_fingerprint="a" * 64, dataset_snapshot_id="b" * 64,
        feature_snapshot_id="c" * 64, event_time=at, available_at=at,
        candidate_id=stock, action=SignalAction.WATCH, reason_codes=("original",),
        evidence={"ratio": 1.8}, expires_at=expires or at + timedelta(minutes=5),
        producer_commit="d" * 40,
    )
    store.ingest(signal, received_at=at)
    result = store.route(signal.signal_id, (DeliveryTarget(
        recipient_id=recipient, channel=DeliveryChannel.PUSHDEER,
    ),), now=at)
    return result[0].outbox_id


def test_first_observation_starts_fixed_window_without_claim_or_lease(tmp_path: Path) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    first = _route(store, "600001.SH")
    assert store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100) == ()
    record = store.outbox_record(first)
    assert record is not None and record.status is OutboxStatus.PENDING
    assert record.attempt_count == 0 and record.lease_until is None
    _route(store, "600002.SH", at=NOW + timedelta(seconds=20))
    assert store.claim_due(worker_id="worker", now=NOW + timedelta(seconds=29), lease_for=timedelta(seconds=30), limit=100) == ()
    due = store.claim_due(worker_id="worker", now=NOW + timedelta(seconds=30), lease_for=timedelta(seconds=30), limit=100)
    assert len(due) == 2
    assert all(item.attempt_count == 1 for item in due)
    groups = store.merge_groups()
    assert len(groups) == 1
    assert groups[0].opened_at == NOW
    assert groups[0].due_at == NOW + timedelta(seconds=30)
    assert len(groups[0].members) == 2


def test_current_mode_changes_before_intent_leave_no_physical_call_permission(tmp_path: Path) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    _route(store, "600001.SH")
    store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    due = NOW + timedelta(seconds=30)
    records = store.claim_due(worker_id="worker", now=due, lease_for=timedelta(seconds=30), limit=100)
    group = store.merge_groups()[0]
    store.merge_binding_guard = lambda: False
    with pytest.raises(ValueError, match="installed|mode|changed"):
        store.commit_merge_intent(group, records, worker_id="worker", now=due,
            request_sha256="a" * 64, request_utf8_bytes=10)
    with store._read_snapshot() as connection:
        assert connection.execute("SELECT COUNT(*) FROM notification_physical_attempt").fetchone()[0] == 0
    assert store.merge_groups()[0].status == "waiting"


def test_exact_expiry_precedes_merge_and_does_not_extend_ttl(tmp_path: Path) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    item = _route(store, "600001.SH", expires=NOW + timedelta(seconds=30))
    store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    assert store.claim_due(worker_id="worker", now=NOW + timedelta(seconds=30), lease_for=timedelta(seconds=30), limit=100) == ()
    record = store.outbox_record(item)
    assert record is not None and record.status is OutboxStatus.EXPIRED
    assert record.expires_at == NOW + timedelta(seconds=30)
    assert record.attempt_count == 0


def test_receiver_and_complete_source_binding_split_groups(tmp_path: Path) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    _route(store, "600001.SH")
    _route(store, "600002.SH", recipient="other")
    store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    assert len(store.merge_groups()) == 2
    assert {item.target.recipient_id for item in store.merge_groups()} == {"admin", "other"}


def test_default_off_keeps_original_immediate_claim(tmp_path: Path) -> None:
    store = _store(tmp_path / "notifications.sqlite", enabled=False)
    _route(store, "600001.SH")
    due = store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    assert len(due) == 1 and due[0].attempt_count == 1
    assert store.merge_groups() == ()


def _provider() -> RecipientScopedNotificationProvider:
    return RecipientScopedNotificationProvider(
        channel=DeliveryChannel.PUSHDEER, endpoint="https://offline.example/send",
        capabilities=RecipientNotificationCapabilities({DeliveryChannel.PUSHDEER: {"admin": "private"}}),
        transport=ExistingClientNotificationTransport(),
    )


@pytest.mark.parametrize("reply", ["accepted", "rejected", "unknown"])
def test_group_one_original_post_and_atomic_member_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reply: str,
) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    ids = (_route(store, "600001.SH"), _route(store, "600002.SH"))
    calls: list[str] = []

    class Reply:
        def json(self) -> dict[str, object]:
            return {"code": 0 if reply == "accepted" else 1, "error": "rejected"}

    def post(endpoint: str, **kwargs: Any) -> Reply:
        calls.append(endpoint)
        if reply == "unknown":
            raise TimeoutError("lost reply")
        return Reply()

    monkeypatch.setattr("rquant.notify.client.requests.post", post)
    providers = {DeliveryChannel.PUSHDEER: _provider()}
    run_notification_batch(store, providers, worker_id="worker", now=NOW,
                           lease_for=timedelta(seconds=30), limit=100, clock=lambda: NOW)
    assert calls == []
    due = NOW + timedelta(seconds=30)
    result = run_notification_batch(store, providers, worker_id="worker", now=due,
                                    lease_for=timedelta(seconds=30), limit=100, clock=lambda: due)
    assert len(calls) == 1, "a merge group must use one original receiver-scoped POST"
    assert result.claimed_count == 2
    assert result.succeeded_count == (2 if reply == "accepted" else 0)
    assert result.failed_count == (2 if reply == "rejected" else 0)
    assert result.unknown_count == (2 if reply == "unknown" else 0)
    assert hasattr(store, "merge_channel_stats"), "same-read physical statistics missing"
    stats = store.merge_channel_stats()[0]
    assert stats.logical_count == 2
    assert stats.physical_requests == 1
    assert stats.accepted_count == (1 if reply == "accepted" else 0)
    assert stats.rejected_count == (1 if reply == "rejected" else 0)
    assert stats.unknown_count == (1 if reply == "unknown" else 0)
    assert stats.possible_requests == 0
    if reply == "unknown":
        store.recover_expired_leases(now=due + timedelta(seconds=30))
        run_notification_batch(store, providers, worker_id="worker", now=due + timedelta(seconds=31),
                               lease_for=timedelta(seconds=30), limit=100,
                               clock=lambda: due + timedelta(seconds=31))
        assert len(calls) == 1
        assert all(store.outbox_record(item).status is OutboxStatus.DEAD_LETTER for item in ids)
    if reply == "rejected":
        # Existing retry starts at five seconds, with no new merge wait.
        run_notification_batch(store, providers, worker_id="worker", now=due + timedelta(seconds=5),
                               lease_for=timedelta(seconds=30), limit=100,
                               clock=lambda: due + timedelta(seconds=5))
        assert len(calls) == 2


def test_intent_without_call_receipt_survives_restart_as_possible_not_retry(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    _route(store, "600001.SH")
    assert store.merge_channel_stats() == ()
    store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    due = NOW + timedelta(seconds=30)
    records = store.claim_due(worker_id="worker", now=due, lease_for=timedelta(seconds=30), limit=100)
    assert hasattr(store, "commit_merge_intent"), "durable pre-call group intent missing"
    group = store.merge_groups()[0]
    store.commit_merge_intent(group, records, worker_id="worker", now=due,
                              request_sha256="e" * 64, request_utf8_bytes=1)
    restored = _store(store.path)
    restored.recover_expired_leases(now=due + timedelta(seconds=30))
    assert restored.claim_due(worker_id="worker-2", now=due + timedelta(seconds=31),
                              lease_for=timedelta(seconds=30), limit=100) == ()
    stats = restored.merge_channel_stats()[0]
    assert stats.physical_requests == 0 and stats.possible_requests == 1


def test_observation_write_failure_is_possible_and_never_resubmitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    _route(store, "600001.SH")
    calls: list[object] = []

    class Reply:
        def json(self) -> dict[str, int]:
            return {"code": 0}

    def post(*args: Any, **kwargs: Any) -> Reply:
        calls.append(args)
        return Reply()

    def fail(_: Any) -> None:
        raise OSError("observation did not commit")

    monkeypatch.setattr("rquant.notify.client.requests.post", post)
    monkeypatch.setattr(store, "record_physical_post", fail)
    providers = {DeliveryChannel.PUSHDEER: _provider()}
    run_notification_batch(store, providers, worker_id="worker", now=NOW,
                           lease_for=timedelta(seconds=30), limit=100, clock=lambda: NOW)
    due = NOW + timedelta(seconds=30)
    result = run_notification_batch(store, providers, worker_id="worker", now=due,
                                   lease_for=timedelta(seconds=30), limit=100, clock=lambda: due)
    assert result.unknown_count == 1 and len(calls) == 1
    stats = store.merge_channel_stats()[0]
    assert stats.physical_requests == 0 and stats.possible_requests == 1
    assert stats.accepted_count == 0
    store.recover_expired_leases(now=due + timedelta(seconds=30))
    assert store.claim_due(worker_id="new", now=due + timedelta(seconds=31),
                           lease_for=timedelta(seconds=30), limit=100) == ()


def test_group_writeback_rolls_back_all_original_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    _route(store, "600001.SH")
    _route(store, "600002.SH")

    class Reply:
        def json(self) -> dict[str, int]:
            return {"code": 0}

    monkeypatch.setattr("rquant.notify.client.requests.post", lambda *a, **k: Reply())
    fired = False

    def before_commit(connection: Any) -> None:
        nonlocal fired
        if not fired and connection.execute("SELECT COUNT(*) FROM delivery_attempt").fetchone()[0] == 2:
            fired = True
            raise OSError("failure after second member before group commit")

    monkeypatch.setattr(store, "_before_commit", before_commit)
    providers = {DeliveryChannel.PUSHDEER: _provider()}
    run_notification_batch(store, providers, worker_id="worker", now=NOW,
                           lease_for=timedelta(seconds=30), limit=100, clock=lambda: NOW)
    due = NOW + timedelta(seconds=30)
    result = run_notification_batch(store, providers, worker_id="worker", now=due,
                                   lease_for=timedelta(seconds=30), limit=100, clock=lambda: due)
    assert fired and result.unknown_count == 2
    with store._read_snapshot() as connection:
        assert connection.execute("SELECT COUNT(*) FROM delivery_attempt").fetchone()[0] == 0
    assert store.merge_channel_stats()[0].accepted_count == 1
    assert all(row.status is OutboxStatus.LEASED for row in store.outbox_records())


def test_shadow_runs_same_member_path_with_zero_physical_requests(tmp_path: Path) -> None:
    store = _store(tmp_path / "notifications.sqlite", mode="shadow")
    _route(store, "600001.SH")
    _route(store, "600002.SH")
    providers = {DeliveryChannel.PUSHDEER: SuppressedNotificationProvider()}
    run_notification_batch(store, providers, worker_id="worker", now=NOW,
                           lease_for=timedelta(seconds=30), limit=100, clock=lambda: NOW)
    due = NOW + timedelta(seconds=30)
    result = run_notification_batch(store, providers, worker_id="worker", now=due,
                                   lease_for=timedelta(seconds=30), limit=100, clock=lambda: due)
    assert result.succeeded_count == 2
    assert store.merge_groups()[0].status == "shadow"
    stats = store.merge_channel_stats()[0]
    assert stats.member_attempts == 2 and stats.physical_requests == stats.possible_requests == 0


@pytest.mark.parametrize("changed", [False, True])
def test_original_price_admission_current_revision_and_single_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: bool,
) -> None:
    from rquant.price_alert_runtime_contracts import require_verified_price_alert_activation
    from tests.unit.test_price_alert_notification_admission import admission_fixture, changed_authority
    from tests.unit.test_price_alert_event_contracts import AT

    producer, _bus, original, cap, authority, _applied, _routed = admission_fixture(tmp_path)
    actual = require_verified_price_alert_activation(cap, "notifier")
    binding = contracts.NotificationMergeBinding(
        owner_id="alice", source_id=original.replication_source_id,
        installation_sha256=actual.producer_manifest_sha256, role_revision=actual.recipient_policy_sha256,
        generation_id=actual.generation_id, mode="live",
    )
    store = NotificationStateStore(original.path, merge_binding=binding)
    provider = RecipientScopedNotificationProvider(
        channel=DeliveryChannel.PUSHDEER, endpoint="https://offline.example/send",
        capabilities=RecipientNotificationCapabilities({DeliveryChannel.PUSHDEER: {"alice.phone": "private"}}),
        transport=ExistingClientNotificationTransport(),
    )
    calls: list[object] = []

    class Reply:
        def json(self) -> dict[str, int]:
            return {"code": 0}

    def post(*args: Any, **kwargs: Any) -> Reply:
        calls.append(args)
        return Reply()

    monkeypatch.setattr("rquant.notify.client.requests.post", post)
    store.claim_due_with_price_activation("worker", activation=cap, now=AT,
                                          lease_for=timedelta(seconds=30), limit=100)
    due = AT + timedelta(seconds=30)
    rows = store.claim_due_with_price_activation("worker", activation=cap, now=due,
                                                 lease_for=timedelta(seconds=30), limit=100)
    assert len(rows) == 1
    assert store.merge_channel_stats()[0].member_attempts == 0
    prepared, _ = prepare_merged_notification(
        provider, store=store, group=store.merge_groups()[0], records=rows, worker_id="worker", now=due,
        price_activation=cap, condition_activation=None,
    )
    if changed:
        new_cap, new = changed_authority(tmp_path, authority, "disable", at=due)
        store.apply_price_alert_delivery_authority(new, activation=new_cap, expected_revision=1, applied_at=due)
        with pytest.raises(ValueError, match="price source"):
            deliver_merged_notification(provider, prepared, store=store, now=due, clock=lambda: due)
        assert calls == []
    else:
        deliver_merged_notification(provider, prepared, store=store, now=due, clock=lambda: due)
        assert len(calls) == 1
        with pytest.raises(TypeError, match="single-use"):
            deliver_merged_notification(provider, prepared, store=store, now=due, clock=lambda: due)
        assert len(calls) == 1
    producer.close()


def test_group_count_bound_does_not_claim_partial_group(tmp_path: Path) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    for index in range(101):
        _route(store, f"{600000 + index}.SH")
    store.claim_due(worker_id="worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    assert sorted(len(g.members) for g in store.merge_groups()) == [1, 100]
    due = NOW + timedelta(seconds=30)
    assert len(store.claim_due(worker_id="worker", now=due, lease_for=timedelta(seconds=30), limit=100)) == 100
    assert len(store.claim_due(worker_id="other", now=due, lease_for=timedelta(seconds=30), limit=100)) == 1


def test_merge_typed_budget_rejects_bool_as_count_and_unbounded_receiver() -> None:
    assert hasattr(contracts, "PhysicalPostMember")
    with pytest.raises(ValueError):
        contracts.PhysicalPostMember(outbox_id="a" * 64, attempt_no=True)
    with pytest.raises(ValueError):
        contracts.PhysicalPostBinding(
            group_id="a" * 64, owner_id="admin",
            target=DeliveryTarget(recipient_id="x" * 10000, channel=DeliveryChannel.PUSHDEER),
            members=({"outbox_id": "b" * 64, "attempt_no": 1},),
            request_sha256="c" * 64, request_utf8_bytes=1, issued_at=NOW,
        )


def test_builtin_merge_uses_current_owner_commit_and_one_post_per_exact_cohort(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from hashlib import sha256
    from rquant.condition_alert_runtime_projection import ConditionAlertDeliveryAuthorityInput
    from rquant.monitor_builtin_runtime import commit_original_builtin_round, inspect_original_builtin_delivery, require_builtin_delivery_inspection
    from tests.unit.test_monitor_builtin_runtime import AT, builtin_delivery_world, publish_stock_capture

    owner, ledger, capture, definitions, read, receipt, original, notifier, policy = builtin_delivery_world(tmp_path)
    binding = contracts.NotificationMergeBinding(owner_id="alice", source_id=original.replication_source_id,
        installation_sha256=sha256((tmp_path / "builtin-notifier.json").read_bytes()).hexdigest(),
        role_revision=policy.sha256, generation_id=owner.source_descriptor().generation_id, mode="live")
    store = NotificationStateStore(original.path, merge_binding=binding)
    calls = []
    class Reply:
        def json(self) -> dict[str, int]:
            return {"code": 0}
    monkeypatch.setattr("rquant.notify.client.requests.post", lambda *a, **k: calls.append(a) or Reply())
    def inspect(read, now, revision):
        witness = inspect_original_builtin_delivery(owner, captured=(read,), inspected_at=now)
        store.apply_condition_alert_delivery_authority(ConditionAlertDeliveryAuthorityInput(
            rules=None, scopes=(), policy=policy, producer=None, notifier_manifest_sha256=binding.installation_sha256,
            delivery_enabled=True, inspected_at=now, builtin=require_builtin_delivery_inspection(witness)),
            activation=notifier, expected_revision=revision, applied_at=now, builtin_inspection=witness)
    inspect(read, AT, 0)
    providers = {DeliveryChannel.PUSHDEER: _provider()}
    first = run_notification_batch(store, providers, worker_id="worker", now=AT,
        lease_for=timedelta(seconds=10), limit=100, clock=lambda: AT, condition_activation=notifier)
    assert first.claimed_count == 0 and calls == []
    due = AT + timedelta(seconds=30)
    read, _ = publish_stock_capture(capture, at=due, sequence=2)
    commit_original_builtin_round(owner, captured=read, definitions=definitions, evaluated_at=due)
    inspect(read, due, 1)
    result = run_notification_batch(store, providers, worker_id="worker", now=due,
        lease_for=timedelta(seconds=10), limit=100, clock=lambda: due, condition_activation=notifier)
    assert result.succeeded_count == len(receipt.events) and len(calls) == 2
    assert store.merge_channel_stats()[0].physical_requests == 2
    assert sorted(len(group.members) for group in store.merge_groups()) == [2, 5]
    assert all(item.status is OutboxStatus.SUCCEEDED for item in store.outbox_records())
    ledger.close()


def test_builtin_merge_keeps_actual_multi_owner_even_with_same_receiver(tmp_path: Path) -> None:
    from hashlib import sha256
    from rquant.condition_alert_runtime_projection import ConditionAlertDeliveryAuthorityInput
    from rquant.monitor_builtin_runtime import commit_original_builtin_round, inspect_original_builtin_delivery, require_builtin_delivery_inspection
    from tests.unit.test_monitor_builtin_runtime import AT, builtin_delivery_world, publish_stock_capture

    owner, ledger, capture, definitions, read, receipt, original, notifier, policy = builtin_delivery_world(tmp_path, owners=("alice", "bob"))
    binding = contracts.NotificationMergeBinding(owner_id="alice", source_id=original.replication_source_id,
        installation_sha256=sha256((tmp_path / "builtin-notifier.json").read_bytes()).hexdigest(),
        role_revision=policy.sha256, generation_id=owner.source_descriptor().generation_id, mode="shadow")
    store = NotificationStateStore(original.path, merge_binding=binding)
    def inspect(read, now, revision):
        witness = inspect_original_builtin_delivery(owner, captured=(read,), inspected_at=now)
        store.apply_condition_alert_delivery_authority(ConditionAlertDeliveryAuthorityInput(
            rules=None, scopes=(), policy=policy, producer=None, notifier_manifest_sha256=binding.installation_sha256,
            delivery_enabled=True, inspected_at=now, builtin=require_builtin_delivery_inspection(witness)),
            activation=notifier, expected_revision=revision, applied_at=now, builtin_inspection=witness)
    inspect(read, AT, 0)
    providers = {DeliveryChannel.PUSHDEER: SuppressedNotificationProvider()}
    run_notification_batch(store, providers, worker_id="worker", now=AT,
        lease_for=timedelta(seconds=10), limit=100, clock=lambda: AT, condition_activation=notifier)
    assert {group.binding.owner_id for group in store.merge_groups()} == {"alice", "bob"}
    due = AT + timedelta(seconds=30)
    read, _ = publish_stock_capture(capture, at=due, sequence=2)
    commit_original_builtin_round(owner, captured=read, definitions=definitions, evaluated_at=due)
    inspect(read, due, 1)
    result = run_notification_batch(store, providers, worker_id="worker", now=due,
        lease_for=timedelta(seconds=10), limit=100, clock=lambda: due, condition_activation=notifier)
    assert result.succeeded_count == len(receipt.events) == 14
    stats = store.merge_channel_stats()
    assert {row.owner_id for row in stats} == {"alice", "bob"}
    assert all(row.logical_count == 7 and row.physical_requests == 0 for row in stats)
    ledger.close()


def test_merge_gc_removes_only_closed_confirmed_metadata_after_seven_days(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    outbox = _route(store, "600001.SH")
    class Reply:
        def json(self) -> dict[str, int]:
            return {"code": 0}
    monkeypatch.setattr("rquant.notify.client.requests.post", lambda *a, **k: Reply())
    providers = {DeliveryChannel.PUSHDEER: _provider()}
    run_notification_batch(store, providers, worker_id="worker", now=NOW,
        lease_for=timedelta(seconds=30), limit=100, clock=lambda: NOW)
    sent = NOW + timedelta(seconds=30)
    run_notification_batch(store, providers, worker_id="worker", now=sent,
        lease_for=timedelta(seconds=30), limit=100, clock=lambda: sent)
    assert store.prune_merge_metadata(now=sent + timedelta(days=7)) == 0
    assert store.prune_merge_metadata(now=sent + timedelta(days=7, microseconds=1)) == 1
    assert store.merge_groups() == ()
    assert store.outbox_record(outbox).status is OutboxStatus.SUCCEEDED
    assert len(store.attempts(outbox)) == 1
    with store._read_snapshot() as connection:
        assert connection.execute("SELECT COUNT(*) FROM notification_physical_attempt").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM signal_envelope").fetchone()[0] == 1


def test_merge_gc_keeps_unresolved_physical_intent_and_original_unknown(tmp_path: Path) -> None:
    store = _store(tmp_path / "notifications.sqlite")
    _route(store, "600001.SH")
    store.claim_due("worker", now=NOW, lease_for=timedelta(seconds=30), limit=100)
    due = NOW + timedelta(seconds=30)
    records = store.claim_due("worker", now=due, lease_for=timedelta(seconds=30), limit=100)
    store.commit_merge_intent(store.merge_groups()[0], records, worker_id="worker", now=due,
        request_sha256="e" * 64, request_utf8_bytes=1)
    store.recover_expired_leases(now=due + timedelta(seconds=30))
    assert store.prune_merge_metadata(now=due + timedelta(days=8)) == 0
    assert len(store.merge_groups()) == 1
    assert store.merge_channel_stats()[0].possible_requests == 1
