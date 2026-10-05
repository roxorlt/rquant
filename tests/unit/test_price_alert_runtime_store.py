import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from rquant.price_alert_runtime_contracts import (
    PriceAlertFrequencyPolicy,
    verify_price_alert_activation,
)
from rquant.price_alert_runtime_store import (
    PriceAlertRuntimeStore,
    PriceEvaluationRecord,
    PriceRoundInput,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from tests.unit.test_price_alert_event_contracts import AT, event
from tests.unit.test_price_alert_runtime_activation import activation_fixture


def store_fixture(
    tmp_path: Path,
) -> tuple[PriceAlertRuntimeStore, object, PriceAlertFrequencyPolicy]:
    policy = PriceAlertFrequencyPolicy(cooldown_seconds=60)
    path, digest = activation_fixture(tmp_path, frequency_policy_sha256=policy.sha256)
    activation = verify_price_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=digest,
        expected_commit="b" * 40,
        expected_kind=RuntimeServiceKind.PRICE_ALERT_RUNTIME,
    )
    return (
        PriceAlertRuntimeStore.install(tmp_path / "runtime.sqlite3", activation=activation),
        activation,
        policy,
    )


def round_input(
    activation: object,
    policy: PriceAlertFrequencyPolicy,
    *,
    at=AT,
    version: int = 1,
    membership: int = 1,
    scope: str = "4" * 64,
    state: str = "triggered",
    quote_at=None,
) -> PriceRoundInput:
    from rquant.price_alert_runtime_contracts import require_price_alert_activation

    binding = require_price_alert_activation(activation, "evaluation")
    quote_at = at if quote_at is None else quote_at
    item = (
        None
        if state != "triggered"
        else event(
            evaluated_at=at,
            available_at=at,
            quote_observed_at=quote_at,
            quote_available_at=quote_at,
            expires_at=at + timedelta(seconds=120),
            rule_version=version,
            membership_version=membership,
            scope_generation_id=scope,
            frequency_policy_sha256=policy.sha256,
            producer_manifest_sha256=binding.producer_manifest_sha256,
            producer_commit=binding.producer_commit,
            source_epoch=binding.source_epoch,
        )
    )
    record = PriceEvaluationRecord(
        owner_id="alice",
        rule_id="r1",
        rule_version=version,
        membership_version=membership,
        ts_code="600000.SH",
        state=state,
        reason="threshold_reached" if item else "quote_missing",
        event=item,
    )
    return PriceRoundInput(
        evaluated_at=at,
        scope_generation_id=scope,
        scope_manifest_sha256="5" * 64,
        scope_source_generation_id="6" * 64,
        scope_source_sequence=1,
        quote_source_generation_id="7" * 64,
        quote_batch_id="8" * 64,
        requested_codes=1,
        valid_quotes=1 if item else 0,
        records=(record,),
    )


def test_cooldown_repeat_rule_edit_restart_and_exact_boundary(tmp_path: Path) -> None:
    store, activation, policy = store_fixture(tmp_path)
    original = store.commit_round(round_input(activation, policy), policy=policy)
    assert len(original.events) == 1
    original_bytes = original.events[0].event.wire_bytes()
    for seconds, version in [(1, 1), (2, 2), (59, 2)]:
        result = store.commit_round(
            round_input(activation, policy, at=AT + timedelta(seconds=seconds), version=version),
            policy=policy,
        )
        assert not result.events
    store.close()
    store = PriceAlertRuntimeStore(tmp_path / "runtime.sqlite3", activation=activation)
    next_result = store.commit_round(
        round_input(activation, policy, at=AT + timedelta(seconds=60), version=2), policy=policy
    )
    assert len(next_result.events) == 1
    assert (
        store.events_after(0, inspected_at=AT + timedelta(seconds=61))[0].event.wire_bytes()
        == original_bytes
    )
    assert store.source_descriptor().high_watermark == 2
    store.close()


def test_same_observation_new_clock_reuses_original_sealed_body(tmp_path: Path) -> None:
    store, activation, policy = store_fixture(tmp_path)
    source = round_input(activation, policy)
    one = store.commit_round(source, policy=policy)
    replay = store.commit_round(source, policy=policy)
    assert replay == one
    two = store.commit_round(
        round_input(activation, policy, at=AT + timedelta(seconds=1), quote_at=AT, scope="9" * 64),
        policy=policy,
    )
    assert not two.events
    assert (
        store.events_after(0, inspected_at=AT + timedelta(seconds=1))[0].event
        == one.events[0].event
    )
    store.close()


@pytest.mark.parametrize("point", ["head", "cooldown", "event", "receipt", "before_commit"])
def test_transaction_failure_rolls_back_every_fact(tmp_path: Path, point: str) -> None:
    store, activation, policy = store_fixture(tmp_path)

    def fail(name: str) -> None:
        if name == point:
            raise OSError("injected journal failure")

    store.failpoint = fail
    with pytest.raises(OSError):
        store.commit_round(round_input(activation, policy), policy=policy)
    assert store.source_descriptor().high_watermark == 0
    with sqlite3.connect(store.path) as connection:
        for table in (
            "price_alert_frequency_state",
            "price_alert_evaluation_head",
            "price_alert_round_receipt",
            "price_alert_event_log",
        ):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    store.failpoint = lambda _: None
    assert len(store.commit_round(round_input(activation, policy), policy=policy).events) == 1
    store.close()


def test_after_commit_lost_ack_recovers_and_pointer_drift_writes_nothing(tmp_path: Path) -> None:
    store, activation, policy = store_fixture(tmp_path)
    source = round_input(activation, policy)
    with pytest.raises(ValueError):
        store.commit_round(source, policy=policy, current_scope=lambda: False)
    assert store.source_descriptor().high_watermark == 0
    store.failpoint = lambda name: (
        (_ for _ in ()).throw(OSError("lost acknowledgement")) if name == "after_commit" else None
    )
    with pytest.raises(OSError):
        store.commit_round(source, policy=policy)
    assert store.source_descriptor().high_watermark == 1
    store.failpoint = lambda _: None
    assert len(store.commit_round(source, policy=policy).events) == 1
    store.close()


def test_ledger_lock_missing_reopen_and_clock_rollback_fail_closed(tmp_path: Path) -> None:
    store, activation, policy = store_fixture(tmp_path)
    with pytest.raises((OSError, ValueError)):
        PriceAlertRuntimeStore(tmp_path / "runtime.sqlite3", activation=activation)
    store.commit_round(round_input(activation, policy), policy=policy)
    with pytest.raises(ValueError):
        store.commit_round(
            round_input(activation, policy, at=AT - timedelta(seconds=1)), policy=policy
        )
    store.close()
    (tmp_path / "runtime.sqlite3").unlink()
    with pytest.raises(ValueError):
        PriceAlertRuntimeStore(tmp_path / "runtime.sqlite3", activation=activation)


def test_unavailable_does_not_reset_cooldown_new_member_has_own_key(tmp_path: Path) -> None:
    store, activation, policy = store_fixture(tmp_path)
    store.commit_round(round_input(activation, policy), policy=policy)
    store.commit_round(
        round_input(activation, policy, at=AT + timedelta(seconds=1), state="unavailable"),
        policy=policy,
    )
    assert not store.commit_round(
        round_input(activation, policy, at=AT + timedelta(seconds=2)), policy=policy
    ).events
    assert (
        len(
            store.commit_round(
                round_input(activation, policy, at=AT + timedelta(seconds=3), membership=2),
                policy=policy,
            ).events
        )
        == 1
    )
    store.close()


def test_runtime_snapshot_uses_latest_committed_round_at_the_same_clock(tmp_path: Path) -> None:
    store, activation, policy = store_fixture(tmp_path)
    first = store.record_unavailable(evaluated_at=AT, reason="scope_unavailable")
    second = store.commit_round(round_input(activation, policy), policy=policy)
    assert store.runtime_snapshot(observed_at=AT).round.round_id == second.round_id
    third = store.record_unavailable(evaluated_at=AT, reason="scope_expired")
    assert store.runtime_snapshot(observed_at=AT).round.round_id == third.round_id
    assert store.runtime_snapshot(observed_at=AT).rules == ()
    assert first.round_id != second.round_id != third.round_id
    store.close()


def test_known_empty_round_does_not_publish_an_old_rule_head(tmp_path: Path) -> None:
    store, activation, policy = store_fixture(tmp_path)
    store.commit_round(round_input(activation, policy), policy=policy)
    empty = round_input(activation, policy).model_copy(
        update={"records": (), "requested_codes": 0, "valid_quotes": 0}
    )
    receipt = store.commit_round(empty, policy=policy)
    snapshot = store.runtime_snapshot(observed_at=AT)
    assert snapshot.round.round_id == receipt.round_id
    assert snapshot.rules == () and snapshot.round.decision_count == 0
    store.close()
