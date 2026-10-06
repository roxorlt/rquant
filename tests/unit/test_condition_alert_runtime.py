from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pytest

AT = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)


def condition_activation(tmp_path: Path):
    from rquant.condition_alert_runtime import ConditionFrequencyPolicy
    from rquant.condition_alert_runtime_contracts import verify_condition_alert_activation
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    path = tmp_path / "condition-evaluator.json"
    path.write_text(
        json.dumps(
            {
                "service_id": "condition.evaluate",
                "service_kind": "condition_alert_runtime",
                "plane": "live",
                "interval_seconds": 5,
                "stale_after_seconds": 30,
                "producer_commit": "a" * 40,
                "settings": {
                    "condition_alert_runtime": {
                        "source_id": "condition-runtime",
                        "source_epoch": "b" * 64,
                        "ledger_id": "d" * 64,
                        "generation_id": "e" * 64,
                        "evaluation_contract_sha256": "7" * 64,
                        "frequency_policy_sha256": ConditionFrequencyPolicy().sha256,
                        "routing_policy_sha256": "f" * 64,
                        "recipient_policy_sha256": "c" * 64,
                        "evaluation_enabled": True,
                        "event_write_enabled": True,
                    }
                },
            }
        )
    )
    path.chmod(0o600)
    return verify_condition_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=sha256(path.read_bytes()).hexdigest(),
        expected_commit="a" * 40,
        expected_kind=RuntimeServiceKind.CONDITION_ALERT_RUNTIME,
    )


def condition_round(
    *,
    at: datetime = AT,
    truth: str = "true",
    version: int = 1,
    scope_version: str = "2" * 64,
    frequency: dict | None = None,
):
    from rquant.alert_rule_contracts import (
        ConditionAlertRuleDefinition,
        ConditionAlertScopeEvidence,
        OwnedConditionAlertRule,
    )
    from rquant.condition_alert_runtime import (
        ConditionEvaluationRecord,
        ConditionRoundInput,
        ConditionRoundRule,
        ConditionSourceFacts,
    )
    from rquant.runtime_contracts import canonical_sha256

    rule = ConditionAlertRuleDefinition(
        rule_id="full-rule",
        name="完整条件",
        priority="P1",
        enabled=True,
        conditions=[{"name": "gt", "args": {"left": "INTRADAY_PRICE[0]", "right": 10}}],
        scope={"kind": "market"},
        frequency=frequency or {"kind": "every_evaluation"},
        governance={"channels": ["pushdeer"], "dedup_window_seconds": 60, "notify_recovery": True},
    )
    codes = ("600000.SH",)
    scope = ConditionAlertScopeEvidence(
        owner_id="alice",
        scope=rule.scope,
        scope_version=scope_version,
        member_codes=codes,
        member_digest=canonical_sha256(codes),
        available_at=at,
    )
    return ConditionRoundInput(
        evaluated_at=at,
        serving_generation_id="3" * 64,
        serving_manifest_sha256="4" * 64,
        source=ConditionSourceFacts(
            source_identity=canonical_sha256(at),
            raw_batch_id="5" * 64,
            feature_snapshot_id="6" * 64,
            daily_anchor_date=date(2026, 7, 30),
            trade_date=date(2026, 7, 31),
            cutoff=at,
            feature_contract_version=4,
            universe_codes=codes,
        ),
        rules=(
            ConditionRoundRule(
                owned=OwnedConditionAlertRule(
                    owner_id="alice", version=version, rule=rule, updated_at=at
                ),
                scope=scope,
            ),
        ),
        records=(
            ConditionEvaluationRecord(
                owner_id="alice",
                rule_id="full-rule",
                ts_code="600000.SH",
                stock_name="样本",
                truth=truth,
                reason="conditions_matched"
                if truth == "true"
                else "conditions_not_matched"
                if truth == "false"
                else "source_missing",
                event_time=at - timedelta(seconds=2),
            ),
        ),
    )


def runtime_world(tmp_path: Path):
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore
    from tests.unit.test_price_alert_runtime_store import store_fixture

    ledger, old_activation, old_policy = store_fixture(tmp_path)
    activation = condition_activation(tmp_path)
    store = ConditionAlertRuntimeStore.install(ledger, activation=activation)
    return store, ledger, old_activation, old_policy, activation


@pytest.mark.parametrize(
    "minutes,source_delta,expected",
    [(1, 0, 0), (1, 59, 0), (1, 60, 1), (1, 61, 1), (2, 119, 0), (2, 120, 1)],
)
def test_symbol_minutes_uses_exact_source_clock_and_survives_reopen(
    tmp_path: Path, minutes: int, source_delta: int, expected: int
) -> None:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore, ConditionRoundInput
    from rquant.price_alert_runtime_store import PriceAlertRuntimeStore

    store, ledger, old_activation, _, activation = runtime_world(tmp_path)
    first_data = condition_round(
        frequency={"kind": "per_symbol_minutes", "minutes": minutes}
    ).model_dump(mode="python")
    first_data["rules"][0]["owned"]["rule"]["governance"]["dedup_window_seconds"] = 0
    first = ConditionRoundInput.model_validate(first_data)
    initial = store.commit_round(first)
    assert len(initial.events) == 1
    with ledger._connection(write=True) as connection:
        # Existing pre-repair rows used the evaluation clock. Their trusted last event is sufficient.
        connection.execute(
            "UPDATE condition_alert_frequency_state SET next_allowed_at=?",
            ((AT + timedelta(days=1)).isoformat(),),
        )
    path = ledger.path
    ledger.close()
    reopened = PriceAlertRuntimeStore(path, activation=old_activation)
    try:
        store = ConditionAlertRuntimeStore(reopened, activation=activation)
        assert store.commit_round(first) == initial
        second_data = condition_round(
            at=AT + timedelta(seconds=max(60 * minutes, source_delta) + 5),
            frequency={"kind": "per_symbol_minutes", "minutes": minutes},
        ).model_dump(mode="python")
        second_data["rules"][0]["owned"]["rule"]["governance"]["dedup_window_seconds"] = 0
        event_time = first.records[0].event_time + timedelta(seconds=source_delta)
        second_data["records"][0]["event_time"] = event_time
        second = ConditionRoundInput.model_validate(second_data)
        receipt = store.commit_round(second)
        assert len(receipt.events) == expected
        assert store.commit_round(second) == receipt
        if expected:
            with reopened._connection() as connection:
                stored = connection.execute(
                    "SELECT next_allowed_at,last_event_time FROM condition_alert_frequency_state"
                ).fetchone()
            assert datetime.fromisoformat(stored[0]) == event_time + timedelta(seconds=60 * minutes)
            assert datetime.fromisoformat(stored[1]) == event_time
    finally:
        reopened.close()


def test_bar_close_uses_the_original_closed_minute_not_a_later_quote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace
    from rquant.condition_alert_runtime import ConditionRoundInput, evaluate_condition_alert_round
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.serving_contracts import ServingCurrentPointer
    from tests.unit.test_serving_screen_intraday import _quote_world
    from tests.unit.test_web_screen_intraday import _borrow

    reader, cutoff = _quote_world(tmp_path, monkeypatch)
    snapshot = reader(cutoff)
    borrowed = _borrow(snapshot)
    borrowed = replace(
        borrowed,
        pointer=ServingCurrentPointer(
            generation_id=borrowed.manifest.generation_id,
            manifest_sha256="4" * 64,
            published_at=cutoff,
        ),
    )
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=date(2026, 7, 30),
        coverage_end=date(2026, 7, 31),
        open_dates=(date(2026, 7, 30), date(2026, 7, 31)),
        generated_at=cutoff - timedelta(days=1),
    )
    try:
        for frequency, expected in (
            ({"kind": "bar_close", "bar_size": "1min"}, "false"),
            ({"kind": "every_evaluation"}, "true"),
        ):
            payload = condition_round(frequency=frequency).model_dump(mode="python")
            payload["rules"][0]["owned"]["rule"]["conditions"][0]["args"]["right"] = 14.5
            bound = ConditionRoundInput.model_validate(payload).rules[0]
            actual = evaluate_condition_alert_round(
                activation=condition_activation(tmp_path),
                borrowed=borrowed,
                rules=(bound.owned,),
                scopes=(bound.scope,),
                calendar=calendar,
                evaluated_at=cutoff,
            )
            assert actual.records[0].truth == expected
        from rquant.runtime_contracts import canonical_sha256

        both_codes = snapshot.source.universe_codes
        scope = bound.scope.model_copy(
            update={"member_codes": both_codes, "member_digest": canonical_sha256(both_codes)}
        )
        # Use the actual typed frequency; no quote-only stock can manufacture a closed line.
        from rquant.alert_rule_contracts import ConditionAlertRuleDefinition

        bar_rule = bound.owned.model_copy(
            update={
                "rule": ConditionAlertRuleDefinition.model_validate(
                    bound.owned.rule.model_dump()
                    | {"frequency": {"kind": "bar_close", "bar_size": "1min"}}
                )
            }
        )
        actual = evaluate_condition_alert_round(
            activation=condition_activation(tmp_path),
            borrowed=borrowed,
            rules=(bar_rule,),
            scopes=(scope,),
            calendar=calendar,
            evaluated_at=cutoff,
        )
        assert [record.truth for record in actual.records] == ["false", "unknown"]
        assert actual.records[1].event_time is None
    finally:
        borrowed.cursor.close()


def test_condition_namespace_keeps_original_ledger_price_reopen_and_single_writer(
    tmp_path: Path,
) -> None:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore
    from rquant.price_alert_runtime_store import PriceAlertRuntimeStore
    from tests.unit.test_price_alert_runtime_store import round_input

    store, ledger, old_activation, old_policy, activation = runtime_world(tmp_path)
    original = ledger.commit_round(round_input(old_activation, old_policy), policy=old_policy)
    one = store.commit_round(condition_round())
    assert len(one.events) == 1
    assert store.commit_round(condition_round()) == one
    with pytest.raises(BlockingIOError):
        PriceAlertRuntimeStore(ledger.path, activation=old_activation)
    path = ledger.path
    ledger.close()
    reopened = PriceAlertRuntimeStore(path, activation=old_activation)
    assert (
        reopened.commit_round(round_input(old_activation, old_policy), policy=old_policy)
        == original
    )
    condition = ConditionAlertRuntimeStore(reopened, activation=activation)
    assert condition.events_after(0, inspected_at=AT) == one.events
    assert condition.source_descriptor().high_watermark == 1
    reopened.close()


@pytest.mark.parametrize("point", ["truth", "cooldown", "event", "receipt", "before_commit"])
def test_condition_truth_frequency_event_receipt_are_one_original_transaction(
    tmp_path: Path, point: str
) -> None:
    store, ledger, *_ = runtime_world(tmp_path)

    def fail(observed: str) -> None:
        if observed == point:
            raise RuntimeError("synthetic rollback")

    store.failpoint = fail
    with pytest.raises(RuntimeError):
        store.commit_round(condition_round())
    with sqlite3.connect(ledger.path) as connection:
        for name in (
            "condition_alert_truth_state",
            "condition_alert_frequency_state",
            "condition_alert_event_log",
            "condition_alert_round_receipt",
            "condition_alert_evaluation_head",
        ):
            assert connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0] == 0
    assert store.source_descriptor().high_watermark == 0
    ledger.close()


def test_unknown_never_recovers_and_rule_scope_versions_clear_confirmed_truth(
    tmp_path: Path,
) -> None:
    store, ledger, *_ = runtime_world(tmp_path)
    assert len(store.commit_round(condition_round()).events) == 1
    assert (
        store.commit_round(condition_round(at=AT + timedelta(seconds=1), truth="unknown")).events
        == ()
    )
    assert (
        store.commit_round(condition_round(at=AT + timedelta(seconds=2), truth="false")).events
        == ()
    )
    assert len(store.commit_round(condition_round(at=AT + timedelta(seconds=61))).events) == 1
    recovered = store.commit_round(condition_round(at=AT + timedelta(seconds=62), truth="false"))
    assert recovered.events[0].event.trigger_kind == "recovered"
    store.commit_round(condition_round(at=AT + timedelta(seconds=122)))
    assert (
        store.commit_round(
            condition_round(at=AT + timedelta(seconds=123), truth="false", version=2)
        ).events
        == ()
    )
    store.commit_round(condition_round(at=AT + timedelta(seconds=183), version=2))
    assert (
        store.commit_round(
            condition_round(
                at=AT + timedelta(seconds=184), truth="false", version=2, scope_version="9" * 64
            )
        ).events
        == ()
    )
    ledger.close()


def test_per_symbol_minute_boundary_bar_close_and_current_generation_guard(tmp_path: Path) -> None:
    store, ledger, *_ = runtime_world(tmp_path)
    frequency = {"kind": "per_symbol_minutes", "minutes": 2}
    first = store.commit_round(condition_round(frequency=frequency))
    assert len(first.events) == 1
    assert (
        store.commit_round(
            condition_round(at=AT + timedelta(seconds=119), frequency=frequency)
        ).events
        == ()
    )
    assert (
        len(
            store.commit_round(
                condition_round(at=AT + timedelta(seconds=120), frequency=frequency)
            ).events
        )
        == 1
    )
    before = store.source_descriptor()
    with pytest.raises(ValueError):
        store.commit_round(
            condition_round(at=AT + timedelta(seconds=240), frequency=frequency),
            current_scope=lambda: False,
        )
    assert store.source_descriptor() == before
    ledger.close()


def test_bar_close_uses_closed_source_bar_and_dedup_uses_source_event_time(tmp_path: Path) -> None:
    store, ledger, *_ = runtime_world(tmp_path)
    frequency = {"kind": "bar_close", "bar_size": "1min"}
    first = condition_round(frequency=frequency)
    assert len(store.commit_round(first).events) == 1
    second = condition_round(at=AT + timedelta(seconds=61), frequency=frequency)
    second = second.model_copy(
        update={
            "records": tuple(
                item.model_copy(update={"event_time": first.records[0].event_time})
                for item in second.records
            )
        }
    )
    assert store.commit_round(second).events == ()
    assert (
        len(
            store.commit_round(
                condition_round(at=AT + timedelta(seconds=62), frequency=frequency)
            ).events
        )
        == 1
    )
    ledger.close()


def test_condition_evaluator_reuses_registry_full_scope_unknown_and_closed_daily_anchor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from rquant.alert_rule_contracts import ConditionAlertScopeEvidence
    from rquant.condition_alert_runtime import evaluate_condition_alert_round
    from rquant.runtime_contracts import canonical_sha256
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.serving_contracts import ServingCurrentPointer
    from tests.unit.test_serving_screen_intraday import _source_world
    from tests.unit.test_web_screen_intraday import _borrow

    activation = condition_activation(tmp_path)
    source, at = _source_world(tmp_path, monkeypatch)
    borrowed = _borrow(source(at))
    borrowed = replace(
        borrowed,
        pointer=ServingCurrentPointer(
            generation_id=borrowed.manifest.generation_id, manifest_sha256="4" * 64, published_at=at
        ),
    )
    original = condition_round()
    owned = original.rules[0].owned
    codes = ("600000.SH", "600001.SH")
    scope = ConditionAlertScopeEvidence(
        owner_id="alice",
        scope=owned.rule.scope,
        scope_version="2" * 64,
        member_codes=codes,
        member_digest=canonical_sha256(codes),
        available_at=at,
    )
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=date(2026, 7, 30),
        coverage_end=date(2026, 7, 31),
        open_dates=(date(2026, 7, 30), date(2026, 7, 31)),
        generated_at=at - timedelta(days=1),
    )
    try:
        actual = evaluate_condition_alert_round(
            activation=activation,
            borrowed=borrowed,
            rules=(owned,),
            scopes=(scope,),
            calendar=calendar,
            evaluated_at=at,
        )
        assert tuple((item.ts_code, item.truth) for item in actual.records) == (
            ("600000.SH", "true"),
            ("600001.SH", "unknown"),
        )
        assert actual.source.daily_anchor_date == date(2026, 7, 30)
        unknown = evaluate_condition_alert_round(
            activation=activation,
            borrowed=borrowed,
            rules=(owned,),
            scopes=(scope,),
            calendar=None,
            evaluated_at=at,
        )
        assert all(item.truth == "unknown" for item in unknown.records)
    finally:
        borrowed.cursor.close()
