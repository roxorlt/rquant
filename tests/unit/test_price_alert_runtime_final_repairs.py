from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import pytest

from rquant.delivery_contracts import DeliveryChannel, OutboxRecord
from rquant.notification_state import (
    _REQUIRED_NOTIFICATION_PROJECTION_TABLES,
    NotificationProjectionAuthoritySnapshot,
    NotificationStateStore,
)
from rquant.notification_worker import run_notification_batch
from rquant.paper_signal_consumer import PaperSignalConsumerStateStore
from rquant.price_alert_route import (
    PriceAlertBusRoutedRecord,
    PriceAlertOwnerTargets,
    PriceAlertRecipientPolicy,
    _price_ingest,
)
from rquant.price_alert_runtime import evaluate_price_alert_round
from rquant.price_alert_runtime_contracts import PriceAlertFrequencyPolicy
from rquant.price_alert_runtime_projection import (
    PriceAlertDeliveryAuthorityInput,
    PriceAlertRuntimeState,
    price_runtime_projections,
)
from rquant.price_alert_runtime_source import PriceAlertScopeSnapshot
from rquant.price_alert_runtime_store import ReadonlyPriceAlertRuntimeStore
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    build_manual_watchlist_projections,
)
from rquant.serving_price_alert_rule_projection import (
    PriceAlertRuleAuthoritySnapshot,
    build_price_alert_rule_projections,
)
from rquant.serving_read_models import (
    PAGE_PROJECTION_CONTRACTS,
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    _projection_json_bytes,
)
from tests.support.web_serving_fixture import build_web_fixture
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_notification_admission import (
    admission_fixture,
    notifier_activation,
)
from tests.unit.test_price_alert_notification_replication import replication_fixture
from tests.unit.test_price_alert_route import route_fixture
from tests.unit.test_price_alert_runtime import inputs
from tests.unit.test_price_alert_runtime_capacity import exact_projection_inputs
from tests.unit.test_web_price_alert_rules import client_for


class LegacyProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def deliver(self, item: OutboxRecord) -> str:
        self.calls.append(item.signal.signal_id)
        return "synthetic-accepted"


def test_price_delivery_disabled_keeps_actual_legacy_claim_and_delivery(tmp_path: Path) -> None:
    producer, bus, spool, reader, state, source, records = replication_fixture(tmp_path)
    try:
        state.replicate_mixed_notification_events(
            source, records, observed_at=AT, source_inspected_at=AT
        )
        policy = PriceAlertRecipientPolicy(
            generation_id="e" * 64,
            owners=(PriceAlertOwnerTargets(owner_id="alice", targets=()),),
        )
        cap, unused = notifier_activation(tmp_path, policy, enabled=False)
        provider = LegacyProvider()
        run_notification_batch(
            state,
            {DeliveryChannel.PUSHDEER: provider},
            worker_id="original-notifier",
            now=AT,
            clock=lambda: AT,
            lease_for=timedelta(seconds=10),
            limit=10,
            price_activation=cap,
        )
        assert len(provider.calls) == 2 and records[1].event_id not in provider.calls
        assert state.replication_cursor().last_global_sequence == 3
        price = next(x for x in state.outbox_records() if x.signal_id == records[1].event_id)
        assert price.status.value == "pending" and price.attempt_count == 0
        assert len(state.attempts()) == 2
    finally:
        producer.close()


@pytest.mark.parametrize("invalid", ["forged", "wrong_role", "changed_manifest"])
def test_disabled_fallback_never_accepts_an_invalid_capability(
    tmp_path: Path, invalid: str
) -> None:
    producer, bus, spool, reader, state, source, records = replication_fixture(tmp_path)
    try:
        state.replicate_mixed_notification_events(
            source, records, observed_at=AT, source_inspected_at=AT
        )
        policy = PriceAlertRecipientPolicy(generation_id="e" * 64, owners=())
        cap, unused = notifier_activation(tmp_path, policy, enabled=False)
        if invalid == "forged":
            cap = object()
        elif invalid == "wrong_role":
            cap = producer.activation
        else:
            path = tmp_path / "notify.json"
            path.write_bytes(path.read_bytes() + b" ")
        provider = LegacyProvider()
        with pytest.raises((TypeError, ValueError)):
            run_notification_batch(
                state,
                {DeliveryChannel.PUSHDEER: provider},
                worker_id="original-notifier",
                now=AT,
                clock=lambda: AT,
                lease_for=timedelta(seconds=10),
                limit=10,
                price_activation=cap,
            )
        assert provider.calls == []
        assert all(
            x.status.value == "pending" and x.attempt_count == 0 for x in state.outbox_records()
        )
    finally:
        producer.close()


@pytest.mark.parametrize("domain", ["route", "paper"])
@pytest.mark.parametrize("tamper", ["column", "index", "trigger"])
def test_mixed_history_requires_exact_schema_at_install_and_actual_entry(
    tmp_path: Path, domain: str, tamper: str
) -> None:
    producer = None
    if domain == "route":
        producer, bus, cap, policy, source, item = route_fixture(tmp_path)
        path, table = bus.path, "price_alert_route_receipt"

        def install() -> None:
            bus.install_price_alert_route_v1(cap)

        def entry() -> object:
            return bus.commit_price_alert_route(
                activation=cap,
                policy=policy,
                source=source,
                record=item,
                source_inspected_at=AT,
                routed_at=AT,
            )
    else:
        paper = PaperSignalConsumerStateStore(tmp_path / "paper.sqlite3")
        paper.install_mixed_notification_history()
        path, table = paper.path, "paper_price_non_trading_receipt"
        install = paper.install_mixed_notification_history

        def entry() -> object:
            return paper.non_trading_receipt(1)

    try:
        with sqlite3.connect(path) as connection:
            if tamper == "column":
                connection.execute(f"ALTER TABLE {table} ADD COLUMN unregistered TEXT")
            elif tamper == "index":
                connection.execute(f"CREATE INDEX unregistered ON {table}(event_id)")
            else:
                connection.execute(
                    f"CREATE TRIGGER unregistered AFTER INSERT ON {table} "
                    f"BEGIN DELETE FROM {table}; END"
                )
        before = sha256(path.read_bytes()).hexdigest()
        for operation in (install, entry):
            with pytest.raises(ValueError, match="schema"):
                operation()
            assert sha256(path.read_bytes()).hexdigest() == before
    finally:
        if producer is not None:
            producer.close()


def legacy_projection_inputs(
    size: int,
    *,
    include_market: bool = False,
) -> tuple[ServingProjectionInput, ...]:
    values = []
    names = ("screen_result", "monitor_event", "surge_event")
    if include_market:
        names += ("market_snapshot",)
    for name in names:
        contract = PAGE_PROJECTION_CONTRACTS[name]
        rows = []
        for index in range(33):
            row = dict.fromkeys(contract.column_names)
            row.update(trade_date=AT.date().isoformat(), ts_code=f"{600000 + index:06d}.SH")
            if name == "screen_result":
                row.update(preset_name="synthetic", name="x" * 60000, close=10.0, pct_chg=1.0)
            elif name == "monitor_event":
                row.update(
                    trigger_time=AT.isoformat(),
                    level="1",
                    trigger_price=10.0,
                    level_price=10.0,
                    trigger_type="x" * 60000,
                    pool="test",
                )
            elif name == "surge_event":
                row.update(
                    confirmed_at=AT.isoformat(),
                    name="x" * 60000,
                    theme="test",
                    price=10.0,
                    pct_chg=1.0,
                    cum_amount=1.0,
                    rel_cum=1.0,
                    room_to_limit_pct=1.0,
                    status="test",
                )
            else:
                row.pop("trade_date")
                row.update(as_of=AT.isoformat(), name="x" * 60000)
            rows.append(row)
        values.append(
            ServingProjectionInput.bind(
                ServingProjectionPayload(table_name=name, available_at=AT, rows=tuple(rows)),
                owner_dataset_id="signals",
                owner_generation_id="6" * 64,
            )
        )
    missing = sum(_projection_json_bytes(x) for x in values) - size
    assert missing >= 0
    for index, value in enumerate(values):
        rows = [dict(row) for row in value.rows]
        key = "trigger_type" if value.table_name == "monitor_event" else "name"
        for row in rows:
            removed = min(len(row[key]), missing)
            row[key] = row[key][removed:]
            missing -= removed
        values[index] = ServingProjectionInput(
            **{**value.model_dump(mode="python"), "rows": tuple(rows)}
        )
    assert missing == 0 and sum(_projection_json_bytes(x) for x in values) == size
    return tuple(values)


@pytest.mark.parametrize("extra", [-1, 0, 1])
def test_price_domain_counts_in_original_signals_owner_exact_total_budget(extra: int) -> None:
    prices = exact_projection_inputs(2 * 1024 * 1024)
    legacy = legacy_projection_inputs(5 * 1024 * 1024 + extra)
    values = tuple(sorted((*legacy, *prices), key=lambda x: x.table_name))
    assert sum(_projection_json_bytes(x) for x in values) == 7 * 1024 * 1024 + extra
    if extra == 1:
        with pytest.raises(ValueError, match="authority byte budget"):
            ServingReadModelInput(observed_at=AT, projections=values)
    else:
        assert len(ServingReadModelInput(observed_at=AT, projections=values).projections) == 7


@pytest.mark.parametrize("legacy_at_limit", [False, True])
def test_actual_notifier_joint_budget_marks_only_price_unavailable(
    tmp_path: Path,
    legacy_at_limit: bool,
) -> None:
    producer, bus, router, policy, source, item = route_fixture(tmp_path)
    peer = None
    try:
        cap, digest = notifier_activation(tmp_path, policy)
        state = NotificationStateStore(tmp_path / "joint-notify.sqlite3")
        state.install_price_alert_delivery_v1(cap)
        archived = tuple(
            PriceAlertBusRoutedRecord.model_validate_json(row["body_json"])
            for row in exact_projection_inputs(3 * 1024 * 1024 // 2)[2].rows
        )
        with state._write_transaction() as connection:
            for record in sorted(archived, key=lambda value: value.global_sequence):
                sequence, inserted = _price_ingest(connection, record.event, record.received_at)
                assert inserted and sequence == record.global_sequence
                connection.execute(
                    "INSERT INTO price_alert_route_receipt VALUES(?,?,?,?,?,?)",
                    (
                        record.event_id,
                        record.source.source_id,
                        record.source_sequence,
                        record.source.wire_bytes(),
                        record.receipt.wire_bytes(),
                        record.bus_generation_id,
                    ),
                )
        large_core = {"screen_result"}
        if legacy_at_limit:
            large_core.add("market_snapshot")
        empty_core = tuple(
            ServingProjectionPayload(table_name=name, available_at=AT, rows=())
            for name in sorted(_REQUIRED_NOTIFICATION_PROJECTION_TABLES - large_core)
        )
        empty_size = sum(
            _projection_json_bytes(
                ServingProjectionInput.bind(
                    value,
                    owner_dataset_id="signals",
                    owner_generation_id="6" * 64,
                )
            )
            for value in empty_core
        )
        legacy_size = 7 * 1024 * 1024 - empty_size if legacy_at_limit else 5_957_296
        legacy = {
            value.table_name: ServingProjectionPayload(
                table_name=value.table_name,
                available_at=value.available_at,
                rows=value.rows,
            )
            for value in legacy_projection_inputs(legacy_size, include_market=legacy_at_limit)
        }
        for name in _REQUIRED_NOTIFICATION_PROJECTION_TABLES:
            legacy.setdefault(
                name, ServingProjectionPayload(table_name=name, available_at=AT, rows=())
            )
        authority = NotificationProjectionAuthoritySnapshot.create(
            observed_at=AT,
            available_at=AT,
            source_receipts={"synthetic": "b" * 64},
            projections=tuple(legacy.values()),
        )
        state.publish_projection_authority(authority)
        peer = ReadonlyPriceAlertRuntimeStore(producer.path, activation=producer.activation)
        with state._read_snapshot() as connection:
            original_price = price_runtime_projections(
                connection,
                producer=peer.runtime_snapshot(observed_at=AT),
                observed_at=AT,
                shadow=False,
            )

        def bound(values: Iterable[ServingProjectionPayload]) -> tuple[ServingProjectionInput, ...]:
            return tuple(
                ServingProjectionInput.bind(
                    value,
                    owner_dataset_id="signals",
                    owner_generation_id="6" * 64,
                )
                for value in values
            )

        price_bytes = sum(_projection_json_bytes(value) for value in bound(original_price))
        joint_bytes = sum(
            _projection_json_bytes(value)
            for value in bound((*authority.projections, *original_price))
        )
        assert price_bytes <= 2 * 1024 * 1024 and joint_bytes > 7 * 1024 * 1024
        if legacy_at_limit:
            before = sha256(state.path.read_bytes()).hexdigest()
            with pytest.raises(ValueError, match="owner byte budget"):
                state.serving_price_enabled_snapshot(
                    producer=peer,
                    activation=cap,
                    observed_at=AT,
                    history_limit=100,
                    shadow=False,
                )
            assert sha256(state.path.read_bytes()).hexdigest() == before
            original = state.serving_snapshot(observed_at=AT, history_limit=100)
            coherent = ServingReadModelInput(
                observed_at=AT, projections=bound(original.payload.projections)
            )
            assert (
                sum(_projection_json_bytes(value) for value in coherent.projections)
                == 7 * 1024 * 1024
            )
            assert original.payload.projections == authority.projections
            print(
                {
                    "legacy_bytes": 7 * 1024 * 1024,
                    "new_domain_paused": True,
                    "legacy_bytes_unchanged": True,
                }
            )
            return
        snapshot = state.serving_price_enabled_snapshot(
            producer=peer,
            activation=cap,
            observed_at=AT,
            history_limit=100,
            shadow=False,
        )
        result = {value.table_name: value for value in snapshot.payload.projections}
        runtime = PriceAlertRuntimeState.model_validate_json(
            result["price_alert_runtime_state"].rows[0]["body_json"]
        )
        assert runtime.availability == "unavailable" and runtime.reason == "facts_unavailable"
        assert runtime.event_count == 0 and runtime.rule_count == 0 and runtime.attempt_count == 0
        assert all(result[name] == value for name, value in legacy.items())
        assert snapshot.projection_generation_id == authority.generation_id
        assert snapshot.projection_source_receipts == authority.source_receipts
        coherent = ServingReadModelInput(observed_at=AT, projections=bound(result.values()))
        assert (
            sum(_projection_json_bytes(value) for value in coherent.projections) <= 7 * 1024 * 1024
        )
        with state._read_snapshot() as connection:
            assert connection.execute("SELECT COUNT(*) FROM price_alert_route_receipt").fetchone()[
                0
            ] == len(archived)
        print(
            {
                "price_bytes": price_bytes,
                "joint_bytes_before_fallback": joint_bytes,
                "joint_bytes_after_fallback": sum(
                    _projection_json_bytes(value) for value in coherent.projections
                ),
            }
        )
    finally:
        if peer is not None:
            peer.close()
        producer.close()


@pytest.mark.parametrize("mode", ["waiting", "all_disabled", "delivery_disabled", "not_started"])
def test_actual_runtime_non_evaluation_states_keep_their_meaning(tmp_path: Path, mode: str) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    peer = None
    try:
        now = AT + timedelta(seconds=5)
        scope, quotes, calendar = inputs(
            now, enabled=mode != "all_disabled", is_open=mode != "waiting"
        )
        rules = authority.scope.rules
        if mode == "all_disabled":
            rules = (
                rules[0].model_copy(update={"enabled": False, "version": 2, "updated_at": now}),
            )
        members = authority.scope.members
        scope = scope.model_copy(
            update={
                "generation_id": "d" * 64,
                "manifest_sha256": "e" * 64,
                "source_sequence": 2,
                "rules": rules,
                "members": members,
                "rule_rows_sha256": PriceAlertRuleAuthoritySnapshot.digest(rules),
                "member_rows_sha256": ManualWatchlistAuthoritySnapshot.digest(members),
            }
        )
        quotes = quotes.model_copy(
            update={
                "scope_generation_id": scope.generation_id,
                "scope_manifest_sha256": scope.manifest_sha256,
            }
        )
        frequency = PriceAlertFrequencyPolicy(cooldown_seconds=60)
        round_ = evaluate_price_alert_round(
            activation=producer.activation,
            scope=scope,
            quotes=None if mode == "all_disabled" else quotes,
            calendar=calendar,
            evaluated_at=now,
            policy=frequency,
        )
        producer.commit_round(round_, policy=frequency, current_scope=lambda: True)
        policy_manifest = authority.owner_policy_manifest_sha256
        if mode == "delivery_disabled":
            cap, digest = notifier_activation(
                tmp_path, authority.policy, suffix="disabled", enabled=False
            )
            policy_manifest = digest
        latest = PriceAlertDeliveryAuthorityInput(
            scope=scope,
            policy=authority.policy,
            owner_policy_manifest_sha256=policy_manifest,
            delivery_enabled=mode != "delivery_disabled",
            inspected_at=now,
        )
        state.apply_price_alert_delivery_authority(
            latest, activation=cap, expected_revision=1, applied_at=now
        )
        peer = ReadonlyPriceAlertRuntimeStore(producer.path, activation=producer.activation)
        snapshot = state.serving_price_enabled_snapshot(
            producer=peer,
            activation=cap,
            observed_at=now,
            history_limit=100,
            shadow=False,
        )
        payloads = () if mode == "not_started" else snapshot.payload.projections
        root = tmp_path / "serving"
        with patch("tests.support.web_serving_fixture.FIXTURE_BUILT_AT", now):
            build_web_fixture(
                root,
                "baseline",
                signal_projections=(
                    *payloads,
                    *build_price_alert_rule_projections(
                        PriceAlertRuleAuthoritySnapshot.create(
                            activated_at=AT - timedelta(days=1), rows=scope.rules
                        ),
                        observed_at=now,
                    ),
                    *build_manual_watchlist_projections(
                        ManualWatchlistAuthoritySnapshot.create(
                            activated_at=AT - timedelta(days=1), rows=scope.members
                        ),
                        observed_at=now,
                    ),
                ),
            )
        with client_for(root, clock=lambda: now) as client:
            reply = client.get(
                "/api/v1/monitor/price-rules/runtime", headers={"x-rquant-user": "alice"}
            )
            assert reply.status_code == 200
            data = reply.json()["data"]
            if mode == "waiting":
                assert (data["status"], data["message"]) == ("not_running", "等待交易时段。")
            elif mode == "all_disabled":
                assert (
                    data["status"] == "not_running"
                    and data["items"][0]["message"] == "规则已停用。"
                )
            elif mode == "delivery_disabled":
                assert (data["status"], data["mode"], data["message"]) == (
                    "attention",
                    "disabled",
                    "通知尚未开放。",
                )
            else:
                assert data["availability"] == "not_running" and data["items"] == []
    finally:
        if peer is not None:
            peer.close()
        producer.close()


@pytest.mark.parametrize("missing_owner", ["alice", "bob"])
def test_actual_partial_quotes_aggregate_only_current_owner(
    tmp_path: Path, missing_owner: str
) -> None:
    producer, bus, state, cap, authority, applied, routed = admission_fixture(tmp_path)
    peer = None
    try:
        now = AT + timedelta(seconds=5)
        old = authority.scope
        rules = tuple(
            sorted(
                (
                    old.rules[0],
                    old.rules[0].model_copy(
                        update={
                            "owner_id": missing_owner,
                            "rule_id": "r2",
                            "ts_code": "600001.SH",
                        }
                    ),
                ),
                key=lambda row: (row.owner_id, row.rule_id),
            )
        )
        members = tuple(
            sorted(
                (
                    old.members[0],
                    old.members[0].model_copy(
                        update={
                            "owner_id": missing_owner,
                            "ts_code": "600001.SH",
                        }
                    ),
                ),
                key=lambda row: (row.owner_id, row.ts_code),
            )
        )
        scope = PriceAlertScopeSnapshot(
            generation_id="d" * 64,
            manifest_sha256="e" * 64,
            source_generation_id=old.source_generation_id,
            source_sequence=2,
            built_at=now,
            available_at=now,
            inspected_at=now,
            rules=rules,
            members=members,
            rule_rows_sha256=PriceAlertRuleAuthoritySnapshot.digest(rules),
            member_rows_sha256=ManualWatchlistAuthoritySnapshot.digest(members),
        )
        unused, quotes, calendar = inputs(now)
        quotes = quotes.model_copy(
            update={
                "scope_generation_id": scope.generation_id,
                "scope_manifest_sha256": scope.manifest_sha256,
                "requested_codes": scope.codes,
            }
        )
        frequency = PriceAlertFrequencyPolicy(cooldown_seconds=60)
        round_ = evaluate_price_alert_round(
            activation=producer.activation,
            scope=scope,
            quotes=quotes,
            calendar=calendar,
            evaluated_at=now,
            policy=frequency,
        )
        assert round_.requested_codes == 2 and round_.valid_quotes == 1
        producer.commit_round(round_, policy=frequency, current_scope=lambda: True)
        latest = PriceAlertDeliveryAuthorityInput(
            scope=scope,
            policy=authority.policy,
            owner_policy_manifest_sha256=authority.owner_policy_manifest_sha256,
            delivery_enabled=True,
            inspected_at=now,
        )
        state.apply_price_alert_delivery_authority(
            latest, activation=cap, expected_revision=1, applied_at=now
        )
        peer = ReadonlyPriceAlertRuntimeStore(producer.path, activation=producer.activation)
        snapshot = state.serving_price_enabled_snapshot(
            producer=peer, activation=cap, observed_at=now, history_limit=100, shadow=False
        )
        root = tmp_path / "serving"
        with patch("tests.support.web_serving_fixture.FIXTURE_BUILT_AT", now):
            build_web_fixture(
                root,
                "baseline",
                signal_projections=(
                    *snapshot.payload.projections,
                    *build_price_alert_rule_projections(
                        PriceAlertRuleAuthoritySnapshot.create(
                            activated_at=AT - timedelta(days=1), rows=rules
                        ),
                        observed_at=now,
                    ),
                    *build_manual_watchlist_projections(
                        ManualWatchlistAuthoritySnapshot.create(
                            activated_at=AT - timedelta(days=1), rows=members
                        ),
                        observed_at=now,
                    ),
                ),
            )
        with client_for(root, clock=lambda: now) as client:
            reply = client.get(
                "/api/v1/monitor/price-rules/runtime", headers={"x-rquant-user": "alice"}
            )
            assert reply.status_code == 200
            data = reply.json()["data"]
            assert data["availability"] == "ready"
            assert data["items"][0]["state"] == "triggered"
            assert data["items"][0]["status"] == "normal"
            if missing_owner == "alice":
                assert data["status_label"] == "注意" and "行情" in data["message"]
                assert data["items"][1]["state"] == "unavailable"
            else:
                assert data["status_label"] == "正常" and len(data["items"]) == 1
    finally:
        if peer is not None:
            peer.close()
        producer.close()
