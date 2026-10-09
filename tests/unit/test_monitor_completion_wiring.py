from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pytest
from pydantic import ValidationError

from rquant.runtime_builder_signal import NotifierSettings

if TYPE_CHECKING:
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore
    from rquant.monitor_builtin_contracts import MonitorBuiltinDefinition
    from rquant.monitor_builtin_runtime import (
        MonitorBuiltinCaptureAuthority,
        OriginalBuiltinSourceOutlet,
    )
    from rquant.price_alert_runtime_store import PriceAlertRuntimeStore
    from rquant.web.models.monitor import MonitorBuiltinTrigger


def _display_source(
    tmp_path: Path, origin: Literal["original_pulse", "original_surge"]
) -> tuple[
    ConditionAlertRuntimeStore,
    PriceAlertRuntimeStore,
    MonitorBuiltinCaptureAuthority,
    tuple[MonitorBuiltinDefinition, ...],
    OriginalBuiltinSourceOutlet,
]:
    """Use original capture validation and the original condition owner for API mapping."""
    import json
    from hashlib import sha256

    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore
    from rquant.condition_alert_runtime_contracts import verify_condition_alert_activation
    from rquant.delivery_contracts import DeliveryChannel
    from rquant.monitor_builtin_contracts import MonitorBuiltinDefinition
    from rquant.monitor_builtin_runtime import (
        bind_original_builtin_source_outlet,
        builtin_source_contract_sha256,
        verify_builtin_capture_authority,
    )
    from rquant.runtime_service_entrypoint import RuntimeServiceKind
    from tests.unit.test_condition_alert_runtime import condition_activation
    from tests.unit.test_price_alert_runtime_store import store_fixture

    tmp_path.chmod(0o700)
    condition_activation(tmp_path)
    path = tmp_path / "condition-evaluator.json"
    raw = json.loads(path.read_bytes())
    contract = builtin_source_contract_sha256()
    definition = MonitorBuiltinDefinition(
        owner_id="alice",
        builtin_id=origin.removeprefix("original_"),
        enabled=True,
        channels=(DeliveryChannel.PUSHDEER,),
        code_contract_sha256=contract,
    )
    raw["settings"]["monitor_builtin"] = {
        "enabled": True,
        "definitions": [definition.model_dump(mode="json")],
    }
    raw["settings"]["monitor_builtin_capture"] = {
        "enabled": True,
        "origin": origin,
        "capture_root": str(tmp_path / "capture"),
        "source_generation_id": "1" * 64,
        "code_contract_sha256": contract,
    }
    path.write_text(json.dumps(raw))
    actual_sha = sha256(path.read_bytes()).hexdigest()
    activation = verify_condition_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=actual_sha,
        expected_commit="a" * 40,
        expected_kind=RuntimeServiceKind.CONDITION_ALERT_RUNTIME,
    )
    authority = verify_builtin_capture_authority(
        path, runtime_root=tmp_path, expected_sha256=actual_sha, expected_commit="a" * 40
    )
    ledger, _, _ = store_fixture(tmp_path)
    owner = ConditionAlertRuntimeStore.install(ledger, activation=activation)
    return (
        owner,
        ledger,
        authority,
        (definition,),
        bind_original_builtin_source_outlet((authority,)),
    )


def _display_items(
    owner: ConditionAlertRuntimeStore,
    authority: MonitorBuiltinCaptureAuthority,
    definitions: tuple[MonitorBuiltinDefinition, ...],
    now: datetime,
    tmp_path: Path,
) -> list[MonitorBuiltinTrigger]:
    from rquant.alert_ack_read import AlertReadModel
    from rquant.condition_alert_runtime_projection import validate_monitor_runtime_projections
    from rquant.monitor_builtin_runtime import (
        builtin_serving_projections,
        commit_original_builtin_round,
        read_builtin_serving_facts,
        read_original_builtin_capture,
    )
    from rquant.web.models.alert_ack import UnacknowledgedSummary
    from rquant.web.models.monitor import MonitorBuiltinTrigger
    from rquant.web.routes.monitor import _Event, _private_item
    from tests.unit.test_notification_merge import _store

    captured = read_original_builtin_capture(authority, read_at=now)
    committed = commit_original_builtin_round(
        owner, captured=captured, definitions=definitions, evaluated_at=now
    )
    original = read_builtin_serving_facts(
        owner, captured=(captured,), observed_at=now, history_limit=100
    )
    notification = _store(tmp_path / "original-notifications.sqlite3", owner="alice")
    projections = tuple(
        row
        for row in notification.serving_snapshot(
            observed_at=now, history_limit=100
        ).payload.projections
        if not row.table_name.startswith("monitor_builtin")
    ) + builtin_serving_projections(original, observed_at=now)
    read = validate_monitor_runtime_projections({row.table_name: row for row in projections})
    assert tuple(row.event for row in read.builtin_events) == tuple(
        reversed(tuple(row.event for row in committed.events))
    )
    alerts = AlertReadModel(
        summary=UnacknowledgedSummary(), activated_at=None, events={}, acknowledgments={}
    )
    result = []
    for row in read.builtin_events:
        item = _private_item(
            _Event("builtin", row.event.event_time, 7, row.event.event_id, (row,)), alerts=alerts
        )
        assert isinstance(item, MonitorBuiltinTrigger)
        result.append(item)
    return result


def test_private_api_preserves_original_pulse_labels_values_and_distinct_units(
    tmp_path: Path,
) -> None:
    import json
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    import pandas as pd

    from rquant.pulse_watch import PulseSession
    from tests.unit.test_monitor_builtin_runtime import AT

    owner, ledger, authority, definitions, outlet = _display_source(tmp_path, "original_pulse")
    now = AT.astimezone(ZoneInfo("Asia/Shanghai"))
    codes = tuple(f"{600000 + i:06d}.SH" for i in range(4000))
    snapshot = pd.DataFrame(
        [
            {
                "ts_code": code,
                "price": 9.95,
                "high": 9.95,
                "pre_close": 10.0,
                "limit_up_price": 11.0,
                "limit_down_price": 9.0,
            }
            for code in codes
        ]
    )
    session = PulseSession(
        tmp_path / "live",
        now.date(),
        notify_fn=lambda *_args, **_facts: None,
        builtin_outlet=outlet,
        market_universe=codes,
        builtin_clock=lambda: now,
    )
    try:
        assert session.on_snapshot(snapshot, now) == []
        changed = snapshot.copy()
        changed.loc[:, ["price", "high"]] = 10.05
        changed.loc[:5, ["price", "high"]] = 11.0
        changed.loc[6:8, "high"] = 11.0
        changed.loc[9:11, "price"] = 9.0
        now += timedelta(minutes=10)
        originals = {item.kind: item for item in session.on_snapshot(changed, now)}
        assert len(originals) == 4
        items = _display_items(owner, authority, definitions, now, tmp_path)
        assert len(items) == 4
        for item in items:
            original = originals[item.source_note]
            assert item.event_label == original.kind_label
            assert (item.before, item.after) == (original.before, original.after)
            assert item.comparison_unit == ("percent" if original.kind == "ratio_jump" else "count")
            assert item.threshold_unit is None and item.threshold is None
            assert item.subject == "market" and item.code is None and item.name is None
            raw = json.loads(item.model_dump_json())
            assert (raw["before"], raw["after"]) == (original.before, original.after)
    finally:
        ledger.close()


def test_private_api_preserves_original_surge_multiplier_and_old_unit_defaults(
    tmp_path: Path,
) -> None:
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    from rquant.surge_watch import SurgeConfig, run_surge_watch
    from rquant.web.models.monitor import MonitorBuiltinTrigger
    from tests.unit.test_monitor_builtin_runtime import AT
    from tests.unit.test_surge_watch import mk_baseline, mk_minute_bars, mk_snap

    owner, ledger, authority, definitions, outlet = _display_source(tmp_path, "original_surge")
    now = AT.astimezone(ZoneInfo("Asia/Shanghai"))
    snapshot = mk_snap(
        [
            {
                "ts_code": "300001.SZ",
                "price": 100.0,
                "pre_close": 90.0,
                "pct_chg": 5.0,
                "volume": 1000.0,
                "amount": 4800.0,
            }
        ]
    )
    baseline = mk_baseline({"300001.SZ": 1000.0}, code_universe=["300001.SZ"])
    bars = mk_minute_bars(
        {now.date() - timedelta(days=day): [200.0, 200.0, 200.0] for day in (1, 2, 3, 4)}
    )
    try:
        assert (
            run_surge_watch(
                force_session=True,
                max_ticks=1,
                now_fn=lambda: now,
                sleep_fn=lambda _: None,
                snapshot_fetcher=lambda: snapshot,
                minute_fetcher=lambda *_: bars,
                baseline=baseline,
                recent_trading_days_fn=lambda _: (now.date(),),
                notify_fn=lambda *_args, **_facts: None,
                base_dir=tmp_path / "live",
                config=SurgeConfig(silent_until_hhmm="09:30"),
                builtin_outlet=outlet,
            )
            == 0
        )
        items = _display_items(owner, authority, definitions, now, tmp_path)
        assert len(items) == 1
        item = items[0]
        assert item.threshold == 8.0 and item.threshold_unit == "multiple"
        assert item.price == 100.0 and item.comparison_unit is None
        old = MonitorBuiltinTrigger.model_validate(
            item.model_dump(exclude={"threshold_unit", "comparison_unit"})
        )
        assert (
            old.threshold == item.threshold
            and old.threshold_unit is None
            and old.comparison_unit is None
        )
    finally:
        ledger.close()
def _runtime_timeline_fixture(tmp_path: Path) -> object:
    """Publish actual two-owner sealed rows beside the unchanged legacy fixture."""
    from datetime import timedelta
    from types import SimpleNamespace
    from rquant.condition_alert_runtime_projection import validate_monitor_runtime_projections
    from rquant.monitor_builtin_runtime import builtin_serving_projections, commit_original_builtin_round, read_builtin_serving_facts
    from rquant.serving_alert_projection import build_alert_read_projections
    from rquant.serving_read_models import ServingProjectionInput, ServingProjectionPayload, ServingReadModelInput
    from rquant.web.serving import GenerationTracker
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, _generation_ids, build_web_fixture
    from tests.unit.test_alert_ack_admission import _setup, _alert_projections, NOW as ACK_NOW
    from tests.unit.test_monitor_builtin_runtime import builtin_world, publish_stock_capture
    from tests.unit.test_notification_merge import _store, _route

    _, service, _ = _setup(tmp_path)
    owner_root = tmp_path / "owner"
    owner_root.mkdir(mode=0o700)
    owner, ledger, _, _, authority, definitions = builtin_world(owner_root, owners=("alice", "bob"))
    event_at = FIXTURE_BUILT_AT - timedelta(minutes=32)
    captured, _ = publish_stock_capture(authority, at=event_at)
    receipt = commit_original_builtin_round(owner, captured=captured, definitions=definitions, evaluated_at=event_at)
    read = read_builtin_serving_facts(owner, captured=(captured,), observed_at=FIXTURE_BUILT_AT, history_limit=100)
    notification = _store(tmp_path / "notifications.sqlite3", owner="alice")
    _route(notification, "600001.SH", at=FIXTURE_BUILT_AT - timedelta(seconds=40), recipient="alice")
    notification.claim_due(worker_id="original", now=FIXTURE_BUILT_AT - timedelta(seconds=40), lease_for=timedelta(seconds=30), limit=100)
    notification.claim_due(worker_id="original", now=FIXTURE_BUILT_AT - timedelta(seconds=10), lease_for=timedelta(seconds=30), limit=100)
    runtime = notification.serving_snapshot(observed_at=FIXTURE_BUILT_AT, history_limit=100).payload.projections
    runtime = tuple(item for item in runtime if not item.table_name.startswith("monitor_builtin")) + builtin_serving_projections(read, observed_at=FIXTURE_BUILT_AT)
    old = {row.table_name: row for row in _alert_projections()}
    source = ServingReadModelInput(observed_at=FIXTURE_BUILT_AT, projections=tuple(ServingProjectionInput.bind(
        row, owner_dataset_id="signals", owner_generation_id=_generation_ids("baseline", 0)["signals"]) for row in (*runtime, old["alert_ack_state"], old["alert_ack"])))
    derived = {row.table_name: row for row in build_alert_read_projections(source, signal_generation_id=_generation_ids("baseline", 0)["signals"])}
    added = tuple(row for row in derived["alert_event"].rows if row["source"] == "monitor_builtin_event")
    combined = tuple(row for name, row in old.items() if name != "alert_event") + (ServingProjectionPayload(
        table_name="alert_event", available_at=FIXTURE_BUILT_AT, rows=old["alert_event"].rows + added), *runtime)
    root = tmp_path / "runtime-serving"
    manifest = build_web_fixture(root, "baseline", sequence=0, signal_projections=combined)
    tracker = GenerationTracker(root)
    tracker.refresh()
    return SimpleNamespace(root=root, tracker=tracker, now=ACK_NOW, manifest=manifest,
        runtime=validate_monitor_runtime_projections({row.table_name: row for row in runtime}),
        owner=owner, definitions=definitions, notification=notification,
        ledger=ledger, service=service, receipt=receipt, combined=combined)


def test_monitor_web_ack_publishes_actual_owner_confirmation_and_pending_count(tmp_path: Path) -> None:
    from datetime import UTC, datetime, timedelta
    from math import ceil
    from types import SimpleNamespace
    import anyio
    from fastapi import HTTPException, Response
    from starlette.requests import Request
    from rquant.alert_ack import stable_alert_id
    from rquant.alert_ack_admission import AckAdmission
    from rquant.monitor_builtin_runtime import builtin_serving_projections, read_builtin_serving_facts
    from rquant.serving_alert_projection import build_ack_source_projections, build_alert_read_projections
    from rquant.serving_page_projection_source import _ReadonlyPageControlAuditReader
    from rquant.serving_read_models import ServingProjectionInput, ServingReadModelInput
    from rquant.web.models.alert_ack import AckCommandRequest
    import rquant.web.routes.monitor as route
    from rquant.web.settings import WebSettings
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, _generation_ids, build_web_fixture

    fixture = _runtime_timeline_fixture(tmp_path)
    admission = AckAdmission(fixture.service, fixture.root, clock=lambda: fixture.now,
        stale_after=timedelta(minutes=10))
    web = SimpleNamespace(tracker=fixture.tracker, settings=WebSettings(serving_root=fixture.root),
        clock=lambda: fixture.now, cursor_key=b"offline-original-cursor-key",
        ack_lookup=SimpleNamespace(lookup=fixture.service.lookup_ack_command),
        ack_admission=SimpleNamespace(submit=admission.admit))
    event = next(item.event for item in fixture.receipt.events if item.event.owner_id == "alice")
    body = AckCommandRequest(command_id="monitor-original-web-confirmation", requested_at=fixture.now,
        generation_id=fixture.manifest.generation_id, alert_id=stable_alert_id("monitor_builtin_event", event))

    def request_for(value: AckCommandRequest) -> Request:
        async def receive() -> dict[str, object]:
            return {"type": "http.request", "body": value.model_dump_json().encode(), "more_body": False}

        return Request({"type": "http", "app": SimpleNamespace(state=SimpleNamespace(web=web))}, receive=receive)

    try:
        foreign = body.model_copy(update={"command_id": "monitor-original-web-foreign"})
        with pytest.raises(HTTPException) as rejected:
            anyio.run(route.acknowledge_alert, request_for(foreign), foreign, "bob", None)
        assert rejected.value.status_code == 409
        assert fixture.service.outbox.receipt(foreign.command_id) is None
        accepted = anyio.run(route.acknowledge_alert, request_for(body), body, "alice", None)
        assert accepted.status == "succeeded"
        assert anyio.run(route.acknowledge_alert, request_for(body), body, "alice", None) == accepted
        original_ack = fixture.service.outbox.acknowledgment(body.alert_id)
        assert original_ack.actor_id == "alice" and original_ack.confirmation_id == accepted.confirmation_id

        # Keep the actual confirmation clock and re-read the original owners as of it.
        # Old source coverage is still incomplete; it cannot become an exact joint total.
        sequence = ceil((max(datetime.now(UTC), original_ack.confirmed_at) - FIXTURE_BUILT_AT).total_seconds() / 60)
        cutoff = FIXTURE_BUILT_AT + timedelta(minutes=sequence)
        fixture.owner.record_builtin_unavailable(definitions=fixture.definitions, origin="watchlist_quote",
            evaluated_at=cutoff, reason="capture_unavailable")
        original = read_builtin_serving_facts(fixture.owner, captured=(), observed_at=cutoff, history_limit=100)
        runtime = tuple(row for row in fixture.notification.serving_snapshot(observed_at=cutoff, history_limit=100).payload.projections
            if not row.table_name.startswith("monitor_builtin")) + builtin_serving_projections(original, observed_at=cutoff)
        authority = _ReadonlyPageControlAuditReader(fixture.service.outbox.path)
        with authority.snapshot():
            ack = authority.alert_ack_snapshot()
        assert ack.rows == (original_ack,)
        ack_sources = build_ack_source_projections(ack, observed_at=cutoff)
        inputs = ServingReadModelInput(observed_at=cutoff, projections=tuple(ServingProjectionInput.bind(row,
            owner_dataset_id="signals", owner_generation_id=_generation_ids("baseline", sequence)["signals"])
            for row in (*runtime, *ack_sources)))
        projections = build_alert_read_projections(inputs, signal_generation_id=_generation_ids("baseline", sequence)["signals"])
        build_web_fixture(fixture.root, "baseline", sequence=sequence, signal_projections=(*runtime, *ack_sources, *projections))
        fixture.tracker.refresh()
        fixture.now = cutoff
        read_request = request_for(body)
        alice = route.get_timeline(read_request, Response(), "alice", page_size=50).data
        bob = route.get_timeline(read_request, Response(), "bob", page_size=50).data
        matches = [row for row in alice.items if row.kind == "builtin" and row.acknowledgment.alert_id == body.alert_id]
        assert len(matches) == 1, alice.model_dump(mode="json")
        confirmed = matches[0]
        assert confirmed.acknowledgment.state == "confirmed"
        assert confirmed.acknowledgment.confirmed_at == original_ack.confirmed_at
        assert alice.builtin_unacknowledged.count == 6 and bob.builtin_unacknowledged.count == 7
        assert alice.total is None and alice.unacknowledged.count is None
        assert all(row.kind != "builtin" or row.acknowledgment.alert_id != body.alert_id for row in bob.items)
    finally:
        fixture.tracker.close()
        fixture.ledger.close()


def test_monitor_api_keeps_builtin_rows_private_and_binds_cursor_to_actor(tmp_path: Path) -> None:
    from types import SimpleNamespace
    from fastapi import HTTPException, Response
    from starlette.requests import Request
    import rquant.web.routes.monitor as route
    from rquant.web.settings import WebSettings

    fixture = _runtime_timeline_fixture(tmp_path)
    web = SimpleNamespace(tracker=fixture.tracker, settings=WebSettings(serving_root=fixture.root),
        clock=lambda: fixture.now, cursor_key=b"offline-original-cursor-key")
    request = Request({"type": "http", "app": SimpleNamespace(state=SimpleNamespace(web=web))})
    try:
        assert hasattr(route, "get_runtime"), "monitor API has no actual owner runtime view"
        alice = route.get_runtime(request, Response(), "alice").data
        bob = route.get_runtime(request, Response(), "bob").data
        assert len(alice.builtins) == len(bob.builtins) == 2
        assert alice.channels[0].logical_count == 1 and alice.channels[0].member_attempts == 0
        assert alice.channels[0].physical_requests == 0 and alice.channels[0].accepted_pct is None
        assert bob.channels == []
        assert "bob" not in alice.model_dump_json() and "alice" not in bob.model_dump_json()
        assert route.get_runtime(request, Response(), None).data.builtins == []
        first = route.get_timeline(request, Response(), "alice", page_size=2)
        assert first.data.next_cursor is not None
        with pytest.raises(HTTPException) as changed:
            route.get_timeline(request, Response(), "bob", page_size=2, cursor=first.data.next_cursor)
        assert changed.value.status_code == 409
        seen = []
        page = first
        while True:
            seen.extend(page.data.items)
            if page.data.next_cursor is None:
                break
            page = route.get_timeline(request, Response(), "alice", page_size=2, cursor=page.data.next_cursor)
        builtin = [row for row in seen if row.kind == "builtin"]
        assert len(builtin) == 7
        assert all(row.acknowledgment.eligible for row in builtin)
        assert len({row.event_key for row in seen}) == len(seen)
        assert [row.at for row in seen] == sorted((row.at for row in seen), reverse=True)
        assert page.data.builtin_unacknowledged.count == 7
        assert page.data.unacknowledged.count == 4
        anonymous = route.get_timeline(request, Response(), None, page_size=50).data
        assert not any(row.kind in {"builtin", "channel_attempt", "condition"} for row in anonymous.items)
        assert len(anonymous.items) == 4
    finally:
        fixture.tracker.close()
        fixture.ledger.close()


def _settings(tmp_path: Path, **extra: object) -> NotifierSettings:
    return NotifierSettings(
        signal_spool_root=tmp_path / "spool", notification_state_path=tmp_path / "notify.sqlite",
        worker_id="original", batch_limit=100, lease_seconds=30, **extra,
    )


def test_composed_factory_accepts_actual_optional_control_binding(tmp_path: Path) -> None:
    from rquant.runtime_builder_price_alert import price_alert_runtime_builder
    from tests.support.monitor_completion_fixture import build_original_monitor_control_fixture

    fixture = build_original_monitor_control_fixture(tmp_path, complete_pipeline=True)
    step = price_alert_runtime_builder(clock=lambda: fixture.now, runtime_root=tmp_path)(fixture.producer)
    try:
        result = step()
        assert "monitor_builtin:source_unknown" in result.degraded_reasons
        assert len(step.condition_store.builtin_heads()) == 4
        assert all(row.source_state == "unknown" and row.matched_count is None for row in step.condition_store.builtin_heads())
    finally:
        step.close()


def test_complete_original_pipeline_routes_four_builtins_for_two_owners_then_merges(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import timedelta
    import tests.support.monitor_completion_fixture as fixture_module
    from rquant.runtime_serving_authority import ServingSourceAuthorityReader
    from rquant.runtime_serving_snapshot import SignalDeliveryReadPayload
    from rquant.condition_alert_runtime_projection import validate_monitor_runtime_projections
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore

    failures = []
    original_commit = ConditionAlertRuntimeStore.commit_builtin_round

    def observed_commit(self: ConditionAlertRuntimeStore, **kwargs: object) -> object:
        try:
            return original_commit(self, **kwargs)
        except Exception as exc:
            failures.append(repr(exc))
            raise

    monkeypatch.setattr(ConditionAlertRuntimeStore, "commit_builtin_round", observed_commit)

    assert hasattr(fixture_module, "build_original_monitor_pipeline"), "complete original monitor factory is missing"
    pipeline = fixture_module.build_original_monitor_pipeline(tmp_path)
    try:
        first = pipeline.tick(timedelta(0))
        assert not failures, failures
        assert first[0].processed_count > 0
        reader = ServingSourceAuthorityReader(root=pipeline.authority_root, expected_producer_commit=fixture_module.COMMIT,
            expected_dataset_id="signals", expected_payload_kind="signal_delivery")
        capture = reader(pipeline.now)
        payload = SignalDeliveryReadPayload.model_validate(capture.payload)
        facts = validate_monitor_runtime_projections({row.table_name: row for row in payload.projections})
        assert len(facts.builtin_heads) == 8
        assert {row.head.builtin_id for row in facts.builtin_heads} == {"pool2_levels", "pool_attack", "surge", "pulse"}
        assert all(row.current_source_ready for row in facts.builtin_heads), [f"{row.head.owner_id}:{row.head.builtin_id}:{row.head.source_state}:{row.head.reason}" for row in facts.builtin_heads if not row.current_source_ready]
        assert {row.event.builtin_id for row in facts.builtin_events} == {"pool2_levels", "pool_attack", "surge", "pulse"}
        assert {row.owner_id for row in facts.channels} == {"alice", "bob"}
        assert all(row.member_attempts == row.physical_requests == 0 for row in facts.channels)
        pipeline.tick(timedelta(seconds=30))
        final = SignalDeliveryReadPayload.model_validate(reader(pipeline.now).payload)
        after = validate_monitor_runtime_projections({row.table_name: row for row in final.projections})
        assert all(row.member_attempts == row.logical_count > 0 for row in after.channels)
        assert all(row.physical_requests == 0 and row.possible_requests == 0 for row in after.channels)
        assert len(after.builtin_events) == len(facts.builtin_events)
        assert all(group.group.status == "shadow" for group in after.groups)
    finally:
        pipeline.close()


def test_original_single_owner_serving_api_timeline_without_ops(tmp_path: Path) -> None:
    from datetime import timedelta

    from rquant.condition_alert_runtime_projection import validate_monitor_runtime_projections
    from rquant.runtime_serving_authority import ServingSourceAuthorityReader
    from rquant.runtime_serving_snapshot import SignalDeliveryReadPayload
    from rquant.serving_alert_projection import build_alert_read_projections
    from rquant.serving_contracts import ServingDatasetWatermark
    from rquant.serving_publisher import ServingPublisher
    from rquant.serving_read_models import (
        SERVING_TABLE_SPECS,
        ServingProjectionInput,
        ServingReadModelInput,
        build_serving_read_models,
    )
    from rquant.web.serving import GenerationTracker
    from rquant.web.settings import WebSettings
    from tests.support.monitor_completion_fixture import COMMIT, build_original_monitor_pipeline
    from tests.support.web_proxy_identity import ProofTestClient, create_private_test_app

    pipeline = build_original_monitor_pipeline(tmp_path / "original-owner")
    tracker = GenerationTracker(tmp_path / "serving")
    try:
        pipeline.tick(timedelta(0))
        pipeline.tick(timedelta(seconds=30))
        captured = ServingSourceAuthorityReader(
            root=pipeline.authority_root,
            expected_producer_commit=COMMIT,
            expected_dataset_id="signals",
            expected_payload_kind="signal_delivery",
        )(pipeline.now)
        payload = SignalDeliveryReadPayload.model_validate(captured.payload)
        facts = validate_monitor_runtime_projections(
            {row.table_name: row for row in payload.projections}
        )
        bound = tuple(
            ServingProjectionInput.bind(
                row, owner_dataset_id="signals", owner_generation_id=captured.generation_id
            )
            for row in payload.projections
        )
        base = ServingReadModelInput(
            observed_at=pipeline.now,
            signals=payload.signals,
            routes=payload.routes,
            deliveries=payload.deliveries,
            projections=bound,
        )
        derived = tuple(
            ServingProjectionInput.bind(
                row, owner_dataset_id="signals", owner_generation_id=captured.generation_id
            )
            for row in build_alert_read_projections(
                base, signal_generation_id=captured.generation_id
            )
        )
        source = ServingReadModelInput(
            **base.model_dump(mode="python", exclude={"projections"}),
            projections=tuple(sorted((*bound, *derived), key=lambda row: row.table_name)),
        )
        watermark = ServingDatasetWatermark(
            dataset_id=captured.dataset_id,
            generation_id=captured.generation_id,
            sequence=captured.sequence,
            event_time=captured.event_time,
            published_at=captured.published_at,
            status=captured.status,
            reason=captured.reason,
        )
        manifest = ServingPublisher(
            tracker.root, COMMIT, table_specs=SERVING_TABLE_SPECS
        ).publish(
            build_serving_read_models(source),
            watermarks=(watermark,),
            source_generations={captured.dataset_id: captured.generation_id},
            built_at=pipeline.now,
        )
        tracker.refresh()
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            assert borrowed.cursor.execute(
                "SELECT count(*) FROM runtime_services"
            ).fetchone() == (0,)
            columns = borrowed.cursor.execute("DESCRIBE runtime_services").fetchall()
            assert next(row[1] for row in columns if row[0] == "service_id") == "INTEGER"
        app = create_private_test_app(
            WebSettings(serving_root=tracker.root),
            tracker=tracker,
            clock=lambda: pipeline.now,
            background=False,
        )
        with ProofTestClient(
            app, headers={"x-rquant-user": "alice"}, raise_server_exceptions=False
        ) as client:
            runtime = client.get("/api/v1/monitor/runtime")
            assert runtime.status_code == 200, runtime.text
            assert runtime.json()["data"]["state"] == "ready"
            assert {row["builtin_id"] for row in runtime.json()["data"]["builtins"]} == {
                "pool2_levels", "pool_attack", "surge", "pulse"
            }
            response = client.get("/api/v1/monitor/timeline", params={"page_size": 50})
            assert response.status_code == 200, response.text
            assert response.headers["X-Rquant-Generation"] == manifest.generation_id
            data = response.json()["data"]
            assert data["mode"] == "unknown" and data["mode_label"] == "未确认"
            assert data["mode_note"] == "看不到推送服务的状态，无法确认手机是否收到"
            actual = {row["event_key"] for row in data["items"] if row["kind"] == "builtin"}
            expected = {
                "builtin:" + row.event.event_id
                for row in facts.builtin_events if row.event.owner_id == "alice"
            }
            assert actual == expected and actual
            assert actual.isdisjoint(
                "builtin:" + row.event.event_id
                for row in facts.builtin_events if row.event.owner_id == "bob"
            )
    finally:
        tracker.close()
        pipeline.close()


@pytest.mark.parametrize(
    ("service_count", "status", "stale", "prefix", "expected"),
    [
        (1, "running", False, "notifier.", "live"),
        (1, "degraded", False, "notifier.", "shadow"),
        (1, "running", True, "notifier.", "unknown"),
        (1, "running", False, "strategy.", "unknown"),
        (50, "running", False, "notifier.", "live"),
        (51, "running", False, "notifier.", "unknown"),
    ],
)
def test_monitor_mode_preserves_nonempty_original_semantics_and_budget(
    service_count: int, status: str, stale: bool, prefix: str, expected: str
) -> None:
    import duckdb

    from rquant.web.routes.monitor import _mode

    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE runtime_services (service_id VARCHAR, status VARCHAR, stale BOOLEAN, "
            "consecutive_failures INTEGER, last_error VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO runtime_services VALUES (?, ?, ?, 0, NULL)",
            [(prefix + str(index), status, stale) for index in range(service_count)],
        )
        mode = _mode(connection)
        assert mode.mode == expected
        if service_count > 50:
            assert mode.note == "推送服务太多，无法确认手机是否收到"
    finally:
        connection.close()


def test_monitor_mode_does_not_cast_invalid_nonempty_service_identity() -> None:
    import duckdb

    from rquant.web.routes.monitor import _mode

    connection = duckdb.connect()
    try:
        connection.execute(
            "CREATE TABLE runtime_services AS SELECT 1 AS service_id, 'running' AS status, "
            "FALSE AS stale, 0 AS consecutive_failures, NULL::VARCHAR AS last_error"
        )
        with pytest.raises(duckdb.BinderException):
            _mode(connection)
    finally:
        connection.close()


def test_merge_settings_default_off_and_enabled_requires_installed_owner(tmp_path: Path) -> None:
    original = _settings(tmp_path)
    assert hasattr(original, "merge_enabled"), "optional merge runtime setting missing"
    assert original.merge_enabled is False
    assert original.open_store().merge_binding is None
    with pytest.raises(ValidationError):
        _settings(tmp_path, merge_enabled=True)
    with pytest.raises(ValueError, match="installed"):
        _settings(tmp_path, merge_enabled=True, merge_owner_id="admin").open_store()


def test_merge_settings_keep_original_attempt_batch_and_retry_caps(tmp_path: Path) -> None:
    for changed in ({"max_attempts": 6}, {"retry_base_seconds": 1}, {"retry_max_seconds": 301}):
        with pytest.raises(ValidationError):
            _settings(tmp_path, merge_enabled=True, merge_owner_id="admin", **changed)


def test_live_capability_comes_from_the_loaded_original_recipient_provider() -> None:
    import rquant.runtime_builder_signal as builder
    from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
    from rquant.runtime_notification_providers import (
        RecipientScopedProviderRegistry, SuppressedNotificationProvider,
        build_environment_notification_provider_loader,
    )

    assert hasattr(builder, "_loaded_notification_targets"), "notifier has no original loader capability observation"
    loaded = build_environment_notification_provider_loader(pushdeer_recipient_id="alice", environment={
        "PUSHDEER_KEYS": "synthetic-a,synthetic-b", "PUSHDEER_RECIPIENT_IDS": "alice,alice.mac",
    })()
    assert builder._loaded_notification_targets(loaded) == (
        DeliveryTarget(channel=DeliveryChannel.PUSHDEER, recipient_id="alice"),
        DeliveryTarget(channel=DeliveryChannel.PUSHDEER, recipient_id="alice.mac"),
    )
    assert builder._loaded_notification_targets(builder._shadow_providers(loaded)) is None
    assert builder._loaded_notification_targets(dict(loaded)) is None
    forged = RecipientScopedProviderRegistry(providers={DeliveryChannel.PUSHDEER: SuppressedNotificationProvider()},
        recipient_ids={DeliveryChannel.PUSHDEER: ("alice",)})
    assert builder._loaded_notification_targets(forged) is None


def test_builtin_ack_uses_actual_owner_source_and_preserves_original_uuid_recovery(tmp_path: Path) -> None:
    from datetime import timedelta
    from rquant.alert_ack import stable_alert_id
    from rquant.alert_ack_admission import AckAdmission, AckAdmissionStaleGenerationError
    from rquant.alert_ack_read import read_alert_ack
    from rquant.condition_alert_runtime_projection import read_monitor_runtime
    from rquant.monitor_builtin_runtime import builtin_serving_projections, commit_original_builtin_round, read_builtin_serving_facts
    from rquant.page_control import AckAlert, PageControlStatus
    from rquant.serving_alert_projection import build_alert_read_projections
    from rquant.serving_publisher import ServingReader
    from rquant.serving_read_models import ServingProjectionInput, ServingProjectionPayload, ServingReadModelInput
    from rquant.web.serving import BorrowedGeneration
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, _generation_ids, build_web_fixture
    from tests.unit.test_alert_ack_admission import _setup, _alert_projections, NOW as ACK_NOW
    from tests.unit.test_monitor_builtin_runtime import builtin_world, publish_stock_capture
    from tests.unit.test_notification_merge import _store

    original_root, service, old_command = _setup(tmp_path)
    original_root = tmp_path / "runtime-serving"
    owner_root = tmp_path / "owner"
    owner_root.mkdir(mode=0o700)
    owner, ledger, _, _, authority, definitions = builtin_world(owner_root, owners=("alice", "bob"))
    event_at = FIXTURE_BUILT_AT - timedelta(minutes=32)
    captured, _ = publish_stock_capture(authority, at=event_at)
    receipt = commit_original_builtin_round(owner, captured=captured, definitions=definitions, evaluated_at=event_at)
    event = next(row.event for row in receipt.events if row.event.owner_id == "alice")
    # Each fixture input is a real new original owner commit at this explicit synthetic clock.
    alert_id = stable_alert_id("monitor_builtin_event", event)
    read = read_builtin_serving_facts(owner, captured=(captured,), observed_at=FIXTURE_BUILT_AT, history_limit=100)
    runtime = _store(tmp_path / "notifications.sqlite3").serving_snapshot(observed_at=FIXTURE_BUILT_AT, history_limit=100).payload.projections
    runtime = tuple(item for item in runtime if not item.table_name.startswith("monitor_builtin")) + builtin_serving_projections(read, observed_at=FIXTURE_BUILT_AT)
    old = {row.table_name: row for row in _alert_projections()}
    generation = _generation_ids("baseline", 0)["signals"]
    source = ServingReadModelInput(observed_at=FIXTURE_BUILT_AT, projections=tuple(ServingProjectionInput.bind(
        row, owner_dataset_id="signals", owner_generation_id=generation) for row in (*runtime, old["alert_ack_state"], old["alert_ack"])))
    derived = {row.table_name: row for row in build_alert_read_projections(source, signal_generation_id=generation)}
    added = tuple(row for row in derived["alert_event"].rows if row["source"] == "monitor_builtin_event")
    assert len(added) == 14
    combined = tuple(row for name, row in old.items() if name != "alert_event") + (
        ServingProjectionPayload(table_name="alert_event", available_at=FIXTURE_BUILT_AT, rows=old["alert_event"].rows + added), *runtime)
    manifest = build_web_fixture(original_root, "baseline", sequence=0, signal_projections=combined)
    with ServingReader(original_root).acquire_generation() as lease:
        cursor = lease.connection.cursor()
        try:
            borrowed = BorrowedGeneration(lease.manifest, lease.pointer, cursor, None)
            assert cursor.execute("SELECT table_name,available,row_count FROM projection_status WHERE table_name LIKE 'monitor_builtin_%' OR table_name LIKE 'notification_runtime_%' ORDER BY table_name").fetchall() == [("monitor_builtin_event", True, 14), ("monitor_builtin_state", True, 5), ("notification_runtime_delivery", True, 0), ("notification_runtime_state", True, 1)]
            assert read_monitor_runtime(borrowed, now=ACK_NOW).builtin_window.history_count == 14
            alice = read_alert_ack(borrowed, serving_ready=True, now=ACK_NOW, stale_after=timedelta(minutes=10), actor_id="alice")
            anonymous = read_alert_ack(borrowed, serving_ready=True, now=ACK_NOW, stale_after=timedelta(minutes=10))
        finally:
            cursor.close()
    assert alice.summary.state == "ready" and alice.summary.count == anonymous.summary.count == 4
    assert alice.is_eligible("monitor_builtin_event", alert_id)
    assert all(row.source != "monitor_builtin_event" or row.alert_id in {stable_alert_id("monitor_builtin_event", item.event) for item in receipt.events if item.event.owner_id == "alice"} for row in alice.events.values())
    assert not anonymous.is_eligible("monitor_builtin_event", alert_id)
    admission = AckAdmission(service, original_root, clock=lambda: ACK_NOW, stale_after=timedelta(minutes=10))
    command = AckAlert(command_id="builtin-owner-confirmation", requested_at=ACK_NOW, generation_id=manifest.generation_id, alert_id=alert_id, actor_id="alice")
    with pytest.raises(ValueError, match="eligible"):
        admission.admit(command.model_copy(update={"actor_id": "bob", "command_id": "foreign-owner"}))
    assert service.outbox.receipt("foreign-owner") is None
    with pytest.raises(AckAdmissionStaleGenerationError):
        admission.admit(command.model_copy(update={"generation_id": "changed", "command_id": "other-generation"}))
    accepted = admission.admit(command)
    assert accepted.status is PageControlStatus.SUCCEEDED
    assert admission.admit(command) == accepted and service.lookup_ack_command(command) == accepted
    assert admission.admit(old_command.model_copy(update={"generation_id": manifest.generation_id})).status is PageControlStatus.SUCCEEDED
    ledger.close()


def test_original_watchlist_quote_factory_captures_values_and_replaces_disconnect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import json
    import os
    import shutil
    from datetime import timedelta
    from hashlib import sha256

    import pandas as pd

    from rquant.monitor_builtin_runtime import builtin_source_contract_sha256, read_original_builtin_capture, require_builtin_captured_read, verify_builtin_capture_authority
    from rquant.replica_generation import capture_database_watermark, replica_generation_path, write_replica_generation_metadata
    from rquant.runtime_service_builtin import watchlist_quote_source_builder
    from rquant.runtime_service_control import RuntimeServicePlane
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_monitor_builtin_runtime import AT
    from tests.unit.test_runtime_market_session import _write_authority

    tmp_path.chmod(0o700)
    primary, replica = tmp_path / "primary.duckdb", tmp_path / "replica.duckdb"
    with DuckDBStore(primary) as original:
        original.upsert_pool2_watch(pd.DataFrame([{"ts_code": "600000.SH", "entry_date": AT.date(),
            "limit_up_date": AT.date() - timedelta(days=1), "body_upper": 11.0, "body_lower": 9.0,
            "level_40": 9.8, "level_30": 9.6, "level_20": 9.4, "stop_strong": 9.0, "stop_weak": 8.8, "status": "active"}]))
    shutil.copy2(primary, replica)
    os.utime(replica, (AT.timestamp() - 1, AT.timestamp() - 1))
    write_replica_generation_metadata(primary_path=primary, replica_path=replica,
        output_path=replica_generation_path(replica), source_before=capture_database_watermark(primary))
    # These are explicit synthetic artifact clocks, not claimed historical quote timestamps.
    os.utime(replica_generation_path(replica), (AT.timestamp() - 1, AT.timestamp() - 1))
    calendar = _write_authority(tmp_path / "calendar.json")
    from rquant.live_contracts import LiveChannel
    from rquant.live_spool import LiveBatchSpool
    spool = LiveBatchSpool(tmp_path / "quotes")
    source_generation = spool._source_generation(LiveChannel.WATCHLIST_QUOTE)
    capture_root, manifest_path = tmp_path / "capture", tmp_path / "quote-source.json"
    clock = [AT]
    disconnected = [False]
    calls = []

    def provider(codes: tuple[str, ...], *, timeout_seconds: float, on_started: object) -> pd.DataFrame:
        calls.append(codes)
        on_started(clock[0])
        if disconnected[0]:
            raise ConnectionError("synthetic source unavailable")
        return pd.DataFrame([{"ts_code": code, "price": 9.1, "low": 8.9, "open": 9.5, "high": 9.6,
            "pre_close": 10.0, "pct_chg": -9.0, "volume": 1000.0, "amount": 9100.0,
            "source_observed_at": clock[0]} for code in codes])

    manifest = RuntimeServiceManifest(service_id="builtin-quotes", service_kind=RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        plane=RuntimeServicePlane.LIVE, producer_commit="a" * 40, interval_seconds=5, stale_after_seconds=15,
        settings={"spool_root": str(spool.root), "quota_path": str(tmp_path / "quota.sqlite"), "quota_units_per_window": 20,
            "producer_version": "fixture", "schema_version": 3, "units_contract_id": "6" * 64, "volume_unit": "shares", "amount_unit": "CNY",
            "rollout_mode": "published", "domain_mode": "builtin_watchlist", "calendar_path": str(calendar),
            "calendar_expected_commit": "a" * 40, "calendar_content_sha256": json.loads(calendar.read_bytes())["content_sha256"],
            "builtin_primary_path": str(primary), "builtin_replica_path": str(replica), "builtin_request_root": str(tmp_path / "requests"),
            "monitor_builtin_capture_manifest_path": str(manifest_path),
            "monitor_builtin_capture": {"enabled": True, "origin": "watchlist_quote", "capture_root": str(capture_root),
                "source_generation_id": source_generation, "code_contract_sha256": builtin_source_contract_sha256()}})
    manifest_path.write_text(json.dumps(manifest.model_dump(mode="json")))
    manifest_path.chmod(0o600)
    import rquant.monitor_builtin_runtime as builtin_runtime
    original_status = builtin_runtime.publish_original_builtin_status

    def retain_actual_status(authority: object, **facts: object) -> Path:
        if not disconnected[0] and facts["reason"] == "source_unavailable":
            raise AssertionError(facts["receipt_json"])
        return original_status(authority, **facts)

    monkeypatch.setattr(builtin_runtime, "publish_original_builtin_status", retain_actual_status)
    step = watchlist_quote_source_builder(provider_factory=lambda: provider, universe_loader=None,
        clock=lambda: clock[0], runtime_root=tmp_path)(manifest)
    authority = verify_builtin_capture_authority(manifest_path, runtime_root=tmp_path,
        expected_sha256=sha256(manifest_path.read_bytes()).hexdigest(), expected_commit="a" * 40)
    first = step()
    original_read = read_original_builtin_capture(authority, read_at=clock[0])
    capture = require_builtin_captured_read(original_read).capture
    assert first.batch_published is True and calls == [("600000.SH",)]
    assert capture.source_state == "ready" and capture.quotes[0].price == 9.1
    assert capture.stock_watch[0].level_40 == 9.8
    assert json.loads(capture.original_source_receipt_json)["quote_read"]["request_json"].find("monitor-watchlist-quote-request/v1") >= 0
    disconnected[0], clock[0] = True, AT + timedelta(seconds=5)
    failed = step()
    latest = require_builtin_captured_read(read_original_builtin_capture(authority, read_at=clock[0])).capture
    # The original gateway published a real STALE record; this is not a usable quote batch.
    assert failed.degraded_reasons and latest.source_state == "disconnected"
    assert latest.quotes == () and latest.original_results == ()
    with pytest.raises(ValueError, match="changed"):
        require_builtin_captured_read(original_read)


def test_original_cli_capture_binding_is_optional_exact_and_checked_before_loop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from argparse import Namespace
    from hashlib import sha256

    import rquant.cli as cli
    import rquant.monitor as monitor
    from rquant.monitor_builtin_runtime import MonitorBuiltinCaptureReference, OriginalBuiltinOutletSettings
    from tests.unit.test_monitor_builtin_runtime import builtin_world

    _, ledger, _, _, _, _ = builtin_world(tmp_path)
    path = tmp_path / "condition-evaluator.json"
    reference = MonitorBuiltinCaptureReference(origin="original_monitor", manifest_path=path, runtime_root=tmp_path,
        manifest_sha256=sha256(path.read_bytes()).hexdigest(), producer_commit="a" * 40)
    config = OriginalBuiltinOutletSettings(sources=(reference,))
    binding = tmp_path / "outlet.json"
    binding.write_bytes(config.wire_bytes())
    binding.chmod(0o600)
    calls = []
    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    monkeypatch.setattr(monitor, "run_monitor", lambda **facts: calls.append(facts) or 0)
    assert cli.cmd_monitor(Namespace(interval=5, monitor_builtin_binding=None)) == 0
    assert calls == [{"interval": 5}]
    assert cli.cmd_monitor(Namespace(interval=5, monitor_builtin_binding=binding)) == 0
    assert calls[-1]["builtin_outlet"].captures("original_monitor") is True
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="changed"):
        cli.cmd_monitor(Namespace(interval=5, monitor_builtin_binding=binding))
    assert len(calls) == 2
    ledger.close()
