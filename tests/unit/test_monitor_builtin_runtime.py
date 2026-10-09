"""The four original detectors share the installed condition event ledger."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path

import pytest


def watch_item(pool: str = "pool2") -> object:
    from rquant.monitor import WatchItem

    return WatchItem(ts_code="600000.SH", pool=pool, limit_up_date=date(2026, 7, 30),
        body_upper=11.0, body_lower=9.0, body=2.0, level_40=10.2, level_30=10.4,
        level_20=10.6, stop_strong=10.8, stop_weak=9.0, name="样本", entry_date=date(2026, 7, 30),
        reference_date=date(2026, 7, 30), t_high=11.0, t_close=10.0, limit_up_price_next=11.0)


def test_original_five_level_le_order_and_realtime_before_daily_low() -> None:
    from rquant.monitor import check_levels

    item = watch_item()
    events = check_levels(item, 9.0, 8.0)
    assert [event["level"] for event in events] == ["40", "30", "20", "strong", "weak"]
    assert [event["trigger_price"] for event in events] == [9.0] * 5
    assert {event["trigger_type"] for event in events} == {"realtime"}
    assert check_levels(item, 8.0, 7.0) == []
    fresh = watch_item()
    low = check_levels(fresh, 12.0, 9.0)
    assert [event["level"] for event in low] == ["40", "30", "20", "strong", "weak"]
    assert {event["trigger_type"] for event in low} == {"daily_low"}


@pytest.mark.parametrize("pool", ["pool1", "pool2"])
def test_original_attack_preserves_both_pools(pool: str) -> None:
    from rquant.monitor import RealtimeQuote, check_attack_signals

    item = watch_item(pool)
    quote = RealtimeQuote(ts_code=item.ts_code, price=11.1, low=10.0, high=11.2, open=10.3, pre_close=10.0)
    actual = check_attack_signals(item, quote)
    assert [item["level"] for item in actual] == ["attack_open_strength", "attack_break_high", "attack_strong_carry", "attack_near_limit"]
    assert check_attack_signals(item, quote) == []


def test_builtin_material_cannot_be_admitted_from_declared_owner_or_numbers(tmp_path: Path) -> None:
    from rquant.monitor_builtin_runtime import MonitorBuiltinCapturedRead

    with pytest.raises(TypeError):
        MonitorBuiltinCapturedRead()


def test_builtin_serving_keeps_original_history_and_exact_same_read_rows(tmp_path: Path) -> None:
    from rquant.monitor_builtin_runtime import (
        builtin_serving_projections, commit_original_builtin_round, read_builtin_serving_facts,
        require_builtin_serving_read,
    )

    store, ledger, _, _, capture, definitions = builtin_world(tmp_path)
    original, _ = publish_stock_capture(capture)
    receipt = commit_original_builtin_round(store, captured=original, definitions=definitions, evaluated_at=AT)
    read = read_builtin_serving_facts(store, captured=(original,), observed_at=AT, history_limit=3)
    facts = require_builtin_serving_read(read)
    assert facts.window.history_count == 7 and len(facts.events) == 3 and facts.window.truncated
    assert tuple(row.event for row in facts.events) == tuple(row.event for row in reversed(receipt.events[-3:]))
    assert all(head.current_source_ready for head in facts.heads)
    tables = builtin_serving_projections(read, observed_at=AT)
    assert len(tables[0].rows) == 3 and len(tables[1].rows) == 3
    with pytest.raises(TypeError, match="capability"):
        builtin_serving_projections(facts, observed_at=AT)
    expired = read_builtin_serving_facts(store, captured=(original,), observed_at=AT + timedelta(minutes=5), history_limit=10)
    history = require_builtin_serving_read(expired)
    assert history.window.history_count == 7 and len(history.events) == 7
    assert not any(head.current_source_ready for head in history.heads)
    assert all(head.source_valid_until == AT + timedelta(seconds=15) for head in history.heads)
    ledger.close()


def test_builtin_serving_rejects_changed_original_event_after_capture(tmp_path: Path) -> None:
    from rquant.monitor_builtin_runtime import commit_original_builtin_round, read_builtin_serving_facts, require_builtin_serving_read

    store, ledger, _, _, capture, definitions = builtin_world(tmp_path)
    original, _ = publish_stock_capture(capture)
    commit_original_builtin_round(store, captured=original, definitions=definitions, evaluated_at=AT)
    read = read_builtin_serving_facts(store, captured=(original,), observed_at=AT, history_limit=10)
    with ledger._connection(write=True) as connection:
        connection.execute("UPDATE condition_alert_event_log SET payload_sha256=? WHERE sequence=1", ("e" * 64,))
    with pytest.raises(ValueError, match="changed|prefix|source"):
        require_builtin_serving_read(read)
    ledger.close()


AT = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)


def builtin_world(tmp_path: Path, *, owners: tuple[str, ...] = ("alice",)):
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore
    from rquant.condition_alert_runtime_contracts import verify_condition_alert_activation
    from rquant.monitor_builtin_contracts import MonitorBuiltinDefinition
    from rquant.delivery_contracts import DeliveryChannel
    from rquant.monitor_builtin_runtime import builtin_source_contract_sha256, verify_builtin_capture_authority
    from rquant.runtime_service_entrypoint import RuntimeServiceKind
    from tests.unit.test_condition_alert_runtime import condition_activation
    from tests.unit.test_price_alert_runtime_store import store_fixture

    tmp_path.chmod(0o700)
    condition_activation(tmp_path)
    path = tmp_path / "condition-evaluator.json"
    raw = json.loads(path.read_bytes())
    contract = builtin_source_contract_sha256()
    definitions = tuple(MonitorBuiltinDefinition(owner_id=owner, builtin_id=kind, enabled=True,
        channels=(DeliveryChannel.PUSHDEER,), code_contract_sha256=contract) for owner in owners for kind in ("pool2_levels", "pool_attack"))
    raw["settings"]["monitor_builtin"] = {"enabled": True, "definitions": [item.model_dump(mode="json") for item in definitions]}
    raw["settings"]["monitor_builtin_capture"] = {"enabled": True, "origin": "original_monitor", "capture_root": str(tmp_path / "capture"),
        "source_generation_id": "1" * 64, "code_contract_sha256": contract}
    path.write_text(json.dumps(raw))
    activation = verify_condition_alert_activation(path, runtime_root=tmp_path, expected_manifest_sha256=sha256(path.read_bytes()).hexdigest(),
        expected_commit="a" * 40, expected_kind=RuntimeServiceKind.CONDITION_ALERT_RUNTIME)
    capture = verify_builtin_capture_authority(path, runtime_root=tmp_path, expected_sha256=sha256(path.read_bytes()).hexdigest(), expected_commit="a" * 40)
    ledger, price_activation, _ = store_fixture(tmp_path)
    store = ConditionAlertRuntimeStore.install(ledger, activation=activation)
    return store, ledger, activation, price_activation, capture, definitions


def publish_stock_capture(authority: object, *, at: datetime = AT, sequence: int = 1, state: str = "ready"):
    from rquant.monitor import RealtimeQuote
    from rquant.monitor_builtin_runtime import OriginalMonitorFetchReceipt, bind_original_builtin_source_outlet, require_builtin_capture_authority, read_original_builtin_capture

    _, settings = require_builtin_capture_authority(authority)
    outlet = bind_original_builtin_source_outlet((authority,))
    item = watch_item()
    quote = RealtimeQuote(ts_code=item.ts_code, price=9.0, low=8.0, open=10.3, high=11.2, pre_close=10.0,
        pct_chg=-10.0, volume=1000.0, amount=9000.0, source="fixture-original-capture")
    outlet.monitor_snapshot(watchlist=(item,), quotes=(quote,) if state == "ready" else (),
        fetch=OriginalMonitorFetchReceipt(requested_at=at, response_received_at=at, tushare_cache_at=None, fallback_codes=(item.ts_code,)))
    path = settings.capture_root / "original_monitor.json"
    assert json.loads(path.read_bytes())["source_sequence"] == sequence
    return read_original_builtin_capture(authority, read_at=at), path


def test_builtin_original_same_read_values_and_daily_dedupe_survive_reopen(tmp_path: Path) -> None:
    from rquant.monitor_builtin_runtime import commit_original_builtin_round, require_builtin_captured_read
    from rquant.condition_alert_runtime import ConditionAlertRuntimeStore
    from rquant.price_alert_runtime_store import PriceAlertRuntimeStore

    store, ledger, activation, price_activation, capture, definitions = builtin_world(tmp_path)
    read, _ = publish_stock_capture(capture)
    material = require_builtin_captured_read(read)
    receipt = commit_original_builtin_round(store, captured=read, definitions=definitions, evaluated_at=AT)
    assert [item.event.detection.kind for item in receipt.events] == ["40", "30", "20", "strong", "weak", "attack_open_strength", "attack_break_high"]
    assert all(item.event.material_sha256 == material.sha256 for item in receipt.events)
    assert store.events_after(0, inspected_at=AT) == receipt.events
    assert commit_original_builtin_round(store, captured=read, definitions=definitions, evaluated_at=AT) == receipt
    path = ledger.path
    ledger.close()
    reopened = PriceAlertRuntimeStore(path, activation=price_activation)
    owner = ConditionAlertRuntimeStore(reopened, activation=activation)
    read2, _ = publish_stock_capture(capture, at=AT + timedelta(seconds=5), sequence=2)
    again = commit_original_builtin_round(owner, captured=read2, definitions=definitions, evaluated_at=AT + timedelta(seconds=5))
    assert again.events == () and again.suppressed_count == 7
    assert owner.source_descriptor().high_watermark == 7
    reopened.close()


@pytest.mark.parametrize("point", ["builtin_dedupe", "builtin_event", "builtin_receipt", "before_commit"])
def test_builtin_original_atomic_commit_and_current_capture_fence(tmp_path: Path, point: str) -> None:
    from rquant.monitor_builtin_runtime import commit_original_builtin_round

    store, ledger, _, _, capture, definitions = builtin_world(tmp_path)
    read, _ = publish_stock_capture(capture)
    store.failpoint = lambda name: (_ for _ in ()).throw(RuntimeError("interrupted")) if name == point else None
    with pytest.raises(RuntimeError, match="interrupted"):
        commit_original_builtin_round(store, captured=read, definitions=definitions, evaluated_at=AT)
    assert store.source_descriptor().high_watermark == 0
    store.failpoint = lambda _: None
    with pytest.raises(ValueError, match="changed"):
        commit_original_builtin_round(store, captured=read, definitions=definitions, evaluated_at=AT, current_scope=lambda: False)
    assert store.source_descriptor().high_watermark == 0
    assert len(commit_original_builtin_round(store, captured=read, definitions=definitions, evaluated_at=AT).events) == 7
    ledger.close()


def test_builtin_capture_number_tamper_wrong_install_and_source_change_are_rejected(tmp_path: Path) -> None:
    from rquant.monitor_builtin_runtime import commit_original_builtin_round, require_builtin_captured_read
    from rquant.monitor_builtin_contracts import MonitorBuiltinCaptureRecord
    from rquant.strict_json import canonical_json_bytes

    store, ledger, _, _, capture, definitions = builtin_world(tmp_path)
    read, path = publish_stock_capture(capture)
    raw = json.loads(path.read_bytes())
    raw["quotes"][0]["price"] = 999.0
    with pytest.raises(ValueError, match="numbers"):
        MonitorBuiltinCaptureRecord.model_validate_json(canonical_json_bytes(raw))
    with pytest.raises(ValueError, match="installation"):
        commit_original_builtin_round(store, captured=read, definitions=tuple(item.model_copy(update={"owner_id": "viewer"}) for item in definitions), evaluated_at=AT)
    publish_stock_capture(capture, at=AT + timedelta(seconds=5), sequence=2, state="disconnected")
    with pytest.raises(ValueError, match="changed"):
        require_builtin_captured_read(read)
    ledger.close()


def test_builtin_v6_uses_original_route_outbox_and_cannot_pass_old_v5_codec(tmp_path: Path) -> None:
    from rquant.monitor_builtin_runtime import commit_original_builtin_round
    from rquant.signal_route_spool import BuiltinConditionAlertRouteSpoolRecord, ConditionAlertRouteSpoolRecord, _decode_notification_spool_record
    from tests.unit.test_condition_alert_route import condition_route_fixture

    store, ledger, _, _, capture, definitions = builtin_world(tmp_path)
    read, _ = publish_stock_capture(capture)
    committed = commit_original_builtin_round(store, captured=read, definitions=definitions, evaluated_at=AT)
    source = store.source_descriptor()
    bus, activation, policy, _, _ = condition_route_fixture(tmp_path, produced_source=source)
    routed = bus.commit_condition_alert_route(activation=activation, policy=policy, source=source,
        record=committed.events[0], source_inspected_at=AT, routed_at=AT)
    assert bus.notification_event(routed.event_id).event == committed.events[0].event
    assert len(bus.outbox_records()) == 1
    assert bus.claim_due("generic", now=AT, lease_for=timedelta(seconds=10), limit=100) == ()
    entry = BuiltinConditionAlertRouteSpoolRecord.create(record=routed, previous_record_hash=None)
    assert entry.schema_version == 6
    assert _decode_notification_spool_record(entry.wire_bytes(), sequence=1) == entry
    with pytest.raises(ValueError, match="codec"):
        ConditionAlertRouteSpoolRecord.create(record=routed, previous_record_hash=None)
    ledger.close()


def test_original_quote_optional_complete_read_has_same_values_and_fences(tmp_path: Path) -> None:
    from rquant.price_alert_runtime_source import PriceQuoteOwnedRead, read_latest_price_quote_snapshot, require_price_quote_owned_read
    from tests.unit.test_price_alert_runtime_source import quote_fixture, AT as QUOTE_AT

    with pytest.raises(TypeError):
        PriceQuoteOwnedRead()
    spool, binding, root = quote_fixture(tmp_path)
    reads = []
    plain = read_latest_price_quote_snapshot(spool, request_root=root, binding=binding, evaluated_at=QUOTE_AT, expected_producer_commit="b" * 40)
    observed = read_latest_price_quote_snapshot(spool, request_root=root, binding=binding, evaluated_at=QUOTE_AT,
        expected_producer_commit="b" * 40, owned_read_observer=reads.append)
    assert plain == observed and len(reads) == 1
    complete = require_price_quote_owned_read(reads[0])
    assert complete.snapshot == plain
    assert json.loads(complete.original_rows_json)[0]["price"] == 10.125
    assert json.loads(complete.original_rows_json)[0]["high"] == 10.2
    pointer = spool._current_path(__import__("rquant.live_contracts", fromlist=["LiveChannel"]).LiveChannel.WATCHLIST_QUOTE)
    pointer.write_bytes(pointer.read_bytes() + b" ")
    with pytest.raises(ValueError, match="changed"):
        require_price_quote_owned_read(reads[0])


def test_builtin_quote_request_is_explicit_and_cannot_relabel_original_price_scope(tmp_path: Path) -> None:
    from rquant.price_alert_runtime_source import BuiltinQuoteRequestBinding, freeze_builtin_quote_request, freeze_price_quote_request

    tmp_path.chmod(0o700)
    request = BuiltinQuoteRequestBinding.create(source="fixture", quote_source_generation_id="1" * 64,
        scope_generation_id="2" * 64, scope_manifest_sha256="3" * 64, watch_basis_sha256="4" * 64,
        codes=("600000.SH",), scheduled_at=AT, universe_as_of=AT, trade_date=AT.date(), schema_version=3)
    root = tmp_path / "requests"
    root.mkdir(mode=0o700)
    path = freeze_builtin_quote_request(root, request)
    assert json.loads(path.read_bytes())["binding_schema"] == "monitor-watchlist-quote-request/v1"
    assert json.loads(path.read_bytes())["scope_kind"] == "original_pool_watchlist"
    with pytest.raises(TypeError):
        freeze_price_quote_request(root, request)


def test_builtin_missing_current_source_clears_counts_without_replaying_old_events(tmp_path: Path) -> None:
    from rquant.monitor_builtin_runtime import commit_original_builtin_round

    store, ledger, _, _, capture, definitions = builtin_world(tmp_path)
    read, _ = publish_stock_capture(capture)
    receipt = commit_original_builtin_round(store, captured=read, definitions=definitions, evaluated_at=AT)
    failed = store.record_builtin_unavailable(definitions=definitions, origin="original_monitor",
        evaluated_at=AT + timedelta(seconds=5), reason="capture_unavailable")
    assert failed.events == () and failed.input.material is None
    assert failed.source_high_watermark == receipt.source_high_watermark
    heads = store.builtin_heads()
    assert all(item.source_state == "unknown" and item.matched_count is None and item.material_sha256 is None for item in heads)
    assert all(item.last_triggered_at == AT for item in heads)
    assert store.events_after(0, inspected_at=AT + timedelta(seconds=5)) == receipt.events
    ledger.close()


def test_pulse_same_original_session_publishes_market_point_and_exact_alerts(tmp_path: Path) -> None:
    import pandas as pd
    from zoneinfo import ZoneInfo

    from rquant.monitor_builtin_runtime import bind_original_builtin_source_outlet, builtin_source_contract_sha256, read_original_builtin_capture, require_builtin_captured_read, verify_builtin_capture_authority
    from rquant.pulse_watch import PulseSession
    from tests.unit.test_condition_alert_runtime import condition_activation

    tmp_path.chmod(0o700)
    condition_activation(tmp_path)
    path = tmp_path / "condition-evaluator.json"
    manifest = json.loads(path.read_bytes())
    manifest["settings"]["monitor_builtin_capture"] = {"enabled": True, "origin": "original_pulse", "capture_root": str(tmp_path / "capture"),
        "source_generation_id": "1" * 64, "code_contract_sha256": builtin_source_contract_sha256()}
    path.write_text(json.dumps(manifest))
    authority = verify_builtin_capture_authority(path, runtime_root=tmp_path, expected_sha256=sha256(path.read_bytes()).hexdigest(), expected_commit="a" * 40)
    outlet = bind_original_builtin_source_outlet((authority,))
    live = tmp_path / "live"
    live.mkdir(mode=0o700)
    codes = tuple(f"{600000 + i:06d}.SH" for i in range(4000))
    now = AT.astimezone(ZoneInfo("Asia/Shanghai"))
    snapshot = pd.DataFrame([{"ts_code": code, "price": 10.0, "high": 10.0, "pre_close": 10.0,
        "limit_up_price": 11.0, "limit_down_price": 9.0} for code in codes])
    notifications = []
    session = PulseSession(live, now.date(), notify_fn=lambda *args, **facts: notifications.append((args, facts)),
        builtin_outlet=outlet, market_universe=codes, builtin_clock=lambda: now)
    assert session.on_snapshot(snapshot, now) == []
    changed = snapshot.copy()
    changed.loc[:5, ["price", "high"]] = 11.0
    now += timedelta(minutes=10)
    alerts = session.on_snapshot(changed, now)
    record = require_builtin_captured_read(read_original_builtin_capture(authority, read_at=now)).capture
    assert len(alerts) == 1 and record.origin == "original_pulse" and record.source_state == "ready"
    assert record.stock_watch == () and record.quotes == ()
    assert record.original_results[0].subject == "market"
    assert record.original_results[0].before == alerts[0].before == 0.0
    assert record.original_results[0].after == alerts[0].after == 6.0
    assert json.loads(record.pulse_point_json)["limit_up"] == 6
    assert json.loads(record.original_results[0].original_result_json) == alerts[0].model_dump(mode="json")
    assert notifications == []
    from rquant.monitor_builtin_contracts import MonitorBuiltinCaptureRecord
    from rquant.strict_json import canonical_json_bytes

    changed_record = record.model_dump(mode="json")
    changed_point = json.loads(changed_record["pulse_point_json"])
    changed_point["limit_up"] = 999
    changed_record["pulse_point_json"] = canonical_json_bytes(changed_point).decode()
    with pytest.raises(ValueError, match="Pulse values"):
        MonitorBuiltinCaptureRecord.model_validate_json(canonical_json_bytes(changed_record))
    now += timedelta(minutes=1)
    assert session.on_snapshot(pd.DataFrame(), now) == []
    latest = require_builtin_captured_read(read_original_builtin_capture(authority, read_at=now)).capture
    assert latest.source_state == "disconnected" and latest.original_results == () and latest.raw_payload_sha256 is None


def test_surge_original_loop_capture_keeps_confirmed_math_and_used_basis(tmp_path: Path) -> None:
    from zoneinfo import ZoneInfo

    from rquant.monitor_builtin_runtime import bind_original_builtin_source_outlet, builtin_source_contract_sha256, read_original_builtin_capture, require_builtin_captured_read, verify_builtin_capture_authority
    from rquant.surge_watch import SurgeConfig, run_surge_watch
    from tests.unit.test_condition_alert_runtime import condition_activation
    from tests.unit.test_surge_watch import mk_baseline, mk_minute_bars, mk_snap

    tmp_path.chmod(0o700)
    condition_activation(tmp_path)
    path = tmp_path / "condition-evaluator.json"
    manifest = json.loads(path.read_bytes())
    manifest["settings"]["monitor_builtin_capture"] = {"enabled": True, "origin": "original_surge", "capture_root": str(tmp_path / "capture"),
        "source_generation_id": "1" * 64, "code_contract_sha256": builtin_source_contract_sha256()}
    path.write_text(json.dumps(manifest))
    authority = verify_builtin_capture_authority(path, runtime_root=tmp_path, expected_sha256=sha256(path.read_bytes()).hexdigest(), expected_commit="a" * 40)
    outlet = bind_original_builtin_source_outlet((authority,))
    now = AT.astimezone(ZoneInfo("Asia/Shanghai"))
    snapshot = mk_snap([{"ts_code": "300001.SZ", "price": 100.0, "pre_close": 90.0, "pct_chg": 5.0, "volume": 1000.0, "amount": 4800.0}])
    baseline = mk_baseline({"300001.SZ": 1000.0}, code_universe=["300001.SZ"])
    bars = mk_minute_bars({now.date() - timedelta(days=day): [200.0, 200.0, 200.0] for day in (1, 2, 3, 4)})
    calls = []
    rc = run_surge_watch(force_session=True, max_ticks=1, now_fn=lambda: now, sleep_fn=lambda _: None,
        snapshot_fetcher=lambda: snapshot, minute_fetcher=lambda *_: bars, baseline=baseline,
        recent_trading_days_fn=lambda _: (now.date(),), notify_fn=lambda *args, **facts: calls.append((args, facts)),
        base_dir=tmp_path / "live", config=SurgeConfig(silent_until_hhmm="09:30"), builtin_outlet=outlet)
    capture = require_builtin_captured_read(read_original_builtin_capture(authority, read_at=now)).capture
    assert rc == 0 and capture.source_state == "ready" and len(capture.original_results) == 1
    result = capture.original_results[0]
    assert result.ts_code == "300001.SZ" and result.trigger_price == 100.0 and result.threshold == 8.0
    basis = json.loads(capture.original_basis_json)
    assert basis["confirm_cache"]["300001.SZ"]["days_used"] == 4
    assert basis["config"] == SurgeConfig(silent_until_hhmm="09:30").model_dump(mode="json")
    assert json.loads(result.original_result_json)["price_source"] == "snapshot"
    assert not any(args and args[0] == "surge_watch" for args, _ in calls)


def test_missing_surge_universe_does_not_commit_event_or_consume_daily_dedupe(
    tmp_path: Path,
) -> None:
    from zoneinfo import ZoneInfo

    from rquant.surge_watch import SurgeConfig, run_surge_watch
    from tests.unit.test_surge_watch import mk_baseline, mk_minute_bars, mk_snap

    from rquant.monitor_builtin_runtime import (
        MonitorBuiltinCapturedRead,
        commit_original_builtin_round,
        read_original_builtin_capture,
        require_builtin_captured_read,
    )
    from tests.unit.test_monitor_completion_wiring import _display_source

    root = tmp_path / "original-owner"
    root.mkdir(mode=0o700)
    owner, ledger, authority, definitions, outlet = _display_source(root, "original_surge")
    now = AT.astimezone(ZoneInfo("Asia/Shanghai"))
    codes = ("300001.SZ", "300002.SZ")
    baseline = mk_baseline(dict.fromkeys(codes, 1000.0), code_universe=list(codes))
    bars = mk_minute_bars(
        {now.date() - timedelta(days=day): [200.0, 200.0, 200.0] for day in (1, 2, 3, 4)}
    )

    def capture(codes_in_snapshot: tuple[str, ...]) -> MonitorBuiltinCapturedRead:
        snapshot = mk_snap([
            {"ts_code": code, "price": 100.0, "pre_close": 90.0, "pct_chg": 5.0,
             "volume": 1000.0, "amount": 4800.0}
            for code in codes_in_snapshot
        ])
        assert run_surge_watch(
            force_session=True, max_ticks=1, now_fn=lambda: now, sleep_fn=lambda _: None,
            snapshot_fetcher=lambda: snapshot, minute_fetcher=lambda *_: bars, baseline=baseline,
            recent_trading_days_fn=lambda _: (now.date(),), notify_fn=lambda *_a, **_k: None,
            base_dir=root / "original-live", config=SurgeConfig(silent_until_hhmm="09:30"),
            builtin_outlet=outlet,
        ) == 0
        return read_original_builtin_capture(authority, read_at=now)

    try:
        partial = capture(codes[:1])
        raw = require_builtin_captured_read(partial).capture
        assert raw.missing_codes == codes[1:] and raw.original_results == ()
        assert len(json.loads(raw.original_source_receipt_json)["results"]) == 1
        committed = commit_original_builtin_round(
            owner, captured=partial, definitions=definitions, evaluated_at=now
        )
        assert committed.events == ()
        assert raw.source_state == "disconnected" and raw.reason == "incomplete_universe"
        head = owner.builtin_heads()[0]
        assert head.source_state == "disconnected" and head.matched_count is None
        with ledger._connection() as connection:
            count = connection.execute(
                "SELECT count(*) FROM monitor_builtin_day_dedupe"
            ).fetchone()[0]
            assert count == 0
        now += timedelta(seconds=5)
        complete = capture(codes)
        full = require_builtin_captured_read(complete).capture
        assert full.missing_codes == () and full.source_state == "ready"
        restored = commit_original_builtin_round(
            owner, captured=complete, definitions=definitions, evaluated_at=now
        )
        assert restored.events
        assert {row.event.detection.ts_code for row in restored.events} == {codes[1]}
        assert owner.builtin_heads()[0].source_state == "ready"
        with ledger._connection() as connection:
            count = connection.execute(
                "SELECT count(*) FROM monitor_builtin_day_dedupe"
            ).fetchone()[0]
            assert count == len(restored.events)
    finally:
        ledger.close()


def test_missing_pulse_universe_does_not_commit_event_or_consume_daily_dedupe(
    tmp_path: Path,
) -> None:
    from zoneinfo import ZoneInfo

    import pandas as pd
    from rquant.pulse_watch import PulseSession

    from rquant.monitor_builtin_runtime import (
        commit_original_builtin_round,
        read_original_builtin_capture,
        require_builtin_captured_read,
    )
    from tests.unit.test_monitor_completion_wiring import _display_source

    root = tmp_path / "original-owner"
    root.mkdir(mode=0o700)
    owner, ledger, authority, definitions, outlet = _display_source(root, "original_pulse")
    now = AT.astimezone(ZoneInfo("Asia/Shanghai"))
    codes = tuple(f"{600000 + i:06d}.SH" for i in range(4001))
    snapshot = pd.DataFrame([
        {"ts_code": code, "price": 10.0, "high": 10.0, "pre_close": 10.0,
         "limit_up_price": 11.0, "limit_down_price": 9.0}
        for code in codes
    ])
    session = PulseSession(
        root / "original-live", now.date(), notify_fn=lambda *_a, **_k: None,
        builtin_outlet=outlet, market_universe=codes, builtin_clock=lambda: now,
    )
    try:
        assert session.on_snapshot(snapshot, now) == []
        partial = snapshot.iloc[:-1].copy()
        partial.loc[:5, ["price", "high"]] = 11.0
        now += timedelta(minutes=10)
        assert session.on_snapshot(partial, now)
        captured = read_original_builtin_capture(authority, read_at=now)
        raw = require_builtin_captured_read(captured).capture
        assert raw.missing_codes == codes[-1:] and raw.original_results == ()
        assert json.loads(raw.original_source_receipt_json)["results"]
        committed = commit_original_builtin_round(
            owner, captured=captured, definitions=definitions, evaluated_at=now
        )
        assert committed.events == ()
        assert raw.source_state == "disconnected" and raw.reason == "incomplete_universe"
        assert owner.builtin_heads()[0].matched_count is None
        with ledger._connection() as connection:
            count = connection.execute(
                "SELECT count(*) FROM monitor_builtin_day_dedupe"
            ).fetchone()[0]
            assert count == 0
        complete = snapshot.copy()
        complete.loc[:5, "high"] = 11.0
        now += timedelta(minutes=1)
        originals = session.on_snapshot(complete, now)
        assert originals
        restored_capture = read_original_builtin_capture(authority, read_at=now)
        full = require_builtin_captured_read(restored_capture).capture
        assert full.missing_codes == () and full.source_state == "ready"
        restored = commit_original_builtin_round(
            owner, captured=restored_capture, definitions=definitions, evaluated_at=now
        )
        assert len(restored.events) == len(originals)
        assert owner.builtin_heads()[0].source_state == "ready"
        assert {row.event.detection.kind for row in restored.events} == {
            row.kind for row in originals
        }
    finally:
        ledger.close()


def test_original_monitor_outlet_binds_full_watch_and_actual_fetch_clock(tmp_path: Path) -> None:
    from rquant.monitor import RealtimeQuote
    from rquant.monitor_builtin_runtime import OriginalMonitorFetchReceipt, bind_original_builtin_source_outlet, read_original_builtin_capture, require_builtin_captured_read

    _, ledger, _, _, authority, _ = builtin_world(tmp_path)
    outlet = bind_original_builtin_source_outlet((authority,))
    item = watch_item()
    quote = RealtimeQuote(ts_code=item.ts_code, price=9.1, low=8.9, open=9.5, high=9.6, pre_close=10.0)
    receipt = OriginalMonitorFetchReceipt(requested_at=AT, response_received_at=AT + timedelta(seconds=1),
        tushare_cache_at=None, fallback_codes=(item.ts_code,))
    outlet.monitor_snapshot(watchlist=(item,), quotes=(quote,), fetch=receipt)
    actual = require_builtin_captured_read(read_original_builtin_capture(authority, read_at=receipt.response_received_at)).capture
    assert actual.quotes[0].price == 9.1 and actual.quotes[0].observed_at == receipt.response_received_at
    assert actual.raw_payload_sha256 is not None and actual.stock_watch[0].pool == "pool2"
    assert json.loads(actual.original_source_receipt_json)["timestamp_provenance"] == "response_received_at_fallback"
    outlet.monitor_snapshot(watchlist=(item,), quotes=(), fetch=receipt.model_copy(update={"requested_at": AT + timedelta(seconds=5), "response_received_at": AT + timedelta(seconds=6)}))
    latest = require_builtin_captured_read(read_original_builtin_capture(authority, read_at=AT + timedelta(seconds=6))).capture
    assert latest.source_state == "disconnected" and latest.missing_codes == (item.ts_code,)
    ledger.close()


def builtin_delivery_world(tmp_path: Path, *, owners: tuple[str, ...] = ("alice",)):
    from rquant.condition_alert_runtime_contracts import verify_condition_alert_activation
    from rquant.notification_state import NotificationStateStore
    from rquant.monitor_builtin_runtime import commit_original_builtin_round
    from rquant.runtime_service_entrypoint import RuntimeServiceKind
    from tests.unit.test_condition_alert_route import condition_route_fixture

    store, ledger, _activation, _price, capture, definitions = builtin_world(tmp_path, owners=owners)
    read, _ = publish_stock_capture(capture)
    receipt = commit_original_builtin_round(store, captured=read, definitions=definitions, evaluated_at=AT)
    bus, router, policy, source, _ = condition_route_fixture(tmp_path, produced_source=store.source_descriptor())
    if owners != ("alice",):
        from rquant.condition_alert_route import ConditionAlertRecipientPolicy
        from rquant.price_alert_route import PriceAlertOwnerTargets
        from rquant.delivery_contracts import DeliveryTarget, DeliveryChannel

        policy = ConditionAlertRecipientPolicy(generation_id=policy.generation_id,
            owners=tuple(PriceAlertOwnerTargets(owner_id=owner, targets=(DeliveryTarget(
                recipient_id="admin", channel=DeliveryChannel.PUSHDEER),)) for owner in owners))
        path = tmp_path / "condition-router.json"
        raw = json.loads(path.read_bytes())
        raw["settings"]["condition_alert_runtime"]["recipient_policy_sha256"] = policy.sha256
        path.write_text(json.dumps(raw))
        router = verify_condition_alert_activation(path, runtime_root=tmp_path,
            expected_manifest_sha256=sha256(path.read_bytes()).hexdigest(), expected_commit="a" * 40,
            expected_kind=RuntimeServiceKind.SIGNAL_ROUTER)
        from rquant.signal_bus import SignalBusStore

        bus = SignalBusStore(tmp_path / "multi-owner-condition-bus.sqlite3")
        bus.install_condition_alert_route_v1(router)
    for record in receipt.events:
        bus.commit_condition_alert_route(activation=router, policy=policy, source=source, record=record,
            source_inspected_at=AT, routed_at=AT)
    path = tmp_path / "builtin-notifier.json"
    raw = json.loads((tmp_path / "condition-router.json").read_bytes())
    raw["service_id"], raw["service_kind"] = "builtin.notifier", "notifier"
    raw["settings"]["condition_alert_runtime"].update(routing_enabled=False, delivery_enabled=True)
    path.write_text(json.dumps(raw))
    path.chmod(0o600)
    notifier = verify_condition_alert_activation(path, runtime_root=tmp_path,
        expected_manifest_sha256=sha256(path.read_bytes()).hexdigest(), expected_commit="a" * 40,
        expected_kind=RuntimeServiceKind.NOTIFIER)
    delivery = NotificationStateStore(bus.path)
    delivery.install_condition_alert_delivery_v1(notifier)
    return store, ledger, capture, definitions, read, receipt, delivery, notifier, policy


def test_builtin_delivery_requires_same_read_original_prefix_and_current_source(tmp_path: Path) -> None:
    from rquant.condition_alert_runtime_projection import ConditionAlertDeliveryAuthorityInput, apply_condition_alert_delivery_authority
    from rquant.monitor_builtin_runtime import inspect_original_builtin_delivery, require_builtin_delivery_inspection
    from rquant.runtime_notification_providers import SuppressedNotificationProvider

    owner, ledger, capture, definitions, read, receipt, store, notifier, policy = builtin_delivery_world(tmp_path)
    witness = inspect_original_builtin_delivery(owner, captured=(read,), inspected_at=AT)
    facts = require_builtin_delivery_inspection(witness)
    value = ConditionAlertDeliveryAuthorityInput(rules=None, scopes=(), policy=policy, producer=None,
        notifier_manifest_sha256=sha256((tmp_path / "builtin-notifier.json").read_bytes()).hexdigest(),
        delivery_enabled=True, inspected_at=AT, builtin=facts)
    with pytest.raises(TypeError, match="inspection"):
        apply_condition_alert_delivery_authority(store, value, activation=notifier, expected_revision=0, applied_at=AT)
    applied = apply_condition_alert_delivery_authority(store, value, activation=notifier,
        expected_revision=0, applied_at=AT, builtin_inspection=witness)
    records = store.claim_due_with_condition_activation("worker", activation=notifier, now=AT,
        lease_for=timedelta(seconds=10), limit=100)
    assert len(records) == len(receipt.events)
    provider = SuppressedNotificationProvider()
    prepared = provider.prepare_condition(store.notification_event(records[0].signal_id), records[0])
    right = store.admit_condition_alert_delivery(records[0], activation=notifier, worker_id="worker",
        expected_revision=applied.authority_revision, admitted_at=AT)
    assert provider.deliver_condition(prepared, right, store=store, record=records[0], now=AT).startswith("shadow:")
    # A receipt made from the same old ready pointer cannot survive latest source failure.
    publish_stock_capture(capture, at=AT + timedelta(seconds=1), sequence=2, state="disconnected")
    with pytest.raises(ValueError, match="changed"):
        require_builtin_delivery_inspection(witness)
    ledger.close()


def test_builtin_delivery_source_failure_does_not_claim_old_ready_events(tmp_path: Path) -> None:
    from rquant.condition_alert_runtime_projection import ConditionAlertDeliveryAuthorityInput, apply_condition_alert_delivery_authority
    from rquant.monitor_builtin_runtime import inspect_original_builtin_delivery, require_builtin_delivery_inspection

    owner, ledger, capture, definitions, _read, _receipt, store, notifier, policy = builtin_delivery_world(tmp_path)
    now = AT + timedelta(seconds=1)
    read, _ = publish_stock_capture(capture, at=now, sequence=2, state="disconnected")
    owner.record_builtin_unavailable(definitions=definitions, origin="original_monitor", evaluated_at=now, reason="capture_unavailable")
    witness = inspect_original_builtin_delivery(owner, captured=(read,), inspected_at=now)
    value = ConditionAlertDeliveryAuthorityInput(rules=None, scopes=(), policy=policy, producer=None,
        notifier_manifest_sha256=sha256((tmp_path / "builtin-notifier.json").read_bytes()).hexdigest(),
        delivery_enabled=True, inspected_at=now, builtin=require_builtin_delivery_inspection(witness))
    apply_condition_alert_delivery_authority(store, value, activation=notifier, expected_revision=0,
        applied_at=now, builtin_inspection=witness)
    assert store.claim_due_with_condition_activation("worker", activation=notifier, now=now,
        lease_for=timedelta(seconds=10), limit=100) == ()
    assert all(item.attempt_count == 0 for item in store.outbox_records())
    ledger.close()
