import json
import os
import threading
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from rquant.live_spool import LiveBatchSpool
from rquant.price_alert_runtime_contracts import (
    PriceAlertFrequencyPolicy,
    verify_price_alert_activation,
)
from rquant.price_alert_runtime_store import PriceAlertRuntimeStore, ReadonlyPriceAlertRuntimeStore
from rquant.runtime_builder_price_alert import (
    price_alert_runtime_builder,
    price_evaluation_contract_sha256,
    price_routing_contract_sha256,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_builtin import build_builtin_registry, watchlist_quote_source_builder
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeServiceStatus
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    run_runtime_service_manifest,
)
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT
from tests.unit.test_price_alert_runtime_activation import activation_fixture
from tests.unit.test_web_price_alert_rules import member, publish, rule


def runtime_fixture(tmp_path: Path):
    tmp_path.chmod(0o700)
    now = (FIXTURE_BUILT_AT + timedelta(days=1)).replace(hour=2, minute=0)
    serving = tmp_path / "serving"
    with (
        patch("tests.support.web_serving_fixture.FIXTURE_BUILT_AT", now),
        patch("tests.unit.test_web_price_alert_rules.FIXTURE_BUILT_AT", now),
    ):
        publish(serving, rules=(rule(),), members=(member(),))
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="b" * 40,
        coverage_start=now.date(),
        coverage_end=now.date(),
        open_dates=(now.date(),),
        generated_at=now - timedelta(days=1),
    )
    calendar_path = tmp_path / "calendar.json"
    calendar_path.write_bytes(calendar.model_dump_json().encode())
    calendar_path.chmod(0o600)
    frequency = PriceAlertFrequencyPolicy(cooldown_seconds=60)
    path, unused = activation_fixture(
        tmp_path,
        frequency_policy_sha256=frequency.sha256,
        evaluation_contract_sha256=price_evaluation_contract_sha256(),
        routing_policy_sha256=price_routing_contract_sha256(),
    )
    manifest = json.loads(path.read_text())
    manifest["interval_seconds"] = 5
    requests = tmp_path / "requests"
    requests.mkdir(mode=0o700)
    LiveBatchSpool(tmp_path / "quotes")
    manifest["settings"].update(
        price_alert_runtime_manifest_path=str(path),
        ledger_path=str(tmp_path / "runtime.sqlite3"),
        scope_serving_root=str(serving),
        quote_spool_root=str(tmp_path / "quotes"),
        quote_request_root=str(requests),
        quote_expected_commit="b" * 40,
        calendar_path=str(calendar_path),
        calendar_expected_commit="b" * 40,
        calendar_content_sha256=calendar.content_sha256,
        frequency_policy=frequency.model_dump(mode="json"),
    )
    path.write_text(json.dumps(manifest))
    path.chmod(0o600)
    actual = RuntimeServiceManifest.model_validate_json(path.read_bytes())
    cap = verify_price_alert_activation(
        path,
        runtime_root=tmp_path,
        expected_manifest_sha256=sha256(path.read_bytes()).hexdigest(),
        expected_commit="b" * 40,
        expected_kind=RuntimeServiceKind.PRICE_ALERT_RUNTIME,
    )
    installed = PriceAlertRuntimeStore.install(tmp_path / "runtime.sqlite3", activation=cap)
    installed.close()
    return now, path, actual, cap, frequency, requests, serving, calendar


def quote_manifest(tmp_path: Path, calendar) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="quote.price.test",
        service_kind=RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=20,
        producer_commit="b" * 40,
        settings=dict(
            spool_root=str(tmp_path / "quotes"),
            quota_path=str(tmp_path / "quota.sqlite3"),
            quota_units_per_window=1000,
            producer_version="test",
            rollout_mode="published",
            domain_mode="price_rules",
            price_scope_serving_root=str(tmp_path / "serving"),
            price_request_root=str(tmp_path / "requests"),
            calendar_path=str(tmp_path / "calendar.json"),
            calendar_expected_commit="b" * 40,
            calendar_content_sha256=calendar.content_sha256,
        ),
    )


def test_actual_quote_domain_freezes_before_gateway_and_runtime_commits_real_scope(
    tmp_path: Path,
) -> None:
    now, path, manifest, cap, frequency, requests, serving, calendar = runtime_fixture(tmp_path)
    calls = []

    def provider(codes, *, timeout_seconds, on_started):
        assert len(tuple(requests.glob("*.json"))) == 1
        calls.append(codes)
        on_started(now)
        return pd.DataFrame(
            [
                dict(
                    ts_code=code,
                    price=11.0,
                    open=10.0,
                    high=11.0,
                    low=10.0,
                    volume=10.0,
                    amount=100.0,
                    source_observed_at=now,
                )
                for code in codes
            ]
        )

    quote = watchlist_quote_source_builder(
        provider_factory=lambda: provider, universe_loader=None, clock=lambda: now
    )(quote_manifest(tmp_path, calendar))
    assert quote().batch_published is True and calls == [("600001.SH",)]
    step = price_alert_runtime_builder(clock=lambda: now, runtime_root=tmp_path)(manifest)
    result = step()
    assert result.processed_count == 1
    assert step.store.source_descriptor().high_watermark == 1
    facts = step.store.runtime_snapshot(observed_at=now)
    assert (
        facts.round.input_metadata.scope_generation_id
        == json.loads((serving / "current.json").read_text())["generation_id"]
    )
    assert facts.rules[0].state == "triggered" and facts.rules[0].last_triggered_at == now
    assert facts.rules[0].next_allowed_at == now + timedelta(seconds=60)
    peer = ReadonlyPriceAlertRuntimeStore(tmp_path / "runtime.sqlite3", activation=cap)
    assert peer.runtime_snapshot(observed_at=now) == facts
    assert peer.events_after(0, inspected_at=now)[0].event.ts_code == "600001.SH"
    peer.close()
    step.close()


def test_actual_loop_missing_scope_is_not_known_zero_and_keeps_original_history(
    tmp_path: Path,
) -> None:
    now, path, manifest, cap, frequency, requests, serving, calendar = runtime_fixture(tmp_path)
    # A trusted scope exists but no matching published quote: record unavailable per rule.
    current = [now]
    step = price_alert_runtime_builder(clock=lambda: current[0], runtime_root=tmp_path)(manifest)
    step()
    assert step.store.runtime_snapshot(observed_at=now).rules[0].state == "unavailable"
    current[0] = now + timedelta(seconds=31)
    result = step()
    snapshot = step.store.runtime_snapshot(observed_at=current[0])
    assert snapshot.round.input_metadata.availability == "unavailable"
    assert snapshot.round.input_metadata.reason == "scope_expired"
    assert snapshot.rules == () and "price_alert:scope_expired" in result.degraded_reasons
    step.close()


def test_registered_public_runtime_runs_and_releases_original_single_writer(
    tmp_path: Path,
) -> None:
    now, path, manifest, cap, frequency, requests, serving, calendar = runtime_fixture(tmp_path)

    def provider(codes, *, timeout_seconds, on_started):
        on_started(now)
        return pd.DataFrame(
            [
                dict(
                    ts_code=code,
                    price=11.0,
                    open=10.0,
                    high=11.0,
                    low=10.0,
                    volume=10.0,
                    amount=100.0,
                    source_observed_at=now,
                )
                for code in codes
            ]
        )

    registry = build_builtin_registry(
        runtime_capabilities={},
        watchlist_quote_provider_factory=lambda: provider,
        clock=lambda: now,
        runtime_root=tmp_path,
    )
    quote_step = registry.build(quote_manifest(tmp_path, calendar))
    assert quote_step().batch_published is True
    quote_close = getattr(quote_step, "close", None)
    if callable(quote_close):
        quote_close()
    source_before = sha256((serving / "current.json").read_bytes()).hexdigest()
    descriptors_before = len(os.listdir("/dev/fd"))
    threads_before = tuple(thread.ident for thread in threading.enumerate())
    for _ in range(2):
        heartbeat = run_runtime_service_manifest(
            manifest,
            registry=registry,
            control_root=tmp_path / "control",
            stop_event=threading.Event(),
            max_iterations=1,
            clock=lambda: now,
        )
        assert heartbeat.status is RuntimeServiceStatus.STOPPED
        assert heartbeat.stop_reason == "loop completed"
        assert heartbeat.total_successes == 1 and heartbeat.total_failures == 0
        assert heartbeat.output_sequence == 1 and heartbeat.stopped_at == now
        peer = ReadonlyPriceAlertRuntimeStore(tmp_path / "runtime.sqlite3", activation=cap)
        assert peer.source_descriptor().high_watermark == 1
        assert len(peer.events_after(0, inspected_at=now)) == 1
        assert peer.runtime_snapshot(observed_at=now).rules[0].state == "triggered"
        peer.close()
        reopened = PriceAlertRuntimeStore(tmp_path / "runtime.sqlite3", activation=cap)
        reopened.close()
    assert len(os.listdir("/dev/fd")) == descriptors_before
    assert tuple(thread.ident for thread in threading.enumerate()) == threads_before
    assert sha256((serving / "current.json").read_bytes()).hexdigest() == source_before
    print(
        "PUBLIC_RUNTIME_PROOF",
        json.dumps(
            {
                "loops": 2,
                "events": 1,
                "original_single_writer_reopened": True,
                "fd_delta": 0,
                "thread_delta": 0,
                "source_unchanged": True,
            },
            sort_keys=True,
        ),
    )


def test_manifest_self_constructed_model_hash_or_missing_install_cannot_start(
    tmp_path: Path,
) -> None:
    now, path, manifest, cap, frequency, requests, serving, calendar = runtime_fixture(tmp_path)
    forged = manifest.model_copy(
        update={"settings": {**manifest.settings, "ledger_path": str(tmp_path / "other.sqlite3")}}
    )
    with pytest.raises((TypeError, ValueError)):
        price_alert_runtime_builder(clock=lambda: now, runtime_root=tmp_path)(forged)
    (tmp_path / "runtime.sqlite3").unlink()
    with pytest.raises((OSError, ValueError)):
        price_alert_runtime_builder(clock=lambda: now, runtime_root=tmp_path)(manifest)
    assert not (tmp_path / "runtime.sqlite3").exists()


def test_actual_notifier_fixed_mixed_reader_relays_price_with_all_price_flags_absent(
    tmp_path: Path,
) -> None:
    from rquant.delivery_contracts import DeliveryChannel, OutboxStatus
    from rquant.notification_state import NotificationStateStore
    from rquant.price_alert_route import install_price_alert_history
    from rquant.runtime_builder_signal import notifier_builder
    from rquant.signal_route_spool import SignalRouteSpool, publish_mixed_notification_bus_prefix
    from tests.unit.test_price_alert_route_spool import legacy_route, mixed_fixture
    from tests.unit.test_runtime_builder_signal import _notifier_manifest

    producer, bus, activation, one, price, three = mixed_fixture(tmp_path)
    spool = SignalRouteSpool(tmp_path / "signal-spool")
    publish_mixed_notification_bus_prefix(
        bus=bus,
        spool=spool,
        limit=100,
        observed_at=__import__("tests.unit.test_price_alert_event_contracts", fromlist=["AT"]).AT,
    )
    manifest = _notifier_manifest(tmp_path)
    state = NotificationStateStore(Path(manifest.settings["notification_state_path"]))
    install_price_alert_history(state)
    delivered = []

    class LegacyProvider:
        def deliver(self, item):
            delivered.append(item.signal.signal_id)
            return "synthetic:accepted"

    now = __import__("tests.unit.test_price_alert_event_contracts", fromlist=["AT"]).AT
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: LegacyProvider()}, clock=lambda: now
    )(manifest)
    assert step().output_sequence == 3
    assert delivered == [one.signal_id, three.signal_id]
    assert (
        next(row for row in state.outbox_records() if row.signal_id == price.event.event_id).status
        is OutboxStatus.PENDING
    )
    four = legacy_route(bus, "d", 3)
    publish_mixed_notification_bus_prefix(bus=bus, spool=spool, limit=100, observed_at=now)
    assert step().output_sequence == 4 and delivered[-1] == four.signal_id
    producer.close()
