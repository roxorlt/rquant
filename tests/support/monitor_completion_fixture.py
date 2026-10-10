"""Synthetic original roles; no Linux install, transport, or observed quotes are implied."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from rquant.condition_alert_route import ConditionAlertRecipientPolicy
from rquant.condition_alert_runtime import ConditionAlertRuntimeStore, ConditionFrequencyPolicy
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.monitor_builtin_contracts import MonitorBuiltinDefinition
from rquant.monitor_builtin_runtime import MonitorBuiltinCaptureReference, builtin_source_contract_sha256
from rquant.notifier_operator import MonitorControlReadSettings
from rquant.page_control import PageControlOutbox
from rquant.price_alert_route import PriceAlertOwnerTargets
from rquant.price_alert_runtime_contracts import PriceAlertFrequencyPolicy
from rquant.price_alert_runtime_store import PriceAlertRuntimeStore
from rquant.runtime_builder_condition_alert import (
    condition_evaluation_contract_sha256,
    condition_routing_contract_sha256,
    verify_condition_role_manifest,
)
from rquant.runtime_builder_price_alert import (
    price_evaluation_contract_sha256,
    price_routing_contract_sha256,
    verify_price_role_manifest,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_profile import (
    PageControlRuntimeProfile,
    RuntimeDeploymentProfile,
    install_runtime_deployment_profile,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_control import RuntimeStepResult
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.strict_json import canonical_json_bytes
from rquant.task_control import TaskControlJournal

AT = datetime(2026, 7, 31, 1, 40, 2, tzinfo=UTC)
COMMIT = "b" * 40


def private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)


def freeze(path: Path, raw: bytes) -> None:
    private_directory(path.parent)
    path.write_bytes(raw)
    path.chmod(0o600)


class SyntheticCredentialTransaction:
    """Only the unavailable root/systemd credential helper is substituted in this fixture."""

    def __init__(self, credentials: dict[str, bytes]) -> None:
        self.sealed_instances = tuple(sorted(credentials))
        self.committed = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.committed = False


@dataclass(frozen=True)
class MonitorCompletionFixture:
    runtime_root: Path
    profile: RuntimeDeploymentProfile
    generation_id: str
    notifier: RuntimeServiceManifest
    producer: RuntimeServiceManifest
    producer_path: Path
    journal: TaskControlJournal
    controls: MonitorControlReadSettings
    ledger_path: Path
    definitions: tuple[MonitorBuiltinDefinition, ...]
    now: datetime = AT


def build_original_monitor_control_fixture(root: Path, *, at: datetime = AT,
    owners: tuple[str, ...] = ("alice",), complete_pipeline: bool = False,
    capture_sources: tuple[MonitorBuiltinCaptureReference, ...] = (),
) -> MonitorCompletionFixture:
    """Install the original notifier profile and explicitly frozen composed peer locally.

    The producer is verified by its original activation and real initialized single
    ledger. It is deliberately absent from the systemd profile generation. Credentials
    are synthetic; the root credential sealer is an explicit test seam, not evidence of
    Linux installation or live delivery capability.
    """
    root = root.absolute()
    private_directory(root)
    control_root = root / "control"
    private_directory(control_root)
    journal = TaskControlJournal(PageControlOutbox(control_root / "page-control.sqlite3"))
    identity = journal.identity()
    Path(identity.path).chmod(0o600)
    controls = MonitorControlReadSettings(outbox_path=Path(identity.path), outbox_device=identity.device,
        outbox_inode=identity.inode, outbox_instance_id=identity.instance_id,
        notifier_service_id="notifier.monitor", condition_service_id="price.monitor")
    roles = root / "role-manifests"
    private_directory(roles)
    producer_path, notifier_path, policy_path = roles / "producer.json", roles / "notifier.json", roles / "recipients.json"
    definitions = tuple(MonitorBuiltinDefinition(owner_id=owner, builtin_id=kind, enabled=True,
        channels=(DeliveryChannel.PUSHDEER,), code_contract_sha256=builtin_source_contract_sha256())
        for owner in owners for kind in ("pool2_levels", "pool_attack", "pulse", "surge"))
    policy = ConditionAlertRecipientPolicy(generation_id="a" * 64,
        owners=tuple(PriceAlertOwnerTargets(owner_id=owner, targets=(DeliveryTarget(recipient_id=owner, channel=DeliveryChannel.PUSHDEER),)) for owner in owners))
    freeze(policy_path, policy.wire_bytes())
    frequency = PriceAlertFrequencyPolicy(cooldown_seconds=60)
    price = {"source_id": "price-monitor", "source_epoch": "1" * 64, "ledger_id": "2" * 64,
        "generation_id": "3" * 64, "evaluation_contract_sha256": price_evaluation_contract_sha256(),
        "routing_policy_sha256": price_routing_contract_sha256(), "frequency_policy_sha256": frequency.sha256,
        "recipient_policy_sha256": "4" * 64, "evaluation_enabled": True, "event_write_enabled": True}
    condition = {"source_id": "condition-monitor", "source_epoch": "5" * 64, "ledger_id": "6" * 64,
        "generation_id": "7" * 64, "evaluation_contract_sha256": condition_evaluation_contract_sha256(),
        "routing_policy_sha256": condition_routing_contract_sha256(), "frequency_policy_sha256": ConditionFrequencyPolicy().sha256,
        "recipient_policy_sha256": policy.sha256}
    ledger_path = root / "live" / "alerts" / "runtime.sqlite3"
    private_directory(ledger_path.parent)
    extra = {}
    if complete_pipeline:
        from rquant.live_spool import LiveBatchSpool
        from rquant.runtime_market_session import MarketCalendarAuthority

        calendar = MarketCalendarAuthority.create(schema_version=1, exchange="SSE", producer_commit=COMMIT,
            coverage_start=at.date(), coverage_end=at.date(), open_dates=(at.date(),), generated_at=at - timedelta(days=1))
        calendar_path = root / "calendar.json"
        freeze(calendar_path, calendar.model_dump_json().encode())
        quote_root = root / "live" / "quotes"
        LiveBatchSpool(quote_root)
        private_directory(root / "scope-serving")
        extra = {"scope_serving_root": str(root / "scope-serving"), "quote_spool_root": str(quote_root),
            "frequency_policy": frequency.model_dump(mode="json"),
            "quote_request_root": str(root / "quote-requests"), "quote_expected_commit": COMMIT,
            "calendar_path": str(calendar_path), "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": calendar.content_sha256,
            "condition_alert": {"scope_serving_root": str(root / "scope-serving"), "calendar_path": str(calendar_path),
                "calendar_expected_commit": COMMIT, "calendar_content_sha256": calendar.content_sha256}}
    producer = RuntimeServiceManifest(schema_version=2, service_id=controls.condition_service_id,
        service_kind=RuntimeServiceKind.PRICE_ALERT_RUNTIME, plane=RuntimeServicePlane.LIVE,
        interval_seconds=5, stale_after_seconds=30, producer_commit=COMMIT,
        settings={"price_alert_runtime": price, "price_alert_runtime_manifest_path": str(producer_path),
            "condition_alert_runtime": condition | {"evaluation_enabled": True, "event_write_enabled": True},
            "condition_alert_runtime_manifest_path": str(producer_path), "ledger_path": str(ledger_path),
            "monitor_control": controls.model_dump(mode="json"),
            "monitor_builtin": {"enabled": True, "definitions": [item.model_dump(mode="json") for item in definitions],
                "sources": [item.model_dump(mode="json") for item in capture_sources]}, **extra})
    freeze(producer_path, canonical_json_bytes(producer.model_dump(mode="json")))
    price_activation = verify_price_role_manifest(producer, runtime_root=root)
    condition_activation = verify_condition_role_manifest(producer, runtime_root=root)
    ledger = PriceAlertRuntimeStore.install(ledger_path, activation=price_activation)
    try:
        ConditionAlertRuntimeStore.install(ledger, activation=condition_activation)
    finally:
        ledger.close()
    instance = "svc-" + sha256(controls.notifier_service_id.encode()).hexdigest()
    notification_root = root / "live" / "notifications" / instance
    notifier = RuntimeServiceManifest(schema_version=2, service_id=controls.notifier_service_id,
        service_kind=RuntimeServiceKind.NOTIFIER, plane=RuntimeServicePlane.LIVE,
        interval_seconds=1, stale_after_seconds=30, producer_commit=COMMIT,
        settings={"signal_spool_root": str(root / "live" / "signal-bus" / "spool"),
            "notification_state_path": str(notification_root / "notification_state.sqlite3"),
            "serving_authority_root": str(notification_root / "serving-authority"),
            "worker_id": "monitor-fixture", "batch_limit": 100, "lease_seconds": 30,
            "merge_enabled": True, "merge_owner_id": owners[0], "suppress_delivery": True,
            "condition_alert_runtime": condition | {"delivery_enabled": True},
            "condition_alert_runtime_manifest_path": str(notifier_path),
            "condition_alert_peer": {"producer_manifest_path": str(producer_path), "ledger_path": str(ledger_path),
                "recipient_policy_path": str(policy_path), "scope_serving_root": str(root / "serving"), "install_namespace": True},
            "monitor_control": controls.model_dump(mode="json")})
    freeze(notifier_path, canonical_json_bytes(notifier.model_dump(mode="json")))
    profile = RuntimeDeploymentProfile(producer_commit=COMMIT, manifests=(notifier,),
        capability_environment={notifier.service_id: ("PUSHDEER_KEYS",)},
        page_control=PageControlRuntimeProfile(endpoint="http://127.0.0.1:8767/v1/commands", outbox_path=controls.outbox_path))
    with patch("rquant.runtime_deployment_bundle._recover_runtime_credentials", return_value=SimpleNamespace(outcome="none")), \
            patch("rquant.runtime_deployment_bundle._seal_runtime_credentials", side_effect=SyntheticCredentialTransaction):
        receipt = install_runtime_deployment_profile(profile, runtime_root=root,
            environ={"PUSHDEER_KEYS": "explicit-offline-fixture-unused-key"},
            schema_bootstrap_reason="explicit local synthetic monitor contract fixture")
    return MonitorCompletionFixture(runtime_root=root, profile=profile, generation_id=receipt.generation_hash,
        notifier=notifier, producer=producer, producer_path=producer_path, journal=journal,
        controls=controls, ledger_path=ledger_path, definitions=definitions, now=at)


@dataclass
class OriginalMonitorPipeline:
    control: MonitorCompletionFixture
    authority_root: Path
    clock: list[datetime]
    quote_step: Callable[[], RuntimeStepResult]
    producer_step: Callable[[], RuntimeStepResult]
    router_step: Callable[[], RuntimeStepResult]
    notifier_step: Callable[[], RuntimeStepResult]

    @property
    def now(self) -> datetime:
        return self.clock[0]

    def tick(self, elapsed: timedelta) -> tuple[RuntimeStepResult, RuntimeStepResult, RuntimeStepResult]:
        if elapsed < timedelta(0):
            raise ValueError("original pipeline clock cannot move backwards")
        self.clock[0] += elapsed
        self.quote_step()
        return self.producer_step(), self.router_step(), self.notifier_step()

    def close(self) -> None:
        for step in (self.notifier_step, self.router_step, self.producer_step, self.quote_step):
            close = getattr(step, "close", None)
            if close is not None:
                close()


def build_original_monitor_pipeline(root: Path) -> OriginalMonitorPipeline:
    """Run original detectors and factories over explicit synthetic local inputs.

    No systemd, provider HTTP response, role activation, or Serving result is substituted.
    The original quote provider receives a synthetic frame; the market loop receives two
    synthetic complete snapshots and four prior daily amount series. The notification
    manifest is shadow, so the complete admission chain grants no physical POST.
    """
    import json
    import os
    import shutil
    from zoneinfo import ZoneInfo
    import pandas as pd
    from rquant.live_contracts import LiveChannel
    from rquant.live_spool import LiveBatchSpool
    from rquant.monitor_builtin_runtime import bind_original_builtin_source_outlet, verify_builtin_capture_authority
    from rquant.replica_generation import capture_database_watermark, replica_generation_path, write_replica_generation_metadata
    from rquant.runtime_builder_price_alert import price_alert_runtime_builder
    from rquant.runtime_builder_signal import notifier_builder, signal_router_builder
    from rquant.runtime_notification_providers import build_environment_notification_provider_loader
    from rquant.runtime_service_builtin import watchlist_quote_source_builder
    from rquant.storage.duckdb import DuckDBStore
    from rquant.surge_watch import SurgeConfig, run_surge_watch
    from tests.unit.test_runtime_builder_signal import _authoritative_router_manifest
    from tests.unit.test_surge_watch import mk_baseline, mk_minute_bars

    root = root.absolute()
    private_directory(root)
    clock = [AT.astimezone(ZoneInfo("Asia/Shanghai"))]
    private_directory(root / "captures")
    refs, authorities = [], []
    roles = root / "source-bindings"
    private_directory(roles)
    for origin in ("original_pulse", "original_surge"):
        path = roles / (origin + ".json")
        manifest = RuntimeServiceManifest(schema_version=2, service_id="source." + origin,
            service_kind=RuntimeServiceKind.CONDITION_ALERT_RUNTIME, plane=RuntimeServicePlane.LIVE,
            interval_seconds=5, stale_after_seconds=90, producer_commit=COMMIT,
            settings={"monitor_builtin_capture": {"enabled": True, "origin": origin,
                "capture_root": str(root / "captures" / origin), "source_generation_id": canonical_sha256(origin),
                "code_contract_sha256": builtin_source_contract_sha256()}})
        raw = canonical_json_bytes(manifest.model_dump(mode="json"))
        freeze(path, raw)
        authorities.append(verify_builtin_capture_authority(path, runtime_root=root,
            expected_sha256=sha256(raw).hexdigest(), expected_commit=COMMIT))
        refs.append(MonitorBuiltinCaptureReference(origin=origin, manifest_path=path, runtime_root=root,
            manifest_sha256=sha256(raw).hexdigest(), producer_commit=COMMIT))

    # One actual original surge loop also owns Pulse over its same full-market snapshots.
    outlet = bind_original_builtin_source_outlet(tuple(authorities))
    codes = tuple(f"{600000 + i:06d}.SH" for i in range(4000)) + ("300001.SZ",)
    baseline = mk_baseline({"300001.SZ": 1000.0}, code_universe=list(sorted(codes)))
    bars = mk_minute_bars({AT.date() - timedelta(days=day): [200.0, 200.0, 200.0] for day in (1, 2, 3, 4)})
    observed = [0]

    def market_frame() -> pd.DataFrame:
        second = observed[0] > 0
        observed[0] += 1
        rows = [{"ts_code": code, "price": 11.0 if second and i < 6 else 10.0,
            "open": 10.0, "high": 11.0 if second and i < 6 else 10.0, "low": 9.0,
            "pre_close": 10.0, "limit_up_price": 11.0, "limit_down_price": 9.0,
            "pct_chg": 10.0 if second and i < 6 else 0.0, "volume": 1000.0, "amount": 1000.0}
            for i, code in enumerate(codes[:-1])]
        rows.append({"ts_code": "300001.SZ", "price": 100.0, "high": 100.0, "low": 90.0,
            "open": 90.0, "pre_close": 90.0, "pct_chg": 5.0, "volume": 1000.0,
            "limit_up_price": 108.0, "limit_down_price": 72.0,
            "amount": 4800.0 if second else 100.0})
        return pd.DataFrame(rows)

    def next_market_tick(_seconds: float) -> None:
        if observed[0] < 2:
            clock[0] += timedelta(minutes=10)

    run_surge_watch(force_session=True, max_ticks=2, now_fn=lambda: clock[0], sleep_fn=next_market_tick,
        snapshot_fetcher=market_frame, minute_fetcher=lambda *_: bars, baseline=baseline,
        recent_trading_days_fn=lambda _: (AT.date(),), notify_fn=lambda *_args, **_facts: None,
        base_dir=root / "market-live", config=SurgeConfig(silent_until_hhmm="09:30"), builtin_outlet=outlet)

    primary, replica = root / "source-primary.duckdb", root / "source-replica.duckdb"
    previous = AT.date() - timedelta(days=1)
    with DuckDBStore(primary) as original:
        original.upsert_pool2_watch(pd.DataFrame([{"ts_code": "600000.SH", "entry_date": previous,
            "limit_up_date": previous, "body_upper": 11.0, "body_lower": 9.0,
            "level_40": 9.8, "level_30": 9.6, "level_20": 9.4, "stop_strong": 9.0,
            "stop_weak": 8.8, "status": "active"}]))
        original.upsert_daily(pd.DataFrame([{"ts_code": "600000.SH", "trade_date": previous,
            "open": 9.0, "high": 11.0, "low": 9.0, "close": 10.0, "pre_close": 9.0,
            "change": 1.0, "pct_chg": 100 / 9, "vol": 100.0, "amount": 1000.0}]))
        original._conn.execute("INSERT INTO daily_state(ts_code,trade_date,limit_pct) VALUES (?,?,?)", ["600000.SH", previous, .1])
    shutil.copy2(primary, replica)
    os.utime(replica, (clock[0].timestamp() - 1,) * 2)
    write_replica_generation_metadata(primary_path=primary, replica_path=replica,
        output_path=replica_generation_path(replica), source_before=capture_database_watermark(primary))
    os.utime(replica_generation_path(replica), (clock[0].timestamp() - 1,) * 2)
    quote_spool = LiveBatchSpool(root / "quote-spool")
    quote_path = roles / "watchlist_quote.json"
    # Freeze the quote source before the composed owner refers to its exact bytes.
    from rquant.runtime_market_session import MarketCalendarAuthority
    calendar = MarketCalendarAuthority.create(schema_version=1, exchange="SSE", producer_commit=COMMIT,
        coverage_start=AT.date(), coverage_end=AT.date(), open_dates=(AT.date(),), generated_at=AT - timedelta(days=1))
    calendar_path = root / "source-calendar.json"
    freeze(calendar_path, calendar.model_dump_json().encode())
    quote_manifest = RuntimeServiceManifest(service_id="source.watchlist", service_kind=RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        plane=RuntimeServicePlane.LIVE, producer_commit=COMMIT, interval_seconds=5, stale_after_seconds=15,
        settings={"spool_root": str(quote_spool.root), "quota_path": str(root / "source-quota.sqlite3"),
            "quota_units_per_window": 20, "producer_version": "explicit-offline-fixture", "schema_version": 3,
            "units_contract_id": "6" * 64, "volume_unit": "shares", "amount_unit": "CNY",
            "rollout_mode": "published", "domain_mode": "builtin_watchlist", "calendar_path": str(calendar_path),
            "calendar_expected_commit": COMMIT, "calendar_content_sha256": calendar.content_sha256,
            "builtin_primary_path": str(primary), "builtin_replica_path": str(replica),
            "builtin_request_root": str(root / "source-requests"), "monitor_builtin_capture_manifest_path": str(quote_path),
            "monitor_builtin_capture": {"enabled": True, "origin": "watchlist_quote", "capture_root": str(root / "captures" / "quotes"),
                "source_generation_id": quote_spool._source_generation(LiveChannel.WATCHLIST_QUOTE),
                "code_contract_sha256": builtin_source_contract_sha256()}})
    raw = canonical_json_bytes(quote_manifest.model_dump(mode="json"))
    freeze(quote_path, raw)
    refs.append(MonitorBuiltinCaptureReference(origin="watchlist_quote", manifest_path=quote_path, runtime_root=root,
        manifest_sha256=sha256(raw).hexdigest(), producer_commit=COMMIT))
    control = build_original_monitor_control_fixture(root, at=clock[0], owners=("alice", "bob"),
        complete_pipeline=True, capture_sources=tuple(refs))

    def quote_provider(requested: tuple[str, ...], *, timeout_seconds: float, on_started: Callable[[datetime], None]) -> pd.DataFrame:
        del timeout_seconds
        on_started(clock[0])
        return pd.DataFrame([{"ts_code": code, "price": 11.1, "low": 8.9, "open": 10.3,
            "high": 11.2, "pre_close": 10.0, "pct_chg": 11.0, "volume": 1000.0,
            "amount": 11100.0, "source_observed_at": clock[0]} for code in requested])

    quote_step = watchlist_quote_source_builder(provider_factory=lambda: quote_provider, universe_loader=None,
        clock=lambda: clock[0], runtime_root=root)(quote_manifest)
    producer_step = price_alert_runtime_builder(clock=lambda: clock[0], runtime_root=root)(control.producer)
    router_root = root / "router"
    private_directory(router_root)
    router_path = router_root / "condition-router.json"
    condition = control.notifier.model_dump(mode="json")["settings"]["condition_alert_runtime"] | {"routing_enabled": True, "delivery_enabled": False}
    router, _empty_original_runner = _authoritative_router_manifest(router_root, signal_spool_root=control.notifier.settings["signal_spool_root"], batch_limit=100,
        condition_alert_runtime=condition, condition_alert_runtime_manifest_path=str(router_path),
        condition_alert_peer=dict(control.notifier.model_dump(mode="json")["settings"]["condition_alert_peer"]))
    router = RuntimeServiceManifest.model_validate(router.model_dump(mode="python") | {"producer_commit": COMMIT})
    freeze(router_path, canonical_json_bytes(router.model_dump(mode="json")))
    os.utime(Path(router.settings["routing_policy_path"]), (clock[0].timestamp() - 1,) * 2)
    router_step = signal_router_builder(clock=lambda: clock[0], runtime_root=root)(router)
    provider_loader = build_environment_notification_provider_loader(pushdeer_recipient_id="alice", environment={
        "PUSHDEER_KEYS": "explicit-offline-unused-alice,explicit-offline-unused-bob",
        "PUSHDEER_RECIPIENT_IDS": "alice,bob"})
    notifier_step = notifier_builder(clock=lambda: clock[0], runtime_root=root, provider_loader=provider_loader)(control.notifier)
    return OriginalMonitorPipeline(control, Path(control.notifier.settings["serving_authority_root"]), clock,
        quote_step, producer_step, router_step, notifier_step)
