from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from subprocess import CompletedProcess

import pandas as pd

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureBatchEnvelope,
    FeatureFieldStatus,
    FeatureRequirement,
    RequirementLevel,
)
from rquant.feature_spool import FeatureBatchSpool, FeatureSessionCloseMarker
from rquant.paper_broker import PaperBrokerStore
from rquant.runtime_definition_bootstrap import plan_builtin_definitions
from rquant.runtime_deployment_bundle import strategy_live_producer_version
from rquant.runtime_deployment_profile import (
    PRODUCTION_SHADOW_SIGNER_COMMAND,
    RuntimeDeploymentProfile,
    ShadowRuntimeProfile,
    install_runtime_deployment_profile,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_service_main import build_parser
from rquant.runtime_service_main import run as run_runtime_service_main
from rquant.runtime_shadow_job import run_shadow_production_session
from rquant.runtime_shadow_sources import (
    legacy_records_raw_input_id,
    legacy_surge_file_raw_input_id,
)
from rquant.runtime_shadow_validation import (
    Ed25519CompletionAttestationKeyring,
    Ed25519CompletionAttestationSigner,
    SecureShadowSigningClient,
    ShadowSigningRequest,
    ShadowSigningResponse,
    ShadowSourceCompletionReceipt,
    ShadowStrategyBinding,
    shadow_session_boundaries,
)
from rquant.signal_bus import SignalBusStore
from rquant.signal_contracts import SignalAction
from rquant.signal_router_runtime import (
    ReadonlySignalRouteAuthority,
    ReadonlyStrategyRunnerSignalSource,
    RoutingDecision,
    SignalRouteCursorStore,
    StrategyRunnerSignalSource,
    route_runner_signals,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_runner import (
    StrategyCandidateState,
    StrategyDecision,
    StrategyRunnerStore,
    StrategySourceBatchReceipt,
    canonical_feature_payload,
)
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)
from rquant.strict_json import canonical_json_bytes
from tests.paper_cost_fixtures import paper_cost_policy
from tests.shadow_ed25519_support import create_shadow_ed25519_test_authority
from tests.unit.test_runtime_builder_strategy import (
    _publish as _publish_strategy_feature,
)
from tests.unit.test_runtime_builder_strategy import (
    _publish_candidates as _publish_strategy_candidates,
)

TRADE_DATE = date(2026, 7, 31)
SESSION_CLOSE = datetime(2026, 7, 31, 7, 0, tzinfo=UTC)
COMMIT = "a" * 40
EVALUATOR = "2" * 64
POLICY = "9" * 64
PRODUCER_VERSION = "runtime-shadow-e2e-v1"
REGISTRATION = "1" * 64
CANDIDATE_SCHEMA = "3" * 64
FEATURE_REGISTRATION = "4" * 64
FEATURE_CONTRACT = "5" * 64
PRODUCER_MANIFEST = "6" * 64


def _spec(strategy_id: str, action: SignalAction) -> StrategySpec:
    return StrategySpec(
        strategy_id=strategy_id,
        version=1,
        feature_contract_id="intraday-pit",
        min_feature_contract_version=1,
        required_features=(
            FeatureRequirement(
                name="rel_same_minute",
                level=RequirementLevel.REQUIRED,
                min_contract_version=1,
            ),
        ),
        optional_features=(),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="entry_ready",
                to_state=StrategyLifecycleState.ARMED,
            ),
        ),
        parameters={},
        allowed_actions=(action.value,),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=COMMIT,
    )


def _write_final_batch(
    store: StrategyRunnerStore,
    spool: FeatureBatchSpool,
    *,
    calendar_generation_id: str,
    candidate_id: str,
    action: SignalAction,
) -> FeatureSessionCloseMarker:
    frame = pd.DataFrame({"ts_code": [candidate_id], "rel_same_minute": [2.0]})
    payload = canonical_feature_payload(frame, schema_version=1)
    envelope = FeatureBatchEnvelope(
        schema_version=1,
        batch_id=f"close-{store.spec.strategy_id}",
        contract_id="intraday-pit",
        contract_version=1,
        input_batch_ids=("minute-close",),
        sequence=0,
        event_time=SESSION_CLOSE,
        available_at=SESSION_CLOSE,
        decision_cutoff=SESSION_CLOSE,
        actual_delay_seconds=0.0,
        row_count=1,
        content_hash=hashlib.sha256(payload).hexdigest(),
        field_statuses=(
            FeatureFieldStatus(
                name="rel_same_minute",
                status=FeatureAvailability.AVAILABLE,
                source_event_time=SESSION_CLOSE,
                available_at=SESSION_CLOSE,
                decision_cutoff=SESSION_CLOSE,
                actual_delay_seconds=0.0,
            ),
        ),
        producer_commit=COMMIT,
    )

    def evaluator(
        _spec: StrategySpec,
        state: StrategyCandidateState,
        _features: dict[str, object],
    ) -> StrategyDecision:
        return StrategyDecision(
            event="entry_ready",
            expected_from_state=state.state,
            expected_to_state=StrategyLifecycleState.ARMED,
            expected_action=action,
            action=action,
            reason_codes=("shadow_e2e",),
            expires_after=timedelta(minutes=5),
        )

    spool.publish(envelope, payload)
    marker = spool.publish_session_close_marker(
        trade_date=TRADE_DATE,
        session_close_at=SESSION_CLOSE,
        produced_at=SESSION_CLOSE + timedelta(seconds=1),
        calendar_generation_id=calendar_generation_id,
        complete_through=SESSION_CLOSE,
        upstream_source_generation_id="7" * 64,
        upstream_final_sequence=0,
        upstream_final_batch_id="raw-close-0",
        upstream_final_content_hash="8" * 64,
    )
    store.process_batch(
        envelope,
        frame,
        feature_payload=payload,
        source_receipt=StrategySourceBatchReceipt(
            source_generation_id=marker.source_generation_id,
            source_sequence=envelope.sequence,
            source_batch_id=envelope.batch_id,
            source_content_hash=envelope.content_hash,
        ),
        dataset_snapshot_id="d" * 64,
        observed_at=SESSION_CLOSE,
        evaluator=evaluator,
    )
    return marker


def _legacy_receipt(
    *,
    source_id: str,
    input_identity: str,
    produced_at: datetime,
) -> ShadowSourceCompletionReceipt:
    _session_open, session_close = shadow_session_boundaries(TRADE_DATE)
    return ShadowSourceCompletionReceipt(
        evidence_origin="production",
        source="legacy",
        source_id=source_id,
        trade_date=TRADE_DATE,
        session_close_at=session_close,
        complete_through=session_close,
        input_identity=input_identity,
        produced_at=produced_at,
        producer_commit=COMMIT,
        producer_version=PRODUCER_VERSION,
    )


def _runtime_instance(service_id: str) -> str:
    return "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()


def _write_calendar_generation(root: Path) -> tuple[Path, MarketCalendarAuthority]:
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=(TRADE_DATE,),
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    path = (
        root / "authorities" / "market-calendar" / "generations" / f"{calendar.content_sha256}.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(calendar.model_dump_json(), encoding="utf-8")
    path.chmod(0o600)
    return path, calendar


def _sign_ed25519_payload(private_key: Path, payload: bytes, signature_path: Path) -> str:
    payload_path = signature_path.with_suffix(".payload")
    payload_path.write_bytes(payload)
    completed = subprocess.run(
        (
            "openssl",
            "pkeyutl",
            "-sign",
            "-inkey",
            str(private_key),
            "-rawin",
            "-in",
            str(payload_path),
            "-out",
            str(signature_path),
        ),
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.decode("utf-8", errors="replace"))
    return base64.b64encode(signature_path.read_bytes()).decode("ascii")


def _patch_fixed_shadow_signer(
    monkeypatch,
    *,
    private_key: Path,
    key_id: str,
    scratch: Path,
) -> None:
    import rquant.runtime_shadow_validation as shadow_module

    scratch.mkdir(parents=True)
    original_run = shadow_module.subprocess.run

    def signer_run(command: object, **kwargs: object) -> CompletedProcess[bytes]:
        if tuple(command) != PRODUCTION_SHADOW_SIGNER_COMMAND:
            return original_run(command, **kwargs)
        request = ShadowSigningRequest.model_validate_json(kwargs["input"])
        payload = base64.b64decode(request.payload_base64, validate=True)
        if request.key_id != key_id or hashlib.sha256(payload).hexdigest() != (
            request.payload_sha256
        ):
            return CompletedProcess(command, 2, b"", b"request mismatch")
        signature = _sign_ed25519_payload(
            private_key,
            payload,
            scratch / f"{request.request_id}.signature",
        )
        response = ShadowSigningResponse(
            request_id=request.request_id,
            key_id=request.key_id,
            namespace=request.namespace,
            payload_sha256=request.payload_sha256,
            signature=signature,
        )
        return CompletedProcess(
            command,
            0,
            canonical_json_bytes(response.model_dump(mode="json")),
            b"",
        )

    monkeypatch.setattr(shadow_module.subprocess, "run", signer_run)


def test_runner_router_close_receipt_to_shadow_job(tmp_path: Path) -> None:
    authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-keys")
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 1),
        coverage_end=date(2026, 8, 31),
        open_dates=(TRADE_DATE,),
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    configurations = (
        ("n_shape", SignalAction.WATCH, "600001.SH"),
        ("growth_board_surge", SignalAction.B_INTENT, "300001.SZ"),
    )
    stores: dict[str, StrategyRunnerStore] = {}
    feature_markers: dict[str, FeatureSessionCloseMarker] = {}
    sources: dict[str, StrategyRunnerSignalSource] = {}
    bindings: list[ShadowStrategyBinding] = []
    for strategy_id, action, candidate_id in configurations:
        source_id = f"strategy.{strategy_id}.v1"
        store = StrategyRunnerStore(
            tmp_path / f"{strategy_id}.sqlite3",
            spec=_spec(strategy_id, action),
            evaluator_contract_fingerprint=EVALUATOR,
        )
        spool = FeatureBatchSpool(tmp_path / f"{strategy_id}-features")
        feature_markers[source_id] = _write_final_batch(
            store,
            spool,
            calendar_generation_id=calendar.content_sha256,
            candidate_id=candidate_id,
            action=action,
        )
        stores[source_id] = store
        sources[source_id] = StrategyRunnerSignalSource(source_id=source_id, store=store)
        bindings.append(
            ShadowStrategyBinding(
                strategy_id=strategy_id,
                strategy_version=1,
                definition_fingerprint=REGISTRATION,
                executable_fingerprint=EVALUATOR,
            )
        )

    bus_path = tmp_path / "signal-bus.sqlite3"
    bus = SignalBusStore(bus_path)
    cursors = SignalRouteCursorStore(
        bus_path,
        routing_policy_fingerprint=POLICY,
    )
    routed_at = SESSION_CLOSE + timedelta(seconds=2)
    for source_id, source in sources.items():
        route_runner_signals(
            source_id=source_id,
            source=source,
            bus=bus,
            cursors=cursors,
            routed_at=routed_at,
            target_resolver=lambda _signal: RoutingDecision.no_target(
                routing_policy_fingerprint=POLICY,
                reason_code="shadow_only",
            ),
            limit=10,
        )

    route_authority = ReadonlySignalRouteAuthority(
        path=bus_path,
        expected_routing_policy_fingerprint=POLICY,
    )
    produced_at = SESSION_CLOSE + timedelta(seconds=3)
    readonly_sources = []
    for binding in bindings:
        source_id = f"strategy.{binding.strategy_id}.v1"
        store = stores[source_id]
        segment_start, segment_final = store.runner_session_route_bounds(TRADE_DATE)
        drain = route_authority.read_drain_evidence(
            source_id=source_id,
            runner_generation_id=store.source_generation_id,
            strategy_spec_fingerprint=store.spec.spec_fingerprint,
            trade_date=TRADE_DATE,
            segment_start_sequence=segment_start,
            routed_through_sequence=segment_final,
            observed_at=produced_at,
        )
        store.publish_session_close_receipt(
            trade_date=TRADE_DATE,
            session_close_at=SESSION_CLOSE,
            source_id=source_id,
            calendar_generation_id=calendar.content_sha256,
            producer_service_id=source_id,
            producer_instance_id=f"{binding.strategy_id}-primary",
            producer_version=PRODUCER_VERSION,
            produced_at=produced_at,
            feature_close_marker=feature_markers[source_id],
            attestation_signer=authority.signer,
            strategy_registration_fingerprint=REGISTRATION,
            executable_fingerprint=EVALUATOR,
            candidate_schema_fingerprint=CANDIDATE_SCHEMA,
            feature_registration_fingerprint=FEATURE_REGISTRATION,
            feature_contract_fingerprint=FEATURE_CONTRACT,
            producer_manifest_fingerprint=PRODUCER_MANIFEST,
            route_evidence=drain,
        )
        readonly_sources.append(
            (
                binding,
                ReadonlyStrategyRunnerSignalSource(
                    source_id=source_id,
                    path=store.path,
                    expected_strategy_spec_fingerprint=store.spec.spec_fingerprint,
                    expected_evaluator_contract_fingerprint=EVALUATOR,
                ),
            )
        )

    monitor_rows = (
        {
            "trade_date": TRADE_DATE,
            "ts_code": "600001.SH",
            "level": "attack_strong_carry",
            "trigger_time": datetime(2026, 7, 31, 14, 59),
        },
    )
    surge_path = tmp_path / "surge.jsonl"
    surge_path.write_text(
        json.dumps(
            {
                "ts_code": "300001.SZ",
                "confirmed_at": "14:59",
                "status": "confirmed",
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    observed_at = SESSION_CLOSE + timedelta(seconds=5)
    monitor_receipt = _legacy_receipt(
        source_id="legacy-monitor-events",
        input_identity=legacy_records_raw_input_id(
            monitor_rows,
            source_id="legacy-monitor-events",
            trade_date=TRADE_DATE,
        ),
        produced_at=observed_at,
    )
    surge_receipt = _legacy_receipt(
        source_id="legacy-surge-jsonl",
        input_identity=legacy_surge_file_raw_input_id(
            surge_path,
            trade_date=TRADE_DATE,
        ),
        produced_at=observed_at,
    )

    report = run_shadow_production_session(
        trade_date=TRADE_DATE,
        observed_at=observed_at,
        producer_commit=COMMIT,
        producer_version=PRODUCER_VERSION,
        calendar=calendar,
        monitor_rows=monitor_rows,
        monitor_completion_receipt=monitor_receipt,
        surge_events_path=surge_path,
        surge_completion_receipt=surge_receipt,
        runner_sources=tuple(readonly_sources),
        report_root=(tmp_path / "reports").resolve(),
        match_tolerance_microseconds=60_000_000,
        attestation_verifier=authority.keyring,
        report_receipt_signer=authority.signer,
        report_receipt_verifier=authority.keyring,
        report_producer_service_id="shadow-daily",
        report_producer_instance_id="completion-e2e",
    )

    assert report.evidence_origin == "production"
    assert report.matched_count == 2
    assert report.legacy_only_count == 0
    assert report.isolated_only_count == 0
    assert all(
        snapshot.completion_receipt.producer_service_id is not None
        for snapshot in report.evidence.input_snapshots
        if snapshot.source == "isolated"
    )


def test_runtime_service_main_once_strategy_live_mints_ed25519_completion_receipt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import rquant.runtime_service_builtin as builtin_module

    key_authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-keys")
    private_key = tmp_path / "shadow-keys" / "shadow-test-v1.private.pem"
    _patch_fixed_shadow_signer(
        monkeypatch,
        private_key=private_key,
        key_id=key_authority.keyring.active_key_id,
        scratch=tmp_path / "shadow-signatures",
    )
    public_key = key_authority.keyring._keys[key_authority.keyring.active_key_id]
    completion_keyring = Ed25519CompletionAttestationKeyring(
        active_key_id=key_authority.keyring.active_key_id,
        active_public_key=public_key,
    )
    runtime_root = tmp_path / "runtime"
    external = tmp_path / "external"
    service_id = "strategy.n_shape.v1"
    instance = _runtime_instance(service_id)
    runner_state_path = runtime_root / "live" / "strategies" / instance / "runner.sqlite3"
    feature_spool_root = runtime_root / "live" / "features"
    signal_bus_path = runtime_root / "live" / "signal-bus" / "signal_bus.sqlite3"
    candidate_root = external / "candidates"
    definition_root = external / "definitions"
    paper_broker_path = (
        runtime_root
        / "live"
        / "paper-brokers"
        / _runtime_instance("paper-broker.shadow-main.v1")
        / "broker.sqlite3"
    )
    calendar_path, calendar = _write_calendar_generation(runtime_root)
    external.mkdir(parents=True)
    PaperBrokerStore(
        paper_broker_path,
        account_id="shadow-main",
        initial_cash=Decimal("100000"),
        cost_policy=paper_cost_policy(),
    )
    plan = plan_builtin_definitions(producer_commit=COMMIT)
    strategy = next(item for item in plan.strategies if item.strategy_id == "n_shape")
    from rquant.runtime_definition_bootstrap import bootstrap_builtin_definitions

    bootstrap_builtin_definitions(
        definition_root,
        producer_commit=COMMIT,
        registered_at=datetime(2026, 7, 1, tzinfo=UTC),
        available_at=datetime(2026, 7, 1, tzinfo=UTC),
        expected_plan_id=plan.plan_id,
    )
    _publish_strategy_candidates(
        candidate_root,
        strategy_id="n_shape",
        captured_at=SESSION_CLOSE - timedelta(minutes=5),
        definition_fingerprint=strategy.registration_fingerprint,
        executable_fingerprint=strategy.executable_fingerprint,
        candidate_schema_fingerprint=strategy.candidate_schema_fingerprint,
    )
    feature_spool = FeatureBatchSpool(feature_spool_root)
    _publish_strategy_feature(
        feature_spool,
        sequence=0,
        available_at=SESSION_CLOSE,
        source_event_time=SESSION_CLOSE,
        decision_cutoff=SESSION_CLOSE,
    )
    feature_spool.publish_session_close_marker(
        trade_date=TRADE_DATE,
        session_close_at=SESSION_CLOSE,
        produced_at=SESSION_CLOSE + timedelta(seconds=1),
        calendar_generation_id=calendar.content_sha256,
        complete_through=SESSION_CLOSE,
        upstream_source_generation_id="7" * 64,
        upstream_final_sequence=0,
        upstream_final_batch_id="raw-close-0",
        upstream_final_content_hash="8" * 64,
    )
    SignalBusStore(signal_bus_path)
    manifest = RuntimeServiceManifest(
        service_id=service_id,
        service_kind=RuntimeServiceKind.STRATEGY_LIVE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=0,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "feature_spool_root": str(feature_spool_root),
            "runner_state_path": str(runner_state_path),
            "definition_registry_root": str(definition_root),
            "strategy_registration_fingerprint": strategy.registration_fingerprint,
            "strategy_spec_fingerprint": strategy.strategy_spec_fingerprint,
            "evaluator_contract_fingerprint": strategy.executable_fingerprint,
            "strategy_executable_fingerprint": strategy.executable_fingerprint,
            "candidate_schema_fingerprint": strategy.candidate_schema_fingerprint,
            "candidate_snapshot_root": str(candidate_root),
            "paper_broker_path": str(paper_broker_path),
            "paper_account_id": "shadow-main",
            "candidate_max_age_seconds": 7 * 24 * 60 * 60,
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "batch_limit": 128,
            "calendar_path": str(calendar_path),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": calendar.content_sha256,
            "signal_bus_path": str(signal_bus_path),
            "routing_policy_fingerprint": POLICY,
            "producer_instance_id": instance,
            "producer_version": strategy_live_producer_version(
                service_id=service_id,
                strategy_version=1,
                producer_commit=COMMIT,
            ),
        },
    )
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        production_runtime_root=str(runtime_root),
        manifests=(manifest,),
        capability_environment={service_id: ()},
        shadow=ShadowRuntimeProfile(
            completion_active_key_id=key_authority.keyring.active_key_id,
            completion_active_public_key_pem=public_key.decode("utf-8"),
            completion_previous_public_key_pems={},
            report_active_key_id="shadow-report-v1",
            report_active_public_key_pem=public_key.decode("utf-8"),
            report_previous_public_key_pems={},
            signer_command=PRODUCTION_SHADOW_SIGNER_COMMAND,
            report_producer_service_id="shadow.session.production.v1",
            report_producer_instance_id="shadow-session-primary",
            timeout_seconds=5.0,
        ),
    )
    receipt = install_runtime_deployment_profile(
        profile,
        runtime_root=runtime_root,
        environ={},
        schema_bootstrap_reason="strategy live completion e2e",
    )

    protected_signer = Ed25519CompletionAttestationSigner(
        key_id=key_authority.keyring.active_key_id,
        client=SecureShadowSigningClient(
            command=PRODUCTION_SHADOW_SIGNER_COMMAND,
            key_id=key_authority.keyring.active_key_id,
            timeout_seconds=5.0,
        ),
    )
    from rquant.runtime_builder_strategy import strategy_live_builder

    first_step = strategy_live_builder(
        clock=lambda: SESSION_CLOSE + timedelta(seconds=3),
        completion_attestation_signer=protected_signer,
        completion_attestation_active_key_id=key_authority.keyring.active_key_id,
    )(manifest)
    first_step()
    source = ReadonlyStrategyRunnerSignalSource(
        source_id=service_id,
        path=runner_state_path,
        expected_strategy_spec_fingerprint=(
            BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)
            .load_spec("n_shape", 1)
            .spec_fingerprint
        ),
        expected_evaluator_contract_fingerprint=strategy.executable_fingerprint,
    )
    route_runner_signals(
        source_id=service_id,
        source=source,
        bus=SignalBusStore(signal_bus_path),
        cursors=SignalRouteCursorStore(
            signal_bus_path,
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=SESSION_CLOSE + timedelta(seconds=4),
        target_resolver=lambda _signal: RoutingDecision.no_target(
            routing_policy_fingerprint=POLICY,
            reason_code="shadow_only",
        ),
        limit=10,
    )

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            value = SESSION_CLOSE + timedelta(seconds=5)
            return value if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(builtin_module, "datetime", _FixedDateTime)
    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )
    args = build_parser().parse_args(
        [
            "--manifest",
            str(runtime_root / "current" / "manifests" / f"{instance}.json"),
            "--control-root",
            str(runtime_root / "control" / "strategies" / instance),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            receipt.generation_hash,
            "--expected-kind",
            RuntimeServiceKind.STRATEGY_LIVE.value,
            "--once",
        ]
    )

    assert run_runtime_service_main(args) == 0
    completion = source.read_completion_receipt(trade_date=TRADE_DATE)
    assert completion.completion_attestation is not None
    assert completion.completion_attestation.key_id == key_authority.keyring.active_key_id
    assert completion_keyring.verify(completion.completion_attestation)
    assert completion.producer_service_id == service_id
    assert completion.producer_instance_id == instance
