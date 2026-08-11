from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.daily_close_gateway import DailyCloseGateway, DailyCloseGatewayConfig
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_bundle import (
    RuntimeDeploymentReceipt,
    activate_runtime_deployment_generation,
    load_current_runtime_deployment_receipt,
    load_runtime_deployment_generation_receipt,
)
from rquant.runtime_deployment_profile import install_runtime_deployment_profile
from rquant.runtime_deployment_rollout import (
    RuntimeDeploymentRolloutError,
    SystemdRuntimeRolloutController,
    build_runtime_generation_health_probe,
    rollout_runtime_deployment,
)
from rquant.runtime_production_profile import build_production_runtime_profile
from rquant.runtime_service_control import RuntimeServiceHeartbeat, RuntimeServiceStatus
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
    load_runtime_service_manifest,
)
from rquant.strict_json import canonical_json_bytes
from tests.daily_ed25519_support import (
    DailyEd25519SocketTestAuthority,
    start_daily_ed25519_socket_authority,
)
from tests.unit.test_daily_close_gateway import AVAILABLE_AT, OBSERVED_AT, TRADE_DATE, _snapshot
from tests.unit.test_runtime_production_profile import _inputs, _retention_writer_capability

COMMIT = "a" * 40
NOW = datetime(2026, 8, 3, 10, tzinfo=UTC)
ServiceMainRunner = Callable[[RuntimeDeploymentReceipt, str], None]


class _CredentialTransaction:
    sealed_instances: tuple[str, ...] = ()

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


class _CredentialRecovery:
    outcome = "none"
    transaction_id = None


def _runtime_environ() -> dict[str, str]:
    return {
        "TUSHARE_TOKEN_MAIN": "tushare-secret",
        "PUSHDEER_KEYS": "pushdeer-secret",
        "PUSHPLUS_TOKENS": "pushplus-secret",
        "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _retention_writer_capability(),
        "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-v1",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": "cHJpdmF0ZS1rZXk=",
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY": "ssh-ed25519 AAAAtest reference-source",
    }


class _SystemctlStateMachine:
    def __init__(
        self,
        *,
        receipt,
        fail_once_unit: str | None = None,
        manifest_mutator=None,
        service_main_runner: ServiceMainRunner | None = None,
    ) -> None:
        self.receipt = receipt
        self.fail_once_unit = fail_once_unit
        self.manifest_mutator = manifest_mutator
        self.service_main_runner = service_main_runner
        self._mutation_generation_hash = receipt.generation_hash
        self.installed = set(receipt.unit_mapping.values())
        self.active: dict[str, bool] = {unit: False for unit in self.installed}
        self.enabled: set[str] = set()
        self.calls: list[tuple[str, ...]] = []
        self._monotonic = 0.0

    def monotonic(self) -> float:
        self._monotonic += 1.0
        return self._monotonic

    def activate_receipt(self, receipt) -> None:
        self.receipt = receipt
        self.installed = set(receipt.unit_mapping.values())
        for unit in self.installed:
            self.active.setdefault(unit, False)

    def __call__(self, arguments: tuple[str, ...]) -> int:
        self.calls.append(arguments)
        command = arguments[0]
        if command == "daemon-reload":
            return 0
        if command == "restart":
            unit = arguments[1]
            if unit not in self.installed:
                return 5
            if unit == self.fail_once_unit:
                self.fail_once_unit = None
                return 0
            service_id = self._service_id_for_unit(unit)
            if service_id == "daily.pipeline.orchestrator.shadow.v1":
                if self.service_main_runner is None:
                    raise AssertionError("daily orchestrator must run through runtime_service_main")
                self._maybe_mutate_manifest(unit)
                self.service_main_runner(self.receipt, unit)
            else:
                self._publish_heartbeat(unit)
            return 0
        if command == "stop":
            unit = arguments[1]
            if unit not in self.installed:
                return 5
            self.active[unit] = False
            return 0
        if arguments[:2] == ("is-active", "--quiet"):
            unit = arguments[2]
            return 0 if self.active.get(unit, False) else 3
        if command in {"enable", "disable"}:
            raise AssertionError("rollout must not enable or disable timers")
        return 9

    def _service_id_for_unit(self, unit: str) -> str:
        return next(
            service_id
            for service_id, mapped_unit in self.receipt.unit_mapping.items()
            if mapped_unit == unit
        )

    def _maybe_mutate_manifest(self, unit: str) -> None:
        if (
            self.manifest_mutator is None
            or self.receipt.generation_hash != self._mutation_generation_hash
        ):
            return
        service_id = self._service_id_for_unit(unit)
        instance = self.receipt.instance_mapping[service_id]
        manifest_path = self.receipt.runtime_root / "current" / "manifests" / f"{instance}.json"
        manifest = load_runtime_service_manifest(
            manifest_path,
            expected_commit=self.receipt.producer_commit,
            expected_generation=self.receipt.generation_hash,
        )
        mutated = self.manifest_mutator(manifest)
        if mutated == manifest:
            return
        manifest_path.write_bytes(canonical_json_bytes(mutated.model_dump(mode="json")))
        manifest_path.chmod(0o600)

    def _publish_heartbeat(self, unit: str) -> None:
        service_id = self._service_id_for_unit(unit)
        instance = self.receipt.instance_mapping[service_id]
        manifest = load_runtime_service_manifest(
            self.receipt.runtime_root / "current" / "manifests" / f"{instance}.json",
            expected_commit=self.receipt.producer_commit,
            expected_generation=self.receipt.generation_hash,
        )
        if (
            self.manifest_mutator is not None
            and self.receipt.generation_hash == self._mutation_generation_hash
        ):
            manifest = self.manifest_mutator(manifest)
        if manifest.service_kind is RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR:
            from rquant.runtime_builder_daily_orchestrator import (
                DailyPipelineOrchestratorSettings,
            )

            settings = DailyPipelineOrchestratorSettings.model_validate(dict(manifest.settings))
            if settings.service_owner != manifest.service_id:
                raise RuntimeError("daily orchestrator manifest authority is wrong")
        control_bucket = {
            RuntimeServiceKind.REFERENCE_SLOW_SOURCE: "reference-slow-sources",
            RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: "reference-slow-publishers",
            RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: "auction-universe-publishers",
            RuntimeServiceKind.AUCTION_MATCH_SOURCE: "auction-match-sources",
            RuntimeServiceKind.MARKET_MINUTE_SOURCE: "market-minute-sources",
            RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: "watchlist-quote-sources",
            RuntimeServiceKind.DAILY_CLOSE_SOURCE: "daily-close-sources",
            RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: "daily-orchestrators",
            RuntimeServiceKind.SHADOW_SESSION: "shadow-sessions",
            RuntimeServiceKind.CANDIDATE_PUBLISHER: "candidates",
            RuntimeServiceKind.FEATURE_LIVE: "features",
            RuntimeServiceKind.STRATEGY_LIVE: "strategies",
            RuntimeServiceKind.SIGNAL_ROUTER: "signal-routers",
            RuntimeServiceKind.NOTIFIER: "notifiers",
            RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: "paper-constraints",
            RuntimeServiceKind.PAPER_BROKER: "paper-brokers",
            RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: "runtime-health-publishers",
            RuntimeServiceKind.LAB_JOBS_PUBLISHER: "lab-jobs-publishers",
            RuntimeServiceKind.LAB_ARTIFACT_CATALOG: "artifact-catalogs",
            RuntimeServiceKind.ARTIFACT_RETENTION: "artifact-retention",
            RuntimeServiceKind.PROMOTIONS_PUBLISHER: "promotions-publishers",
            RuntimeServiceKind.SERVING_PUBLISHER: "serving-publishers",
        }[manifest.service_kind]
        completion_sources = {
            RuntimeServiceKind.DAILY_CLOSE_SOURCE: {"daily_close": "0" * 64},
            RuntimeServiceKind.SHADOW_SESSION: {"shadow_session": "4" * 64},
            RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: {
                "daily_pipeline_completion_receipt": "1" * 64
            },
            RuntimeServiceKind.ARTIFACT_RETENTION: {"artifact_gc_health": "2" * 64},
        }
        oneshot = manifest.service_kind in {
            RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
            RuntimeServiceKind.ARTIFACT_RETENTION,
        }
        self.active[unit] = not oneshot
        heartbeat = RuntimeServiceHeartbeat(
            service_id=manifest.service_id,
            spec_fingerprint=manifest.service_spec.identity,
            run_id="3" * 64,
            generation=1,
            status=RuntimeServiceStatus.STOPPED if oneshot else RuntimeServiceStatus.RUNNING,
            started_at=NOW,
            heartbeat_at=NOW,
            last_success_at=NOW,
            stopped_at=NOW if oneshot else None,
            stop_reason="loop completed" if oneshot else None,
            total_successes=1,
            source_generations=completion_sources.get(manifest.service_kind, {}),
        )
        heartbeat_root = (
            self.receipt.runtime_root / "control" / control_bucket / instance / "heartbeats"
        )
        heartbeat_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        path = heartbeat_root / f"{canonical_sha256({'service_id': manifest.service_id})}.json"
        path.write_bytes(canonical_json_bytes(heartbeat.model_dump(mode="json")))
        path.chmod(0o600)


def _publish_daily_close_source(runtime_root: Path) -> None:
    gateway = DailyCloseGateway(
        spool=LiveBatchSpool(runtime_root / "live" / "daily-close"),
        fetcher=lambda _request: _snapshot(),
        config=DailyCloseGatewayConfig(
            producer_version="daily-close-e2e-v1",
            producer_commit=COMMIT,
        ),
        completion_clock=lambda: AVAILABLE_AT,
    )
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)


def _runtime_service_main_runner(monkeypatch: pytest.MonkeyPatch) -> ServiceMainRunner:
    from rquant.runtime_service_builtin import build_builtin_registry as real_registry
    from rquant.runtime_service_entrypoint import run_runtime_service_manifest as real_run_manifest
    from rquant.runtime_service_main import build_parser, run

    def build_registry(**kwargs: object) -> object:
        return real_registry(**kwargs, clock=lambda: NOW)  # type: ignore[arg-type]

    def run_manifest(
        manifest: RuntimeServiceManifest,
        *,
        registry: object,
        control_root: Path,
        stop_event: object,
        max_iterations: int | None,
    ) -> object:
        return real_run_manifest(
            manifest,
            registry=registry,  # type: ignore[arg-type]
            control_root=control_root,
            stop_event=stop_event,  # type: ignore[arg-type]
            max_iterations=max_iterations,
            clock=lambda: NOW,
        )

    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr("rquant.runtime_service_main.build_builtin_registry", build_registry)
    monkeypatch.setattr(
        "rquant.runtime_service_main.run_runtime_service_manifest",
        run_manifest,
    )
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )

    def runner(receipt: RuntimeDeploymentReceipt, unit: str) -> None:
        service_id = next(
            candidate
            for candidate, mapped_unit in receipt.unit_mapping.items()
            if mapped_unit == unit
        )
        instance = receipt.instance_mapping[service_id]
        control_root = receipt.runtime_root / "control" / "daily-orchestrators" / instance
        args = build_parser().parse_args(
            [
                "--manifest",
                str(receipt.runtime_root / "current" / "manifests" / f"{instance}.json"),
                "--control-root",
                str(control_root),
                "--expected-commit",
                receipt.producer_commit,
                "--expected-generation",
                receipt.generation_hash,
                "--expected-kind",
                RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR.value,
                "--once",
            ]
        )
        assert run(args) == 0

    return runner


def _install_two_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[RuntimeDeploymentReceipt, RuntimeDeploymentReceipt, DailyEd25519SocketTestAuthority]:
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._recover_runtime_credentials",
        lambda **_kwargs: _CredentialRecovery(),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda _credentials: _CredentialTransaction(),
    )
    authority = start_daily_ed25519_socket_authority(tmp_path / "daily-receipt")
    inputs = _inputs(tmp_path).model_copy(
        update={
            "daily_receipt_active_key_id": authority.key_id,
            "daily_receipt_active_public_key_pem": authority.public_key_pem,
            "daily_receipt_signer_socket_endpoint": authority.endpoint,
            "daily_receipt_trusted_keyring_path": tmp_path / "daily-receipt-trusted-keys.json",
        }
    )
    baseline = build_production_runtime_profile(inputs)
    previous = install_runtime_deployment_profile(
        baseline,
        runtime_root=inputs.runtime_root,
        environ=_runtime_environ(),
        schema_bootstrap_reason="daily rollout e2e bootstrap",
    )
    changed = build_production_runtime_profile(
        inputs.model_copy(update={"schema_rollout_stage_timeout_seconds": 601})
    )
    current = install_runtime_deployment_profile(
        changed,
        runtime_root=inputs.runtime_root,
        environ=_runtime_environ(),
    )
    _publish_daily_close_source(inputs.runtime_root)
    return previous, current, authority


def _install_changed_generation(
    tmp_path: Path,
    *,
    authority: DailyEd25519SocketTestAuthority,
    timeout_seconds: int,
    forged_public_key: bool = False,
) -> RuntimeDeploymentReceipt:
    inputs = _inputs(tmp_path).model_copy(
        update={
            "daily_receipt_active_key_id": authority.key_id,
            "daily_receipt_active_public_key_pem": (
                "forged-daily-public-key" if forged_public_key else authority.public_key_pem
            ),
            "daily_receipt_signer_socket_endpoint": authority.endpoint,
            "daily_receipt_trusted_keyring_path": tmp_path / "daily-receipt-trusted-keys.json",
        }
    )
    changed = build_production_runtime_profile(
        inputs.model_copy(update={"schema_rollout_stage_timeout_seconds": timeout_seconds})
    )
    receipt = install_runtime_deployment_profile(
        changed,
        runtime_root=inputs.runtime_root,
        environ=_runtime_environ(),
    )
    _publish_daily_close_source(inputs.runtime_root)
    return receipt


@pytest.fixture
def daily_rollout_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[RuntimeDeploymentReceipt, RuntimeDeploymentReceipt, DailyEd25519SocketTestAuthority]:
    generations = _install_two_generations(tmp_path, monkeypatch)
    try:
        yield generations
    finally:
        generations[2].stop()


def _mutate_daily_orchestrator_manifest(
    manifest: RuntimeServiceManifest,
    *,
    wrong_authority: bool = False,
) -> RuntimeServiceManifest:
    if manifest.service_kind is not RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR:
        return manifest
    payload = manifest.model_dump(mode="python")
    settings = dict(payload["settings"])
    if wrong_authority:
        settings["service_owner"] = "daily.pipeline.orchestrator.forged.v1"
    payload["settings"] = settings
    return RuntimeServiceManifest.model_validate(payload)


def _load_bound_current(receipt):
    return load_current_runtime_deployment_receipt(
        receipt.runtime_root,
        expected_commit=receipt.producer_commit,
        expected_profile_id=receipt.deployment_profile_id,
    )


def _rollout_expect_rollback(
    *,
    current,
    previous,
    state: _SystemctlStateMachine,
) -> None:
    controller = SystemdRuntimeRolloutController(
        command_runner=state,
        health_probe=build_runtime_generation_health_probe(clock=lambda: NOW),
        monotonic_clock=state.monotonic,
        sleep=lambda _seconds: None,
    )

    with pytest.raises(RuntimeDeploymentRolloutError, match="rolled back"):
        rollout_runtime_deployment(
            current,
            controller=controller,
            current_receipt_loader=lambda: _load_bound_current(current),
            previous_receipt_loader=lambda generation: load_runtime_deployment_generation_receipt(
                current.runtime_root,
                generation_hash=generation,
            ),
            previous_generation_activator=lambda receipt: (
                activate_runtime_deployment_generation(
                    receipt.runtime_root,
                    generation_hash=receipt.generation_hash,
                    expected_commit=receipt.producer_commit,
                    expected_profile_id=receipt.deployment_profile_id,
                ),
                state.activate_receipt(receipt),
            ),
            audit_root=current.runtime_root / "control" / f"rollout-audit-{id(state)}",
            health_timeout_seconds=10,
            clock=lambda: NOW,
        )

    assert _load_bound_current(previous).generation_hash == previous.generation_hash
    assert state.enabled == set()
    assert all("rquant-daily" not in " ".join(call) for call in state.calls)


def test_clean_production_profile_rollout_and_health_rollback_state_machine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    daily_rollout_generations: tuple[
        RuntimeDeploymentReceipt,
        RuntimeDeploymentReceipt,
        DailyEd25519SocketTestAuthority,
    ],
) -> None:
    previous, current, authority = daily_rollout_generations
    service_main_runner = _runtime_service_main_runner(monkeypatch)
    state = _SystemctlStateMachine(
        receipt=current,
        service_main_runner=service_main_runner,
    )
    controller = SystemdRuntimeRolloutController(
        command_runner=state,
        health_probe=build_runtime_generation_health_probe(clock=lambda: NOW),
        monotonic_clock=state.monotonic,
        sleep=lambda _seconds: None,
    )

    audit = rollout_runtime_deployment(
        current,
        controller=controller,
        current_receipt_loader=lambda: _load_bound_current(current),
        previous_receipt_loader=lambda generation: load_runtime_deployment_generation_receipt(
            current.runtime_root,
            generation_hash=generation,
        ),
        previous_generation_activator=lambda receipt: (
            activate_runtime_deployment_generation(
                receipt.runtime_root,
                generation_hash=receipt.generation_hash,
                expected_commit=receipt.producer_commit,
                expected_profile_id=receipt.deployment_profile_id,
            ),
            state.activate_receipt(receipt),
        ),
        audit_root=current.runtime_root / "control" / "rollout-audit",
        health_timeout_seconds=10,
        clock=lambda: NOW,
    )

    assert audit.status == "succeeded"
    assert state.enabled == set()
    assert all("rquant-daily" not in " ".join(call) for call in state.calls)

    failing_candidate = _install_changed_generation(
        tmp_path,
        authority=authority,
        timeout_seconds=602,
    )
    failing_state = _SystemctlStateMachine(
        receipt=failing_candidate,
        fail_once_unit=failing_candidate.unit_mapping["market-minute.source.v1"],
        service_main_runner=service_main_runner,
    )
    failing_controller = SystemdRuntimeRolloutController(
        command_runner=failing_state,
        health_probe=build_runtime_generation_health_probe(clock=lambda: NOW),
        monotonic_clock=failing_state.monotonic,
        sleep=lambda _seconds: None,
    )
    activate_runtime_deployment_generation(
        failing_candidate.runtime_root,
        generation_hash=failing_candidate.generation_hash,
        expected_commit=failing_candidate.producer_commit,
        expected_profile_id=failing_candidate.deployment_profile_id,
    )

    with pytest.raises(RuntimeDeploymentRolloutError, match="rolled back"):
        rollout_runtime_deployment(
            failing_candidate,
            controller=failing_controller,
            current_receipt_loader=lambda: _load_bound_current(failing_candidate),
            previous_receipt_loader=lambda generation: load_runtime_deployment_generation_receipt(
                failing_candidate.runtime_root,
                generation_hash=generation,
            ),
            previous_generation_activator=lambda receipt: (
                activate_runtime_deployment_generation(
                    receipt.runtime_root,
                    generation_hash=receipt.generation_hash,
                    expected_commit=receipt.producer_commit,
                    expected_profile_id=receipt.deployment_profile_id,
                ),
                failing_state.activate_receipt(receipt),
            ),
            audit_root=failing_candidate.runtime_root / "control" / "rollout-audit-failure",
            health_timeout_seconds=10,
            clock=lambda: NOW,
        )

    assert _load_bound_current(current).generation_hash == current.generation_hash
    assert failing_state.enabled == set()
    assert all("rquant-daily" not in " ".join(call) for call in failing_state.calls)


def test_rollout_rolls_back_when_real_daily_stage_child_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    daily_rollout_generations: tuple[
        RuntimeDeploymentReceipt,
        RuntimeDeploymentReceipt,
        DailyEd25519SocketTestAuthority,
    ],
) -> None:
    _previous, current, _authority = daily_rollout_generations
    candidate_authority = start_daily_ed25519_socket_authority(
        tmp_path / "stopped-candidate-authority",
        key_id="daily-stopped-candidate-v1",
    )
    try:
        candidate_authority.stop()
        failing_candidate = _install_changed_generation(
            tmp_path,
            authority=candidate_authority,
            timeout_seconds=602,
        )
        state = _SystemctlStateMachine(
            receipt=failing_candidate,
            service_main_runner=_runtime_service_main_runner(monkeypatch),
        )
        activate_runtime_deployment_generation(
            failing_candidate.runtime_root,
            generation_hash=failing_candidate.generation_hash,
            expected_commit=failing_candidate.producer_commit,
            expected_profile_id=failing_candidate.deployment_profile_id,
        )

        _rollout_expect_rollback(
            current=failing_candidate,
            previous=current,
            state=state,
        )
    finally:
        candidate_authority.stop()


def test_rollout_rejects_daily_orchestrator_manifest_with_wrong_profile_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    daily_rollout_generations: tuple[
        RuntimeDeploymentReceipt,
        RuntimeDeploymentReceipt,
        DailyEd25519SocketTestAuthority,
    ],
) -> None:
    previous, current, _authority = daily_rollout_generations
    service_main_runner = _runtime_service_main_runner(monkeypatch)
    state = _SystemctlStateMachine(
        receipt=current,
        manifest_mutator=lambda manifest: _mutate_daily_orchestrator_manifest(
            manifest,
            wrong_authority=True,
        ),
        service_main_runner=service_main_runner,
    )

    _rollout_expect_rollback(current=current, previous=previous, state=state)


def test_rollout_rolls_back_when_socket_authority_signature_uses_wrong_keyring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    daily_rollout_generations: tuple[
        RuntimeDeploymentReceipt,
        RuntimeDeploymentReceipt,
        DailyEd25519SocketTestAuthority,
    ],
) -> None:
    _previous, current, authority = daily_rollout_generations
    failing_candidate = _install_changed_generation(
        tmp_path,
        authority=authority,
        timeout_seconds=602,
        forged_public_key=True,
    )
    state = _SystemctlStateMachine(
        receipt=failing_candidate,
        service_main_runner=_runtime_service_main_runner(monkeypatch),
    )
    activate_runtime_deployment_generation(
        failing_candidate.runtime_root,
        generation_hash=failing_candidate.generation_hash,
        expected_commit=failing_candidate.producer_commit,
        expected_profile_id=failing_candidate.deployment_profile_id,
    )

    _rollout_expect_rollback(
        current=failing_candidate,
        previous=current,
        state=state,
    )
