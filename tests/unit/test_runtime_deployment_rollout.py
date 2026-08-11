from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant import runtime_deployment_rollout as rollout_module
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_bundle import RuntimeDeploymentReceipt
from rquant.runtime_deployment_rollout import (
    RuntimeDeploymentRolloutAudit,
    RuntimeDeploymentRolloutError,
    RuntimeSchemaRetireObservation,
    SystemdRuntimeRolloutController,
    _write_audit,
    build_runtime_generation_health_probe,
    preview_runtime_schema_retirement,
    rollback_runtime_deployment,
    rollout_runtime_deployment,
)
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeServiceStatus
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

COMMIT = "a" * 40
PROFILE = "b" * 64


def _receipt(
    root: Path, *, generation: str, previous: str | None = None
) -> RuntimeDeploymentReceipt:
    minute_instance = f"svc-{'1' * 64}"
    strategy_instance = f"svc-{'2' * 64}"
    return RuntimeDeploymentReceipt(
        runtime_root=root,
        producer_commit=COMMIT,
        generation_hash=generation,
        deployment_profile_id=PROFILE,
        previous_generation_hash=previous,
        instance_mapping={
            "z-strategy": strategy_instance,
            "a-market-minute": minute_instance,
        },
        unit_mapping={
            "z-strategy": f"rquant-runtime-strategy@{strategy_instance}.service",
            "a-market-minute": f"rquant-runtime-market-minute@{minute_instance}.service",
        },
    )


def _runtime_unit_receipt(
    root: Path,
    *,
    generation: str = "f" * 64,
    previous: str | None = None,
) -> RuntimeDeploymentReceipt:
    service_ids = (
        "daily-close.source.v1",
        "daily.pipeline.orchestrator.shadow.v1",
        "shadow.session.production.v1",
        "artifact-retention.primary.v1",
    )
    instances = {service_id: f"svc-{canonical_sha256(service_id)}" for service_id in service_ids}
    return RuntimeDeploymentReceipt(
        runtime_root=root,
        producer_commit=COMMIT,
        generation_hash=generation,
        deployment_profile_id=PROFILE,
        previous_generation_hash=previous,
        instance_mapping=instances,
        unit_mapping={
            "daily-close.source.v1": (
                f"rquant-runtime-daily-close@{instances['daily-close.source.v1']}.service"
            ),
            "daily.pipeline.orchestrator.shadow.v1": (
                "rquant-runtime-daily-orchestrator@"
                f"{instances['daily.pipeline.orchestrator.shadow.v1']}.service"
            ),
            "shadow.session.production.v1": (
                f"rquant-runtime-shadow@{instances['shadow.session.production.v1']}.service"
            ),
            "artifact-retention.primary.v1": "rquant-artifact-retention.service",
        },
    )


class FakeController:
    def __init__(self, *, unhealthy_unit: str | None = None) -> None:
        self.unhealthy_unit = unhealthy_unit
        self.calls: list[tuple[str, str | None]] = []

    def daemon_reload(self) -> None:
        self.calls.append(("daemon_reload", None))

    def restart(self, unit: str) -> None:
        self.calls.append(("restart", unit))

    def stop(self, unit: str) -> None:
        self.calls.append(("stop", unit))

    def wait_healthy(
        self,
        unit: str,
        *,
        receipt: RuntimeDeploymentReceipt,
        timeout_seconds: float,
    ) -> None:
        self.calls.append(("health", unit))
        assert timeout_seconds > 0
        if unit == self.unhealthy_unit:
            self.unhealthy_unit = None
            raise RuntimeError("service never became healthy")


def _daily_orchestrator_probe_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    completion_receipt_id: str = "rcpt-backup",
    missing_receipt_stage: str | None = None,
    mutate_receipt: tuple[str, str, object] | None = None,
    nonzero_stage: str | None = None,
    previous_key_stage: str | None = None,
    missing_stage: str | None = None,
    misordered_stages: bool = False,
) -> tuple[rollout_module.HealthProbe, RuntimeDeploymentReceipt, str, list[dict[str, object]]]:
    import rquant.daily_pipeline_ledger as daily_pipeline_ledger_module
    import rquant.daily_pipeline_orchestrator as daily_pipeline_orchestrator_module
    import rquant.runtime_builder_daily_orchestrator as daily_builder_module
    import rquant.runtime_deployment_profile as deployment_profile_module

    receipt = _runtime_unit_receipt(tmp_path / "runtime")
    unit = receipt.unit_mapping["daily.pipeline.orchestrator.shadow.v1"]
    instance = receipt.instance_mapping["daily.pipeline.orchestrator.shadow.v1"]
    mode_shadow = object()
    run_succeeded = object()
    stage_succeeded = object()
    profile_hash = receipt.deployment_profile_id
    command_manifest = {"stages": ["raw_capture", "screen", "backup"]}
    command_manifest_hash = canonical_sha256(command_manifest)
    namespace_id = "shadow-namespace"
    source_generation_id = "source-generation"
    source_content_hash = "1" * 64
    stage_ids = ("raw_capture", "screen", "backup")
    stage_dependencies = {
        "raw_capture": (),
        "screen": ("raw_capture",),
        "backup": ("screen",),
    }
    settings_stages = ("screen", "raw_capture", "backup") if misordered_stages else stage_ids
    if missing_stage is not None:
        settings_stages = tuple(stage_id for stage_id in settings_stages if stage_id != missing_stage)
    storage_root = tmp_path / "storage"
    namespace_root = storage_root / namespace_id
    deployment_profile_path = tmp_path / "deployment-profile.json"
    command_manifest_path = namespace_root / "command-manifest.json"
    effects_root = namespace_root / "effects"
    deployment_profile_path.write_text("{}", encoding="utf-8")
    command_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    command_manifest_path.write_text(json.dumps(command_manifest), encoding="utf-8")

    def _result(stage_id: str) -> dict[str, str]:
        return {
            "content_hash": canonical_sha256({"stage": stage_id, "kind": "result"}),
            "uri": f"file:///{stage_id}.json",
        }

    def _output_identity(stage_id: str) -> str:
        return canonical_sha256({"stage": stage_id, "kind": "output"})

    def _receipt_payload(stage_id: str, order: int, dependency_receipt_ids: tuple[str, ...]) -> dict[str, object]:
        key_id = "previous-key" if stage_id == previous_key_stage else "active-key"
        exit_status = 1 if stage_id == nonzero_stage else 0
        payload = {
            "contract": "daily-shadow-stage-completion-receipt/v1",
            "receipt_id": f"rcpt-{stage_id}",
            "mode": "shadow",
            "run_id": "run-1",
            "stage_id": stage_id,
            "effect_id": f"effect-{stage_id}",
            "idempotency_key": f"idem-{stage_id}",
            "input_identity": "input-1",
            "source_generation_id": source_generation_id,
            "source_content_hash": source_content_hash,
            "command_manifest_hash": command_manifest_hash,
            "profile_hash": profile_hash,
            "command_identity": f"adapter-{stage_id}",
            "stage_order": order,
            "exit_status": exit_status,
            "output_identity": _output_identity(stage_id),
            "dependency_receipt_ids": list(dependency_receipt_ids),
            "previous_receipt_hash": canonical_sha256(
                {
                    "contract": "daily-shadow-stage-previous-receipt-chain/v1",
                    "dependency_receipt_ids": dependency_receipt_ids,
                }
            ),
            "key_id": key_id,
            "signature_algorithm": "ed25519",
            "signature": f"sig:{key_id}:{stage_id}:{exit_status}:{_output_identity(stage_id)}",
            "result": _result(stage_id),
            "issued_at": "2026-08-02T01:00:00Z",
        }
        return payload

    receipt_payloads: dict[str, dict[str, object]] = {}
    receipt_paths: dict[str, Path] = {}
    stage_receipt_ids: dict[str, str] = {}
    effect_by_stage: dict[str, SimpleNamespace] = {}
    receipt_by_id: dict[str, SimpleNamespace] = {}
    dependency_receipt_ids: dict[str, tuple[str, ...]] = {}
    for order, stage_id in enumerate(stage_ids):
        deps = tuple(stage_receipt_ids[item] for item in stage_dependencies[stage_id])
        dependency_receipt_ids[stage_id] = deps
        payload = _receipt_payload(stage_id, order, deps)
        receipt_payloads[stage_id] = payload
        stage_receipt_ids[stage_id] = str(payload["receipt_id"])
        receipt_path = effects_root / stage_id / f"{payload['receipt_id']}.json"
        receipt_paths[stage_id] = receipt_path
        if stage_id != missing_receipt_stage:
            receipt_path.parent.mkdir(parents=True, exist_ok=True)
            receipt_path.write_text(json.dumps(payload), encoding="utf-8")
        effect_by_stage[stage_id] = SimpleNamespace(
            intent=SimpleNamespace(
                receipt_locator=str(receipt_path),
                effect_id=payload["effect_id"],
                idempotency_key=payload["idempotency_key"],
                adapter_identity=payload["command_identity"],
            )
        )

    def _to_result(data: dict[str, str]) -> SimpleNamespace:
        return SimpleNamespace(**data)

    def _to_receipt(payload: dict[str, object]) -> SimpleNamespace:
        return SimpleNamespace(
            **{
                **payload,
                "mode": mode_shadow,
                "dependency_receipt_ids": tuple(payload["dependency_receipt_ids"]),
                "result": _to_result(payload["result"]),
            }
        )

    for stage_id, payload in receipt_payloads.items():
        receipt_by_id[str(payload["receipt_id"])] = _to_receipt(payload)

    if mutate_receipt is not None:
        stage_id, field, value = mutate_receipt
        mutated_payload = dict(receipt_payloads[stage_id])
        mutated_payload[field] = value
        receipt_paths[stage_id].write_text(json.dumps(mutated_payload), encoding="utf-8")

    backup_payload = receipt_payloads["backup"]
    backup_artifact_path = effects_root / "backup" / f"{backup_payload['effect_id']}.json"
    backup_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    backup_artifact_path.write_text(
        json.dumps(
            {
                "contract": "daily-shadow-stage-artifact/v1",
                "artifact_id": backup_payload["result"]["content_hash"],
                "mode": "shadow",
                "run_id": backup_payload["run_id"],
                "stage_id": "backup",
                "action": "store",
                "effect_id": backup_payload["effect_id"],
                "idempotency_key": backup_payload["idempotency_key"],
                "input_identity": backup_payload["input_identity"],
                "source_generation_id": source_generation_id,
                "source_content_hash": source_content_hash,
                "command_manifest_hash": command_manifest_hash,
                "profile_hash": profile_hash,
                "command_identity": backup_payload["command_identity"],
                "stage_order": backup_payload["stage_order"],
                "exit_status": 0,
                "output_identity": backup_payload["output_identity"],
                "generation_id": receipt.generation_hash,
                "output_files": [
                    {
                        "path": "/tmp/output.parquet",
                        "sha256": backup_payload["output_identity"],
                    }
                ],
                "dependency_receipt_ids": list(dependency_receipt_ids["backup"]),
                "artifact_path": "/tmp/artifact.json",
                "created_at": "2026-08-02T01:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    recorded_keyrings: list[dict[str, object]] = []

    class RecordingKeyring:
        def __init__(
            self,
            *,
            active_key_id: str,
            active_public_key: bytes,
            previous_public_keys: dict[str, bytes] | None = None,
        ) -> None:
            recorded_keyrings.append(
                {
                    "active_key_id": active_key_id,
                    "active_public_key": active_public_key,
                    "previous_public_keys": previous_public_keys,
                }
            )
            self.active_key_id = active_key_id

        def verify_stage_receipt(self, receipt_obj: SimpleNamespace, *, require_active: bool = False) -> bool:
            expected_signature = (
                f"sig:{receipt_obj.key_id}:{receipt_obj.stage_id}:"
                f"{receipt_obj.exit_status}:{receipt_obj.output_identity}"
            )
            if require_active and receipt_obj.key_id != self.active_key_id:
                return False
            return receipt_obj.signature == expected_signature

        def verify_run_completion_receipt(
            self,
            receipt_obj: SimpleNamespace,
            *,
            require_active: bool = False,
        ) -> bool:
            if require_active and receipt_obj.key_id != self.active_key_id:
                return False
            return receipt_obj.signature == f"sig:{receipt_obj.key_id}:run:{receipt_obj.exit_status}"

    class FakeStorageProfile:
        @staticmethod
        def create(*, root: Path, mode: object, profile_hash: str) -> SimpleNamespace:
            assert root == storage_root
            assert mode is mode_shadow
            assert profile_hash == receipt.deployment_profile_id
            return SimpleNamespace(
                namespace_id=namespace_id,
                command_manifest_path=command_manifest_path,
                namespace_root=namespace_root,
            )

    class FakeLedger:
        def __init__(self, *, storage_profile: object, service_owner: str) -> None:
            assert service_owner == "daily.pipeline.orchestrator.shadow.v1"
            self.storage_profile = storage_profile

        def receipt(self, receipt_id: str) -> SimpleNamespace | None:
            return receipt_by_id.get(receipt_id)

        def run(self, run_id: str) -> SimpleNamespace:
            assert run_id == "run-1"
            return SimpleNamespace(
                run_id=run_id,
                state=run_succeeded,
                input_identity="input-1",
                spec=SimpleNamespace(
                    mode=mode_shadow,
                    trade_date=datetime(2026, 8, 2, tzinfo=UTC).date(),
                    profile_hash=profile_hash,
                    command_manifest_hash=command_manifest_hash,
                    code_commit=receipt.producer_commit,
                    source_generation_id=source_generation_id,
                    source_content_hash=source_content_hash,
                ),
            )

        def stage(self, run_id: str, stage_id: str) -> SimpleNamespace:
            assert run_id == "run-1"
            if stage_id not in stage_receipt_ids:
                return SimpleNamespace(state=object(), terminal_receipt_id=None)
            return SimpleNamespace(
                state=stage_succeeded,
                terminal_receipt_id=stage_receipt_ids[stage_id],
            )

        def effect_for_stage(self, run_id: str, stage_id: str) -> SimpleNamespace | None:
            assert run_id == "run-1"
            return effect_by_stage.get(stage_id)

    class FakePipeline:
        def __init__(self, stage_ids: tuple[str, ...]) -> None:
            self.stage_ids = stage_ids

        def runtime_spec(self, stage_id: str) -> SimpleNamespace:
            return SimpleNamespace(depends_on=stage_dependencies[stage_id])

    def _receipt_model_validate(payload: dict[str, object]) -> SimpleNamespace:
        return _to_receipt(payload)

    def _artifact_model_validate(payload: dict[str, object]) -> SimpleNamespace:
        output_files = [SimpleNamespace(**item) for item in payload["output_files"]]
        return SimpleNamespace(**{**payload, "output_files": output_files})

    monkeypatch.setattr(
        daily_builder_module.DailyPipelineOrchestratorSettings,
        "model_validate",
        staticmethod(
            lambda _settings: SimpleNamespace(
                service_owner="daily.pipeline.orchestrator.shadow.v1",
                deployment_profile_path=deployment_profile_path,
                storage_root=storage_root,
                stages=settings_stages,
                receipt_active_key_id="active-key",
                receipt_active_public_key_pem="ACTIVE-PEM",
                receipt_previous_public_key_pems={"previous-key": "OLD-PEM"},
            )
        ),
    )
    monkeypatch.setattr(
        daily_builder_module,
        "Ed25519DailyShadowStageReceiptKeyring",
        RecordingKeyring,
    )
    monkeypatch.setattr(
        daily_builder_module.DailyShadowStageCompletionReceipt,
        "model_validate",
        staticmethod(_receipt_model_validate),
    )
    monkeypatch.setattr(
        daily_builder_module.DailyShadowStageArtifact,
        "model_validate",
        staticmethod(_artifact_model_validate),
    )
    monkeypatch.setattr(
        daily_builder_module,
        "load_daily_shadow_run_completion_receipt",
        lambda _storage, _receipt_id: SimpleNamespace(
            receipt_id=completion_receipt_id,
            run_id="run-1",
            mode=mode_shadow,
            trade_date=datetime(2026, 8, 2, tzinfo=UTC).date(),
            profile_hash=profile_hash,
            source_generation_id=source_generation_id,
            source_content_hash=source_content_hash,
            command_manifest_hash=command_manifest_hash,
            input_identity="input-1",
            stage_order=stage_ids,
            stage_receipt_ids=tuple(stage_receipt_ids[stage_id] for stage_id in stage_ids),
            previous_receipt_hash=canonical_sha256(
                {
                    "contract": "daily-shadow-stage-previous-receipt-chain/v1",
                    "dependency_receipt_ids": tuple(
                        stage_receipt_ids[stage_id] for stage_id in stage_ids
                    ),
                }
            ),
            final_backup_stage_id="backup",
            final_backup_receipt_id=stage_receipt_ids["backup"],
            final_backup_output_identity=backup_payload["output_identity"],
            final_backup_result=_to_result(backup_payload["result"]),
            exit_status=0,
            code_commit=receipt.producer_commit,
            key_id="active-key",
            signature_algorithm="ed25519",
            signature="sig:active-key:run:0",
        )
        if _receipt_id == "rcpt-backup"
        else (_ for _ in ()).throw(FileNotFoundError(_receipt_id)),
    )
    monkeypatch.setattr(
        daily_builder_module,
        "daily_shadow_ledger_owner",
        lambda owner: owner,
    )
    monkeypatch.setattr(
        deployment_profile_module.RuntimeDeploymentProfile,
        "model_validate",
        staticmethod(
            lambda _payload: SimpleNamespace(
                profile_id=profile_hash,
                producer_commit=receipt.producer_commit,
                manifests=(
                    SimpleNamespace(
                        service_id="daily.pipeline.orchestrator.shadow.v1",
                        manifest_fingerprint="fp-1",
                    ),
                ),
            )
        ),
    )
    monkeypatch.setattr(
        daily_pipeline_ledger_module,
        "DailyPipelineMode",
        SimpleNamespace(SHADOW=mode_shadow),
    )
    monkeypatch.setattr(
        daily_pipeline_ledger_module,
        "DailyRunState",
        SimpleNamespace(SUCCEEDED=run_succeeded),
    )
    monkeypatch.setattr(
        daily_pipeline_ledger_module,
        "DailyStageState",
        SimpleNamespace(SUCCEEDED=stage_succeeded),
    )
    monkeypatch.setattr(
        daily_pipeline_ledger_module,
        "DailyPipelineStorageProfile",
        FakeStorageProfile,
    )
    monkeypatch.setattr(
        daily_pipeline_ledger_module,
        "DailyPipelineLedger",
        FakeLedger,
    )
    monkeypatch.setattr(
        daily_pipeline_orchestrator_module,
        "DEFAULT_DAILY_CLOSE_PIPELINE",
        FakePipeline(stage_ids),
    )

    manifest = RuntimeServiceManifest(
        service_id="daily.pipeline.orchestrator.shadow.v1",
        service_kind=RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=60,
        stale_after_seconds=600,
        producer_commit=COMMIT,
        settings={
            "storage_root": str(storage_root),
            "source_spool_root": str(tmp_path / "spool"),
            "deployment_profile_path": str(deployment_profile_path),
            "mode": "shadow",
            "service_owner": "daily.pipeline.orchestrator.shadow.v1",
            "stages": list(settings_stages),
            "stage_commands": [],
            "receipt_active_key_id": "active-key",
            "receipt_active_public_key_pem": "ACTIVE-PEM",
            "receipt_previous_public_key_pems": {"previous-key": "OLD-PEM"},
            "receipt_signer_command": ["/usr/bin/false"],
            "receipt_signer_timeout_seconds": 5.0,
        },
    )
    now = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    heartbeat = SimpleNamespace(
        status=RuntimeServiceStatus.STOPPED,
        last_success_at=now,
        heartbeat_at=now,
        stop_reason="loop completed",
        consecutive_failures=0,
        total_successes=1,
        source_generations={
            "daily_pipeline_profile": profile_hash,
            "daily_pipeline_namespace": namespace_id,
            "daily_pipeline_command_manifest": command_manifest_hash,
            "daily_pipeline_completion_receipt": completion_receipt_id,
        },
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.load_runtime_service_manifest",
        lambda path, **kwargs: (
            path
            == receipt.runtime_root / "current" / "manifests" / f"{instance}.json"
            and kwargs["expected_commit"] == receipt.producer_commit
            and kwargs["expected_generation"] == receipt.generation_hash
            and manifest
        ),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.RuntimeServiceControl.read_heartbeat",
        lambda root, spec: (
            root == receipt.runtime_root / "control" / "daily-orchestrators" / instance
            and spec == manifest.service_spec
            and heartbeat
        ),
    )
    return build_runtime_generation_health_probe(clock=lambda: now), receipt, unit, recorded_keyrings


def test_rollout_orders_dependencies_and_replays_completed_audit_without_actions(
    tmp_path: Path,
) -> None:
    receipt = _receipt(tmp_path / "runtime", generation="c" * 64)
    controller = FakeController()

    first = rollout_runtime_deployment(
        receipt,
        controller=controller,
        current_receipt_loader=lambda: receipt,
        previous_receipt_loader=lambda _generation: None,
        previous_generation_activator=lambda _receipt: None,
        audit_root=tmp_path / "audit",
        health_timeout_seconds=10,
    )

    minute_unit = receipt.unit_mapping["a-market-minute"]
    strategy_unit = receipt.unit_mapping["z-strategy"]
    assert first.status == "succeeded"
    assert controller.calls == [
        ("daemon_reload", None),
        ("restart", minute_unit),
        ("health", minute_unit),
        ("restart", strategy_unit),
        ("health", strategy_unit),
    ]

    replay_controller = FakeController()
    replay = rollout_runtime_deployment(
        receipt,
        controller=replay_controller,
        current_receipt_loader=lambda: receipt,
        previous_receipt_loader=lambda _generation: None,
        previous_generation_activator=lambda _receipt: None,
        audit_root=tmp_path / "audit",
        health_timeout_seconds=10,
    )

    assert replay == first
    assert replay_controller.calls == []


def test_rollout_accepts_minimal_watchlist_quote_receipt(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    instance = f"svc-{'3' * 64}"
    unit = f"rquant-runtime-watchlist-quote@{instance}.service"
    receipt = RuntimeDeploymentReceipt(
        runtime_root=runtime_root,
        producer_commit=COMMIT,
        generation_hash="d" * 64,
        deployment_profile_id=PROFILE,
        instance_mapping={"watchlist.quote.source.v1": instance},
        unit_mapping={"watchlist.quote.source.v1": unit},
    )
    controller = FakeController()

    audit = rollout_runtime_deployment(
        receipt,
        controller=controller,
        current_receipt_loader=lambda: receipt,
        previous_receipt_loader=lambda _generation: None,
        previous_generation_activator=lambda _receipt: None,
        audit_root=tmp_path / "audit",
        health_timeout_seconds=10,
    )

    assert audit.status == "succeeded"
    assert audit.service_units == (unit,)
    assert controller.calls == [
        ("daemon_reload", None),
        ("restart", unit),
        ("health", unit),
    ]


def test_rollout_allowlist_orders_daily_shadow_and_retention_units(
    tmp_path: Path,
) -> None:
    receipt = _runtime_unit_receipt(tmp_path / "runtime")

    assert rollout_module._ordered_units(receipt) == (
        receipt.unit_mapping["daily-close.source.v1"],
        receipt.unit_mapping["shadow.session.production.v1"],
        receipt.unit_mapping["artifact-retention.primary.v1"],
        receipt.unit_mapping["daily.pipeline.orchestrator.shadow.v1"],
    )


def test_daily_orchestrator_oneshot_health_rejects_fake_completion_receipt_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe, receipt, unit, _ = _daily_orchestrator_probe_fixture(
        tmp_path,
        monkeypatch,
        completion_receipt_id="a" * 64,
    )

    assert probe(receipt, unit) is False


def test_daily_orchestrator_oneshot_health_requires_exact_signed_stage_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe, receipt, unit, recorded_keyrings = _daily_orchestrator_probe_fixture(
        tmp_path,
        monkeypatch,
    )

    assert probe(receipt, unit) is True
    assert recorded_keyrings == [
        {
            "active_key_id": "active-key",
            "active_public_key": b"ACTIVE-PEM",
            "previous_public_keys": {"previous-key": b"OLD-PEM"},
        }
    ]


@pytest.mark.parametrize(
    ("kwargs", "label"),
    (
        ({"missing_receipt_stage": "screen"}, "missing receipt"),
        (
            {
                "mutate_receipt": (
                    "backup",
                    "output_identity",
                    canonical_sha256({"mutated": "output"}),
                )
            },
            "altered output",
        ),
        (
            {
                "mutate_receipt": (
                    "backup",
                    "result",
                    {
                        "content_hash": canonical_sha256({"mutated": "result"}),
                        "uri": "file:///backup.json",
                    },
                )
            },
            "altered result",
        ),
        ({"nonzero_stage": "screen"}, "nonzero exit"),
        ({"previous_key_stage": "backup"}, "previous key"),
        ({"missing_stage": "screen"}, "missing stage"),
        ({"misordered_stages": True}, "misordered stage"),
    ),
)
def test_daily_orchestrator_oneshot_health_rejects_invalid_stage_receipt_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, object],
    label: str,
) -> None:
    probe, receipt, unit, _ = _daily_orchestrator_probe_fixture(
        tmp_path,
        monkeypatch,
        **kwargs,
    )

    assert probe(receipt, unit) is False, label


@pytest.mark.parametrize(
    ("service_id", "kind", "bucket", "required_key"),
    (
        (
            "daily-close.source.v1",
            RuntimeServiceKind.DAILY_CLOSE_SOURCE,
            "daily-close-sources",
            "daily_close",
        ),
        (
            "shadow.session.production.v1",
            RuntimeServiceKind.SHADOW_SESSION,
            "shadow-sessions",
            "shadow_session",
        ),
    ),
)
def test_rollout_health_requires_generation_receipt_for_daily_close_and_shadow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    service_id: str,
    kind: RuntimeServiceKind,
    bucket: str,
    required_key: str,
) -> None:
    receipt = _runtime_unit_receipt(tmp_path / "runtime")
    unit = receipt.unit_mapping[service_id]
    instance = receipt.instance_mapping[service_id]
    manifest = RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=60,
        stale_after_seconds=600,
        producer_commit=COMMIT,
        settings={},
    )
    now = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    heartbeat = SimpleNamespace(
        status=RuntimeServiceStatus.RUNNING,
        last_success_at=now,
        heartbeat_at=now,
        stop_reason=None,
        consecutive_failures=0,
        total_successes=1,
        source_generations={},
    )
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.load_runtime_service_manifest",
        lambda path, **kwargs: observed.update(path=path, **kwargs) or manifest,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.RuntimeServiceControl.read_heartbeat",
        lambda root, spec: observed.update(control_root=root, spec=spec) or heartbeat,
    )

    probe = build_runtime_generation_health_probe(clock=lambda: now)

    assert probe(receipt, unit) is False
    heartbeat.source_generations = {required_key: "a" * 64}
    assert probe(receipt, unit) is True
    assert observed["path"] == (receipt.runtime_root / "current" / "manifests" / f"{instance}.json")
    assert observed["control_root"] == receipt.runtime_root / "control" / bucket / instance


def test_systemd_wait_accepts_completed_oneshot_without_active_running_state(
    tmp_path: Path,
) -> None:
    receipt = _runtime_unit_receipt(tmp_path / "runtime")
    unit = receipt.unit_mapping["daily.pipeline.orchestrator.shadow.v1"]
    commands: list[tuple[str, ...]] = []
    ticks = iter((0.0, 0.1, 0.2))

    def command_runner(arguments: tuple[str, ...]) -> int:
        commands.append(arguments)
        if arguments[:2] == ("is-active", "--quiet"):
            return 3
        return 0

    controller = SystemdRuntimeRolloutController(
        command_runner=command_runner,
        health_probe=lambda observed_receipt, observed_unit: (
            observed_receipt == receipt and observed_unit == unit
        ),
        monotonic_clock=lambda: next(ticks),
        sleep=lambda _seconds: None,
        poll_interval_seconds=0.01,
    )

    controller.wait_healthy(unit, receipt=receipt, timeout_seconds=1)

    assert commands == [("is-active", "--quiet", unit)]


def test_rollout_rollback_never_stops_legacy_rquant_daily_authority(
    tmp_path: Path,
) -> None:
    previous = _runtime_unit_receipt(tmp_path / "runtime", generation="d" * 64)
    receipt = _runtime_unit_receipt(
        tmp_path / "runtime",
        generation="e" * 64,
        previous=previous.generation_hash,
    )
    controller = FakeController(
        unhealthy_unit=receipt.unit_mapping["daily.pipeline.orchestrator.shadow.v1"],
    )

    with pytest.raises(RuntimeDeploymentRolloutError, match="rolled back"):
        rollout_runtime_deployment(
            receipt,
            controller=controller,
            current_receipt_loader=lambda: receipt,
            previous_receipt_loader=lambda generation: (
                previous if generation == previous.generation_hash else None
            ),
            previous_generation_activator=lambda _receipt: None,
            audit_root=tmp_path / "audit",
            health_timeout_seconds=10,
        )

    assert ("stop", "rquant-daily.service") not in controller.calls
    assert ("stop", "rquant-daily.timer") not in controller.calls


def test_rollout_failure_stops_new_units_and_reactivates_previous_generation(
    tmp_path: Path,
) -> None:
    previous = _receipt(tmp_path / "runtime", generation="d" * 64)
    receipt = _receipt(
        tmp_path / "runtime",
        generation="e" * 64,
        previous=previous.generation_hash,
    )
    strategy_unit = receipt.unit_mapping["z-strategy"]
    controller = FakeController(unhealthy_unit=strategy_unit)
    activations: list[RuntimeDeploymentReceipt] = []

    with pytest.raises(RuntimeDeploymentRolloutError, match="rolled back"):
        rollout_runtime_deployment(
            receipt,
            controller=controller,
            current_receipt_loader=lambda: receipt,
            previous_receipt_loader=lambda generation: (
                previous if generation == previous.generation_hash else None
            ),
            previous_generation_activator=activations.append,
            audit_root=tmp_path / "audit",
            health_timeout_seconds=10,
        )

    assert activations == [previous]
    assert ("stop", strategy_unit) in controller.calls
    assert ("stop", receipt.unit_mapping["a-market-minute"]) in controller.calls
    assert controller.calls[-5:] == [
        ("daemon_reload", None),
        ("restart", previous.unit_mapping["a-market-minute"]),
        ("health", previous.unit_mapping["a-market-minute"]),
        ("restart", previous.unit_mapping["z-strategy"]),
        ("health", previous.unit_mapping["z-strategy"]),
    ]


def test_explicit_runtime_rollback_is_durable_and_idempotent(tmp_path: Path) -> None:
    previous = _receipt(tmp_path / "runtime", generation="7" * 64)
    receipt = _receipt(
        tmp_path / "runtime",
        generation="8" * 64,
        previous=previous.generation_hash,
    )
    current = [receipt]
    controller = FakeController()

    def activate(candidate: RuntimeDeploymentReceipt) -> None:
        assert candidate == previous
        current[0] = previous

    kwargs = {
        "operation_id": "9" * 64,
        "current_receipt_loader": lambda: current[0],
        "previous_receipt_loader": lambda generation: (
            previous if generation == previous.generation_hash else None
        ),
        "previous_generation_activator": activate,
        "audit_root": tmp_path / "rollback-audit",
        "health_timeout_seconds": 10,
    }
    first = rollback_runtime_deployment(receipt, controller=controller, **kwargs)

    assert first.status == "rolled_back"
    assert current == [previous]
    first_calls = tuple(controller.calls)
    replay_controller = FakeController()
    second = rollback_runtime_deployment(
        receipt,
        controller=replay_controller,
        **kwargs,
    )
    assert second == first
    assert tuple(controller.calls) == first_calls
    assert replay_controller.calls == []


def test_incomplete_running_audit_is_recovered_by_rolling_back_before_retry(
    tmp_path: Path,
) -> None:
    previous = _receipt(tmp_path / "runtime", generation="3" * 64)
    receipt = _receipt(
        tmp_path / "runtime",
        generation="4" * 64,
        previous=previous.generation_hash,
    )
    units = tuple(
        sorted(
            receipt.unit_mapping.values(),
            key=lambda unit: ("market-minute" not in unit, unit),
        )
    )
    operation_id = canonical_sha256(
        {
            "contract": "runtime-deployment-rollout/v2",
            "runtime_root": str(receipt.runtime_root),
            "profile_id": receipt.deployment_profile_id,
            "generation_hash": receipt.generation_hash,
            "previous_generation_hash": receipt.previous_generation_hash,
            "service_units": units,
            "schema_rollout_plan_ids": (),
        }
    )
    audit_root = tmp_path / "audit"
    audit_root.mkdir(mode=0o700)
    _write_audit(
        audit_root,
        RuntimeDeploymentRolloutAudit(
            operation_id=operation_id,
            profile_id=PROFILE,
            generation_hash=receipt.generation_hash,
            previous_generation_hash=previous.generation_hash,
            status="running",
            service_units=units,
        ),
    )
    controller = FakeController()
    activations: list[RuntimeDeploymentReceipt] = []

    recovered = rollout_runtime_deployment(
        receipt,
        controller=controller,
        current_receipt_loader=lambda: receipt,
        previous_receipt_loader=lambda generation: (
            previous if generation == previous.generation_hash else None
        ),
        previous_generation_activator=activations.append,
        audit_root=audit_root,
        health_timeout_seconds=10,
    )

    assert recovered.status == "rolled_back"
    assert recovered.error_type == "InterruptedRuntimeRollout"
    assert activations == [previous]
    assert controller.calls[:2] == [
        ("stop", receipt.unit_mapping["z-strategy"]),
        ("stop", receipt.unit_mapping["a-market-minute"]),
    ]
    assert controller.calls[-5:] == [
        ("daemon_reload", None),
        ("restart", previous.unit_mapping["a-market-minute"]),
        ("health", previous.unit_mapping["a-market-minute"]),
        ("restart", previous.unit_mapping["z-strategy"]),
        ("health", previous.unit_mapping["z-strategy"]),
    ]


def test_incomplete_rollout_converges_when_previous_pointer_was_already_restored(
    tmp_path: Path,
) -> None:
    previous = _receipt(tmp_path / "runtime", generation="5" * 64)
    receipt = _receipt(
        tmp_path / "runtime",
        generation="6" * 64,
        previous=previous.generation_hash,
    )
    units = tuple(
        sorted(
            receipt.unit_mapping.values(),
            key=lambda unit: ("market-minute" not in unit, unit),
        )
    )
    operation_id = canonical_sha256(
        {
            "contract": "runtime-deployment-rollout/v2",
            "runtime_root": str(receipt.runtime_root),
            "profile_id": receipt.deployment_profile_id,
            "generation_hash": receipt.generation_hash,
            "previous_generation_hash": receipt.previous_generation_hash,
            "service_units": units,
            "schema_rollout_plan_ids": (),
        }
    )
    audit_root = tmp_path / "audit"
    audit_root.mkdir(mode=0o700)
    _write_audit(
        audit_root,
        RuntimeDeploymentRolloutAudit(
            operation_id=operation_id,
            profile_id=PROFILE,
            generation_hash=receipt.generation_hash,
            previous_generation_hash=previous.generation_hash,
            status="running",
            service_units=units,
        ),
    )
    controller = FakeController()

    recovered = rollout_runtime_deployment(
        receipt,
        controller=controller,
        current_receipt_loader=lambda: previous,
        previous_receipt_loader=lambda generation: (
            previous if generation == previous.generation_hash else None
        ),
        previous_generation_activator=lambda _receipt: None,
        audit_root=audit_root,
        health_timeout_seconds=10,
    )

    assert recovered.status == "rolled_back"
    assert recovered.error_type == "InterruptedRuntimeRollout"


def test_rollout_audit_requires_one_receipt_hash_per_schema_plan() -> None:
    with pytest.raises(ValueError, match="schema.*receipt"):
        RuntimeDeploymentRolloutAudit(
            operation_id="1" * 64,
            profile_id=PROFILE,
            generation_hash="2" * 64,
            previous_generation_hash="3" * 64,
            status="running",
            service_units=(),
            schema_rollout_plan_ids=("4" * 64,),
            schema_receipt_hashes=(),
        )


def test_successful_rollout_audit_requires_observation_for_every_schema_plan() -> None:
    with pytest.raises(ValueError, match="retire observation"):
        RuntimeDeploymentRolloutAudit(
            operation_id="1" * 64,
            profile_id=PROFILE,
            generation_hash="2" * 64,
            previous_generation_hash="3" * 64,
            status="succeeded",
            service_units=(),
            schema_rollout_plan_ids=("4" * 64,),
            schema_receipt_hashes=("5" * 64,),
        )


def test_schema_retirement_status_is_bounded_before_store_scanning(tmp_path: Path) -> None:
    now = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan_ids = tuple(f"{index:064x}" for index in range(1, 66))
    hashes = tuple(f"{index + 100:064x}" for index in range(1, 66))
    receipt = _receipt(tmp_path / "runtime", generation="6" * 64).model_copy(
        update={"schema_rollout_plan_ids": plan_ids}
    )
    audit = RuntimeDeploymentRolloutAudit(
        operation_id="7" * 64,
        profile_id=PROFILE,
        generation_hash=receipt.generation_hash,
        previous_generation_hash=None,
        status="succeeded",
        service_units=(),
        schema_rollout_plan_ids=plan_ids,
        schema_receipt_hashes=hashes,
        schema_retire_observations=tuple(
            RuntimeSchemaRetireObservation(
                plan_id=plan_id,
                cutover_receipt_hash=receipt_hash,
                cutover_observed_at=now,
                retire_eligible_at=now + timedelta(days=1),
            )
            for plan_id, receipt_hash in zip(plan_ids, hashes, strict=True)
        ),
    )

    with pytest.raises(ValueError, match="bounded plan limit"):
        preview_runtime_schema_retirement(
            receipt,
            rollout_audit=audit,
            now=now,
        )


def test_rollout_failure_preserves_last_verified_schema_receipt_when_store_breaks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = _receipt(tmp_path / "runtime", generation="7" * 64).model_copy(
        update={"schema_rollout_plan_ids": ("8" * 64,)}
    )
    receipt_reads = 0

    def schema_receipts(_receipt: RuntimeDeploymentReceipt) -> tuple[str, ...]:
        nonlocal receipt_reads
        receipt_reads += 1
        if receipt_reads == 1:
            return ("9" * 64,)
        raise OSError("schema state became unreadable")

    monkeypatch.setattr(rollout_module, "_schema_receipt_hashes", schema_receipts)
    controller = FakeController(
        unhealthy_unit=receipt.unit_mapping["a-market-minute"],
    )
    audit_root = tmp_path / "audit"

    with pytest.raises(RuntimeDeploymentRolloutError, match="failed closed"):
        rollout_runtime_deployment(
            receipt,
            controller=controller,
            current_receipt_loader=lambda: receipt,
            previous_receipt_loader=lambda _generation: None,
            previous_generation_activator=lambda _receipt: None,
            audit_root=audit_root,
            health_timeout_seconds=10,
        )

    payload = next(audit_root.glob("*.json")).read_bytes()
    audit = RuntimeDeploymentRolloutAudit.model_validate_json(payload)
    assert audit.status == "failed_closed"
    assert audit.schema_receipt_hashes == ("9" * 64,)


def test_failed_previous_generation_restart_stops_every_partially_started_unit(
    tmp_path: Path,
) -> None:
    previous = _receipt(tmp_path / "runtime", generation="5" * 64)
    receipt = _receipt(
        tmp_path / "runtime",
        generation="6" * 64,
        previous=previous.generation_hash,
    )
    failing_unit = receipt.unit_mapping["z-strategy"]
    remaining_failures = 2

    class FailingController(FakeController):
        def wait_healthy(
            self,
            unit: str,
            *,
            receipt: RuntimeDeploymentReceipt,
            timeout_seconds: float,
        ) -> None:
            nonlocal remaining_failures
            super().wait_healthy(
                unit,
                receipt=receipt,
                timeout_seconds=timeout_seconds,
            )
            if unit == failing_unit and remaining_failures:
                remaining_failures -= 1
                raise RuntimeError("injected health failure")

    controller = FailingController()

    with pytest.raises(RuntimeDeploymentRolloutError, match="failed closed"):
        rollout_runtime_deployment(
            receipt,
            controller=controller,
            current_receipt_loader=lambda: receipt,
            previous_receipt_loader=lambda _generation: previous,
            previous_generation_activator=lambda _receipt: None,
            audit_root=tmp_path / "audit",
            health_timeout_seconds=10,
        )

    previous_minute = previous.unit_mapping["a-market-minute"]
    assert controller.calls.count(("stop", previous_minute)) == 2
    second_restart = max(
        index for index, call in enumerate(controller.calls) if call == ("restart", previous_minute)
    )
    assert ("stop", previous_minute) in controller.calls[second_restart + 1 :]


def test_systemd_controller_requires_both_active_unit_and_generation_bound_health(
    tmp_path: Path,
) -> None:
    receipt = _receipt(tmp_path / "runtime", generation="f" * 64)
    unit = receipt.unit_mapping["a-market-minute"]
    active_results = iter((1, 0, 0))
    health_results = iter((False, True))
    commands: list[tuple[str, ...]] = []
    health_checks: list[tuple[RuntimeDeploymentReceipt, str]] = []
    sleeps: list[float] = []
    ticks = iter((0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6))

    def command_runner(arguments: tuple[str, ...]) -> int:
        commands.append(arguments)
        if arguments[:2] == ("is-active", "--quiet"):
            return next(active_results)
        return 0

    controller = SystemdRuntimeRolloutController(
        command_runner=command_runner,
        health_probe=lambda observed_receipt, observed_unit: (
            health_checks.append((observed_receipt, observed_unit)) is None
            and observed_receipt == receipt
            and observed_unit == unit
            and next(health_results)
        ),
        monotonic_clock=lambda: next(ticks),
        sleep=sleeps.append,
        poll_interval_seconds=0.01,
    )

    controller.daemon_reload()
    controller.restart(unit)
    controller.wait_healthy(unit, receipt=receipt, timeout_seconds=1)
    controller.stop(unit)

    assert commands[0] == ("daemon-reload",)
    assert commands[1] == ("restart", unit)
    assert commands[-1] == ("stop", unit)
    assert commands.count(("is-active", "--quiet", unit)) == 3
    assert health_checks == [(receipt, unit), (receipt, unit)]
    assert sleeps == [0.01, 0.01]


def test_generation_health_probe_binds_current_manifest_control_root_and_fresh_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = _receipt(tmp_path / "runtime", generation="f" * 64)
    unit = receipt.unit_mapping["a-market-minute"]
    instance = receipt.instance_mapping["a-market-minute"]
    manifest = RuntimeServiceManifest(
        service_id="a-market-minute",
        service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "spool_root": str(receipt.runtime_root / "live" / "market-minute"),
            "quota_path": str(receipt.runtime_root / "live" / "market-minute" / "quota.sqlite3"),
        },
    )
    now = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    observed: dict[str, object] = {}

    def load_manifest(path: Path, **kwargs: object) -> RuntimeServiceManifest:
        observed.update(path=path, **kwargs)
        return manifest

    heartbeat = SimpleNamespace(
        status=RuntimeServiceStatus.RUNNING,
        last_success_at=now,
        heartbeat_at=now,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.load_runtime_service_manifest",
        load_manifest,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.RuntimeServiceControl.read_heartbeat",
        lambda root, spec: observed.update(control_root=root, spec=spec) or heartbeat,
    )

    probe = build_runtime_generation_health_probe(clock=lambda: now)

    assert probe(receipt, unit) is True
    assert observed["path"] == (receipt.runtime_root / "current" / "manifests" / f"{instance}.json")
    assert observed["expected_commit"] == COMMIT
    assert observed["expected_generation"] == receipt.generation_hash
    assert observed["control_root"] == (
        receipt.runtime_root / "control" / "market-minute-sources" / instance
    )


def test_generation_health_probe_binds_watchlist_quote_kind_and_control_bucket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    instance = f"svc-{'3' * 64}"
    service_id = "watchlist.quote.source.v1"
    unit = f"rquant-runtime-watchlist-quote@{instance}.service"
    receipt = RuntimeDeploymentReceipt(
        runtime_root=runtime_root,
        producer_commit=COMMIT,
        generation_hash="f" * 64,
        deployment_profile_id=PROFILE,
        instance_mapping={service_id: instance},
        unit_mapping={service_id: unit},
    )
    manifest = RuntimeServiceManifest(
        service_id=service_id,
        service_kind=RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "spool_root": str(runtime_root / "live" / "watchlist-quote"),
            "quota_path": str(runtime_root / "live" / "watchlist-quote" / "quota.sqlite3"),
        },
    )
    now = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    observed: dict[str, object] = {}
    heartbeat = SimpleNamespace(
        status=RuntimeServiceStatus.RUNNING,
        last_success_at=now,
        heartbeat_at=now,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.load_runtime_service_manifest",
        lambda path, **kwargs: observed.update(path=path, **kwargs) or manifest,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.RuntimeServiceControl.read_heartbeat",
        lambda root, spec: observed.update(control_root=root, spec=spec) or heartbeat,
    )

    assert build_runtime_generation_health_probe(clock=lambda: now)(receipt, unit) is True
    assert observed["control_root"] == (
        runtime_root / "control" / "watchlist-quote-sources" / instance
    )
