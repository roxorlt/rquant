from __future__ import annotations

import hashlib
import json
import os
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from rquant.daily_close_gateway import DailyCloseGateway, DailyCloseGatewayConfig
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
)
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_profile import RuntimeDeploymentProfile
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.strict_json import canonical_json_bytes, strict_json_loads
from tests.daily_ed25519_support import (
    DailyEd25519TestAuthority,
    create_daily_ed25519_test_authority,
    create_rotating_daily_ed25519_test_authority,
)
from tests.unit.test_daily_close_gateway import AVAILABLE_AT, OBSERVED_AT, TRADE_DATE, _snapshot

COMMIT = "a" * 40


def _stage_commands() -> list[dict[str, object]]:
    project_root = Path(__file__).resolve().parents[2]
    from rquant.runtime_builder_daily_orchestrator import build_daily_shadow_stage_commands

    return [
        command.model_dump(mode="json")
        for command in build_daily_shadow_stage_commands(
            python_executable=Path(sys.executable),
            working_directory=project_root,
            environment={"PYTHONPATH": str(project_root / "src")},
        )
    ]


def _publish_daily_close_source(runtime_root: Path) -> None:
    gateway = DailyCloseGateway(
        spool=LiveBatchSpool(runtime_root / "live" / "daily-close"),
        fetcher=lambda _request: _snapshot(),
        config=DailyCloseGatewayConfig(
            producer_version="daily-close-test-v1",
            producer_commit=COMMIT,
        ),
        completion_clock=lambda: AVAILABLE_AT,
    )
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)


def _daily_profile(
    runtime_root: Path,
    *,
    receipt_authority: DailyEd25519TestAuthority | None = None,
    receipt_signer_command: tuple[str, ...] | None = None,
    include_receipt_authority: bool = True,
    service_owner: str = "daily.pipeline.orchestrator.shadow.v1",
    include_stage_commands: bool = True,
) -> tuple[RuntimeDeploymentProfile, RuntimeServiceManifest]:
    daily_close = RuntimeServiceManifest(
        service_id="daily-close.source.v1",
        service_kind=RuntimeServiceKind.DAILY_CLOSE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=60,
        stale_after_seconds=3600,
        producer_commit=COMMIT,
        settings={
            "spool_root": str(runtime_root / "live" / "daily-close"),
            "producer_version": "daily-close-test-v1",
            "calendar_path": str(runtime_root / "authorities" / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "c" * 64,
        },
    )
    settings: dict[str, object] = {
        "storage_root": str(runtime_root / "research" / "daily-pipeline"),
        "source_spool_root": str(runtime_root / "live" / "daily-close"),
        "deployment_profile_path": str(runtime_root / "current" / "deployment-profile.json"),
        "mode": "shadow",
        "service_owner": service_owner,
        "stages": [command["stage_id"] for command in _stage_commands()],
    }
    if include_receipt_authority:
        assert receipt_authority is not None
        settings.update(
            {
                "receipt_active_key_id": receipt_authority.key_id,
                "receipt_active_public_key_pem": receipt_authority.public_key_pem,
                "receipt_previous_public_key_pems": {},
                "offline_test_receipt_signer_command": list(
                    receipt_signer_command or receipt_authority.signer_command
                ),
                "receipt_signer_timeout_seconds": 5.0,
            }
        )
    if include_stage_commands:
        settings["stage_commands"] = _stage_commands()
    orchestrator = RuntimeServiceManifest(
        service_id="daily.pipeline.orchestrator.shadow.v1",
        service_kind=RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=60,
        stale_after_seconds=172800,
        producer_commit=COMMIT,
        settings=settings,
    )
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        production_runtime_root=str(runtime_root),
        manifests=(daily_close, orchestrator),
        capability_environment={
            daily_close.service_id: ("TUSHARE_TOKEN_MAIN",),
            orchestrator.service_id: (),
        },
    )
    return profile, orchestrator


def _write_profile(profile: RuntimeDeploymentProfile, runtime_root: Path) -> None:
    path = runtime_root / "current" / "deployment-profile.json"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(profile.model_dump(mode="json")))
    path.chmod(0o600)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


@contextmanager
def _temporary_environ(values: dict[str, str]):
    previous = os.environ.copy()
    try:
        os.environ.clear()
        os.environ.update(values)
        yield
    finally:
        os.environ.clear()
        os.environ.update(previous)


@contextmanager
def _shadow_stage_environment(
    storage_profile: DailyPipelineStorageProfile,
    *,
    stage_id: str,
    run_id: str,
    profile_hash: str,
    input_identity: str,
    source_generation_id: str,
    source_content_hash: str,
    command_manifest_hash: str,
    effect_id: str,
    idempotency_key: str,
    receipt_authority: DailyEd25519TestAuthority,
    dependency_receipt_ids: tuple[str, ...] = (),
):
    previous = os.environ.copy()
    try:
        os.environ.update(
            {
                "RQUANT_DAILY_STAGE_ID": stage_id,
                "RQ_DAILY_SHADOW_ADAPTER_STAGE": stage_id,
                "RQ_DAILY_SHADOW_ADAPTER_IDENTITY": (
                    f"daily-shadow-stage/{stage_id}/v1"
                ),
                "RQUANT_DAILY_RUN_MODE": DailyPipelineMode.SHADOW.value,
                "RQUANT_DAILY_PROFILE_HASH": profile_hash,
                "RQUANT_DAILY_STORAGE_ROOT": str(storage_profile.root),
                "RQUANT_DAILY_STORAGE_NAMESPACE_ID": str(storage_profile.namespace_id),
                "RQUANT_DAILY_EFFECT_RECEIPT_LOCATOR": str(
                    storage_profile.receipt_root / f"{effect_id}.json"
                ),
                "RQUANT_DAILY_RUN_ID": run_id,
                "RQUANT_DAILY_EFFECT_ID": effect_id,
                "RQUANT_DAILY_EFFECT_IDEMPOTENCY_KEY": idempotency_key,
                "RQUANT_DAILY_INPUT_IDENTITY": input_identity,
                "RQUANT_DAILY_SOURCE_GENERATION_ID": source_generation_id,
                "RQUANT_DAILY_SOURCE_CONTENT_HASH": source_content_hash,
                "RQUANT_DAILY_COMMAND_MANIFEST_HASH": command_manifest_hash,
                "RQ_DAILY_SHADOW_DEPENDENCY_RECEIPT_IDS": canonical_json_bytes(
                    dependency_receipt_ids
                ).decode("utf-8"),
                "RQ_DAILY_SHADOW_OFFLINE_TEST_RECEIPT_SIGNER_COMMAND": (
                    json.dumps(
                        receipt_authority.signer_command,
                        separators=(",", ":"),
                    )
                ),
                "RQ_DAILY_SHADOW_RECEIPT_SIGNER_KEY_ID": receipt_authority.key_id,
                "RQ_DAILY_SHADOW_RECEIPT_SIGNER_TIMEOUT_SECONDS": "5.0",
            }
        )
        yield
    finally:
        os.environ.clear()
        os.environ.update(previous)


def test_daily_orchestrator_rejects_nonzero_child_with_valid_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = (tmp_path / "runtime").resolve()
    _publish_daily_close_source(runtime_root)
    receipt_authority = create_daily_ed25519_test_authority(tmp_path / "daily-receipt")
    profile, manifest = _daily_profile(
        runtime_root,
        receipt_authority=receipt_authority,
    )
    assert profile.profile_id is not None
    _write_profile(profile, runtime_root)
    calls: list[str] = []

    from rquant.daily_pipeline_ledger import DailyStageState
    from rquant.runtime_builder_daily_orchestrator import (
        DailyPipelineOrchestratorSettings,
        DailyShadowStageArtifact,
        DailyShadowStageCompletionReceipt,
        daily_pipeline_orchestrator_builder,
        daily_shadow_ledger_owner,
        daily_shadow_stage_receipt_keyring,
        run_daily_shadow_stage_adapter_from_environment,
    )

    receipt_keyring = daily_shadow_stage_receipt_keyring(
        DailyPipelineOrchestratorSettings.model_validate(dict(manifest.settings))
    )
    from rquant.daily_pipeline_orchestrator import subprocess as orchestrator_subprocess

    real_popen = orchestrator_subprocess.Popen
    stage_argvs = {
        tuple(command["argv"]) for command in manifest.settings["stage_commands"]
    }

    class ReceiptWritingProcess:
        pid = 123456

        def __init__(
            self,
            _argv: tuple[str, ...],
            *,
            env: dict[str, str],
            **_kwargs: Any,
        ) -> None:
            stage_id = env["RQUANT_DAILY_STAGE_ID"]
            calls.append(stage_id)
            with _temporary_environ(env):
                run_daily_shadow_stage_adapter_from_environment(
                    stage_id,
                    issued_at=datetime(2026, 8, 3, 10, tzinfo=UTC),
                )
            self.returncode = 17 if stage_id == "raw_capture" else 0

        def wait(self, timeout: float | None = None) -> int:
            return self.returncode

        def poll(self) -> int:
            return self.returncode

    def stage_process_or_real_popen(
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        argv = args[0] if args else kwargs.get("args")
        if tuple(argv) in stage_argvs:
            return ReceiptWritingProcess(*args, **kwargs)
        return real_popen(*args, **kwargs)

    monkeypatch.setattr(
        "rquant.daily_pipeline_orchestrator.subprocess.Popen",
        stage_process_or_real_popen,
    )
    monkeypatch.setattr(
        "rquant.runtime_builder_daily_orchestrator.daily_shadow_stage_receipt_keyring",
        lambda _settings: receipt_keyring,
    )

    with pytest.raises(RuntimeError, match="shadow DAG did not complete"):
        daily_pipeline_orchestrator_builder(
            clock=lambda: datetime(2026, 8, 3, 10, tzinfo=UTC),
        )(manifest)()

    storage_profile = DailyPipelineStorageProfile.create(
        root=runtime_root / "research" / "daily-pipeline",
        mode=DailyPipelineMode.SHADOW,
        profile_hash=profile.profile_id,
    )
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner=daily_shadow_ledger_owner(manifest.service_id),
    )
    artifact_path = next(
        (storage_profile.namespace_root / "effects" / "raw_capture").glob("*.json")
    )
    artifact = DailyShadowStageArtifact.model_validate_json(
        artifact_path.read_text(encoding="utf-8")
    )
    effect = ledger.effect_for_stage(artifact.run_id, "raw_capture")
    assert effect is not None
    external_receipt_path = Path(effect.intent.receipt_locator)
    external_receipt = DailyShadowStageCompletionReceipt.model_validate_json(
        external_receipt_path.read_text(encoding="utf-8")
    )
    run_id = external_receipt.run_id

    assert calls == ["raw_capture"]
    assert ledger.stage(run_id, "raw_capture").state is not DailyStageState.SUCCEEDED
    assert ledger.stage(run_id, "validate_candidate").state is DailyStageState.PENDING


def test_daily_orchestrator_runs_full_shadow_dag_from_immutable_profile(
    tmp_path: Path,
) -> None:
    runtime_root = (tmp_path / "runtime").resolve()
    _publish_daily_close_source(runtime_root)
    receipt_authority = create_daily_ed25519_test_authority(tmp_path / "daily-receipt")
    profile, manifest = _daily_profile(
        runtime_root,
        receipt_authority=receipt_authority,
    )
    assert profile.profile_id is not None
    _write_profile(profile, runtime_root)

    from rquant.daily_pipeline_orchestrator import DEFAULT_DAILY_CLOSE_PIPELINE
    from rquant.runtime_builder_daily_orchestrator import (
        daily_pipeline_orchestrator_builder,
        daily_shadow_ledger_owner,
        load_daily_shadow_run_completion_receipt,
    )

    result = daily_pipeline_orchestrator_builder(
        clock=lambda: datetime(2026, 8, 3, 10, tzinfo=UTC),
    )(manifest)()

    storage_profile = DailyPipelineStorageProfile.create(
        root=runtime_root / "research" / "daily-pipeline",
        mode=DailyPipelineMode.SHADOW,
        profile_hash=profile.profile_id,
    )
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner=daily_shadow_ledger_owner(manifest.service_id),
    )
    completion_receipt = load_daily_shadow_run_completion_receipt(
        storage_profile,
        result.source_generations["daily_pipeline_completion_receipt"],
    )
    assert completion_receipt.final_backup_stage_id == "backup"
    assert completion_receipt.exit_status == 0
    assert receipt_authority.keyring.verify_run_completion_receipt(
        completion_receipt,
        require_active=True,
    )
    assert result.processed_count == len(DEFAULT_DAILY_CLOSE_PIPELINE.stage_ids)
    assert result.source_generations["daily_pipeline_profile"] == profile.profile_id
    assert result.source_generations["daily_pipeline_namespace"] == storage_profile.namespace_id
    assert result.source_generations["daily_pipeline_command_manifest"]
    assert len(tuple(storage_profile.receipt_root.glob("*.json"))) == len(
        DEFAULT_DAILY_CLOSE_PIPELINE.stage_ids
    )
    artifact_root = storage_profile.namespace_root / "effects"
    from rquant.runtime_builder_daily_orchestrator import (
        DailyShadowStageArtifact,
        DailyShadowStageCompletionReceipt,
    )

    artifact_by_stage: dict[str, DailyShadowStageArtifact] = {}
    completion_by_stage: dict[str, DailyShadowStageCompletionReceipt] = {}
    signed_stage_receipts: list[DailyShadowStageCompletionReceipt] = []
    signed_stage_receipt_ids: list[str] = []
    for stage_id in DEFAULT_DAILY_CLOSE_PIPELINE.stage_ids:
        effect = ledger.effect_for_stage(completion_receipt.run_id, stage_id)
        assert effect is not None
        stage_receipt = DailyShadowStageCompletionReceipt.model_validate_json(
            Path(effect.intent.receipt_locator).read_text(encoding="utf-8")
        )
        assert receipt_authority.keyring.verify_stage_receipt(
            stage_receipt,
            require_active=True,
        )
        assert stage_receipt.exit_status == 0
        signed_stage_receipts.append(stage_receipt)
        signed_stage_receipt_ids.append(str(stage_receipt.receipt_id))
    assert completion_receipt.stage_order == DEFAULT_DAILY_CLOSE_PIPELINE.stage_ids
    assert completion_receipt.stage_receipt_ids == tuple(signed_stage_receipt_ids)
    assert completion_receipt.final_backup_receipt_id == signed_stage_receipt_ids[-1]
    assert completion_receipt.previous_receipt_hash == canonical_sha256(
        {
            "contract": "daily-shadow-stage-previous-receipt-chain/v1",
            "dependency_receipt_ids": tuple(signed_stage_receipt_ids),
        }
    )

    for stage_id in ("serving_refresh", "replica_sync", "research_ingest", "backup"):
        artifact_paths = tuple((artifact_root / stage_id).glob("*.json"))
        assert len(artifact_paths) == 1
        artifact = DailyShadowStageArtifact.model_validate_json(
            artifact_paths[0].read_text(encoding="utf-8")
        )
        artifact_by_stage[stage_id] = artifact
        effect = ledger.effect_for_stage(completion_receipt.run_id, stage_id)
        assert effect is not None
        assert artifact.effect_id == effect.intent.effect_id
        receipt = DailyShadowStageCompletionReceipt.model_validate_json(
            Path(effect.intent.receipt_locator).read_text(encoding="utf-8")
        )
        completion_by_stage[stage_id] = receipt
        assert receipt.exit_status == 0
        assert receipt_authority.keyring.verify_stage_receipt(
            receipt,
            require_active=True,
        )
        assert receipt.result.content_hash == artifact.artifact_id
        assert receipt.output_identity in {item.sha256 for item in artifact.output_files}

    serving_root = storage_profile.namespace_root / "serving-refresh"
    serving_current = strict_json_loads((serving_root / "current.json").read_bytes())
    serving_manifest_path = (
        serving_root / "manifests" / f"{serving_current['manifest_sha256']}.json"
    )
    serving_payload_path = serving_root / "payloads" / f"{serving_current['payload_sha256']}.bin"
    assert serving_manifest_path.is_file()
    assert serving_payload_path.is_file()
    assert _sha256_file(serving_manifest_path) == serving_current["manifest_sha256"]
    assert _sha256_file(serving_payload_path) == serving_current["payload_sha256"]
    assert artifact_by_stage["serving_refresh"].output_identity in {
        serving_current["manifest_sha256"],
        serving_current["payload_sha256"],
    }
    assert completion_by_stage["serving_refresh"].output_identity in {
        serving_current["manifest_sha256"],
        serving_current["payload_sha256"],
    }

    replica_root = storage_profile.namespace_root / "replica-sync"
    replica_current = strict_json_loads((replica_root / "current.json").read_bytes())
    replica_manifest_path = (
        replica_root / "manifests" / f"{replica_current['manifest_sha256']}.json"
    )
    replica_payload_path = replica_root / "payloads" / f"{replica_current['payload_sha256']}.bin"
    assert replica_manifest_path.is_file()
    assert replica_payload_path.is_file()
    assert _sha256_file(replica_manifest_path) == replica_current["manifest_sha256"]
    assert _sha256_file(replica_payload_path) == replica_current["payload_sha256"]
    assert replica_payload_path.read_bytes() == serving_payload_path.read_bytes()
    assert artifact_by_stage["replica_sync"].output_identity in {
        replica_current["manifest_sha256"],
        replica_current["payload_sha256"],
    }
    assert completion_by_stage["replica_sync"].output_identity in {
        replica_current["manifest_sha256"],
        replica_current["payload_sha256"],
    }

    research_root = storage_profile.namespace_root / "research-ingest"
    research_current = strict_json_loads((research_root / "current.json").read_bytes())
    research_manifest_path = (
        research_root / "manifests" / f"{research_current['manifest_sha256']}.json"
    )
    research_catalog_path = (
        research_root / "catalogs" / f"{research_current['catalog_sha256']}.json"
    )
    research_lake_path = research_root / "lake" / f"{research_current['lake_sha256']}.json"
    assert research_manifest_path.is_file()
    assert research_catalog_path.is_file()
    assert research_lake_path.is_file()
    assert _sha256_file(research_manifest_path) == research_current["manifest_sha256"]
    assert _sha256_file(research_catalog_path) == research_current["catalog_sha256"]
    assert _sha256_file(research_lake_path) == research_current["lake_sha256"]
    assert artifact_by_stage["research_ingest"].output_identity in {
        research_current["manifest_sha256"],
        research_current["catalog_sha256"],
        research_current["lake_sha256"],
    }
    assert completion_by_stage["research_ingest"].output_identity in {
        research_current["manifest_sha256"],
        research_current["catalog_sha256"],
        research_current["lake_sha256"],
    }

    backup_root = storage_profile.namespace_root / "backup"
    backup_current = strict_json_loads((backup_root / "current.json").read_bytes())
    backup_manifest_path = backup_root / "manifests" / f"{backup_current['manifest_sha256']}.json"
    backup_catalog_path = backup_root / "catalogs" / f"{backup_current['catalog_sha256']}.json"
    backup_lake_path = backup_root / "lake" / f"{backup_current['lake_sha256']}.json"
    assert backup_manifest_path.is_file()
    assert backup_catalog_path.is_file()
    assert backup_lake_path.is_file()
    assert _sha256_file(backup_manifest_path) == backup_current["manifest_sha256"]
    assert _sha256_file(backup_catalog_path) == backup_current["catalog_sha256"]
    assert _sha256_file(backup_lake_path) == backup_current["lake_sha256"]
    assert backup_catalog_path.read_bytes() == research_catalog_path.read_bytes()
    assert backup_lake_path.read_bytes() == research_lake_path.read_bytes()
    assert artifact_by_stage["backup"].output_identity in {
        backup_current["manifest_sha256"],
        backup_current["catalog_sha256"],
        backup_current["lake_sha256"],
    }
    assert completion_by_stage["backup"].output_identity in {
        backup_current["manifest_sha256"],
        backup_current["catalog_sha256"],
        backup_current["lake_sha256"],
    }
    assert not (runtime_root / "rquant.duckdb").exists()
    assert not (runtime_root / "notification_outbox.sqlite3").exists()
    assert not (runtime_root / "control" / "rquant-daily.timer").exists()

    from rquant.runtime_builder_daily_orchestrator import (
        issue_daily_shadow_run_completion_receipt,
    )

    rotating = create_rotating_daily_ed25519_test_authority(tmp_path / "rotating-daily")
    historical_completion = issue_daily_shadow_run_completion_receipt(
        signer=rotating.previous.signer,
        run=ledger.run(completion_receipt.run_id),
        stage_receipts=tuple(signed_stage_receipts),
        issued_at=completion_receipt.issued_at,
    )
    assert rotating.keyring.verify_run_completion_receipt(historical_completion)
    assert not rotating.keyring.verify_run_completion_receipt(
        historical_completion,
        require_active=True,
    )

    completion_path = storage_profile.report_root / f"{completion_receipt.receipt_id}.json"
    completion_payload = strict_json_loads(completion_path.read_bytes())
    completion_payload["final_backup_output_identity"] = "f" * 64
    completion_path.write_bytes(canonical_json_bytes(completion_payload))
    with pytest.raises(ValueError, match="identity|verification"):
        load_daily_shadow_run_completion_receipt(
            storage_profile,
            str(completion_receipt.receipt_id),
        )


def test_daily_shadow_stage_commands_are_strict_allowlisted_identity_mapping() -> None:
    project_root = Path(__file__).resolve().parents[2]
    from rquant.runtime_builder_daily_orchestrator import (
        DailyShadowStageCommandSettings,
        build_daily_shadow_stage_commands,
    )

    commands = build_daily_shadow_stage_commands(
        python_executable=Path(sys.executable),
        working_directory=project_root,
        environment={"PYTHONPATH": str(project_root / "src")},
    )

    assert tuple(command.stage_id for command in commands) == (
        "raw_capture",
        "validate_candidate",
        "canonical_publish",
        "screen",
        "pool",
        "summary",
        "serving_refresh",
        "replica_sync",
        "research_ingest",
        "backup",
    )
    for command in commands:
        assert command.adapter_identity == f"daily-shadow-stage/{command.stage_id}/v1"
        assert command.argv == (
            str(Path(sys.executable).absolute()),
            "-m",
            "rquant.runtime_builder_daily_orchestrator",
            "--run-shadow-adapter",
            command.stage_id,
        )

    with pytest.raises(ValueError, match="allowlisted"):
        DailyShadowStageCommandSettings(
            stage_id="backup",
            adapter_identity="daily-shadow-stage/serving_refresh/v1",
            argv=(
                str(Path(sys.executable).absolute()),
                "-m",
                "rquant.runtime_builder_daily_orchestrator",
                "--run-shadow-adapter",
                "serving_refresh",
            ),
        )


def test_daily_orchestrator_profile_requires_controlled_stage_commands(
    tmp_path: Path,
) -> None:
    runtime_root = (tmp_path / "runtime").resolve()
    receipt_authority = create_daily_ed25519_test_authority(tmp_path / "daily-receipt")
    profile, manifest = _daily_profile(
        runtime_root,
        receipt_authority=receipt_authority,
        include_stage_commands=False,
    )
    _write_profile(profile, runtime_root)

    from rquant.runtime_builder_daily_orchestrator import daily_pipeline_orchestrator_builder

    with pytest.raises(ValueError, match="stage_commands"):
        daily_pipeline_orchestrator_builder(
            clock=lambda: datetime(2026, 8, 3, 10, tzinfo=UTC),
        )(manifest)


def test_daily_orchestrator_rejects_wrong_profile_authority_owner(
    tmp_path: Path,
) -> None:
    runtime_root = (tmp_path / "runtime").resolve()
    receipt_authority = create_daily_ed25519_test_authority(tmp_path / "daily-receipt")
    profile, manifest = _daily_profile(
        runtime_root,
        receipt_authority=receipt_authority,
        service_owner="daily-shadow-forged",
    )
    _write_profile(profile, runtime_root)

    from rquant.runtime_builder_daily_orchestrator import daily_pipeline_orchestrator_builder

    with pytest.raises(ValueError, match="authority|owner"):
        daily_pipeline_orchestrator_builder(
            clock=lambda: datetime(2026, 8, 3, 10, tzinfo=UTC),
        )(manifest)


def test_daily_orchestrator_requires_protected_receipt_authority(
    tmp_path: Path,
) -> None:
    runtime_root = (tmp_path / "runtime").resolve()
    profile, manifest = _daily_profile(
        runtime_root,
        include_receipt_authority=False,
    )
    _write_profile(profile, runtime_root)

    from rquant.runtime_builder_daily_orchestrator import daily_pipeline_orchestrator_builder

    with pytest.raises(ValueError, match="receipt.*(authority|signer|key)|Ed25519"):
        daily_pipeline_orchestrator_builder(
            clock=lambda: datetime(2026, 8, 3, 10, tzinfo=UTC),
        )(manifest)


def test_daily_orchestrator_settings_rejects_production_helper_command(
    tmp_path: Path,
) -> None:
    receipt_authority = create_daily_ed25519_test_authority(tmp_path / "daily-receipt")
    profile, manifest = _daily_profile(
        (tmp_path / "runtime").resolve(),
        receipt_authority=receipt_authority,
    )
    settings = dict(manifest.settings)
    settings["receipt_signer_command"] = settings.pop("offline_test_receipt_signer_command")
    assert "receipt_signer_command" in settings

    from rquant.runtime_builder_daily_orchestrator import DailyPipelineOrchestratorSettings

    with pytest.raises(ValueError, match="socket|helper|command"):
        DailyPipelineOrchestratorSettings.model_validate(settings)


def test_daily_orchestrator_allows_custom_socket_only_for_an_explicit_test_profile(
    tmp_path: Path,
) -> None:
    receipt_authority = create_daily_ed25519_test_authority(tmp_path / "daily-receipt")
    _profile, manifest = _daily_profile(
        (tmp_path / "runtime").resolve(),
        receipt_authority=receipt_authority,
    )

    from rquant.runtime_builder_daily_orchestrator import DailyPipelineOrchestratorSettings

    settings = dict(manifest.settings)
    settings.pop("offline_test_receipt_signer_command")
    settings["receipt_signer_socket_endpoint"] = str(tmp_path / "daily.sock")
    settings["receipt_trusted_keyring_path"] = str(tmp_path / "daily-keys.json")
    with pytest.raises(ValueError, match="fixed"):
        DailyPipelineOrchestratorSettings.model_validate(settings)

    settings["receipt_signer_test_mode"] = True
    verified = DailyPipelineOrchestratorSettings.model_validate(settings)
    assert verified.receipt_signer_test_mode is True
    assert verified.receipt_signer_socket_endpoint == tmp_path / "daily.sock"


def test_daily_stage_adapter_environment_rejects_helper_command_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RQ_DAILY_SHADOW_RECEIPT_SIGNER_SOCKET_ENDPOINT", raising=False)
    monkeypatch.setenv(
        "RQ_DAILY_SHADOW_RECEIPT_SIGNER_COMMAND",
        '["/usr/bin/sudo","-n","/usr/local/libexec/rquant-daily-receipt-signer"]',
    )
    monkeypatch.setenv("RQ_DAILY_SHADOW_RECEIPT_SIGNER_KEY_ID", "daily-v1")
    monkeypatch.setenv("RQ_DAILY_SHADOW_RECEIPT_SIGNER_TIMEOUT_SECONDS", "5")

    from rquant.runtime_builder_daily_orchestrator import _daily_receipt_signer_from_environment

    with pytest.raises(RuntimeError, match="socket|fallback|command"):
        _daily_receipt_signer_from_environment()


def test_daily_orchestrator_rejects_receipt_signed_by_untrusted_key(
    tmp_path: Path,
) -> None:
    runtime_root = (tmp_path / "runtime").resolve()
    _publish_daily_close_source(runtime_root)
    trusted_authority = create_daily_ed25519_test_authority(
        tmp_path / "trusted-daily-receipt"
    )
    forged_authority = create_daily_ed25519_test_authority(
        tmp_path / "forged-daily-receipt",
        key_id=trusted_authority.key_id,
    )
    profile, manifest = _daily_profile(
        runtime_root,
        receipt_authority=trusted_authority,
        receipt_signer_command=forged_authority.signer_command,
    )
    assert profile.profile_id is not None
    _write_profile(profile, runtime_root)

    from rquant.daily_pipeline_ledger import DailyStageState
    from rquant.runtime_builder_daily_orchestrator import (
        DailyShadowStageArtifact,
        daily_pipeline_orchestrator_builder,
        daily_shadow_ledger_owner,
    )

    with pytest.raises(RuntimeError, match="shadow DAG did not complete"):
        daily_pipeline_orchestrator_builder(
            clock=lambda: datetime(2026, 8, 3, 10, tzinfo=UTC),
        )(manifest)()

    storage_profile = DailyPipelineStorageProfile.create(
        root=runtime_root / "research" / "daily-pipeline",
        mode=DailyPipelineMode.SHADOW,
        profile_hash=profile.profile_id,
    )
    artifact_path = next(
        (storage_profile.namespace_root / "effects" / "raw_capture").glob("*.json")
    )
    artifact = DailyShadowStageArtifact.model_validate_json(
        artifact_path.read_text(encoding="utf-8")
    )
    ledger = DailyPipelineLedger(
        storage_profile=storage_profile,
        service_owner=daily_shadow_ledger_owner(manifest.service_id),
    )

    assert ledger.stage(artifact.run_id, "raw_capture").state is not DailyStageState.SUCCEEDED
    assert ledger.stage(artifact.run_id, "validate_candidate").state is DailyStageState.PENDING


def test_daily_receipt_keyring_rejects_previous_key_for_active_stage_completion(
    tmp_path: Path,
) -> None:
    rotating = create_rotating_daily_ed25519_test_authority(tmp_path / "rotating-daily")

    from rquant.daily_pipeline_ledger import StageResult
    from rquant.runtime_builder_daily_orchestrator import (
        issue_daily_shadow_stage_completion_receipt,
    )

    receipt = issue_daily_shadow_stage_completion_receipt(
        signer=rotating.previous.signer,
        mode=DailyPipelineMode.SHADOW,
        run_id="daily-shadow-test",
        stage_id="raw_capture",
        effect_id="1" * 64,
        idempotency_key="2" * 64,
        input_identity="3" * 64,
        source_generation_id="4" * 64,
        source_content_hash="5" * 64,
        command_manifest_hash="6" * 64,
        profile_hash="7" * 64,
        command_identity="daily-shadow-stage/raw_capture/v1",
        stage_order=0,
        exit_status=0,
        output_identity="8" * 64,
        dependency_receipt_ids=(),
        result=StageResult(content_hash="8" * 64, evidence_hash="9" * 64),
        issued_at=datetime(2026, 8, 3, 10, tzinfo=UTC),
    )

    assert rotating.keyring.verify_stage_receipt(receipt)
    assert not rotating.keyring.verify_stage_receipt(receipt, require_active=True)


def test_replica_sync_fails_closed_after_serving_payload_tamper(
    tmp_path: Path,
) -> None:
    runtime_root = (tmp_path / "runtime").resolve()
    _publish_daily_close_source(runtime_root)
    receipt_authority = create_daily_ed25519_test_authority(tmp_path / "daily-receipt")
    profile, _manifest = _daily_profile(
        runtime_root,
        receipt_authority=receipt_authority,
    )
    assert profile.profile_id is not None

    storage_profile = DailyPipelineStorageProfile.create(
        root=runtime_root / "research" / "daily-pipeline",
        mode=DailyPipelineMode.SHADOW,
        profile_hash=profile.profile_id,
    )
    run_id = "daily-shadow-test"
    input_identity = "1" * 64
    source_generation_id = "2" * 64
    source_content_hash = "3" * 64
    command_manifest_hash = "4" * 64
    serving_effect_id = "5" * 64
    serving_idempotency_key = "6" * 64

    from rquant.runtime_builder_daily_orchestrator import (
        run_daily_shadow_stage_adapter_from_environment,
    )

    with _shadow_stage_environment(
        storage_profile,
        stage_id="serving_refresh",
        run_id=run_id,
        profile_hash=profile.profile_id,
        input_identity=input_identity,
        source_generation_id=source_generation_id,
        source_content_hash=source_content_hash,
        command_manifest_hash=command_manifest_hash,
        effect_id=serving_effect_id,
        idempotency_key=serving_idempotency_key,
        receipt_authority=receipt_authority,
    ):
        serving_receipt = run_daily_shadow_stage_adapter_from_environment(
            "serving_refresh",
            issued_at=datetime(2026, 8, 3, 10, tzinfo=UTC),
        )

    serving_root = storage_profile.namespace_root / "serving-refresh"
    serving_current = strict_json_loads((serving_root / "current.json").read_bytes())
    payload_path = serving_root / "payloads" / f"{serving_current['payload_sha256']}.bin"
    payload_path.write_bytes(payload_path.read_bytes() + b"\nTAMPERED\n")
    assert _sha256_file(payload_path) != serving_current["payload_sha256"]

    with (
        pytest.raises(RuntimeError, match="serving|payload|hash|tamper|verify"),
        _shadow_stage_environment(
            storage_profile,
            stage_id="replica_sync",
            run_id=run_id,
            profile_hash=profile.profile_id,
            input_identity=input_identity,
            source_generation_id=source_generation_id,
            source_content_hash=source_content_hash,
            command_manifest_hash=command_manifest_hash,
            effect_id="7" * 64,
            idempotency_key="8" * 64,
            dependency_receipt_ids=(str(serving_receipt.receipt_id),),
            receipt_authority=receipt_authority,
        ),
    ):
            run_daily_shadow_stage_adapter_from_environment(
                "replica_sync",
                issued_at=datetime(2026, 8, 3, 10, 1, tzinfo=UTC),
            )

    assert not (storage_profile.namespace_root / "replica-sync" / "current.json").exists()
    assert not (storage_profile.namespace_root / "research-ingest").exists()
    assert not (storage_profile.namespace_root / "backup").exists()


def test_daily_offline_test_signer_rejects_sudo_and_the_production_helper() -> None:
    """The offline fixture must not become a privileged capability by argv alone."""

    from rquant.runtime_builder_daily_orchestrator import (
        SecureDailyShadowStageReceiptSigningClient,
    )

    for command in (
        ("/usr/bin/sudo", "-n", "/usr/local/libexec/rquant-daily-receipt-signer"),
        ("/usr/bin/doas", "/usr/local/libexec/rquant-daily-receipt-signer"),
        ("/usr/bin/pkexec", "/opt/signer"),
    ):
        with pytest.raises(ValueError, match="escalate privilege"):
            SecureDailyShadowStageReceiptSigningClient(
                command=command,
                key_id="daily-v1",
                timeout_seconds=5,
            )

    with pytest.raises(ValueError, match="production signer helper"):
        SecureDailyShadowStageReceiptSigningClient(
            command=("/usr/local/libexec/rquant-daily-receipt-signer",),
            key_id="daily-v1",
            timeout_seconds=5,
        )


def test_daily_orchestrator_settings_reject_privileged_offline_signer_command(
    tmp_path: Path,
) -> None:
    receipt_authority = create_daily_ed25519_test_authority(tmp_path / "daily-receipt")
    _profile, manifest = _daily_profile(
        (tmp_path / "runtime").resolve(),
        receipt_authority=receipt_authority,
    )

    from rquant.runtime_builder_daily_orchestrator import DailyPipelineOrchestratorSettings

    for command, expected in (
        (
            ["/usr/bin/sudo", "-n", "/usr/local/libexec/rquant-daily-receipt-signer"],
            "escalate privilege",
        ),
        (["/usr/local/libexec/rquant-daily-receipt-signer"], "production signer helper"),
    ):
        settings = dict(manifest.settings)
        settings["offline_test_receipt_signer_command"] = command
        with pytest.raises(ValueError, match=expected):
            DailyPipelineOrchestratorSettings.model_validate(settings)


def test_daily_offline_test_signer_is_impossible_in_a_production_process_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forging the offline env on a host carrying the fixed Daily authority must fail."""

    import rquant.runtime_builder_daily_orchestrator as builder

    monkeypatch.delenv("RQ_DAILY_SHADOW_RECEIPT_SIGNER_SOCKET_ENDPOINT", raising=False)
    monkeypatch.delenv("RQ_DAILY_SHADOW_RECEIPT_SIGNER_COMMAND", raising=False)
    monkeypatch.setenv(
        "RQ_DAILY_SHADOW_OFFLINE_TEST_RECEIPT_SIGNER_COMMAND",
        json.dumps([sys.executable, str(tmp_path / "offline-signer.py")]),
    )
    monkeypatch.setenv("RQ_DAILY_SHADOW_RECEIPT_SIGNER_KEY_ID", "daily-v1")
    monkeypatch.setenv("RQ_DAILY_SHADOW_RECEIPT_SIGNER_TIMEOUT_SECONDS", "5")

    trusted_keyring = tmp_path / "daily-receipt-trusted-keys.json"
    trusted_keyring.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(builder, "DAILY_RECEIPT_TRUSTED_KEYRING_PATH", trusted_keyring)
    monkeypatch.setattr(
        builder,
        "DAILY_RECEIPT_SOCKET_ENDPOINT",
        tmp_path / "absent-daily-receipt-signer.sock",
    )
    with pytest.raises(RuntimeError, match="production host"):
        builder._daily_receipt_signer_from_environment()

    monkeypatch.setattr(
        builder,
        "DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
        tmp_path / "absent-trusted-keys.json",
    )
    socket_endpoint = tmp_path / "daily-receipt-signer.sock"
    socket_endpoint.write_text("", encoding="utf-8")
    monkeypatch.setattr(builder, "DAILY_RECEIPT_SOCKET_ENDPOINT", socket_endpoint)
    with pytest.raises(RuntimeError, match="production host"):
        builder._daily_receipt_signer_from_environment()


def test_daily_production_signer_client_has_no_sudo_key_argv_or_endpoint_choice() -> None:
    """Production selection is exactly the fixed socket plus the root public keyring."""

    import inspect

    from rquant.daily_receipt_socket_authority import DailyReceiptSocketSigningClient
    from rquant.runtime_builder_daily_orchestrator import (
        _daily_receipt_signer_from_environment,
        daily_shadow_stage_receipt_signer,
    )

    for function in (daily_shadow_stage_receipt_signer, _daily_receipt_signer_from_environment):
        source = inspect.getsource(function)
        assert "DailyReceiptSocketSigningClient.production(" in source
        assert "sudo" not in source

    test_only_source = inspect.getsource(daily_shadow_stage_receipt_signer)
    assert "if verified.receipt_signer_test_mode" in test_only_source
    assert "DailyReceiptSocketSigningClient.for_test_endpoint(" in test_only_source

    production = inspect.signature(DailyReceiptSocketSigningClient.production).parameters
    assert "socket_path" not in production
    assert set(production) == {
        "active_key_id",
        "active_public_key_pem",
        "previous_public_key_pems",
        "timeout_seconds",
        "max_attempts",
    }
