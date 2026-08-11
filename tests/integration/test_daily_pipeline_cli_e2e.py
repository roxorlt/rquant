"""CLI control-plane coverage with a real immutable daily-close source spool."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from rquant.cli import build_parser, cmd_daily_dag, main
from rquant.daily_close_gateway import DailyCloseGateway
from rquant.daily_pipeline_command_manifest import (
    DailyPipelineCommandManifest,
    DailyPipelineStageCommand,
)
from rquant.daily_pipeline_ledger import (
    DailyPipelineMode,
    DailyPipelineStorageBinding,
    DailyPipelineStorageProfile,
)
from rquant.daily_pipeline_orchestrator import DEFAULT_DAILY_CLOSE_PIPELINE
from rquant.daily_pipeline_report_authority import (
    DailyPipelineDevelopmentTestReportAuthority,
    DailyPipelineReportAuthorityCapability,
)
from rquant.live_contracts import LiveChannel
from rquant.strict_json import canonical_model_json_bytes
from tests.unit.test_daily_close_gateway import (
    AVAILABLE_AT,
    OBSERVED_AT,
    TRADE_DATE,
    _gateway,
    _snapshot,
)


@pytest.fixture(autouse=True)
def _fixed_absent_production_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import runtime_deployment_profile as deployment_module

    parent = tmp_path / "fixed-production-parent"
    parent.mkdir(mode=0o700)
    monkeypatch.setattr(
        deployment_module,
        "LINUX_PRODUCTION_RUNTIME_ROOT",
        parent / "runtime",
    )


def _install_valid_linux_production_profile(runtime_root: Path):
    from rquant.lab_highwater_authority import PRODUCTION_LAB_HIGHWATER_COMMAND
    from rquant.runtime_deployment_profile import (
        PRODUCTION_CANVAS_SIGNER_COMMAND,
        PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
        PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT,
        PRODUCTION_PAGE_CONTROL_INSTANCE_ID,
        PRODUCTION_PAGE_CONTROL_SERVICE_ID,
        PRODUCTION_SHADOW_INSTANCE_ID,
        PRODUCTION_SHADOW_SERVICE_ID,
        PRODUCTION_SHADOW_SIGNER_COMMAND,
        LabHighWaterRuntimeProfile,
        PageControlRuntimeProfile,
        RuntimeDeploymentProfile,
        ShadowRuntimeProfile,
        install_runtime_deployment_profile,
    )
    from rquant.runtime_service_control import RuntimeServicePlane
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

    lab_jobs_manifest = RuntimeServiceManifest(
        service_id="lab-jobs.serving.v1",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit="a" * 40,
        settings={
            "lab_jobs_path": str(runtime_root / "research" / "lab_jobs.sqlite3"),
            "authority_root": str(runtime_root / "research" / "serving-authorities" / "lab-jobs"),
        },
    )
    daily_manifest = RuntimeServiceManifest(
        service_id="daily.pipeline.orchestrator.shadow.v1",
        service_kind=RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=60,
        stale_after_seconds=172_800,
        producer_commit="a" * 40,
        settings={
            "storage_root": str(runtime_root / "research" / "daily-pipeline"),
            "source_spool_root": str(runtime_root / "live" / "daily-close"),
            "deployment_profile_path": str(runtime_root / "current" / "deployment-profile.json"),
            "mode": "shadow",
            "service_owner": "daily.pipeline.orchestrator.shadow.v1",
            "stages": ["raw_capture"],
            "stage_commands": [],
            "receipt_active_key_id": "daily-v2",
            "receipt_active_public_key_pem": "test-daily-active-public-key",
            "receipt_previous_public_key_pems": {"daily-v1": "test-daily-previous-public-key"},
            "receipt_signer_socket_endpoint": PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT,
            "receipt_trusted_keyring_path": str(PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH),
            "receipt_signer_timeout_seconds": 5.0,
        },
    )
    profile = RuntimeDeploymentProfile(
        producer_commit="a" * 40,
        runtime_mode="linux-production",
        production_runtime_root=str(runtime_root),
        manifests=(lab_jobs_manifest, daily_manifest),
        capability_environment={
            lab_jobs_manifest.service_id: (),
            daily_manifest.service_id: (),
        },
        page_control=PageControlRuntimeProfile.model_validate(
            {
                "endpoint": "http://127.0.0.1:8767/v1/commands",
                "outbox_path": runtime_root / "control" / "page-control.sqlite3",
                "data_dir": runtime_root / "serving" / "page-control",
                "log_dir": runtime_root / "control" / "page-control-logs",
                "page_projection_canvas_catalog_root": (
                    runtime_root / "serving" / "page-control" / "canvases"
                ),
                "canvas_publication": {
                    "active_key_id": "canvas-v1",
                    "active_public_key_pem": "test-canvas-public-key",
                    "previous_public_key_pems": {},
                    "signer_command": PRODUCTION_CANVAS_SIGNER_COMMAND,
                    "consumer_service_id": PRODUCTION_PAGE_CONTROL_SERVICE_ID,
                    "consumer_instance_id": PRODUCTION_PAGE_CONTROL_INSTANCE_ID,
                },
            }
        ),
        shadow=ShadowRuntimeProfile(
            completion_active_key_id="shadow-completion-v1",
            completion_active_public_key_pem="test-shadow-completion-public-key",
            completion_previous_public_key_pems={},
            report_active_key_id="shadow-report-v1",
            report_active_public_key_pem="test-shadow-report-public-key",
            report_previous_public_key_pems={},
            signer_command=PRODUCTION_SHADOW_SIGNER_COMMAND,
            report_producer_service_id=PRODUCTION_SHADOW_SERVICE_ID,
            report_producer_instance_id=PRODUCTION_SHADOW_INSTANCE_ID,
        ),
        lab_highwater=LabHighWaterRuntimeProfile(
            authority_command=PRODUCTION_LAB_HIGHWATER_COMMAND,
            stable_identity="daily-cli-production-profile-test",
            trusted_keyring_path=runtime_root.parent / "trusted-highwater-keys.json",
            production_mode=True,
        ),
    )
    receipt = install_runtime_deployment_profile(
        profile,
        runtime_root=runtime_root,
        environ={},
        schema_bootstrap_reason="daily CLI production-profile exclusion test",
    )
    return profile, receipt


def _daily_dev_cli_argv(profile_root: Path, *, action: str = "preview") -> list[str]:
    arguments = [
        "rquant",
        "daily-dag-dev",
        "--profile-root",
        str(profile_root),
        "--trade-date",
        TRADE_DATE.isoformat(),
        "--source-generation-id",
        "1" * 64,
        "--source-content-hash",
        "2" * 64,
        "--code-commit",
        "a" * 40,
        "--profile-hash",
        "b" * 64,
        "--command-manifest-hash",
        "3" * 64,
    ]
    if action != "preview":
        from rquant.daily_pipeline_control import DailyPipelineControlPlan

        storage_profile = DailyPipelineStorageProfile.create(
            root=profile_root,
            mode=DailyPipelineMode.SHADOW,
            profile_hash="b" * 64,
        )
        run_spec = DEFAULT_DAILY_CLOSE_PIPELINE.to_run_spec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=TRADE_DATE,
            source_generation_id="1" * 64,
            source_content_hash="2" * 64,
            command_manifest_hash="3" * 64,
            code_commit="a" * 40,
            profile_hash="b" * 64,
        )
        plan = DailyPipelineControlPlan.create(
            mode=DailyPipelineMode.SHADOW,
            run_spec=run_spec,
            command_manifest_hash="3" * 64,
            storage_profile=storage_profile,
        )
        arguments.extend(
            (
                "--action",
                action,
                "--apply",
                "--run-id",
                str(run_spec.run_id),
                "--plan-hash",
                str(plan.plan_hash),
                "--source-spool-root",
                str(profile_root.parent / "source-spool"),
            )
        )
    return arguments


def test_daily_dag_dev_real_cli_rejects_installed_production_profile_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import config as config_module
    from rquant import runtime_deployment_profile as deployment_module

    production_root = tmp_path / "managed-production-runtime"
    monkeypatch.setattr(
        deployment_module,
        "LINUX_PRODUCTION_RUNTIME_ROOT",
        production_root,
    )
    _install_valid_linux_production_profile(production_root)
    monkeypatch.setattr(config_module.settings, "app_env", "dev")
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("RQUANT_APP_ENV", "dev")
    monkeypatch.setenv("RQUANT_PRODUCTION_RUNTIME_ROOT", str(tmp_path / "attacker-root"))
    development_root = tmp_path / "daily-development"

    for action in ("preview", "apply", "status", "recover"):
        monkeypatch.setattr(
            sys,
            "argv",
            _daily_dev_cli_argv(development_root, action=action),
        )
        assert main() == 2

    assert development_root.exists() is False


def test_daily_dag_dev_fails_closed_on_polluted_production_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import config as config_module
    from rquant import runtime_deployment_profile as deployment_module

    production_root = tmp_path / "managed-production-runtime"
    monkeypatch.setattr(
        deployment_module,
        "LINUX_PRODUCTION_RUNTIME_ROOT",
        production_root,
    )
    _profile, receipt = _install_valid_linux_production_profile(production_root)
    installed_profile = (
        production_root / "generations" / receipt.generation_hash / "deployment-profile.json"
    )
    installed_profile.write_bytes(b"{}")
    installed_profile.chmod(0o600)
    monkeypatch.setattr(config_module.settings, "app_env", "dev")
    development_root = tmp_path / "daily-development"
    monkeypatch.setattr(sys, "argv", _daily_dev_cli_argv(development_root))

    assert main() == 2
    assert development_root.exists() is False


def test_daily_dag_dev_rejects_symlinked_fixed_production_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import config as config_module
    from rquant import runtime_deployment_profile as deployment_module

    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    production_root = tmp_path / "managed-production-runtime"
    production_root.symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(
        deployment_module,
        "LINUX_PRODUCTION_RUNTIME_ROOT",
        production_root,
    )
    monkeypatch.setattr(config_module.settings, "app_env", "dev")
    development_root = tmp_path / "daily-development"
    monkeypatch.setattr(sys, "argv", _daily_dev_cli_argv(development_root))

    assert main() == 2
    assert development_root.exists() is False
    assert list(outside.iterdir()) == []


def test_daily_dag_dev_rejects_symlinked_ancestor_when_final_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import config as config_module
    from rquant import runtime_deployment_profile as deployment_module

    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    linked_ancestor = tmp_path / "linked-ancestor"
    linked_ancestor.symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(
        deployment_module,
        "LINUX_PRODUCTION_RUNTIME_ROOT",
        linked_ancestor / "missing-runtime",
    )
    monkeypatch.setattr(config_module.settings, "app_env", "dev")
    development_root = tmp_path / "daily-development"
    monkeypatch.setattr(sys, "argv", _daily_dev_cli_argv(development_root))

    assert main() == 2
    assert development_root.exists() is False
    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("action", ("preview", "apply"))
def test_daily_dag_dev_rejects_absent_to_present_production_root_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    from rquant import cli as cli_module
    from rquant import config as config_module
    from rquant import runtime_deployment_profile as deployment_module

    production_parent = tmp_path / "managed-production-parent"
    production_parent.mkdir(mode=0o700)
    production_root = production_parent / "runtime"
    monkeypatch.setattr(
        deployment_module,
        "LINUX_PRODUCTION_RUNTIME_ROOT",
        production_root,
    )
    monkeypatch.setattr(config_module.settings, "app_env", "dev")
    development_root = tmp_path / "daily-development"
    real_plan = cli_module._daily_dag_control_plan

    def create_production_root_after_initial_check(args):
        plan = real_plan(args)
        production_root.mkdir(mode=0o700)
        return plan

    monkeypatch.setattr(
        cli_module,
        "_daily_dag_control_plan",
        create_production_root_after_initial_check,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        _daily_dev_cli_argv(development_root, action=action),
    )

    assert main() == 2
    assert development_root.exists() is False


def _write_manifest(storage_profile: DailyPipelineStorageProfile) -> DailyPipelineCommandManifest:
    child = (
        "from datetime import UTC,datetime; "
        "from rquant.daily_pipeline_command_manifest import "
        "publish_external_stage_receipt_from_environment; "
        "from rquant.daily_pipeline_ledger import StageResult; "
        "publish_external_stage_receipt_from_environment("
        "StageResult(content_hash='a'*64,evidence_hash='b'*64),"
        "issued_at=datetime.now(UTC))"
    )
    manifest = DailyPipelineCommandManifest(
        mode=storage_profile.mode,
        storage_profile=storage_profile,
        stages=tuple(
            DailyPipelineStageCommand(
                stage_id=stage.stage_id,
                adapter_identity=f"cli-e2e/{stage.stage_id}/v1",
                argv=(sys.executable, "-c", child),
                receipt_root=storage_profile.receipt_root,
                receipt_key_id="cli-e2e-stage",
            )
            for stage in DEFAULT_DAILY_CLOSE_PIPELINE.stages
        ),
    )
    path = storage_profile.command_manifest_path
    DailyPipelineStorageBinding.open(storage_profile, leaf="control").close()
    path.write_bytes(canonical_model_json_bytes(manifest))
    path.chmod(0o600)
    return manifest


@pytest.fixture
def report_authority(tmp_path: Path) -> tuple[Path, bytes]:
    helper = Path(__file__).resolve().parents[2] / "deploy/libexec/rquant-lab-highwater-authority"
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is required for the real Ed25519 authority fixture")
    private_key = tmp_path / "report-authority-private.pem"
    public_key = tmp_path / "report-authority-public.pem"
    subprocess.run(
        [openssl, "genpkey", "-algorithm", "ED25519", "-out", str(private_key)],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [openssl, "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)],
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    public_key.chmod(0o600)
    keys = tmp_path / "report-authority-keys.json"
    keys.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "generation": 1,
                "previous_manifest_hash": "0" * 64,
                "active_key_id": "daily-report-authority",
                "active_private_key_path": str(private_key),
                "previous_public_keys": {},
            }
        ),
        encoding="utf-8",
    )
    keys.chmod(0o600)
    wrapper = tmp_path / "daily-report-authority"
    wrapper.write_text(
        "#!/bin/sh\n"
        f"exec {sys.executable} {helper} "
        f"--state-root {tmp_path / 'report-authority-state'} "
        f"--keys-file {keys}\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o700)
    return wrapper.resolve(), public_key.read_bytes()


def _development_report_authority(
    capability: Path,
    public_key: bytes,
    storage_profile: DailyPipelineStorageProfile,
) -> DailyPipelineDevelopmentTestReportAuthority:
    from rquant.daily_pipeline_report_authority import DailyPipelineReportAuthorityClient
    from rquant.lab_highwater_authority import (
        LabHighWaterAuthorityClient,
        LabHighWaterAuthorityConfig,
    )

    authority: DailyPipelineReportAuthorityCapability = DailyPipelineReportAuthorityClient(
        LabHighWaterAuthorityClient(
            LabHighWaterAuthorityConfig(
                command=(str(capability),),
                stable_identity=(
                    "daily-pipeline-report:daily-close:"
                    f"{storage_profile.mode.value}:{storage_profile.namespace_id}:v2"
                ),
                code_identity="a" * 40,
                profile_identity=storage_profile.profile_hash,
                active_key_id="daily-report-authority",
                trusted_key_provider=lambda key_id: (
                    public_key if key_id == "daily-report-authority" else None
                ),
                allow_identity_rotation=True,
            )
        )
    )
    return DailyPipelineDevelopmentTestReportAuthority(capability=authority)


def test_daily_dag_apply_requires_preview_binding_and_advances_one_receipted_stage(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
    report_authority: tuple[Path, bytes],
) -> None:
    monkeypatch.setenv("RQUANT_DAILY_EXTERNAL_RECEIPT_KEY_ID", "cli-e2e-stage")
    monkeypatch.setenv("RQUANT_DAILY_EXTERNAL_RECEIPT_SECRET", "s" * 32)
    gateway = _gateway(
        tmp_path,
        lambda _request: _snapshot(),
        completion_clock=lambda: AVAILABLE_AT,
    )
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    current = gateway.spool.current(LiveChannel.DAILY_CLOSE)
    assert current is not None
    record = gateway.spool.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)[0]
    payload = DailyCloseGateway.decode_payload(gateway.spool.read_payload(record))
    storage_profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="b" * 64,
    )
    manifest = _write_manifest(storage_profile)
    state_path = storage_profile.state_path
    base = [
        "daily-dag-dev",
        "--profile-root",
        str(storage_profile.root),
        "--trade-date",
        TRADE_DATE.isoformat(),
        "--source-generation-id",
        current.source_generation_id,
        "--source-content-hash",
        payload.content_sha256,
        "--code-commit",
        "a" * 40,
        "--profile-hash",
        "b" * 64,
        "--command-manifest-hash",
        manifest.manifest_hash,
        "--source-spool-root",
        str((tmp_path / "live").resolve()),
    ]
    parser = build_parser()

    development_authority = _development_report_authority(
        *report_authority,
        storage_profile,
    )

    assert (
        cmd_daily_dag(
            parser.parse_args(base),
            development_test_report_authority=development_authority,
        )
        == 0
    )
    preview = json.loads(capsys.readouterr().out)
    assert state_path.exists() is False

    apply = [
        *base,
        "--action",
        "apply",
        "--apply",
        "--run-id",
        preview["run_id"],
        "--plan-hash",
        preview["plan_hash"],
    ]
    assert (
        cmd_daily_dag(
            parser.parse_args(apply),
            development_test_report_authority=development_authority,
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)

    assert result["outcome"]["stage_id"] == "raw_capture"
    assert result["outcome"]["disposition"] == "succeeded"
    assert result["status"]["next_stage_id"] == "validate_candidate"

    status = [
        *base,
        "--action",
        "status",
        "--run-id",
        preview["run_id"],
        "--plan-hash",
        preview["plan_hash"],
    ]
    assert (
        cmd_daily_dag(
            parser.parse_args(status),
            development_test_report_authority=development_authority,
        )
        == 0
    )
    status_result = json.loads(capsys.readouterr().out)
    assert status_result["write_performed"] is False
    assert status_result["status"]["stage_states"]["raw_capture"] == "succeeded"

    recover = [
        *base,
        "--action",
        "recover",
        "--apply",
        "--run-id",
        preview["run_id"],
        "--plan-hash",
        preview["plan_hash"],
    ]
    assert (
        cmd_daily_dag(
            parser.parse_args(recover),
            development_test_report_authority=development_authority,
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["recovery"]["failed_stage_ids"] == []

    retry = [
        *base,
        "--action",
        "retry",
        "--apply",
        "--run-id",
        preview["run_id"],
        "--plan-hash",
        preview["plan_hash"],
    ]
    assert (
        cmd_daily_dag(
            parser.parse_args(retry),
            development_test_report_authority=development_authority,
        )
        == 0
    )
    retry_result = json.loads(capsys.readouterr().out)
    assert retry_result["outcome"]["stage_id"] == "validate_candidate"

    for _unused in DEFAULT_DAILY_CLOSE_PIPELINE.stages[2:]:
        assert (
            cmd_daily_dag(
                parser.parse_args(apply),
                development_test_report_authority=development_authority,
            )
            == 0
        )
        result = json.loads(capsys.readouterr().out)

    assert result["status"]["state"] == "succeeded"
    assert result["report"] is not None
    assert Path(result["report"]["path"]).is_file()

    # Repeating the terminal apply must reuse the immutable report rather than
    # advance the monotonic authority with a clock-dependent replacement.
    assert (
        cmd_daily_dag(
            parser.parse_args(apply),
            development_test_report_authority=development_authority,
        )
        == 0
    )
    repeated = json.loads(capsys.readouterr().out)
    assert repeated["outcome"] is None
    assert repeated["report"] == result["report"]
