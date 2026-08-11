from __future__ import annotations

import json
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import rquant.cli as cli_module
from rquant.cli import (
    build_parser,
    cmd_preflight,
    cmd_runtime_deployment_profile,
    cmd_runtime_deployment_rollback,
    cmd_runtime_deployment_rollout,
    cmd_runtime_production_prerequisites,
    cmd_runtime_production_profile,
    cmd_runtime_recovery_production_config,
    cmd_runtime_schema_retirement,
)
from rquant.preflight import CheckResult, RuntimeRecoveryPreflightConfig
from rquant.runtime_deployment_bundle import RuntimeDeploymentReceipt
from rquant.runtime_deployment_profile import (
    RuntimeDeploymentProfile,
    RuntimeDeploymentProfilePreview,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

COMMIT = "a" * 40


def _profile(tmp_path: Path) -> RuntimeDeploymentProfile:
    manifest = RuntimeServiceManifest(
        service_id="lab-jobs.serving.v1",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(tmp_path / "research" / "lab_jobs.sqlite3"),
            "authority_root": str(tmp_path / "research" / "serving-authorities" / "lab-jobs"),
        },
    )
    return RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
    )


def test_parser_requires_apply_and_profile_id_together(tmp_path: Path) -> None:
    common = [
        "runtime-deployment-profile",
        "--profile",
        str(tmp_path / "profile.json"),
        "--runtime-root",
        str(tmp_path / "runtime"),
        "--expected-commit",
        COMMIT,
    ]

    assert build_parser().parse_args(common).apply is False
    with pytest.raises(SystemExit):
        build_parser().parse_args([*common, "--apply"])
    with pytest.raises(SystemExit):
        build_parser().parse_args([*common, "--profile-id", "b" * 64])


def test_runtime_production_profile_preview_is_pure_and_does_not_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile = _profile(tmp_path)
    assert profile.profile_id is not None
    inputs = Namespace(runtime_root=tmp_path / "runtime")
    output_dir = tmp_path / "profiles"
    expected_path = output_dir / f"{profile.profile_id}.json"
    monkeypatch.setattr(
        "rquant.runtime_production_profile.load_production_runtime_profile_inputs",
        lambda *args, **kwargs: inputs,
    )
    monkeypatch.setattr(
        "rquant.runtime_production_profile.build_production_runtime_profile",
        lambda value: profile if value is inputs else pytest.fail("wrong production inputs"),
    )

    monkeypatch.setattr(
        "rquant.runtime_production_profile.publish_production_runtime_profile",
        lambda *_args, **_kwargs: pytest.fail("preview must not publish"),
    )

    result = cmd_runtime_production_profile(
        Namespace(
            inputs=tmp_path / "inputs.json",
            output_dir=output_dir,
            expected_commit=COMMIT,
            apply=False,
            profile_id=None,
        )
    )

    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "dry_run"
    assert output["profile_id"] == profile.profile_id
    assert output["profile_path"] == str(expected_path)
    assert not output_dir.exists()


def test_runtime_production_profile_apply_publishes_exact_previewed_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile = _profile(tmp_path)
    assert profile.profile_id is not None
    inputs = Namespace(runtime_root=tmp_path / "runtime")
    output_dir = tmp_path / "profiles"
    expected_path = output_dir / f"{profile.profile_id}.json"
    observed: dict[str, object] = {}
    monkeypatch.setattr(
        "rquant.runtime_production_profile.load_production_runtime_profile_inputs",
        lambda *args, **kwargs: inputs,
    )
    monkeypatch.setattr(
        "rquant.runtime_production_profile.build_production_runtime_profile",
        lambda value: profile if value is inputs else pytest.fail("wrong production inputs"),
    )

    def publish(
        value: object,
        path: Path,
        *,
        production_runtime_root: Path,
    ) -> Path:
        observed.update(
            profile=value,
            path=path,
            production_runtime_root=production_runtime_root,
        )
        return path

    monkeypatch.setattr(
        "rquant.runtime_production_profile.publish_production_runtime_profile",
        publish,
    )

    result = cmd_runtime_production_profile(
        Namespace(
            inputs=tmp_path / "inputs.json",
            output_dir=output_dir,
            expected_commit=COMMIT,
            apply=True,
            profile_id=profile.profile_id,
        )
    )

    assert result == 0
    assert observed == {
        "profile": profile,
        "path": expected_path,
        "production_runtime_root": inputs.runtime_root,
    }
    assert json.loads(capsys.readouterr().out)["status"] == "published"


def test_parser_accepts_runtime_production_profile_command(tmp_path: Path) -> None:
    parsed = build_parser().parse_args(
        [
            "runtime-production-profile",
            "--inputs",
            str(tmp_path / "inputs.json"),
            "--output-dir",
            str(tmp_path / "profiles"),
            "--expected-commit",
            COMMIT,
        ]
    )

    assert parsed.command == "runtime-production-profile"
    assert parsed.apply is False
    assert parsed.runtime_mode == "local-test"


def test_parser_accepts_explicit_linux_production_runtime_mode(tmp_path: Path) -> None:
    parsed = build_parser().parse_args(
        [
            "runtime-production-profile",
            "--inputs",
            str(tmp_path / "inputs.json"),
            "--output-dir",
            str(tmp_path / "profiles"),
            "--expected-commit",
            COMMIT,
            "--runtime-mode",
            "linux-production",
        ]
    )

    assert parsed.runtime_mode == "linux-production"


def test_parser_requires_apply_and_profile_id_for_runtime_production_profile(
    tmp_path: Path,
) -> None:
    common = [
        "runtime-production-profile",
        "--inputs",
        str(tmp_path / "inputs.json"),
        "--output-dir",
        str(tmp_path / "profiles"),
        "--expected-commit",
        COMMIT,
    ]

    with pytest.raises(SystemExit):
        build_parser().parse_args([*common, "--apply"])
    with pytest.raises(SystemExit):
        build_parser().parse_args([*common, "--profile-id", "b" * 64])
    parsed = build_parser().parse_args([*common, "--apply", "--profile-id", "b" * 64])
    assert parsed.apply is True
    assert parsed.profile_id == "b" * 64


def test_preflight_runtime_root_uses_current_profile_recovery_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime_root = tmp_path / "runtime"
    profile = object()
    recovery = RuntimeRecoveryPreflightConfig(
        publication_root=tmp_path / "backup",
        service_state_path=runtime_root / "control" / "recovery" / "service.sqlite3",
        service_receipt_root=runtime_root / "control" / "recovery" / "receipts",
        restore_root=tmp_path / "restore",
        expected_profile_generation="b" * 64,
        expected_manifest_id=None,
        max_rpo=__import__("datetime").timedelta(minutes=30),
        max_rehearsal_age=timedelta(days=7),
        max_rto=__import__("datetime").timedelta(minutes=15),
    )
    observed: dict[str, object] = {}
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda root: profile if Path(root) == runtime_root else pytest.fail("wrong runtime root"),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.build_runtime_recovery_preflight_config",
        lambda value: recovery if value is profile else pytest.fail("wrong profile"),
    )

    def checks(**kwargs: object) -> list[CheckResult]:
        observed.update(kwargs)
        return [CheckResult("runtime_recovery", "ok", "ready")]

    monkeypatch.setattr("rquant.preflight.run_all_checks", checks)

    result = cmd_preflight(Namespace(profile="production", notify=False, runtime_root=runtime_root))

    assert result == 0
    assert observed["recovery_config"] is recovery
    assert "ready" in capsys.readouterr().out


def test_preflight_runtime_root_fails_closed_when_recovery_profile_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda _root: (_ for _ in ()).throw(ValueError("profile missing")),
    )
    monkeypatch.setattr("rquant.preflight.run_all_checks", lambda **_kwargs: [])

    result = cmd_preflight(
        Namespace(profile="production", notify=False, runtime_root=tmp_path / "runtime")
    )

    assert result == 1
    assert "recovery production profile" in capsys.readouterr().out


def test_recovery_production_config_command_emits_only_current_hash_bound_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime_root = tmp_path / "runtime"
    profile = type(
        "Profile",
        (),
        {
            "profile_id": "a" * 64,
            "producer_commit": COMMIT,
            "recovery": type(
                "Recovery",
                (),
                {
                    "profile_generation": "b" * 64,
                    "backup_config_path": tmp_path / "backup.json",
                    "backup_environment": lambda self: {
                        "RQUANT_RECOVERY_BACKUP_ENABLED": "true",
                        "RQUANT_RECOVERY_PROFILE_GENERATION": "b" * 64,
                    },
                    "recovery_service_arguments": lambda self: {
                        "deadline_seconds": "3600",
                    },
                },
            )(),
        },
    )()
    backup_config = object()
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda root: profile if Path(root) == runtime_root else pytest.fail("wrong root"),
    )
    monkeypatch.setattr(
        "rquant.runtime_recovery_backup.load_recovery_backup_config",
        lambda path: (
            backup_config
            if Path(path) == profile.recovery.backup_config_path
            else pytest.fail("wrong backup config")
        ),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.validate_runtime_recovery_backup_config",
        lambda candidate, config: (
            None
            if candidate is profile and config is backup_config
            else pytest.fail("untrusted config")
        ),
    )

    result = cmd_runtime_recovery_production_config(Namespace(runtime_root=runtime_root))

    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output == {
        "backup_environment": {
            "RQUANT_RECOVERY_BACKUP_ENABLED": "true",
            "RQUANT_RECOVERY_PROFILE_GENERATION": "b" * 64,
        },
        "producer_commit": COMMIT,
        "profile_generation": "b" * 64,
        "profile_id": "a" * 64,
        "recovery_service_arguments": {"deadline_seconds": "3600"},
        "runtime_root": str(runtime_root),
        "status": "ready",
    }


@pytest.mark.parametrize(
    ("action", "expected_schedule"),
    (("execute", None), ("rehearse", 604_800)),
)
def test_recovery_production_runner_resolves_every_argument_from_current_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    expected_schedule: int | None,
) -> None:
    runtime_root = tmp_path / "runtime"
    generation = "b" * 64
    recovery_arguments = {
        "publication_root": "/var/lib/rquant/runtime-recovery/backups",
        "state_path": str(runtime_root / "control" / "recovery" / "service.sqlite3"),
        "receipt_root": str(runtime_root / "control" / "recovery" / "receipts"),
        "restore_root": "/var/lib/rquant/runtime-recovery/restores",
        "credential_file": str(tmp_path / "credential.json"),
        "lease_seconds": "300",
        "max_attempts": "4",
        "retry_delay_seconds": "45",
        "deadline_seconds": "3600",
        "rehearsal_interval_seconds": "604800",
    }
    recovery = type(
        "Recovery",
        (),
        {
            "profile_generation": generation,
            "backup_config_path": tmp_path / "backup-config.json",
            "recovery_service_arguments": lambda self: recovery_arguments,
        },
    )()
    profile = type("Profile", (), {"recovery": recovery})()
    backup_config = object()
    observed: dict[str, object] = {}
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda root: profile if Path(root) == runtime_root else pytest.fail("wrong runtime root"),
    )
    monkeypatch.setattr(
        "rquant.runtime_recovery_backup.load_recovery_backup_config",
        lambda path: (
            backup_config
            if Path(path) == recovery.backup_config_path
            else pytest.fail("wrong backup config")
        ),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.validate_runtime_recovery_backup_config",
        lambda candidate, config: (
            None
            if candidate is profile and config is backup_config
            else pytest.fail("untrusted recovery config")
        ),
    )

    def run(resolved: Namespace) -> int:
        observed.update(vars(resolved))
        return 0

    monkeypatch.setattr(cli_module, "cmd_runtime_recovery", run)

    result = cli_module.cmd_runtime_recovery_production(
        Namespace(
            runtime_root=runtime_root,
            expected_profile_generation=generation,
            production_recovery_action=action,
        )
    )

    assert result == 0
    assert observed == {
        "recovery_action": "execute",
        "publication_root": Path(recovery_arguments["publication_root"]),
        "state_path": Path(recovery_arguments["state_path"]),
        "receipt_root": Path(recovery_arguments["receipt_root"]),
        "restore_root": Path(recovery_arguments["restore_root"]),
        "credential_file": Path(recovery_arguments["credential_file"]),
        "lease_seconds": 300,
        "max_attempts": 4,
        "retry_delay_seconds": 45,
        "deadline_seconds": 3600,
        "schedule_cycle_seconds": expected_schedule,
        "worker_id": f"runtime-recovery-{action}-{generation[:12]}",
        "accept_current_plan": True,
        "plan_id": None,
    }


def test_recovery_rehearsal_skips_idempotently_until_profile_interval_is_due(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime_root = tmp_path / "runtime"
    generation = "b" * 64
    state_path = runtime_root / "control" / "recovery" / "service.sqlite3"
    receipt_root = state_path.parent / "receipts"
    state_path.parent.mkdir(parents=True)
    state_path.touch(mode=0o600)
    receipt_root.mkdir()
    now = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)
    interval = 3600
    recovery_arguments = {
        "publication_root": "/var/lib/rquant/runtime-recovery/backups",
        "state_path": str(state_path),
        "receipt_root": str(receipt_root),
        "restore_root": "/var/lib/rquant/runtime-recovery/restores",
        "credential_file": str(tmp_path / "credential.json"),
        "lease_seconds": "300",
        "max_attempts": "4",
        "retry_delay_seconds": "45",
        "deadline_seconds": "3600",
        "rehearsal_interval_seconds": str(interval),
    }
    recovery = SimpleNamespace(
        profile_generation=generation,
        backup_config_path=tmp_path / "backup-config.json",
        recovery_service_arguments=lambda: recovery_arguments,
    )
    profile = SimpleNamespace(recovery=recovery)
    backup_config = object()
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda _root: profile,
    )
    monkeypatch.setattr(
        "rquant.runtime_recovery_backup.load_recovery_backup_config",
        lambda _path: backup_config,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.validate_runtime_recovery_backup_config",
        lambda _profile, _config: None,
    )
    monkeypatch.setattr(
        "rquant.runtime_recovery_service.load_verified_recovery_service_receipts",
        lambda **_kwargs: (
            SimpleNamespace(
                status="succeeded",
                verification_level="full",
                completed_at=now - timedelta(minutes=30),
            ),
        ),
    )
    monkeypatch.setattr(cli_module, "_utc_now", lambda: now)
    monkeypatch.setattr(
        cli_module,
        "cmd_runtime_recovery",
        lambda _args: pytest.fail("not-due rehearsal must not run"),
    )

    result = cli_module.cmd_runtime_recovery_production(
        Namespace(
            runtime_root=runtime_root,
            expected_profile_generation=generation,
            production_recovery_action="rehearse",
        )
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out) == {
        "last_successful_at": "2026-08-03T01:00:00+00:00",
        "next_due_at": "2026-08-03T02:00:00+00:00",
        "profile_generation": generation,
        "reason": "rehearsal_not_due",
        "status": "skipped",
    }


def test_recovery_production_runner_rejects_stale_unit_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    current_generation = "b" * 64
    profile = type(
        "Profile",
        (),
        {
            "recovery": type(
                "Recovery",
                (),
                {"profile_generation": current_generation},
            )()
        },
    )()
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_current_runtime_deployment_profile",
        lambda _root: profile,
    )
    monkeypatch.setattr(
        cli_module,
        "cmd_runtime_recovery",
        lambda _args: pytest.fail("stale generation must fail before recovery"),
    )

    with pytest.raises(ValueError, match="generation"):
        cli_module.cmd_runtime_recovery_production(
            Namespace(
                runtime_root=runtime_root,
                expected_profile_generation="c" * 64,
                production_recovery_action="execute",
            )
        )


@pytest.mark.parametrize("action", ("execute", "rehearse"))
def test_parser_accepts_profile_bound_recovery_runner(action: str, tmp_path: Path) -> None:
    parsed = build_parser().parse_args(
        [
            "runtime-recovery-production",
            action,
            "--runtime-root",
            str(tmp_path / "runtime"),
            "--expected-profile-generation",
            "b" * 64,
        ]
    )

    assert parsed.production_recovery_action == action


def test_parser_requires_apply_and_profile_id_for_production_prerequisites(
    tmp_path: Path,
) -> None:
    common = [
        "runtime-production-prerequisites",
        "--inputs",
        str(tmp_path / "inputs.json"),
        "--expected-commit",
        COMMIT,
    ]

    assert build_parser().parse_args(common).apply is False
    with pytest.raises(SystemExit):
        build_parser().parse_args([*common, "--apply"])
    with pytest.raises(SystemExit):
        build_parser().parse_args([*common, "--profile-id", "b" * 64])


def test_runtime_production_prerequisites_dry_run_then_bound_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_profile = _profile(tmp_path)
    retention = RuntimeServiceManifest(
        service_id="artifact-retention.primary.v1",
        service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=300,
        stale_after_seconds=900,
        producer_commit=COMMIT,
        settings={
            "state_root": str(tmp_path / "runtime" / "research" / "artifact-retention" / "svc"),
            "reference_store_path": str(
                tmp_path
                / "runtime"
                / "research"
                / "artifact-retention"
                / "svc"
                / "references.sqlite3"
            ),
            "catalog_authority_root": str(
                tmp_path
                / "runtime"
                / "research"
                / "artifact-retention"
                / "svc"
                / "catalog-authority"
            ),
        },
    )
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(*base_profile.manifests, retention),
        capability_environment={
            **base_profile.capability_environment,
            retention.service_id: (),
        },
    )
    assert profile.profile_id is not None
    inputs = type(
        "Inputs",
        (),
        {
            "runtime_root": tmp_path / "runtime",
            "market_calendar_content_sha256": "c" * 64,
            "definition_registry_root": tmp_path / "definitions",
        },
    )()
    installs: list[object] = []
    monkeypatch.setattr(
        "rquant.runtime_production_profile.load_production_runtime_profile_inputs",
        lambda *args, **kwargs: inputs,
    )
    monkeypatch.setattr(
        "rquant.runtime_production_profile.build_production_runtime_profile",
        lambda value: profile if value is inputs else pytest.fail("wrong production inputs"),
    )
    monkeypatch.setattr(
        "rquant.runtime_production_profile.install_production_runtime_prerequisites",
        lambda value: (
            installs.append(value)
            or (
                tmp_path / "calendar.json",
                tmp_path / "definitions",
                tmp_path
                / "runtime"
                / "research"
                / "artifact-retention"
                / "svc"
                / "catalog-authority"
                / "current.json",
            )
        ),
    )

    dry_run = cmd_runtime_production_prerequisites(
        Namespace(
            inputs=tmp_path / "inputs.json",
            expected_commit=COMMIT,
            apply=False,
            profile_id=None,
        )
    )
    assert dry_run == 0
    assert installs == []
    preview = json.loads(capsys.readouterr().out)
    assert preview["status"] == "dry_run"
    assert preview["targets"] == [
        str(
            tmp_path
            / "runtime"
            / "authorities"
            / "market-calendar"
            / "generations"
            / f"{'c' * 64}.json"
        ),
        str(tmp_path / "definitions"),
        str(
            next(
                Path(str(manifest.settings["catalog_authority_root"])) / "current.json"
                for manifest in profile.manifests
                if manifest.service_kind.value == "artifact_retention"
            )
        ),
    ]

    mismatch = cmd_runtime_production_prerequisites(
        Namespace(
            inputs=tmp_path / "inputs.json",
            expected_commit=COMMIT,
            apply=True,
            profile_id="d" * 64,
        )
    )
    assert mismatch == 2
    assert installs == []

    applied = cmd_runtime_production_prerequisites(
        Namespace(
            inputs=tmp_path / "inputs.json",
            expected_commit=COMMIT,
            apply=True,
            profile_id=profile.profile_id,
        )
    )
    assert applied == 0
    assert installs == [inputs]
    assert "applied" in capsys.readouterr().out


def test_runtime_deployment_profile_dry_run_is_read_only_and_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile = _profile(tmp_path)
    preview = RuntimeDeploymentProfilePreview(
        profile_id=profile.profile_id,
        producer_commit=COMMIT,
        runtime_root=tmp_path / "runtime",
        service_ids=(profile.manifests[0].service_id,),
        capability_names={profile.manifests[0].service_id: ()},
    )
    calls: list[str] = []
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_runtime_deployment_profile",
        lambda *args, **kwargs: profile,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.preview_runtime_deployment_profile",
        lambda *args, **kwargs: preview,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.install_runtime_deployment_profile",
        lambda *args, **kwargs: calls.append("install"),
    )

    result = cmd_runtime_deployment_profile(
        Namespace(
            profile=tmp_path / "profile.json",
            runtime_root=tmp_path / "runtime",
            expected_commit=COMMIT,
            apply=False,
            profile_id=None,
            schema_bootstrap_reason=None,
        )
    )

    assert result == 0
    assert calls == []
    output = capsys.readouterr().out
    assert str(profile.profile_id) in output
    assert "secret" not in output


def test_runtime_deployment_profile_apply_requires_the_previewed_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _profile(tmp_path)
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_runtime_deployment_profile",
        lambda *args, **kwargs: profile,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.install_runtime_deployment_profile",
        lambda *args, **kwargs: pytest.fail("mismatched profile must not install"),
    )

    result = cmd_runtime_deployment_profile(
        Namespace(
            profile=tmp_path / "profile.json",
            runtime_root=tmp_path / "runtime",
            expected_commit=COMMIT,
            apply=True,
            profile_id="b" * 64,
            schema_bootstrap_reason=None,
        )
    )

    assert result == 2


def test_runtime_deployment_profile_apply_requires_explicit_v1_migration_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = object()
    profile = _profile(tmp_path).model_copy(update={"schema_v1_migration_authority": authority})
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_runtime_deployment_profile",
        lambda *args, **kwargs: profile,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.install_runtime_deployment_profile",
        lambda *args, **kwargs: pytest.fail("implicit migration must not install"),
    )

    result = cmd_runtime_deployment_profile(
        Namespace(
            profile=tmp_path / "profile.json",
            runtime_root=tmp_path / "runtime",
            expected_commit=COMMIT,
            apply=True,
            profile_id=profile.profile_id,
            schema_bootstrap_reason=None,
            schema_v1_migration_authority=None,
        )
    )

    assert result == 2


def test_runtime_deployment_profile_apply_accepts_only_matching_explicit_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = object()
    profile = _profile(tmp_path).model_copy(update={"schema_v1_migration_authority": authority})
    installed: list[object] = []
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_runtime_deployment_profile",
        lambda *args, **kwargs: profile,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.load_runtime_schema_v1_migration_authorization",
        lambda path: authority if Path(path) == tmp_path / "authority.json" else object(),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile.install_runtime_deployment_profile",
        lambda candidate, **_kwargs: (
            installed.append(candidate)
            or RuntimeDeploymentReceipt(
                runtime_root=tmp_path / "runtime",
                producer_commit=COMMIT,
                generation_hash="d" * 64,
                deployment_profile_id=str(profile.profile_id),
                instance_mapping={"service": "svc-" + "1" * 64},
                unit_mapping={"service": "rquant-runtime-serving@svc-" + "1" * 64 + ".service"},
            )
        ),
    )

    result = cmd_runtime_deployment_profile(
        Namespace(
            profile=tmp_path / "profile.json",
            runtime_root=tmp_path / "runtime",
            expected_commit=COMMIT,
            apply=True,
            profile_id=profile.profile_id,
            schema_bootstrap_reason=None,
            schema_v1_migration_authority=tmp_path / "authority.json",
        )
    )

    assert result == 0
    assert installed == [profile]


@pytest.mark.parametrize(
    ("audit_status", "expected_exit"),
    (("succeeded", 0), ("rolled_back", 2), ("failed_closed", 2)),
)
def test_runtime_deployment_rollout_binds_current_and_previous_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    audit_status: str,
    expected_exit: int,
) -> None:
    runtime_root = tmp_path / "runtime"
    instance = f"svc-{'1' * 64}"
    previous = RuntimeDeploymentReceipt(
        runtime_root=runtime_root,
        producer_commit=COMMIT,
        generation_hash="c" * 64,
        deployment_profile_id="d" * 64,
        instance_mapping={"service": instance},
        unit_mapping={
            "service": f"rquant-runtime-market-minute@{instance}.service",
        },
    )
    current = RuntimeDeploymentReceipt(
        runtime_root=runtime_root,
        producer_commit=COMMIT,
        generation_hash="e" * 64,
        deployment_profile_id="f" * 64,
        instance_mapping=previous.instance_mapping,
        unit_mapping=previous.unit_mapping,
    )
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle.load_current_runtime_deployment_receipt",
        lambda *args, **kwargs: current,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle.load_runtime_deployment_generation_receipt",
        lambda *args, **kwargs: previous,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.build_runtime_generation_health_probe",
        lambda: object(),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.SystemdRuntimeRolloutController",
        lambda **kwargs: object(),
    )

    class Audit:
        status = audit_status

        def model_dump_json(self, *, indent: int) -> str:
            assert indent == 2
            return f'{{"status":"{self.status}"}}'

    def rollout(receipt: RuntimeDeploymentReceipt, **kwargs: object) -> Audit:
        observed.update(receipt=receipt, **kwargs)
        loader = kwargs["previous_receipt_loader"]
        assert callable(loader)
        assert loader(previous.generation_hash) == previous
        return Audit()

    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.rollout_runtime_deployment",
        rollout,
    )

    result = cmd_runtime_deployment_rollout(
        Namespace(
            runtime_root=runtime_root,
            expected_commit=COMMIT,
            profile_id=current.deployment_profile_id,
            generation_hash=current.generation_hash,
            previous_generation_hash=previous.generation_hash,
            audit_root=None,
            health_timeout_seconds=30.0,
        )
    )

    assert result == expected_exit
    bound = observed["receipt"]
    assert isinstance(bound, RuntimeDeploymentReceipt)
    assert bound.previous_generation_hash == previous.generation_hash
    assert observed["audit_root"] == runtime_root / "control" / "deployment-rollouts"
    assert audit_status in capsys.readouterr().out


def test_schema_retirement_parser_separates_read_only_status_from_explicit_apply(
    tmp_path: Path,
) -> None:
    common = [
        "--runtime-root",
        str(tmp_path / "runtime"),
        "--expected-commit",
        COMMIT,
        "--profile-id",
        "b" * 64,
        "--generation-hash",
        "c" * 64,
        "--rollout-operation-id",
        "d" * 64,
    ]
    status = build_parser().parse_args(["runtime-schema-retirement", "status", *common])
    assert status.retirement_action == "status"
    dry_run = build_parser().parse_args(
        ["runtime-schema-retirement", "dry-run", *common, "--plan-id", "e" * 64]
    )
    assert dry_run.retirement_action == "dry-run"
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["runtime-schema-retirement", "apply", *common, "--plan-id", "e" * 64]
        )


def test_schema_retirement_dry_run_never_advances_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime_root = tmp_path / "runtime"
    plan_id = "e" * 64
    receipt = type(
        "Receipt",
        (),
        {"generation_hash": "c" * 64},
    )()
    status = type(
        "Status",
        (),
        {
            "plan_id": plan_id,
            "eligible": True,
            "model_dump": lambda self, **_kwargs: {
                "plan_id": self.plan_id,
                "eligible": self.eligible,
            },
        },
    )()
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle.load_current_runtime_deployment_receipt",
        lambda *args, **kwargs: receipt,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.load_runtime_deployment_rollout_audit",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.preview_runtime_schema_retirement",
        lambda *args, **kwargs: (status,),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.retire_runtime_schema_plan",
        lambda *args, **kwargs: pytest.fail("dry-run must not retire"),
    )

    result = cmd_runtime_schema_retirement(
        Namespace(
            retirement_action="dry-run",
            runtime_root=runtime_root,
            expected_commit=COMMIT,
            profile_id="b" * 64,
            generation_hash="c" * 64,
            rollout_operation_id="d" * 64,
            audit_root=None,
            plan_id=plan_id,
        )
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out)["status"] == "eligible"


def test_runtime_deployment_rollback_is_a_noop_when_previous_is_already_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    previous = RuntimeDeploymentReceipt(
        runtime_root=tmp_path / "runtime",
        producer_commit=COMMIT,
        generation_hash="c" * 64,
        deployment_profile_id="d" * 64,
        instance_mapping={"service": "svc-" + "1" * 64},
        unit_mapping={"service": "rquant-runtime-serving@svc-" + "1" * 64 + ".service"},
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle.load_current_runtime_deployment_receipt_unbound",
        lambda root: previous,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_rollout.rollback_runtime_deployment",
        lambda *args, **kwargs: pytest.fail("already-current rollback must not restart services"),
    )

    result = cmd_runtime_deployment_rollback(
        Namespace(
            runtime_root=previous.runtime_root,
            failed_commit="f" * 40,
            expected_previous_commit=COMMIT,
            operation_id="e" * 64,
            audit_root=None,
            health_timeout_seconds=30.0,
        )
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out)["status"] == "already_rolled_back"
