from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant import runtime_deployment_bundle as deployment_module
from rquant.lab_highwater_authority import PRODUCTION_LAB_HIGHWATER_COMMAND
from rquant.runtime_deployment_bundle import (
    RuntimeSchemaV1MigrationAuthorization,
    activate_runtime_deployment_generation,
    load_current_runtime_deployment_receipt,
    load_runtime_deployment_generation_receipt,
    load_runtime_schema_rollout,
    rollback_runtime_schema_rollout,
)
from rquant.runtime_deployment_profile import (
    LINUX_PRODUCTION_RUNTIME_ROOT,
    PRODUCTION_CANVAS_SIGNER_COMMAND,
    PRODUCTION_PAGE_CONTROL_INSTANCE_ID,
    PRODUCTION_PAGE_CONTROL_SERVICE_ID,
    PRODUCTION_SHADOW_INSTANCE_ID,
    PRODUCTION_SHADOW_SERVICE_ID,
    PRODUCTION_SHADOW_SIGNER_COMMAND,
    CanvasPublicationRuntimeProfile,
    LabHighWaterRuntimeProfile,
    PageControlRuntimeProfile,
    RuntimeDeploymentProfile,
    RuntimeSchemaRolloutPolicy,
    ShadowRuntimeProfile,
    install_runtime_deployment_profile,
    load_current_runtime_deployment_profile,
    load_runtime_deployment_profile,
    preview_runtime_deployment_profile,
    resolve_profile_capabilities,
)
from rquant.runtime_schema_registry import (
    RuntimeSchemaV1LifecycleReview,
    build_runtime_schema_contract_bundle,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.schema_compatibility import RolloutPhase
from rquant.strict_json import canonical_json_bytes

COMMIT = "a" * 40
CALENDAR_SHA256 = "c" * 64


def _linux_production_daily_profile(
    *,
    daily_socket_endpoint: str = "/run/rquant/daily-receipt-signer.sock",
    daily_trusted_keyring_path: Path = Path("/etc/rquant/daily-receipt-trusted-keys.json"),
) -> RuntimeDeploymentProfile:
    root = LINUX_PRODUCTION_RUNTIME_ROOT
    daily = RuntimeServiceManifest(
        service_id="daily.pipeline.orchestrator.shadow.v1",
        service_kind=RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=60,
        stale_after_seconds=172_800,
        producer_commit=COMMIT,
        settings={
            "storage_root": str(root / "research" / "daily-pipeline"),
            "source_spool_root": str(root / "live" / "daily-close"),
            "deployment_profile_path": str(root / "current" / "deployment-profile.json"),
            "mode": "shadow",
            "service_owner": "daily.pipeline.orchestrator.shadow.v1",
            "stages": ["raw_capture"],
            "stage_commands": [],
            "receipt_active_key_id": "daily-v2",
            "receipt_active_public_key_pem": "daily-active-public-key",
            "receipt_previous_public_key_pems": {"daily-v1": "daily-previous-public-key"},
            "receipt_signer_socket_endpoint": daily_socket_endpoint,
            "receipt_trusted_keyring_path": str(daily_trusted_keyring_path),
            "receipt_signer_timeout_seconds": 5.0,
        },
    )
    return RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        runtime_mode="linux-production",
        production_runtime_root=str(root),
        manifests=(daily,),
        capability_environment={daily.service_id: ()},
        page_control=PageControlRuntimeProfile(
            endpoint="http://127.0.0.1:8767/v1/commands",
            outbox_path=root / "control" / "page-control.sqlite3",
            data_dir=root / "serving" / "page-control",
            log_dir=root / "control" / "page-control-logs",
            page_projection_canvas_catalog_root=root / "serving" / "page-control" / "canvases",
            canvas_publication=CanvasPublicationRuntimeProfile(
                active_key_id="canvas-v1",
                active_public_key_pem="canvas-public-key",
                signer_command=PRODUCTION_CANVAS_SIGNER_COMMAND,
                consumer_service_id=PRODUCTION_PAGE_CONTROL_SERVICE_ID,
                consumer_instance_id=PRODUCTION_PAGE_CONTROL_INSTANCE_ID,
            ),
        ),
        shadow=ShadowRuntimeProfile(
            completion_active_key_id="shadow-completion-v1",
            completion_active_public_key_pem="shadow-completion-public-key",
            report_active_key_id="shadow-report-v1",
            report_active_public_key_pem="shadow-report-public-key",
            signer_command=PRODUCTION_SHADOW_SIGNER_COMMAND,
            report_producer_service_id=PRODUCTION_SHADOW_SERVICE_ID,
            report_producer_instance_id=PRODUCTION_SHADOW_INSTANCE_ID,
        ),
        lab_highwater=LabHighWaterRuntimeProfile(
            authority_command=PRODUCTION_LAB_HIGHWATER_COMMAND,
            stable_identity="rquant-lab-job-center-production-v1",
            trusted_keyring_path=Path("/etc/rquant/lab-highwater-trusted-keys.json"),
            production_mode=True,
        ),
    )


def _manifest(root: Path) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="lab-jobs.serving.v1",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=30,
        stale_after_seconds=120,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(root / "research" / "lab_jobs.sqlite3"),
            "authority_root": str(root / "research" / "serving-authorities" / "lab-jobs"),
        },
    )


def _market_minute_manifest(root: Path) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="market-minute.source.v1",
        service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "spool_root": str(root / "live" / "market-minute"),
            "quota_path": str(root / "live" / "market-minute" / "quota.sqlite3"),
            "calendar_path": str(
                root / "authorities" / "market-calendar" / "generations" / f"{CALENDAR_SHA256}.json"
            ),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": CALENDAR_SHA256,
        },
    )


def _notifier_manifest(root: Path) -> RuntimeServiceManifest:
    service_id = "notifier.live.v1"
    instance = "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=RuntimeServiceKind.NOTIFIER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=30,
        producer_commit=COMMIT,
        settings={
            "signal_spool_root": str(root / "live" / "signal-bus" / "spool"),
            "notification_state_path": str(
                root / "live" / "notifications" / "notifier.live.v1.sqlite3"
            ),
            "serving_authority_root": str(
                root / "live" / "notifications" / instance / "serving-authority"
            ),
        },
    )


def _schema_rollout_profile(root: Path, *, commit: str) -> RuntimeDeploymentProfile:
    minute = _market_minute_manifest(root).model_copy(update={"producer_commit": commit})
    feature = RuntimeServiceManifest(
        service_id="feature-live.v1",
        service_kind=RuntimeServiceKind.FEATURE_LIVE,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=2,
        stale_after_seconds=30,
        producer_commit=commit,
        settings={
            "raw_spool_root": str(root / "live" / "market-minute"),
            "feature_spool_root": str(root / "live" / "features"),
            "historical_minutes_snapshot_path": str(root / "research" / "minute.parquet"),
        },
    )
    channel_id = "runtime.market_minute.batch-envelope"
    bundle = build_runtime_schema_contract_bundle(
        (minute, feature),
        producer_commit=commit,
    )
    channel = bundle.channel(channel_id)
    return RuntimeDeploymentProfile(
        producer_commit=commit,
        production_runtime_root=str(root),
        manifests=(minute, feature),
        capability_environment={
            minute.service_id: ("TUSHARE_TOKEN_MAIN",),
            feature.service_id: (),
        },
        schema_rollout_policies=(
            RuntimeSchemaRolloutPolicy(
                channel_id=channel_id,
                state_path=root / "control" / "schema-rollouts",
                stage_timeout_seconds=600,
                consumer_ack_max_age_seconds=300,
                required_consumers=tuple(
                    binding.requirement.consumer_id for binding in channel.consumers
                ),
            ),
        ),
    )


def _disable_test_credential_sealer(monkeypatch: pytest.MonkeyPatch) -> None:
    class Transaction:
        sealed_instances: tuple[str, ...] = ()

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    class Recovery:
        outcome = "none"
        transaction_id = None

    monkeypatch.setattr(
        deployment_module,
        "_seal_runtime_credentials",
        lambda _items: Transaction(),
    )
    monkeypatch.setattr(
        deployment_module,
        "_recover_runtime_credentials",
        lambda **_kwargs: Recovery(),
    )


def test_profile_identity_binds_manifests_and_capability_names(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path / "runtime")
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
    )

    assert profile.profile_id is not None
    assert len(profile.profile_id) == 64
    assert RuntimeDeploymentProfile.model_validate(profile.model_dump(mode="python")) == profile


def test_profile_identity_is_independent_of_manifest_and_mapping_order(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    lab_jobs = _manifest(runtime_root)
    market_minute = _market_minute_manifest(runtime_root)
    capabilities = {
        lab_jobs.service_id: (),
        market_minute.service_id: ("TUSHARE_TOKEN_MAIN",),
    }

    first = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(lab_jobs, market_minute),
        capability_environment=capabilities,
    )
    second = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(market_minute, lab_jobs),
        capability_environment=dict(reversed(tuple(capabilities.items()))),
    )

    assert second == first
    assert second.profile_id == first.profile_id


def test_profile_rejects_capability_names_outside_the_service_allowlist(
    tmp_path: Path,
) -> None:
    manifest = _market_minute_manifest(tmp_path / "runtime")

    with pytest.raises(ValueError, match="capability"):
        RuntimeDeploymentProfile(
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_environment={manifest.service_id: ("PASSWORD",)},
        )


def test_profile_requires_the_primary_market_data_capability(tmp_path: Path) -> None:
    manifest = _market_minute_manifest(tmp_path / "runtime")

    with pytest.raises(ValueError, match="TUSHARE_TOKEN_MAIN"):
        RuntimeDeploymentProfile(
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_environment={
                manifest.service_id: ("TUSHARE_TOKEN_BACKUP",),
            },
        )


def test_profile_requires_at_least_one_notifier_delivery_capability(tmp_path: Path) -> None:
    manifest = _notifier_manifest(tmp_path / "runtime")

    with pytest.raises(ValueError, match="PUSHDEER_KEYS or PUSHPLUS_TOKENS"):
        RuntimeDeploymentProfile(
            producer_commit=COMMIT,
            manifests=(manifest,),
            capability_environment={
                manifest.service_id: ("PUSHDEER_ENDPOINT",),
            },
        )


def test_profile_resolves_only_declared_capabilities_without_repr_leak(
    tmp_path: Path,
) -> None:
    manifest = _market_minute_manifest(tmp_path / "runtime")
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={
            manifest.service_id: (
                "TUSHARE_TOKEN_MAIN",
                "TUSHARE_TOKEN_BACKUP",
            ),
        },
    )

    resolved = resolve_profile_capabilities(
        profile,
        environ={
            "TUSHARE_TOKEN_MAIN": "primary-secret",
            "TUSHARE_TOKEN_BACKUP": "backup-secret",
            "UNDECLARED_SECRET": "must-not-be-read",
        },
    )

    assert dict(resolved[manifest.service_id]) == {
        "TUSHARE_TOKEN_BACKUP": "backup-secret",
        "TUSHARE_TOKEN_MAIN": "primary-secret",
    }
    assert "primary-secret" not in repr(resolved)
    assert "backup-secret" not in repr(resolved)
    assert "UNDECLARED_SECRET" not in repr(resolved)


def test_profile_loader_accepts_only_the_expected_canonical_commit(
    tmp_path: Path,
) -> None:
    manifest = _manifest(tmp_path / "runtime")
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
    )
    path = tmp_path / "production-runtime-profile.json"
    path.write_bytes(canonical_json_bytes(profile.model_dump(mode="json")))
    path.chmod(0o600)

    assert load_runtime_deployment_profile(path, expected_commit=COMMIT) == profile
    with pytest.raises(ValueError, match="commit"):
        load_runtime_deployment_profile(path, expected_commit="b" * 40)


def test_linux_production_profile_requires_fixed_daily_socket_authority_only() -> None:
    profile = _linux_production_daily_profile()

    daily = profile.manifests[0]
    assert daily.service_kind is RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR
    assert daily.settings["receipt_signer_socket_endpoint"] == (
        "/run/rquant/daily-receipt-signer.sock"
    )
    assert daily.settings["receipt_trusted_keyring_path"] == (
        "/etc/rquant/daily-receipt-trusted-keys.json"
    )
    assert "receipt_signer_command" not in daily.settings
    assert all(
        "helper" not in key and "argv" not in key and "env" not in key
        for key in daily.settings
    )

    with pytest.raises(ValueError, match="Daily.*socket authority"):
        _linux_production_daily_profile(daily_socket_endpoint="/tmp/daily.sock")

    with pytest.raises(ValueError, match="Daily.*keyring"):
        _linux_production_daily_profile(
            daily_trusted_keyring_path=Path("/home/lighthouse/rquant/keyring.json")
        )


@pytest.mark.parametrize("unsafe_kind", ["symlink", "hardlink", "wide_permissions"])
def test_profile_loader_rejects_unsafe_files(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    manifest = _manifest(tmp_path / "runtime")
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
    )
    source = tmp_path / "source.json"
    source.write_bytes(canonical_json_bytes(profile.model_dump(mode="json")))
    source.chmod(0o600)
    candidate = tmp_path / "production-runtime-profile.json"
    if unsafe_kind == "symlink":
        candidate.symlink_to(source)
    elif unsafe_kind == "hardlink":
        os.link(source, candidate)
    else:
        candidate.write_bytes(source.read_bytes())
        candidate.chmod(0o620)

    with pytest.raises(ValueError, match="unsafe|unavailable"):
        load_runtime_deployment_profile(candidate, expected_commit=COMMIT)


def test_profile_loader_rejects_a_symlinked_parent_directory(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    manifest = _manifest(runtime_root)
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
    )
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    path = real_parent / "profile.json"
    path.write_bytes(canonical_json_bytes(profile.model_dump(mode="json")))
    path.chmod(0o600)
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(ValueError, match="unsafe|symlink|normalized"):
        load_runtime_deployment_profile(
            linked_parent / "profile.json",
            expected_commit=COMMIT,
        )


def test_profile_install_passes_only_resolved_capabilities_to_bundle_installer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _market_minute_manifest(tmp_path / "runtime")
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={
            manifest.service_id: ("TUSHARE_TOKEN_MAIN",),
        },
    )
    observed: dict[str, object] = {}
    receipt = object()

    def installer(runtime_root: Path, **kwargs: object) -> object:
        observed.update(runtime_root=runtime_root, **kwargs)
        return receipt

    monkeypatch.setattr(
        "rquant.runtime_deployment_profile._install_runtime_deployment_bundle",
        installer,
    )

    result = install_runtime_deployment_profile(
        profile,
        runtime_root=tmp_path / "runtime",
        environ={
            "TUSHARE_TOKEN_MAIN": "primary-secret",
            "UNDECLARED_SECRET": "must-not-be-read",
        },
        schema_bootstrap_reason="first isolated runtime generation",
    )

    assert result is receipt
    assert observed["producer_commit"] == COMMIT
    assert observed["manifests"] == (manifest,)
    assert observed["deployment_profile_id"] == profile.profile_id
    assert observed["schema_bootstrap_reason"] == "first isolated runtime generation"
    capability_env = observed["capability_env"]
    assert isinstance(capability_env, dict)
    assert capability_env == {manifest.service_id: {"TUSHARE_TOKEN_MAIN": "primary-secret"}}


def test_profile_preview_and_install_pass_explicit_v1_migration_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "runtime"
    manifest = _market_minute_manifest(root)
    bundle = build_runtime_schema_contract_bundle(
        (manifest,),
        producer_commit=COMMIT,
    )
    authorization = RuntimeSchemaV1MigrationAuthorization(
        reason="reviewed legacy profile lifecycle",
        reviewed_lifecycles=tuple(
            RuntimeSchemaV1LifecycleReview(
                channel_id=channel.channel_id,
                field_name=field.name,
                introduced_in=field.introduced_in,
                deprecated_in=field.deprecated_in,
                removed_in=field.removed_in,
            )
            for channel in bundle.channels
            for field in channel.declaration.fields
        ),
        migrated_at=datetime(2026, 8, 2, 1, 0, tzinfo=UTC),
    )
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        production_runtime_root=str(root),
        manifests=(manifest,),
        capability_environment={manifest.service_id: ("TUSHARE_TOKEN_MAIN",)},
        schema_v1_migration_authority=authorization,
    )
    observed: list[object] = []

    def validator(_runtime_root: Path, **kwargs: object) -> tuple[RuntimeServiceManifest, ...]:
        observed.append(kwargs["schema_v1_migration"])
        return (manifest,)

    sentinel = object()

    def installer(_runtime_root: Path, **kwargs: object) -> object:
        observed.append(kwargs["schema_v1_migration"])
        return sentinel

    monkeypatch.setattr(
        "rquant.runtime_deployment_profile._validate_runtime_deployment_bundle",
        validator,
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_profile._install_runtime_deployment_bundle",
        installer,
    )

    preview_runtime_deployment_profile(
        profile,
        runtime_root=root,
        environ={"TUSHARE_TOKEN_MAIN": "secret"},
    )
    installed = install_runtime_deployment_profile(
        profile,
        runtime_root=root,
        environ={"TUSHARE_TOKEN_MAIN": "secret"},
    )

    assert installed is sentinel
    assert observed == [authorization, authorization]


def test_profile_preview_is_stable_redacted_and_has_no_write_side_effect(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    manifest = _market_minute_manifest(runtime_root)
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={
            manifest.service_id: ("TUSHARE_TOKEN_MAIN",),
        },
    )

    preview = preview_runtime_deployment_profile(
        profile,
        runtime_root=runtime_root,
        environ={"TUSHARE_TOKEN_MAIN": "primary-secret"},
        schema_bootstrap_reason="first isolated runtime generation",
    )

    assert preview.profile_id == profile.profile_id
    assert preview.producer_commit == COMMIT
    assert preview.runtime_root == runtime_root
    assert preview.service_ids == (manifest.service_id,)
    assert preview.capability_names == {
        manifest.service_id: ("TUSHARE_TOKEN_MAIN",),
    }
    assert "primary-secret" not in preview.model_dump_json()
    assert not runtime_root.exists()


def test_profile_preview_runs_the_same_manifest_authority_validation_as_install(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    manifest = _manifest(runtime_root).model_copy(
        update={
            "settings": {
                "lab_jobs_path": str(runtime_root / "research" / "lab_jobs.sqlite3"),
                "authority_root": str(runtime_root / "wrong-owner"),
            }
        }
    )
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
    )

    with pytest.raises(ValueError, match="authority_root"):
        preview_runtime_deployment_profile(
            profile,
            runtime_root=runtime_root,
            environ={},
            schema_bootstrap_reason="first isolated runtime generation",
        )


def test_profile_install_persists_profile_identity_in_the_immutable_generation(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    manifest = _manifest(runtime_root)
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
    )

    receipt = install_runtime_deployment_profile(
        profile,
        runtime_root=runtime_root,
        environ={},
        schema_bootstrap_reason="first isolated runtime generation",
    )

    basis = json.loads(
        (
            runtime_root / "generations" / receipt.generation_hash / "generation-basis.json"
        ).read_text()
    )
    assert receipt.deployment_profile_id == profile.profile_id
    assert basis["deployment_profile_id"] == profile.profile_id
    assert (
        runtime_root / "generations" / receipt.generation_hash / "deployment-profile.json"
    ).read_bytes() == canonical_json_bytes(profile.model_dump(mode="json"))
    assert load_current_runtime_deployment_profile(runtime_root) == profile
    assert (
        load_current_runtime_deployment_receipt(
            runtime_root,
            expected_commit=COMMIT,
            expected_profile_id=str(profile.profile_id),
        )
        == receipt
    )

    with pytest.raises(ValueError, match="profile"):
        load_current_runtime_deployment_receipt(
            runtime_root,
            expected_commit=COMMIT,
            expected_profile_id="b" * 64,
        )


def test_current_profile_loader_fails_closed_when_persisted_profile_is_missing(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    manifest = _manifest(runtime_root)
    profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(manifest,),
        capability_environment={manifest.service_id: ()},
    )
    receipt = install_runtime_deployment_profile(
        profile,
        runtime_root=runtime_root,
        environ={},
        schema_bootstrap_reason="first isolated runtime generation",
    )
    profile_path = (
        runtime_root / "generations" / receipt.generation_hash / "deployment-profile.json"
    )
    profile_path.unlink()

    with pytest.raises(ValueError, match="unavailable|profile"):
        load_current_runtime_deployment_profile(runtime_root)


def test_profile_install_receipt_captures_the_previous_generation_for_rollback(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    first_manifest = _manifest(runtime_root)
    first_profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(first_manifest,),
        capability_environment={first_manifest.service_id: ()},
    )
    first = install_runtime_deployment_profile(
        first_profile,
        runtime_root=runtime_root,
        environ={},
        schema_bootstrap_reason="first isolated runtime generation",
    )
    second_manifest = first_manifest.model_copy(update={"interval_seconds": 31})
    second_profile = RuntimeDeploymentProfile(
        producer_commit=COMMIT,
        manifests=(second_manifest,),
        capability_environment={second_manifest.service_id: ()},
    )

    second = install_runtime_deployment_profile(
        second_profile,
        runtime_root=runtime_root,
        environ={},
    )

    assert second.generation_hash != first.generation_hash
    assert second.previous_generation_hash == first.generation_hash
    assert (
        load_runtime_deployment_generation_receipt(
            runtime_root,
            generation_hash=first.generation_hash,
        )
        == first
    )

    restored = activate_runtime_deployment_generation(
        runtime_root,
        generation_hash=first.generation_hash,
        expected_commit=COMMIT,
        expected_profile_id=str(first_profile.profile_id),
    )

    assert restored.generation_hash == first.generation_hash
    assert (
        load_current_runtime_deployment_receipt(
            runtime_root,
            expected_commit=COMMIT,
            expected_profile_id=str(first_profile.profile_id),
        ).generation_hash
        == first.generation_hash
    )


def test_profile_install_retry_excludes_terminal_schema_rollout_plans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_test_credential_sealer(monkeypatch)
    root = tmp_path / "runtime"
    old_profile = _schema_rollout_profile(root, commit=COMMIT)
    new_profile = _schema_rollout_profile(root, commit="b" * 40)
    first = install_runtime_deployment_profile(
        old_profile,
        runtime_root=root,
        environ={"TUSHARE_TOKEN_MAIN": "secret"},
        schema_bootstrap_reason="reviewed profile bootstrap",
    )
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    candidate = install_runtime_deployment_profile(
        new_profile,
        runtime_root=root,
        environ={"TUSHARE_TOKEN_MAIN": "secret"},
        schema_rollout_started_at=started_at,
    )
    assert len(candidate.schema_rollout_plan_ids) == 1
    first_plan_id = candidate.schema_rollout_plan_ids[0]
    _authority, store = load_runtime_schema_rollout(root, plan_id=first_plan_id)
    state = store.get_state(first_plan_id)
    rollback_runtime_schema_rollout(
        root,
        plan_id=first_plan_id,
        expected_revision=state.revision,
        reason="simulated failed service rollout",
        now=started_at + timedelta(seconds=1),
        operation_id="profile-retry-first-rollback",
    )
    assert (root / "current").readlink() == Path("generations") / first.generation_hash

    retried = install_runtime_deployment_profile(
        new_profile,
        runtime_root=root,
        environ={"TUSHARE_TOKEN_MAIN": "secret"},
        schema_rollout_started_at=started_at + timedelta(seconds=2),
    )

    assert len(retried.schema_rollout_plan_ids) == 1
    assert retried.schema_rollout_plan_ids[0] != first_plan_id
    _authority, retried_store = load_runtime_schema_rollout(
        root,
        plan_id=retried.schema_rollout_plan_ids[0],
    )
    assert retried_store.get_state(retried.schema_rollout_plan_ids[0]).phase is RolloutPhase.PREPARE
