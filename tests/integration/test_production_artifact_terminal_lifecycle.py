from __future__ import annotations

import hashlib
import hmac
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    ArtifactRetentionWriterCredential,
)
from rquant.artifact_retention_catalog_authority import (
    initialize_retention_catalog_authority,
)
from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DatasetSnapshot,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    DatasetSnapshotFinalization,
)
from rquant.research_metadata_terminal_inbox import (
    ResearchMetadataTerminalCommand,
    ResearchMetadataTerminalCommandProcessor,
    ResearchMetadataTerminalInbox,
)
from rquant.runtime_artifact_retention import GcWorkerConfig
from rquant.runtime_artifact_terminal_lifecycle import (
    artifact_retention_state_root,
    operational_database_path,
)
from rquant.runtime_builder_retention import (
    ArtifactRetentionSettings,
    TierMigrationRuntimeConfig,
)
from rquant.runtime_capabilities import serialize_runtime_credential
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_recovery_artifacts import RealRecoveryRestorer
from rquant.runtime_recovery_backup import (
    RecoveryBackupAuthenticator,
    RecoveryBackupConfig,
    RecoveryBackupProducer,
    load_recovery_backup_generation,
)
from rquant.runtime_recovery_coordinator import RuntimeRecoveryFixedReplayVerifier
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServicePlane,
    RuntimeServiceSpec,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_service_main import (
    build_parser,
    run,
)
from rquant.strict_json import canonical_json_bytes
from tests.integration.lab_runtime_e2e_support import create_real_sealed_lab_job

# The real runtime builder owns its clock; terminal evidence must therefore be
# safely in the past for this end-to-end invocation instead of a fixture future.
NOW = datetime.now(UTC) - timedelta(minutes=5)
COMMIT = "1" * 40
GENERATION = "b" * 64
_WRITER_CREDENTIAL_CAPABILITY = "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"
_RECOVERY_KEY_ID = "retention-terminal-e2e"
_RECOVERY_SECRET = b"retention-terminal-e2e-recovery-secret"


class _RecoverySigner:
    key_id = _RECOVERY_KEY_ID

    def sign(self, payload: bytes) -> str:
        return hmac.new(_RECOVERY_SECRET, payload, hashlib.sha256).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        return hmac.compare_digest(self.sign(payload), signature)


def _credential() -> ArtifactRetentionWriterCredential:
    issued_at = datetime.now(UTC)
    return ArtifactRetentionWriterCredential(
        key_id="retention-e2e",
        sequence=1,
        secret_hex="1" * 64,
        not_before=issued_at - timedelta(minutes=1),
        expires_at=issued_at + timedelta(days=1),
    )


def _retention_settings(runtime_root: Path, state_root: Path) -> ArtifactRetentionSettings:
    managed_root = runtime_root / "research" / "final-artifacts"
    backup_root = runtime_root / "recovery-backups"
    restore_root = runtime_root / "recovery-restores"
    schema_root = runtime_root / "schema-authority"
    for path in (managed_root, backup_root, restore_root, schema_root):
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.chmod(0o700)
    payload = b"schema-bound artifact"
    authority = {
        "schema_version": 1,
        "bindings": [
            {
                "content_sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
                "schema_sha256": "5" * 64,
            }
        ],
    }
    authority["authority_id"] = canonical_sha256(authority)
    authority_payload = canonical_json_bytes(authority)
    authority_path = schema_root / "descriptor-schema-authority.json"
    authority_path.write_bytes(authority_payload)
    authority_path.chmod(0o600)
    return ArtifactRetentionSettings(
        managed_root=managed_root,
        state_root=state_root,
        reference_store_path=state_root / "references.sqlite3",
        recovery_publication_root=backup_root,
        recovery_restore_root=restore_root,
        recovery_target_manifest_id="4" * 64,
        recovery_profile_generation="1" * 64,
        full_recovery_receipt_id="3" * 64,
        max_recovery_age=timedelta(days=30),
        schema_authority_root=schema_root,
        schema_authority_path=authority_path,
        schema_authority_sha256=hashlib.sha256(authority_payload).hexdigest(),
        migration=TierMigrationRuntimeConfig(
            warm_root=managed_root / "warm",
            cold_root=managed_root / "cold",
            warm_failure_domain="warm-volume",
            cold_failure_domain="cold-volume",
            batch_items=1,
            batch_bytes=1024,
            max_runtime=timedelta(seconds=1),
            query_page_items=2,
        ),
        max_bundle_items=16,
        max_bundle_bytes=1024**3,
        retention_policy={
            "hot_min_age": timedelta(days=7),
            "warm_min_age": timedelta(days=30),
            "cold_min_age": timedelta(days=90),
            "minimum_verified_copies": 1,
            "verification_max_age": timedelta(days=1),
            "plan_ttl": timedelta(hours=1),
            "claim_ttl": timedelta(minutes=10),
        },
        worker=GcWorkerConfig(
            batch_items=16,
            batch_bytes=1024**3,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=3,
            retry_delay=timedelta(seconds=10),
        ),
    )


def _prepare_verified_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, str, str, Path, str]:
    """Build the real backup and full-restore evidence required by the GC gate."""

    from tests.unit.test_runtime_recovery_backup import _config

    source_root = tmp_path / "recovery-source"
    source_root.mkdir(mode=0o700)
    base_config = _config(source_root)
    payload = base_config.model_dump(mode="python", exclude={"config_id"})
    payload["signer_key_id"] = _RECOVERY_KEY_ID
    config = RecoveryBackupConfig.model_validate(payload)
    signer = _RecoverySigner()
    producer = RecoveryBackupProducer(config=config, signer=signer)
    producer.execute(expected_plan_id=producer.preview().plan_id)

    credentials_root = tmp_path / "recovery-credentials"
    credentials_root.mkdir(mode=0o700)
    credentials_root.chmod(0o700)
    credential_path = credentials_root / "retention-terminal-e2e.json"
    credential_path.write_bytes(
        canonical_json_bytes({"key_id": _RECOVERY_KEY_ID, "secret_hex": _RECOVERY_SECRET.hex()})
    )
    credential_path.chmod(0o600)
    monkeypatch.setenv("RQUANT_RECOVERY_TRUSTED_CREDENTIAL_FILES", str(credential_path))

    authenticator = RecoveryBackupAuthenticator(
        key_id=_RECOVERY_KEY_ID,
        secret=_RECOVERY_SECRET,
    )
    pointer, _backup, target, tool, expectations = load_recovery_backup_generation(
        Path(config.publication_root),
        trusted_verifiers={_RECOVERY_KEY_ID: authenticator},
    )
    restore_root = tmp_path / "retention-recovery-restores"
    restore_root.mkdir(mode=0o700)
    restore_root.chmod(0o700)
    restored = RealRecoveryRestorer(
        backup_root=Path(config.publication_root) / pointer.generation_path,
        restore_root=restore_root,
        signature_verifier=authenticator,
        fixed_replay_verifier=RuntimeRecoveryFixedReplayVerifier(
            expectations=expectations.expectations
        ),
        max_artifacts=64,
        max_total_bytes=256 * 1024 * 1024,
        deadline_seconds=60,
    ).restore(target=target, tool_bundle=tool)
    assert restored.receipt_id is not None
    return (
        Path(config.publication_root),
        target.manifest_id,
        target.target_profile_generation,
        restore_root,
        restored.receipt_id,
    )


def _write_current_manifests(
    runtime_root: Path,
    *,
    manifests: tuple[RuntimeServiceManifest, ...],
) -> dict[RuntimeServiceKind, Path]:
    manifest_directory = runtime_root / "generations" / GENERATION / "manifests"
    manifest_directory.mkdir(parents=True, mode=0o700)
    paths: dict[RuntimeServiceKind, Path] = {}
    for manifest in manifests:
        instance = "svc-" + hashlib.sha256(manifest.service_id.encode("utf-8")).hexdigest()
        path = manifest_directory / f"{instance}.json"
        path.write_text(manifest.model_dump_json(), encoding="utf-8")
        path.chmod(0o600)
        paths[manifest.service_kind] = runtime_root / "current" / "manifests" / path.name
    (runtime_root / "current").symlink_to(
        Path("generations") / GENERATION,
        target_is_directory=True,
    )
    return paths


def _write_current_manifest(
    runtime_root: Path,
    *,
    manifest: RuntimeServiceManifest,
) -> Path:
    return _write_current_manifests(runtime_root, manifests=(manifest,))[manifest.service_kind]


def _submit_terminal_metadata(
    *,
    runtime_root: Path,
    research_root: Path,
) -> tuple[str, str, str, Path]:
    """Feed the real metadata authority through its typed durable inbox."""

    audit = DataAuditRun.create(
        as_of_date=date(2026, 8, 2),
        range_start=date(2026, 7, 1),
        range_end=date(2026, 8, 2),
        rule_set_version="terminal-owner-process-e2e/v2",
        observed_at=NOW - timedelta(minutes=6),
    )
    snapshot = DatasetSnapshot.create(
        strategy_name="n_shape",
        as_of_time=NOW - timedelta(days=1),
        code_commit="1" * 40,
        origin="production",
        created_at=NOW - timedelta(minutes=5),
    )
    binding = DatasetSnapshotBinding.create(
        manifest=DatasetSnapshotBindingManifest(
            snapshot_id=snapshot.snapshot_id,
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 6, 30),
            as_of_time=snapshot.as_of_time,
            code_commit=snapshot.code_commit,
            dependency_contract_version="stage1-v1",
            builder_version="terminal-owner-process-e2e/v2",
            artifacts=(
                DatasetSnapshotArtifact(
                    artifact_type="materialized_table",
                    dataset_id="daily_bar",
                    table_name="daily_bar",
                    artifact_key="daily_bar:2026-04-01:2026-06-30",
                    relative_path="tables/daily_bar.parquet",
                    row_count=1,
                    schema_hash="7" * 64,
                    content_hash="8" * 64,
                    file_hash="9" * 64,
                ),
            ),
        ),
        artifact_root="/srv/rquant/research-lake",
        manifest_relative_path="snapshots/runtime-terminal-e2e.json",
        created_at=NOW - timedelta(minutes=4),
    )
    operational_path = operational_database_path(runtime_root)
    operational_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    operational_path.parent.chmod(0o700)
    inbox = ResearchMetadataTerminalInbox(research_root / "metadata-terminal-inbox")
    assert inbox.submit(
        ResearchMetadataTerminalCommand(
            kind="audit_completed",
            submitted_at=NOW - timedelta(minutes=1),
            audit_run=audit,
            audit_finalization=DataAuditRunFinalization(
                p0_count=0,
                completed_at=NOW - timedelta(minutes=2),
            ),
        )
    )
    assert inbox.submit(
        ResearchMetadataTerminalCommand(
            kind="snapshot_ready",
            submitted_at=NOW - timedelta(minutes=1),
            snapshot=snapshot,
            snapshot_finalization=DatasetSnapshotFinalization(
                completed_at=NOW - timedelta(minutes=2),
            ),
            snapshot_binding=binding,
            snapshot_binding_finalization=DatasetSnapshotBindingFinalization(
                completed_at=NOW - timedelta(minutes=1),
            ),
        )
    )
    assert (
        ResearchMetadataTerminalCommandProcessor(
            inbox=inbox,
            database_path=operational_path,
        ).run_once()
        == 2
    )
    assert (
        ResearchMetadataTerminalCommandProcessor(
            inbox=ResearchMetadataTerminalInbox(research_root / "metadata-terminal-inbox"),
            database_path=operational_path,
        ).run_once()
        == 0
    )
    dataset_authority_path = research_root / "research_ro.duckdb"
    shutil.copyfile(operational_path, dataset_authority_path)
    dataset_authority_path.chmod(0o600)
    return audit.audit_run_id, snapshot.snapshot_id, binding.binding_hash, dataset_authority_path


def test_runtime_main_applies_catalog_ipc_and_terminal_hooks_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real authorities and the retention builder close one restart-safe IPC loop.

    This intentionally does not seed ``references.sqlite3`` or replace the
    lifecycle factory.  The catalog-facing capability only stages an immutable
    request; the real retention runtime owns every metadata mutation and all
    terminal releases.
    """

    assert not any(tmp_path.iterdir())
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir(mode=0o700)
    runtime_root.chmod(0o700)
    research_root = runtime_root / "research"
    research_root.mkdir(parents=True, mode=0o700)
    research_root.chmod(0o700)
    state_root = artifact_retention_state_root(runtime_root)
    (
        recovery_publication_root,
        recovery_target_manifest_id,
        recovery_profile_generation,
        recovery_restore_root,
        full_recovery_receipt_id,
    ) = _prepare_verified_recovery(tmp_path, monkeypatch)
    settings = _retention_settings(runtime_root, state_root).model_copy(
        update={
            "recovery_publication_root": recovery_publication_root,
            "recovery_target_manifest_id": recovery_target_manifest_id,
            "recovery_profile_generation": recovery_profile_generation,
            "recovery_restore_root": recovery_restore_root,
            "full_recovery_receipt_id": full_recovery_receipt_id,
            "catalog_authority_root": state_root / "catalog-authority",
        }
    )
    retention_authority = initialize_retention_catalog_authority(
        state_root=state_root,
        reference_store_path=settings.reference_store_path,
        producer_commit=COMMIT,
    )
    audit_run_id, snapshot_id, snapshot_binding_hash, dataset_authority_path = (
        _submit_terminal_metadata(runtime_root=runtime_root, research_root=research_root)
    )
    artifact_root = tmp_path / "artifacts"
    experiment = create_real_sealed_lab_job(
        tmp_path=runtime_root,
        research_root=research_root,
        artifact_root=artifact_root,
        snapshot_id=snapshot_id,
        snapshot_binding_hash=snapshot_binding_hash,
        audit_run_id=audit_run_id,
        catalog_authority_root=retention_authority.root,
        catalog_authority_receipt_path=retention_authority.current_receipt_path,
        code_sha=COMMIT,
        now=NOW,
    )
    lab_jobs_path = research_root / "lab_jobs.sqlite3"
    experiment_registry_path = research_root / "experiment_registry.sqlite3"
    serving_authorities_root = research_root / "serving-authorities"
    serving_authorities_root.mkdir(mode=0o700)
    serving_authorities_root.chmod(0o700)
    catalog_state_root = research_root / "artifact-catalogs" / "catalog-e2e"
    catalog_state_root.mkdir(parents=True, mode=0o700)
    catalog_state_root.chmod(0o700)

    manifest = RuntimeServiceManifest(
        service_id="artifact-retention.primary.v1",
        service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=0,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings=settings.model_dump(mode="json"),
    )
    lab_manifest = RuntimeServiceManifest(
        service_id="lab-jobs.serving.v1",
        service_kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=0,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "lab_jobs_path": str(lab_jobs_path),
            "authority_root": str(research_root / "serving-authorities" / "lab-jobs"),
        },
    )
    promotion_manifest = RuntimeServiceManifest(
        service_id="promotions.serving.v1",
        service_kind=RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=0,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "experiment_registry_path": str(experiment_registry_path),
            "experiment_registry_managed_trust_root": str(research_root),
            "authority_root": str(research_root / "serving-authorities" / "promotions"),
        },
    )
    catalog_manifest = RuntimeServiceManifest(
        service_id="artifact-catalog.primary.v1",
        service_kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=0,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "research_root": str(research_root),
            "artifact_root": str(artifact_root),
            "state_root": str(catalog_state_root),
            "lab_jobs_path": str(lab_jobs_path),
            "dataset_authority_path": str(dataset_authority_path),
            "experiment_registry_path": str(experiment_registry_path),
            "location_id": "e2e-primary",
            "failure_domain": "e2e-local",
        },
    )
    manifest_paths = _write_current_manifests(
        runtime_root,
        manifests=(lab_manifest, catalog_manifest, promotion_manifest, manifest),
    )
    manifest_path = manifest_paths[RuntimeServiceKind.ARTIFACT_RETENTION]

    instance = manifest_path.stem
    writer_credential = _credential()
    credential_directory = tmp_path / "credentials"
    credential_directory.mkdir(mode=0o700)
    credential_directory.chmod(0o700)
    credential_directory.joinpath("capabilities.json").write_bytes(
        serialize_runtime_credential(
            service_id=manifest.service_id,
            service_kind=manifest.service_kind,
            instance_name=instance,
            bundle_generation=GENERATION,
            values={
                _WRITER_CREDENTIAL_CAPABILITY: canonical_json_bytes(
                    writer_credential.model_dump(mode="json")
                ).decode("utf-8")
            },
        )
    )
    credential_directory.joinpath("capabilities.json").chmod(0o600)
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(credential_directory))
    monkeypatch.setattr("rquant.runtime_service_main.resolve_checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(
        "rquant.runtime_service_main.load_runtime_schema_service_bindings",
        lambda *_args, **_kwargs: (),
    )

    def run_once(service_manifest: RuntimeServiceManifest) -> None:
        service_path = manifest_paths[service_manifest.service_kind]
        service_instance = service_path.stem
        control_root = (
            runtime_root
            / "control"
            / service_manifest.service_kind.value.replace("_", "-")
            / service_instance
        )
        service_credentials = tmp_path / f"credentials-{service_manifest.service_kind.value}"
        service_credentials.mkdir(mode=0o700, exist_ok=True)
        service_credentials.chmod(0o700)
        values = (
            {
                _WRITER_CREDENTIAL_CAPABILITY: canonical_json_bytes(
                    writer_credential.model_dump(mode="json")
                ).decode("utf-8")
            }
            if service_manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION
            else {}
        )
        service_credentials.joinpath("capabilities.json").write_bytes(
            serialize_runtime_credential(
                service_id=service_manifest.service_id,
                service_kind=service_manifest.service_kind,
                instance_name=service_instance,
                bundle_generation=GENERATION,
                values=values,
            )
        )
        service_credentials.joinpath("capabilities.json").chmod(0o600)
        monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(service_credentials))
        args = build_parser().parse_args(
            [
                "--manifest",
                str(service_path),
                "--control-root",
                str(control_root),
                "--expected-commit",
                COMMIT,
                "--expected-generation",
                GENERATION,
                "--expected-kind",
                service_manifest.service_kind.value,
                "--once",
            ]
        )
        assert run(args) == 0
        heartbeat = RuntimeServiceControl.read_heartbeat(
            control_root,
            service_manifest.service_spec,
        )
        assert heartbeat is not None
        assert heartbeat.total_failures == 0
        assert heartbeat.last_error is None
        assert heartbeat.total_successes == 1

    run_once(lab_manifest)
    run_once(lab_manifest)
    run_once(catalog_manifest)
    catalog_outbox_root = state_root / "catalog-registration-outbox"
    assert len(tuple((catalog_outbox_root / "queued").glob("*.json"))) == 1
    run_once(catalog_manifest)
    run_once(promotion_manifest)
    run_once(promotion_manifest)

    run_once(manifest)
    run_once(manifest)

    retention = ArtifactReferenceStore(
        settings.reference_store_path,
        managed_trust_root=settings.state_root,
        writer_owner="artifact-retention",
        retention_writer_credential=writer_credential,
    )
    try:
        pending = retention.pending_owner_terminal_releases(limit=10)
        assert pending == (), [receipt.model_dump(mode="json") for receipt in pending]
        for owner_type, owner_id in (
            ("audit", audit_run_id),
            ("experiment", experiment.experiment_id),
            ("job", "11111111-1111-4111-8111-111111111111"),
            ("snapshot", snapshot_id),
        ):
            assert (
                retention.list_active_owner_references(
                    owner_type=owner_type,
                    owner_id=str(owner_id),
                )
                == ()
            )
        assert not tuple((catalog_outbox_root / "queued").glob("*.json"))
        assert not tuple((catalog_outbox_root / "claimed").glob("*.json"))
        assert len(tuple((catalog_outbox_root / "completed").glob("*.json"))) == 1
        events = retention.list_audit_events()
        assert sum(event.event_type == "owner_terminal_released" for event in events) == 4
        assert sum(event.event_type == "owner_terminal_release_published" for event in events) == 4
    finally:
        retention.close()

    heartbeat = RuntimeServiceControl.read_heartbeat(
        runtime_root / "control" / "artifact-retention" / instance,
        RuntimeServiceSpec(
            service_id=manifest.service_id,
            plane=RuntimeServicePlane.RESEARCH,
            stale_after=timedelta(seconds=60),
            producer_commit=COMMIT,
        ),
    )
    assert heartbeat is not None
    assert heartbeat.total_successes == 1
