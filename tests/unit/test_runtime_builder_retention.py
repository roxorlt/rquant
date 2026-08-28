from __future__ import annotations

import hashlib
import multiprocessing
import queue
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.artifact_catalog_registration_outbox import ArtifactCatalogRegistrationOutbox
from rquant.artifact_retention import (
    ArtifactRetentionWriterCredential,
    ObjectCopy,
    ObjectIdentity,
    ObjectReference,
    OwnerTerminalReleaseReceipt,
    RetentionPolicy,
    StorageTier,
)
from rquant.artifact_terminal_owners import (
    ArtifactTerminalLifecycleHooks,
    AuditTerminalReceiptProducer,
)
from rquant.data_metadata import DataAuditRun, DataAuditRunFinalization
from rquant.runtime_artifact_retention import (
    ArtifactGcHealthProjector,
    ArtifactGcHealthSummary,
    ExactFullVerifiedRecoveryDeletionGate,
    FullVerifiedDeletionAuthorization,
    GcWorkerConfig,
)
from rquant.runtime_builder_retention import (
    ArtifactGcHealthAuthorityAdapter,
    ArtifactRetentionSettings,
    TierMigrationRuntimeConfig,
    artifact_retention_builder,
    build_artifact_retention_runtime,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_health_authority import (
    RuntimeHealthControlSource,
    RuntimeHealthSourceReader,
)
from rquant.runtime_service_builtin import build_builtin_registry
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeServiceStatus,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.storage.duckdb import DuckDBStore
from rquant.strict_json import canonical_json_bytes

NOW = datetime(2026, 7, 31, 8, 0, tzinfo=UTC)
_WRITER_CREDENTIAL_CAPABILITY = "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"


def _writer_credential(
    *,
    secret_hex: str = "1" * 64,
    key_id: str = "retention-writer-service",
    sequence: int = 1,
    not_before: datetime = NOW - timedelta(minutes=1),
    expires_at: datetime = NOW + timedelta(hours=1),
    revoked_at: datetime | None = None,
    previous_secret_hex: str | None = None,
) -> ArtifactRetentionWriterCredential:
    return ArtifactRetentionWriterCredential(
        key_id=key_id,
        sequence=sequence,
        secret_hex=secret_hex,
        previous_secret_hex=previous_secret_hex,
        not_before=not_before,
        expires_at=expires_at,
        revoked_at=revoked_at,
    )


def _writer_capability_env(
    credential: ArtifactRetentionWriterCredential,
) -> dict[str, str]:
    return {
        _WRITER_CREDENTIAL_CAPABILITY: canonical_json_bytes(
            credential.model_dump(mode="json"),
        ).decode("utf-8")
    }


def _retention_manifest(settings: ArtifactRetentionSettings) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="artifact-retention.primary.v1",
        service_kind=RuntimeServiceKind.ARTIFACT_RETENTION,
        plane=RuntimeServicePlane.RESEARCH,
        interval_seconds=300,
        stale_after_seconds=900,
        producer_commit="a" * 40,
        settings=settings.model_dump(mode="json"),
    )


def _terminal_lifecycle(settings: ArtifactRetentionSettings):
    class Lifecycle:
        reference_store = type("ReferenceStore", (), {"path": settings.reference_store_path})()
        catalog_registration_outbox = ArtifactCatalogRegistrationOutbox(
            settings.state_root / "catalog-registration-outbox"
        )
        hooks = object()

        def close(self) -> None:
            return None

    return Lifecycle()


class FullGate:
    def authorize(self, candidate: object, *, as_of: datetime) -> FullVerifiedDeletionAuthorization:
        del candidate
        return FullVerifiedDeletionAuthorization(
            profile="current",
            profile_generation="1" * 64,
            generation_id="2" * 64,
            receipt_id="3" * 64,
            verification_level="full_verified",
            verified_at=as_of - timedelta(seconds=1),
            recovery_completed_at=as_of - timedelta(seconds=1),
            current_published_at=as_of - timedelta(seconds=2),
            expires_at=as_of + timedelta(days=30),
        )


def _settings(tmp_path: Path) -> ArtifactRetentionSettings:
    managed_root = tmp_path / "artifacts"
    state_root = tmp_path / "state"
    backup_root = tmp_path / "recovery-backups"
    restore_root = tmp_path / "recovery-restores"
    schema_root = tmp_path / "schema-authority"
    for root in (managed_root, state_root, backup_root, restore_root, schema_root):
        root.mkdir(mode=0o700)
    binding = {
        "content_sha256": hashlib.sha256(b"trusted artifact").hexdigest(),
        "size_bytes": len(b"trusted artifact"),
        "schema_sha256": "5" * 64,
    }
    authority = {
        "schema_version": 1,
        "bindings": [binding],
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
        retention_policy=RetentionPolicy(
            hot_min_age=timedelta(days=7),
            warm_min_age=timedelta(days=30),
            cold_min_age=timedelta(days=90),
            minimum_verified_copies=1,
            verification_max_age=timedelta(days=1),
            plan_ttl=timedelta(hours=1),
            claim_ttl=timedelta(minutes=10),
        ),
        worker=GcWorkerConfig(
            batch_items=16,
            batch_bytes=1024**3,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=3,
            retry_delay=timedelta(seconds=10),
        ),
    )


def _bind_schema_payloads(
    settings: ArtifactRetentionSettings,
    payloads: tuple[bytes, ...],
) -> ArtifactRetentionSettings:
    authority: dict[str, object] = {
        "schema_version": 1,
        "bindings": [
            {
                "content_sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
                "schema_sha256": "5" * 64,
            }
            for payload in payloads
        ],
    }
    authority["authority_id"] = canonical_sha256(authority)
    payload = canonical_json_bytes(authority)
    settings.schema_authority_path.write_bytes(payload)
    settings.schema_authority_path.chmod(0o600)
    return settings.model_copy(
        update={"schema_authority_sha256": hashlib.sha256(payload).hexdigest()}
    )


def _run_retention_builder_with_capability_in_process(
    settings_json: str,
    manifest_json: str,
    capability: str,
    sink: object,
) -> None:
    import json as _json

    from rquant.artifact_retention import (
        ArtifactReferenceStore,
        ArtifactRetentionWriterCredential,
    )
    from rquant.runtime_builder_retention import ArtifactRetentionSettings
    from rquant.runtime_service_entrypoint import RuntimeServiceManifest

    try:
        settings = ArtifactRetentionSettings.model_validate(
            _json.loads(settings_json),
        )
        RuntimeServiceManifest.model_validate(_json.loads(manifest_json))
        credential = ArtifactRetentionWriterCredential.model_validate_json(capability)
        ArtifactReferenceStore(
            settings.reference_store_path,
            managed_trust_root=settings.state_root,
            writer_owner="artifact-retention",
            retention_writer_credential=credential,
            clock=lambda: NOW,
        )
        sink.put(("written", ""))
    except Exception as exc:
        sink.put((type(exc).__name__, str(exc)))


def test_retention_runtime_builder_uses_independent_state_and_exact_gate_boundary(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runtime = build_artifact_retention_runtime(
        settings=settings,
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
    )

    assert runtime.reference_store.path == settings.reference_store_path
    assert runtime.gc_state.path == settings.state_root / "gc-runtime.sqlite3"
    assert runtime.gc_state.path != runtime.reference_store.path
    assert runtime.worker.deletion_gate.__class__ is FullGate
    assert runtime.migration.transport is runtime.transport
    artifact = settings.managed_root / "trusted.bin"
    artifact.write_bytes(b"trusted artifact")
    artifact.chmod(0o600)
    assert runtime.transport.verify(artifact.as_uri()).schema_sha256 == "5" * 64

    result = runtime.run_step()
    assert result.processed_count == 0
    assert result.backlog_count == 0
    assert result.source_generations.keys() == {
        "artifact_gc",
        "artifact_gc_health",
        "artifact_tier_migration",
        "terminal_release_outbox",
        "artifact_catalog_registration_outbox",
    }


@pytest.mark.parametrize("capabilities", ({}, None))
def test_retention_builder_fails_closed_without_writer_capability(
    tmp_path: Path,
    capabilities: dict[str, str] | None,
) -> None:
    settings = _settings(tmp_path)
    manifest = _retention_manifest(settings)
    with pytest.raises(ValueError, match="retention writer credential"):
        artifact_retention_builder(
            clock=lambda: NOW,
            deletion_gate_factory=lambda _settings: FullGate(),
            capability_environment=capabilities,
        )(manifest)


def test_retention_builder_fails_closed_with_invalid_writer_capability_payload(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    manifest = _retention_manifest(settings)
    with pytest.raises(ValueError, match="invalid"):
        artifact_retention_builder(
            clock=lambda: NOW,
            deletion_gate_factory=lambda _settings: FullGate(),
            capability_environment={
                _WRITER_CREDENTIAL_CAPABILITY: "{not-json",
            },
        )(manifest)


def test_retention_builder_fails_closed_for_expired_writer_capability(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manifest = _retention_manifest(settings)
    expired_credential = _writer_credential(
        not_before=NOW - timedelta(hours=1),
        expires_at=NOW,
    )
    with pytest.raises(ValueError, match="expired"):
        artifact_retention_builder(
            clock=lambda: NOW,
            deletion_gate_factory=lambda _settings: FullGate(),
            capability_environment=_writer_capability_env(expired_credential),
            open_artifact_terminal_lifecycle=lambda: _terminal_lifecycle(settings),
        )(manifest)


def test_retention_builder_fails_closed_for_revoked_writer_capability(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manifest = _retention_manifest(settings)
    revoked_credential = _writer_credential(
        revoked_at=NOW - timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="revoked"):
        artifact_retention_builder(
            clock=lambda: NOW,
            deletion_gate_factory=lambda _settings: FullGate(),
            capability_environment=_writer_capability_env(revoked_credential),
            open_artifact_terminal_lifecycle=lambda: _terminal_lifecycle(settings),
        )(manifest)


def test_retention_builder_rejects_old_writer_credential_after_rotation(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manifest = _retention_manifest(settings)
    current = _writer_credential(secret_hex="1" * 64, sequence=1)
    rotated = _writer_credential(
        secret_hex="2" * 64,
        sequence=2,
        previous_secret_hex="1" * 64,
    )

    artifact_retention_builder(
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
        capability_environment=_writer_capability_env(current),
        open_artifact_terminal_lifecycle=lambda: _terminal_lifecycle(settings),
    )(manifest)
    artifact_retention_builder(
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
        capability_environment=_writer_capability_env(rotated),
        open_artifact_terminal_lifecycle=lambda: _terminal_lifecycle(settings),
    )(manifest)
    with pytest.raises(ValueError, match="old|superseded|rotation"):
        artifact_retention_builder(
            clock=lambda: NOW,
            deletion_gate_factory=lambda _settings: FullGate(),
            capability_environment=_writer_capability_env(current),
            open_artifact_terminal_lifecycle=lambda: _terminal_lifecycle(settings),
        )(manifest)


def test_retention_builder_fails_closed_in_race_when_processes_use_uncontrolled_credential(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    manifest = _retention_manifest(settings)
    bootstrap = _writer_credential(secret_hex="1" * 64, sequence=1)
    controlled = _writer_credential(
        secret_hex="2" * 64,
        sequence=2,
        previous_secret_hex="1" * 64,
    )
    uncontrolled = _writer_credential(
        secret_hex="3" * 64,
        sequence=2,
        previous_secret_hex="0" * 64,
    )

    artifact_retention_builder(
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
        capability_environment=_writer_capability_env(bootstrap),
        open_artifact_terminal_lifecycle=lambda: _terminal_lifecycle(settings),
    )(manifest)

    context = multiprocessing.get_context("spawn")
    settings_payload = canonical_json_bytes(
        settings.model_dump(mode="json"),
    ).decode("utf-8")
    manifest_payload = canonical_json_bytes(
        manifest.model_dump(mode="json"),
    ).decode("utf-8")
    result = context.Queue()
    contenders = (
        context.Process(
            target=_run_retention_builder_with_capability_in_process,
            args=(
                settings_payload,
                manifest_payload,
                _writer_capability_env(controlled)[_WRITER_CREDENTIAL_CAPABILITY],
                result,
            ),
        ),
        context.Process(
            target=_run_retention_builder_with_capability_in_process,
            args=(
                settings_payload,
                manifest_payload,
                _writer_capability_env(uncontrolled)[_WRITER_CREDENTIAL_CAPABILITY],
                result,
            ),
        ),
    )

    for contender in contenders:
        contender.start()
    for contender in contenders:
        contender.join(timeout=10)
        assert contender.exitcode == 0

    try:
        outcomes = sorted(
            [result.get(timeout=1) for _ in contenders],
            key=lambda item: item[0] != "written",
        )
    except queue.Empty as exc:
        raise AssertionError(
            "retention builder race test did not collect all process results"
        ) from exc
    names = [entry[0] for entry in outcomes]
    assert names.count("written") == 1, outcomes
    failure_message = next(message for name, message in outcomes if name != "written")
    assert "retention writer" in failure_message.lower()


def test_real_audit_terminal_event_reaches_retention_run_step_via_outbox(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runtime = build_artifact_retention_runtime(
        settings=settings,
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
    )
    audit = DataAuditRun.create(
        as_of_date=date(2026, 7, 31),
        range_start=date(2026, 7, 1),
        range_end=date(2026, 7, 31),
        rule_set_version="retention-terminal-hook/v1",
        observed_at=NOW - timedelta(minutes=2),
    )
    content_sha256 = hashlib.sha256(b"audit terminal artifact").hexdigest()
    runtime.reference_store.register_object(
        ObjectIdentity(
            content_sha256=content_sha256,
            size_bytes=1,
            object_kind="audit-result",
            created_at=NOW - timedelta(minutes=2),
        )
    )
    runtime.reference_store.register_reference(
        ObjectReference(
            owner_type="audit",
            owner_id=audit.audit_run_id,
            content_sha256=content_sha256,
            created_at=NOW - timedelta(minutes=1),
        )
    )
    hooks: dict[str, ArtifactTerminalLifecycleHooks] = {}
    duck = DuckDBStore(
        tmp_path / "audit-authority.duckdb",
        artifact_terminal_hook=lambda owner_type, owner_id, observed_at: hooks["value"](
            owner_type,
            owner_id,
            observed_at,
        ),
    )
    hooks["value"] = ArtifactTerminalLifecycleHooks(
        reference_store=runtime.reference_store,
        producers={"audit": AuditTerminalReceiptProducer(runtime.reference_store, duck)},
    )

    duck.begin_data_audit_run(audit)
    duck.finalize_data_audit_run(
        audit.audit_run_id,
        DataAuditRunFinalization(p0_count=0, completed_at=NOW),
    )
    assert len(runtime.reference_store.pending_owner_terminal_releases(limit=10)) == 1

    result = runtime.run_step()

    assert result.processed_count == 1
    assert (
        runtime.reference_store.list_active_owner_references(
            owner_type="audit",
            owner_id=audit.audit_run_id,
        )
        == ()
    )
    duck.close()


def test_real_terminal_to_run_step_recovery_gc_and_health_chain(tmp_path: Path) -> None:
    """The terminal authority event, recovery gate, GC, and health share durable state."""

    from tests.unit.test_runtime_recovery_artifacts import _build_bundle, _restorer

    recovery_fixture = tmp_path / "real-recovery"
    recovery_fixture.mkdir(mode=0o700)
    source, target, tool, replay = _build_bundle(recovery_fixture, formal_replay=True)
    restore_root = recovery_fixture / "restore"
    restore_root.mkdir(mode=0o700)
    receipt = _restorer(source=source, target=restore_root, replay=replay).restore(
        target=target,
        tool_bundle=tool,
    )
    assert receipt.receipt_id is not None
    gate = ExactFullVerifiedRecoveryDeletionGate(
        restore_root=restore_root,
        receipt_id=receipt.receipt_id,
        target=target,
        fixed_replay_verifier=replay,
        max_recovery_age=timedelta(days=30),
    )
    current_now = [receipt.completed_at + timedelta(seconds=1)]
    settings = _settings(tmp_path)
    settings = settings.model_copy(
        update={
            "retention_policy": settings.retention_policy.model_copy(
                update={
                    "hot_min_age": timedelta(0),
                    "warm_min_age": timedelta(0),
                    "cold_min_age": timedelta(0),
                }
            )
        }
    )
    settings.migration.warm_root.mkdir(mode=0o700)
    settings.migration.cold_root.mkdir(mode=0o700)
    # 这条用例走真实的 full-verified 恢复闸门（每次 authorize 都重跑一遍固定回放校验），
    # 单个 run_step 里要花掉几秒真实时间。GC worker / 迁移 / 健康投影的时间预算默认挂在
    # time.monotonic 上，机器一慢就会在 _process 中途撞 deadline，把工作项打成 retry
    # （表现为 processed_count 少 1 且 degraded_reasons=('artifact_gc:degraded',)）。
    # 这里把 monotonic 也绑到注入时钟上，让预算只随 current_now 前进，用例断言的是
    # 「终态权威事件 → 恢复闸门 → GC → 健康」这条持久化链路，与真实耗时无关。
    # 真实预算到期的行为另有确定性覆盖：tests/unit/test_runtime_artifact_retention.py 的
    # test_deadline_reached_* 三条用例。
    def virtual_monotonic() -> float:
        return current_now[0].timestamp()

    runtime = build_artifact_retention_runtime(
        settings=settings,
        clock=lambda: current_now[0],
        monotonic=virtual_monotonic,
        deletion_gate_factory=lambda _settings: gate,
    )
    payload = b"trusted artifact"
    hot = settings.managed_root / "terminal-chain.bin"
    hot.write_bytes(payload)
    hot.chmod(0o600)
    content_sha256 = hashlib.sha256(payload).hexdigest()
    runtime.reference_store.register_object(
        ObjectIdentity(
            content_sha256=content_sha256,
            size_bytes=len(payload),
            object_kind="audit-result",
            created_at=NOW - timedelta(days=100),
        )
    )
    runtime.reference_store.register_copy(
        ObjectCopy(
            content_sha256=content_sha256,
            location_id="hot-primary",
            storage_uri=hot.as_uri(),
            storage_tier=StorageTier.HOT,
            verified_at=current_now[0],
            failure_domain="hot-volume",
            tier_entered_at=NOW - timedelta(days=100),
        )
    )
    audit = DataAuditRun.create(
        as_of_date=date(2026, 7, 31),
        range_start=date(2026, 7, 1),
        range_end=date(2026, 7, 31),
        rule_set_version="terminal-recovery-gc/v1",
        observed_at=NOW - timedelta(minutes=2),
    )
    owner_ids = {
        "audit": audit.audit_run_id,
        "experiment": "e" * 64,
        "job": "11111111-1111-4111-8111-111111111111",
        "snapshot": "s" * 64,
    }
    references: dict[str, ObjectReference] = {}
    for owner_type, owner_id in owner_ids.items():
        reference = ObjectReference(
            owner_type=owner_type,
            owner_id=owner_id,
            content_sha256=content_sha256,
            created_at=NOW - timedelta(minutes=1),
        )
        runtime.reference_store.register_reference(reference)
        references[owner_type] = reference
    for owner_type in ("experiment", "job", "snapshot"):
        reference = references[owner_type]
        runtime.reference_store.release_owner_terminal(
            OwnerTerminalReleaseReceipt(
                reference_id=reference.reference_id,
                owner_type=reference.owner_type,
                owner_id=reference.owner_id,
                content_sha256=reference.content_sha256,
                terminal_state="succeeded",
                lifecycle_revision=1,
                evidence_sha256={
                    "experiment": "e" * 64,
                    "job": "a" * 64,
                    "snapshot": "b" * 64,
                }[owner_type],
                released_at=current_now[0],
            )
        )
    hooks: dict[str, ArtifactTerminalLifecycleHooks] = {}
    duck = DuckDBStore(
        tmp_path / "terminal-chain-authority.duckdb",
        artifact_terminal_hook=lambda owner_type, owner_id, observed_at: hooks["value"](
            owner_type,
            owner_id,
            observed_at,
        ),
    )
    hooks["value"] = ArtifactTerminalLifecycleHooks(
        reference_store=runtime.reference_store,
        producers={"audit": AuditTerminalReceiptProducer(runtime.reference_store, duck)},
    )
    duck.begin_data_audit_run(audit)
    duck.finalize_data_audit_run(
        audit.audit_run_id,
        DataAuditRunFinalization(p0_count=0, completed_at=NOW),
    )

    first = runtime.run_step()
    current_now[0] += timedelta(seconds=11)
    result = runtime.run_step()
    health = ArtifactGcHealthProjector(
        catalog=runtime.reference_store,
        state=runtime.gc_state,
        quarantine_inspector=runtime.transport,
        monotonic=virtual_monotonic,
    ).snapshot(now=current_now[0])

    assert first.processed_count == 2
    assert result.processed_count == 3
    assert not hot.exists()
    assert any(
        copy.storage_tier is StorageTier.COLD
        for copy in runtime.reference_store.list_active_copies(content_sha256)
    )
    assert health.status == "healthy"
    assert health.dead_letter_count == 0
    duck.close()


def test_retention_run_step_checkpoints_bounded_hot_warm_cold_migration(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    settings.migration.warm_root.mkdir(mode=0o700)
    settings.migration.cold_root.mkdir(mode=0o700)
    observed_at = [NOW]
    runtime = build_artifact_retention_runtime(
        settings=settings,
        clock=lambda: observed_at[0],
        deletion_gate_factory=lambda _settings: FullGate(),
    )
    hot = settings.managed_root / "trusted.bin"
    hot.write_bytes(b"trusted artifact")
    hot.chmod(0o600)
    content_sha256 = hashlib.sha256(hot.read_bytes()).hexdigest()
    runtime.reference_store.register_object(
        ObjectIdentity(
            content_sha256=content_sha256,
            size_bytes=hot.stat().st_size,
            object_kind="test-bundle",
            created_at=NOW - timedelta(days=100),
        )
    )
    runtime.reference_store.register_copy(
        ObjectCopy(
            content_sha256=content_sha256,
            location_id="hot-primary",
            storage_uri=hot.as_uri(),
            storage_tier=StorageTier.HOT,
            verified_at=NOW - timedelta(days=100),
            failure_domain="hot-volume",
            tier_entered_at=NOW - timedelta(days=100),
        )
    )
    for owner_type, owner_id in (
        ("audit", "a" * 64),
        ("experiment", "b" * 64),
        ("job", "11111111-1111-4111-8111-111111111111"),
        ("snapshot", "c" * 64),
    ):
        runtime.reference_store.register_reference(
            ObjectReference(
                owner_type=owner_type,
                owner_id=owner_id,
                content_sha256=content_sha256,
                created_at=NOW - timedelta(days=99),
            )
        )

    first = runtime.run_step()
    observed_at[0] = NOW + timedelta(days=31)
    second = runtime.run_step()
    copies = runtime.reference_store.list_active_copies(content_sha256)

    assert first.processed_count == 1
    assert second.processed_count == 1
    assert {copy.storage_tier for copy in copies} == {
        StorageTier.HOT,
        StorageTier.WARM,
        StorageTier.COLD,
    }
    assert runtime.gc_state.tier_migration_cursor() is not None


def test_retention_migrates_one_legal_oversized_bundle_without_cursor_starvation(
    tmp_path: Path,
) -> None:
    """A valid bundle may exceed a run budget, but may not be deferred forever."""

    payload = b"a legal bundle that is larger than the migration byte budget"
    settings = _bind_schema_payloads(_settings(tmp_path), (payload,))
    settings = settings.model_copy(
        update={
            "migration": settings.migration.model_copy(
                update={"batch_items": 1, "batch_bytes": len(payload) - 1}
            )
        }
    )
    settings.migration.warm_root.mkdir(mode=0o700)
    settings.migration.cold_root.mkdir(mode=0o700)
    runtime = build_artifact_retention_runtime(
        settings=settings,
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
    )
    hot = settings.managed_root / "oversized.bundle"
    hot.write_bytes(payload)
    hot.chmod(0o600)
    content_sha256 = hashlib.sha256(payload).hexdigest()
    runtime.reference_store.register_object(
        ObjectIdentity(
            content_sha256=content_sha256,
            size_bytes=len(payload),
            object_kind="test-bundle",
            created_at=NOW - timedelta(days=100),
        )
    )
    runtime.reference_store.register_copy(
        ObjectCopy(
            content_sha256=content_sha256,
            location_id="hot-primary",
            storage_uri=hot.as_uri(),
            storage_tier=StorageTier.HOT,
            verified_at=NOW - timedelta(days=100),
            failure_domain="hot-volume",
            tier_entered_at=NOW - timedelta(days=100),
        )
    )
    for owner_type, owner_id in (
        ("audit", "a" * 64),
        ("experiment", "b" * 64),
        ("job", "11111111-1111-4111-8111-111111111111"),
        ("snapshot", "c" * 64),
    ):
        runtime.reference_store.register_reference(
            ObjectReference(
                owner_type=owner_type,
                owner_id=owner_id,
                content_sha256=content_sha256,
                created_at=NOW - timedelta(days=99),
            )
        )

    result = runtime.run_step()

    assert result.processed_count == 1
    active_tiers = {
        copy.storage_tier for copy in runtime.reference_store.list_active_copies(content_sha256)
    }
    assert active_tiers == {
        StorageTier.HOT,
        StorageTier.WARM,
    }
    assert runtime.gc_state.tier_migration_cursor() is not None


def test_retention_migration_item_budget_checkpoints_across_runtime_restart(
    tmp_path: Path,
) -> None:
    payloads = (b"first trusted artifact", b"second trusted artifact")
    settings = _bind_schema_payloads(_settings(tmp_path), payloads)
    settings = settings.model_copy(
        update={
            "migration": settings.migration.model_copy(
                update={"batch_items": 1, "query_page_items": 2}
            )
        }
    )
    first_runtime = build_artifact_retention_runtime(
        settings=settings,
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
    )
    for index, payload in enumerate(payloads):
        content_sha256 = hashlib.sha256(payload).hexdigest()
        source = settings.managed_root / f"source-{index}.bin"
        source.write_bytes(payload)
        source.chmod(0o600)
        first_runtime.reference_store.register_object(
            ObjectIdentity(
                content_sha256=content_sha256,
                size_bytes=len(payload),
                object_kind="restart-test-bundle",
                created_at=NOW - timedelta(days=100),
            )
        )
        first_runtime.reference_store.register_copy(
            ObjectCopy(
                content_sha256=content_sha256,
                location_id=f"hot-primary-{index}",
                storage_uri=source.as_uri(),
                storage_tier=StorageTier.HOT,
                verified_at=NOW,
                failure_domain="hot-volume",
                tier_entered_at=NOW - timedelta(days=100),
            )
        )
        for owner_type, owner_id in (
            ("audit", f"{index + 1:064x}"),
            ("experiment", f"{index + 3:064x}"),
            (
                "job",
                (
                    "11111111-1111-4111-8111-111111111111"
                    if index == 0
                    else "22222222-2222-4222-8222-222222222222"
                ),
            ),
            ("snapshot", f"{index + 5:064x}"),
        ):
            first_runtime.reference_store.register_reference(
                ObjectReference(
                    owner_type=owner_type,
                    owner_id=owner_id,
                    content_sha256=content_sha256,
                    created_at=NOW - timedelta(days=99),
                )
            )

    first_result = first_runtime.run_step()
    assert first_result.processed_count == 1
    assert (
        sum(
            copy.storage_tier is StorageTier.WARM
            for payload in payloads
            for copy in first_runtime.reference_store.list_active_copies(
                hashlib.sha256(payload).hexdigest()
            )
        )
        == 1
    )
    first_runtime.reference_store.close()
    first_runtime.transport.close()

    restarted = build_artifact_retention_runtime(
        settings=settings,
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
    )
    second_result = restarted.run_step()

    assert second_result.processed_count == 1
    assert (
        sum(
            copy.storage_tier is StorageTier.WARM
            for payload in payloads
            for copy in restarted.reference_store.list_active_copies(
                hashlib.sha256(payload).hexdigest()
            )
        )
        == 2
    )


def test_retention_step_projects_through_existing_runtime_health_authority(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    runtime = build_artifact_retention_runtime(
        settings=settings,
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
    )
    spec = RuntimeServiceSpec(
        service_id="artifact-retention.primary.v1",
        plane=RuntimeServicePlane.RESEARCH,
        stale_after=timedelta(minutes=10),
        producer_commit="a" * 40,
    )
    control_root = tmp_path / "runtime-control"
    control = RuntimeServiceControl(control_root, spec=spec, clock=lambda: NOW)
    control.start()
    expected = runtime.run_step()
    control.record_success(expected, duration_seconds=0.1)

    health = RuntimeHealthSourceReader(
        sources=(RuntimeHealthControlSource(control_root=control_root, spec=spec),),
        serving_service_id="serving.primary.v1",
    )(NOW)
    heartbeat = health.payload.runtime_services[0].heartbeat

    assert heartbeat is not None
    assert heartbeat.status is RuntimeServiceStatus.RUNNING
    assert heartbeat.source_generations == expected.source_generations
    assert heartbeat.degraded_reasons == expected.degraded_reasons
    control.stop(reason="test complete")


def test_retention_service_builder_requires_research_plane_and_registered_kind(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    manifest = _retention_manifest(settings)
    valid_environment = _writer_capability_env(
        _writer_credential(),
    )
    step = artifact_retention_builder(
        clock=lambda: NOW,
        deletion_gate_factory=lambda _settings: FullGate(),
        capability_environment=valid_environment,
        open_artifact_terminal_lifecycle=lambda: _terminal_lifecycle(settings),
    )(manifest)
    assert step().processed_count == 0

    with pytest.raises(ValueError, match="research plane"):
        artifact_retention_builder(
            clock=lambda: NOW,
            deletion_gate_factory=lambda _settings: FullGate(),
        )(manifest.model_copy(update={"plane": RuntimeServicePlane.LIVE}))

    assert (
        RuntimeServiceKind.ARTIFACT_RETENTION
        in build_builtin_registry(
            clock=lambda: NOW,
        ).registered_kinds
    )


def test_retention_settings_fail_closed_on_shared_or_escaping_paths(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    payload = settings.model_dump(mode="python")
    with pytest.raises(ValueError, match="independent|state"):
        ArtifactRetentionSettings.model_validate(
            {**payload, "reference_store_path": settings.state_root / "gc-runtime.sqlite3"}
        )
    with pytest.raises(ValueError, match="state_root"):
        ArtifactRetentionSettings.model_validate(
            {**payload, "reference_store_path": tmp_path / "outside.sqlite3"}
        )


def test_gc_health_adapter_projects_standard_runtime_health_state() -> None:
    health = ArtifactGcHealthSummary(
        observed_at=NOW,
        status="critical",
        backlog_count=3,
        oldest_backlog_age_seconds=60,
        operation_reconciliation_pending_count=1,
        quarantine_orphan_count=1,
        retry_count=2,
        dead_letter_count=1,
        lease_fence=7,
        lease_active=False,
        scanned_items=2,
        scanned_bytes=512,
        truncated=True,
    )

    result = ArtifactGcHealthAuthorityAdapter().project(
        health,
        processed_count=4,
        terminal_releases=1,
    )

    assert result.input_sequence == result.output_sequence == 7
    assert result.backlog_count == 3
    assert result.degraded_reasons == (
        "artifact_gc:critical",
        "artifact_gc:dead_letters=1",
        "artifact_gc:projection_truncated",
        "artifact_gc:quarantine_orphans=1",
        "artifact_gc:reconciliation_pending=1",
    )
    assert result.source_generations.keys() == {
        "artifact_gc_health",
        "terminal_release_outbox",
    }
