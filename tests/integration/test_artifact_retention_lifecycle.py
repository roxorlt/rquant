from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.artifact_retention import (
    ArtifactBundleRegistration,
    ArtifactReferenceStore,
    ArtifactRetentionWriterCredential,
    ArtifactTierMigrationCoordinator,
    ObjectCopy,
    ObjectIdentity,
    ObjectReference,
    StorageTier,
)
from rquant.artifact_terminal_owners import (
    ArtifactTerminalLifecycleHooks,
    AuditTerminalReceiptProducer,
    ExperimentTerminalReceiptProducer,
    SnapshotTerminalReceiptProducer,
    TerminalReleaseOutboxPublisher,
)
from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DatasetSnapshot,
    DatasetSnapshotFinalization,
)
from rquant.experiment_registry import (
    DateRange,
    ExperimentRegistry,
    ExperimentSpec,
    HypothesisFamilyManifest,
)
from rquant.runtime_artifact_retention import LocalAtomicArtifactTransport
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 8, 2, 3, 0, tzinfo=UTC)
PAYLOAD = b"restorable immutable artifact\n"
CONTENT_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()
SCHEMA_SHA256 = "7" * 64


def _retention_writer_credential() -> ArtifactRetentionWriterCredential:
    return ArtifactRetentionWriterCredential(
        key_id="retention-lifecycle-v1",
        sequence=1,
        secret_hex="1" * 64,
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )


def _copy(path: Path, tier: StorageTier, observed_at: datetime) -> ObjectCopy:
    return ObjectCopy(
        content_sha256=CONTENT_SHA256,
        location_id=f"local-{tier.value}",
        storage_uri=path.as_uri(),
        storage_tier=tier,
        verified_at=observed_at,
        failure_domain=f"disk-{tier.value}",
        tier_entered_at=observed_at,
    )


def test_interrupted_hot_warm_cold_migration_resumes_and_rejects_bad_copy(
    tmp_path: Path,
) -> None:
    observed_at = [NOW]
    hot_path = tmp_path / "hot.bundle"
    warm_path = tmp_path / "warm.bundle"
    cold_path = tmp_path / "cold.bundle"
    hot_path.write_bytes(PAYLOAD)
    hot_path.chmod(0o600)
    hot = _copy(hot_path, StorageTier.HOT, NOW)
    store = ArtifactReferenceStore(
        tmp_path / "catalog.sqlite3",
        managed_trust_root=tmp_path,
        clock=lambda: observed_at[0] + timedelta(hours=1),
    )
    identity = ObjectIdentity(
        content_sha256=CONTENT_SHA256,
        size_bytes=len(PAYLOAD),
        object_kind="strategy_lab_result",
        created_at=NOW,
    )
    store.register_bundle_atomic(
        ArtifactBundleRegistration(
            object_identity=identity,
            object_copy=hot,
            references=tuple(
                ObjectReference(
                    owner_type=owner_type,
                    owner_id=owner_id,
                    content_sha256=CONTENT_SHA256,
                    created_at=NOW,
                )
                for owner_type, owner_id in (
                    ("audit", "a" * 64),
                    ("experiment", "b" * 64),
                    ("job", "11111111-1111-4111-8111-111111111111"),
                    ("snapshot", "c" * 64),
                )
            ),
        )
    )
    local = LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: observed_at[0],
        schema_resolver=lambda _path: SCHEMA_SHA256,
    )

    class InterruptAfterCopy:
        def copy(self, source_uri: str, target_uri: str) -> None:
            local.copy(source_uri, target_uri)
            raise SystemExit("simulated interruption after durable copy")

        def durably_sync(self, storage_uri: str) -> None:
            local.durably_sync(storage_uri)

        def verify(self, storage_uri: str) -> object:
            return local.verify(storage_uri)

    warm = _copy(warm_path, StorageTier.WARM, NOW + timedelta(minutes=1))
    observed_at[0] = warm.verified_at
    with pytest.raises(SystemExit, match="interruption"):
        ArtifactTierMigrationCoordinator(store=store, transport=InterruptAfterCopy()).migrate(
            source=hot,
            target=warm,
            observed_at=observed_at[0],
            expected_schema_sha256=SCHEMA_SHA256,
        )
    assert store.list_active_copies(CONTENT_SHA256) == (hot,)

    ArtifactTierMigrationCoordinator(store=store, transport=local).migrate(
        source=hot,
        target=warm,
        observed_at=observed_at[0],
        expected_schema_sha256=SCHEMA_SHA256,
    )
    cold = _copy(cold_path, StorageTier.COLD, NOW + timedelta(minutes=2))
    observed_at[0] = cold.verified_at
    ArtifactTierMigrationCoordinator(store=store, transport=local).migrate(
        source=warm,
        target=cold,
        observed_at=observed_at[0],
        expected_schema_sha256=SCHEMA_SHA256,
    )

    assert [copy.storage_tier for copy in store.list_active_copies(CONTENT_SHA256)] == [
        StorageTier.COLD,
        StorageTier.HOT,
        StorageTier.WARM,
    ]
    assert cold_path.read_bytes() == PAYLOAD

    bad_path = tmp_path / "bad-warm.bundle"
    bad_path.write_bytes(b"corrupt")
    bad_path.chmod(0o600)
    bad = warm.model_copy(
        update={
            "location_id": "bad-warm",
            "storage_uri": bad_path.as_uri(),
            "failure_domain": "bad-disk",
        }
    )
    with pytest.raises(ValueError, match="exists|hash|size"):
        local.copy(hot.storage_uri, bad.storage_uri)
    assert all(copy.location_id != "bad-warm" for copy in store.list_active_copies(CONTENT_SHA256))


def test_real_audit_snapshot_and_experiment_terminal_lifecycles_publish_release_outbox(
    tmp_path: Path,
) -> None:
    reference_store = ArtifactReferenceStore(
        tmp_path / "owner-references.sqlite3",
        managed_trust_root=tmp_path,
        clock=lambda: NOW,
        writer_owner="artifact-retention",
        retention_writer_credential=_retention_writer_credential(),
    )
    hooks: dict[str, ArtifactTerminalLifecycleHooks] = {}
    duck = DuckDBStore(
        tmp_path / "owner-authority.duckdb",
        artifact_terminal_hook=lambda *args: hooks["value"](*args),
    )
    experiment_registry = ExperimentRegistry(
        tmp_path / "experiment-authority.sqlite3",
        managed_trust_root=tmp_path,
        artifact_terminal_hook=lambda *args: hooks["value"](*args),
    )
    audit = DataAuditRun.create(
        as_of_date=date(2026, 8, 1),
        range_start=date(2026, 7, 1),
        range_end=date(2026, 8, 1),
        rule_set_version="retention-e2e-v1",
        observed_at=NOW - timedelta(minutes=10),
    )
    snapshot = DatasetSnapshot(
        strategy_name="n-shape",
        as_of_time=NOW - timedelta(days=1),
        code_commit="1" * 40,
        origin="production",
        status="building",
        created_at=NOW - timedelta(minutes=9),
    )
    experiment = ExperimentSpec(
        strategy_spec_fingerprint="1" * 64,
        strategy_executable_fingerprint="2" * 64,
        candidate_schema_fingerprint="3" * 64,
        dataset_snapshot_id=snapshot.snapshot_id,
        code_commit="4" * 40,
        parameter_fingerprint="5" * 64,
        hypothesis_family="retention-e2e",
        metric_definition_fingerprint="6" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 3, 31)),
        validation_range=DateRange(start_date=date(2025, 4, 1), end_date=date(2025, 6, 30)),
        frozen_outer_test_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 9, 30)),
        cost_model_fingerprint="7" * 64,
        execution_model_fingerprint="8" * 64,
        seed=11,
    )
    owner_ids = {
        "audit": audit.audit_run_id,
        "snapshot": snapshot.snapshot_id,
        "experiment": experiment.experiment_id,
    }
    content_by_owner = {
        owner_type: hashlib.sha256(owner_type.encode()).hexdigest() for owner_type in owner_ids
    }
    for owner_type, owner_id in owner_ids.items():
        content_sha256 = content_by_owner[owner_type]
        reference_store.register_object(
            ObjectIdentity(
                content_sha256=content_sha256,
                size_bytes=1,
                object_kind=f"{owner_type}-result",
                created_at=NOW - timedelta(minutes=8),
            )
        )
        reference_store.register_reference(
            ObjectReference(
                owner_type=owner_type,
                owner_id=owner_id,
                content_sha256=content_sha256,
                created_at=NOW - timedelta(minutes=7),
            )
        )
    hooks["value"] = ArtifactTerminalLifecycleHooks(
        reference_store=reference_store,
        producers={
            "audit": AuditTerminalReceiptProducer(reference_store, duck),
            "snapshot": SnapshotTerminalReceiptProducer(reference_store, duck),
            "experiment": ExperimentTerminalReceiptProducer(reference_store, experiment_registry),
        },
    )

    duck.begin_data_audit_run(audit)
    duck.begin_dataset_snapshot(snapshot)
    experiment_registry.register_hypothesis_family(
        HypothesisFamilyManifest(
            hypothesis_family=experiment.hypothesis_family,
            experiment_ids=(experiment.experiment_id,),
            search_space_fingerprint="9" * 64,
            metric_definition_fingerprint=experiment.metric_definition_fingerprint,
            preregistered_at=NOW - timedelta(minutes=6),
        )
    )
    experiment_registry.register_attempt(
        experiment,
        registered_at=NOW - timedelta(minutes=5),
    )
    experiment_registry.start_attempt(
        experiment.experiment_id,
        started_at=NOW - timedelta(minutes=4),
    )
    assert reference_store.pending_owner_terminal_releases(limit=10) == ()

    duck.finalize_data_audit_run(
        audit.audit_run_id,
        DataAuditRunFinalization(p0_count=0, completed_at=NOW - timedelta(minutes=3)),
    )
    duck.finalize_dataset_snapshot(
        snapshot.snapshot_id,
        DatasetSnapshotFinalization(completed_at=NOW - timedelta(minutes=2)),
    )
    experiment_registry.record_failure(
        experiment.experiment_id,
        first_error="expected bounded failure",
        completed_at=NOW - timedelta(minutes=1),
    )

    pending = reference_store.pending_owner_terminal_releases(limit=10)
    assert {receipt.owner_type for receipt in pending} == {
        "audit",
        "experiment",
        "snapshot",
    }
    assert TerminalReleaseOutboxPublisher(reference_store).run_batch(limit=10) == 3
    assert all(
        reference_store.list_active_owner_references(
            owner_type=owner_type,
            owner_id=owner_id,
        )
        == ()
        for owner_type, owner_id in owner_ids.items()
    )
    duck.close()


def test_terminal_authority_commit_then_hook_crash_retries_one_immutable_receipt(
    tmp_path: Path,
) -> None:
    reference_store = ArtifactReferenceStore(
        tmp_path / "owner-references.sqlite3",
        managed_trust_root=tmp_path,
        clock=lambda: NOW,
        writer_owner="artifact-retention",
        retention_writer_credential=_retention_writer_credential(),
    )
    hooks: dict[str, ArtifactTerminalLifecycleHooks] = {}
    crash_once = [True]

    def terminal_hook(owner_type: str, owner_id: str, observed_at: datetime) -> None:
        hooks["value"](owner_type, owner_id, observed_at)
        if crash_once[0]:
            crash_once[0] = False
            raise RuntimeError("simulated crash after durable terminal outbox")

    duck = DuckDBStore(
        tmp_path / "owner-authority.duckdb",
        artifact_terminal_hook=terminal_hook,
    )
    audit = DataAuditRun.create(
        as_of_date=date(2026, 8, 1),
        range_start=date(2026, 7, 1),
        range_end=date(2026, 8, 1),
        rule_set_version="retention-crash-v1",
        observed_at=NOW - timedelta(minutes=3),
    )
    content_sha256 = hashlib.sha256(b"audit-crash-result").hexdigest()
    reference_store.register_object(
        ObjectIdentity(
            content_sha256=content_sha256,
            size_bytes=1,
            object_kind="audit-result",
            created_at=NOW - timedelta(minutes=2),
        )
    )
    reference_store.register_reference(
        ObjectReference(
            owner_type="audit",
            owner_id=audit.audit_run_id,
            content_sha256=content_sha256,
            created_at=NOW - timedelta(minutes=1),
        )
    )
    hooks["value"] = ArtifactTerminalLifecycleHooks(
        reference_store=reference_store,
        producers={"audit": AuditTerminalReceiptProducer(reference_store, duck)},
    )
    duck.begin_data_audit_run(audit)
    finalization = DataAuditRunFinalization(p0_count=0, completed_at=NOW)

    with pytest.raises(RuntimeError, match="after durable terminal outbox"):
        duck.finalize_data_audit_run(audit.audit_run_id, finalization)

    persisted = duck.get_data_audit_run(audit.audit_run_id)
    pending_after_crash = reference_store.pending_owner_terminal_releases(limit=10)
    assert persisted is not None and persisted.status == "completed"
    assert len(pending_after_crash) == 1

    assert duck.finalize_data_audit_run(audit.audit_run_id, finalization) == persisted
    pending_after_retry = reference_store.pending_owner_terminal_releases(limit=10)
    assert pending_after_retry == pending_after_crash
    assert TerminalReleaseOutboxPublisher(reference_store).run_batch(limit=10) == 1
    assert (
        reference_store.list_active_owner_references(
            owner_type="audit",
            owner_id=audit.audit_run_id,
        )
        == ()
    )
    duck.close()
