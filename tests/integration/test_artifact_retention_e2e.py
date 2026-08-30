from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    ArtifactTierMigrationCoordinator,
    ObjectCopy,
    ObjectIdentity,
    ObjectReference,
    OwnerTerminalReleaseReceipt,
    RetentionPolicy,
    StorageTier,
)
from rquant.artifact_terminal_owners import (
    AuditTerminalReceiptProducer,
    ExperimentTerminalReceiptProducer,
    SnapshotTerminalReceiptProducer,
    TerminalReleaseOutboxPublisher,
)
from rquant.data_metadata import DataAuditRun, DatasetSnapshot
from rquant.experiment_registry import (
    DateRange,
    ExperimentAttempt,
    ExperimentSpec,
    ExperimentStatus,
)
from rquant.runtime_artifact_retention import (
    ArtifactGcHealthProjector,
    ArtifactGcRuntimeStore,
    ArtifactGcWorker,
    ExactFullVerifiedRecoveryDeletionGate,
    GcWorkerConfig,
    LocalAtomicArtifactTransport,
)
from rquant.runtime_contracts import canonical_sha256

# Anchored to today rather than to a calendar literal. These cases drive the GC
# worker at `receipt.completed_at + 1s` from a *real* restore, so the frozen
# timeline has to stay within the gate's `max_recovery_age` of the real clock;
# a fixed date silently ages out of that window and detonates on one specific
# day (2026-07-31 + 30d).
NOW = datetime.now(UTC).replace(hour=8, minute=0, second=0, microsecond=0)


class AuditAuthority:
    def __init__(self, record: DataAuditRun) -> None:
        self.record = record

    def get_data_audit_run(self, owner_id: str) -> DataAuditRun | None:
        return self.record if self.record.audit_run_id == owner_id else None


class SnapshotAuthority:
    def __init__(self, record: DatasetSnapshot) -> None:
        self.record = record

    def get_dataset_snapshot(self, owner_id: str) -> DatasetSnapshot | None:
        return self.record if self.record.snapshot_id == owner_id else None


class ExperimentAuthority:
    def __init__(self, record: ExperimentAttempt) -> None:
        self.record = record

    def get_attempt(self, owner_id: str) -> ExperimentAttempt:
        if self.record.spec.experiment_id != owner_id:
            raise KeyError(owner_id)
        return self.record


def _experiment() -> ExperimentAttempt:
    spec = ExperimentSpec(
        strategy_spec_fingerprint="1" * 64,
        strategy_executable_fingerprint="2" * 64,
        candidate_schema_fingerprint="3" * 64,
        dataset_snapshot_id="4" * 64,
        code_commit="5" * 40,
        parameter_fingerprint="6" * 64,
        hypothesis_family="family-a",
        metric_definition_fingerprint="7" * 64,
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 3, 31)),
        validation_range=DateRange(start_date=date(2025, 4, 1), end_date=date(2025, 6, 30)),
        frozen_outer_test_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 9, 30)),
        cost_model_fingerprint="8" * 64,
        execution_model_fingerprint="9" * 64,
        seed=1,
    )
    return ExperimentAttempt(
        spec=spec,
        status=ExperimentStatus.FAILED,
        registered_at=NOW - timedelta(hours=4),
        started_at=NOW - timedelta(hours=3),
        completed_at=NOW - timedelta(hours=2),
        first_error="terminal failure",
    )


def _authorities() -> tuple[DataAuditRun, DatasetSnapshot, ExperimentAttempt]:
    audit = DataAuditRun(
        as_of_date=(NOW - timedelta(days=1)).date(),
        range_start=(NOW - timedelta(days=30)).date(),
        range_end=(NOW - timedelta(days=1)).date(),
        rule_set_version="stage1-v2",
        status="completed",
        observed_at=NOW - timedelta(hours=4),
        completed_at=NOW - timedelta(hours=2),
    )
    snapshot = DatasetSnapshot(
        strategy_name="n-shape",
        as_of_time=NOW - timedelta(days=1),
        code_commit="a" * 40,
        origin="production",
        status="ready",
        created_at=NOW - timedelta(hours=4),
        completed_at=NOW - timedelta(hours=2),
    )
    return audit, snapshot, _experiment()


def _schema_resolver(descriptor: int) -> str:
    payload = json.loads(os.read(descriptor, 1024 * 1024))
    return canonical_sha256({"json_keys": tuple(sorted(payload))})


def _setup(
    tmp_path: Path,
) -> tuple[
    ArtifactReferenceStore,
    ArtifactGcRuntimeStore,
    LocalAtomicArtifactTransport,
    Path,
    str,
    dict[str, str],
]:
    managed = tmp_path / "managed"
    state_root = tmp_path / "state"
    for path in (managed, state_root, managed / "hot", managed / "warm", managed / "cold"):
        path.mkdir(mode=0o700)
    payload = b'{"price": 10, "volume": 20}'
    content_sha256 = hashlib.sha256(payload).hexdigest()
    hot_path = managed / "hot" / "artifact.json"
    hot_path.write_bytes(payload)
    hot_path.chmod(0o600)
    store = ArtifactReferenceStore(
        state_root / "references.sqlite3",
        managed_trust_root=state_root,
        clock=lambda: NOW,
    )
    store.register_object(
        ObjectIdentity(
            content_sha256=content_sha256,
            size_bytes=len(payload),
            object_kind="research_json",
            created_at=NOW - timedelta(days=100),
        )
    )
    store.register_copy(
        ObjectCopy(
            content_sha256=content_sha256,
            location_id="hot-primary",
            storage_uri=hot_path.as_uri(),
            storage_tier=StorageTier.HOT,
            verified_at=NOW,
            failure_domain="hot-disk",
            tier_entered_at=NOW - timedelta(days=100),
        )
    )
    audit, snapshot, experiment = _authorities()
    owner_ids = {
        "audit": audit.audit_run_id,
        "experiment": experiment.spec.experiment_id,
        "job": "job-terminal-1",
        "snapshot": snapshot.snapshot_id,
    }
    for owner_type, owner_id in owner_ids.items():
        store.register_reference(
            ObjectReference(
                owner_type=owner_type,
                owner_id=owner_id,
                content_sha256=content_sha256,
                created_at=NOW - timedelta(minutes=1),
            )
        )
    return (
        store,
        ArtifactGcRuntimeStore(
            state_root / "gc-runtime.sqlite3",
            managed_trust_root=state_root,
        ),
        LocalAtomicArtifactTransport(
            managed_root=managed,
            clock=lambda: NOW,
            schema_resolver=_schema_resolver,
        ),
        hot_path,
        content_sha256,
        owner_ids,
    )


def _release_all(
    store: ArtifactReferenceStore,
    content_sha256: str,
    owner_ids: dict[str, str],
    *,
    missing: str | None = None,
) -> None:
    audit, snapshot, experiment = _authorities()
    producers = {
        "audit": AuditTerminalReceiptProducer(store, AuditAuthority(audit)),
        "snapshot": SnapshotTerminalReceiptProducer(store, SnapshotAuthority(snapshot)),
        "experiment": ExperimentTerminalReceiptProducer(store, ExperimentAuthority(experiment)),
    }
    for owner_type, producer in producers.items():
        if owner_type != missing:
            producer.on_terminal(
                owner_id=owner_ids[owner_type],
                content_sha256=content_sha256,
                observed_at=NOW,
            )
    TerminalReleaseOutboxPublisher(store).run_batch(limit=10)
    if missing == "job":
        return
    reference = next(
        item for item in store.list_active_references(content_sha256) if item.owner_type == "job"
    )
    store.release_owner_terminal(
        OwnerTerminalReleaseReceipt(
            reference_id=reference.reference_id,
            owner_type="job",
            owner_id=owner_ids["job"],
            content_sha256=content_sha256,
            terminal_state="succeeded",
            lifecycle_revision=1,
            evidence_sha256="f" * 64,
            released_at=NOW,
        )
    )


def _migrate_to_cold(
    store: ArtifactReferenceStore,
    transport: LocalAtomicArtifactTransport,
    hot_path: Path,
    content_sha256: str,
) -> Path:
    coordinator = ArtifactTierMigrationCoordinator(store=store, transport=transport)
    schema_sha256 = canonical_sha256({"json_keys": ("price", "volume")})
    hot = next(
        item for item in store.list_active_copies(content_sha256) if item.storage_tier == "hot"
    )
    warm_path = hot_path.parents[1] / "warm" / hot_path.name
    coordinator.migrate(
        source=hot,
        target=ObjectCopy(
            content_sha256=content_sha256,
            location_id="warm-primary",
            storage_uri=warm_path.as_uri(),
            storage_tier=StorageTier.WARM,
            verified_at=NOW,
            failure_domain="warm-disk",
            tier_entered_at=NOW,
        ),
        observed_at=NOW,
        expected_schema_sha256=schema_sha256,
    )
    warm = next(
        item for item in store.list_active_copies(content_sha256) if item.storage_tier == "warm"
    )
    cold_path = hot_path.parents[1] / "cold" / hot_path.name
    coordinator.migrate(
        source=warm,
        target=ObjectCopy(
            content_sha256=content_sha256,
            location_id="cold-primary",
            storage_uri=cold_path.as_uri(),
            storage_tier=StorageTier.COLD,
            verified_at=NOW,
            failure_domain="cold-disk",
            tier_entered_at=NOW,
        ),
        observed_at=NOW,
        expected_schema_sha256=schema_sha256,
    )
    return cold_path


def _worker(
    store: ArtifactReferenceStore,
    state: ArtifactGcRuntimeStore,
    transport: LocalAtomicArtifactTransport,
    gate: object,
    now: datetime,
) -> ArtifactGcWorker:
    store._clock = lambda: now
    return ArtifactGcWorker(
        catalog=store,
        state=state,
        transport=transport,
        deletion_gate=gate,
        policy=RetentionPolicy(
            hot_min_age=timedelta(0),
            warm_min_age=timedelta(0),
            cold_min_age=timedelta(0),
            minimum_verified_copies=1,
            verification_max_age=timedelta(days=30),
            plan_ttl=timedelta(hours=1),
            claim_ttl=timedelta(minutes=10),
        ),
        config=GcWorkerConfig(
            batch_items=1,
            batch_bytes=1024**2,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=3,
            retry_delay=timedelta(seconds=1),
        ),
        worker_id="retention-e2e",
        clock=lambda: now,
    )


def _real_recovery_gate(
    tmp_path: Path,
    *,
    max_recovery_age: timedelta = timedelta(days=30),
) -> tuple[ExactFullVerifiedRecoveryDeletionGate, datetime, Path]:
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
    return (
        ExactFullVerifiedRecoveryDeletionGate(
            restore_root=restore_root,
            receipt_id=receipt.receipt_id,
            target=target,
            fixed_replay_verifier=replay,
            max_recovery_age=max_recovery_age,
        ),
        receipt.completed_at + timedelta(seconds=1),
        restore_root,
    )


def test_four_owner_terminal_to_cold_copy_full_gate_gc_and_health(tmp_path: Path) -> None:
    store, state, transport, hot_path, content_sha256, owner_ids = _setup(tmp_path)
    _release_all(store, content_sha256, owner_ids)
    cold_path = _migrate_to_cold(store, transport, hot_path, content_sha256)
    gate, recovery_now, _restore_root = _real_recovery_gate(tmp_path)

    result = _worker(store, state, transport, gate, recovery_now).run_once()
    health = ArtifactGcHealthProjector(
        catalog=store,
        state=state,
        quarantine_inspector=transport,
    ).snapshot(now=NOW)

    assert result.completed == 1
    assert not hot_path.exists()
    assert cold_path.read_bytes() == b'{"price": 10, "volume": 20}'
    assert health.status == "healthy"
    assert health.operation_reconciliation_pending_count == 0
    assert health.quarantine_orphan_count == 0


@pytest.mark.parametrize("missing", ["audit", "experiment", "job", "snapshot"])
def test_missing_any_durable_owner_receipt_blocks_physical_gc(
    tmp_path: Path,
    missing: str,
) -> None:
    store, state, transport, hot_path, content_sha256, owner_ids = _setup(tmp_path)
    _release_all(store, content_sha256, owner_ids, missing=missing)

    class ForbiddenDeletionGate:
        def authorize(self, candidate: object, *, as_of: datetime) -> object:
            del candidate, as_of
            pytest.fail("active durable owner must block recovery authorization")

    result = _worker(store, state, transport, ForbiddenDeletionGate(), NOW).run_once()

    assert result.completed == 0
    assert hot_path.exists()


@pytest.mark.parametrize("failure", ["recovery generation is degraded", "recovery gate expired"])
def test_degraded_or_expired_full_recovery_gate_blocks_unlink(
    tmp_path: Path,
    failure: str,
) -> None:
    store, state, transport, hot_path, content_sha256, owner_ids = _setup(tmp_path)
    _release_all(store, content_sha256, owner_ids)
    _migrate_to_cold(store, transport, hot_path, content_sha256)
    gate, recovery_now, restore_root = _real_recovery_gate(
        tmp_path,
        max_recovery_age=(
            timedelta(microseconds=1) if failure == "recovery gate expired" else timedelta(days=30)
        ),
    )
    if failure == "recovery generation is degraded":
        current = json.loads((restore_root / "current.json").read_text(encoding="utf-8"))
        current["target_profile_generation"] = "0" * 64
        (restore_root / "current.json").write_text(
            json.dumps(current, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )

    result = _worker(store, state, transport, gate, recovery_now).run_once()

    assert result.completed == 0
    assert result.failed == 1
    assert hot_path.exists()
