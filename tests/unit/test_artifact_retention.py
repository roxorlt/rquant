from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.artifact_retention as artifact_retention_module
from rquant.artifact_retention import (
    ArtifactReferenceStore,
    ArtifactRetentionWriterAuthorizationError,
    ArtifactRetentionWriterCredential,
    GcClaim,
    LegalHold,
    ObjectCopy,
    ObjectIdentity,
    ObjectReference,
    RetentionPolicy,
    StorageTier,
)
from rquant.runtime_contracts import RuntimeContractModel

NOW = datetime(2026, 7, 31, 8, 0, tzinfo=UTC)
HASH_A = "a" * 64


def _new_store(path: Path) -> ArtifactReferenceStore:
    return ArtifactReferenceStore(
        path,
        managed_trust_root=path.parent,
        clock=lambda: NOW + timedelta(days=400),
    )


def _object(
    *,
    content_sha256: str = HASH_A,
    size_bytes: int = 128,
    created_at: datetime = NOW - timedelta(days=120),
) -> ObjectIdentity:
    return ObjectIdentity(
        content_sha256=content_sha256,
        size_bytes=size_bytes,
        object_kind="research_snapshot",
        created_at=created_at,
    )


def _copy(
    *,
    content_sha256: str = HASH_A,
    location_id: str = "cloud-primary",
    storage_uri: str = "s3://research/a.parquet",
    tier: StorageTier = StorageTier.HOT,
    verified_at: datetime | None = NOW - timedelta(minutes=5),
    failure_domain: str | None = None,
    tier_entered_at: datetime = NOW - timedelta(days=120),
) -> ObjectCopy:
    return ObjectCopy(
        content_sha256=content_sha256,
        location_id=location_id,
        storage_uri=storage_uri,
        storage_tier=tier,
        verified_at=verified_at,
        failure_domain=failure_domain or location_id,
        tier_entered_at=tier_entered_at,
    )


def _policy(*, minimum_verified_copies: int = 1) -> RetentionPolicy:
    return RetentionPolicy(
        hot_min_age=timedelta(days=30),
        warm_min_age=timedelta(days=60),
        cold_min_age=timedelta(days=90),
        minimum_verified_copies=minimum_verified_copies,
        verification_max_age=timedelta(days=1),
        plan_ttl=timedelta(minutes=10),
        claim_ttl=timedelta(minutes=2),
    )


def _store_with_copies(path: Path, *, copies: int = 2) -> ArtifactReferenceStore:
    store = _new_store(path)
    store.register_object(_object())
    for index in range(copies):
        store.register_copy(
            _copy(
                location_id=f"location-{index}",
                storage_uri=f"s3://research/a-{index}.parquet",
            )
        )
    return store


def _release_terminal(
    store: ArtifactReferenceStore,
    reference: ObjectReference,
    *,
    released_at: datetime,
) -> None:
    store.release_owner_terminal(
        artifact_retention_module.OwnerTerminalReleaseReceipt(
            reference_id=reference.reference_id,
            owner_type=reference.owner_type,
            owner_id=reference.owner_id,
            content_sha256=reference.content_sha256,
            terminal_state="retired",
            lifecycle_revision=1,
            evidence_sha256="e" * 64,
            released_at=released_at,
        )
    )


def test_contracts_are_frozen_runtime_models_and_validate_identity() -> None:
    identity = _object()

    assert isinstance(identity, RuntimeContractModel)
    with pytest.raises(ValidationError):
        identity.size_bytes = 10  # type: ignore[misc]
    with pytest.raises(ValidationError, match="content_sha256"):
        _object(content_sha256="ABC")
    with pytest.raises(ValidationError, match="timezone-aware"):
        _object(created_at=NOW.replace(tzinfo=None))


def test_register_object_rejects_conflicting_metadata_for_same_hash(tmp_path: Path) -> None:
    store = _new_store(tmp_path / "references.sqlite3")
    store.register_object(_object())
    store.register_object(_object())

    with pytest.raises(ValueError, match="conflicting object metadata"):
        store.register_object(_object(size_bytes=129))


def test_active_reference_blocks_gc_and_release_is_append_only(tmp_path: Path) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    reference = ObjectReference(
        owner_type="snapshot",
        owner_id="snapshot-20260731",
        content_sha256=HASH_A,
        created_at=NOW - timedelta(days=10),
    )
    store.register_reference(reference)

    assert store.plan_gc(now=NOW, policy=_policy()).candidates == ()

    _release_terminal(store, reference, released_at=NOW)
    plan = store.plan_gc(now=NOW + timedelta(seconds=1), policy=_policy())

    assert len(plan.candidates) == 1
    events = store.list_audit_events()
    assert [event.event_type for event in events][-2:] == [
        "reference_registered",
        "owner_terminal_released",
    ]
    assert f'"reference_id":"{reference.reference_id}"' in events[-1].payload_json


@pytest.mark.parametrize("owner_type", ["snapshot", "experiment"])
def test_snapshot_and_experiment_references_can_never_be_candidates(
    tmp_path: Path,
    owner_type: str,
) -> None:
    store = _store_with_copies(tmp_path / f"{owner_type}.sqlite3")
    store.register_reference(
        ObjectReference(
            owner_type=owner_type,
            owner_id=f"{owner_type}-1",
            content_sha256=HASH_A,
            created_at=NOW - timedelta(days=1),
        )
    )

    assert store.plan_gc(now=NOW + timedelta(days=365), policy=_policy()).candidates == ()


def test_expired_reference_no_longer_blocks_gc(tmp_path: Path) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    store.register_reference(
        ObjectReference(
            owner_type="download",
            owner_id="temporary-export",
            content_sha256=HASH_A,
            created_at=NOW - timedelta(days=2),
            expires_at=NOW - timedelta(seconds=1),
        )
    )

    plan = store.plan_gc(now=NOW, policy=_policy())

    assert len(plan.candidates) == 1


def test_plan_normalizes_non_utc_now_before_reference_expiry_comparison(
    tmp_path: Path,
) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    store.register_reference(
        ObjectReference(
            owner_type="download",
            owner_id="still-active",
            content_sha256=HASH_A,
            created_at=NOW - timedelta(days=1),
            expires_at=NOW + timedelta(minutes=1),
        )
    )
    cst = timezone(timedelta(hours=8))
    equivalent_now = (NOW - timedelta(minutes=1)).astimezone(cst)

    plan = store.plan_gc(now=equivalent_now, policy=_policy())

    assert plan.planned_at == NOW - timedelta(minutes=1)
    assert plan.candidates == ()


def test_legal_hold_blocks_gc_until_released(tmp_path: Path) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    hold = LegalHold(
        hold_id="investigation-17",
        content_sha256=HASH_A,
        reason="audit investigation",
        created_at=NOW - timedelta(days=1),
    )
    store.register_legal_hold(hold)

    assert store.plan_gc(now=NOW, policy=_policy()).candidates == ()

    store.release_legal_hold(hold.hold_id, released_at=NOW)
    assert len(store.plan_gc(now=NOW, policy=_policy()).candidates) == 1


def test_plan_respects_tier_age_and_verified_copy_count(tmp_path: Path) -> None:
    store = _new_store(tmp_path / "references.sqlite3")
    store.register_object(_object(created_at=NOW - timedelta(days=70)))
    store.register_copy(
        _copy(
            location_id="hot",
            storage_uri="s3://research/hot.parquet",
            tier=StorageTier.HOT,
        )
    )
    store.register_copy(
        _copy(
            location_id="warm",
            storage_uri="s3://research/warm.parquet",
            tier=StorageTier.WARM,
        )
    )
    store.register_copy(
        _copy(
            location_id="cold",
            storage_uri="s3://research/cold.parquet",
            tier=StorageTier.COLD,
        )
    )

    plan = store.plan_gc(now=NOW, policy=_policy(minimum_verified_copies=2))

    assert [candidate.object_copy.location_id for candidate in plan.candidates] == ["hot"]
    assert (
        store.plan_gc(
            now=NOW,
            policy=_policy(minimum_verified_copies=3),
        ).candidates
        == ()
    )


def test_unverified_copy_does_not_satisfy_copy_safety(tmp_path: Path) -> None:
    store = _new_store(tmp_path / "references.sqlite3")
    store.register_object(_object())
    store.register_copy(_copy(location_id="verified", storage_uri="s3://a"))
    store.register_copy(
        _copy(
            location_id="not-verified",
            storage_uri="s3://b",
            verified_at=None,
        )
    )

    assert store.plan_gc(now=NOW, policy=_policy()).candidates == ()


def test_future_verification_timestamp_does_not_satisfy_copy_safety(tmp_path: Path) -> None:
    store = _new_store(tmp_path / "references.sqlite3")
    store.register_object(_object())
    store.register_copy(_copy(location_id="verified", storage_uri="s3://a"))
    store.register_copy(
        _copy(
            location_id="future-verification",
            storage_uri="s3://b",
            verified_at=NOW + timedelta(minutes=1),
        )
    )

    assert store.plan_gc(now=NOW, policy=_policy()).candidates == ()


def test_stale_verification_and_recent_tier_entry_do_not_qualify(tmp_path: Path) -> None:
    store = _new_store(tmp_path / "references.sqlite3")
    store.register_object(_object())
    store.register_copy(
        _copy(
            location_id="stale",
            storage_uri="s3://stale",
            verified_at=NOW - timedelta(days=2),
        )
    )
    store.register_copy(
        _copy(
            location_id="recent-tier",
            storage_uri="s3://recent-tier",
            tier_entered_at=NOW - timedelta(days=1),
        )
    )

    assert store.plan_gc(now=NOW, policy=_policy()).candidates == ()


def test_copy_registration_requires_unique_uri_and_independent_failure_domain(
    tmp_path: Path,
) -> None:
    store = _new_store(tmp_path / "references.sqlite3")
    store.register_object(_object())
    store.register_copy(_copy(location_id="first", storage_uri="s3://same", failure_domain="az-a"))

    with pytest.raises(ValueError, match="storage URI"):
        store.register_copy(
            _copy(location_id="alias", storage_uri="s3://same", failure_domain="az-b")
        )
    with pytest.raises(ValueError, match="failure domain"):
        store.register_copy(
            _copy(location_id="same-az", storage_uri="s3://other", failure_domain="az-a")
        )
    with pytest.raises(ValidationError, match="verified_at"):
        _copy(
            location_id="time-travel",
            storage_uri="s3://time-travel",
            tier_entered_at=NOW,
            verified_at=NOW - timedelta(seconds=1),
        )


def test_gc_plan_rejects_future_clock_and_expires(tmp_path: Path) -> None:
    store = ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
        clock=lambda: NOW,
    )
    store.register_object(_object())
    store.register_copy(_copy(location_id="one", storage_uri="s3://one"))
    store.register_copy(_copy(location_id="two", storage_uri="s3://two"))

    with pytest.raises(ValueError, match="trusted clock"):
        store.plan_gc(now=NOW + timedelta(days=31), policy=_policy())

    plan = store.plan_gc(now=NOW, policy=_policy())
    with pytest.raises(ValueError, match="expired"):
        store.claim_deletion(
            plan=plan,
            candidate=plan.candidates[0],
            owner_id="gc-worker",
            now=plan.expires_at + timedelta(seconds=1),
        )


def test_plan_and_candidate_ids_are_deterministic(tmp_path: Path) -> None:
    first = _store_with_copies(tmp_path / "first.sqlite3", copies=3)
    second = _store_with_copies(tmp_path / "second.sqlite3", copies=3)

    first_plan = first.plan_gc(now=NOW, policy=_policy())
    second_plan = second.plan_gc(now=NOW, policy=_policy())

    assert first_plan.plan_id == second_plan.plan_id
    assert [item.candidate_id for item in first_plan.candidates] == [
        item.candidate_id for item in second_plan.candidates
    ]


def test_mark_deleted_requires_exact_identity_and_records_only_ledger_state(
    tmp_path: Path,
) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    plan = store.plan_gc(now=NOW, policy=_policy())
    candidate = plan.candidates[0]
    remaining_copy = next(
        copy for copy in store.list_active_copies(HASH_A) if copy != candidate.object_copy
    )
    mismatched = candidate.model_copy(
        update={
            "object_copy": candidate.object_copy.model_copy(
                update={"storage_uri": "s3://attacker/replacement.parquet"}
            )
        }
    )

    claim = store.claim_deletion(
        plan=plan,
        candidate=candidate,
        owner_id="gc-worker",
        now=NOW + timedelta(seconds=1),
    )
    assert isinstance(claim, GcClaim)

    with pytest.raises(ValueError, match="observed identity"):
        store.mark_deleted(
            claim=claim,
            observed_identity=mismatched,
            now=NOW + timedelta(seconds=2),
        )

    assert (
        store.mark_deleted(
            claim=claim,
            observed_identity=candidate,
            now=NOW + timedelta(seconds=3),
        )
        is True
    )

    assert store.list_active_copies(HASH_A) == (remaining_copy,)
    assert store.list_deleted_candidates(plan.plan_id) == (candidate.candidate_id,)
    assert (
        store.mark_deleted(
            claim=claim,
            observed_identity=candidate,
            now=NOW + timedelta(seconds=4),
        )
        is False
    )


def test_mark_deleted_rejects_candidate_not_in_plan(tmp_path: Path) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3", copies=3)
    plan = store.plan_gc(now=NOW, policy=_policy())
    unplanned = plan.candidates[1].model_copy(update={"candidate_id": "f" * 64})

    with pytest.raises(ValueError, match="candidate is not part of plan"):
        store.claim_deletion(
            plan=plan,
            candidate=unplanned,
            owner_id="gc-worker",
            now=NOW,
        )


def test_gc_operation_id_rejects_conflicting_candidate_content(tmp_path: Path) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3", copies=3)
    plan = store.plan_gc(now=NOW, policy=_policy())
    operation_id = "d" * 64
    store.claim_deletion(
        plan=plan,
        candidate=plan.candidates[0],
        owner_id="gc-worker",
        operation_id=operation_id,
        now=NOW,
    )

    with pytest.raises(ValueError, match="operation identity conflicts"):
        store.claim_deletion(
            plan=plan,
            candidate=plan.candidates[1],
            owner_id="gc-worker",
            operation_id=operation_id,
            now=NOW,
        )


def test_mark_deleted_rejects_stale_plan_after_governance_change(tmp_path: Path) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    plan = store.plan_gc(now=NOW, policy=_policy())
    store.register_reference(
        ObjectReference(
            owner_type="snapshot",
            owner_id="late-reference",
            content_sha256=HASH_A,
            created_at=NOW + timedelta(seconds=1),
        )
    )

    with pytest.raises(ValueError, match="stale GC plan"):
        store.claim_deletion(
            plan=plan,
            candidate=plan.candidates[0],
            owner_id="gc-worker",
            now=NOW + timedelta(seconds=2),
        )


def test_claim_freezes_new_reference_and_legal_hold_until_explicit_release(
    tmp_path: Path,
) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    plan = store.plan_gc(now=NOW, policy=_policy())
    claim = store.claim_deletion(
        plan=plan,
        candidate=plan.candidates[0],
        owner_id="gc-worker",
        now=NOW + timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="deletion claim"):
        store.register_reference(
            ObjectReference(
                owner_type="snapshot",
                owner_id="late-reference",
                content_sha256=HASH_A,
                created_at=NOW + timedelta(seconds=2),
            )
        )
    with pytest.raises(ValueError, match="deletion claim"):
        store.register_legal_hold(
            LegalHold(
                hold_id="late-hold",
                content_sha256=HASH_A,
                reason="legal request",
                created_at=NOW + timedelta(seconds=2),
            )
        )

    store.release_claim(
        claim_id=claim.claim_id,
        owner_id="gc-worker",
        now=NOW + timedelta(seconds=3),
        reason="external deletion did not start",
    )
    store.register_reference(
        ObjectReference(
            owner_type="snapshot",
            owner_id="late-reference",
            content_sha256=HASH_A,
            created_at=NOW + timedelta(seconds=4),
        )
    )


def test_mark_deleted_rechecks_claim_lease_age_and_copy_safety(tmp_path: Path) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3", copies=3)
    plan = store.plan_gc(now=NOW, policy=_policy(minimum_verified_copies=1))
    claim = store.claim_deletion(
        plan=plan,
        candidate=plan.candidates[0],
        owner_id="gc-worker",
        now=NOW + timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="claim expired"):
        store.mark_deleted(
            claim=claim,
            observed_identity=claim.candidate,
            now=claim.expires_at + timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="cannot precede"):
        store.mark_deleted(
            claim=claim,
            observed_identity=claim.candidate,
            now=claim.claimed_at - timedelta(microseconds=1),
        )


def test_store_reopens_with_wal_persistence_and_append_only_audit(tmp_path: Path) -> None:
    path = tmp_path / "references.sqlite3"
    store = _store_with_copies(path)
    plan = store.plan_gc(now=NOW, policy=_policy())
    store.close()

    reopened = _new_store(path)
    assert reopened.get_object(HASH_A) == _object()
    assert reopened.plan_gc(now=NOW, policy=_policy()).plan_id == plan.plan_id

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            connection.execute("DELETE FROM artifact_audit")


def test_writer_does_not_mask_begin_lock_error_with_rollback(tmp_path: Path) -> None:
    store = _new_store(tmp_path / "references.sqlite3")

    class LockedConnection:
        in_transaction = False
        rollback_called = False

        def execute(self, statement: str) -> None:
            if statement == "BEGIN IMMEDIATE":
                raise sqlite3.OperationalError("database is locked")
            if statement == "ROLLBACK":
                self.rollback_called = True
                raise sqlite3.OperationalError("no transaction is active")

        def close(self) -> None:
            return None

    locked = LockedConnection()
    store._connect = lambda: locked  # type: ignore[method-assign,return-value]

    with pytest.raises(sqlite3.OperationalError, match="database is locked"), store._writer():
        pass
    assert locked.rollback_called is False


def test_module_never_performs_external_file_deletion() -> None:
    source = Path("src/rquant/artifact_retention.py").read_text(encoding="utf-8")

    assert "os.unlink" not in source
    assert "os.remove" not in source
    assert "shutil.rmtree" not in source


def test_terminal_owner_release_requires_content_bound_idempotent_receipt(
    tmp_path: Path,
) -> None:
    receipt_type = getattr(artifact_retention_module, "OwnerTerminalReleaseReceipt", None)
    assert receipt_type is not None, "terminal owner release receipt contract is missing"

    store = _store_with_copies(tmp_path / "references.sqlite3")
    reference = ObjectReference(
        owner_type="job",
        owner_id="job-42",
        content_sha256=HASH_A,
        created_at=NOW - timedelta(days=2),
    )
    store.register_reference(reference)
    with pytest.raises(ValueError, match="terminal release receipt"):
        store.release_reference(reference.reference_id, released_at=NOW)
    receipt = receipt_type(
        reference_id=reference.reference_id,
        owner_type=reference.owner_type,
        owner_id=reference.owner_id,
        content_sha256=reference.content_sha256,
        terminal_state="completed",
        lifecycle_revision=7,
        evidence_sha256="b" * 64,
        released_at=NOW,
    )

    assert store.release_owner_terminal(receipt) is True
    assert store.release_owner_terminal(receipt) is False
    assert len(store.plan_gc(now=NOW + timedelta(seconds=1), policy=_policy()).candidates) == 1

    conflicting = receipt_type(
        reference_id=reference.reference_id,
        owner_type=reference.owner_type,
        owner_id=reference.owner_id,
        content_sha256=reference.content_sha256,
        terminal_state="failed",
        lifecycle_revision=8,
        evidence_sha256="c" * 64,
        released_at=NOW + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="conflicting terminal release"):
        store.release_owner_terminal(conflicting)

    invalid_payload = {
        "reference_id": reference.reference_id,
        "owner_type": reference.owner_type,
        "owner_id": reference.owner_id,
        "content_sha256": reference.content_sha256,
        "terminal_state": "running",
        "lifecycle_revision": 9,
        "evidence_sha256": "d" * 64,
        "released_at": NOW + timedelta(seconds=2),
    }
    with pytest.raises(ValidationError, match="terminal_state"):
        receipt_type(**invalid_payload)
    with pytest.raises(ValidationError, match="durable owner"):
        receipt_type(**{**invalid_payload, "owner_type": "temporary", "terminal_state": "retired"})

    deployment_reference = ObjectReference(
        owner_type="deployment",
        owner_id="deploy-evidence-42",
        content_sha256=HASH_A,
        created_at=NOW,
    )
    store.register_reference(deployment_reference)
    with pytest.raises(ValueError, match="terminal release receipt"):
        store.release_reference(
            deployment_reference.reference_id,
            released_at=NOW + timedelta(seconds=3),
        )
    assert store.release_owner_terminal(
        receipt_type(
            reference_id=deployment_reference.reference_id,
            owner_type=deployment_reference.owner_type,
            owner_id=deployment_reference.owner_id,
            content_sha256=deployment_reference.content_sha256,
            terminal_state="retired",
            lifecycle_revision=1,
            evidence_sha256="f" * 64,
            released_at=NOW + timedelta(seconds=3),
        )
    )


@pytest.mark.parametrize("owner_type", ["audit", "experiment", "snapshot"])
def test_durable_owner_terminal_receipt_outbox_is_idempotent_and_publishes_atomically(
    tmp_path: Path,
    owner_type: str,
) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    reference = ObjectReference(
        owner_type=owner_type,
        owner_id=f"{owner_type}-42",
        content_sha256=HASH_A,
        created_at=NOW - timedelta(days=2),
    )
    store.register_reference(reference)
    receipt = artifact_retention_module.OwnerTerminalReleaseReceipt(
        reference_id=reference.reference_id,
        owner_type=reference.owner_type,
        owner_id=reference.owner_id,
        content_sha256=reference.content_sha256,
        terminal_state="completed",
        lifecycle_revision=7,
        evidence_sha256="b" * 64,
        released_at=NOW,
    )

    assert store.enqueue_owner_terminal_release(receipt, enqueued_at=NOW) is True
    assert store.enqueue_owner_terminal_release(receipt, enqueued_at=NOW) is False
    assert store.pending_owner_terminal_releases(limit=10) == (receipt,)
    assert store.plan_gc(now=NOW + timedelta(seconds=1), policy=_policy()).candidates == ()

    conflicting = receipt.model_copy(
        update={
            "receipt_id": None,
            "lifecycle_revision": 8,
            "evidence_sha256": "c" * 64,
        }
    )
    with pytest.raises(ValueError, match="conflicting terminal release outbox"):
        store.enqueue_owner_terminal_release(conflicting, enqueued_at=NOW)

    assert store.publish_owner_terminal_release(receipt.receipt_id) is True
    assert store.publish_owner_terminal_release(receipt.receipt_id) is False
    assert store.pending_owner_terminal_releases(limit=10) == ()
    assert len(store.plan_gc(now=NOW + timedelta(seconds=1), policy=_policy()).candidates) == 1


def test_terminal_receipt_producer_outbox_excludes_job_owner_until_job_integration(
    tmp_path: Path,
) -> None:
    producer = getattr(
        artifact_retention_module,
        "DurableOwnerTerminalReceiptProducer",
        None,
    )
    assert producer is not None
    store = _store_with_copies(tmp_path / "references.sqlite3")
    reference = ObjectReference(
        owner_type="job",
        owner_id="job-42",
        content_sha256=HASH_A,
        created_at=NOW - timedelta(days=2),
    )
    store.register_reference(reference)
    receipt = artifact_retention_module.OwnerTerminalReleaseReceipt(
        reference_id=reference.reference_id,
        owner_type=reference.owner_type,
        owner_id=reference.owner_id,
        content_sha256=reference.content_sha256,
        terminal_state="completed",
        lifecycle_revision=7,
        evidence_sha256="b" * 64,
        released_at=NOW,
    )

    with pytest.raises(ValueError, match="audit|experiment|snapshot|producer"):
        store.enqueue_owner_terminal_release(receipt, enqueued_at=NOW)

    assert store.release_owner_terminal(receipt) is True


def test_catalog_job_release_requires_retention_capability_and_survives_rotation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "references.sqlite3"
    initial = ArtifactRetentionWriterCredential(
        key_id="retention",
        sequence=1,
        secret_hex="1" * 64,
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )
    store = ArtifactReferenceStore(
        path,
        managed_trust_root=tmp_path,
        writer_owner="artifact-retention",
        retention_writer_credential=initial,
        clock=lambda: NOW,
    )
    reference = ObjectReference(
        owner_type="job",
        owner_id="job-retention-owned",
        content_sha256=HASH_A,
        created_at=NOW - timedelta(minutes=1),
    )
    store.register_object(_object(created_at=NOW - timedelta(minutes=2)))
    store.register_reference(reference)
    receipt = artifact_retention_module.OwnerTerminalReleaseReceipt(
        reference_id=reference.reference_id,
        owner_type="job",
        owner_id=reference.owner_id,
        content_sha256=reference.content_sha256,
        terminal_state="completed",
        lifecycle_revision=1,
        evidence_sha256="b" * 64,
        released_at=NOW,
    )

    assert store.apply_catalog_job_terminal_release(receipt, applied_at=NOW) is True
    assert store.apply_catalog_job_terminal_release(receipt, applied_at=NOW) is False

    terminal_outbox_only = ArtifactReferenceStore(
        path,
        managed_trust_root=tmp_path,
        writer_owner="artifact-terminal-outbox",
        terminal_outbox_only=True,
        clock=lambda: NOW,
    )
    with pytest.raises(ArtifactRetentionWriterAuthorizationError, match="retention-owned"):
        terminal_outbox_only.apply_catalog_job_terminal_release(receipt, applied_at=NOW)
    terminal_outbox_only.close()

    rotated = ArtifactRetentionWriterCredential(
        key_id="retention",
        sequence=2,
        secret_hex="2" * 64,
        previous_secret_hex="1" * 64,
        not_before=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )
    rotated_store = ArtifactReferenceStore(
        path,
        managed_trust_root=tmp_path,
        writer_owner="artifact-retention",
        retention_writer_credential=rotated,
        clock=lambda: NOW,
    )
    assert rotated_store.apply_catalog_job_terminal_release(receipt, applied_at=NOW) is False
    rotated_store.close()
    store.close()

    with pytest.raises(ValueError, match="old|superseded|rotation"):
        ArtifactReferenceStore(
            path,
            managed_trust_root=tmp_path,
            writer_owner="artifact-retention",
            retention_writer_credential=initial,
            clock=lambda: NOW,
        )


def test_terminal_release_outbox_cannot_be_forged_updated_or_deleted(tmp_path: Path) -> None:
    store = _store_with_copies(tmp_path / "references.sqlite3")
    reference = ObjectReference(
        owner_type="audit",
        owner_id="audit-immutable",
        content_sha256=HASH_A,
        created_at=NOW - timedelta(days=2),
    )
    store.register_reference(reference)
    receipt = artifact_retention_module.OwnerTerminalReleaseReceipt(
        reference_id=reference.reference_id,
        owner_type=reference.owner_type,
        owner_id=reference.owner_id,
        content_sha256=reference.content_sha256,
        terminal_state="completed",
        lifecycle_revision=1,
        evidence_sha256="b" * 64,
        released_at=NOW,
    )
    store.enqueue_owner_terminal_release(receipt, enqueued_at=NOW)

    with pytest.raises(sqlite3.IntegrityError), store._writer() as connection:
        connection.execute(
            "UPDATE artifact_owner_release_outbox SET published_at = ?",
            (NOW.isoformat(),),
        )

    store.publish_owner_terminal_release(receipt.receipt_id)
    with pytest.raises(sqlite3.IntegrityError), store._writer() as connection:
        connection.execute("UPDATE artifact_owner_release_outbox SET owner_id = 'forged'")
    with pytest.raises(sqlite3.IntegrityError), store._writer() as connection:
        connection.execute("DELETE FROM artifact_owner_release_outbox")


def test_retention_rules_apply_strict_owner_object_and_tier_boundaries(
    tmp_path: Path,
) -> None:
    rule_type = getattr(artifact_retention_module, "RetentionRule", None)
    assert rule_type is not None, "per-owner retention rules are missing"
    store = _store_with_copies(tmp_path / "references.sqlite3")
    reference = ObjectReference(
        owner_type="snapshot",
        owner_id="snapshot-late-revision",
        content_sha256=HASH_A,
        created_at=NOW - timedelta(days=150),
    )
    store.register_reference(reference)
    receipt_type = artifact_retention_module.OwnerTerminalReleaseReceipt
    store.release_owner_terminal(
        receipt_type(
            reference_id=reference.reference_id,
            owner_type=reference.owner_type,
            owner_id=reference.owner_id,
            content_sha256=reference.content_sha256,
            terminal_state="retired",
            lifecycle_revision=4,
            evidence_sha256="d" * 64,
            released_at=NOW - timedelta(days=121),
        )
    )
    policy = _policy().model_copy(
        update={
            "rules": (
                rule_type(
                    owner_type="snapshot",
                    object_kind="research_snapshot",
                    storage_tier=StorageTier.HOT,
                    minimum_age=timedelta(days=120),
                ),
            )
        }
    )

    assert store.plan_gc(now=NOW - timedelta(microseconds=1), policy=policy).candidates == ()
    assert len(store.plan_gc(now=NOW, policy=policy).candidates) == 1
