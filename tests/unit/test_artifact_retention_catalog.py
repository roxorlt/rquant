from __future__ import annotations

import multiprocessing
import os
import queue
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.artifact_retention as artifact_retention_module
from rquant.artifact_retention import (
    ArtifactBundleRegistration,
    ArtifactReferenceStore,
    ArtifactTierMigrationCoordinator,
    ObjectCopy,
    ObjectCopyVerification,
    ObjectIdentity,
    ObjectReference,
    RetentionPolicy,
    StorageTier,
)

NOW = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
CONTENT_HASH = "a" * 64
MIGRATION_SCHEMA_SHA256 = "9" * 64


def _writer_credential(
    *,
    secret_hex: str = "1" * 64,
    key_id: str = "retention-writer-1",
    sequence: int = 1,
    not_before: datetime = NOW - timedelta(minutes=1),
    expires_at: datetime = NOW + timedelta(hours=1),
    revoked_at: datetime | None = None,
    previous_secret_hex: str | None = None,
) -> artifact_retention_module.ArtifactRetentionWriterCredential:
    return artifact_retention_module.ArtifactRetentionWriterCredential(
        key_id=key_id,
        sequence=sequence,
        secret_hex=secret_hex,
        previous_secret_hex=previous_secret_hex,
        not_before=not_before,
        expires_at=expires_at,
        revoked_at=revoked_at,
    )


def _hold_catalog_writer(
    path: str,
    root: str,
    credential_payload: dict[str, object],
    ready: object,
    release: object,
) -> None:
    store = ArtifactReferenceStore(
        Path(path),
        managed_trust_root=Path(root),
        writer_owner="catalog-process",
        retention_writer_credential=(
            artifact_retention_module.ArtifactRetentionWriterCredential.model_validate(
                credential_payload
            )
        ),
        clock=lambda: NOW,
    )
    with store._writer():
        ready.set()
        release.wait(10)


def _acquire_retention_writer(
    path: str,
    root: str,
    credential_payload: dict[str, object],
    result: object,
) -> None:
    try:
        store = ArtifactReferenceStore(
            Path(path),
            managed_trust_root=Path(root),
            writer_owner="retention-process",
            retention_writer_credential=(
                artifact_retention_module.ArtifactRetentionWriterCredential.model_validate(
                    credential_payload
                )
            ),
            clock=lambda: NOW,
        )
        started = time.monotonic()
        with store._writer():
            result.put(("written", time.monotonic() - started))
    except Exception as exc:
        result.put((type(exc).__name__, str(exc)))


def _registration() -> ArtifactBundleRegistration:
    identity = ObjectIdentity(
        content_sha256=CONTENT_HASH,
        size_bytes=128,
        object_kind="strategy_lab_shard_result_bundle",
        created_at=NOW,
    )
    copy = ObjectCopy(
        content_sha256=CONTENT_HASH,
        location_id="lab-hot",
        storage_uri="file:///lab/hot/bundle",
        storage_tier=StorageTier.HOT,
        verified_at=NOW,
        failure_domain="macbook-primary",
        tier_entered_at=NOW,
    )
    references = tuple(
        ObjectReference(
            owner_type=owner_type,
            owner_id=owner_id,
            content_sha256=CONTENT_HASH,
            created_at=NOW,
        )
        for owner_type, owner_id in (
            ("audit", "b" * 64),
            ("experiment", "c" * 64),
            ("job", "11111111-1111-4111-8111-111111111111"),
            ("snapshot", "d" * 64),
        )
    )
    return ArtifactBundleRegistration(
        object_identity=identity,
        object_copy=copy,
        references=references,
    )


def _registration_with_owner_drift(
    registration: ArtifactBundleRegistration,
    *,
    owner_type: str,
    owner_id: str,
) -> ArtifactBundleRegistration:
    return registration.model_copy(
        update={
            "references": tuple(
                ObjectReference(
                    owner_type=reference.owner_type,
                    owner_id=(
                        owner_id if reference.owner_type == owner_type else reference.owner_id
                    ),
                    content_sha256=reference.content_sha256,
                    created_at=reference.created_at,
                )
                for reference in registration.references
            )
        }
    )


def _policy() -> RetentionPolicy:
    return RetentionPolicy(
        hot_min_age=timedelta(0),
        warm_min_age=timedelta(0),
        cold_min_age=timedelta(0),
        minimum_verified_copies=1,
        verification_max_age=timedelta(days=1),
        plan_ttl=timedelta(minutes=10),
        claim_ttl=timedelta(minutes=2),
    )


def _reference_store(
    managed_trust_root: Path,
    path: Path | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
) -> ArtifactReferenceStore:
    return ArtifactReferenceStore(
        path or managed_trust_root / "references.sqlite3",
        managed_trust_root=managed_trust_root,
        clock=clock,
    )


def test_reference_store_process_fence_serializes_writers_and_recovers_after_crash(
    tmp_path: Path,
) -> None:
    path = tmp_path / "references.sqlite3"
    credential = _writer_credential()
    ArtifactReferenceStore(
        path,
        managed_trust_root=tmp_path,
        retention_writer_credential=credential,
        clock=lambda: NOW,
    ).close()
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    result = context.Queue()
    holder = context.Process(
        target=_hold_catalog_writer,
        args=(
            str(path),
            str(tmp_path),
            credential.model_dump(mode="python"),
            ready,
            release,
        ),
    )
    contender = context.Process(
        target=_acquire_retention_writer,
        args=(
            str(path),
            str(tmp_path),
            credential.model_dump(mode="python"),
            result,
        ),
    )
    holder.start()
    assert ready.wait(5)
    contender.start()
    with pytest.raises(queue.Empty):
        result.get(timeout=0.25)

    holder.terminate()
    holder.join(timeout=5)
    contender.join(timeout=5)

    assert contender.exitcode == 0
    outcome, elapsed = result.get(timeout=1)
    assert outcome == "written"
    assert elapsed >= 0
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT fence, owner_label, lease_token FROM artifact_writer_fence WHERE singleton = 1"
        ).fetchone()
    assert row is not None
    assert row[0] >= 1
    assert row[1] == "artifact-metadata-service/v1"
    assert len(row[2]) == 32


def test_retention_writer_rejects_label_takeover_and_old_credential_after_rotation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "references.sqlite3"
    original = _writer_credential(not_before=NOW - timedelta(minutes=5))
    correct = ArtifactReferenceStore(
        path,
        managed_trust_root=tmp_path,
        writer_owner="arbitrary-original-label",
        retention_writer_credential=original,
        clock=lambda: NOW,
    )
    with correct._writer():
        pass

    forged_credential = _writer_credential(
        secret_hex="f" * 64,
        key_id="artifact-retention-forged",
        sequence=2,
        not_before=NOW - timedelta(minutes=2),
    )
    with pytest.raises(ValueError, match="rotation|previous|credential"):
        ArtifactReferenceStore(
            path,
            managed_trust_root=tmp_path,
            writer_owner="artifact-retention",
            retention_writer_credential=forged_credential,
            clock=lambda: NOW,
        )

    rotated_credential = _writer_credential(
        secret_hex="3" * 64,
        key_id="artifact-retention-writer-v2",
        sequence=2,
        previous_secret_hex="1" * 64,
    )
    rotated = ArtifactReferenceStore(
        path,
        managed_trust_root=tmp_path,
        writer_owner="different-arbitrary-label",
        retention_writer_credential=rotated_credential,
        clock=lambda: NOW,
    )
    with rotated._writer():
        pass

    with pytest.raises(ValueError, match="old|rotated|credential"):
        ArtifactReferenceStore(
            path,
            managed_trust_root=tmp_path,
            writer_owner="artifact-retention",
            retention_writer_credential=original,
            clock=lambda: NOW,
        )

    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT fence, owner_label FROM artifact_writer_fence WHERE singleton = 1"
        ).fetchone()
    assert row == (2, "artifact-metadata-service/v1")


@pytest.mark.parametrize("state", ("expired", "revoked"))
def test_retention_writer_fails_closed_for_expired_or_revoked_credential(
    tmp_path: Path,
    state: str,
) -> None:
    credential = _writer_credential(
        expires_at=(NOW if state == "expired" else NOW + timedelta(hours=1)),
        revoked_at=(NOW - timedelta(seconds=1) if state == "revoked" else None),
    )

    with pytest.raises(ValueError, match=state):
        ArtifactReferenceStore(
            tmp_path / "references.sqlite3",
            managed_trust_root=tmp_path,
            writer_owner="artifact-retention",
            retention_writer_credential=credential,
            clock=lambda: NOW,
        )


def test_retention_writer_requires_capability_not_owner_label(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="capability|credential"):
        ArtifactReferenceStore(
            tmp_path / "references.sqlite3",
            managed_trust_root=tmp_path,
            writer_owner="artifact-retention",
            clock=lambda: NOW,
        )


def test_real_concurrent_cross_process_writer_race_accepts_only_provisioned_credential(
    tmp_path: Path,
) -> None:
    path = tmp_path / "references.sqlite3"
    credential = _writer_credential()
    provisioned = ArtifactReferenceStore(
        path,
        managed_trust_root=tmp_path,
        retention_writer_credential=credential,
        clock=lambda: NOW,
    )
    with provisioned._writer():
        pass
    context = multiprocessing.get_context("spawn")
    result = context.Queue()
    forged_credential = _writer_credential(
        secret_hex="f" * 64,
        key_id="artifact-retention-forged",
    )
    contenders = (
        context.Process(
            target=_acquire_retention_writer,
            args=(
                str(path),
                str(tmp_path),
                credential.model_dump(mode="python"),
                result,
            ),
        ),
        context.Process(
            target=_acquire_retention_writer,
            args=(
                str(path),
                str(tmp_path),
                forged_credential.model_dump(mode="python"),
                result,
            ),
        ),
    )
    for contender in contenders:
        contender.start()
    for contender in contenders:
        contender.join(timeout=10)
        assert contender.exitcode == 0

    observed = sorted((result.get(timeout=1) for _ in contenders), key=lambda item: item[0])
    outcomes = [item[0] for item in observed]
    assert outcomes == ["ArtifactRetentionWriterAuthorizationError", "written"], observed
    rejection = str(observed[0][1])
    assert "rotation" in rejection or "credential" in rejection, observed


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


def _query_plan_details(
    store: ArtifactReferenceStore,
    sql: str,
    parameters: tuple[object, ...],
) -> tuple[str, ...]:
    with store._reader() as connection:
        rows = connection.execute(
            f"EXPLAIN QUERY PLAN {sql}",
            parameters,
        ).fetchall()
    return tuple(str(row[3]) for row in rows)


@pytest.mark.parametrize(
    ("table", "index_name", "sql", "parameters"),
    [
        (
            "artifact_reference",
            "artifact_reference_content_owner_status_idx",
            """
            SELECT * FROM artifact_reference
            WHERE content_sha256 = ? AND owner_type = ?
            ORDER BY reference_id
            """,
            (CONTENT_HASH, "job"),
        ),
        (
            "artifact_reference",
            "artifact_reference_content_status_expiry_idx",
            """
            SELECT 1 FROM artifact_reference
            WHERE content_sha256 = ? AND released_at IS NULL
              AND (expires_at IS NULL OR expires_at > ?)
            LIMIT 1
            """,
            (CONTENT_HASH, NOW.isoformat()),
        ),
        (
            "artifact_legal_hold",
            "artifact_legal_hold_content_status_idx",
            """
            SELECT 1 FROM artifact_legal_hold
            WHERE content_sha256 = ? AND released_at IS NULL
            LIMIT 1
            """,
            (CONTENT_HASH,),
        ),
        (
            "artifact_gc_claim",
            "artifact_gc_claim_content_status_idx",
            """
            SELECT 1 FROM artifact_gc_claim
            WHERE content_sha256 = ? AND status = 'claimed'
            LIMIT 1
            """,
            (CONTENT_HASH,),
        ),
    ],
)
def test_retention_owner_and_status_lookups_use_content_first_indexes(
    tmp_path: Path,
    table: str,
    index_name: str,
    sql: str,
    parameters: tuple[object, ...],
) -> None:
    details = _query_plan_details(_reference_store(tmp_path), sql, parameters)

    assert any(index_name in detail for detail in details)
    assert all(f"SCAN {table}" not in detail for detail in details)


def test_gc_planning_uses_keyset_pages_and_stops_at_requested_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    object_count = 2_000
    object_rows = tuple(
        (f"{index:064x}", 128, "research_snapshot", NOW.isoformat())
        for index in range(object_count)
    )
    copy_rows = tuple(
        (
            content_sha256,
            f"location-{copy_index}",
            f"s3://research/{content_sha256}/{copy_index}",
            StorageTier.HOT.value,
            NOW.isoformat(),
            f"domain-{copy_index}",
            NOW.isoformat(),
        )
        for content_sha256, *_rest in object_rows
        for copy_index in range(2)
    )
    with store._writer() as connection:
        connection.executemany(
            """
            INSERT INTO artifact_object(
                content_sha256, size_bytes, object_kind, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            object_rows,
        )
        connection.executemany(
            """
            INSERT INTO artifact_copy(
                content_sha256, location_id, storage_uri, storage_tier,
                verified_at, failure_domain, tier_entered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            copy_rows,
        )

    select_statements: list[str] = []
    original_reader = store._reader

    class CountingConnection:
        def __init__(self, connection: object) -> None:
            self._connection = connection

        def execute(
            self,
            sql: str,
            parameters: tuple[object, ...] = (),
        ) -> object:
            if sql.lstrip().upper().startswith(("SELECT", "WITH")):
                select_statements.append(sql)
            return self._connection.execute(sql, parameters)  # type: ignore[attr-defined]

    @contextmanager
    def counted_reader() -> Iterator[object]:
        with original_reader() as connection:
            yield CountingConnection(connection)

    monkeypatch.setattr(store, "_reader", counted_reader)

    plan = store.plan_gc(
        now=NOW,
        policy=_policy(),
        max_items=25,
        max_bytes=25 * 128,
        max_runtime=timedelta(seconds=5),
    )

    assert len(plan.candidates) == 25
    assert sum(item.object_identity.size_bytes for item in plan.candidates) <= 25 * 128
    assert 1 <= len(select_statements) <= 4
    assert all("LIMIT" in statement.upper() for statement in select_statements[1:])


def test_gc_planning_defers_oversize_candidate_without_starving_smaller_work(
    tmp_path: Path,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    object_rows = (
        ("1" * 64, 26, "oversize-result", NOW.isoformat()),
        ("2" * 64, 10, "small-result", NOW.isoformat()),
    )
    copy_rows = tuple(
        (
            content_sha256,
            f"location-{copy_index}",
            f"s3://research/{content_sha256}/{copy_index}",
            StorageTier.HOT.value,
            NOW.isoformat(),
            f"domain-{copy_index}",
            NOW.isoformat(),
        )
        for content_sha256, *_rest in object_rows
        for copy_index in range(2)
    )
    with store._writer() as connection:
        connection.executemany(
            """
            INSERT INTO artifact_object(
                content_sha256, size_bytes, object_kind, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            object_rows,
        )
        connection.executemany(
            """
            INSERT INTO artifact_copy(
                content_sha256, location_id, storage_uri, storage_tier,
                verified_at, failure_domain, tier_entered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            copy_rows,
        )

    plan = store.plan_gc(
        now=NOW,
        policy=_policy(),
        max_items=1,
        max_bytes=25,
        max_runtime=timedelta(seconds=5),
    )

    assert [item.object_identity.size_bytes for item in plan.candidates] == [10]
    assert len(plan.deferred_candidates) == 1
    assert plan.deferred_candidates[0].candidate.object_identity.size_bytes == 26
    assert plan.deferred_candidates[0].reason == "byte_budget_exceeded"


def test_gc_planning_honors_caller_absolute_monotonic_deadline(tmp_path: Path) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)

    plan = store.plan_gc(
        now=NOW,
        policy=_policy(),
        max_runtime=timedelta(minutes=1),
        deadline_monotonic=5.0,
        monotonic=lambda: 5.0,
    )

    assert plan.candidates == plan.deferred_candidates == ()


class _CloseProbeConnection:
    def __init__(
        self,
        events: list[str],
        *,
        close_error: BaseException | None = None,
    ) -> None:
        self.events = events
        self.close_error = close_error
        self.closed = False
        self.in_transaction = False
        self.row_factory: object | None = None

    def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> _CloseProbeConnection:
        del parameters
        statement = sql.strip().upper()
        if statement.startswith("BEGIN"):
            self.in_transaction = True
        elif statement in {"COMMIT", "ROLLBACK"}:
            self.in_transaction = False
        return self

    def executescript(self, sql: str) -> _CloseProbeConnection:
        del sql
        return self

    def fetchone(self) -> dict[str, int]:
        return {"fence": 0}

    def close(self) -> None:
        self.events.append("close")
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class _CtimeShiftedStat:
    def __init__(self, observed: os.stat_result, *, offset: int = 1) -> None:
        self._observed = observed
        self.st_ctime_ns = observed.st_ctime_ns + offset

    def __getattr__(self, name: str) -> object:
        return getattr(self._observed, name)


@pytest.mark.parametrize("target_kind", ["trust-root", "ancestor", "database"])
def test_sqlite_path_authority_binds_ctime_for_every_identity_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_kind: str,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "references.sqlite3"
    path.touch(mode=0o600)
    authority = artifact_retention_module.PrivateSqlitePathAuthority(
        path,
        label="ctime probe",
        create_if_missing=False,
        managed_trust_root=tmp_path,
    )
    target = {
        "trust-root": tmp_path,
        "ancestor": private,
        "database": path,
    }[target_kind]
    real_lstat = artifact_retention_module.os.lstat

    def shifted_lstat(candidate: object) -> os.stat_result:
        observed = real_lstat(candidate)
        if Path(candidate) == target:
            return _CtimeShiftedStat(observed)  # type: ignore[return-value]
        return observed

    monkeypatch.setattr(artifact_retention_module.os, "lstat", shifted_lstat)

    with pytest.raises(ValueError, match="identity|changed"):
        authority.assert_current()


def test_sqlite_path_authority_rebind_ignores_ctime_churn_above_managed_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed_root = tmp_path / "managed"
    managed_root.mkdir(mode=0o700)
    path = managed_root / "authority.sqlite3"
    path.touch(mode=0o600)
    authority = artifact_retention_module.PrivateSqlitePathAuthority(
        path,
        label="shared ancestor probe",
        create_if_missing=False,
        managed_trust_root=managed_root,
    )
    real_lstat = artifact_retention_module.os.lstat
    calls = 0

    def churn_shared_ancestor(candidate: object) -> os.stat_result:
        nonlocal calls
        observed = real_lstat(candidate)
        if Path(candidate) == tmp_path:
            calls += 1
            return _CtimeShiftedStat(observed, offset=calls)  # type: ignore[return-value]
        return observed

    monkeypatch.setattr(artifact_retention_module.os, "lstat", churn_shared_ancestor)

    authority.rebind_and_assert_current_after_trusted_sqlite_change()

    assert calls >= 2


def test_bundle_registration_is_atomic_and_restart_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "references.sqlite3"
    store = _reference_store(tmp_path, path, clock=lambda: NOW)

    first = store.register_bundle_atomic(_registration())
    restarted = _reference_store(
        tmp_path,
        path,
        clock=lambda: NOW + timedelta(hours=1),
    )
    second = restarted.register_bundle_atomic(_registration())

    assert first.registered_objects == 1
    assert first.registered_copies == 1
    assert first.registered_references == 4
    assert second.registered_objects == 0
    assert second.registered_copies == 0
    assert second.registered_references == 0
    assert {item.owner_type for item in store.list_active_references(CONTENT_HASH)} == {
        "audit",
        "experiment",
        "job",
        "snapshot",
    }


def test_bundle_retry_keeps_first_registration_timestamps(tmp_path: Path) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    initial = _registration()
    store.register_bundle_atomic(initial)
    later = NOW + timedelta(days=1)
    retry = ArtifactBundleRegistration(
        object_identity=initial.object_identity.model_copy(update={"created_at": later}),
        object_copy=initial.object_copy.model_copy(
            update={"verified_at": later, "tier_entered_at": later}
        ),
        references=tuple(
            ObjectReference(
                owner_type=reference.owner_type,
                owner_id=reference.owner_id,
                content_sha256=reference.content_sha256,
                created_at=later,
            )
            for reference in initial.references
        ),
    )

    counts = store.register_bundle_atomic(retry)

    assert counts.registered_objects == 0
    assert counts.registered_copies == 0
    assert counts.registered_references == 0
    assert store.get_object(CONTENT_HASH).created_at == NOW
    assert store.list_active_copies(CONTENT_HASH)[0].verified_at == NOW
    assert store.list_active_copies(CONTENT_HASH)[0].tier_entered_at == NOW
    assert {item.created_at for item in store.list_active_references(CONTENT_HASH)} == {NOW}


@pytest.mark.parametrize(
    ("component", "update"),
    [
        ("object", {"size_bytes": 129}),
        ("object", {"object_kind": "different-kind"}),
        ("copy", {"storage_uri": "file:///different/bundle"}),
        ("copy", {"storage_tier": StorageTier.COLD}),
        ("copy", {"failure_domain": "different-domain"}),
    ],
)
def test_bundle_retry_rejects_conflicting_immutable_metadata(
    tmp_path: Path,
    component: str,
    update: dict[str, object],
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    initial = _registration()
    store.register_bundle_atomic(initial)
    conflicting = initial.model_copy(
        update={
            "object_identity": (
                initial.object_identity.model_copy(update=update)
                if component == "object"
                else initial.object_identity
            ),
            "object_copy": (
                initial.object_copy.model_copy(update=update)
                if component == "copy"
                else initial.object_copy
            ),
        }
    )

    with pytest.raises(ValueError, match="conflicting"):
        store.register_bundle_atomic(conflicting)

    assert store.get_object(CONTENT_HASH) == initial.object_identity
    assert store.list_active_copies(CONTENT_HASH) == (initial.object_copy,)


def test_bundle_retry_rejects_owner_drift_and_preserves_original_audit(
    tmp_path: Path,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    initial = _registration()
    store.register_bundle_atomic(initial)
    references_before = store.list_active_references(CONTENT_HASH)
    audit_before = store.list_audit_events()
    drifted = _registration_with_owner_drift(
        initial,
        owner_type="job",
        owner_id="99999999-9999-4999-8999-999999999999",
    )

    with pytest.raises(ValueError, match="owner|conflicting"):
        store.register_bundle_atomic(drifted)

    assert store.list_active_references(CONTENT_HASH) == references_before
    assert store.list_audit_events() == audit_before


def test_concurrent_bundle_retries_cannot_append_drifted_owner(
    tmp_path: Path,
) -> None:
    path = tmp_path / "references.sqlite3"
    initial_store = _reference_store(tmp_path, path, clock=lambda: NOW)
    initial = _registration()
    initial_store.register_bundle_atomic(initial)
    references_before = initial_store.list_active_references(CONTENT_HASH)
    audit_before = initial_store.list_audit_events()
    same_store = _reference_store(
        tmp_path,
        path,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    drift_store = _reference_store(
        tmp_path,
        path,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    drifted = _registration_with_owner_drift(
        initial,
        owner_type="snapshot",
        owner_id="f" * 64,
    )
    start = threading.Barrier(3)
    outcomes: list[tuple[str, object]] = []
    outcome_lock = threading.Lock()

    def retry(
        label: str,
        store: ArtifactReferenceStore,
        registration: ArtifactBundleRegistration,
    ) -> None:
        start.wait(timeout=5)
        try:
            result: object = store.register_bundle_atomic(registration)
        except BaseException as exc:
            result = exc
        with outcome_lock:
            outcomes.append((label, result))

    same_thread = threading.Thread(target=retry, args=("same", same_store, initial))
    drift_thread = threading.Thread(target=retry, args=("drift", drift_store, drifted))
    same_thread.start()
    drift_thread.start()
    start.wait(timeout=5)
    same_thread.join(timeout=5)
    drift_thread.join(timeout=5)

    assert not same_thread.is_alive() and not drift_thread.is_alive()
    by_label = dict(outcomes)
    assert not isinstance(by_label["same"], BaseException)
    assert isinstance(by_label["drift"], ValueError)
    assert initial_store.list_active_references(CONTENT_HASH) == references_before
    assert initial_store.list_audit_events() == audit_before


def test_bundle_registration_rejects_conflicting_existing_reference_metadata(
    tmp_path: Path,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_object(registration.object_identity)
    store.register_copy(registration.object_copy)
    audit = registration.references[0]
    store.register_reference(
        ObjectReference(
            owner_type=audit.owner_type,
            owner_id=audit.owner_id,
            content_sha256=audit.content_sha256,
            created_at=NOW,
            expires_at=NOW + timedelta(days=1),
        )
    )

    with pytest.raises(ValueError, match="conflicting reference metadata"):
        store.register_bundle_atomic(registration)

    assert {item.owner_type for item in store.list_active_references(CONTENT_HASH)} == {"audit"}


def test_reference_store_rejects_relative_sqlite_path(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exact absolute"):
        ArtifactReferenceStore(
            Path("relative/references.sqlite3"),
            managed_trust_root=tmp_path,
        )


def test_reference_store_rejects_non_normalized_absolute_path(tmp_path: Path) -> None:
    path = Path(f"{tmp_path}/nested/../references.sqlite3")

    with pytest.raises(ValueError, match="exact absolute"):
        ArtifactReferenceStore(path, managed_trust_root=tmp_path)


def test_reference_store_checks_every_descendant_of_explicit_trust_root(
    tmp_path: Path,
) -> None:
    managed_root = tmp_path / "managed"
    managed_root.mkdir(mode=0o700)
    unsafe = managed_root / "unsafe"
    unsafe.mkdir(mode=0o755)
    private = unsafe / "private"
    private.mkdir(mode=0o700)

    with pytest.raises(ValueError, match="parent owner or mode is unsafe"):
        ArtifactReferenceStore(
            private / "references.sqlite3",
            managed_trust_root=managed_root,
        )


def test_reference_store_initialization_verifies_before_and_after_native_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    connection = _CloseProbeConnection(events)

    def return_probe(
        _authority: artifact_retention_module.PrivateSqlitePathAuthority,
        _opener: Callable[[Path], object],
    ) -> _CloseProbeConnection:
        return connection

    def record_identity(
        _authority: artifact_retention_module.PrivateSqlitePathAuthority,
    ) -> None:
        events.append("assert")

    monkeypatch.setattr(
        artifact_retention_module.PrivateSqlitePathAuthority,
        "open_verified_connection",
        return_probe,
    )
    monkeypatch.setattr(
        artifact_retention_module.PrivateSqlitePathAuthority,
        "assert_current",
        record_identity,
    )

    ArtifactReferenceStore(
        tmp_path / "references.sqlite3",
        managed_trust_root=tmp_path,
    )

    assert events[-3:] == ["assert", "close", "assert"]


@pytest.mark.parametrize("context_name", ["reader", "writer"])
def test_reference_store_context_verifies_before_and_after_native_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    context_name: str,
) -> None:
    store = _reference_store(tmp_path)
    events: list[str] = []
    connection = _CloseProbeConnection(events)

    def record_identity() -> None:
        events.append("assert")

    monkeypatch.setattr(store, "_connect", lambda **_kwargs: connection)
    monkeypatch.setattr(store._path_authority, "assert_current", record_identity)
    context = store._reader() if context_name == "reader" else store._writer()

    with context:
        pass

    assert events[-3:] == ["assert", "close", "assert"]


def test_reference_store_close_preserves_close_and_postcheck_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _reference_store(tmp_path)
    events: list[str] = []
    close_error = OSError("reference connection close failed")
    postcheck_error = ValueError("reference path changed after close")
    connection = _CloseProbeConnection(events, close_error=close_error)

    def fail_after_close() -> None:
        events.append("assert")
        if connection.closed:
            raise postcheck_error

    monkeypatch.setattr(store, "_connect", lambda **_kwargs: connection)
    monkeypatch.setattr(store._path_authority, "assert_current", fail_after_close)

    with pytest.raises(BaseExceptionGroup) as captured, store._reader():
        pass

    assert captured.value.exceptions == (close_error, postcheck_error)
    assert events[-3:] == ["assert", "close", "assert"]


def test_verified_close_preserves_precheck_and_postcheck_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _reference_store(tmp_path)
    events: list[str] = []
    connection = _CloseProbeConnection(events)
    precheck_error = ValueError("pre-close identity changed")
    postcheck_error = ValueError("post-close identity changed")
    calls = 0

    def fail_both_identity_checks() -> None:
        nonlocal calls
        calls += 1
        events.append(f"assert-{calls}")
        raise precheck_error if calls == 1 else postcheck_error

    monkeypatch.setattr(store._path_authority, "assert_current", fail_both_identity_checks)

    with pytest.raises(BaseExceptionGroup) as captured:
        artifact_retention_module.close_verified_sqlite_connection(
            connection,
            store._path_authority,
        )

    assert captured.value.exceptions == (precheck_error, postcheck_error)
    assert events == ["assert-1", "close", "assert-2"]


@pytest.mark.parametrize("context_name", ["reader", "writer"])
def test_reference_store_preserves_business_error_and_all_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    context_name: str,
) -> None:
    store = _reference_store(tmp_path)
    events: list[str] = []
    business_error = RuntimeError(f"{context_name} business failed")
    precheck_error = ValueError("pre-close identity changed")
    close_error = OSError("connection close failed")
    postcheck_error = ValueError("post-close identity changed")
    connection = _CloseProbeConnection(events, close_error=close_error)
    cleanup_armed = False

    def fail_cleanup_checks() -> None:
        events.append("assert")
        if cleanup_armed:
            raise precheck_error if not connection.closed else postcheck_error

    monkeypatch.setattr(store, "_connect", lambda **_kwargs: connection)
    monkeypatch.setattr(store._path_authority, "assert_current", fail_cleanup_checks)
    context = store._reader() if context_name == "reader" else store._writer()

    with pytest.raises(BaseExceptionGroup) as captured, context:
        cleanup_armed = True
        connection.in_transaction = False
        raise business_error

    assert captured.value.exceptions == (
        business_error,
        precheck_error,
        close_error,
        postcheck_error,
    )
    assert connection.closed


def test_reference_store_initialization_routes_schema_error_into_verified_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    business_error = RuntimeError("artifact schema initialization failed")
    connection = _CloseProbeConnection([])
    observed_primary: list[BaseException | None] = []

    def fail_schema(_sql: str) -> _CloseProbeConnection:
        raise business_error

    def return_connection(
        _authority: artifact_retention_module.PrivateSqlitePathAuthority,
        _opener: Callable[[Path], object],
    ) -> _CloseProbeConnection:
        return connection

    def close_with_primary(
        candidate: object,
        _authority: artifact_retention_module.PrivateSqlitePathAuthority,
        *,
        primary_error: BaseException | None = None,
        known_identity_failure: bool = False,
    ) -> None:
        del known_identity_failure
        observed_primary.append(primary_error)
        candidate.close()  # type: ignore[attr-defined]

    connection.executescript = fail_schema  # type: ignore[method-assign]
    monkeypatch.setattr(
        artifact_retention_module.PrivateSqlitePathAuthority,
        "open_verified_connection",
        return_connection,
    )
    monkeypatch.setattr(
        artifact_retention_module.PrivateSqlitePathAuthority,
        "rebind_ctime_after_trusted_sqlite_setup",
        lambda _authority: None,
    )
    monkeypatch.setattr(
        artifact_retention_module,
        "close_verified_sqlite_connection",
        close_with_primary,
    )

    with pytest.raises(RuntimeError, match="schema initialization"):
        ArtifactReferenceStore(
            tmp_path / "references.sqlite3",
            managed_trust_root=tmp_path,
        )

    assert observed_primary == [business_error]
    assert connection.closed


def test_reference_store_uses_effective_uid_consistently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        artifact_retention_module.os,
        "getuid",
        lambda: os.geteuid() + 1,
    )

    store = _reference_store(tmp_path)

    assert store.path.exists()


def test_path_authority_open_failure_runs_verified_close_and_preserves_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "authority.sqlite3"
    path.touch(mode=0o600)
    authority = artifact_retention_module.PrivateSqlitePathAuthority(
        path,
        label="open probe",
        create_if_missing=False,
        managed_trust_root=tmp_path,
    )
    events: list[str] = []
    open_assert_error = ValueError("path changed immediately after open")
    close_error = OSError("opened connection close failed")
    postcheck_error = ValueError("path remained changed after close")
    connection = _CloseProbeConnection(events, close_error=close_error)
    assert_calls = 0

    def assert_identity() -> None:
        nonlocal assert_calls
        assert_calls += 1
        events.append(f"assert-{assert_calls}")
        if assert_calls == 2:
            raise open_assert_error
        if assert_calls == 3:
            raise ValueError("known preclose identity failure")
        if assert_calls == 4:
            raise postcheck_error

    def open_connection(_path: Path) -> _CloseProbeConnection:
        events.append("open")
        return connection

    monkeypatch.setattr(authority, "assert_current", assert_identity)

    with pytest.raises(BaseExceptionGroup) as captured:
        authority.open_verified_connection(open_connection)

    assert captured.value.exceptions == (
        open_assert_error,
        close_error,
        postcheck_error,
    )
    assert events == [
        "assert-1",
        "open",
        "assert-2",
        "assert-3",
        "close",
        "assert-4",
    ]
    assert connection.closed


def test_reference_store_connect_preserves_pragma_and_postcheck_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _reference_store(tmp_path)
    events: list[str] = []
    setup_error = RuntimeError("foreign_keys PRAGMA failed")
    postcheck_error = ValueError("reference path changed after setup close")
    connection = _CloseProbeConnection(events)

    def fail_pragma(sql: str, parameters: tuple[object, ...] = ()) -> _CloseProbeConnection:
        del sql, parameters
        events.append("pragma")
        raise setup_error

    def assert_identity() -> None:
        events.append("assert")
        if connection.closed:
            raise postcheck_error

    connection.execute = fail_pragma  # type: ignore[method-assign]
    monkeypatch.setattr(
        store._path_authority,
        "open_verified_connection",
        lambda _opener: connection,
    )
    monkeypatch.setattr(store._path_authority, "assert_current", assert_identity)

    with pytest.raises(BaseExceptionGroup) as captured:
        store._connect()

    assert captured.value.exceptions == (setup_error, postcheck_error)
    assert events == ["pragma", "assert", "close", "assert"]
    assert connection.closed


def test_reference_store_connect_closes_after_final_identity_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _reference_store(tmp_path)
    events: list[str] = []
    setup_identity_error = ValueError("reference path changed after PRAGMAs")
    postcheck_error = ValueError("reference path remained changed after close")
    connection = _CloseProbeConnection(events)
    assert_calls = 0

    def assert_identity() -> None:
        nonlocal assert_calls
        assert_calls += 1
        events.append(f"assert-{assert_calls}")
        if assert_calls == 1:
            raise setup_identity_error
        if assert_calls == 2:
            raise ValueError("known preclose identity failure")
        if assert_calls == 3:
            raise postcheck_error

    monkeypatch.setattr(
        store._path_authority,
        "open_verified_connection",
        lambda _opener: connection,
    )
    monkeypatch.setattr(store._path_authority, "assert_current", assert_identity)

    with pytest.raises(BaseExceptionGroup) as captured:
        store._connect()

    assert captured.value.exceptions == (setup_identity_error, postcheck_error)
    assert events[-4:] == ["assert-1", "assert-2", "close", "assert-3"]
    assert connection.closed


def test_reference_store_requires_current_uid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "references.sqlite3"
    path.touch(mode=0o600)
    current_uid = os.geteuid()
    monkeypatch.setattr(artifact_retention_module.os, "geteuid", lambda: current_uid + 1)

    with pytest.raises(ValueError, match="owner"):
        ArtifactReferenceStore(path, managed_trust_root=tmp_path)


@pytest.mark.parametrize("hazard", ["parent-symlink", "final-symlink", "hardlink", "mode"])
def test_reference_store_rejects_unsafe_sqlite_path(tmp_path: Path, hazard: str) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "references.sqlite3"
    if hazard == "parent-symlink":
        linked = tmp_path / "linked"
        linked.symlink_to(private, target_is_directory=True)
        path = linked / path.name
    else:
        seed = private / "seed.sqlite3"
        seed.touch(mode=0o600)
        if hazard == "final-symlink":
            path.symlink_to(seed)
        elif hazard == "hardlink":
            os.link(seed, path)
        else:
            path.touch(mode=0o600)
            path.chmod(0o640)

    with pytest.raises(ValueError, match="symlink|hard link|mode|unsafe"):
        ArtifactReferenceStore(path, managed_trust_root=tmp_path)


def test_reference_store_rejects_private_parent_generation_swap(tmp_path: Path) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "references.sqlite3"
    store = ArtifactReferenceStore(path, managed_trust_root=tmp_path)
    retired = tmp_path / "retired"
    private.rename(retired)
    private.mkdir(mode=0o700)
    for suffix in ("", "-wal", "-shm"):
        source = Path(f"{retired / path.name}{suffix}")
        if source.exists():
            source.rename(Path(f"{path}{suffix}"))

    with pytest.raises(ValueError, match="parent.*changed|path identity"):
        store.list_audit_events()


def test_bundle_registration_rolls_back_every_row_on_reference_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    real_audit = store._audit

    def fail_during_reference_audit(*args: object, **kwargs: object) -> None:
        if kwargs.get("event_type") == "reference_registered":
            raise RuntimeError("injected reference audit failure")
        real_audit(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(store, "_audit", fail_during_reference_audit)

    with pytest.raises(RuntimeError, match="injected reference audit failure"):
        store.register_bundle_atomic(_registration())

    with pytest.raises(KeyError):
        store.get_object(CONTENT_HASH)
    assert store.list_audit_events() == ()


def test_concurrent_gc_cannot_observe_object_before_its_references_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    copy_inserted = threading.Event()
    allow_commit = threading.Event()
    real_audit = store._audit
    failures: list[BaseException] = []

    def pause_after_copy(*args: object, **kwargs: object) -> None:
        real_audit(*args, **kwargs)  # type: ignore[arg-type]
        if kwargs.get("event_type") == "copy_registered":
            copy_inserted.set()
            assert allow_commit.wait(timeout=5)

    def register() -> None:
        try:
            store.register_bundle_atomic(_registration())
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(store, "_audit", pause_after_copy)
    worker = threading.Thread(target=register)
    worker.start()
    assert copy_inserted.wait(timeout=5)

    with pytest.raises(KeyError):
        store.get_object(CONTENT_HASH)
    assert store.plan_gc(now=NOW, policy=_policy()).candidates == ()

    allow_commit.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert failures == []
    assert store.plan_gc(now=NOW, policy=_policy()).candidates == ()


def test_audit_reference_alone_protects_object_from_gc(tmp_path: Path) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_bundle_atomic(registration)
    references = store.list_active_references(CONTENT_HASH)
    for reference in references:
        if reference.owner_type != "audit":
            assert reference.reference_id is not None
            _release_terminal(store, reference, released_at=NOW)

    assert {item.owner_type for item in store.list_active_references(CONTENT_HASH)} == {"audit"}
    assert store.plan_gc(now=NOW, policy=_policy()).candidates == ()


class _CopyTransport:
    def __init__(self, verification: ObjectCopyVerification) -> None:
        self.verification = verification
        self.copied: list[tuple[str, str]] = []
        self.events: list[str] = []

    def copy(self, source_uri: str, target_uri: str) -> None:
        self.copied.append((source_uri, target_uri))
        self.events.append("copy")

    def durably_sync(self, storage_uri: str) -> None:
        assert storage_uri == self.verification.storage_uri
        self.events.append("fsync")

    def verify(self, storage_uri: str) -> ObjectCopyVerification:
        assert storage_uri == self.verification.storage_uri
        self.events.append("verify")
        return self.verification


def _migration_target(*, failure_domain: str = "cloud-az-b") -> ObjectCopy:
    return ObjectCopy(
        content_sha256=CONTENT_HASH,
        location_id="lab-warm",
        storage_uri="s3://research-warm/bundle",
        storage_tier=StorageTier.WARM,
        verified_at=NOW + timedelta(minutes=1),
        failure_domain=failure_domain,
        tier_entered_at=NOW + timedelta(minutes=1),
    )


def _verification(
    *,
    content_sha256: str = CONTENT_HASH,
    size_bytes: int = 128,
) -> ObjectCopyVerification:
    return ObjectCopyVerification(
        storage_uri="s3://research-warm/bundle",
        content_sha256=content_sha256,
        size_bytes=size_bytes,
        schema_sha256=MIGRATION_SCHEMA_SHA256,
        verified_at=NOW + timedelta(minutes=1),
    )


def test_tier_migration_copies_verifies_and_atomically_registers_target(
    tmp_path: Path,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_bundle_atomic(registration)
    transport = _CopyTransport(_verification())
    coordinator = ArtifactTierMigrationCoordinator(store=store, transport=transport)

    receipt = coordinator.migrate(
        source=registration.object_copy,
        target=_migration_target(),
        observed_at=NOW + timedelta(minutes=1),
        expected_schema_sha256=MIGRATION_SCHEMA_SHA256,
    )

    assert transport.copied == [
        (registration.object_copy.storage_uri, _migration_target().storage_uri)
    ]
    assert receipt.registered_target_copy is True
    assert receipt.retirement_plan.source_location_id == registration.object_copy.location_id
    assert receipt.retirement_plan.target_location_id == _migration_target().location_id
    assert receipt.retirement_plan.ledger_revision > 0
    assert {copy.location_id for copy in store.list_active_copies(CONTENT_HASH)} == {
        "lab-hot",
        "lab-warm",
    }
    assert {item.owner_type for item in store.list_active_references(CONTENT_HASH)} == {
        "audit",
        "experiment",
        "job",
        "snapshot",
    }


def test_tier_migration_requires_durable_schema_verified_copy_before_catalog_publish(
    tmp_path: Path,
) -> None:
    assert "schema_sha256" in ObjectCopyVerification.model_fields
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_bundle_atomic(registration)
    schema_sha256 = "9" * 64
    verification = _verification().model_copy(update={"schema_sha256": schema_sha256})
    transport = _CopyTransport(verification)
    coordinator = ArtifactTierMigrationCoordinator(store=store, transport=transport)

    receipt = coordinator.migrate(
        source=registration.object_copy,
        target=_migration_target(),
        observed_at=NOW + timedelta(minutes=1),
        expected_schema_sha256=schema_sha256,
    )

    assert transport.events == ["copy", "fsync", "verify"]
    assert receipt.verification.schema_sha256 == schema_sha256


def test_tier_migration_cas_rejects_catalog_change_during_copy(tmp_path: Path) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW + timedelta(minutes=1))
    registration = _registration()
    store.register_bundle_atomic(registration)
    transport = _CopyTransport(_verification())
    original_verify = transport.verify

    def verify_after_catalog_change(storage_uri: str) -> ObjectCopyVerification:
        store.register_reference(
            ObjectReference(
                owner_type="temporary",
                owner_id="late-recovery-manifest",
                content_sha256=CONTENT_HASH,
                created_at=NOW + timedelta(seconds=30),
            )
        )
        return original_verify(storage_uri)

    transport.verify = verify_after_catalog_change  # type: ignore[method-assign]
    coordinator = ArtifactTierMigrationCoordinator(store=store, transport=transport)

    with pytest.raises(ValueError, match="CAS revision changed"):
        coordinator.migrate(
            source=registration.object_copy,
            target=_migration_target(),
            observed_at=NOW + timedelta(minutes=1),
            expected_schema_sha256=MIGRATION_SCHEMA_SHA256,
        )

    assert transport.events == ["copy", "fsync", "verify"]
    assert store.list_active_copies(CONTENT_HASH) == (registration.object_copy,)


@pytest.mark.parametrize(
    ("verification", "message"),
    [
        (_verification(content_sha256="f" * 64), "hash"),
        (_verification(size_bytes=129), "size"),
    ],
)
def test_tier_migration_rejects_unverified_target_before_metadata_registration(
    tmp_path: Path,
    verification: ObjectCopyVerification,
    message: str,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_bundle_atomic(registration)
    coordinator = ArtifactTierMigrationCoordinator(
        store=store,
        transport=_CopyTransport(verification),
    )

    with pytest.raises(ValueError, match=message):
        coordinator.migrate(
            source=registration.object_copy,
            target=_migration_target(),
            observed_at=NOW + timedelta(minutes=1),
            expected_schema_sha256=MIGRATION_SCHEMA_SHA256,
        )

    assert store.list_active_copies(CONTENT_HASH) == (registration.object_copy,)


def test_tier_migration_requires_independent_domain_and_governed_durable_owners(
    tmp_path: Path,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_bundle_atomic(registration)
    coordinator = ArtifactTierMigrationCoordinator(
        store=store,
        transport=_CopyTransport(_verification()),
    )

    with pytest.raises(ValueError, match="failure domain"):
        coordinator.migrate(
            source=registration.object_copy,
            target=_migration_target(failure_domain=registration.object_copy.failure_domain),
            observed_at=NOW + timedelta(minutes=1),
            expected_schema_sha256=MIGRATION_SCHEMA_SHA256,
        )

    audit = next(
        reference
        for reference in store.list_active_references(CONTENT_HASH)
        if reference.owner_type == "audit"
    )
    assert audit.reference_id is not None
    _release_terminal(store, audit, released_at=NOW + timedelta(seconds=1))
    receipt = coordinator.migrate(
        source=registration.object_copy,
        target=_migration_target(),
        observed_at=NOW + timedelta(minutes=1),
        expected_schema_sha256=MIGRATION_SCHEMA_SHA256,
    )

    assert receipt.registered_target_copy is True
    assert {copy.location_id for copy in store.list_active_copies(CONTENT_HASH)} == {
        "lab-hot",
        "lab-warm",
    }


def test_tier_migration_allows_only_adjacent_tiers_and_never_deletes_source(
    tmp_path: Path,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_bundle_atomic(registration)
    cold = _migration_target().model_copy(update={"storage_tier": StorageTier.COLD})
    coordinator = ArtifactTierMigrationCoordinator(
        store=store,
        transport=_CopyTransport(_verification()),
    )

    with pytest.raises(ValueError, match="adjacent"):
        coordinator.migrate(
            source=registration.object_copy,
            target=cold,
            observed_at=NOW + timedelta(minutes=1),
            expected_schema_sha256=MIGRATION_SCHEMA_SHA256,
        )

    assert store.list_active_copies(CONTENT_HASH) == (registration.object_copy,)


def test_tier_migration_rejects_future_verification_evidence(tmp_path: Path) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_bundle_atomic(registration)
    target = _migration_target().model_copy(
        update={
            "verified_at": NOW + timedelta(minutes=2),
            "tier_entered_at": NOW + timedelta(minutes=2),
        }
    )
    verification = _verification().model_copy(update={"verified_at": NOW + timedelta(minutes=2)})
    coordinator = ArtifactTierMigrationCoordinator(
        store=store,
        transport=_CopyTransport(verification),
    )

    with pytest.raises(ValueError, match="future"):
        coordinator.migrate(
            source=registration.object_copy,
            target=target,
            observed_at=NOW + timedelta(minutes=1),
            expected_schema_sha256=MIGRATION_SCHEMA_SHA256,
        )

    assert store.list_active_copies(CONTENT_HASH) == (registration.object_copy,)


@pytest.mark.parametrize(
    "verification",
    [
        _verification().model_copy(update={"schema_sha256": None}),
        _verification().model_copy(update={"schema_sha256": "8" * 64}),
    ],
)
def test_tier_migration_rejects_missing_or_mismatched_resolved_schema(
    tmp_path: Path,
    verification: ObjectCopyVerification,
) -> None:
    store = _reference_store(tmp_path, clock=lambda: NOW)
    registration = _registration()
    store.register_bundle_atomic(registration)
    coordinator = ArtifactTierMigrationCoordinator(
        store=store,
        transport=_CopyTransport(verification),
    )

    with pytest.raises(ValueError, match="schema"):
        coordinator.migrate(
            source=registration.object_copy,
            target=_migration_target(),
            observed_at=NOW + timedelta(minutes=1),
            expected_schema_sha256=MIGRATION_SCHEMA_SHA256,
        )
    with pytest.raises(ValueError, match="required"):
        coordinator.migrate(
            source=registration.object_copy,
            target=_migration_target(),
            observed_at=NOW + timedelta(minutes=1),
        )

    assert store.list_active_copies(CONTENT_HASH) == (registration.object_copy,)


def _sidecar_churn_adversary_process(
    path: str,
    root: str,
    credential_payload: dict[str, object],
    ready: object,
    requests: object,
    responses: object,
) -> None:
    """Legitimate concurrent catalog writer that churns SQLite sidecars on request.

    Each ``churn`` request opens and closes one real writer transaction, so the
    managed trust root sees the same ``-wal``/``-shm`` create/delete pair that a
    production writer produces. The handshake keeps the interleaving
    deterministic: no sleeps, no polling, one round trip per injection point.
    """

    store = ArtifactReferenceStore(
        Path(path),
        managed_trust_root=Path(root),
        writer_owner="sidecar-churn-process",
        retention_writer_credential=(
            artifact_retention_module.ArtifactRetentionWriterCredential.model_validate(
                credential_payload
            )
        ),
        clock=lambda: NOW,
    )
    ready.set()
    while True:
        command = requests.get()
        if command != "churn":
            responses.put(("stopped", True))
            store.close()
            return
        before = os.lstat(root).st_ctime_ns
        moved = False
        for _cycle in range(50):
            with store._writer():
                pass
            if os.lstat(root).st_ctime_ns != before:
                moved = True
                break
        responses.put(("churned", moved))


@contextmanager
def _sidecar_churn_adversary(
    trust_root: Path,
    database: Path,
    credential: artifact_retention_module.ArtifactRetentionWriterCredential,
) -> Iterator[Callable[[], bool]]:
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    requests = context.Queue()
    responses = context.Queue()
    worker = context.Process(
        target=_sidecar_churn_adversary_process,
        args=(
            str(database),
            str(trust_root),
            credential.model_dump(mode="python"),
            ready,
            requests,
            responses,
        ),
    )
    worker.start()
    try:
        assert ready.wait(60)

        def churn() -> bool:
            requests.put("churn")
            outcome, moved = responses.get(timeout=60)
            assert outcome == "churned"
            return bool(moved)

        yield churn
    finally:
        requests.put("stop")
        worker.join(timeout=60)
        if worker.exitcode is None:
            worker.terminate()
            worker.join(timeout=10)
        assert worker.exitcode == 0


@contextmanager
def _inject_between_trust_root_bind_and_chain_scan(
    monkeypatch: pytest.MonkeyPatch,
    injection: Callable[[], None],
) -> Iterator[None]:
    """Fire ``injection`` in the exact window issue #158 names.

    ``PrivateSqlitePathAuthority.__init__`` binds the managed trust root
    generation from ``_validate_managed_trust_root`` and only then rescans the
    parent chain, with no cross-process lock held in between.
    """

    authority_type = artifact_retention_module.PrivateSqlitePathAuthority
    real_validate = authority_type._validate_managed_trust_root
    fired = {"done": False}

    def validate_then_inject(
        self: object,
        managed_trust_root: Path,
    ) -> tuple[Path, tuple[int, int, int]]:
        result = real_validate(self, managed_trust_root)
        if not fired["done"]:
            fired["done"] = True
            injection()
        return result

    monkeypatch.setattr(authority_type, "_validate_managed_trust_root", validate_then_inject)
    yield
    assert fired["done"]


@contextmanager
def _churn_before_every_bootstrap_trust_root_stat(
    monkeypatch: pytest.MonkeyPatch,
    trust_root: Path,
    churn: Callable[[], bool],
) -> Iterator[list[bool]]:
    """Churn the trust root before every stat taken while binding the authority.

    The whole of ``PrivateSqlitePathAuthority.__init__`` runs before the
    cross-process writer flock exists, so a legitimate peer may touch the shared
    managed root before any of its stats. Binding must therefore never depend on
    the trust root ctime holding still.
    """

    authority_type = artifact_retention_module.PrivateSqlitePathAuthority
    real_init = authority_type.__init__
    real_lstat = artifact_retention_module.os.lstat
    state = {"armed": 0, "reentrant": False}
    moves: list[bool] = []

    def churning_lstat(candidate: object, *args: object, **kwargs: object) -> os.stat_result:
        if (
            state["armed"]
            and not state["reentrant"]
            and isinstance(candidate, str | os.PathLike)
            and Path(candidate) == trust_root
        ):
            state["reentrant"] = True
            try:
                moves.append(churn())
            finally:
                state["reentrant"] = False
        return real_lstat(candidate, *args, **kwargs)

    def armed_init(self: object, *args: object, **kwargs: object) -> None:
        state["armed"] += 1
        try:
            real_init(self, *args, **kwargs)
        finally:
            state["armed"] -= 1

    monkeypatch.setattr(authority_type, "__init__", armed_init)
    monkeypatch.setattr(artifact_retention_module.os, "lstat", churning_lstat)
    yield moves


def _provision_catalog(
    trust_root: Path,
    path: Path,
    credential: artifact_retention_module.ArtifactRetentionWriterCredential,
) -> None:
    ArtifactReferenceStore(
        path,
        managed_trust_root=trust_root,
        retention_writer_credential=credential,
        clock=lambda: NOW,
    ).close()


def _run_catalog_victim(
    trust_root: Path,
    path: Path,
    credential: artifact_retention_module.ArtifactRetentionWriterCredential,
) -> None:
    store = ArtifactReferenceStore(
        path,
        managed_trust_root=trust_root,
        retention_writer_credential=credential,
        clock=lambda: NOW,
    )
    try:
        with store._writer():
            pass
    finally:
        store.close()


def _writer_fence(path: Path) -> int:
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT fence FROM artifact_writer_fence WHERE singleton = 1"
        ).fetchone()
    assert row is not None
    return int(row[0])


@pytest.mark.parametrize("victim", ["provisioned", "forged"])
def test_bootstrap_binding_survives_one_sidecar_churn_between_trust_root_bind_and_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    victim: str,
) -> None:
    path = tmp_path / "references.sqlite3"
    credential = _writer_credential()
    _provision_catalog(tmp_path, path, credential)
    forged = _writer_credential(secret_hex="f" * 64, key_id="artifact-retention-forged")
    moves: list[bool] = []

    with (
        _sidecar_churn_adversary(tmp_path, path, credential) as churn,
        _inject_between_trust_root_bind_and_chain_scan(
            monkeypatch,
            lambda: moves.append(churn()),
        ),
    ):
        if victim == "provisioned":
            _run_catalog_victim(tmp_path, path, credential)
        else:
            with pytest.raises(
                artifact_retention_module.ArtifactRetentionWriterAuthorizationError,
                match="rotation|credential",
            ):
                _run_catalog_victim(tmp_path, path, forged)

    assert moves == [True]


@pytest.mark.parametrize("victim", ["provisioned", "forged"])
def test_bootstrap_binding_survives_sidecar_churn_before_every_trust_root_stat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    victim: str,
) -> None:
    path = tmp_path / "references.sqlite3"
    credential = _writer_credential()
    _provision_catalog(tmp_path, path, credential)
    forged = _writer_credential(secret_hex="f" * 64, key_id="artifact-retention-forged")

    with (
        _sidecar_churn_adversary(tmp_path, path, credential) as churn,
        _churn_before_every_bootstrap_trust_root_stat(monkeypatch, tmp_path, churn) as moves,
    ):
        if victim == "provisioned":
            _run_catalog_victim(tmp_path, path, credential)
        else:
            with pytest.raises(
                artifact_retention_module.ArtifactRetentionWriterAuthorizationError,
                match="rotation|credential",
            ):
                _run_catalog_victim(tmp_path, path, forged)

    assert len(moves) >= 2
    assert all(moves)


def test_bootstrap_creation_survives_sidecar_churn_before_every_trust_root_stat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential = _writer_credential()
    neighbour = tmp_path / "neighbour.sqlite3"
    _provision_catalog(tmp_path, neighbour, credential)
    path = tmp_path / "references.sqlite3"

    with (
        _sidecar_churn_adversary(tmp_path, neighbour, credential) as churn,
        _churn_before_every_bootstrap_trust_root_stat(monkeypatch, tmp_path, churn) as moves,
    ):
        _run_catalog_victim(tmp_path, path, credential)

    assert path.exists()
    assert _writer_fence(path) >= 1
    assert len(moves) >= 2
    assert all(moves)


@pytest.mark.parametrize(
    ("hazard", "expected"),
    [
        ("trust-root-inode", "managed trust root"),
        ("trust-root-mode", "parent owner or mode is unsafe"),
        ("trust-root-owner", "parent owner or mode is unsafe"),
        ("ancestor-symlink", "symlink"),
        ("database-hardlink", "hard link"),
    ],
)
def test_bootstrap_binding_fails_closed_for_substitution_in_the_unlocked_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hazard: str,
    expected: str,
) -> None:
    managed = tmp_path / "managed"
    managed.mkdir(mode=0o700)
    private = managed / "private"
    private.mkdir(mode=0o700)
    path = private / "references.sqlite3"
    credential = _writer_credential()
    _provision_catalog(managed, path, credential)
    current_uid = os.geteuid()

    def inject() -> None:
        if hazard == "trust-root-inode":
            retired = tmp_path / "retired"
            managed.rename(retired)
            managed.mkdir(mode=0o700)
            (retired / "private").rename(private)
        elif hazard == "trust-root-mode":
            managed.chmod(0o755)
        elif hazard == "trust-root-owner":
            monkeypatch.setattr(
                artifact_retention_module.os,
                "geteuid",
                lambda: current_uid + 1,
            )
        elif hazard == "ancestor-symlink":
            real = managed / "real"
            private.rename(real)
            os.symlink(real, private, target_is_directory=True)
        else:
            os.link(path, private / "alias.sqlite3")

    with (
        _inject_between_trust_root_bind_and_chain_scan(monkeypatch, inject),
        pytest.raises(ValueError, match=expected),
    ):
        _run_catalog_victim(managed, path, credential)


def test_unlocked_window_inode_fence_reports_expected_and_observed_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed = tmp_path / "managed"
    managed.mkdir(mode=0o700)
    private = managed / "private"
    private.mkdir(mode=0o700)
    path = private / "references.sqlite3"
    credential = _writer_credential()
    _provision_catalog(managed, path, credential)
    bound = os.lstat(managed)

    def swap_trust_root_inode() -> None:
        retired = tmp_path / "retired"
        managed.rename(retired)
        managed.mkdir(mode=0o700)
        (retired / "private").rename(private)

    with (
        _inject_between_trust_root_bind_and_chain_scan(monkeypatch, swap_trust_root_inode),
        pytest.raises(ValueError) as failure,
    ):
        _run_catalog_victim(managed, path, credential)

    observed = os.lstat(managed)
    message = str(failure.value)
    assert "managed trust root" in message
    assert "inode" in message
    assert f"{bound.st_dev}" in message and f"{bound.st_ino}" in message
    assert f"{observed.st_dev}" in message and f"{observed.st_ino}" in message
