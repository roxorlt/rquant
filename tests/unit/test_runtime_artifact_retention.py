from __future__ import annotations

import hashlib
import importlib
import inspect
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    LegalHold,
    ObjectCopy,
    ObjectIdentity,
    ObjectReference,
    RetentionPolicy,
    StorageTier,
)

NOW = datetime(2026, 8, 2, 2, 0, tzinfo=UTC)
PAYLOAD = b"immutable research result\n"
CONTENT_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()
SCHEMA_SHA256 = "e" * 64


class _FullVerifiedGate:
    def authorize(self, candidate: object, *, as_of: datetime) -> dict[str, object]:
        del candidate, as_of
        return {
            "profile": "current",
            "profile_generation": "d" * 64,
            "generation_id": "a" * 64,
            "receipt_id": "b" * 64,
            "verification_level": "full_verified",
            "verified_at": NOW - timedelta(minutes=1),
            "recovery_completed_at": NOW - timedelta(minutes=1),
            "current_published_at": NOW - timedelta(minutes=2),
            "expires_at": NOW + timedelta(days=30),
        }


def _runtime_module() -> object:
    try:
        return importlib.import_module("rquant.runtime_artifact_retention")
    except ModuleNotFoundError:
        pytest.fail("production artifact retention runtime is missing")


def _policy() -> RetentionPolicy:
    return RetentionPolicy(
        hot_min_age=timedelta(0),
        warm_min_age=timedelta(0),
        cold_min_age=timedelta(0),
        minimum_verified_copies=1,
        verification_max_age=timedelta(days=1),
        plan_ttl=timedelta(minutes=10),
        claim_ttl=timedelta(minutes=5),
    )


def _catalog_with_local_tiers(tmp_path: Path) -> tuple[ArtifactReferenceStore, dict[str, Path]]:
    files: dict[str, Path] = {}
    store = ArtifactReferenceStore(
        tmp_path / "catalog.sqlite3",
        managed_trust_root=tmp_path,
        clock=lambda: NOW + timedelta(hours=1),
    )
    store.register_object(
        ObjectIdentity(
            content_sha256=CONTENT_SHA256,
            size_bytes=len(PAYLOAD),
            object_kind="strategy_lab_result",
            created_at=NOW - timedelta(days=100),
        )
    )
    for tier in StorageTier:
        path = tmp_path / f"{tier.value}.artifact"
        path.write_bytes(PAYLOAD)
        path.chmod(0o600)
        files[tier.value] = path
        store.register_copy(
            ObjectCopy(
                content_sha256=CONTENT_SHA256,
                location_id=f"local-{tier.value}",
                storage_uri=path.as_uri(),
                storage_tier=tier,
                verified_at=NOW - timedelta(minutes=1),
                failure_domain=f"disk-{tier.value}",
                tier_entered_at=NOW - timedelta(days=100),
            )
        )
    return store, files


def test_gc_worker_deletes_only_after_durable_cold_verification_and_recovers(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    required = (
        "ArtifactGcRuntimeStore",
        "ArtifactGcWorker",
        "GcWorkerConfig",
        "LocalAtomicArtifactTransport",
    )
    assert all(hasattr(runtime, name) for name in required)
    catalog, files = _catalog_with_local_tiers(tmp_path)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _path: SCHEMA_SHA256,
    )
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    config = runtime.GcWorkerConfig(
        batch_items=1,
        batch_bytes=1024,
        max_runtime=timedelta(seconds=5),
        lease_ttl=timedelta(seconds=30),
        max_attempts=3,
        retry_delay=timedelta(0),
    )
    worker = runtime.ArtifactGcWorker(
        catalog=catalog,
        state=state,
        transport=transport,
        policy=_policy(),
        config=config,
        worker_id="gc-worker-a",
        clock=lambda: NOW,
        deletion_gate=_FullVerifiedGate(),
    )

    first = worker.run_once()
    reopened = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )

    assert first.completed == 1
    assert first.bytes_deleted == len(PAYLOAD)
    assert len(catalog.list_active_copies(CONTENT_SHA256)) == 2
    assert files[StorageTier.COLD.value].read_bytes() == PAYLOAD
    assert reopened.completed_count() == 1
    assert reopened.dead_letter_count() == 0
    assert {event.event_type for event in reopened.audit_events()} >= {
        "lease_acquired",
        "deletion_quarantined",
        "deletion_completed",
    }


def _worker(
    runtime: object,
    tmp_path: Path,
    catalog: ArtifactReferenceStore,
    transport: object,
    *,
    worker_id: str = "gc-worker",
    max_attempts: int = 3,
    clock: object | None = None,
    deletion_gate: object | None = None,
) -> tuple[object, object]:
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    worker = runtime.ArtifactGcWorker(
        catalog=catalog,
        state=state,
        transport=transport,
        policy=_policy(),
        config=runtime.GcWorkerConfig(
            batch_items=1,
            batch_bytes=1024,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=max_attempts,
            retry_delay=timedelta(0),
        ),
        worker_id=worker_id,
        clock=clock or (lambda: NOW),
        deletion_gate=deletion_gate or _FullVerifiedGate(),
    )
    return state, worker


def test_active_reference_and_legal_hold_never_enter_physical_gc(tmp_path: Path) -> None:
    runtime = _runtime_module()
    for guard in ("reference", "hold"):
        root = tmp_path / guard
        root.mkdir(mode=0o700)
        catalog, files = _catalog_with_local_tiers(root)
        if guard == "reference":
            catalog.register_reference(
                ObjectReference(
                    owner_type="deployment",
                    owner_id="deploy-evidence",
                    content_sha256=CONTENT_SHA256,
                    created_at=NOW - timedelta(days=1),
                )
            )
        else:
            catalog.register_legal_hold(
                LegalHold(
                    hold_id="legal-1",
                    content_sha256=CONTENT_SHA256,
                    reason="retain for restoration exercise",
                    created_at=NOW - timedelta(days=1),
                )
            )
        transport = runtime.LocalAtomicArtifactTransport(
            managed_root=root,
            clock=lambda: NOW,
            schema_resolver=lambda _path: SCHEMA_SHA256,
        )
        state, worker = _worker(runtime, root, catalog, transport)

        summary = worker.run_once()

        assert summary.completed == summary.failed == 0
        assert all(path.exists() for path in files.values())
        assert state.completed_count() == state.dead_letter_count() == 0


def test_runtime_lease_fence_allows_only_one_gc_executor(tmp_path: Path) -> None:
    runtime = _runtime_module()
    catalog, _files = _catalog_with_local_tiers(tmp_path)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _path: SCHEMA_SHA256,
    )
    state, worker = _worker(runtime, tmp_path, catalog, transport, worker_id="contender")
    first_lease = state.acquire_lease(
        "lease-holder",
        lease_token="1" * 64,
        now=NOW,
        ttl=timedelta(minutes=1),
    )

    with pytest.raises(runtime.GcLeaseBusyError):
        worker.run_once()

    state.release_lease(
        "lease-holder",
        "1" * 64,
        first_lease.fence,
        now=NOW,
    )
    assert worker.run_once().completed == 1


def test_restart_recovers_when_process_dies_after_unlink_before_receipt(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    catalog, _files = _catalog_with_local_tiers(tmp_path)
    current = [NOW]
    local = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: current[0],
        schema_resolver=lambda _path: SCHEMA_SHA256,
    )

    class DieOnceAfterUnlink:
        def __init__(self) -> None:
            self.failed = False

        def verify(self, storage_uri: str) -> object:
            return local.verify(storage_uri)

        def observe(self, candidate: object) -> object:
            return local.observe(candidate)

        def quarantine(self, candidate: object, claim: object, expected: object) -> object:
            return local.quarantine(candidate, claim, expected)

        def delete_quarantined(self, token: object) -> object:
            receipt = local.delete_quarantined(token)
            if not self.failed:
                self.failed = True
                raise OSError("simulated host loss after unlink")
            return receipt

    transport = DieOnceAfterUnlink()
    state, worker = _worker(
        runtime,
        tmp_path,
        catalog,
        transport,
        clock=lambda: current[0],
    )

    first = worker.run_once()
    current[0] = NOW + timedelta(minutes=6)
    restarted_state, restarted_worker = _worker(
        runtime,
        tmp_path,
        catalog,
        transport,
        clock=lambda: current[0],
    )
    second = restarted_worker.run_once()

    assert first.failed == 1
    assert second.completed == 1
    assert restarted_state.completed_count() == 1
    assert any(
        '"recovered_after_unlink":true' in event.payload_json
        for event in restarted_state.audit_events()
        if event.event_type == "physical_deletion_recorded"
    )


@pytest.mark.parametrize("replacement", ["symlink", "hardlink", "regular"])
def test_claimed_artifact_path_replacement_fails_closed(
    tmp_path: Path,
    replacement: str,
) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _path: SCHEMA_SHA256,
    )
    plan = catalog.plan_gc(now=NOW, policy=_policy())
    candidate = next(
        item for item in plan.candidates if item.object_copy.storage_tier is StorageTier.HOT
    )
    claim = catalog.claim_deletion(
        plan=plan,
        candidate=candidate,
        owner_id="gc-worker",
        now=NOW,
    )
    expected = transport.observe(candidate)
    source = files[StorageTier.HOT.value]
    retired = tmp_path / "retired-original"
    source.rename(retired)
    if replacement == "symlink":
        source.symlink_to(files[StorageTier.COLD.value])
    elif replacement == "hardlink":
        os.link(files[StorageTier.COLD.value], source)
    else:
        source.write_bytes(PAYLOAD)
        source.chmod(0o600)

    with pytest.raises((OSError, ValueError), match="changed|link|identity|regular|unsafe"):
        transport.quarantine(candidate, claim, expected)

    assert retired.read_bytes() == PAYLOAD
    assert source.exists()


def test_transport_rejects_intermediate_ancestor_swap_during_schema_verify(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    parent = tmp_path / "tier" / "day"
    parent.mkdir(parents=True, mode=0o700)
    (tmp_path / "tier").chmod(0o700)
    parent.chmod(0o700)
    artifact = parent / "artifact.bin"
    artifact.write_bytes(PAYLOAD)
    artifact.chmod(0o600)
    retired = tmp_path / "tier-retired"

    def swap_ancestor(_descriptor: object) -> str:
        (tmp_path / "tier").rename(retired)
        replacement = tmp_path / "tier" / "day"
        replacement.mkdir(parents=True, mode=0o700)
        (tmp_path / "tier").chmod(0o700)
        replacement.chmod(0o700)
        replacement_artifact = replacement / artifact.name
        replacement_artifact.write_bytes(PAYLOAD)
        replacement_artifact.chmod(0o600)
        return SCHEMA_SHA256

    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=swap_ancestor,
    )

    with pytest.raises(ValueError, match="ancestor|identity|changed"):
        transport.verify(artifact.as_uri())

    assert (retired / "day" / artifact.name).read_bytes() == PAYLOAD


def test_transport_uses_only_root_relative_openat_after_root_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime_module()
    nested = tmp_path / "warm" / "partition"
    nested.mkdir(parents=True, mode=0o700)
    (tmp_path / "warm").chmod(0o700)
    nested.chmod(0o700)
    artifact = nested / "artifact.bin"
    artifact.write_bytes(PAYLOAD)
    artifact.chmod(0o600)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda descriptor: SCHEMA_SHA256,
    )
    real_open = os.open

    def reject_absolute_reopen(
        path: object,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if dir_fd is None and isinstance(path, (str, bytes, os.PathLike)):
            assert not os.path.isabs(path), "transport reopened an absolute path"
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", reject_absolute_reopen)

    verification = transport.verify(artifact.as_uri())

    assert verification.content_sha256 == CONTENT_SHA256


def test_transport_initialization_rejects_intermediate_symlink_from_anchor(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    physical = tmp_path / "physical"
    managed = physical / "managed"
    managed.mkdir(parents=True, mode=0o700)
    physical.chmod(0o700)
    managed.chmod(0o700)
    alias = tmp_path / "alias"
    alias.symlink_to(physical, target_is_directory=True)

    with pytest.raises((OSError, ValueError), match="symlink|ancestor|unsafe"):
        runtime.LocalAtomicArtifactTransport(
            managed_root=alias / "managed",
            clock=lambda: NOW,
            schema_resolver=lambda _descriptor: SCHEMA_SHA256,
        )


def test_transport_rejects_managed_root_rename_and_replacement(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    managed = tmp_path / "managed"
    managed.mkdir(mode=0o700)
    artifact = managed / "artifact.bin"
    artifact.write_bytes(PAYLOAD)
    artifact.chmod(0o600)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=managed,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )
    retired = tmp_path / "managed-retired"
    managed.rename(retired)
    managed.mkdir(mode=0o700)
    replacement = managed / artifact.name
    replacement.write_bytes(PAYLOAD)
    replacement.chmod(0o600)

    with pytest.raises(ValueError, match="root|ancestor|identity|changed"):
        transport.verify(artifact.as_uri())

    assert replacement.read_bytes() == PAYLOAD
    assert (retired / artifact.name).read_bytes() == PAYLOAD


def test_transport_checks_bound_root_after_operation_body_raises(tmp_path: Path) -> None:
    runtime = _runtime_module()
    managed = tmp_path / "managed"
    managed.mkdir(mode=0o700)
    artifact = managed / "artifact.bin"
    artifact.write_bytes(PAYLOAD)
    artifact.chmod(0o600)
    retired = tmp_path / "managed-retired"

    def swap_root_then_raise(_descriptor: object) -> str:
        managed.rename(retired)
        managed.mkdir(mode=0o700)
        raise RuntimeError("resolver failed after root swap")

    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=managed,
        clock=lambda: NOW,
        schema_resolver=swap_root_then_raise,
    )

    with pytest.raises(ValueError, match="root|ancestor|identity|changed"):
        transport.verify(artifact.as_uri())

    assert (retired / artifact.name).read_bytes() == PAYLOAD


def test_corrupt_cold_copy_retries_then_dead_letters_without_deleting_source(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    files[StorageTier.COLD.value].write_bytes(b"corrupt cold archive")
    files[StorageTier.COLD.value].chmod(0o600)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _path: SCHEMA_SHA256,
    )
    state, worker = _worker(runtime, tmp_path, catalog, transport, max_attempts=3)

    results = [worker.run_once() for _ in range(3)]

    assert [result.failed for result in results] == [1, 1, 1]
    assert results[-1].dead_lettered == 1
    assert state.dead_letter_count() == 1
    assert files[StorageTier.HOT.value].read_bytes() == PAYLOAD
    assert len(catalog.list_active_copies(CONTENT_SHA256)) == 3
    assert any(event.event_type == "work_dead_lettered" for event in state.audit_events())


def test_worker_enforces_byte_and_wall_clock_budgets_before_claim(tmp_path: Path) -> None:
    runtime = _runtime_module()
    for constraint in ("bytes", "time"):
        root = tmp_path / constraint
        root.mkdir(mode=0o700)
        catalog, files = _catalog_with_local_tiers(root)
        transport = runtime.LocalAtomicArtifactTransport(
            managed_root=root,
            clock=lambda: NOW,
            schema_resolver=lambda _path: SCHEMA_SHA256,
        )
        state = runtime.ArtifactGcRuntimeStore(
            root / "gc-state.sqlite3",
            managed_trust_root=root,
        )
        monotonic_values = iter((0.0, 6.0))
        worker = runtime.ArtifactGcWorker(
            catalog=catalog,
            state=state,
            transport=transport,
            policy=_policy(),
            config=runtime.GcWorkerConfig(
                batch_items=2,
                batch_bytes=len(PAYLOAD) - 1 if constraint == "bytes" else 1024,
                max_runtime=timedelta(seconds=5),
                lease_ttl=timedelta(seconds=30),
                max_attempts=2,
                retry_delay=timedelta(0),
            ),
            worker_id=f"budget-{constraint}",
            clock=lambda: NOW,
            monotonic=(
                (lambda values=monotonic_values: next(values)) if constraint == "time" else None
            ),
            deletion_gate=_FullVerifiedGate(),
        )

        summary = worker.run_once()

        assert summary.completed == summary.failed == 0
        assert all(path.exists() for path in files.values())


def test_deadline_reached_by_cold_verify_prevents_quarantine_and_unlink(tmp_path: Path) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    monotonic_now = [0.0]
    local = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )

    class DeadlineAdvancingTransport:
        quarantine_calls = 0
        unlink_calls = 0

        def verify(self, storage_uri: str) -> object:
            verification = local.verify(storage_uri)
            monotonic_now[0] = 5.0
            return verification

        def observe(self, candidate: object) -> object:
            return local.observe(candidate)

        def quarantine(self, candidate: object, claim: object, expected: object) -> object:
            self.quarantine_calls += 1
            return local.quarantine(candidate, claim, expected)

        def delete_quarantined(self, token: object) -> object:
            self.unlink_calls += 1
            return local.delete_quarantined(token)

    transport = DeadlineAdvancingTransport()
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    worker = runtime.ArtifactGcWorker(
        catalog=catalog,
        state=state,
        transport=transport,
        deletion_gate=_FullVerifiedGate(),
        policy=_policy(),
        config=runtime.GcWorkerConfig(
            batch_items=1,
            batch_bytes=1024,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=1,
            retry_delay=timedelta(0),
        ),
        worker_id="deadline-fence",
        clock=lambda: NOW,
        monotonic=lambda: monotonic_now[0],
    )

    result = worker.run_once()

    assert result.failed == result.dead_lettered == 1
    assert transport.quarantine_calls == transport.unlink_calls == 0
    assert all(path.exists() for path in files.values())
    assert len(catalog.list_active_copies(CONTENT_SHA256)) == 3


def test_deadline_reached_by_recovery_gate_prevents_all_physical_gc(tmp_path: Path) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    monotonic_now = [0.0]
    local = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )

    class DeadlineGate:
        def authorize(self, candidate: object, *, as_of: datetime) -> dict[str, object]:
            del candidate, as_of
            monotonic_now[0] = 5.0
            return {
                "profile": "current",
                "profile_generation": "d" * 64,
                "generation_id": "a" * 64,
                "receipt_id": "b" * 64,
                "verification_level": "full_verified",
                "verified_at": NOW - timedelta(minutes=1),
                "recovery_completed_at": NOW - timedelta(minutes=1),
                "current_published_at": NOW - timedelta(minutes=2),
                "expires_at": NOW + timedelta(days=30),
            }

    class CountingTransport:
        physical_calls = 0

        def verify(self, storage_uri: str) -> object:
            self.physical_calls += 1
            return local.verify(storage_uri)

        def observe(self, candidate: object) -> object:
            self.physical_calls += 1
            return local.observe(candidate)

        def quarantine(self, candidate: object, claim: object, expected: object) -> object:
            self.physical_calls += 1
            return local.quarantine(candidate, claim, expected)

        def delete_quarantined(self, token: object) -> object:
            self.physical_calls += 1
            return local.delete_quarantined(token)

    transport = CountingTransport()
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    worker = runtime.ArtifactGcWorker(
        catalog=catalog,
        state=state,
        transport=transport,
        deletion_gate=DeadlineGate(),
        policy=_policy(),
        config=runtime.GcWorkerConfig(
            batch_items=1,
            batch_bytes=1024,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=1,
            retry_delay=timedelta(0),
        ),
        worker_id="gate-deadline",
        clock=lambda: NOW,
        monotonic=lambda: monotonic_now[0],
    )

    result = worker.run_once()

    assert result.failed == result.dead_lettered == 1
    assert transport.physical_calls == 0
    assert all(path.exists() for path in files.values())


def test_deadline_reached_after_quarantine_prevents_unlink(tmp_path: Path) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    monotonic_now = [0.0]
    local = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )

    class QuarantineDeadlineTransport:
        unlink_calls = 0

        def verify(self, storage_uri: str) -> object:
            return local.verify(storage_uri)

        def observe(self, candidate: object) -> object:
            return local.observe(candidate)

        def quarantine(self, candidate: object, claim: object, expected: object) -> object:
            token = local.quarantine(candidate, claim, expected)
            monotonic_now[0] = 5.0
            return token

        def delete_quarantined(self, token: object) -> object:
            self.unlink_calls += 1
            return local.delete_quarantined(token)

    transport = QuarantineDeadlineTransport()
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    worker = runtime.ArtifactGcWorker(
        catalog=catalog,
        state=state,
        transport=transport,
        deletion_gate=_FullVerifiedGate(),
        policy=_policy(),
        config=runtime.GcWorkerConfig(
            batch_items=1,
            batch_bytes=1024,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=1,
            retry_delay=timedelta(0),
        ),
        worker_id="quarantine-deadline",
        clock=lambda: NOW,
        monotonic=lambda: monotonic_now[0],
    )

    result = worker.run_once()

    assert result.failed == result.dead_lettered == 1
    assert transport.unlink_calls == 0
    assert not files[StorageTier.HOT.value].exists()
    assert next(tmp_path.glob(".rquant-gc-*")).read_bytes() == PAYLOAD


def test_lease_expiry_during_cold_verify_prevents_quarantine_and_unlink(tmp_path: Path) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    wall_now = [NOW]
    local = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )

    class LeaseExpiringTransport:
        quarantine_calls = 0
        unlink_calls = 0

        def verify(self, storage_uri: str) -> object:
            verification = local.verify(storage_uri)
            wall_now[0] = NOW + timedelta(seconds=31)
            return verification

        def observe(self, candidate: object) -> object:
            return local.observe(candidate)

        def quarantine(self, candidate: object, claim: object, expected: object) -> object:
            self.quarantine_calls += 1
            return local.quarantine(candidate, claim, expected)

        def delete_quarantined(self, token: object) -> object:
            self.unlink_calls += 1
            return local.delete_quarantined(token)

    transport = LeaseExpiringTransport()
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    worker = runtime.ArtifactGcWorker(
        catalog=catalog,
        state=state,
        transport=transport,
        deletion_gate=_FullVerifiedGate(),
        policy=_policy(),
        config=runtime.GcWorkerConfig(
            batch_items=1,
            batch_bytes=1024,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=1,
            retry_delay=timedelta(0),
        ),
        worker_id="lease-fence",
        clock=lambda: wall_now[0],
        monotonic=lambda: 0.0,
    )

    with pytest.raises(ValueError, match="lease|fence"):
        worker.run_once()

    assert transport.quarantine_calls == transport.unlink_calls == 0
    assert all(path.exists() for path in files.values())
    assert len(catalog.list_active_copies(CONTENT_SHA256)) == 3


@pytest.mark.parametrize("checkpoint", ["queued", "claimed"])
def test_current_budget_defers_oversized_queued_and_recovery_work_without_deletion(
    tmp_path: Path,
    checkpoint: str,
) -> None:
    runtime = _runtime_module()
    assert len(PAYLOAD) == 26
    catalog, files = _catalog_with_local_tiers(tmp_path)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    setup_lease = state.acquire_lease(
        "setup",
        lease_token="c" * 64,
        now=NOW,
        ttl=timedelta(minutes=1),
    )
    plan = catalog.plan_gc(now=NOW, policy=_policy())
    candidate = next(
        item for item in plan.candidates if item.object_copy.storage_tier is StorageTier.HOT
    )
    work_id = state.enqueue(
        plan=plan,
        candidate=candidate,
        owner_id="setup",
        lease_token="c" * 64,
        fence=setup_lease.fence,
        now=NOW,
    )
    if checkpoint == "claimed":
        identity = transport.observe(candidate)
        state.transition(
            work_id,
            owner_id="setup",
            lease_token="c" * 64,
            fence=setup_lease.fence,
            now=NOW,
            status="queued",
            event_type="physical_identity_bound",
            values={"physical_identity_json": identity.model_dump_json()},
        )
        claim = catalog.claim_deletion(
            plan=plan,
            candidate=candidate,
            owner_id="c" * 64,
            operation_id=work_id,
            now=NOW,
        )
        state.transition(
            work_id,
            owner_id="setup",
            lease_token="c" * 64,
            fence=setup_lease.fence,
            now=NOW,
            status="claimed",
            event_type="catalog_claimed",
            values={"claim_json": claim.model_dump_json()},
        )
    state.release_lease(
        "setup",
        "c" * 64,
        setup_lease.fence,
        now=NOW,
    )
    worker = runtime.ArtifactGcWorker(
        catalog=catalog,
        state=state,
        transport=transport,
        deletion_gate=_FullVerifiedGate(),
        policy=_policy(),
        config=runtime.GcWorkerConfig(
            batch_items=2,
            batch_bytes=25,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=1,
            retry_delay=timedelta(0),
        ),
        worker_id="bounded-restart",
        clock=lambda: NOW,
    )

    result = worker.run_once()

    assert result.deferred == 1
    assert result.completed == result.failed == result.dead_lettered == 0
    assert all(path.exists() for path in files.values())
    assert state.dead_letter_count() == 0


def test_expired_lease_increments_fence_and_stale_owner_cannot_mutate_state(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    first = state.acquire_lease(
        "shared-label",
        lease_token="1" * 64,
        now=NOW,
        ttl=timedelta(seconds=1),
    )
    second = state.acquire_lease(
        "shared-label",
        lease_token="2" * 64,
        now=NOW + timedelta(seconds=2),
        ttl=timedelta(seconds=30),
    )

    assert second.fence == first.fence + 1
    with pytest.raises(ValueError, match="stale"):
        state.release_lease(
            "shared-label",
            "1" * 64,
            first.fence,
            now=NOW + timedelta(seconds=2),
        )
    renewed = state.renew_lease(
        "shared-label",
        "2" * 64,
        second.fence,
        now=NOW + timedelta(seconds=3),
        ttl=timedelta(seconds=30),
    )
    assert renewed.fence == second.fence


def test_same_owner_label_with_distinct_run_tokens_cannot_share_live_fence(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    path = tmp_path / "gc-state.sqlite3"
    first_state = runtime.ArtifactGcRuntimeStore(path, managed_trust_root=tmp_path)
    second_state = runtime.ArtifactGcRuntimeStore(path, managed_trust_root=tmp_path)
    first = first_state.acquire_lease(
        "production-gc",
        lease_token="a" * 64,
        now=NOW,
        ttl=timedelta(minutes=1),
    )

    with pytest.raises(runtime.GcLeaseBusyError):
        second_state.acquire_lease(
            "production-gc",
            lease_token="b" * 64,
            now=NOW,
            ttl=timedelta(minutes=1),
        )
    with pytest.raises(ValueError, match="stale"):
        second_state.renew_lease(
            "production-gc",
            "b" * 64,
            first.fence,
            now=NOW + timedelta(seconds=1),
            ttl=timedelta(minutes=1),
        )


def test_runtime_state_rejects_schema_drift_before_gc_can_run(tmp_path: Path) -> None:
    runtime = _runtime_module()
    path = tmp_path / "gc-state.sqlite3"
    runtime.ArtifactGcRuntimeStore(path, managed_trust_root=tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE injected_bypass(value TEXT)")

    with pytest.raises(ValueError, match="schema"):
        runtime.ArtifactGcRuntimeStore(path, managed_trust_root=tmp_path)


def test_runtime_state_migrates_legacy_v2_column_order_without_schema_drift(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    path = tmp_path / "gc-state.sqlite3"
    runtime.ArtifactGcRuntimeStore(path, managed_trust_root=tmp_path)
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            DROP INDEX gc_runtime_work_due_idx;
            ALTER TABLE gc_runtime_work RENAME TO gc_runtime_work_v3;
            CREATE TABLE gc_runtime_work (
                work_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                plan_json TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                claim_json TEXT,
                physical_identity_json TEXT,
                token_json TEXT,
                deletion_receipt_json TEXT,
                status TEXT NOT NULL CHECK(status IN (
                    'queued', 'claimed', 'quarantined', 'deleted', 'retry', 'completed', 'dead'
                )),
                attempts INTEGER NOT NULL,
                next_attempt_at TEXT NOT NULL,
                last_error TEXT,
                updated_at TEXT NOT NULL
            );
            DROP TABLE gc_runtime_work_v3;
            CREATE INDEX gc_runtime_work_due_idx
            ON gc_runtime_work(status, next_attempt_at, work_id);
            PRAGMA user_version = 2;
            """
        )

    reopened = runtime.ArtifactGcRuntimeStore(path, managed_trust_root=tmp_path)

    assert reopened.completed_count() == 0
    with sqlite3.connect(path) as connection:
        columns = tuple(
            str(row[1]) for row in connection.execute("PRAGMA table_info(gc_runtime_work)")
        )
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    assert columns[-3:] == ("authorization_id", "authorization_json", "created_at")
    assert version == 6


@pytest.mark.parametrize(
    ("crash_event", "expected_reconciliation", "expected_orphans"),
    [
        ("catalog_claimed", 1, 0),
        ("deletion_quarantined", 0, 1),
    ],
)
def test_gc_health_projects_reconciliation_backlog_and_quarantine_orphans(
    tmp_path: Path,
    crash_event: str,
    expected_reconciliation: int,
    expected_orphans: int,
) -> None:
    runtime = _runtime_module()
    catalog, _files = _catalog_with_local_tiers(tmp_path)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )
    state, worker = _worker(runtime, tmp_path, catalog, transport)
    original_transition = state.transition

    def crash_at_checkpoint(*args: object, **kwargs: object) -> None:
        if kwargs.get("event_type") == crash_event:
            raise SystemExit(f"crash at {crash_event}")
        original_transition(*args, **kwargs)

    state.transition = crash_at_checkpoint
    with pytest.raises(SystemExit, match=crash_event):
        worker.run_once()

    summary = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=state,
        quarantine_inspector=transport,
    ).snapshot(now=NOW + timedelta(seconds=10))

    assert summary.status == "critical"
    assert summary.backlog_count == 1
    assert summary.oldest_backlog_age_seconds == 10
    assert summary.operation_reconciliation_pending_count == expected_reconciliation
    assert summary.quarantine_orphan_count == expected_orphans
    assert summary.retry_count == summary.dead_letter_count == 0
    assert summary.lease_fence == 1
    assert summary.lease_active is False


def test_gc_health_projects_retry_dead_letter_and_fence_metrics(tmp_path: Path) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    files[StorageTier.COLD.value].write_bytes(b"corrupt")
    files[StorageTier.COLD.value].chmod(0o600)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )
    state, worker = _worker(
        runtime,
        tmp_path,
        catalog,
        transport,
        max_attempts=2,
    )

    first = worker.run_once()
    retry_health = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=state,
        quarantine_inspector=transport,
    ).snapshot(now=NOW + timedelta(seconds=10))
    second = worker.run_once()
    dead_health = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=state,
        quarantine_inspector=transport,
    ).snapshot(now=NOW + timedelta(seconds=20))

    assert first.failed == 1
    assert retry_health.status == "degraded"
    assert retry_health.retry_count == 1
    assert retry_health.dead_letter_count == 0
    assert retry_health.oldest_backlog_age_seconds == 10
    assert retry_health.lease_fence == 1
    assert second.dead_lettered == 1
    assert dead_health.status == "critical"
    assert dead_health.retry_count == 0
    assert dead_health.dead_letter_count == 1
    assert dead_health.lease_fence == 2


def test_gc_health_projection_is_keyset_bounded_and_batches_catalog_reads(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    catalog = ArtifactReferenceStore(
        tmp_path / "catalog.sqlite3",
        managed_trust_root=tmp_path,
    )
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )

    def seed(connection: sqlite3.Connection) -> None:
        for index in range(3):
            work_id = f"{index + 1:064x}"
            connection.execute(
                """
                INSERT INTO gc_runtime_work(
                    work_id, candidate_id, content_sha256, size_bytes,
                    plan_json, candidate_json, status, attempts,
                    next_attempt_at, updated_at, created_at
                ) VALUES (?, ?, ?, 26, '{}', '{}', 'queued', 0, ?, ?, ?)
                """,
                (
                    work_id,
                    work_id,
                    "a" * 64,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    (NOW - timedelta(seconds=10)).isoformat(),
                ),
            )

    state._write(seed)
    batch_calls: list[tuple[str, ...]] = []
    real_batch = catalog.get_gc_operations

    def batch_lookup(operation_ids: tuple[str, ...]) -> dict[str, object]:
        batch_calls.append(operation_ids)
        return real_batch(operation_ids)

    catalog.get_gc_operations = batch_lookup
    catalog.get_gc_operation = lambda _operation_id: pytest.fail("N+1 catalog lookup")

    class NoQuarantine:
        def is_quarantined(self, *args: object) -> bool:
            del args
            pytest.fail("queued health rows must not touch the filesystem")

    projector = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=state,
        quarantine_inspector=NoQuarantine(),
        monotonic=lambda: 0.0,
    )
    first = projector.snapshot(
        now=NOW,
        max_items=1,
        max_bytes=1024,
        deadline_monotonic=10.0,
    )
    second = projector.snapshot(
        now=NOW,
        max_items=1,
        max_bytes=1024,
        deadline_monotonic=10.0,
        cursor=first.next_cursor,
    )

    assert first.truncated is True
    assert first.scanned_items == 1
    assert first.scanned_bytes == 26
    assert first.next_cursor is not None
    assert first.next_cursor.work_id == f"{1:064x}"
    assert second.next_cursor is not None
    assert second.next_cursor.work_id == f"{2:064x}"
    assert batch_calls == [(f"{1:064x}",), (f"{2:064x}",)]


def test_gc_health_projection_stops_before_oversized_or_deadline_row(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    catalog = ArtifactReferenceStore(
        tmp_path / "catalog.sqlite3",
        managed_trust_root=tmp_path,
    )
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )

    def seed(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            INSERT INTO gc_runtime_work(
                work_id, candidate_id, content_sha256, size_bytes,
                plan_json, candidate_json, status, attempts,
                next_attempt_at, updated_at, created_at
            ) VALUES (?, ?, ?, 26, '{}', '{}', 'queued', 0, ?, ?, ?)
            """,
            (
                "1" * 64,
                "1" * 64,
                "a" * 64,
                NOW.isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )

    state._write(seed)

    class NoQuarantine:
        def is_quarantined(self, *args: object) -> bool:
            del args
            pytest.fail("bounded projection must not inspect an unconsumed row")

    monotonic_now = [0.0]
    projector = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=state,
        quarantine_inspector=NoQuarantine(),
        monotonic=lambda: monotonic_now[0],
    )
    oversized = projector.snapshot(
        now=NOW,
        max_items=10,
        max_bytes=25,
        deadline_monotonic=10.0,
    )
    monotonic_now[0] = 10.0
    expired = projector.snapshot(
        now=NOW,
        max_items=10,
        max_bytes=1024,
        deadline_monotonic=10.0,
    )

    assert oversized.truncated is True
    assert oversized.scanned_items == oversized.scanned_bytes == 0
    assert oversized.next_cursor is not None
    assert oversized.next_cursor.work_id == "1" * 64
    assert oversized.blocked_work_id == "1" * 64
    assert expired.truncated is True
    assert expired.scanned_items == expired.scanned_bytes == 0
    assert expired.next_cursor is None


def test_gc_health_oversized_row_advances_persistent_cursor_without_starving_followers(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    catalog = ArtifactReferenceStore(
        tmp_path / "catalog.sqlite3",
        managed_trust_root=tmp_path,
    )
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )

    def seed(connection: sqlite3.Connection) -> None:
        for work_id, size_bytes in (("1" * 64, 26), ("2" * 64, 1)):
            connection.execute(
                """
                INSERT INTO gc_runtime_work(
                    work_id, candidate_id, content_sha256, size_bytes,
                    plan_json, candidate_json, status, attempts,
                    next_attempt_at, updated_at, created_at
                ) VALUES (?, ?, ?, ?, '{}', '{}', 'queued', 0, ?, ?, ?)
                """,
                (
                    work_id,
                    work_id,
                    "a" * 64,
                    size_bytes,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    NOW.isoformat(),
                ),
            )

    state._write(seed)

    class NoQuarantine:
        def is_quarantined(self, *args: object) -> bool:
            del args
            pytest.fail("queued rows must not inspect quarantine")

    first = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=state,
        quarantine_inspector=NoQuarantine(),
        monotonic=lambda: 0.0,
    ).snapshot(
        now=NOW,
        max_items=1,
        max_bytes=25,
        deadline_monotonic=10.0,
    )
    second = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=runtime.ArtifactGcRuntimeStore(state.path, managed_trust_root=tmp_path),
        quarantine_inspector=NoQuarantine(),
        monotonic=lambda: 0.0,
    ).snapshot(
        now=NOW,
        max_items=1,
        max_bytes=25,
        deadline_monotonic=10.0,
    )

    assert first.blocked_work_id == "1" * 64
    assert first.next_cursor is not None and first.next_cursor.work_id == "1" * 64
    assert second.scanned_items == 1
    assert second.next_cursor is not None and second.next_cursor.work_id == "2" * 64


def test_gc_health_blocked_cursor_wins_after_prior_rows_were_projected(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    catalog = ArtifactReferenceStore(
        tmp_path / "catalog.sqlite3",
        managed_trust_root=tmp_path,
    )
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )

    def seed(connection: sqlite3.Connection) -> None:
        for work_id, size_bytes in (
            ("1" * 64, 1),
            ("2" * 64, 2),
            ("3" * 64, 1),
        ):
            connection.execute(
                """
                INSERT INTO gc_runtime_work(
                    work_id, candidate_id, content_sha256, size_bytes,
                    plan_json, candidate_json, status, attempts,
                    next_attempt_at, updated_at, created_at
                ) VALUES (?, ?, ?, ?, '{}', '{}', 'queued', 0, ?, ?, ?)
                """,
                (
                    work_id,
                    work_id,
                    "a" * 64,
                    size_bytes,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    NOW.isoformat(),
                ),
            )

    state._write(seed)

    class NoQuarantine:
        def is_quarantined(self, *args: object) -> bool:
            del args
            pytest.fail("queued rows must not inspect quarantine")

    projector = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=state,
        quarantine_inspector=NoQuarantine(),
        monotonic=lambda: 0.0,
    )
    first = projector.snapshot(
        now=NOW,
        max_items=3,
        max_bytes=2,
        deadline_monotonic=10.0,
    )
    second = projector.snapshot(
        now=NOW,
        max_items=1,
        max_bytes=2,
        deadline_monotonic=10.0,
    )

    assert first.scanned_items == first.scanned_bytes == 1
    assert first.blocked_work_id == "2" * 64
    assert first.next_cursor is not None and first.next_cursor.work_id == "2" * 64
    assert second.scanned_items == 1
    assert second.next_cursor is not None and second.next_cursor.work_id == "3" * 64


def test_gc_health_snapshot_expired_deadline_performs_no_authority_io() -> None:
    runtime = _runtime_module()

    class NoIo:
        def __getattr__(self, name: str) -> object:
            pytest.fail(f"expired health snapshot touched {name}")

    projector = runtime.ArtifactGcHealthProjector(
        catalog=NoIo(),
        state=NoIo(),
        quarantine_inspector=NoIo(),
        monotonic=lambda: 10.0,
    )

    summary = projector.snapshot(
        now=NOW,
        max_items=1,
        max_bytes=1,
        deadline_monotonic=10.0,
    )

    assert summary.status == "degraded"
    assert summary.truncated is True
    assert summary.scanned_items == summary.scanned_bytes == 0


def test_gc_health_cursor_is_persistent_and_round_robin_without_starvation(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    catalog = ArtifactReferenceStore(
        tmp_path / "catalog.sqlite3",
        managed_trust_root=tmp_path,
    )
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )

    def seed(connection: sqlite3.Connection) -> None:
        for index in range(3):
            work_id = f"{index + 1:064x}"
            connection.execute(
                """
                INSERT INTO gc_runtime_work(
                    work_id, candidate_id, content_sha256, size_bytes,
                    plan_json, candidate_json, status, attempts,
                    next_attempt_at, updated_at, created_at
                ) VALUES (?, ?, ?, 1, '{}', '{}', 'queued', 0, ?, ?, ?)
                """,
                (
                    work_id,
                    work_id,
                    "a" * 64,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    NOW.isoformat(),
                ),
            )

    state._write(seed)

    class NoQuarantine:
        def is_quarantined(self, *args: object) -> bool:
            del args
            pytest.fail("queued health rows must not touch the filesystem")

    observed: list[str] = []
    for _ in range(4):
        summary = runtime.ArtifactGcHealthProjector(
            catalog=catalog,
            state=runtime.ArtifactGcRuntimeStore(
                state.path,
                managed_trust_root=tmp_path,
            ),
            quarantine_inspector=NoQuarantine(),
            monotonic=lambda: 0.0,
        ).snapshot(
            now=NOW,
            max_items=1,
            max_bytes=1024,
            deadline_monotonic=10.0,
        )
        assert summary.next_cursor is not None
        observed.append(summary.next_cursor.work_id)

    assert observed == [f"{value:064x}" for value in (1, 2, 3, 1)]


def test_gc_health_uses_materialized_counts_and_indexed_keyset_state(tmp_path: Path) -> None:
    runtime = _runtime_module()
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    source = inspect.getsource(runtime.ArtifactGcRuntimeStore.health_aggregate).upper()

    assert "SUM(CASE" not in source
    assert "MIN(CASE" not in source
    assert "COUNT(*) FROM GC_RUNTIME_WORK" not in source
    with sqlite3.connect(state.path) as connection:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        indexes = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
    assert "gc_runtime_health_state" in tables
    assert "gc_runtime_work_health_status_idx" in indexes


def test_retention_exposes_no_legacy_fast_recovery_deletion_gate() -> None:
    runtime = _runtime_module()
    source = Path(runtime.__file__).read_text(encoding="utf-8")

    assert not hasattr(runtime, "RecoveryReceiptDeletionGate")
    assert "authorize_uri" not in source
    assert "verify_runtime_recovery" not in source
    assert "load_verified_real_recovery_receipt" not in source


@pytest.mark.parametrize(
    "crash_event",
    ["catalog_claimed", "deletion_completed"],
)
def test_cross_database_operation_recovers_after_catalog_commit_before_runtime_checkpoint(
    tmp_path: Path,
    crash_event: str,
) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    local = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )
    state, worker = _worker(runtime, tmp_path, catalog, local)
    original_transition = state.transition
    crashed = False

    def crash_after_catalog_commit(*args: object, **kwargs: object) -> None:
        nonlocal crashed
        if kwargs.get("event_type") == crash_event and not crashed:
            crashed = True
            raise SystemExit(f"crash after {crash_event}")
        original_transition(*args, **kwargs)

    state.transition = crash_after_catalog_commit
    with pytest.raises(SystemExit, match=crash_event):
        worker.run_once()

    operation_id = next(
        event.work_id for event in state.audit_events() if event.event_type == "work_enqueued"
    )
    operation = catalog.get_gc_operation(operation_id)
    assert operation is not None
    assert operation.status == ("claimed" if crash_event == "catalog_claimed" else "completed")

    restarted_state, restarted = _worker(runtime, tmp_path, catalog, local)
    result = restarted.run_once()

    assert result.completed == 1
    assert result.failed == result.dead_lettered == 0
    assert restarted_state.completed_count() == 1
    assert restarted_state.dead_letter_count() == 0
    assert files[StorageTier.COLD.value].read_bytes() == PAYLOAD


@pytest.mark.parametrize(
    "rotation",
    ["profile_generation", "generation_id", "receipt_id", "verified_at"],
)
def test_quarantine_crash_persists_authorization_and_rotation_fails_closed_on_restart(
    tmp_path: Path,
    rotation: str,
) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )
    authority: dict[str, object] = {
        "profile_generation": "d" * 64,
        "generation_id": "a" * 64,
        "receipt_id": "b" * 64,
        "verified_at": NOW - timedelta(seconds=2),
    }

    class RotatingGate:
        def authorize(self, candidate: object, *, as_of: datetime) -> dict[str, object]:
            del candidate
            return {
                "profile": "current",
                "profile_generation": authority["profile_generation"],
                "generation_id": authority["generation_id"],
                "receipt_id": authority["receipt_id"],
                "verification_level": "full_verified",
                "verified_at": authority["verified_at"],
                "recovery_completed_at": authority["verified_at"],
                "current_published_at": NOW - timedelta(minutes=2),
                "expires_at": NOW + timedelta(days=30),
            }

    state, worker = _worker(
        runtime,
        tmp_path,
        catalog,
        transport,
        deletion_gate=RotatingGate(),
    )
    with sqlite3.connect(state.path) as connection:
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(gc_runtime_work)")}
    assert {"authorization_id", "authorization_json"} <= columns

    original_transition = state.transition
    crashed = False

    def crash_after_quarantine(*args: object, **kwargs: object) -> None:
        nonlocal crashed
        if kwargs.get("event_type") == "deletion_quarantined" and not crashed:
            crashed = True
            raise SystemExit("crash after quarantine")
        original_transition(*args, **kwargs)

    state.transition = crash_after_quarantine
    with pytest.raises(SystemExit, match="after quarantine"):
        worker.run_once()

    quarantine = next(tmp_path.glob(".rquant-gc-*"))
    assert not files[StorageTier.HOT.value].exists()
    assert quarantine.read_bytes() == PAYLOAD
    with sqlite3.connect(state.path) as connection:
        authorization_id, authorization_json = connection.execute(
            "SELECT authorization_id, authorization_json FROM gc_runtime_work"
        ).fetchone()
    persisted = runtime.FullVerifiedDeletionAuthorization.model_validate_json(authorization_json)
    assert authorization_id == persisted.authorization_id
    assert persisted.generation_id == "a" * 64

    replacement: object = {
        "profile_generation": "e" * 64,
        "generation_id": "c" * 64,
        "receipt_id": "f" * 64,
        "verified_at": NOW - timedelta(seconds=1),
    }[rotation]
    authority[rotation] = replacement
    restarted_state, restarted = _worker(
        runtime,
        tmp_path,
        catalog,
        transport,
        max_attempts=1,
        deletion_gate=RotatingGate(),
    )
    result = restarted.run_once()

    assert result.failed == result.dead_lettered == 1
    assert quarantine.read_bytes() == PAYLOAD
    assert len(catalog.list_active_copies(CONTENT_SHA256)) == 3
    assert restarted_state.dead_letter_count() == 1
    health = runtime.ArtifactGcHealthProjector(
        catalog=catalog,
        state=restarted_state,
        quarantine_inspector=transport,
    ).snapshot(now=NOW)
    assert health.quarantine_orphan_count == 1


def test_worker_requires_current_full_verified_recovery_authorization(
    tmp_path: Path,
) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )
    state = runtime.ArtifactGcRuntimeStore(
        tmp_path / "gc-state.sqlite3",
        managed_trust_root=tmp_path,
    )
    arguments = {
        "catalog": catalog,
        "state": state,
        "transport": transport,
        "policy": _policy(),
        "config": runtime.GcWorkerConfig(
            batch_items=1,
            batch_bytes=1024,
            max_runtime=timedelta(seconds=5),
            lease_ttl=timedelta(seconds=30),
            max_attempts=1,
            retry_delay=timedelta(0),
        ),
        "worker_id": "gate-review",
        "clock": lambda: NOW,
    }

    with pytest.raises(TypeError, match="deletion_gate"):
        runtime.ArtifactGcWorker(**arguments)

    class LightweightGate:
        def authorize(self, candidate: object, *, as_of: datetime) -> dict[str, object]:
            del candidate
            return {
                "profile": "current",
                "profile_generation": "d" * 64,
                "generation_id": "a" * 64,
                "receipt_id": "b" * 64,
                "verification_level": "directory_verified",
                "verified_at": as_of,
            }

    denied = runtime.ArtifactGcWorker(**arguments, deletion_gate=LightweightGate()).run_once()

    assert denied.failed == denied.dead_lettered == 1
    assert all(path.exists() for path in files.values())


def test_exact_recovery_deletion_gate_uses_public_full_verified_loader() -> None:
    runtime = _runtime_module()
    calls: list[dict[str, object]] = []
    current = SimpleNamespace(
        generation_id="a" * 64,
        target_profile_generation="d" * 64,
        published_at=NOW - timedelta(minutes=2),
    )
    receipt = SimpleNamespace(
        receipt_id="b" * 64,
        status="succeeded",
        manifest_id="a" * 64,
        published_generation_id="a" * 64,
        target_profile_generation="d" * 64,
        completed_at=NOW - timedelta(minutes=1),
    )

    def load_full_verified(**kwargs: object) -> tuple[object, object]:
        calls.append(kwargs)
        return current, receipt

    restore_root = Path("/private/recovery")
    target = SimpleNamespace(
        manifest_id="a" * 64,
        target_profile_generation="d" * 64,
    )
    verifier = object()
    budget = object()
    gate = runtime.ExactFullVerifiedRecoveryDeletionGate(
        restore_root=restore_root,
        receipt_id="b" * 64,
        target=target,
        fixed_replay_verifier=verifier,
        verification_budget=budget,
        max_recovery_age=timedelta(days=30),
        loader=load_full_verified,
    )

    authorization = gate.authorize(object(), as_of=NOW)

    assert calls == [
        {
            "restore_root": restore_root,
            "receipt_id": "b" * 64,
            "target": target,
            "fixed_replay_verifier": verifier,
            "verification_budget": budget,
        }
    ]
    assert authorization.profile == "current"
    assert authorization.profile_generation == "d" * 64
    assert authorization.generation_id == "a" * 64
    assert authorization.receipt_id == "b" * 64
    assert authorization.verification_level == "full_verified"
    assert authorization.verified_at == NOW - timedelta(minutes=1)
    assert authorization.recovery_completed_at == NOW - timedelta(minutes=1)
    assert authorization.current_published_at == NOW - timedelta(minutes=2)
    assert authorization.expires_at == NOW - timedelta(minutes=1) + timedelta(days=30)


def test_exact_recovery_deletion_gate_rejects_ten_year_old_receipt() -> None:
    runtime = _runtime_module()
    completed_at = NOW - timedelta(days=3650)

    def load_stale(**_kwargs: object) -> tuple[object, object]:
        return (
            SimpleNamespace(
                generation_id="a" * 64,
                target_profile_generation="d" * 64,
                published_at=completed_at - timedelta(seconds=1),
            ),
            SimpleNamespace(
                receipt_id="b" * 64,
                status="succeeded",
                manifest_id="a" * 64,
                published_generation_id="a" * 64,
                target_profile_generation="d" * 64,
                completed_at=completed_at,
            ),
        )

    gate = runtime.ExactFullVerifiedRecoveryDeletionGate(
        restore_root=Path("/private/recovery"),
        receipt_id="b" * 64,
        target=SimpleNamespace(
            manifest_id="a" * 64,
            target_profile_generation="d" * 64,
        ),
        fixed_replay_verifier=object(),
        max_recovery_age=timedelta(days=30),
        loader=load_stale,
    )

    with pytest.raises(ValueError, match="expired|age|stale"):
        gate.authorize(object(), as_of=NOW)


def test_exact_recovery_deletion_gate_rejects_noncurrent_receipt_binding() -> None:
    runtime = _runtime_module()

    def load_conflicting(**_kwargs: object) -> tuple[object, object]:
        return (
            SimpleNamespace(
                generation_id="a" * 64,
                target_profile_generation="d" * 64,
                published_at=NOW - timedelta(minutes=2),
            ),
            SimpleNamespace(
                receipt_id="b" * 64,
                status="succeeded",
                manifest_id="a" * 64,
                published_generation_id="c" * 64,
                target_profile_generation="d" * 64,
                completed_at=NOW - timedelta(minutes=1),
            ),
        )

    gate = runtime.ExactFullVerifiedRecoveryDeletionGate(
        restore_root=Path("/private/recovery"),
        receipt_id="b" * 64,
        target=SimpleNamespace(
            manifest_id="a" * 64,
            target_profile_generation="d" * 64,
        ),
        fixed_replay_verifier=object(),
        max_recovery_age=timedelta(days=30),
        loader=load_conflicting,
    )

    with pytest.raises(ValueError, match="current|generation|receipt"):
        gate.authorize(object(), as_of=NOW)


def test_worker_rejects_recovery_generation_change_during_deletion(tmp_path: Path) -> None:
    runtime = _runtime_module()
    catalog, files = _catalog_with_local_tiers(tmp_path)
    transport = runtime.LocalAtomicArtifactTransport(
        managed_root=tmp_path,
        clock=lambda: NOW,
        schema_resolver=lambda _descriptor: SCHEMA_SHA256,
    )

    class SwitchingFullGate:
        calls = 0

        def authorize(self, candidate: object, *, as_of: datetime) -> dict[str, object]:
            del candidate
            self.calls += 1
            return {
                "profile": "current",
                "profile_generation": "d" * 64,
                "generation_id": ("a" if self.calls == 1 else "c") * 64,
                "receipt_id": "b" * 64,
                "verification_level": "full_verified",
                "verified_at": NOW - timedelta(minutes=1),
                "recovery_completed_at": NOW - timedelta(minutes=1),
                "current_published_at": NOW - timedelta(minutes=2),
                "expires_at": NOW + timedelta(days=30),
            }

    state, worker = _worker(
        runtime,
        tmp_path,
        catalog,
        transport,
        max_attempts=1,
        deletion_gate=SwitchingFullGate(),
    )

    result = worker.run_once()

    assert result.failed == result.dead_lettered == 1
    assert all(path.exists() for path in files.values())
    assert state.dead_letter_count() == 1
