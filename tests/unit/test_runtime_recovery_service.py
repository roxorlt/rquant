from __future__ import annotations

import gc
import hashlib
import json
import os
import resource
import sqlite3
import stat
import sys
import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_recovery_artifacts import (
    FixedReplayReceipt,
    RealRecoveryIntegrityError,
    RealRecoveryReceipt,
)
from rquant.runtime_recovery_service import (
    LegacyRecoveryServiceReceipt,
    RecoveryServiceIntegrityError,
    RecoveryServiceLease,
    RecoveryServiceLeaseLostError,
    RecoveryServiceReceipt,
    RuntimeRecoveryService,
    load_verified_recovery_service_receipts,
)
from rquant.strict_json import canonical_json_bytes

MANIFEST_ID = canonical_sha256({"manifest": "service-test"})
TOOL_ID = canonical_sha256({"tool": "service-test"})
PROFILE_ID = canonical_sha256({"profile": "service-test"})
COMMIT = "a" * 40
LEGACY_RECEIPT_FIXTURE = Path(__file__).parents[1] / "fixtures" / "recovery_service_receipt_v1.json"


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


def _recovery_receipt(operation: str = "a" * 32) -> RealRecoveryReceipt:
    return RealRecoveryReceipt(
        operation_id=operation,
        status="succeeded",
        manifest_id=MANIFEST_ID,
        tool_bundle_id=TOOL_ID,
        target_commit=COMMIT,
        target_profile_generation=PROFILE_ID,
        published_generation_id=MANIFEST_ID,
        fixed_replays=tuple(
            FixedReplayReceipt(
                strategy_id=strategy_id,
                replay_fingerprint=canonical_sha256({"strategy": strategy_id}),
            )
            for strategy_id in ("auction_gap", "growth_board_surge", "n_shape")
        ),
        started_at=datetime(2026, 8, 2, 1, 0, tzinfo=UTC),
        completed_at=datetime(2026, 8, 2, 1, 1, tzinfo=UTC),
    )


def _service(tmp_path: Path, clock: _Clock, *, worker_id: str) -> RuntimeRecoveryService:
    return RuntimeRecoveryService(
        state_path=tmp_path / "state" / "recovery.sqlite3",
        receipt_root=tmp_path / "receipts",
        worker_id=worker_id,
        clock=clock.now,
        lease_seconds=10,
        max_attempts=3,
        retry_delay_seconds=2,
    )


def _insert_receipt_index(
    connection: sqlite3.Connection,
    *,
    receipt: dict[str, object],
    raw: bytes,
    relative_path: str,
) -> None:
    connection.execute(
        """
        INSERT INTO recovery_receipt(
            receipt_id, job_id, fence, status, verification_level,
            recovery_receipt_id, content_sha256, relative_path, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            receipt["receipt_id"],
            receipt["job_id"],
            receipt["fence"],
            receipt["status"],
            receipt["verification_level"],
            receipt["recovery_receipt_id"],
            hashlib.sha256(raw).hexdigest(),
            relative_path,
            str(receipt["completed_at"]).replace("Z", ".000000Z"),
        ),
    )


def _downgrade_to_v3(connection: sqlite3.Connection) -> None:
    connection.execute("DROP INDEX recovery_receipt_migration_status_idx")
    connection.execute("DROP TABLE recovery_receipt_migration")
    connection.execute("DROP INDEX recovery_receipt_outbox_created_idx")
    connection.execute("DROP TABLE recovery_receipt_outbox")
    connection.execute("PRAGMA user_version = 3")
    connection.execute("UPDATE recovery_metadata SET value = '3' WHERE key = 'schema_version'")


def _install_historical_v1_receipt(
    service: RuntimeRecoveryService,
) -> tuple[dict[str, object], bytes]:
    raw = LEGACY_RECEIPT_FIXTURE.read_bytes()
    legacy = json.loads(raw)
    assert canonical_json_bytes(legacy) == raw
    legacy_path = service.receipt_root / f"{legacy['receipt_id']}.json"
    legacy_path.write_bytes(raw)
    legacy_path.chmod(0o400)
    return legacy, raw


def _migration_events(connection: sqlite3.Connection) -> list[dict[str, object]]:
    return [
        json.loads(row[0])["event"]
        for row in connection.execute("SELECT event_json FROM recovery_audit ORDER BY sequence")
        if json.loads(row[0])["event"].get("event") == "legacy_receipt_upgraded"
    ]


def _schema_objects(connection: sqlite3.Connection) -> set[tuple[str, str]]:
    return {
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        )
    }


def _submit(service: RuntimeRecoveryService, tmp_path: Path, clock: _Clock, **kwargs):
    manifest = tmp_path / "backup" / "target.json"
    tool = tmp_path / "backup" / "tool.json"
    restore = tmp_path / "restore"
    manifest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    restore.mkdir(mode=0o700, exist_ok=True)
    manifest.write_text("{}", encoding="utf-8")
    tool.write_text("{}", encoding="utf-8")
    return service.submit(
        request_id="request-1",
        backup_root=manifest.parent,
        manifest_path=manifest,
        tool_bundle_path=tool,
        restore_root=restore,
        deadline_at=clock.now() + timedelta(minutes=5),
        **kwargs,
    )


def test_service_persists_success_receipt_and_is_idempotent(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(service, tmp_path, clock)
    calls: list[str] = []

    def execute(lease: RecoveryServiceLease) -> RealRecoveryReceipt:
        calls.append(lease.job.job_id)
        lease.checkpoint("verified", {"artifact_index": 12})
        return _recovery_receipt()

    result = service.run_once(execute)
    repeated = service.submit_from_record(job)

    assert result is not None and result.status == "succeeded"
    assert repeated.job_id == job.job_id
    assert calls == [job.job_id]
    persisted = service.job(job.job_id)
    assert persisted.status == "succeeded"
    assert persisted.checkpoint_stage == "verified"
    receipts = tuple((tmp_path / "receipts").glob("*.json"))
    assert len(receipts) == 1
    document = json.loads(receipts[0].read_text(encoding="utf-8"))
    assert document["recovery_receipt_id"] == _recovery_receipt().receipt_id
    verified = service.verified_receipts(job_id=job.job_id)
    assert len(verified) == 1
    assert verified[0].receipt_id == result.service_receipt_id
    assert verified[0].verification_level == "full"
    assert service.run_once(execute) is None


def test_job_identity_is_stable_when_only_deadline_changes(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    first = _submit(service, tmp_path, clock)

    repeated = service.submit(
        request_id=first.request_id,
        backup_root=Path(first.backup_root),
        manifest_path=Path(first.manifest_path),
        tool_bundle_path=Path(first.tool_bundle_path),
        restore_root=Path(first.restore_root),
        deadline_at=clock.now() + timedelta(minutes=10),
    )

    assert repeated.job_id == first.job_id


def test_repeated_timer_submission_is_idempotent_within_one_cycle(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    first = _submit(service, tmp_path, clock)

    for offset in range(1, 25):
        repeated = service.submit(
            request_id=first.request_id,
            backup_root=Path(first.backup_root),
            manifest_path=Path(first.manifest_path),
            tool_bundle_path=Path(first.tool_bundle_path),
            restore_root=Path(first.restore_root),
            deadline_at=clock.now() + timedelta(minutes=5, seconds=offset),
        )
        assert repeated.job_id == first.job_id

    connection = sqlite3.connect(tmp_path / "state" / "recovery.sqlite3")
    try:
        assert int(connection.execute("SELECT COUNT(*) FROM recovery_job").fetchone()[0]) == 1
    finally:
        connection.close()


def test_verified_receipts_fail_closed_on_file_tamper(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(service, tmp_path, clock)
    service.run_once(lambda _lease: _recovery_receipt())
    receipt_path = next((tmp_path / "receipts").glob("*.json"))
    receipt_path.chmod(0o600)
    receipt_path.write_text("{}", encoding="utf-8")

    with pytest.raises(RecoveryServiceIntegrityError, match="receipt"):
        service.verified_receipts(job_id=job.job_id)


def test_readonly_receipt_loader_does_not_mutate_service_state(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(service, tmp_path, clock)
    service.run_once(lambda _lease: _recovery_receipt())
    state = tmp_path / "state" / "recovery.sqlite3"
    before = state.read_bytes()

    receipts = load_verified_recovery_service_receipts(
        state_path=state,
        receipt_root=tmp_path / "receipts",
        job_id=job.job_id,
    )

    assert len(receipts) == 1
    assert receipts[0].status == "succeeded"
    assert state.read_bytes() == before


def test_expired_lease_resumes_from_checkpoint_after_hard_crash(tmp_path: Path) -> None:
    clock = _Clock()
    first = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(first, tmp_path, clock)

    def crash(lease: RecoveryServiceLease) -> RealRecoveryReceipt:
        lease.checkpoint("copied", {"artifact_index": 4})
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        first.run_once(crash)
    assert first.job(job.job_id).status == "running"

    clock.advance(11)
    second = _service(tmp_path, clock, worker_id="worker-b")
    observed: list[tuple[str | None, object]] = []

    def resume(lease: RecoveryServiceLease) -> RealRecoveryReceipt:
        observed.append((lease.job.checkpoint_stage, lease.job.checkpoint))
        return _recovery_receipt("b" * 32)

    result = second.run_once(resume)

    assert result is not None and result.status == "succeeded"
    assert observed == [("copied", {"artifact_index": 4})]
    assert second.job(job.job_id).attempt_count == 2
    assert second.job(job.job_id).fence == 2


def test_exhausted_crashed_lease_emits_immutable_failure_receipt(tmp_path: Path) -> None:
    clock = _Clock()
    service = RuntimeRecoveryService(
        state_path=tmp_path / "state" / "recovery.sqlite3",
        receipt_root=tmp_path / "receipts",
        worker_id="worker-a",
        clock=clock.now,
        lease_seconds=10,
        max_attempts=1,
        retry_delay_seconds=2,
    )
    job = _submit(service, tmp_path, clock)

    with pytest.raises(KeyboardInterrupt):
        service.run_once(lambda _lease: (_ for _ in ()).throw(KeyboardInterrupt))
    clock.advance(11)

    assert service.run_once(lambda _lease: _recovery_receipt()) is None
    failed = service.job(job.job_id)
    assert failed.status == "failed"
    receipts = tuple((tmp_path / "receipts").glob("*.json"))
    assert len(receipts) == 1
    payload = json.loads(receipts[0].read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["error_type"] == "ExpiredRecoveryLease"
    assert payload["verification_level"] is None


def test_concurrent_services_claim_one_fenced_attempt(tmp_path: Path) -> None:
    clock = _Clock()
    first = _service(tmp_path, clock, worker_id="worker-a")
    second = _service(tmp_path, clock, worker_id="worker-b")
    _submit(first, tmp_path, clock)
    barrier = threading.Barrier(2)
    calls: list[int] = []
    lock = threading.Lock()

    def execute(lease: RecoveryServiceLease) -> RealRecoveryReceipt:
        with lock:
            calls.append(lease.fence)
        return _recovery_receipt()

    def run(service: RuntimeRecoveryService):
        barrier.wait()
        return service.run_once(execute)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(run, (first, second)))

    assert sum(result is not None for result in results) == 1
    assert calls == [1]


def test_retry_budget_and_deadline_produce_immutable_failure(tmp_path: Path) -> None:
    clock = _Clock()
    service = RuntimeRecoveryService(
        state_path=tmp_path / "state" / "recovery.sqlite3",
        receipt_root=tmp_path / "receipts",
        worker_id="worker-a",
        clock=clock.now,
        lease_seconds=10,
        max_attempts=2,
        retry_delay_seconds=2,
    )
    job = _submit(service, tmp_path, clock)

    def fail(_lease: RecoveryServiceLease) -> RealRecoveryReceipt:
        raise RuntimeError("restore unavailable")

    first = service.run_once(fail)
    assert first is not None and first.status == "retry_scheduled"
    clock.advance(2)
    second = service.run_once(fail)

    assert second is not None and second.status == "failed"
    persisted = service.job(job.job_id)
    assert persisted.status == "failed"
    assert persisted.attempt_count == 2
    receipts = tuple(
        json.loads(path.read_text()) for path in (tmp_path / "receipts").glob("*.json")
    )
    assert {(item["fence"], item["status"]) for item in receipts} == {
        (1, "retry_scheduled"),
        (2, "failed"),
    }
    assert {item["error_type"] for item in receipts} == {"RuntimeError"}


@pytest.mark.parametrize(
    ("outcome", "expected_job_status", "expected_receipt_status"),
    (
        ("success", "succeeded", "succeeded"),
        ("permanent_failure", "failed", "failed"),
        ("transient_failure", "pending", "retry_scheduled"),
    ),
)
def test_receipt_outbox_recovers_crash_after_fenced_transition_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    expected_job_status: str,
    expected_receipt_status: str,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(service, tmp_path, clock)

    def crash_before_physical_publish(_receipt: object) -> Path:
        raise KeyboardInterrupt("crash after receipt intent commit")

    monkeypatch.setattr(service, "_write_service_receipt", crash_before_physical_publish)

    def execute(_lease: RecoveryServiceLease) -> RealRecoveryReceipt:
        if outcome == "permanent_failure":
            raise RealRecoveryIntegrityError("signed artifact is invalid")
        if outcome == "transient_failure":
            raise OSError("temporary receipt filesystem outage")
        return _recovery_receipt()

    with pytest.raises(KeyboardInterrupt, match="intent commit"):
        service.run_once(execute)

    assert service.job(job.job_id).status == expected_job_status
    connection = sqlite3.connect(service.state_path)
    try:
        assert int(connection.execute("SELECT COUNT(*) FROM recovery_receipt").fetchone()[0]) == 0
        assert (
            int(connection.execute("SELECT COUNT(*) FROM recovery_receipt_outbox").fetchone()[0])
            == 1
        )
    finally:
        connection.close()
    assert not tuple(service.receipt_root.glob("*.json"))

    restarted = _service(tmp_path, clock, worker_id="reconciler")
    verified = restarted.verified_receipts(job_id=job.job_id)

    assert len(verified) == 1
    assert verified[0].status == expected_receipt_status
    connection = sqlite3.connect(restarted.state_path)
    try:
        assert (
            int(connection.execute("SELECT COUNT(*) FROM recovery_receipt_outbox").fetchone()[0])
            == 0
        )
    finally:
        connection.close()


def test_stale_fence_cannot_stage_or_publish_failure_receipt(tmp_path: Path) -> None:
    clock = _Clock()
    stale = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(stale, tmp_path, clock)
    fence_one = stale._claim()
    assert fence_one is not None and fence_one.fence == 1

    clock.advance(11)
    current = _service(tmp_path, clock, worker_id="worker-b")
    fence_two = current._claim()
    assert fence_two is not None and fence_two.fence == 2
    before_files = {path.name for path in stale.receipt_root.glob("*.json")}
    connection = sqlite3.connect(stale.state_path)
    try:
        before_outbox = int(
            connection.execute("SELECT COUNT(*) FROM recovery_receipt_outbox").fetchone()[0]
        )
    finally:
        connection.close()

    with pytest.raises(RecoveryServiceLeaseLostError, match="fence|lease"):
        stale._complete_failure(fence_one, RealRecoveryIntegrityError("stale failure"))

    assert {path.name for path in stale.receipt_root.glob("*.json")} == before_files
    connection = sqlite3.connect(stale.state_path)
    try:
        assert (
            int(connection.execute("SELECT COUNT(*) FROM recovery_receipt_outbox").fetchone()[0])
            == before_outbox
        )
        assert (
            int(
                connection.execute(
                    "SELECT COUNT(*) FROM recovery_receipt_outbox WHERE job_id = ? AND fence = 1 "
                    "AND payload_json LIKE '%stale failure%'",
                    (job.job_id,),
                ).fetchone()[0]
            )
            == 0
        )
    finally:
        connection.close()


def test_reconciler_quarantines_unindexed_orphan_receipt(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    orphan = service.receipt_root / f"{'f' * 64}.json"
    orphan.write_text('{"orphan":true}', encoding="utf-8")
    orphan.chmod(0o400)
    partial = service.receipt_root / ".interrupted-receipt.tmp"
    partial.write_bytes(b"partial")
    partial.chmod(0o600)

    _service(tmp_path, clock, worker_id="reconciler")

    assert not orphan.exists()
    assert not partial.exists()
    quarantined = tuple((service.receipt_root / ".quarantine").iterdir())
    assert len(quarantined) == 2
    assert {path.read_bytes() for path in quarantined} == {b'{"orphan":true}', b"partial"}


def test_service_migrates_v3_state_to_receipt_outbox_schema(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    connection = sqlite3.connect(service.state_path)
    try:
        connection.execute("DROP INDEX recovery_receipt_outbox_created_idx")
        connection.execute("DROP TABLE recovery_receipt_outbox")
        connection.execute("PRAGMA user_version = 3")
        connection.execute("UPDATE recovery_metadata SET value = '3' WHERE key = 'schema_version'")
        connection.commit()
    finally:
        connection.close()

    migrated = _service(tmp_path, clock, worker_id="worker-b")
    connection = sqlite3.connect(migrated.state_path)
    try:
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == 4
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'recovery_receipt_outbox'"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT value FROM recovery_metadata WHERE key = 'schema_version'"
        ).fetchone() == ("4",)
    finally:
        connection.close()


def test_downgrade_helper_builds_true_v3_without_v4_migration_objects(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    connection = sqlite3.connect(service.state_path)
    try:
        _downgrade_to_v3(connection)
        connection.commit()
        objects = _schema_objects(connection)
        assert ("table", "recovery_receipt_migration") not in objects
        assert ("index", "recovery_receipt_migration_status_idx") not in objects
        assert ("table", "recovery_receipt_outbox") not in objects
        assert ("index", "recovery_receipt_outbox_created_idx") not in objects
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == 3
        assert connection.execute(
            "SELECT value FROM recovery_metadata WHERE key = 'schema_version'"
        ).fetchone() == ("3",)
    finally:
        connection.close()

    migrated = _service(tmp_path, clock, worker_id="worker-b")
    connection = sqlite3.connect(migrated.state_path)
    try:
        assert ("table", "recovery_receipt_migration") in _schema_objects(connection)
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == 4
    finally:
        connection.close()


def test_v3_to_v4_migration_upgrades_historical_v1_receipt_for_readonly_verifier(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    legacy_path = service.receipt_root / f"{legacy['receipt_id']}.json"

    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=legacy_path.name,
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    migrated = _service(tmp_path, clock, worker_id="worker-b")

    receipts = load_verified_recovery_service_receipts(
        state_path=migrated.state_path,
        receipt_root=migrated.receipt_root,
    )

    assert len(receipts) == 1
    assert receipts[0].schema_version == 2
    assert receipts[0].job_id == legacy["job_id"]
    assert receipts[0].receipt_id != legacy["receipt_id"]
    archive = migrated.receipt_root / ".legacy-v1" / legacy_path.name
    assert archive.read_bytes() == raw


def test_readonly_receipt_loader_rejects_indexed_v1_receipt_without_migration(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RecoveryServiceIntegrityError, match="contract|receipt|schema"):
        load_verified_recovery_service_receipts(
            state_path=service.state_path,
            receipt_root=service.receipt_root,
        )


def test_v3_to_v4_receipt_migration_is_idempotent_and_audited(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    first = _service(tmp_path, clock, worker_id="worker-b")
    second = _service(tmp_path, clock, worker_id="worker-c")

    assert first.verified_receipts() == second.verified_receipts()
    connection = sqlite3.connect(second.state_path)
    try:
        events = tuple(
            json.loads(row[0])
            for row in connection.execute("SELECT event_json FROM recovery_audit ORDER BY sequence")
        )
    finally:
        connection.close()
    upgrades = [
        item["event"] for item in events if item["event"]["event"] == "legacy_receipt_upgraded"
    ]
    assert upgrades == [
        {
            "event": "legacy_receipt_upgraded",
            "legacy_receipt_id": legacy["receipt_id"],
            "receipt_id": first.verified_receipts()[0].receipt_id,
        }
    ]


def test_v3_to_v4_receipt_migration_rejects_corrupt_or_forged_v1_receipt(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    legacy_path = service.receipt_root / f"{legacy['receipt_id']}.json"
    corrupted = raw.replace(b'"status":"succeeded"', b'"status":"failed"')
    legacy_path.chmod(0o600)
    legacy_path.write_bytes(corrupted)
    legacy_path.chmod(0o400)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=corrupted,
            relative_path=legacy_path.name,
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RecoveryServiceIntegrityError, match="legacy recovery receipt"):
        _service(tmp_path, clock, worker_id="worker-b")

    assert legacy_path.read_bytes() == corrupted
    assert not tuple((service.receipt_root / ".legacy-v1").glob("*.json"))


def test_v3_to_v4_receipt_migration_supports_mixed_v1_and_v2_inventory(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    current = RecoveryServiceReceipt(
        job_id="d" * 64,
        fence=2,
        status="failed",
        error_type="HistoricalFailure",
        error_message="already current",
        completed_at=clock.now(),
    )
    current_path = service._write_service_receipt(current)
    current_raw = canonical_json_bytes(current.model_dump(mode="json"))
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _insert_receipt_index(
            connection,
            receipt=current.model_dump(mode="json"),
            raw=current_raw,
            relative_path=current_path.name,
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    migrated = _service(tmp_path, clock, worker_id="worker-b")

    assert {receipt.job_id for receipt in migrated.verified_receipts()} == {
        legacy["job_id"],
        current.job_id,
    }


def test_v3_to_v4_receipt_migration_recovers_after_atomic_publish_before_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    original_archive = RuntimeRecoveryService._archive_v3_receipt

    def crash_after_v2_publish(*args, **kwargs):
        raise OSError("injected archive interruption")

    monkeypatch.setattr(RuntimeRecoveryService, "_archive_v3_receipt", crash_after_v2_publish)
    with pytest.raises(OSError, match="injected archive interruption"):
        _service(tmp_path, clock, worker_id="worker-b")
    monkeypatch.setattr(RuntimeRecoveryService, "_archive_v3_receipt", original_archive)

    recovered = _service(tmp_path, clock, worker_id="worker-c")

    assert len(recovered.verified_receipts()) == 1
    assert (recovered.receipt_root / ".legacy-v1" / f"{legacy['receipt_id']}.json").exists()


@pytest.mark.parametrize("mutation", ("delete", "tamper"))
def test_v3_to_v4_receipt_migration_rejects_missing_or_tampered_v2_after_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    legacy_receipt = LegacyRecoveryServiceReceipt.model_validate(legacy)
    upgraded = RuntimeRecoveryService._upgrade_v3_receipt(legacy_receipt)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    def crash_before_index(self: RuntimeRecoveryService, *args, **kwargs) -> None:
        raise KeyboardInterrupt("crash before v2 index")

    monkeypatch.setattr(RuntimeRecoveryService, "_ensure_v3_migration_indexed", crash_before_index)
    with pytest.raises(KeyboardInterrupt, match="v2 index"):
        _service(tmp_path, clock, worker_id="worker-b")
    monkeypatch.undo()

    upgraded_path = service.receipt_root / f"{upgraded.receipt_id}.json"
    assert upgraded_path.exists()
    connection = sqlite3.connect(service.state_path)
    try:
        assert connection.execute(
            "SELECT status FROM recovery_receipt_migration WHERE legacy_receipt_id = ?",
            (legacy["receipt_id"],),
        ).fetchone() == ("v2_published",)
    finally:
        connection.close()
    if mutation == "delete":
        upgraded_path.unlink()
    else:
        upgraded_path.chmod(0o600)
        upgraded_path.write_bytes(b'{"tampered":true}')
        upgraded_path.chmod(0o400)

    with pytest.raises(RecoveryServiceIntegrityError, match="receipt|migration|v2"):
        _service(tmp_path, clock, worker_id="worker-c")

    connection = sqlite3.connect(service.state_path)
    try:
        assert connection.execute(
            "SELECT receipt_id FROM recovery_receipt WHERE job_id = ? AND fence = ?",
            (legacy["job_id"], legacy["fence"]),
        ).fetchone() == (legacy["receipt_id"],)
    finally:
        connection.close()


def test_completed_v3_to_v4_migration_attestation_rejects_missing_v2_receipt(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    migrated = _service(tmp_path, clock, worker_id="worker-b")
    upgraded = migrated.verified_receipts()[0]
    (migrated.receipt_root / f"{upgraded.receipt_id}.json").unlink()

    with pytest.raises(RecoveryServiceIntegrityError, match="receipt|migration|v2"):
        _service(tmp_path, clock, worker_id="worker-c")
    with pytest.raises(RecoveryServiceIntegrityError, match="receipt"):
        load_verified_recovery_service_receipts(
            state_path=migrated.state_path,
            receipt_root=migrated.receipt_root,
        )


def test_v3_to_v4_receipt_migration_resumes_v2_index_without_audit_or_completion(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    legacy_receipt = LegacyRecoveryServiceReceipt.model_validate(legacy)
    upgraded = RuntimeRecoveryService._upgrade_v3_receipt(legacy_receipt)
    upgraded_path = service._write_service_receipt(upgraded)
    upgraded_raw = canonical_json_bytes(upgraded.model_dump(mode="json"))
    archive = service.receipt_root / ".legacy-v1"
    archive.mkdir(mode=0o700)
    legacy_path = service.receipt_root / f"{legacy['receipt_id']}.json"
    legacy_path.replace(archive / legacy_path.name)

    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=upgraded.model_dump(mode="json"),
            raw=upgraded_raw,
            relative_path=upgraded_path.name,
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    recovered = _service(tmp_path, clock, worker_id="worker-b")

    assert len(recovered.verified_receipts()) == 1
    connection = sqlite3.connect(recovered.state_path)
    try:
        assert _migration_events(connection) == [
            {
                "event": "legacy_receipt_upgraded",
                "legacy_receipt_id": legacy["receipt_id"],
                "receipt_id": upgraded.receipt_id,
            }
        ]
        assert connection.execute(
            """
            SELECT status, completed_at FROM recovery_receipt_migration
            WHERE legacy_receipt_id = ?
            """,
            (legacy["receipt_id"],),
        ).fetchone() == ("completed", "2026-08-02T01:00:00.000000Z")
    finally:
        connection.close()
    assert (recovered.receipt_root / ".legacy-v1" / f"{legacy['receipt_id']}.json").read_bytes()
    assert not (recovered.receipt_root / f"{legacy['receipt_id']}.json").exists()


def test_v3_to_v4_receipt_migration_restarts_after_index_before_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    original_append = RuntimeRecoveryService._append_audit
    injected = False

    def crash_upgrade_audit(
        self: RuntimeRecoveryService,
        connection: sqlite3.Connection,
        event: object,
        *,
        now: datetime,
    ) -> None:
        nonlocal injected
        if (
            isinstance(event, dict)
            and event.get("event") == "legacy_receipt_upgraded"
            and not injected
        ):
            injected = True
            raise KeyboardInterrupt("crash before migration audit")
        original_append(self, connection, event, now=now)

    monkeypatch.setattr(RuntimeRecoveryService, "_append_audit", crash_upgrade_audit)
    with pytest.raises(KeyboardInterrupt, match="migration audit"):
        _service(tmp_path, clock, worker_id="worker-b")
    monkeypatch.setattr(RuntimeRecoveryService, "_append_audit", original_append)

    for worker in ("worker-c", "worker-d", "worker-e"):
        recovered = _service(tmp_path, clock, worker_id=worker)

    assert len(recovered.verified_receipts()) == 1
    connection = sqlite3.connect(recovered.state_path)
    try:
        events = _migration_events(connection)
        assert len(events) == 1
        assert events[0]["legacy_receipt_id"] == legacy["receipt_id"]
        assert connection.execute(
            "SELECT status FROM recovery_receipt_migration WHERE legacy_receipt_id = ?",
            (legacy["receipt_id"],),
        ).fetchone() == ("completed",)
    finally:
        connection.close()


def test_v3_to_v4_receipt_migration_rejects_archive_directory_substitution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_service as service_module

    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    original_replace = service_module.os.replace
    original_rename = service_module.os.rename
    substituted = False

    def substitute_archive_directory() -> None:
        archive = service.receipt_root / ".legacy-v1"
        moved = service.receipt_root / ".legacy-v1-replaced"
        if archive.exists() and not moved.exists():
            archive.rename(moved)
            archive.mkdir(mode=0o777)
            archive.chmod(0o777)

    def replacing_replace(src: object, dst: object, *args: object, **kwargs: object) -> None:
        nonlocal substituted
        if not substituted and str(dst).endswith(f".legacy-v1/{legacy['receipt_id']}.json"):
            substituted = True
            substitute_archive_directory()
        original_replace(src, dst, *args, **kwargs)

    def replacing_rename(src: object, dst: object, *args: object, **kwargs: object) -> None:
        nonlocal substituted
        if not substituted and str(dst) == f"{legacy['receipt_id']}.json":
            substituted = True
            substitute_archive_directory()
        original_rename(src, dst, *args, **kwargs)

    monkeypatch.setattr(service_module.os, "replace", replacing_replace)
    monkeypatch.setattr(service_module.os, "rename", replacing_rename)

    with pytest.raises(RecoveryServiceIntegrityError, match="archive|directory|changed|unsafe"):
        _service(tmp_path, clock, worker_id="worker-b")

    archive = service.receipt_root / ".legacy-v1"
    assert substituted is True
    assert stat.S_IMODE(os.lstat(archive).st_mode) == 0o777


def test_v3_to_v4_receipt_migration_rejects_archive_replacement_after_move_before_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    original_archive = RuntimeRecoveryService._archive_v3_receipt
    replaced = False

    def archive_then_replace(
        self: RuntimeRecoveryService,
        *,
        source: Path,
        receipt: LegacyRecoveryServiceReceipt,
    ) -> tuple[Path, tuple[tuple[int, ...], tuple[int, ...]]]:
        nonlocal replaced
        archive_result = original_archive(self, source=source, receipt=receipt)
        archive = self.receipt_root / ".legacy-v1"
        moved = self.receipt_root / ".legacy-v1-after-move"
        archive.rename(moved)
        archive.mkdir(mode=0o700)
        replacement = archive / f"{legacy['receipt_id']}.json"
        replacement.write_bytes(raw)
        replacement.chmod(0o400)
        replaced = True
        return archive_result

    monkeypatch.setattr(RuntimeRecoveryService, "_archive_v3_receipt", archive_then_replace)

    with pytest.raises(RecoveryServiceIntegrityError, match="archive|directory|changed|receipt"):
        _service(tmp_path, clock, worker_id="worker-b")

    assert replaced is True
    connection = sqlite3.connect(service.state_path)
    try:
        assert connection.execute(
            "SELECT status FROM recovery_receipt_migration WHERE legacy_receipt_id = ?",
            (legacy["receipt_id"],),
        ).fetchone() == ("indexed",)
    finally:
        connection.close()


def test_completed_v3_to_v4_migration_attestation_rejects_archived_legacy_tamper(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    migrated = _service(tmp_path, clock, worker_id="worker-b")
    archive = migrated.receipt_root / ".legacy-v1" / f"{legacy['receipt_id']}.json"
    assert archive.read_bytes() == raw
    archive.chmod(0o600)
    archive.write_bytes(b'{"tampered":true}')
    archive.chmod(0o400)

    with pytest.raises(RecoveryServiceIntegrityError, match="archive|legacy|receipt"):
        _service(tmp_path, clock, worker_id="worker-c")


@pytest.mark.parametrize(
    "crash_stage",
    (
        "after_intent",
        "before_v2_publish",
        "after_v2_publish",
        "before_index",
        "before_archive",
        "before_audit",
        "before_complete",
    ),
)
def test_v3_to_v4_receipt_migration_fault_injects_every_state_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_stage: str,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    injected = False

    def maybe_crash(stage: str) -> None:
        nonlocal injected
        if crash_stage == stage and not injected:
            injected = True
            raise KeyboardInterrupt(f"crash {stage}")

    original_stage_intent = RuntimeRecoveryService._stage_v3_migration_intent
    original_write = RuntimeRecoveryService._write_service_receipt
    original_index = RuntimeRecoveryService._ensure_v3_migration_indexed
    original_archive = RuntimeRecoveryService._archive_v3_receipt
    original_append = RuntimeRecoveryService._append_audit
    original_advance = RuntimeRecoveryService._advance_v3_migration_status

    def stage_intent_then_maybe_crash(self: RuntimeRecoveryService, *args, **kwargs):
        row = original_stage_intent(self, *args, **kwargs)
        maybe_crash("after_intent")
        return row

    def write_with_crash(self: RuntimeRecoveryService, receipt: RecoveryServiceReceipt) -> Path:
        maybe_crash("before_v2_publish")
        path = original_write(self, receipt)
        maybe_crash("after_v2_publish")
        return path

    def index_with_crash(self: RuntimeRecoveryService, *args, **kwargs) -> None:
        maybe_crash("before_index")
        original_index(self, *args, **kwargs)

    def archive_with_crash(self: RuntimeRecoveryService, *args, **kwargs) -> object:
        maybe_crash("before_archive")
        return original_archive(self, *args, **kwargs)

    def audit_with_crash(
        self: RuntimeRecoveryService,
        connection: sqlite3.Connection,
        event: object,
        *,
        now: datetime,
    ) -> None:
        if isinstance(event, dict) and event.get("event") == "legacy_receipt_upgraded":
            maybe_crash("before_audit")
        original_append(self, connection, event, now=now)

    def advance_with_crash(
        connection: sqlite3.Connection,
        *,
        legacy_receipt_id: str,
        status: str,
        now: datetime,
    ) -> None:
        if status == "completed":
            maybe_crash("before_complete")
        original_advance(
            connection,
            legacy_receipt_id=legacy_receipt_id,
            status=status,
            now=now,
        )

    monkeypatch.setattr(
        RuntimeRecoveryService,
        "_stage_v3_migration_intent",
        stage_intent_then_maybe_crash,
    )
    monkeypatch.setattr(RuntimeRecoveryService, "_write_service_receipt", write_with_crash)
    monkeypatch.setattr(RuntimeRecoveryService, "_ensure_v3_migration_indexed", index_with_crash)
    monkeypatch.setattr(RuntimeRecoveryService, "_archive_v3_receipt", archive_with_crash)
    monkeypatch.setattr(RuntimeRecoveryService, "_append_audit", audit_with_crash)
    monkeypatch.setattr(
        RuntimeRecoveryService,
        "_advance_v3_migration_status",
        staticmethod(advance_with_crash),
    )

    with pytest.raises(KeyboardInterrupt, match=crash_stage):
        _service(tmp_path, clock, worker_id=f"worker-{crash_stage}")
    monkeypatch.undo()

    for worker in ("worker-restart-1", "worker-restart-2", "worker-restart-3"):
        recovered = _service(tmp_path, clock, worker_id=worker)

    assert len(recovered.verified_receipts()) == 1
    connection = sqlite3.connect(recovered.state_path)
    try:
        assert _migration_events(connection) == [
            {
                "event": "legacy_receipt_upgraded",
                "legacy_receipt_id": legacy["receipt_id"],
                "receipt_id": recovered.verified_receipts()[0].receipt_id,
            }
        ]
        assert connection.execute(
            "SELECT status FROM recovery_receipt_migration WHERE legacy_receipt_id = ?",
            (legacy["receipt_id"],),
        ).fetchone() == ("completed",)
    finally:
        connection.close()


def test_v3_to_v4_receipt_migration_rejects_corrupt_persisted_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    legacy, raw = _install_historical_v1_receipt(service)
    connection = sqlite3.connect(service.state_path)
    try:
        _insert_receipt_index(
            connection,
            receipt=legacy,
            raw=raw,
            relative_path=f"{legacy['receipt_id']}.json",
        )
        _downgrade_to_v3(connection)
        connection.commit()
    finally:
        connection.close()

    original_stage_intent = RuntimeRecoveryService._stage_v3_migration_intent

    def crash_after_intent(self: RuntimeRecoveryService, *args, **kwargs):
        original_stage_intent(self, *args, **kwargs)
        raise KeyboardInterrupt("crash after intent")

    monkeypatch.setattr(RuntimeRecoveryService, "_stage_v3_migration_intent", crash_after_intent)
    with pytest.raises(KeyboardInterrupt, match="after intent"):
        _service(tmp_path, clock, worker_id="worker-b")
    monkeypatch.undo()

    connection = sqlite3.connect(service.state_path)
    try:
        connection.execute(
            "UPDATE recovery_receipt_migration SET upgraded_payload_json = ?",
            ('{"corrupt":true}',),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RecoveryServiceIntegrityError, match="migration.*payload|intent"):
        _service(tmp_path, clock, worker_id="worker-c")


def test_permanent_recovery_integrity_failure_is_not_retried_and_audits_reason(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(service, tmp_path, clock)

    result = service.run_once(
        lambda _lease: (_ for _ in ()).throw(
            RealRecoveryIntegrityError("recovery signature is invalid")
        )
    )

    assert result is not None and result.status == "failed"
    persisted = service.job(job.job_id)
    assert persisted.attempt_count == 1
    assert persisted.last_error_type == "RealRecoveryIntegrityError"
    connection = sqlite3.connect(service.state_path)
    try:
        events = tuple(
            json.loads(row[0])
            for row in connection.execute("SELECT event_json FROM recovery_audit ORDER BY sequence")
        )
    finally:
        connection.close()
    assert any(item["event"].get("error_class") == "permanent_integrity" for item in events)


def test_transient_io_failure_retries_and_audits_reason(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    _submit(service, tmp_path, clock)

    result = service.run_once(
        lambda _lease: (_ for _ in ()).throw(OSError("temporary storage unavailable"))
    )

    assert result is not None and result.status == "retry_scheduled"
    connection = sqlite3.connect(service.state_path)
    try:
        events = tuple(
            json.loads(row[0])
            for row in connection.execute("SELECT event_json FROM recovery_audit ORDER BY sequence")
        )
    finally:
        connection.close()
    assert any(item["event"].get("error_class") == "transient_io" for item in events)


def test_lost_fence_is_checked_before_service_completion_receipt_write(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    _submit(service, tmp_path, clock)

    def expire_before_completion(_lease: RecoveryServiceLease) -> RealRecoveryReceipt:
        clock.advance(11)
        return _recovery_receipt()

    with pytest.raises(RecoveryServiceIntegrityError, match="completion.*fence|lease"):
        service.run_once(expire_before_completion)

    assert not tuple(service.receipt_root.glob("*.json"))


def test_service_rejects_internal_periodic_rehearsal_scheduler(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")

    with pytest.raises(ValueError, match="systemd timer|external scheduler"):
        _submit(service, tmp_path, clock, rehearsal_interval_seconds=3600)


def test_expired_scheduled_rehearsal_is_finalized_instead_of_accumulating(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(service, tmp_path, clock)
    state = tmp_path / "state" / "recovery.sqlite3"
    connection = sqlite3.connect(state)
    try:
        connection.execute(
            """
            UPDATE recovery_job
            SET status = 'scheduled', rehearsal_interval_seconds = 60
            WHERE job_id = ?
            """,
            (job.job_id,),
        )
        connection.commit()
    finally:
        connection.close()

    assert service.run_once(lambda _lease: _recovery_receipt()) is None
    assert service.job(job.job_id).status == "scheduled"
    clock.value = job.deadline_at + timedelta(seconds=1)

    assert service.run_once(lambda _lease: _recovery_receipt()) is None
    assert service.job(job.job_id).status == "failed"
    assert any(
        receipt.status == "failed" for receipt in service.verified_receipts(job_id=job.job_id)
    )


def test_full_audit_verification_peak_allocation_stays_below_400k() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE recovery_audit(
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            previous_sha256 TEXT,
            event_sha256 TEXT NOT NULL UNIQUE,
            event_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        ) STRICT
        """
    )
    previous: str | None = None
    created_at = "2026-08-03T01:00:00.000000Z"
    for index in range(5000):
        payload = {
            "contract": "runtime-recovery-service-audit/v1",
            "previous_sha256": previous,
            "event": {"event": "fixture", "index": index},
            "created_at": created_at,
        }
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        event_sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        connection.execute(
            """
            INSERT INTO recovery_audit(
                previous_sha256, event_sha256, event_json, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (previous, event_sha, raw, created_at),
        )
        previous = event_sha
    connection.commit()
    gc.collect()
    tracemalloc.start()
    try:
        RuntimeRecoveryService._attest_audit_chain(connection)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        connection.close()

    assert peak < 400_000


def test_repeated_full_audit_verification_rss_stays_within_400k() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE recovery_audit(
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            previous_sha256 TEXT,
            event_sha256 TEXT NOT NULL UNIQUE,
            event_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        ) STRICT
        """
    )
    previous: str | None = None
    created_at = "2026-08-03T01:00:00.000000Z"
    for index in range(5000):
        payload = {
            "contract": "runtime-recovery-service-audit/v1",
            "previous_sha256": previous,
            "event": {"event": "fixture", "index": index},
            "created_at": created_at,
        }
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        event_sha = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        connection.execute(
            """
            INSERT INTO recovery_audit(
                previous_sha256, event_sha256, event_json, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (previous, event_sha, raw, created_at),
        )
        previous = event_sha
    connection.commit()

    RuntimeRecoveryService._attest_audit_chain(connection)
    gc.collect()
    unit = 1 if sys.platform == "darwin" else 1024
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
    for _ in range(10):
        RuntimeRecoveryService._attest_audit_chain(connection)
    gc.collect()
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
    connection.close()

    assert after - before <= 400_000


def test_service_fails_closed_when_schema_or_fence_is_tampered(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    _submit(service, tmp_path, clock)
    connection = sqlite3.connect(tmp_path / "state" / "recovery.sqlite3")
    try:
        connection.execute("ALTER TABLE recovery_job ADD COLUMN forged TEXT")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RecoveryServiceIntegrityError, match="schema"):
        service.run_once(lambda _lease: _recovery_receipt())


def test_service_rejects_state_database_path_replacement(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(service, tmp_path, clock)
    state = tmp_path / "state" / "recovery.sqlite3"
    moved = state.with_name("recovery-moved.sqlite3")
    state.rename(moved)
    state.symlink_to(moved)

    with pytest.raises(RecoveryServiceIntegrityError, match="state|path|unsafe"):
        service.job(job.job_id)


def test_service_rejects_rewritten_append_only_audit_chain(tmp_path: Path) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    _submit(service, tmp_path, clock)
    connection = sqlite3.connect(tmp_path / "state" / "recovery.sqlite3")
    try:
        connection.execute(
            "UPDATE recovery_audit SET event_json = ? WHERE sequence = 1",
            ('{"event":{"event":"forged"}}',),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RecoveryServiceIntegrityError, match="audit"):
        _service(tmp_path, clock, worker_id="worker-b")


def test_real_executor_loads_canonical_contracts_and_binds_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_recovery_service as service_module

    clock = _Clock()
    service = RuntimeRecoveryService(
        state_path=tmp_path / "state" / "recovery.sqlite3",
        receipt_root=tmp_path / "receipts",
        worker_id="worker-a",
        clock=clock.now,
        lease_seconds=1,
        max_attempts=3,
        retry_delay_seconds=2,
    )
    from tests.unit.test_runtime_recovery_artifacts import _build_bundle

    backup, target, tool, _replay = _build_bundle(tmp_path)
    restore = tmp_path / "restore"
    restore.mkdir(mode=0o700)
    manifest_path = backup / "target.json"
    tool_path = backup / "tool.json"
    manifest_path.write_bytes(service_module.canonical_json_bytes(target.model_dump(mode="json")))
    tool_path.write_bytes(service_module.canonical_json_bytes(tool.model_dump(mode="json")))
    job = service.submit(
        request_id="real-request",
        backup_root=backup,
        manifest_path=manifest_path,
        tool_bundle_path=tool_path,
        restore_root=restore,
        deadline_at=clock.now() + timedelta(minutes=5),
    )
    observed: dict[str, object] = {}

    class FakeRestorer:
        def __init__(self, **kwargs: object) -> None:
            observed.update(kwargs)

        def restore(self, **kwargs: object) -> RealRecoveryReceipt:
            observed.update(kwargs)
            clock.advance(0.8)
            time.sleep(0.4)
            clock.advance(0.8)
            time.sleep(0.4)
            return RealRecoveryReceipt(
                operation_id="d" * 32,
                status="succeeded",
                manifest_id=str(target.manifest_id),
                tool_bundle_id=str(tool.bundle_id),
                target_commit=target.target_commit,
                target_profile_generation=target.target_profile_generation,
                published_generation_id=str(target.manifest_id),
                fixed_replays=tuple(
                    FixedReplayReceipt(
                        strategy_id=strategy_id,
                        replay_fingerprint=canonical_sha256({"strategy": strategy_id}),
                    )
                    for strategy_id in ("auction_gap", "growth_board_surge", "n_shape")
                ),
                started_at=clock.now(),
                completed_at=clock.now(),
            )

    monkeypatch.setattr(service_module, "RealRecoveryRestorer", FakeRestorer)
    result = service.run_real_once(
        signature_verifier=object(),
        fixed_replay_verifier=object(),
    )

    assert result is not None and result.status == "succeeded"
    assert observed["backup_root"] == backup
    assert observed["restore_root"] == restore
    assert observed["target"] == target
    assert observed["tool_bundle"] == tool
    assert callable(observed["publication_fence"])
    assert callable(observed["cancelled"])
    persisted = service.job(job.job_id)
    assert persisted.checkpoint_stage == "recovery-published"
    assert persisted.checkpoint == {"recovery_receipt_id": result.recovery_receipt_id}


def test_real_executor_rejects_noncanonical_or_replaced_contract_file(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    service = _service(tmp_path, clock, worker_id="worker-a")
    job = _submit(service, tmp_path, clock)
    Path(job.manifest_path).write_text('{"forged": true}\n', encoding="utf-8")

    result = service.run_real_once(
        signature_verifier=object(),
        fixed_replay_verifier=object(),
    )

    assert result is not None and result.status == "failed"
    assert service.job(job.job_id).last_error_type == "RecoveryServiceIntegrityError"
