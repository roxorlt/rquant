from __future__ import annotations

import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunSpec,
    DailyStageSpec,
    DailyStageState,
    StageResult,
)

NOW = datetime(2026, 8, 3, 9, 0, tzinfo=UTC)


def _spec() -> DailyRunSpec:
    return DailyRunSpec(
        mode=DailyPipelineMode.SHADOW,
        trade_date=date(2026, 8, 3),
        source_generation_id="a" * 64,
        source_content_hash="b" * 64,
        command_manifest_hash="e" * 64,
        code_commit="c" * 40,
        profile_hash="d" * 64,
        stages=(
            DailyStageSpec(stage_id="capture"),
            DailyStageSpec(stage_id="validate", depends_on=("capture",)),
        ),
    )


def _profile(tmp_path: Path) -> DailyPipelineStorageProfile:
    return DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="d" * 64,
    )


def _ledger(profile: DailyPipelineStorageProfile) -> DailyPipelineLedger:
    return DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")


def test_crash_after_receipt_before_state_is_idempotently_recovered(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    first = _ledger(profile)
    old_lease = first.acquire_writer(owner="daily-close", now=NOW, lease_for=timedelta(seconds=2))
    run = first.create_run(old_lease, _spec(), now=NOW)
    attempt = first.claim_next(old_lease, now=NOW)
    assert attempt is not None
    receipt = first.prepare_success(
        old_lease,
        attempt,
        StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        now=NOW + timedelta(seconds=1),
    )

    restarted = _ledger(profile)
    next_lease = restarted.acquire_writer(
        owner="daily-close", now=NOW + timedelta(seconds=3), lease_for=timedelta(minutes=1)
    )
    summary = restarted.recover(next_lease, now=NOW + timedelta(seconds=3))

    assert summary.finalized_receipt_ids == (receipt.receipt_id,)
    assert restarted.stage(run.run_id, "capture").state is DailyStageState.SUCCEEDED
    validate = restarted.claim_next(next_lease, now=NOW + timedelta(seconds=4))
    assert validate is not None
    assert validate.stage_id == "validate"


def test_crash_without_receipt_retries_but_never_skips_dependency(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    first = _ledger(profile)
    old_lease = first.acquire_writer(owner="daily-close", now=NOW, lease_for=timedelta(seconds=1))
    run = first.create_run(old_lease, _spec(), now=NOW)
    attempt = first.claim_next(old_lease, now=NOW)
    assert attempt is not None

    restarted = _ledger(profile)
    next_lease = restarted.acquire_writer(
        owner="daily-close", now=NOW + timedelta(seconds=2), lease_for=timedelta(minutes=1)
    )
    restarted.recover(next_lease, now=NOW + timedelta(seconds=2))

    assert restarted.stage(run.run_id, "validate").state is DailyStageState.PENDING
    retry = restarted.claim_next(next_lease, now=NOW + timedelta(seconds=2))
    assert retry is not None
    assert retry.stage_id == "capture"
    assert retry.attempt_number == 2


def test_competing_claims_use_independent_connections_and_only_one_wins(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    primary = _ledger(profile)
    lease = primary.acquire_writer(owner="daily-close", now=NOW, lease_for=timedelta(minutes=1))
    primary.create_run(
        lease,
        DailyRunSpec(
            mode=DailyPipelineMode.SHADOW,
            trade_date=date(2026, 8, 3),
            source_generation_id="a" * 64,
            source_content_hash="b" * 64,
            command_manifest_hash="e" * 64,
            code_commit="c" * 40,
            profile_hash="d" * 64,
            stages=(DailyStageSpec(stage_id="capture"),),
        ),
        now=NOW,
    )
    barrier = threading.Barrier(2)

    def claim_once() -> str | None:
        ledger = _ledger(profile)
        barrier.wait(timeout=5)
        attempt = ledger.claim_next(lease, now=NOW + timedelta(seconds=1))
        return None if attempt is None else attempt.stage_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        left = executor.submit(claim_once)
        right = executor.submit(claim_once)

    winners = [result for result in (left.result(), right.result()) if result is not None]
    assert winners == ["capture"]


def test_writer_reopen_waits_for_independent_lock_holder_then_acquires_fresh_lease(
    tmp_path: Path,
) -> None:
    profile = _profile(tmp_path)
    path = profile.state_path
    worker = _ledger(profile)
    blocker = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    blocker.execute("PRAGMA journal_mode = WAL")
    blocker.execute("BEGIN IMMEDIATE")
    blocker.execute(
        "UPDATE daily_pipeline_writer SET fencing_token = fencing_token WHERE singleton = 1"
    )

    started = threading.Event()

    def acquire_after_wait() -> tuple[int, float]:
        started.set()
        begin = time.monotonic()
        lease = worker.acquire_writer(
            owner="daily-close",
            now=NOW + timedelta(seconds=1),
            lease_for=timedelta(minutes=1),
        )
        return lease.fencing_token, time.monotonic() - begin

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(acquire_after_wait)
        assert started.wait(timeout=5)
        time.sleep(0.25)
        blocker.commit()
        blocker.close()
        token, elapsed = future.result(timeout=5)

    assert token == 1
    assert elapsed >= 0.2


def test_recover_finalizes_prepared_receipt_before_claiming_dependent_stage(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    path = profile.state_path
    first = _ledger(profile)
    old_lease = first.acquire_writer(owner="daily-close", now=NOW, lease_for=timedelta(seconds=1))
    run = first.create_run(old_lease, _spec(), now=NOW)
    attempt = first.claim_next(old_lease, now=NOW)
    assert attempt is not None
    receipt = first.prepare_success(
        old_lease,
        attempt,
        StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        now=NOW,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE daily_pipeline_run SET state = ? WHERE run_id = ?",
            ("running", run.run_id),
        )

    restarted = _ledger(profile)
    next_lease = restarted.acquire_writer(
        owner="daily-close", now=NOW + timedelta(seconds=2), lease_for=timedelta(minutes=1)
    )
    summary = restarted.recover(next_lease, now=NOW + timedelta(seconds=2), limit=1)

    assert summary.finalized_receipt_ids == (receipt.receipt_id,)
    dependent = restarted.claim_next(next_lease, now=NOW + timedelta(seconds=2))
    assert dependent is not None
    assert dependent.stage_id == "validate"


def test_recover_cursor_rejects_invalid_token(tmp_path: Path) -> None:
    ledger = _ledger(_profile(tmp_path))
    lease = ledger.acquire_writer(owner="daily-close", now=NOW, lease_for=timedelta(minutes=1))

    with pytest.raises(DailyPipelineLedgerError, match="cursor"):
        ledger.recover(lease, now=NOW, limit=1, cursor="not-a-valid-cursor")
