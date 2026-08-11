from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.daily_ledger_fence import DailyLedgerFenceGuard
from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunSpec,
    DailyStageSpec,
    StageResult,
)

NOW = datetime(2026, 8, 3, 9, 0, tzinfo=UTC)


def _run_spec(*, source_generation_id: str = "a" * 64) -> DailyRunSpec:
    return DailyRunSpec(
        run_id="daily-fence-primary",
        mode=DailyPipelineMode.PRODUCTION,
        trade_date=date(2026, 8, 3),
        source_generation_id=source_generation_id,
        source_content_hash="b" * 64,
        command_manifest_hash="e" * 64,
        code_commit="c" * 40,
        profile_hash="d" * 64,
        stages=(DailyStageSpec(stage_id="screen"),),
    )


def test_production_guard_uses_public_ledger_fence_for_source_input_and_expiry(
    tmp_path: Path,
) -> None:
    profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.PRODUCTION,
        profile_hash="d" * 64,
    )
    ledger = DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")
    lease = ledger.acquire_writer(
        owner="daily-close",
        now=NOW,
        lease_for=timedelta(minutes=5),
    )
    run = ledger.create_run(lease, _run_spec(), now=NOW)
    second_run = ledger.create_run(
        lease,
        _run_spec(source_generation_id="e" * 64).model_copy(
            update={"run_id": "daily-fence-secondary"}
        ),
        now=NOW,
    )
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None
    guard = DailyLedgerFenceGuard(ledger=ledger, lease=lease)

    with guard(attempt, NOW + timedelta(seconds=1)) as fence:
        fence.assert_current(NOW + timedelta(seconds=1))
        fence.assert_source("a" * 64, "b" * 64)
        fence.assert_input(run.input_identity)
        with pytest.raises(DailyPipelineLedgerError, match="source identity is stale"):
            fence.assert_source("e" * 64, "b" * 64)
        with pytest.raises(DailyPipelineLedgerError, match="input identity is stale"):
            fence.assert_input("f" * 64)

    ledger.succeed(
        lease,
        attempt,
        StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
        now=NOW + timedelta(seconds=2),
    )
    second_attempt = ledger.claim_next(lease, now=NOW + timedelta(seconds=3))
    assert second_attempt is not None and second_attempt.run_id == second_run.run_id
    with guard(second_attempt, NOW + timedelta(seconds=3)) as fence:
        with pytest.raises(DailyPipelineLedgerError, match="source identity is stale"):
            fence.assert_source("a" * 64, "b" * 64)
        with pytest.raises(DailyPipelineLedgerError, match="input identity is stale"):
            fence.assert_input(run.input_identity)

    second_connection = DailyPipelineLedger(
        storage_profile=profile,
        service_owner="daily-close",
    )
    second_connection.acquire_writer(
        owner="daily-close",
        now=NOW + timedelta(seconds=2),
        lease_for=timedelta(minutes=5),
    )
    with (
        pytest.raises(DailyPipelineLedgerError, match="writer lease is stale"),
        guard(second_attempt, NOW + timedelta(seconds=4)),
    ):
        pass


def test_production_guard_rejects_a_naturally_expired_lease(tmp_path: Path) -> None:
    profile = DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.PRODUCTION,
        profile_hash="d" * 64,
    )
    ledger = DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")
    lease = ledger.acquire_writer(
        owner="daily-close",
        now=NOW,
        lease_for=timedelta(seconds=1),
    )
    run = ledger.create_run(lease, _run_spec(), now=NOW)
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None

    with (
        pytest.raises(DailyPipelineLedgerError, match="writer lease is stale"),
        DailyLedgerFenceGuard(ledger=ledger, lease=lease)(
            attempt,
            NOW + timedelta(seconds=1),
        ),
    ):
        pass

    assert ledger.run(run.run_id).run_id == run.run_id
