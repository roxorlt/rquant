"""Persisted positive evidence for completed data audits."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.data_metadata import DataAuditRun, DataAuditRunFinalization
from rquant.storage.duckdb import DuckDBStore

OBSERVED_AT = datetime(2026, 7, 15, 8, tzinfo=UTC)


def _run() -> DataAuditRun:
    return DataAuditRun.create(
        as_of_date=date(2026, 7, 14),
        range_start=date(2026, 4, 1),
        range_end=date(2026, 7, 14),
        rule_set_version="stage1-v1",
        observed_at=OBSERVED_AT,
    )


def test_data_audit_run_lifecycle_is_fail_closed() -> None:
    run = _run()

    assert run.status == "running"
    assert len(run.audit_run_id) == 64
    with pytest.raises(ValidationError, match="completed"):
        DataAuditRun.model_validate(
            {
                **run.model_dump(exclude_computed_fields=True),
                "status": "completed",
            }
        )


def test_data_audit_run_round_trip_and_latest_completed(tmp_path: Path) -> None:
    with DuckDBStore(tmp_path / "audit.duckdb") as store:
        begun = store.begin_data_audit_run(_run())
        finalized = store.finalize_data_audit_run(
            begun.audit_run_id,
            DataAuditRunFinalization(
                finding_issue_ids=("a" * 64,),
                p0_count=0,
                completed_at=OBSERVED_AT + timedelta(minutes=3),
            ),
        )

        assert finalized.status == "completed"
        assert store.get_data_audit_run(begun.audit_run_id) == finalized
        assert store.latest_completed_data_audit_run(as_of_date=date(2026, 7, 14)) == finalized


def test_data_audit_real_terminal_transition_invokes_artifact_outbox_hook(
    tmp_path: Path,
) -> None:
    events: list[tuple[str, str, datetime]] = []
    with DuckDBStore(
        tmp_path / "audit-hook.duckdb",
        artifact_terminal_hook=lambda owner_type, owner_id, observed_at: events.append(
            (owner_type, owner_id, observed_at)
        ),
    ) as store:
        begun = store.begin_data_audit_run(_run())
        assert events == []
        completed_at = OBSERVED_AT + timedelta(minutes=3)
        store.finalize_data_audit_run(
            begun.audit_run_id,
            DataAuditRunFinalization(p0_count=0, completed_at=completed_at),
        )

    assert events == [("audit", begun.audit_run_id, completed_at)]


def test_failed_audit_is_not_returned_as_completed(tmp_path: Path) -> None:
    with DuckDBStore(tmp_path / "audit.duckdb") as store:
        begun = store.begin_data_audit_run(_run())
        failed = store.fail_data_audit_run(
            begun.audit_run_id,
            error_message="source read failed",
            completed_at=OBSERVED_AT + timedelta(minutes=1),
        )

        assert failed.status == "failed"
        assert failed.error_message == "source read failed"
        assert store.latest_completed_data_audit_run(as_of_date=date(2026, 7, 14)) is None


def test_latest_audit_can_be_observed_after_the_covered_backtest_range(
    tmp_path: Path,
) -> None:
    running = DataAuditRun.create(
        as_of_date=date(2026, 7, 15),
        range_start=date(2026, 4, 1),
        range_end=date(2026, 7, 14),
        rule_set_version="stage1-v2",
        observed_at=OBSERVED_AT,
    )
    with DuckDBStore(tmp_path / "audit-after-range.duckdb") as store:
        store.begin_data_audit_run(running)
        completed = store.finalize_data_audit_run(
            running.audit_run_id,
            DataAuditRunFinalization(
                p0_count=0,
                completed_at=OBSERVED_AT + timedelta(minutes=1),
            ),
        )

        selected = store.latest_completed_data_audit_run(
            as_of_date=date(2026, 7, 14),
            range_start=date(2026, 4, 1),
            range_end=date(2026, 7, 14),
        )

    assert selected == completed
