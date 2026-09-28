"""The Lab audit projection uses only a bounded, stable metadata snapshot."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.data_metadata import DataAuditRun, DataAuditRunFinalization, DataQualityIssue
from rquant.serving_page_projection_source import (
    DuckDBLabPageProjectionSource,
    PageProjectionSourceIntegrityError,
)
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 9, 24, 8, tzinfo=UTC)


def _run(observed_at: datetime) -> DataAuditRun:
    return DataAuditRun.create(
        as_of_date=date(2026, 9, 23),
        range_start=date(2026, 9, 1),
        range_end=date(2026, 9, 23),
        rule_set_version="stage1-v3",
        observed_at=observed_at,
    )


def _rows(path: Path) -> dict[str, tuple[object, ...]]:
    snapshot = DuckDBLabPageProjectionSource(path)(NOW)
    return {item.table_name: item.rows for item in snapshot.projections}


def test_no_audit_is_explicitly_never_run(tmp_path: Path) -> None:
    path = tmp_path / "research.duckdb"
    with DuckDBStore(path):
        pass

    rows = _rows(path)

    assert rows["data_audit_status"][0]["latest_status"] == "never_run"
    assert rows["data_audit_status"][0]["successful_audit_id"] is None
    assert rows["data_audit_issue"] == ()


def test_later_running_attempt_keeps_success_and_exact_issue_membership(tmp_path: Path) -> None:
    path = tmp_path / "research.duckdb"
    issue = DataQualityIssue.detected(
        rule_id="minute-without-daily",
        dataset_id="minute_bar",
        severity="P1",
        scope_key="private/path/2026-09-01",
        message="do not publish /private/path",
        evidence={"secret": "/private/path"},
        observed_at=NOW - timedelta(hours=2),
    )
    with DuckDBStore(path) as store:
        store.record_data_quality_issue(issue)
        completed = store.begin_data_audit_run(_run(NOW - timedelta(hours=2)))
        store.finalize_data_audit_run(
            completed.audit_run_id,
            DataAuditRunFinalization(
                finding_issue_ids=(issue.issue_id,),
                p0_count=0,
                completed_at=NOW - timedelta(hours=1),
            ),
        )
        store.begin_data_audit_run(_run(NOW - timedelta(minutes=5)))

    rows = _rows(path)
    status = rows["data_audit_status"][0]
    assert status["latest_status"] == "running"
    assert status["successful_audit_id"] == completed.audit_run_id
    assert status["finding_count"] == 1
    assert len(rows["data_audit_issue"]) == 1
    projected = rows["data_audit_issue"][0]
    assert projected["audit_run_id"] == completed.audit_run_id
    assert projected["dataset_id"] == "minute_bar"
    assert projected["status"] == "open"
    assert "private" not in str(rows)


def test_missing_issue_in_completed_audit_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "research.duckdb"
    with DuckDBStore(path) as store:
        run = store.begin_data_audit_run(_run(NOW - timedelta(hours=2)))
        store.finalize_data_audit_run(
            run.audit_run_id,
            DataAuditRunFinalization(
                finding_issue_ids=("a" * 64,),
                p0_count=0,
                completed_at=NOW - timedelta(hours=1),
            ),
        )

    with pytest.raises(PageProjectionSourceIntegrityError, match="issue.*missing"):
        _rows(path)


def test_later_issue_severity_keeps_historical_audit_count(tmp_path: Path) -> None:
    path = tmp_path / "research.duckdb"
    first = DataQualityIssue.detected(
        rule_id="minute-without-daily",
        dataset_id="minute_bar",
        severity="P0",
        scope_key="2026-09-01",
        message="first",
        observed_at=NOW - timedelta(hours=2),
    )
    with DuckDBStore(path) as store:
        store.record_data_quality_issue(first)
        run = store.begin_data_audit_run(_run(NOW - timedelta(hours=2)))
        store.finalize_data_audit_run(
            run.audit_run_id,
            DataAuditRunFinalization(
                finding_issue_ids=(first.issue_id,),
                p0_count=1,
                completed_at=NOW - timedelta(hours=1),
            ),
        )
        store.record_data_quality_issue(
            DataQualityIssue.detected(
                rule_id=first.rule_id,
                dataset_id=first.dataset_id,
                severity="P1",
                scope_key=first.scope_key,
                message="current",
                observed_at=NOW - timedelta(minutes=5),
            )
        )

    rows = _rows(path)
    assert rows["data_audit_status"][0]["p0_count"] == 1
    assert rows["data_audit_issue"][0]["severity"] == "P1"


def test_audit_issue_budget_fails_explicitly_before_issue_scan(tmp_path: Path) -> None:
    path = tmp_path / "research.duckdb"
    with DuckDBStore(path):
        pass
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "INSERT INTO data_audit_run VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "a" * 64,
                date(2026, 9, 23),
                date(2026, 9, 1),
                date(2026, 9, 23),
                "stage1-v3",
                "completed",
                "[" + ",".join('"' + str(index).zfill(64) + '"' for index in range(257)) + "]",
                0,
                NOW - timedelta(hours=2),
                NOW - timedelta(hours=1),
                None,
            ),
        )

    with pytest.raises(PageProjectionSourceIntegrityError, match="issue.*limit"):
        _rows(path)


@pytest.mark.parametrize(
    "findings", ["{}", '["' + "x" * 50_000 + '"]'], ids=["object", "oversized"]
)
def test_malformed_or_oversized_finding_list_cannot_look_healthy(
    tmp_path: Path, findings: str
) -> None:
    path = tmp_path / "research.duckdb"
    with DuckDBStore(path):
        pass
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "INSERT INTO data_audit_run VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "a" * 64,
                date(2026, 9, 23),
                date(2026, 9, 1),
                date(2026, 9, 23),
                "stage1-v3",
                "completed",
                findings,
                0,
                NOW - timedelta(hours=2),
                NOW - timedelta(hours=1),
                None,
            ),
        )

    with pytest.raises(PageProjectionSourceIntegrityError, match="finding list"):
        _rows(path)
