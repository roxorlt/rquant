"""研究数据快照、覆盖率和质量问题的 typed storage 行为。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.storage.duckdb import DuckDBStore


def test_stable_ids_normalize_equivalent_inputs() -> None:
    from rquant.data_metadata import DataQualityIssue, DatasetSnapshot

    as_of_utc = datetime(2026, 7, 13, 6, 30, tzinfo=UTC)
    as_of_cst = as_of_utc.astimezone(timezone(timedelta(hours=8)))
    left = DatasetSnapshot.create(
        strategy_name="growth-board-surge",
        manifest_id="manifest-42",
        as_of_time=as_of_utc,
        code_commit="abc123",
        origin="local-research",
    )
    right = DatasetSnapshot.create(
        strategy_name=" growth-board-surge ",
        manifest_id="manifest-42",
        as_of_time=as_of_cst,
        code_commit="abc123",
        origin="local-research",
    )
    issue_a = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute_bar",
        severity="P1",
        scope_key="trade-date:2026-07-13",
        message="coverage incomplete",
        evidence={"missing_count": 2},
        observed_at=as_of_utc,
    )
    issue_b = DataQualityIssue.detected(
        rule_id=" minute-coverage ",
        dataset_id="minute_bar",
        severity="P1",
        scope_key="trade-date:2026-07-13",
        message="new scan message does not affect identity",
        evidence={"missing_count": 1},
        observed_at=as_of_utc + timedelta(minutes=1),
    )

    assert left.snapshot_id == right.snapshot_id
    assert issue_a.issue_id == issue_b.issue_id
    assert len(left.snapshot_id) == 64
    assert len(issue_a.issue_id) == 64
    int(left.snapshot_id, 16)
    int(issue_a.issue_id, 16)


def test_quality_issue_evidence_supports_nested_json_values() -> None:
    from rquant.data_metadata import DataQualityIssue

    issue = DataQualityIssue.detected(
        rule_id="nested-evidence",
        dataset_id="minute-bars",
        severity="P2",
        scope_key="trade-date:2026-07-13",
        message="nested evidence remains typed",
        evidence={
            "sample": {
                "ts_code": "600000.SH",
                "checks": [True, None, 3],
            }
        },
        observed_at=datetime(2026, 7, 13, 6, 30, tzinfo=UTC),
    )

    assert issue.evidence["sample"] == {
        "ts_code": "600000.SH",
        "checks": [True, None, 3],
    }


def test_dataset_coverage_derives_missing_count_and_ratio() -> None:
    from rquant.data_metadata import DatasetCoverage

    coverage = DatasetCoverage(
        snapshot_id="snapshot-1",
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=4,
        available_count=3,
    )
    empty = DatasetCoverage(
        snapshot_id="snapshot-1",
        dataset_id="empty-universe",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=0,
        available_count=0,
    )

    assert coverage.missing_count == 1
    assert coverage.coverage_ratio == pytest.approx(0.75)
    assert empty.missing_count == 0
    assert empty.coverage_ratio is None


def test_dataset_coverage_rejects_invalid_or_caller_derived_counts() -> None:
    from rquant.data_metadata import DatasetCoverage

    base = {
        "snapshot_id": "snapshot-1",
        "dataset_id": "minute-bars",
        "coverage_scope": "trade-date:2026-07-13",
        "table_name": "minute_bar",
    }
    with pytest.raises(ValidationError, match="available_count.*expected_count"):
        DatasetCoverage(**base, expected_count=2, available_count=3)
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        DatasetCoverage(**base, expected_count=-1, available_count=0)
    with pytest.raises(ValidationError, match="missing_count"):
        DatasetCoverage(
            **base,
            expected_count=2,
            available_count=1,
            missing_count=0,
        )
    with pytest.raises(ValidationError, match="coverage_ratio"):
        DatasetCoverage(
            **base,
            expected_count=0,
            available_count=0,
            coverage_ratio=1.0,
        )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: __import__(
            "rquant.data_metadata", fromlist=["DatasetSnapshot"]
        ).DatasetSnapshot.create(
            strategy_name="strategy",
            as_of_time=datetime(2026, 7, 13, 6, 30),
            code_commit="abc123",
            origin="local",
        ),
        lambda: __import__(
            "rquant.data_metadata", fromlist=["DatasetCoverage"]
        ).DatasetCoverage(
            snapshot_id="snapshot-1",
            dataset_id="minute-bars",
            coverage_scope="all",
            table_name="minute_bar",
            expected_count=0,
            available_count=0,
            created_at=datetime(2026, 7, 13, 6, 30),
        ),
        lambda: __import__(
            "rquant.data_metadata", fromlist=["DataQualityIssue"]
        ).DataQualityIssue.detected(
            rule_id="rule",
            dataset_id="minute-bars",
            severity="P2",
            scope_key="all",
            message="bad time",
            observed_at=datetime(2026, 7, 13, 6, 30),
        ),
        lambda: __import__(
            "rquant.data_metadata", fromlist=["DatasetSnapshotFinalization"]
        ).DatasetSnapshotFinalization(
            completed_at=datetime(2026, 7, 13, 6, 30)
        ),
    ],
)
def test_models_reject_naive_business_times(factory: object) -> None:
    with pytest.raises(ValidationError, match="timezone-aware UTC"):
        factory()  # type: ignore[operator]


def test_metadata_storage_lifecycle_and_typed_json_round_trip(
    tmp_path: Path,
) -> None:
    from rquant.data_metadata import (
        DataQualityIssue,
        DatasetCoverage,
        DatasetSnapshot,
        DatasetSnapshotFinalization,
    )

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="growth-board-surge",
        manifest_id="manifest-42",
        as_of_time=t0,
        code_commit="abc123",
        origin="local-research",
        created_at=t0,
    )
    retry = DatasetSnapshot.create(
        strategy_name="growth-board-surge",
        manifest_id="manifest-42",
        as_of_time=t0,
        code_commit="abc123",
        origin="local-research",
        created_at=t0 + timedelta(minutes=1),
    )
    first_coverage = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=8,
        missing_reasons=("two symbols absent",),
        created_at=t0,
    )
    updated_coverage = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=9,
        missing_reasons=("one symbol absent",),
        created_at=t0 + timedelta(minutes=1),
    )
    first_issue = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P1",
        scope_key="trade-date:2026-07-13",
        message="two symbols absent",
        evidence={"missing_count": 2, "symbols": ["000001.SZ", "600000.SH"]},
        observed_at=t0,
    )

    with DuckDBStore(tmp_path / "metadata.duckdb") as store:
        begun = store.begin_dataset_snapshot(snapshot)
        begun_again = store.begin_dataset_snapshot(retry)
        assert begun.status == "building"
        assert begun_again.created_at == t0

        stored_coverage = store.upsert_dataset_coverage(first_coverage)
        assert stored_coverage.missing_count == 2
        stored_coverage = store.upsert_dataset_coverage(updated_coverage)
        stored_coverage = store.upsert_dataset_coverage(updated_coverage)
        assert stored_coverage.available_count == 9
        assert stored_coverage.missing_count == 1
        assert stored_coverage.coverage_ratio == pytest.approx(0.9)
        assert stored_coverage.missing_reasons == ("one symbol absent",)
        assert stored_coverage.created_at == t0

        opened = store.record_data_quality_issue(first_issue)
        assert opened.status == "open"
        resolved = store.resolve_data_quality_issue(
            first_issue.issue_id,
            resolved_at=t0 + timedelta(minutes=2),
        )
        assert resolved.status == "resolved"
        assert resolved.resolved_at == t0 + timedelta(minutes=2)

        rescanned = DataQualityIssue.detected(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P0",
            scope_key="trade-date:2026-07-13",
            message="one symbol still absent",
            evidence={"missing_count": 1, "symbols": ["600000.SH"]},
            observed_at=t0 + timedelta(minutes=3),
        )
        reopened = store.record_data_quality_issue(rescanned)
        assert reopened.issue_id == first_issue.issue_id
        assert reopened.status == "open"
        assert reopened.severity == "P0"
        assert reopened.first_seen_at == t0
        assert reopened.last_seen_at == t0 + timedelta(minutes=3)
        assert reopened.resolved_at is None
        assert reopened.evidence == {"missing_count": 1, "symbols": ["600000.SH"]}

        finalized = store.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(
                table_watermarks={"minute_bar": "2026-07-13T06:00:00Z"},
                quality_issue_ids=(reopened.issue_id,),
                completed_at=t0 + timedelta(minutes=4),
            ),
        )
        assert finalized.status == "ready"
        assert finalized.completed_at == t0 + timedelta(minutes=4)
        assert finalized.table_watermarks == {
            "minute_bar": "2026-07-13T06:00:00Z"
        }
        assert finalized.quality_issue_ids == (reopened.issue_id,)
        assert store.get_dataset_snapshot(snapshot.snapshot_id) == finalized
        assert store.list_dataset_coverages(snapshot.snapshot_id) == [stored_coverage]
        assert store.list_snapshot_quality_issues(snapshot.snapshot_id) == [reopened]


def test_metadata_queries_and_transitions_explain_missing_ids(tmp_path: Path) -> None:
    from rquant.data_metadata import DatasetSnapshotFinalization

    with DuckDBStore(tmp_path / "missing.duckdb") as store:
        assert store.get_dataset_snapshot("missing") is None
        assert store.list_dataset_coverages("missing") == []
        assert store.list_snapshot_quality_issues("missing") == []
        with pytest.raises(KeyError, match="dataset snapshot.*missing"):
            store.finalize_dataset_snapshot(
                "missing", DatasetSnapshotFinalization()
            )
        with pytest.raises(KeyError, match="data quality issue.*missing"):
            store.resolve_data_quality_issue("missing")
