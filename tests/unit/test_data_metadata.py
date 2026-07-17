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


def _execution_manifest(
    snapshot_id: str,
    *,
    reverse_artifacts: bool = False,
) -> object:
    from rquant.data_metadata import (
        DatasetSnapshotArtifact,
        DatasetSnapshotBindingManifest,
    )

    artifacts = [
        DatasetSnapshotArtifact(
            artifact_type="lake_partition",
            dataset_id="minute_bar",
            table_name="minute_bar",
            artifact_key="minute_bar:2026-07-14:1min",
            relative_path=(
                "minute/freq=1min/year=2026/month=07/"
                "trade_date=2026-07-14/versions/" + "a" * 64 + ".parquet"
            ),
            row_count=241,
            schema_hash="b" * 64,
            content_hash="c" * 64,
            file_hash="a" * 64,
        ),
        DatasetSnapshotArtifact(
            artifact_type="materialized_table",
            dataset_id="daily_bar",
            table_name="daily_bar",
            artifact_key="daily_bar:2026-04-01:2026-07-14",
            relative_path="tables/daily_bar.parquet",
            row_count=42,
            schema_hash="d" * 64,
            content_hash="e" * 64,
            file_hash="f" * 64,
        ),
    ]
    if reverse_artifacts:
        artifacts.reverse()
    return DatasetSnapshotBindingManifest(
        snapshot_id=snapshot_id,
        strategy_name="growth_board_surge",
        start_date=datetime(2026, 4, 1, tzinfo=UTC).date(),
        end_date=datetime(2026, 7, 14, tzinfo=UTC).date(),
        as_of_time=datetime(2026, 7, 15, 8, tzinfo=UTC),
        code_commit="1" * 40,
        dependency_contract_version="stage1-v1",
        builder_version="snapshot-builder-v1",
        artifacts=tuple(artifacts),
    )


def test_execution_binding_hash_is_canonical_and_path_independent() -> None:
    from rquant.data_metadata import DatasetSnapshotBinding

    manifest = _execution_manifest("9" * 64)
    reordered = _execution_manifest("9" * 64, reverse_artifacts=True)
    left = DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="/srv/rquant/research-lake",
        manifest_relative_path="snapshots/9/manifest.json",
        created_at=datetime(2026, 7, 15, 8, tzinfo=UTC),
    )
    right = DatasetSnapshotBinding.create(
        manifest=reordered,
        artifact_root="/Users/test/rquant/research-lake",
        manifest_relative_path="relocated/snapshots/9/manifest.json",
        created_at=datetime(2026, 7, 15, 8, tzinfo=UTC),
    )

    assert manifest.manifest_hash == reordered.manifest_hash
    assert left.binding_hash == right.binding_hash
    assert left.artifact_root != right.artifact_root
    assert len(left.binding_hash) == 64
    assert len(manifest.manifest_hash) == 64


def test_execution_binding_requires_ready_parent_and_is_immutable(
    tmp_path: Path,
) -> None:
    from rquant.data_metadata import (
        DatasetSnapshot,
        DatasetSnapshotBinding,
        DatasetSnapshotBindingFinalization,
        DatasetSnapshotFinalization,
    )

    t0 = datetime(2026, 7, 15, 8, tzinfo=UTC)
    parent = DatasetSnapshot.create(
        strategy_name="growth_board_surge",
        manifest_id="m" * 64,
        as_of_time=t0,
        code_commit="1" * 40,
        origin="test",
        created_at=t0,
    )
    manifest = _execution_manifest(parent.snapshot_id)
    binding = DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="/srv/rquant/research-lake",
        manifest_relative_path=(
            f"snapshots/{parent.snapshot_id}/{manifest.manifest_hash}/manifest.json"
        ),
        created_at=t0,
    )
    finalization = DatasetSnapshotBindingFinalization(
        completed_at=t0 + timedelta(minutes=2)
    )

    with DuckDBStore(tmp_path / "binding.duckdb") as store:
        store.begin_dataset_snapshot(parent)
        with pytest.raises(ValueError, match="ready dataset snapshot"):
            store.begin_dataset_snapshot_binding(binding)

        store.finalize_dataset_snapshot(
            parent.snapshot_id,
            DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=1)),
        )
        begun = store.begin_dataset_snapshot_binding(binding)
        begun_again = store.begin_dataset_snapshot_binding(
            binding.model_copy(update={"created_at": t0 + timedelta(seconds=30)})
        )
        assert begun.status == "building"
        assert begun_again.created_at == t0

        ready = store.finalize_dataset_snapshot_binding(
            parent.snapshot_id,
            finalization,
        )
        assert ready.status == "ready"
        assert ready.completed_at == t0 + timedelta(minutes=2)
        assert store.get_dataset_snapshot_binding(parent.snapshot_id) == ready
        assert (
            store.finalize_dataset_snapshot_binding(parent.snapshot_id, finalization)
            == ready
        )

        different_manifest = manifest.model_copy(
            update={"builder_version": "snapshot-builder-v2"}
        )
        conflicting = DatasetSnapshotBinding.create(
            manifest=different_manifest,
            artifact_root=binding.artifact_root,
            manifest_relative_path=binding.manifest_relative_path,
            created_at=t0,
        )
        with pytest.raises(ValueError, match="already bound|immutable|conflict"):
            store.begin_dataset_snapshot_binding(conflicting)


def test_execution_binding_detects_caller_tampering_at_write_boundary(
    tmp_path: Path,
) -> None:
    from rquant.data_metadata import (
        DatasetSnapshot,
        DatasetSnapshotBinding,
        DatasetSnapshotFinalization,
    )

    t0 = datetime(2026, 7, 15, 8, tzinfo=UTC)
    parent = DatasetSnapshot.create(
        strategy_name="growth_board_surge",
        manifest_id="m" * 64,
        as_of_time=t0,
        code_commit="1" * 40,
        origin="test",
        created_at=t0,
    )
    binding = DatasetSnapshotBinding.create(
        manifest=_execution_manifest(parent.snapshot_id),
        artifact_root="/srv/rquant/research-lake",
        manifest_relative_path=f"snapshots/{parent.snapshot_id}/manifest.json",
        created_at=t0,
    )
    object.__setattr__(binding, "manifest_hash", "0" * 64)

    with DuckDBStore(tmp_path / "tampered-binding.duckdb") as store:
        store.begin_dataset_snapshot(parent)
        store.finalize_dataset_snapshot(
            parent.snapshot_id,
            DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=1)),
        )
        with pytest.raises(ValueError, match="write-boundary|manifest_hash"):
            store.begin_dataset_snapshot_binding(binding)


def test_finalized_snapshot_allows_exact_coverage_retry(tmp_path: Path) -> None:
    from rquant.data_metadata import (
        DatasetCoverage,
        DatasetSnapshot,
        DatasetSnapshotFinalization,
    )

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )
    coverage = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=9,
        missing_reasons=("one symbol absent",),
        created_at=t0,
    )

    with DuckDBStore(tmp_path / "ready-coverage-retry.duckdb") as store:
        store.begin_dataset_snapshot(snapshot)
        stored = store.upsert_dataset_coverage(coverage)
        store.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=1)),
        )

        assert store.upsert_dataset_coverage(coverage) == stored
        assert store.list_dataset_coverages(snapshot.snapshot_id) == [stored]


def test_finalized_snapshot_coverage_retry_ignores_write_once_created_at(
    tmp_path: Path,
) -> None:
    from rquant.data_metadata import (
        DatasetCoverage,
        DatasetSnapshot,
        DatasetSnapshotFinalization,
    )

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )
    initial = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=8,
        missing_reasons=("two symbols absent",),
        created_at=t0,
    )
    accepted_request = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=9,
        missing_reasons=("one symbol absent",),
        created_at=t0 + timedelta(minutes=1),
    )

    with DuckDBStore(tmp_path / "ready-coverage-created-at.duckdb") as store:
        store.begin_dataset_snapshot(snapshot)
        store.upsert_dataset_coverage(initial)
        stored = store.upsert_dataset_coverage(accepted_request)
        assert stored.created_at == t0
        store.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=2)),
        )

        assert store.upsert_dataset_coverage(accepted_request) == stored
        assert store.list_dataset_coverages(snapshot.snapshot_id) == [stored]


def test_finalized_snapshot_rejects_changed_or_new_coverage_before_write(
    tmp_path: Path,
) -> None:
    from rquant.data_metadata import (
        DatasetCoverage,
        DatasetSnapshot,
        DatasetSnapshotFinalization,
    )

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )
    coverage = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=9,
        missing_reasons=("one symbol absent",),
        created_at=t0,
    )
    changed = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=10,
        created_at=t0,
    )
    new_coverage = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="daily-bars",
        coverage_scope="trade-date:2026-07-12",
        table_name="daily_bar",
        expected_count=1,
        available_count=1,
        created_at=t0,
    )

    with DuckDBStore(tmp_path / "ready-coverage-immutable.duckdb") as store:
        store.begin_dataset_snapshot(snapshot)
        stored = store.upsert_dataset_coverage(coverage)
        store.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(completed_at=t0 + timedelta(minutes=1)),
        )

        with pytest.raises(ValueError, match="finalized.*coverage"):
            store.upsert_dataset_coverage(changed)
        with pytest.raises(ValueError, match="finalized.*coverage"):
            store.upsert_dataset_coverage(new_coverage)

        assert store.list_dataset_coverages(snapshot.snapshot_id) == [stored]


def test_coverage_upsert_serializes_with_snapshot_finalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.data_metadata import (
        DatasetCoverage,
        DatasetSnapshot,
        DatasetSnapshotFinalization,
        DatasetSnapshotWriteConflictError,
    )

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    db_path = tmp_path / "coverage-finalize-race.duckdb"
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )
    initial = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=9,
        missing_reasons=("one symbol absent",),
        created_at=t0,
    )
    changed = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=10,
        created_at=t0,
    )
    finalization = DatasetSnapshotFinalization(
        completed_at=t0 + timedelta(minutes=1)
    )

    with DuckDBStore(db_path) as primary, DuckDBStore(db_path) as competitor:
        primary.begin_dataset_snapshot(snapshot)
        primary.upsert_dataset_coverage(initial)
        real_get = primary.get_dataset_snapshot
        finalization_conflicts: list[DatasetSnapshotWriteConflictError] = []
        interleaved = False

        def get_with_concurrent_finalization(
            snapshot_id: str,
        ) -> DatasetSnapshot | None:
            nonlocal interleaved
            current = real_get(snapshot_id)
            if not interleaved:
                interleaved = True
                try:
                    competitor.finalize_dataset_snapshot(snapshot_id, finalization)
                except DatasetSnapshotWriteConflictError as exc:
                    finalization_conflicts.append(exc)
            return current

        monkeypatch.setattr(
            primary,
            "get_dataset_snapshot",
            get_with_concurrent_finalization,
        )

        stored = primary.upsert_dataset_coverage(changed)

        assert len(finalization_conflicts) == 1
        assert snapshot.snapshot_id in str(finalization_conflicts[0])
        assert stored.available_count == 10
        finalized = competitor.finalize_dataset_snapshot(
            snapshot.snapshot_id, finalization
        )
        assert finalized.status == "ready"
        assert primary.list_dataset_coverages(snapshot.snapshot_id) == [stored]


def test_finalization_serializes_with_coverage_upsert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.data_metadata import (
        DatasetCoverage,
        DatasetSnapshot,
        DatasetSnapshotFinalization,
        DatasetSnapshotWriteConflictError,
    )

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    db_path = tmp_path / "finalize-coverage-race.duckdb"
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )
    initial = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=9,
        missing_reasons=("one symbol absent",),
        created_at=t0,
    )
    changed = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute-bars",
        coverage_scope="trade-date:2026-07-13",
        table_name="minute_bar",
        expected_count=10,
        available_count=10,
        created_at=t0,
    )
    finalization = DatasetSnapshotFinalization(
        completed_at=t0 + timedelta(minutes=1)
    )

    with DuckDBStore(db_path) as primary, DuckDBStore(db_path) as finalizer:
        primary.begin_dataset_snapshot(snapshot)
        stored = primary.upsert_dataset_coverage(initial)
        real_get = finalizer.get_dataset_snapshot
        coverage_conflicts: list[DatasetSnapshotWriteConflictError] = []
        snapshot_reads = 0

        def get_with_concurrent_coverage(
            snapshot_id: str,
        ) -> DatasetSnapshot | None:
            nonlocal snapshot_reads
            current = real_get(snapshot_id)
            snapshot_reads += 1
            if snapshot_reads == 2:
                try:
                    primary.upsert_dataset_coverage(changed)
                except DatasetSnapshotWriteConflictError as exc:
                    coverage_conflicts.append(exc)
            return current

        monkeypatch.setattr(
            finalizer,
            "get_dataset_snapshot",
            get_with_concurrent_coverage,
        )

        finalized = finalizer.finalize_dataset_snapshot(
            snapshot.snapshot_id, finalization
        )

        assert finalized.status == "ready"
        assert len(coverage_conflicts) == 1
        assert snapshot.snapshot_id in str(coverage_conflicts[0])
        with pytest.raises(ValueError, match="finalized.*coverage"):
            primary.upsert_dataset_coverage(changed)
        assert primary.list_dataset_coverages(snapshot.snapshot_id) == [stored]


def test_record_issue_ignores_stale_and_equal_detections(tmp_path: Path) -> None:
    from rquant.data_metadata import DataQualityIssue

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    current = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P1",
        scope_key="trade-date:2026-07-13",
        message="current payload",
        evidence={"missing_count": 1},
        observed_at=t0 + timedelta(minutes=2),
    )
    stale = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P3",
        scope_key="trade-date:2026-07-13",
        message="stale payload",
        evidence={"missing_count": 99},
        observed_at=t0 + timedelta(minutes=1),
    )
    equal = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P0",
        scope_key="trade-date:2026-07-13",
        message="equal-time conflicting payload",
        evidence={"missing_count": 42},
        observed_at=t0 + timedelta(minutes=2),
    )

    with DuckDBStore(tmp_path / "stale-issue.duckdb") as store:
        stored = store.record_data_quality_issue(current)

        assert store.record_data_quality_issue(stale) == stored
        assert store.record_data_quality_issue(equal) == stored
        assert store.get_data_quality_issue(current.issue_id) == stored


def test_delayed_detection_cannot_reopen_newer_resolution(tmp_path: Path) -> None:
    from rquant.data_metadata import DataQualityIssue

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    issue = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P1",
        scope_key="trade-date:2026-07-13",
        message="initial",
        observed_at=t0,
    )
    delayed = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P0",
        scope_key="trade-date:2026-07-13",
        message="delayed scan",
        evidence={"delayed": True},
        observed_at=t0 + timedelta(minutes=1),
    )

    with DuckDBStore(tmp_path / "delayed-issue.duckdb") as store:
        store.record_data_quality_issue(issue)
        resolved = store.resolve_data_quality_issue(
            issue.issue_id,
            resolved_at=t0 + timedelta(minutes=2),
        )

        assert store.record_data_quality_issue(delayed) == resolved
        assert store.get_data_quality_issue(issue.issue_id) == resolved


def test_resolve_issue_without_timestamp_is_idempotent(tmp_path: Path) -> None:
    from rquant.data_metadata import DataQualityIssue

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    issue = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P1",
        scope_key="all",
        message="initial",
        observed_at=t0,
    )

    with DuckDBStore(tmp_path / "resolve-idempotent.duckdb") as store:
        store.record_data_quality_issue(issue)
        resolved = store.resolve_data_quality_issue(
            issue.issue_id,
            resolved_at=t0 + timedelta(minutes=1),
        )

        assert store.resolve_data_quality_issue(issue.issue_id) == resolved
        assert store.get_data_quality_issue(issue.issue_id) == resolved


def test_resolve_issue_rejects_conflicting_audit_timestamp(tmp_path: Path) -> None:
    from rquant.data_metadata import DataQualityIssue

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    issue = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P1",
        scope_key="all",
        message="initial",
        observed_at=t0,
    )
    resolved_at = t0 + timedelta(minutes=1)

    with DuckDBStore(tmp_path / "resolve-conflict.duckdb") as store:
        store.record_data_quality_issue(issue)
        resolved = store.resolve_data_quality_issue(
            issue.issue_id,
            resolved_at=resolved_at,
        )

        assert (
            store.resolve_data_quality_issue(
                issue.issue_id,
                resolved_at=resolved_at,
            )
            == resolved
        )
        with pytest.raises(ValueError, match="already resolved.*different"):
            store.resolve_data_quality_issue(
                issue.issue_id,
                resolved_at=resolved_at + timedelta(minutes=1),
            )
        assert store.get_data_quality_issue(issue.issue_id) == resolved


def test_coverage_requires_existing_snapshot(tmp_path: Path) -> None:
    from rquant.data_metadata import DatasetCoverage

    coverage = DatasetCoverage(
        snapshot_id="missing-snapshot",
        dataset_id="minute-bars",
        coverage_scope="all",
        table_name="minute_bar",
        expected_count=1,
        available_count=1,
    )

    with DuckDBStore(tmp_path / "coverage-reference.duckdb") as store:
        with pytest.raises(KeyError, match="dataset snapshot.*missing-snapshot"):
            store.upsert_dataset_coverage(coverage)
        count = store._conn.execute(
            "SELECT COUNT(*) FROM dataset_coverage"
        ).fetchone()[0]
        assert count == 0


def test_finalize_requires_all_quality_issue_references(tmp_path: Path) -> None:
    from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotFinalization

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )

    with DuckDBStore(tmp_path / "finalize-reference.duckdb") as store:
        store.begin_dataset_snapshot(snapshot)
        with pytest.raises(KeyError, match="quality issue.*missing-issue"):
            store.finalize_dataset_snapshot(
                snapshot.snapshot_id,
                DatasetSnapshotFinalization(
                    quality_issue_ids=("missing-issue",),
                    completed_at=t0 + timedelta(minutes=1),
                ),
            )
        assert store.get_dataset_snapshot(snapshot.snapshot_id).status == "building"


def test_list_snapshot_issues_rejects_missing_reference(tmp_path: Path) -> None:
    from rquant.data_metadata import DatasetSnapshot

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )

    with DuckDBStore(tmp_path / "corrupt-reference.duckdb") as store:
        store.begin_dataset_snapshot(snapshot)
        store._conn.execute(
            "UPDATE dataset_snapshot SET status = 'ready', "
            "quality_issue_ids = CAST('[\"missing-issue\"]' AS JSON), "
            "completed_at = ? WHERE snapshot_id = ?",
            [t0 + timedelta(minutes=1), snapshot.snapshot_id],
        )

        with pytest.raises(RuntimeError, match="missing quality issue.*missing-issue"):
            store.list_snapshot_quality_issues(snapshot.snapshot_id)


def test_begin_snapshot_rolls_back_shallow_mutation(tmp_path: Path) -> None:
    from rquant.data_metadata import DatasetSnapshot

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )
    snapshot.table_watermarks["daily_bar"] = "unexpected-finalization"

    with DuckDBStore(tmp_path / "mutated-begin.duckdb") as store:
        with pytest.raises(ValueError, match="write-boundary|building snapshot"):
            store.begin_dataset_snapshot(snapshot)
        count = store._conn.execute(
            "SELECT COUNT(*) FROM dataset_snapshot"
        ).fetchone()[0]
        assert count == 0


def test_finalize_snapshot_rolls_back_shallow_mutation(tmp_path: Path) -> None:
    from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotFinalization

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )
    finalization = DatasetSnapshotFinalization(
        table_watermarks={"daily_bar": "2026-07-13"},
        completed_at=t0 + timedelta(minutes=1),
    )
    finalization.table_watermarks["daily_bar"] = {  # type: ignore[assignment]
        "invalid": "nested"
    }

    with DuckDBStore(tmp_path / "mutated-finalize.duckdb") as store:
        store.begin_dataset_snapshot(snapshot)
        with pytest.raises(ValueError, match="write-boundary|table_watermarks"):
            store.finalize_dataset_snapshot(snapshot.snapshot_id, finalization)
        stored = store.get_dataset_snapshot(snapshot.snapshot_id)
        assert stored is not None
        assert stored.status == "building"
        assert stored.table_watermarks == {}


def test_finalize_snapshot_cas_preserves_concurrent_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotFinalization

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    db_path = tmp_path / "finalize-cas.duckdb"
    snapshot = DatasetSnapshot.create(
        strategy_name="strategy",
        as_of_time=t0,
        code_commit="abc123",
        origin="local",
        created_at=t0,
    )
    winner = DatasetSnapshotFinalization(
        table_watermarks={"daily_bar": "winner"},
        completed_at=t0 + timedelta(minutes=1),
    )
    loser = DatasetSnapshotFinalization(
        table_watermarks={"daily_bar": "loser"},
        completed_at=t0 + timedelta(minutes=2),
    )

    with DuckDBStore(db_path) as primary, DuckDBStore(db_path) as competitor:
        primary.begin_dataset_snapshot(snapshot)
        real_get = primary.get_dataset_snapshot
        interleaved = False

        def get_with_concurrent_winner(
            snapshot_id: str,
        ) -> DatasetSnapshot | None:
            nonlocal interleaved
            current = real_get(snapshot_id)
            if not interleaved:
                interleaved = True
                competitor.finalize_dataset_snapshot(snapshot_id, winner)
            return current

        monkeypatch.setattr(
            primary,
            "get_dataset_snapshot",
            get_with_concurrent_winner,
        )

        with pytest.raises(ValueError, match="concurrent|different"):
            primary.finalize_dataset_snapshot(snapshot.snapshot_id, loser)

        stored = real_get(snapshot.snapshot_id)
        assert stored is not None
        assert stored.status == "ready"
        assert stored.table_watermarks == {"daily_bar": "winner"}
        assert stored.completed_at == winner.completed_at


def test_resolved_issue_rejects_resolution_before_last_observation() -> None:
    from rquant.data_metadata import DataQualityIssue

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)

    with pytest.raises(ValidationError, match="resolved_at.*last_seen_at"):
        DataQualityIssue(
            rule_id="minute-coverage",
            dataset_id="minute-bars",
            severity="P1",
            status="resolved",
            scope_key="all",
            message="invalid timeline",
            first_seen_at=t0,
            last_seen_at=t0 + timedelta(minutes=2),
            resolved_at=t0 + timedelta(minutes=1),
        )


def test_resolve_issue_cas_rejects_concurrent_newer_detection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.data_metadata import DataQualityIssue

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    db_path = tmp_path / "resolve-cas.duckdb"
    initial = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P1",
        scope_key="all",
        message="initial",
        observed_at=t0,
    )
    newer = DataQualityIssue.detected(
        rule_id="minute-coverage",
        dataset_id="minute-bars",
        severity="P0",
        scope_key="all",
        message="newer detection wins",
        evidence={"observation": "t2"},
        observed_at=t0 + timedelta(minutes=2),
    )

    with DuckDBStore(db_path) as primary, DuckDBStore(db_path) as competitor:
        primary.record_data_quality_issue(initial)
        real_get = primary.get_data_quality_issue
        interleaved = False

        def get_with_newer_detection(issue_id: str):  # noqa: ANN202
            nonlocal interleaved
            current = real_get(issue_id)
            if not interleaved:
                interleaved = True
                competitor.record_data_quality_issue(newer)
            return current

        monkeypatch.setattr(
            primary,
            "get_data_quality_issue",
            get_with_newer_detection,
        )

        with pytest.raises(ValueError, match="last_seen_at|newer detection"):
            primary.resolve_data_quality_issue(
                initial.issue_id,
                resolved_at=t0 + timedelta(minutes=1),
            )

        stored = real_get(initial.issue_id)
        assert stored is not None
        assert stored.status == "open"
        assert stored.last_seen_at == t0 + timedelta(minutes=2)
        assert stored.resolved_at is None
        assert stored.message == "newer detection wins"


@pytest.mark.parametrize(
    "evidence",
    [
        {"value": float("nan")},
        {"nested": {"values": [float("inf")]}},
        {"nested": [{"value": float("-inf")}]},
    ],
    ids=("nan", "nested-positive-infinity", "nested-negative-infinity"),
)
def test_quality_issue_rejects_non_finite_evidence(
    evidence: dict[str, object],
) -> None:
    from rquant.data_metadata import DataQualityIssue

    with pytest.raises(ValidationError, match="finite"):
        DataQualityIssue.detected(
            rule_id="finite-evidence",
            dataset_id="minute-bars",
            severity="P1",
            scope_key="all",
            message="invalid evidence",
            evidence=evidence,  # type: ignore[arg-type]
            observed_at=datetime(2026, 7, 13, 6, 0, tzinfo=UTC),
        )


def test_issue_write_boundary_rejects_mutated_non_finite_evidence(
    tmp_path: Path,
) -> None:
    from rquant.data_metadata import DataQualityIssue

    t0 = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    ordinary_evidence = {
        "sample": {"values": [1.5, None, True], "symbol": "600000.SH"}
    }
    ordinary = DataQualityIssue.detected(
        rule_id="finite-evidence",
        dataset_id="minute-bars",
        severity="P1",
        scope_key="ordinary",
        message="ordinary nested evidence",
        evidence=ordinary_evidence,
        observed_at=t0,
    )
    mutated = DataQualityIssue.detected(
        rule_id="finite-evidence",
        dataset_id="minute-bars",
        severity="P1",
        scope_key="mutated",
        message="mutated nested evidence",
        evidence={"sample": {"values": [1.5]}},
        observed_at=t0,
    )
    sample = mutated.evidence["sample"]
    assert isinstance(sample, dict)
    values = sample["values"]
    assert isinstance(values, list)
    values.append(float("nan"))

    with DuckDBStore(tmp_path / "finite-evidence.duckdb") as store:
        stored = store.record_data_quality_issue(ordinary)
        assert stored.evidence == ordinary_evidence
        assert store.get_data_quality_issue(ordinary.issue_id) == stored

        with pytest.raises(ValueError, match="finite|write-boundary"):
            store.record_data_quality_issue(mutated)
        assert store.get_data_quality_issue(mutated.issue_id) is None
