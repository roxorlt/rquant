"""Formal research is fail-closed while exploratory runs remain available."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DataQualityIssue,
    DatasetCoverage,
    DatasetSnapshot,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    DatasetSnapshotFinalization,
)
from rquant.data_quality import STAGE1_AUDIT_RULE_SET_VERSION
from rquant.research_gate import (
    ResearchGateRequest,
    evaluate_research_gate,
    evaluate_store_research_gate,
    research_gate_metadata_ready,
)
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 7, 15, 8, tzinfo=UTC)
ELIGIBILITY_HASH = "e" * 64


def _audit(
    *,
    p0_count: int = 0,
    rule_set_version: str = STAGE1_AUDIT_RULE_SET_VERSION,
) -> DataAuditRun:
    running = DataAuditRun.create(
        as_of_date=date(2026, 7, 14),
        range_start=date(2026, 4, 1),
        range_end=date(2026, 7, 14),
        rule_set_version=rule_set_version,
        observed_at=NOW,
    )
    return running.finalize(
        DataAuditRunFinalization(
            finding_issue_ids=(("f" * 64,) if p0_count else ()),
            p0_count=p0_count,
            completed_at=NOW + timedelta(minutes=1),
        )
    )


def _snapshot(
    *,
    origin: str = "test",
    range_start: date = date(2026, 4, 1),
    range_end: date = date(2026, 7, 14),
) -> DatasetSnapshot:
    building = DatasetSnapshot.create(
        strategy_name="growth_board_surge",
        manifest_id="m" * 64,
        as_of_time=NOW,
        code_commit="c" * 40,
        origin=origin,
        created_at=NOW,
    )
    return building.finalize(
        DatasetSnapshotFinalization(
            table_watermarks={
                "minute_bar": "2026-07-14T15:00:00",
                "manifest_start_date": range_start.isoformat(),
                "manifest_end_date": range_end.isoformat(),
                "eligibility_resolution_hash": ELIGIBILITY_HASH,
            },
            completed_at=NOW + timedelta(minutes=1),
        )
    )


def _coverage(snapshot_id: str, scope: str, ratio: float) -> DatasetCoverage:
    expected = 10_000
    return DatasetCoverage(
        snapshot_id=snapshot_id,
        dataset_id=("strategy_eligibility" if scope == "eligibility" else "minute_bar"),
        coverage_scope=scope,
        table_name=("backfill_manifest" if scope == "eligibility" else "minute_bar"),
        expected_count=expected,
        available_count=int(expected * ratio),
    )


def _binding(
    snapshot: DatasetSnapshot,
    *,
    eligibility_hash: str = ELIGIBILITY_HASH,
) -> DatasetSnapshotBinding:
    manifest = DatasetSnapshotBindingManifest(
        snapshot_id=snapshot.snapshot_id,
        strategy_name=snapshot.strategy_name,
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 14),
        as_of_time=snapshot.as_of_time,
        code_commit=snapshot.code_commit,
        dependency_contract_version="stage1-v1",
        builder_version="snapshot-builder-v1",
        eligibility_resolution_hash=eligibility_hash,
        eligibility_expected_dates=10_000,
        eligibility_complete_dates=10_000,
        artifacts=(
            DatasetSnapshotArtifact(
                artifact_type="materialized_table",
                dataset_id="strategy_eligibility",
                table_name="strategy_eligibility",
                artifact_key=f"strategy_eligibility:{eligibility_hash}",
                relative_path=(
                    "tables/strategy_eligibility/versions/"
                    + "d" * 64
                    + ".parquet"
                ),
                row_count=100,
                schema_hash="a" * 64,
                content_hash="b" * 64,
                file_hash="d" * 64,
                primary_key=("eligibility_id",),
            ),
            DatasetSnapshotArtifact(
                artifact_type="lake_partition",
                dataset_id="minute_bar",
                table_name="minute_bar",
                artifact_key="minute_bar:2026-07-14:1min",
                partition_id="minute_bar:2026-07-14:1min",
                relative_path="minute/versions/" + "a" * 64 + ".parquet",
                row_count=241,
                schema_hash="b" * 64,
                content_hash="c" * 64,
                file_hash="a" * 64,
            ),
        ),
    )
    return DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="research_lake",
        manifest_relative_path=f"snapshots/{snapshot.snapshot_id}/manifest.json",
        created_at=NOW,
    ).finalize(
        DatasetSnapshotBindingFinalization(
            completed_at=NOW + timedelta(minutes=2)
        )
    )


def _request(mode: str = "formal") -> ResearchGateRequest:
    return ResearchGateRequest(
        mode=mode,
        strategy_name="growth_board_surge",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 14),
        code_commit="c" * 40,
    )


def _passing_coverages(snapshot_id: str) -> tuple[DatasetCoverage, ...]:
    return tuple(
        _coverage(snapshot_id, scope, 1.0)
        for scope in ("eligibility", "baseline", "entry", "exit")
    )


def test_exploratory_run_is_allowed_without_evidence() -> None:
    decision = evaluate_research_gate(
        _request("exploratory"),
        audit_run=None,
        snapshot=None,
        coverages=(),
        open_p0_issues=(),
    )

    assert decision.allowed is True
    assert decision.research_status == "exploratory"
    assert decision.failures


def test_formal_run_requires_completed_audit_and_ready_snapshot() -> None:
    decision = evaluate_research_gate(
        _request(),
        audit_run=None,
        snapshot=None,
        coverages=(),
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert {failure.code for failure in decision.failures} >= {
        "audit_missing",
        "snapshot_missing",
    }


def test_formal_run_rejects_evidence_from_an_obsolete_audit_rule_set() -> None:
    snapshot = _snapshot()
    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(rule_set_version="stage1-v1"),
        snapshot=snapshot,
        coverages=_passing_coverages(snapshot.snapshot_id),
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert "audit_rule_set" in {
        failure.code for failure in decision.failures
    }


def test_exact_coverage_boundaries_still_require_snapshot_bound_execution() -> None:
    snapshot = _snapshot()
    coverages = (
        _coverage(snapshot.snapshot_id, "eligibility", 0.99),
        _coverage(snapshot.snapshot_id, "baseline", 0.95),
        _coverage(snapshot.snapshot_id, "entry", 0.99),
        _coverage(snapshot.snapshot_id, "exit", 0.99),
    )

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(),
        snapshot=snapshot,
        coverages=coverages,
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert decision.research_status == "exploratory"
    assert "snapshot_execution_unbound" in {
        failure.code for failure in decision.failures
    }


def test_formal_gate_rejects_ready_binding_until_artifacts_are_verified() -> None:
    snapshot = _snapshot()
    binding = _binding(snapshot)

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(),
        snapshot=snapshot,
        binding=binding,
        coverages=_passing_coverages(snapshot.snapshot_id),
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert decision.research_status == "exploratory"
    assert "snapshot_artifacts_unverified" in {
        failure.code for failure in decision.failures
    }
    assert research_gate_metadata_ready(decision) is True


def test_formal_gate_allows_verified_ready_execution_binding() -> None:
    snapshot = _snapshot()
    binding = _binding(snapshot)

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(),
        snapshot=snapshot,
        binding=binding,
        binding_verified=True,
        coverages=_passing_coverages(snapshot.snapshot_id),
        open_p0_issues=(),
    )

    assert decision.allowed is True
    assert decision.research_status == "comparable"
    assert decision.dataset_snapshot_id == snapshot.snapshot_id
    assert decision.dataset_binding_hash == binding.binding_hash
    assert decision.failures == ()


def test_formal_gate_rejects_binding_from_another_eligibility_resolution() -> None:
    snapshot = _snapshot()
    binding = _binding(snapshot, eligibility_hash="f" * 64)

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(),
        snapshot=snapshot,
        binding=binding,
        binding_verified=True,
        coverages=_passing_coverages(snapshot.snapshot_id),
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert "snapshot_binding_eligibility" in {
        failure.code for failure in decision.failures
    }


def test_formal_gate_rejects_binding_for_another_snapshot() -> None:
    snapshot = _snapshot()
    other = DatasetSnapshot.create(
        strategy_name=snapshot.strategy_name,
        manifest_id="x" * 64,
        as_of_time=snapshot.as_of_time,
        code_commit=snapshot.code_commit,
        origin=snapshot.origin,
        created_at=NOW,
    ).finalize(
        DatasetSnapshotFinalization(completed_at=NOW + timedelta(minutes=1))
    )

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(),
        snapshot=snapshot,
        binding=_binding(other),
        coverages=_passing_coverages(snapshot.snapshot_id),
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert "snapshot_binding_mismatch" in {
        failure.code for failure in decision.failures
    }


def test_formal_gate_rejects_low_or_zero_coverage() -> None:
    snapshot = _snapshot()
    coverages = list(_passing_coverages(snapshot.snapshot_id))
    coverages[1] = _coverage(snapshot.snapshot_id, "baseline", 0.9499)
    coverages[2] = DatasetCoverage(
        snapshot_id=snapshot.snapshot_id,
        dataset_id="minute_bar",
        coverage_scope="entry",
        table_name="minute_bar",
        expected_count=0,
        available_count=0,
    )

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(),
        snapshot=snapshot,
        coverages=tuple(coverages),
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert {failure.code for failure in decision.failures} >= {
        "coverage_baseline_low",
        "coverage_entry_empty",
    }


def test_formal_gate_rejects_snapshot_that_does_not_cover_requested_range() -> None:
    snapshot = _snapshot(range_start=date(2026, 6, 1))

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(),
        snapshot=snapshot,
        coverages=_passing_coverages(snapshot.snapshot_id),
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert "snapshot_range" in {failure.code for failure in decision.failures}


def test_formal_gate_rejects_metadata_only_snapshot() -> None:
    snapshot = _snapshot(origin="rquant.backfill_manifest.metadata_only")

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(),
        snapshot=snapshot,
        coverages=_passing_coverages(snapshot.snapshot_id),
        open_p0_issues=(),
    )

    assert decision.allowed is False
    assert "snapshot_execution_unbound" in {
        failure.code for failure in decision.failures
    }


def test_p0_from_audit_or_later_open_issue_blocks_formal_run() -> None:
    snapshot = _snapshot()
    issue = DataQualityIssue.detected(
        rule_id="future-data",
        dataset_id="minute_bar",
        severity="P0",
        scope_key="all",
        message="future rows detected",
        observed_at=NOW + timedelta(minutes=2),
    )

    decision = evaluate_research_gate(
        _request(),
        audit_run=_audit(p0_count=1),
        snapshot=snapshot,
        coverages=_passing_coverages(snapshot.snapshot_id),
        open_p0_issues=(issue,),
    )

    assert decision.allowed is False
    assert {failure.code for failure in decision.failures} >= {
        "audit_p0",
        "open_p0",
    }


def test_store_gate_auto_selects_evidence_observed_after_backtest_end(
    tmp_path: Path,
) -> None:
    audit = _audit()
    snapshot = _snapshot()
    with DuckDBStore(tmp_path / "gate.duckdb") as store:
        store.begin_data_audit_run(
            audit.model_copy(
                update={
                    "status": "running",
                    "finding_issue_ids": (),
                    "p0_count": 0,
                    "completed_at": None,
                }
            )
        )
        store.finalize_data_audit_run(
            audit.audit_run_id,
            DataAuditRunFinalization(
                p0_count=0,
                completed_at=audit.completed_at or NOW,
            ),
        )
        building = snapshot.model_copy(
            update={
                "status": "building",
                "table_watermarks": {},
                "completed_at": None,
            }
        )
        store.begin_dataset_snapshot(building)
        for coverage in _passing_coverages(snapshot.snapshot_id):
            store.upsert_dataset_coverage(coverage)
        store.finalize_dataset_snapshot(
            snapshot.snapshot_id,
            DatasetSnapshotFinalization(
                table_watermarks=snapshot.table_watermarks,
                completed_at=snapshot.completed_at or NOW,
            ),
        )

        decision = evaluate_store_research_gate(store, _request())

    assert decision.allowed is False
    assert "snapshot_execution_unbound" in {
        failure.code for failure in decision.failures
    }
    assert decision.audit_run_id == audit.audit_run_id
    assert decision.dataset_snapshot_id == snapshot.snapshot_id


def test_formal_gated_store_uses_the_verified_bound_execution_session() -> None:
    from rquant.research_gate import open_gated_research_store

    snapshot = _snapshot()
    binding = _binding(snapshot)

    class Metadata:
        def get_data_audit_run(self, _run_id):
            return _audit()

        def latest_completed_data_audit_run(self, **_kwargs):
            return _audit()

        def get_dataset_snapshot(self, _snapshot_id):
            return snapshot

        def latest_ready_dataset_snapshot(self, **_kwargs):
            return snapshot

        def list_dataset_coverages(self, _snapshot_id):
            return _passing_coverages(snapshot.snapshot_id)

        def get_dataset_snapshot_binding(self, _snapshot_id):
            return binding

        def list_open_data_quality_issues(self, **_kwargs):
            return ()

    execution_store = object()
    request = _request().model_copy(
        update={
            "dataset_snapshot_id": snapshot.snapshot_id,
            "dataset_binding_hash": binding.binding_hash,
        }
    )

    with open_gated_research_store(
        request,
        metadata_store_factory=lambda: nullcontext(Metadata()),
        execution_session_factory=lambda _binding, _lake_root: nullcontext(
            execution_store
        ),
        lake_root=Path("/unused"),
    ) as (store, decision):
        assert store is execution_store
        assert decision.allowed is True
        assert decision.dataset_binding_hash == binding.binding_hash
