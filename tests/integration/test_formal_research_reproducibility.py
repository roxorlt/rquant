"""Formal research is reproducible from one immutable execution binding."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.backfill_manifest import EligibilityRecord, EligibilityResolution
from rquant.dashboard.strategy_lab_runs import build_strategy_lab_run
from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DatasetCoverage,
    DatasetSnapshot,
    DatasetSnapshotFinalization,
)
from rquant.data_quality import STAGE1_AUDIT_RULE_SET_VERSION
from rquant.research_catalog import ResearchCatalog
from rquant.research_gate import (
    ResearchGateRequest,
    build_gate_research_manifest,
    open_gated_research_store,
)
from rquant.research_lake import export_research_dataset
from rquant.research_snapshot import build_dataset_snapshot_binding
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_dependencies import (
    StrategyExecutionDependencies,
    StrategyTableDependency,
)

_COMMIT = "a" * 40
_AS_OF = datetime(2026, 7, 14, 8, tzinfo=UTC)
_TRADE_DATE = date(2026, 7, 14)


def _eligibility_resolution() -> EligibilityResolution:
    record = EligibilityRecord(
        strategy_id="test_strategy",
        strategy_version="v1",
        ts_code="000001.SZ",
        eligibility_date=_TRADE_DATE,
        entry_date=_TRADE_DATE,
        decision_at=datetime(2026, 7, 14, 1, 30, tzinfo=UTC),
        variant="test",
    )
    return EligibilityResolution(
        strategy_id="test_strategy",
        strategy_version="v1",
        requested_dates=(_TRADE_DATE,),
        evaluated_dates=(_TRADE_DATE,),
        complete_dates=(_TRADE_DATE,),
        records=(record,),
    )


def _seed_source(store: DuckDBStore) -> None:
    store._conn.execute(
        """
        INSERT INTO trade_calendar
        (exchange, cal_date, is_open, pretrade_date, source, updated_at)
        VALUES (
            'SSE', DATE '2026-07-14', TRUE, DATE '2026-07-13',
            'test', TIMESTAMPTZ '2026-07-14 08:00:00+00'
        );

        INSERT INTO daily_bar
        (ts_code, trade_date, open, high, low, close)
        VALUES ('000001.SZ', DATE '2026-07-14', 10, 10.2, 9.9, 10.1);

        INSERT INTO minute_bar
        (ts_code, trade_time, freq, open, high, low, close, vol, amount,
         source, created_at)
        VALUES (
            '000001.SZ', TIMESTAMP '2026-07-14 09:30:00', '1min',
            10, 10.2, 9.9, 10.1, 1000, 10100, 'tushare',
            TIMESTAMP '2026-07-14 16:00:00'
        );
        """
    )


def _seed_gate_evidence(
    store: DuckDBStore,
    *,
    eligibility_resolution: EligibilityResolution,
) -> tuple[DataAuditRun, DatasetSnapshot]:
    running_audit = DataAuditRun.create(
        as_of_date=_TRADE_DATE,
        range_start=_TRADE_DATE,
        range_end=_TRADE_DATE,
        rule_set_version=STAGE1_AUDIT_RULE_SET_VERSION,
        observed_at=_AS_OF,
    )
    store.begin_data_audit_run(running_audit)
    audit = store.finalize_data_audit_run(
        running_audit.audit_run_id,
        DataAuditRunFinalization(
            p0_count=0,
            completed_at=_AS_OF + timedelta(minutes=1),
        ),
    )

    building_snapshot = DatasetSnapshot.create(
        strategy_name="test_strategy",
        manifest_id="m" * 64,
        as_of_time=_AS_OF,
        code_commit=_COMMIT,
        origin="integration_test",
        created_at=_AS_OF,
    )
    store.begin_dataset_snapshot(building_snapshot)
    for scope in ("eligibility", "baseline", "entry", "exit"):
        store.upsert_dataset_coverage(
            DatasetCoverage(
                snapshot_id=building_snapshot.snapshot_id,
                dataset_id=(
                    "strategy_eligibility"
                    if scope == "eligibility"
                    else "minute_bar"
                ),
                coverage_scope=scope,
                table_name=(
                    "backfill_manifest"
                    if scope == "eligibility"
                    else "minute_bar"
                ),
                expected_count=1,
                available_count=1,
            )
        )
    snapshot = store.finalize_dataset_snapshot(
        building_snapshot.snapshot_id,
        DatasetSnapshotFinalization(
            table_watermarks={
                "manifest_start_date": _TRADE_DATE.isoformat(),
                "manifest_end_date": _TRADE_DATE.isoformat(),
                "eligibility_resolution_hash": (
                    eligibility_resolution.resolution_hash
                ),
            },
            completed_at=_AS_OF + timedelta(minutes=2),
        ),
    )
    return audit, snapshot


def _dependencies() -> StrategyExecutionDependencies:
    return StrategyExecutionDependencies(
        strategy_id="test_strategy",
        contract_version="integration-v1",
        lake_datasets=("minute_bar",),
        materialized_tables=(
            StrategyTableDependency(
                dataset_id="daily_bar",
                table_name="daily_bar",
                date_column="trade_date",
                code_column="ts_code",
            ),
        ),
    )


def _run_formal_result(
    store: DuckDBStore,
    request: ResearchGateRequest,
    lake_root: Path,
) -> tuple[pd.DataFrame, str]:
    with open_gated_research_store(
        request,
        metadata_store_factory=lambda: nullcontext(store),
        lake_root=lake_root,
    ) as (session, decision):
        result = session._conn.execute(
            """
            SELECT d.ts_code, d.trade_date, d.close AS daily_close,
                   m.trade_time, m.close AS minute_close
            FROM daily_bar AS d
            JOIN minute_bar AS m USING (ts_code)
            ORDER BY d.ts_code, m.trade_time
            """
        ).fetchdf()
    run = build_strategy_lab_run(
        run_type="formal_reproducibility",
        title="formal reproducibility",
        params={"signal": "fixed-v1"},
        metrics={"row_count": len(result)},
        tables={"result": result},
        manifest=build_gate_research_manifest(request, decision),
    )
    assert run.manifest.schema_version == 2
    assert run.manifest.result_hash is not None
    return result, run.manifest.result_hash


@pytest.mark.integration
def test_formal_result_survives_source_updates_and_rejects_corruption(
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    with DuckDBStore(tmp_path / "source.duckdb") as store:
        eligibility_resolution = _eligibility_resolution()
        _seed_source(store)
        export_research_dataset(
            store._conn,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=_TRADE_DATE,
            end_date=_TRADE_DATE,
            code_commit=_COMMIT,
        )
        audit, snapshot = _seed_gate_evidence(
            store,
            eligibility_resolution=eligibility_resolution,
        )
        binding = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=_TRADE_DATE,
            end_date=_TRADE_DATE,
            ts_codes=("000001.SZ",),
            dependencies=_dependencies(),
            eligibility_resolution=eligibility_resolution,
            now=lambda: _AS_OF + timedelta(minutes=3),
        )
        request = ResearchGateRequest(
            mode="formal",
            strategy_name="test_strategy",
            start_date=_TRADE_DATE,
            end_date=_TRADE_DATE,
            audit_run_id=audit.audit_run_id,
            dataset_snapshot_id=snapshot.snapshot_id,
            dataset_binding_hash=binding.binding_hash,
            code_commit=_COMMIT,
        )

        first_result, first_hash = _run_formal_result(store, request, lake_root)

        store._conn.execute(
            "UPDATE daily_bar SET close = 99 WHERE ts_code = '000001.SZ';"
            "UPDATE minute_bar SET close = 88 WHERE ts_code = '000001.SZ';"
        )
        export_research_dataset(
            store._conn,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=_TRADE_DATE,
            end_date=_TRADE_DATE,
            code_commit=_COMMIT,
        )

        second_result, second_hash = _run_formal_result(store, request, lake_root)
        pd.testing.assert_frame_equal(first_result, second_result)
        assert first_hash == second_hash

        lake_artifact = next(
            artifact
            for artifact in binding.manifest.artifacts
            if artifact.artifact_type == "lake_partition"
        )
        (lake_root / lake_artifact.relative_path).write_bytes(b"corrupt")

        with pytest.raises(ValueError, match="file (size|hash)|Parquet|parquet"):
            _run_formal_result(store, request, lake_root)
