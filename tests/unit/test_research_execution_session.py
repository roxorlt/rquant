"""Bound research execution uses one immutable verified data session."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from rquant.backfill_manifest import EligibilityRecord, EligibilityResolution
from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotFinalization
from rquant.research_catalog import ResearchCatalog
from rquant.research_lake import export_research_dataset
from rquant.research_snapshot import (
    SnapshotArtifactResolver,
    build_dataset_snapshot_binding,
    materialize_table_dependency,
    open_research_execution_session,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_dependencies import (
    StrategyExecutionDependencies,
    StrategyTableDependency,
)

_COMMIT = "a" * 40
_AS_OF = datetime(2026, 7, 14, 8, tzinfo=UTC)


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


def _dependencies() -> StrategyExecutionDependencies:
    return StrategyExecutionDependencies(
        strategy_id="test_strategy",
        contract_version="test-v1",
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


def _ready_snapshot(store: DuckDBStore) -> DatasetSnapshot:
    snapshot = DatasetSnapshot.create(
        strategy_name="test_strategy",
        manifest_id="m" * 64,
        as_of_time=_AS_OF,
        code_commit=_COMMIT,
        origin="test",
        created_at=_AS_OF,
    )
    store.begin_dataset_snapshot(snapshot)
    return store.finalize_dataset_snapshot(
        snapshot.snapshot_id,
        DatasetSnapshotFinalization(completed_at=_AS_OF),
    )


def _eligibility_resolution() -> EligibilityResolution:
    record = EligibilityRecord(
        strategy_id="test_strategy",
        strategy_version="v1",
        ts_code="000001.SZ",
        eligibility_date=date(2026, 7, 14),
        entry_date=date(2026, 7, 14),
        decision_at=datetime(2026, 7, 14, 1, 30, tzinfo=UTC),
        variant="test",
    )
    return EligibilityResolution(
        strategy_id="test_strategy",
        strategy_version="v1",
        requested_dates=(date(2026, 7, 14),),
        evaluated_dates=(date(2026, 7, 14),),
        complete_dates=(date(2026, 7, 14),),
        records=(record,),
    )


def test_binding_contains_exact_eligibility_resolution_table(
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    resolution = _eligibility_resolution()
    with DuckDBStore(tmp_path / "source.duckdb") as store:
        _seed_source(store)
        export_research_dataset(
            store._conn,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
        )
        building = DatasetSnapshot.create(
            strategy_name="test_strategy",
            manifest_id="m" * 64,
            as_of_time=_AS_OF,
            code_commit=_COMMIT,
            origin="test",
            created_at=_AS_OF,
        )
        store.begin_dataset_snapshot(building)
        snapshot = store.finalize_dataset_snapshot(
            building.snapshot_id,
            DatasetSnapshotFinalization(
                table_watermarks={
                    "eligibility_resolution_hash": resolution.resolution_hash,
                },
                completed_at=_AS_OF,
            ),
        )

        binding = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            dependencies=_dependencies(),
            eligibility_resolution=resolution,
            now=lambda: _AS_OF,
        )

        assert (
            binding.manifest.eligibility_resolution_hash
            == resolution.resolution_hash
        )
        with open_research_execution_session(
            store,
            snapshot_id=snapshot.snapshot_id,
            lake_root=lake_root,
        ) as session:
            assert session._conn.execute(
                """
                SELECT strategy_id, ts_code, eligibility_date, variant,
                       resolution_hash
                FROM strategy_eligibility
                """
            ).fetchall() == [
                (
                    "test_strategy",
                    "000001.SZ",
                    date(2026, 7, 14),
                    "test",
                    resolution.resolution_hash,
                )
            ]


def test_builder_is_idempotent_and_session_isolated_from_source_changes(
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    with DuckDBStore(tmp_path / "source.duckdb") as store:
        _seed_source(store)
        export_research_dataset(
            store._conn,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
        )
        snapshot = _ready_snapshot(store)

        first = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            ts_codes=("000001.SZ",),
            dependencies=_dependencies(),
            now=lambda: _AS_OF,
        )
        second = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            ts_codes=("000001.SZ",),
            dependencies=_dependencies(),
            now=lambda: _AS_OF,
        )

        assert first == second
        assert first.status == "ready"
        assert (lake_root / first.manifest_relative_path).is_file()

        with open_research_execution_session(
            store,
            snapshot_id=snapshot.snapshot_id,
            lake_root=lake_root,
        ) as session:
            assert session.binding_hash == first.binding_hash
            assert session._conn.execute(
                "SELECT close FROM daily_bar"
            ).fetchall() == [(10.1,)]
            assert session._conn.execute(
                "SELECT close FROM minute_bar"
            ).fetchall() == [(10.1,)]
            assert session.query_minute_bars(
                "000001.SZ",
                "2026-07-14 09:30:00",
                "2026-07-14 15:00:00",
            )["close"].tolist() == [10.1]

            store._conn.execute(
                "UPDATE daily_bar SET close = 99 "
                "WHERE ts_code = '000001.SZ'"
            )
            store._conn.execute(
                "UPDATE minute_bar SET close = 88 "
                "WHERE ts_code = '000001.SZ'"
            )
            export_research_dataset(
                store._conn,
                catalog=catalog,
                lake_root=lake_root,
                dataset="minute_bar",
                start_date=date(2026, 7, 14),
                end_date=date(2026, 7, 14),
                code_commit=_COMMIT,
            )

            assert session._conn.execute(
                "SELECT close FROM daily_bar"
            ).fetchall() == [(10.1,)]
            assert session._conn.execute(
                "SELECT close FROM minute_bar"
            ).fetchall() == [(10.1,)]


def test_session_fails_before_query_when_bound_artifact_is_corrupt(
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    with DuckDBStore(tmp_path / "source.duckdb") as store:
        _seed_source(store)
        export_research_dataset(
            store._conn,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
        )
        snapshot = _ready_snapshot(store)
        binding = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            dependencies=_dependencies(),
            now=lambda: _AS_OF,
        )
        materialized = next(
            artifact
            for artifact in binding.manifest.artifacts
            if artifact.artifact_type == "materialized_table"
        )
        (lake_root / materialized.relative_path).write_bytes(b"corrupt")

        with pytest.raises(ValueError, match="file (size|hash)|Parquet|parquet"):
            open_research_execution_session(
                store,
                snapshot_id=snapshot.snapshot_id,
                lake_root=lake_root,
            )


def test_open_session_keeps_verified_inode_when_bound_path_is_replaced(
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    with DuckDBStore(tmp_path / "source.duckdb") as store:
        _seed_source(store)
        export_research_dataset(
            store._conn,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
        )
        snapshot = _ready_snapshot(store)
        binding = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            dependencies=_dependencies(),
            now=lambda: _AS_OF,
        )
        bound = next(
            artifact
            for artifact in binding.manifest.artifacts
            if artifact.artifact_type == "materialized_table"
        )
        bound_path = lake_root / bound.relative_path

        with open_research_execution_session(
            store,
            snapshot_id=snapshot.snapshot_id,
            lake_root=lake_root,
        ) as session:
            store._conn.execute(
                "UPDATE daily_bar SET close = 99 "
                "WHERE ts_code = '000001.SZ'"
            )
            replacement = materialize_table_dependency(
                store._conn,
                dependency=_dependencies().materialized_tables[0],
                artifact_root=lake_root,
                start_date=date(2026, 7, 14),
                end_date=date(2026, 7, 14),
                as_of_time=_AS_OF,
            )
            os.replace(lake_root / replacement.relative_path, bound_path)

            assert session._conn.execute(
                "SELECT close FROM daily_bar"
            ).fetchall() == [(10.1,)]

        with pytest.raises(ValueError, match="file (size|hash) mismatch"):
            open_research_execution_session(
                store,
                snapshot_id=snapshot.snapshot_id,
                lake_root=lake_root,
            )


def test_builder_uses_pinned_lake_artifacts_instead_of_new_catalog_head(
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    with DuckDBStore(tmp_path / "source.duckdb") as store:
        _seed_source(store)
        export_research_dataset(
            store._conn,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
        )
        pinned = SnapshotArtifactResolver(
            catalog=catalog,
            lake_root=lake_root,
        ).resolve_lake_partitions(
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            freq="1min",
            as_of_time=_AS_OF,
        )
        old_hash = pinned[0].file_hash

        store._conn.execute(
            "UPDATE minute_bar SET close = 88 "
            "WHERE ts_code = '000001.SZ'"
        )
        export_research_dataset(
            store._conn,
            catalog=catalog,
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
        )
        current = catalog.get_partition("minute_bar:2026-07-14:1min")
        assert current is not None
        assert current.file_hash != old_hash

        snapshot = _ready_snapshot(store)
        binding = build_dataset_snapshot_binding(
            metadata_store=store,
            source_connection=store._conn,
            catalog=catalog,
            lake_root=lake_root,
            snapshot_id=snapshot.snapshot_id,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            dependencies=_dependencies(),
            lake_artifacts=pinned,
            now=lambda: _AS_OF,
        )

        minute = next(
            artifact
            for artifact in binding.manifest.artifacts
            if artifact.dataset_id == "minute_bar"
        )
        assert minute.file_hash == old_hash
