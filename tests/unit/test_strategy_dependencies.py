"""Strategy execution dependency contracts and immutable small-table artifacts."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

import rquant.research_snapshot as research_snapshot
from rquant.research_snapshot import materialize_table_dependency
from rquant.strategy_dependencies import (
    STRATEGY_EXECUTION_DEPENDENCIES,
    StrategyTableDependency,
    query_bound_strategy_eligibility,
    strategy_execution_dependencies,
)


def test_three_strategy_dependency_contracts_are_unique_and_explicit() -> None:
    assert set(STRATEGY_EXECUTION_DEPENDENCIES) == {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }
    for strategy_id, contract in STRATEGY_EXECUTION_DEPENDENCIES.items():
        assert contract.strategy_id == strategy_id
        table_names = [item.table_name for item in contract.materialized_tables]
        assert len(table_names) == len(set(table_names))
        assert "minute_bar" in contract.lake_datasets
        assert {"daily_bar", "stock_status_daily", "trade_calendar"} <= set(
            table_names
        )
        assert "index_daily_bar" in table_names

    assert "auction_bar" in strategy_execution_dependencies(
        "auction_gap"
    ).lake_datasets
    with pytest.raises(ValueError, match="unknown strategy"):
        strategy_execution_dependencies("unknown")


def test_growth_snapshot_binds_full_pit_suspension_evidence() -> None:
    contract = strategy_execution_dependencies("growth_board_surge")
    dependencies = {
        dependency.table_name: dependency for dependency in contract.materialized_tables
    }

    assert contract.contract_version == "stage1-v2"
    assert dependencies["stock_suspend_session_evidence"] == StrategyTableDependency(
        dataset_id="stock_suspend_session_evidence",
        table_name="stock_suspend_session_evidence",
    )
    assert "stock_suspend_event" not in dependencies
    assert "stock_suspend_coverage" not in dependencies


def test_growth_suspension_evidence_closes_history_before_materialization(
    tmp_path: Path,
) -> None:
    materialize = getattr(
        research_snapshot,
        "materialize_suspension_session_evidence",
        None,
    )
    assert materialize is not None
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE stock_suspend_event (
            source VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            suspend_type VARCHAR NOT NULL,
            suspend_timing VARCHAR NOT NULL,
            session_scope VARCHAR NOT NULL,
            available_at TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (
                source, ts_code, trade_date, suspend_type, suspend_timing
            )
        );
        INSERT INTO stock_suspend_event VALUES
            ('tushare', '000001.SZ', DATE '2026-03-02', 'S', '全天',
             'full_day', TIMESTAMPTZ '2026-07-14 08:00:00+00',
             TIMESTAMPTZ '2026-07-14 08:00:00+00'),
            ('tushare', '000002.SZ', DATE '2026-03-03', 'S', '全天',
             'full_day', TIMESTAMPTZ '2026-07-14 08:00:00+00',
             TIMESTAMPTZ '2026-07-14 08:00:00+00');

        CREATE TABLE stock_suspend_coverage (
            source VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            coverage_state VARCHAR NOT NULL,
            row_count BIGINT NOT NULL,
            snapshot_hash VARCHAR NOT NULL,
            queried_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (source, trade_date)
        );
        INSERT INTO stock_suspend_coverage VALUES
            ('tushare', DATE '2026-03-02', 'complete', 1, 'hash-1',
             TIMESTAMPTZ '2026-07-14 08:00:00+00'),
            ('tushare', DATE '2026-03-03', 'complete', 1, 'hash-2',
             TIMESTAMPTZ '2026-07-14 08:00:00+00');

        CREATE TABLE daily_bar (
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            PRIMARY KEY (ts_code, trade_date)
        );
        INSERT INTO daily_bar VALUES ('000001.SZ', DATE '2026-03-02');

        CREATE TABLE minute_bar (
            ts_code VARCHAR NOT NULL,
            trade_time TIMESTAMP NOT NULL,
            vol DOUBLE,
            amount DOUBLE
        );
        """
    )
    as_of = datetime(2026, 7, 15, 8, tzinfo=UTC)

    artifact = materialize(
        connection,
        artifact_root=tmp_path,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 15),
        as_of_time=as_of,
    )

    assert duckdb.connect().execute(
        "SELECT ts_code, trade_date, evidence_state FROM read_parquet(?) ORDER BY ts_code",
        [str(tmp_path / artifact.relative_path)],
    ).fetchall() == [
        ("000001.SZ", date(2026, 3, 2), "conflict"),
        ("000002.SZ", date(2026, 3, 3), "full_day"),
    ]


def test_growth_suspension_evidence_rejects_unreconstructable_as_of(
    tmp_path: Path,
) -> None:
    materialize = getattr(
        research_snapshot,
        "materialize_suspension_session_evidence",
        None,
    )
    assert materialize is not None
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE stock_suspend_event (
            source VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            suspend_type VARCHAR NOT NULL,
            suspend_timing VARCHAR NOT NULL,
            session_scope VARCHAR NOT NULL,
            available_at TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (
                source, ts_code, trade_date, suspend_type, suspend_timing
            )
        );
        CREATE TABLE stock_suspend_coverage (
            source VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            coverage_state VARCHAR NOT NULL,
            row_count BIGINT NOT NULL,
            snapshot_hash VARCHAR NOT NULL,
            queried_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (source, trade_date)
        );
        INSERT INTO stock_suspend_coverage VALUES (
            'tushare', DATE '2026-03-02', 'complete', 0, 'newer-empty',
            TIMESTAMPTZ '2026-07-16 08:00:00+00'
        );
        CREATE TABLE daily_bar (
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL
        );
        CREATE TABLE minute_bar (
            ts_code VARCHAR NOT NULL,
            trade_time TIMESTAMP NOT NULL,
            vol DOUBLE,
            amount DOUBLE
        );
        """
    )

    with pytest.raises(ValueError, match="cannot reconstruct suspension evidence"):
        materialize(
            connection,
            artifact_root=tmp_path,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 15),
            as_of_time=datetime(2026, 7, 15, 8, tzinfo=UTC),
        )


def test_bound_eligibility_query_distinguishes_operational_and_snapshot_stores() -> None:
    connection = duckdb.connect(":memory:")
    store = type("Store", (), {"_conn": connection})()
    assert query_bound_strategy_eligibility(
        store,
        strategy_id="n_shape",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
    ) is None

    connection.execute(
        """
        CREATE TABLE strategy_eligibility (
            eligibility_id VARCHAR PRIMARY KEY,
            strategy_id VARCHAR NOT NULL,
            strategy_version VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            eligibility_date DATE NOT NULL,
            entry_date DATE NOT NULL,
            decision_at TIMESTAMPTZ NOT NULL,
            variant VARCHAR NOT NULL,
            resolution_hash VARCHAR NOT NULL
        );
        INSERT INTO strategy_eligibility VALUES (
            'eligibility-1', 'n_shape', 'v1', '000001.SZ',
            DATE '2026-07-14', DATE '2026-07-15',
            TIMESTAMPTZ '2026-07-14 09:00:00+00', 'pool2', 'hash'
        );
        """
    )

    records = query_bound_strategy_eligibility(
        store,
        strategy_id="n_shape",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
    )
    assert records is not None
    assert [(row.ts_code, row.variant) for row in records] == [
        ("000001.SZ", "pool2")
    ]


def test_materialized_table_is_pit_filtered_content_addressed_and_idempotent(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE stock_status_daily (
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            name VARCHAR,
            is_st BOOLEAN,
            available_at TIMESTAMPTZ,
            PRIMARY KEY (ts_code, trade_date)
        );
        INSERT INTO stock_status_daily VALUES
            ('000001.SZ', DATE '2026-07-14', '平安银行', FALSE,
             TIMESTAMPTZ '2026-07-14 01:25:00+00'),
            ('000002.SZ', DATE '2026-07-14', '未来可见', FALSE,
             TIMESTAMPTZ '2026-07-16 01:25:00+00'),
            ('600000.SH', DATE '2026-07-13', '区间外', FALSE,
             TIMESTAMPTZ '2026-07-13 01:25:00+00');
        """
    )
    dependency = StrategyTableDependency(
        dataset_id="stock_status_daily",
        table_name="stock_status_daily",
        date_column="trade_date",
        code_column="ts_code",
        available_at_column="available_at",
    )
    root = tmp_path / "snapshot"

    first = materialize_table_dependency(
        connection,
        dependency=dependency,
        artifact_root=root,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        as_of_time=datetime(2026, 7, 15, 8, tzinfo=UTC),
        ts_codes=("000001.SZ", "000002.SZ"),
    )
    second = materialize_table_dependency(
        connection,
        dependency=dependency,
        artifact_root=root,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        as_of_time=datetime(2026, 7, 15, 8, tzinfo=UTC),
        ts_codes=("000002.SZ", "000001.SZ"),
    )

    assert first == second
    assert first.artifact_type == "materialized_table"
    assert first.row_count == 1
    assert first.relative_path.endswith(f"{first.file_hash}.parquet")
    path = root / first.relative_path
    assert path.is_file()
    assert duckdb.connect().execute(
        "SELECT ts_code, name FROM read_parquet(?)",
        [str(path)],
    ).fetchall() == [("000001.SZ", "平安银行")]


def test_materialization_rejects_missing_table_or_primary_key(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(":memory:")
    missing = StrategyTableDependency(
        dataset_id="missing",
        table_name="missing",
        date_column="trade_date",
    )
    with pytest.raises(ValueError, match="source table missing"):
        materialize_table_dependency(
            connection,
            dependency=missing,
            artifact_root=tmp_path,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            as_of_time=datetime(2026, 7, 15, 8, tzinfo=UTC),
        )

    connection.execute("CREATE TABLE no_key (trade_date DATE, value INTEGER)")
    no_key = StrategyTableDependency(
        dataset_id="no_key",
        table_name="no_key",
        date_column="trade_date",
    )
    with pytest.raises(ValueError, match="primary key"):
        materialize_table_dependency(
            connection,
            dependency=no_key,
            artifact_root=tmp_path,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            as_of_time=datetime(2026, 7, 15, 8, tzinfo=UTC),
        )
