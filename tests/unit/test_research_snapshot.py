"""Immutable research-lake artifact resolution and verification."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from rquant.backfill_manifest import EligibilityRecord, EligibilityResolution
from rquant.research_catalog import ResearchCatalog
from rquant.research_lake import export_research_dataset
from rquant.research_snapshot import (
    SnapshotArtifactResolver,
    resolve_strategy_eligibility_from_artifacts,
    verify_snapshot_artifact,
)

_COMMIT = "a" * 40


@pytest.fixture()
def source_connection(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))
    connection.execute(
        """
        CREATE TABLE trade_calendar (
            exchange VARCHAR NOT NULL,
            cal_date DATE NOT NULL,
            is_open BOOLEAN NOT NULL,
            PRIMARY KEY (exchange, cal_date)
        );
        INSERT INTO trade_calendar VALUES
            ('SSE', '2026-07-14', TRUE),
            ('SSE', '2026-07-15', TRUE);

        CREATE TABLE minute_bar (
            ts_code VARCHAR NOT NULL,
            trade_time TIMESTAMP NOT NULL,
            freq VARCHAR NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            source VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (ts_code, trade_time, freq, source)
        );
        INSERT INTO minute_bar VALUES
            ('000001.SZ', '2026-07-14 09:30:00', '1min', 10, 10.2, 9.9, 10.1,
             1000, 10100, 'tushare', '2026-07-14 16:00:00'),
            ('000001.SZ', '2026-07-15 09:30:00', '1min', 10.1, 10.4, 10, 10.3,
             1100, 11330, 'tushare', '2026-07-15 16:00:00');
        """
    )
    try:
        yield connection
    finally:
        connection.close()


def _export_minutes(
    source: duckdb.DuckDBPyConnection,
    *,
    catalog: ResearchCatalog,
    lake_root: Path,
    start_date: date = date(2026, 7, 14),
    end_date: date = date(2026, 7, 15),
) -> None:
    export_research_dataset(
        source,
        catalog=catalog,
        lake_root=lake_root,
        dataset="minute_bar",
        start_date=start_date,
        end_date=end_date,
        code_commit=_COMMIT,
    )


def test_catalog_lists_exact_partition_heads_in_range(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    _export_minutes(
        source_connection,
        catalog=catalog,
        lake_root=tmp_path / "lake",
    )

    records = catalog.list_partitions(
        dataset="minute_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 15),
        freq="1min",
    )

    assert [record.partition_id for record in records] == [
        "minute_bar:2026-07-14:1min",
        "minute_bar:2026-07-15:1min",
    ]
    assert all("/versions/" in record.relative_path for record in records)


def test_resolved_artifact_keeps_old_version_after_catalog_head_changes(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    _export_minutes(
        source_connection,
        catalog=catalog,
        lake_root=lake_root,
        end_date=date(2026, 7, 14),
    )
    resolver = SnapshotArtifactResolver(catalog=catalog, lake_root=lake_root)
    first = resolver.resolve_lake_partitions(
        dataset="minute_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        freq="1min",
        as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
    )[0]

    source_connection.execute(
        "UPDATE minute_bar SET close = 10.15 "
        "WHERE trade_time = TIMESTAMP '2026-07-14 09:30:00'"
    )
    _export_minutes(
        source_connection,
        catalog=catalog,
        lake_root=lake_root,
        end_date=date(2026, 7, 14),
    )
    second = resolver.resolve_lake_partitions(
        dataset="minute_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        freq="1min",
        as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
    )[0]

    assert first.file_hash != second.file_hash
    assert first.relative_path != second.relative_path
    assert first.revision_created_at is not None
    assert first.catalog_updated_at is not None
    assert first.catalog_updated_at >= first.revision_created_at
    assert verify_snapshot_artifact(
        first,
        lake_root=lake_root,
        as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
    ).is_file()
    assert verify_snapshot_artifact(
        second,
        lake_root=lake_root,
        as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
    ).is_file()


def test_artifact_verification_fails_for_missing_or_tampered_file(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    _export_minutes(
        source_connection,
        catalog=catalog,
        lake_root=lake_root,
        end_date=date(2026, 7, 14),
    )
    artifact = SnapshotArtifactResolver(
        catalog=catalog,
        lake_root=lake_root,
    ).resolve_lake_partitions(
        dataset="minute_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        freq="1min",
        as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
    )[0]
    path = lake_root / artifact.relative_path
    original = path.read_bytes()
    path.write_bytes(b"tampered")

    with pytest.raises(ValueError, match="file (size|hash)|Parquet|parquet"):
        verify_snapshot_artifact(
            artifact,
            lake_root=lake_root,
            as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
        )

    path.write_bytes(original)
    path.unlink()
    with pytest.raises(ValueError, match="missing"):
        verify_snapshot_artifact(
            artifact,
            lake_root=lake_root,
            as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
        )


def test_resolver_rejects_partition_not_yet_visible_at_as_of_time(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    _export_minutes(
        source_connection,
        catalog=catalog,
        lake_root=lake_root,
        end_date=date(2026, 7, 14),
    )

    with pytest.raises(ValueError, match="future|as_of"):
        SnapshotArtifactResolver(
            catalog=catalog,
            lake_root=lake_root,
        ).resolve_lake_partitions(
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            freq="1min",
            as_of_time=datetime(2026, 7, 14, 1, 29, tzinfo=UTC),
        )


def test_auction_eligibility_uses_the_exact_bound_lake_generation(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    source_connection.execute(
        """
        CREATE TABLE auction_bar (
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            auction_type VARCHAR NOT NULL,
            price DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            turnover_rate DOUBLE,
            volume_ratio DOUBLE,
            source VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (ts_code, trade_date, auction_type, source)
        );
        INSERT INTO auction_bar VALUES (
            '000001.SZ', DATE '2026-07-14', 'open_realtime',
            10.0, 1000, 10000, 0.1, 1.5, 'tushare',
            TIMESTAMP '2026-07-14 09:26:00'
        );
        """
    )
    export_research_dataset(
        source_connection,
        catalog=catalog,
        lake_root=lake_root,
        dataset="auction_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        code_commit=_COMMIT,
    )
    artifact = SnapshotArtifactResolver(
        catalog=catalog,
        lake_root=lake_root,
    ).resolve_lake_partitions(
        dataset="auction_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
    )[0]
    source_connection.execute("UPDATE auction_bar SET price = 99.0")

    def fake_resolve(store, **_kwargs) -> EligibilityResolution:
        price = float(store._conn.execute("SELECT price FROM auction_bar").fetchone()[0])
        record = EligibilityRecord(
            strategy_id="auction_gap",
            strategy_version="v1",
            ts_code="000001.SZ",
            eligibility_date=date(2026, 7, 14),
            entry_date=date(2026, 7, 14),
            decision_at=datetime(2026, 7, 14, 9, 27, tzinfo=UTC),
            variant=f"price-{price:.1f}",
        )
        return EligibilityResolution(
            strategy_id="auction_gap",
            strategy_version="v1",
            requested_dates=(date(2026, 7, 14),),
            evaluated_dates=(date(2026, 7, 14),),
            complete_dates=(date(2026, 7, 14),),
            records=(record,),
        )

    monkeypatch.setattr(
        "rquant.backfill_manifest.resolve_strategy_eligibility",
        fake_resolve,
    )
    resolution = resolve_strategy_eligibility_from_artifacts(
        SimpleNamespace(_conn=source_connection),
        strategy_id="auction_gap",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        input_artifacts=(artifact,),
        lake_root=lake_root,
        as_of_time=datetime(2026, 7, 14, 8, tzinfo=UTC),
    )

    assert resolution.records[0].variant == "price-10.0"
    assert resolution.input_artifacts == (artifact,)
    assert source_connection.execute(
        "SELECT price FROM auction_bar"
    ).fetchone() == (99.0,)
