"""Research lake contract, atomic export, and catalog tests."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

import rquant.research_lake as research_lake_module
from rquant.data_contracts import research_dataset_contract, research_export_schema
from rquant.research_catalog import ResearchCatalog
from rquant.research_lake import (
    ResearchPartitionKey,
    export_research_dataset,
    partition_directory,
    partition_manifest_relative_path,
    partition_version_relative_path,
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
            ('SSE', '2026-07-13', FALSE),
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
            ('000002.SZ', '2026-07-14 09:30:00', '1min', 20, 20.3, 19.8, 20.2,
             2000, 40400, 'tushare_rt_daily', '2026-07-14 16:00:00'),
            ('000001.SZ', '2026-07-15 09:30:00', '1min', 10.1, 10.4, 10, 10.3,
             1100, 11330, 'tushare', '2026-07-15 16:00:00');

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
        INSERT INTO auction_bar VALUES
            ('000001.SZ', '2026-07-14', 'open', 10.0, 1000, 10000, 0.1, 1.5,
             'tushare', '2026-07-14 09:26:00'),
            ('000002.SZ', '2026-07-14', 'open', 20.0, 2000, 40000, 0.2, 1.8,
             'minute_0930_fallback', '2026-07-14 09:31:00');
        """
    )
    try:
        yield connection
    finally:
        connection.close()


def test_research_dataset_contracts_keep_schema_key_and_source_truth() -> None:
    minute = research_dataset_contract("minute_bar")
    auction = research_dataset_contract("auction_bar")

    assert minute.physical_primary_key == (
        "ts_code",
        "trade_time",
        "freq",
        "source",
    )
    assert minute.sources == ("tushare", "tushare_rt", "tushare_rt_daily")
    assert auction.physical_primary_key == (
        "ts_code",
        "trade_date",
        "auction_type",
        "source",
    )
    assert auction.sources == ("tushare", "minute_0930_fallback")
    assert research_export_schema("minute_bar")[-2:] == (
        ("source", "VARCHAR"),
        ("created_at", "TIMESTAMP"),
    )

    with pytest.raises(ValueError, match="research dataset"):
        research_dataset_contract("daily_bar")


def test_partition_paths_are_stable_and_frequency_aware() -> None:
    minute = ResearchPartitionKey(dataset="minute_bar", trade_date=date(2026, 7, 14), freq="1min")
    auction = ResearchPartitionKey(dataset="auction_bar", trade_date=date(2026, 7, 14))

    assert partition_directory(minute) == Path(
        "minute/freq=1min/year=2026/month=07/trade_date=2026-07-14"
    )
    assert partition_manifest_relative_path(auction) == Path(
        "auction/year=2026/month=07/trade_date=2026-07-14/manifest.json"
    )
    assert partition_version_relative_path(auction, "f" * 64) == Path(
        "auction/year=2026/month=07/trade_date=2026-07-14/versions/" + "f" * 64 + ".parquet"
    )

    with pytest.raises(ValueError, match="freq"):
        ResearchPartitionKey(dataset="minute_bar", trade_date=date(2026, 7, 14))
    with pytest.raises(ValueError, match="auction"):
        ResearchPartitionKey(dataset="auction_bar", trade_date=date(2026, 7, 14), freq="1min")


def test_read_only_catalog_never_creates_or_mutates_files(tmp_path: Path) -> None:
    catalog_path = tmp_path / "catalog" / "research.duckdb"

    with pytest.raises(ValueError, match="read-only research catalog"):
        ResearchCatalog(catalog_path, read_only=True)
    assert not catalog_path.parent.exists()

    writable = ResearchCatalog(catalog_path)
    with writable._connection():
        pass
    writable.lock_path.unlink()
    before_catalog = catalog_path.read_bytes()
    before_entries = tuple(sorted(path.name for path in catalog_path.parent.iterdir()))

    read_only = ResearchCatalog(catalog_path, read_only=True)

    assert read_only.list_partitions(
        dataset="minute_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        freq="1min",
    ) == []
    assert read_only.get_coverage("minute_bar") is None
    assert catalog_path.read_bytes() == before_catalog
    assert tuple(
        sorted(path.name for path in catalog_path.parent.iterdir())
    ) == before_entries


def test_export_writes_valid_parquet_manifest_and_catalog(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")

    summary = export_research_dataset(
        source_connection,
        catalog=catalog,
        lake_root=lake_root,
        dataset="minute_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        code_commit=_COMMIT,
    )

    assert summary.status == "completed"
    assert summary.partition_count == 1
    assert summary.row_count == 2
    assert summary.exported_count == 1
    assert summary.unchanged_count == 0
    partition = ResearchPartitionKey(
        dataset="minute_bar", trade_date=date(2026, 7, 14), freq="1min"
    )
    data_path = summary.partitions[0].data_path
    assert data_path is not None
    target = lake_root / data_path
    assert target.is_file()
    assert not list(target.parent.glob("*.tmp-*"))

    rows = (
        duckdb.connect()
        .execute("SELECT * FROM read_parquet(?) ORDER BY ts_code", [str(target)])
        .fetchall()
    )
    assert len(rows) == 2
    assert rows[0][0] == "000001.SZ"

    manifest_path = lake_root / partition_manifest_relative_path(partition)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["dataset"] == "minute_bar"
    assert manifest["partition"]["freq"] == "1min"
    assert manifest["row_count"] == 2
    assert manifest["earliest_time"] == "2026-07-14T09:30:00"
    assert manifest["latest_time"] == "2026-07-14T09:30:00"
    assert manifest["source"] == "mixed"
    assert manifest["sources"] == ["tushare", "tushare_rt_daily"]
    assert manifest["primary_key"] == [
        "ts_code",
        "trade_time",
        "freq",
        "source",
    ]
    assert len(manifest["schema_hash"]) == 64
    assert len(manifest["content_hash"]) == 64
    assert manifest["code_commit"] == _COMMIT

    current = catalog.get_partition(
        ResearchPartitionKey(
            dataset="minute_bar", trade_date=date(2026, 7, 14), freq="1min"
        ).partition_id
    )
    assert current is not None
    assert current.content_hash == manifest["content_hash"]
    coverage = catalog.get_coverage("minute_bar")
    assert coverage is not None
    assert coverage.partition_count == 1
    assert coverage.row_count == 2
    assert coverage.earliest_date == date(2026, 7, 14)
    assert coverage.latest_date == date(2026, 7, 14)

    original_connect = research_lake_module.duckdb.connect
    configs: list[dict[str, str] | None] = []

    def connect_spy(*args, **kwargs):
        configs.append(kwargs.get("config"))
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(
        research_lake_module.duckdb,
        "connect",
        connect_spy,
    )
    research_lake_module.verify_research_partition(
        lake_root=lake_root,
        manifest=research_lake_module.ResearchPartitionManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        ),
        as_of_time=datetime(2026, 7, 15, tzinfo=UTC),
    )

    assert configs == [
        {"temp_directory": ""},
        {"temp_directory": ""},
    ]


def test_same_partition_is_idempotent_and_change_records_replacement(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    kwargs = {
        "catalog": catalog,
        "lake_root": lake_root,
        "dataset": "auction_bar",
        "start_date": date(2026, 7, 14),
        "end_date": date(2026, 7, 14),
        "code_commit": _COMMIT,
    }

    first = export_research_dataset(source_connection, **kwargs)
    second = export_research_dataset(source_connection, **kwargs)

    assert first.exported_count == 1
    assert second.exported_count == 0
    assert second.unchanged_count == 1
    before = catalog.list_ingest_runs("auction_bar")
    assert [run.status for run in before] == ["exported", "unchanged"]

    source_connection.execute(
        """
        UPDATE auction_bar
        SET price = 10.2
        WHERE ts_code = '000001.SZ' AND trade_date = '2026-07-14'
        """
    )
    third = export_research_dataset(source_connection, **kwargs)

    assert third.replaced_count == 1
    runs = catalog.list_ingest_runs("auction_bar")
    assert [run.status for run in runs] == ["exported", "unchanged", "replaced"]
    replacement = runs[-1]
    assert replacement.previous_content_hash
    assert replacement.content_hash
    assert replacement.previous_content_hash != replacement.content_hash


def test_unknown_source_fails_closed_without_publishing(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    source_connection.execute(
        """
        INSERT INTO minute_bar VALUES
            ('000003.SZ', '2026-07-14 09:30:00', '1min', 30, 31, 29, 30.5,
             3000, 91500, 'mystery', '2026-07-14 16:00:00')
        """
    )
    catalog = ResearchCatalog(tmp_path / "research.duckdb")

    with pytest.raises(ValueError, match="unknown source"):
        export_research_dataset(
            source_connection,
            catalog=catalog,
            lake_root=tmp_path / "lake",
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
        )

    assert not (tmp_path / "lake").exists()
    runs = catalog.list_ingest_runs("minute_bar")
    assert len(runs) == 1
    assert runs[0].status == "failed"


def test_dry_run_plans_without_writing_files_or_catalog(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog_path = tmp_path / "research.duckdb"

    summary = export_research_dataset(
        source_connection,
        catalog=ResearchCatalog(catalog_path),
        lake_root=lake_root,
        dataset="minute_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 15),
        code_commit=_COMMIT,
        dry_run=True,
    )

    assert summary.status == "planned"
    assert summary.partition_count == 2
    assert summary.row_count == 3
    assert summary.partitions[0].trade_date == date(2026, 7, 14)
    assert not lake_root.exists()
    assert not catalog_path.exists()


def test_dry_run_rejects_unknown_source_without_writing_catalog(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    source_connection.execute(
        """
        INSERT INTO minute_bar VALUES
            ('000003.SZ', '2026-07-14 09:30:00', '1min', 30, 31, 29, 30.5,
             3000, 91500, 'mystery', '2026-07-14 16:00:00')
        """
    )
    catalog_path = tmp_path / "research.duckdb"

    with pytest.raises(ValueError, match="unknown source"):
        export_research_dataset(
            source_connection,
            catalog=ResearchCatalog(catalog_path),
            lake_root=tmp_path / "lake",
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
            dry_run=True,
        )

    assert not catalog_path.exists()
    assert not (tmp_path / "lake").exists()


def test_corrupt_published_file_is_replaced_not_trusted_from_manifest(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    kwargs = {
        "catalog": catalog,
        "lake_root": lake_root,
        "dataset": "auction_bar",
        "start_date": date(2026, 7, 14),
        "end_date": date(2026, 7, 14),
        "code_commit": _COMMIT,
    }
    first = export_research_dataset(source_connection, **kwargs)
    data_path = first.partitions[0].data_path
    assert data_path is not None
    target = lake_root / data_path
    target.write_bytes(b"corrupt")

    second = export_research_dataset(source_connection, **kwargs)

    assert second.replaced_count == 1
    assert duckdb.connect().execute(
        "SELECT COUNT(*) FROM read_parquet(?)", [str(target)]
    ).fetchone() == (2,)
    replacement = catalog.list_ingest_runs("auction_bar")[-1]
    assert replacement.status == "replaced"
    assert replacement.previous_content_hash == replacement.content_hash
    assert replacement.previous_file_hash is not None
    assert replacement.observed_previous_file_hash is not None
    assert replacement.observed_previous_file_hash != replacement.previous_file_hash


def test_logically_identical_rows_ignore_new_parquet_byte_encoding(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    kwargs = {
        "catalog": catalog,
        "lake_root": lake_root,
        "dataset": "auction_bar",
        "start_date": date(2026, 7, 14),
        "end_date": date(2026, 7, 14),
        "code_commit": _COMMIT,
    }
    first = export_research_dataset(source_connection, **kwargs)
    original_hash = research_lake_module._file_sha256

    def changed_encoder_hash(path: Path) -> str:
        if ".tmp-" in path.name:
            return "b" * 64
        return original_hash(path)

    monkeypatch.setattr(research_lake_module, "_file_sha256", changed_encoder_hash)
    second = export_research_dataset(source_connection, **kwargs)

    assert second.unchanged_count == 1
    assert second.partitions[0].data_path == first.partitions[0].data_path


def test_existing_manifest_must_bind_to_current_partition(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    partition = ResearchPartitionKey(dataset="auction_bar", trade_date=date(2026, 7, 14))
    kwargs = {
        "catalog": ResearchCatalog(tmp_path / "research.duckdb"),
        "lake_root": lake_root,
        "dataset": "auction_bar",
        "start_date": date(2026, 7, 14),
        "end_date": date(2026, 7, 14),
        "code_commit": _COMMIT,
    }
    export_research_dataset(source_connection, **kwargs)
    manifest_path = lake_root / partition_manifest_relative_path(partition)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["partition"]["trade_date"] = "2026-07-15"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest binding mismatch"):
        export_research_dataset(source_connection, **kwargs)


def test_manifest_content_hash_must_bind_to_referenced_parquet(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    partition = ResearchPartitionKey(dataset="auction_bar", trade_date=date(2026, 7, 14))
    kwargs = {
        "catalog": catalog,
        "lake_root": lake_root,
        "dataset": "auction_bar",
        "start_date": date(2026, 7, 14),
        "end_date": date(2026, 7, 14),
        "code_commit": _COMMIT,
    }
    old = export_research_dataset(source_connection, **kwargs).partitions[0].manifest
    assert old is not None
    source_connection.execute(
        "UPDATE auction_bar SET price = 10.2 WHERE ts_code = '000001.SZ'"
    )
    current = export_research_dataset(source_connection, **kwargs).partitions[0].manifest
    assert current is not None

    manifest_path = lake_root / partition_manifest_relative_path(partition)
    payload = current.model_dump(mode="json")
    payload["relative_path"] = old.relative_path
    payload["file_hash"] = old.file_hash
    payload["file_size"] = old.file_size
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    repaired = export_research_dataset(source_connection, **kwargs)

    assert repaired.replaced_count == 1
    assert repaired.partitions[0].data_path == current.relative_path
    published = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert published["file_hash"] == current.file_hash
    assert published["content_hash"] == current.content_hash


def test_crash_before_manifest_commit_leaves_only_unreferenced_version(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    kwargs = {
        "catalog": catalog,
        "lake_root": lake_root,
        "dataset": "auction_bar",
        "start_date": date(2026, 7, 14),
        "end_date": date(2026, 7, 14),
        "code_commit": _COMMIT,
    }
    original_writer = research_lake_module._write_manifest_temp

    def fail_manifest(*_: object, **__: object) -> None:
        raise RuntimeError("simulated crash before commit point")

    monkeypatch.setattr(research_lake_module, "_write_manifest_temp", fail_manifest)
    with pytest.raises(RuntimeError, match="simulated crash"):
        export_research_dataset(source_connection, **kwargs)

    partition = ResearchPartitionKey(dataset="auction_bar", trade_date=date(2026, 7, 14))
    assert not (lake_root / partition_manifest_relative_path(partition)).exists()
    versions = lake_root / partition_directory(partition) / "versions"
    assert len(list(versions.glob("*.parquet"))) == 1

    monkeypatch.setattr(research_lake_module, "_write_manifest_temp", original_writer)
    recovered = export_research_dataset(source_connection, **kwargs)

    assert recovered.exported_count == 1
    assert (lake_root / partition_manifest_relative_path(partition)).is_file()
    assert [run.status for run in catalog.list_ingest_runs("auction_bar")] == [
        "failed",
        "exported",
    ]


def test_catalog_can_catch_up_after_multiple_manifest_generations(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    partition = ResearchPartitionKey(dataset="auction_bar", trade_date=date(2026, 7, 14))
    kwargs = {
        "catalog": catalog,
        "lake_root": lake_root,
        "dataset": "auction_bar",
        "start_date": date(2026, 7, 14),
        "end_date": date(2026, 7, 14),
        "code_commit": _COMMIT,
    }
    first = export_research_dataset(source_connection, **kwargs).partitions[0].manifest
    assert first is not None
    source_connection.execute(
        "UPDATE auction_bar SET price = 10.2 WHERE ts_code = '000001.SZ'"
    )
    original_finish = catalog.finish_run

    def fail_replacement_catalog_write(run_id: str, **call_kwargs: object) -> None:
        if call_kwargs["status"] == "replaced":
            raise RuntimeError("simulated catalog outage after manifest commit")
        original_finish(run_id, **call_kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(catalog, "finish_run", fail_replacement_catalog_write)
    with pytest.raises(RuntimeError, match="catalog outage"):
        export_research_dataset(source_connection, **kwargs)

    manifest_path = lake_root / partition_manifest_relative_path(partition)
    second = json.loads(manifest_path.read_text(encoding="utf-8"))
    indexed = catalog.get_partition(partition.partition_id)
    assert indexed is not None
    assert indexed.content_hash == first.content_hash
    assert second["content_hash"] != first.content_hash

    monkeypatch.setattr(catalog, "finish_run", original_finish)
    source_connection.execute(
        "UPDATE auction_bar SET price = 10.4 WHERE ts_code = '000001.SZ'"
    )
    recovered = export_research_dataset(source_connection, **kwargs)

    assert recovered.replaced_count == 1
    recovered_manifest = recovered.partitions[0].manifest
    assert recovered_manifest is not None
    assert recovered_manifest.parent_content_hash == second["content_hash"]
    indexed = catalog.get_partition(partition.partition_id)
    assert indexed is not None
    assert indexed.content_hash == recovered_manifest.content_hash


def test_same_partition_exports_are_serialized(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = Path(source_connection.execute("PRAGMA database_list").fetchone()[2])
    lake_root = tmp_path / "lake"
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    original_writer = research_lake_module._write_partition_temp
    guard = threading.Lock()
    active = 0
    max_active = 0

    def slow_writer(*args: object, **kwargs: object) -> None:
        nonlocal active, max_active
        with guard:
            active += 1
            max_active = max(max_active, active)
        try:
            time.sleep(0.05)
            original_writer(*args, **kwargs)  # type: ignore[arg-type]
        finally:
            with guard:
                active -= 1

    monkeypatch.setattr(research_lake_module, "_write_partition_temp", slow_writer)

    def run_once() -> str:
        connection = duckdb.connect(str(source_path))
        try:
            result = export_research_dataset(
                connection,
                catalog=catalog,
                lake_root=lake_root,
                dataset="auction_bar",
                start_date=date(2026, 7, 14),
                end_date=date(2026, 7, 14),
                code_commit=_COMMIT,
            )
            return result.partitions[0].status
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(run_once)
        second = executor.submit(run_once)
        statuses = sorted((first.result(), second.result()))

    assert statuses == ["exported", "unchanged"]
    assert max_active == 1


def test_partition_validation_rejects_rows_from_another_date(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def write_mixed_dates(
        connection: duckdb.DuckDBPyConnection,
        *,
        temp_path: Path,
        **_: object,
    ) -> None:
        path = str(temp_path).replace("'", "''")
        connection.execute(
            f"""
            COPY (
                SELECT * FROM minute_bar
                WHERE ts_code = '000001.SZ'
                ORDER BY trade_time
            ) TO '{path}' (FORMAT PARQUET)
            """
        )

    monkeypatch.setattr(
        research_lake_module,
        "_write_partition_temp",
        write_mixed_dates,
    )
    lake_root = tmp_path / "lake"

    with pytest.raises(ValueError, match="partition mismatch"):
        export_research_dataset(
            source_connection,
            catalog=ResearchCatalog(tmp_path / "research.duckdb"),
            lake_root=lake_root,
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
        )

    assert not list(lake_root.rglob("data.parquet"))


def test_unchanged_export_repairs_catalog_missing_after_file_publish(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    lake_root = tmp_path / "lake"
    catalog_path = tmp_path / "research.duckdb"
    kwargs = {
        "lake_root": lake_root,
        "dataset": "auction_bar",
        "start_date": date(2026, 7, 14),
        "end_date": date(2026, 7, 14),
        "code_commit": _COMMIT,
    }
    first = export_research_dataset(
        source_connection,
        catalog=ResearchCatalog(catalog_path),
        **kwargs,
    )
    manifest = first.partitions[0].manifest
    assert manifest is not None
    partition_id = manifest.partition.partition_id
    catalog_path.unlink()

    repaired_catalog = ResearchCatalog(catalog_path)
    second = export_research_dataset(
        source_connection,
        catalog=repaired_catalog,
        **kwargs,
    )

    assert second.unchanged_count == 1
    assert repaired_catalog.get_partition(partition_id) is not None
    assert repaired_catalog.get_coverage("auction_bar") is not None


def test_manifest_created_at_is_utc_and_stable_model_roundtrip(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    summary = export_research_dataset(
        source_connection,
        catalog=ResearchCatalog(tmp_path / "research.duckdb"),
        lake_root=tmp_path / "lake",
        dataset="auction_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 16, 8, 0, tzinfo=UTC),
    )

    manifest = summary.partitions[0].manifest
    assert manifest is not None
    assert manifest.created_at == datetime(2026, 7, 16, 8, 0, tzinfo=UTC)
    assert manifest.model_validate_json(manifest.model_dump_json()) == manifest


def test_export_rejects_schema_drift_from_canonical_contract(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    source_connection.execute("ALTER TABLE minute_bar ADD COLUMN surprise VARCHAR")

    with pytest.raises(ValueError, match="schema mismatch"):
        export_research_dataset(
            source_connection,
            catalog=ResearchCatalog(tmp_path / "research.duckdb"),
            lake_root=tmp_path / "lake",
            dataset="minute_bar",
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            code_commit=_COMMIT,
            dry_run=True,
        )


def test_export_rejects_pre_availability_or_closed_day_partition(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    source_connection.execute(
        """
        INSERT INTO auction_bar VALUES
            ('000001.SZ', '2024-12-31', 'open', 10.0, 1000, 10000, 0.1, 1.5,
             'tushare', '2024-12-31 09:26:00');
        INSERT INTO trade_calendar VALUES ('SSE', '2024-12-31', TRUE);
        INSERT INTO minute_bar VALUES
            ('000001.SZ', '2026-07-13 09:30:00', '1min', 10, 10.2, 9.9, 10.1,
             1000, 10100, 'tushare', '2026-07-13 16:00:00');
        """
    )

    with pytest.raises(ValueError, match="earliest date"):
        export_research_dataset(
            source_connection,
            catalog=ResearchCatalog(tmp_path / "research.duckdb"),
            lake_root=tmp_path / "lake",
            dataset="auction_bar",
            start_date=date(2024, 12, 31),
            end_date=date(2024, 12, 31),
            code_commit=_COMMIT,
            dry_run=True,
        )
    with pytest.raises(ValueError, match="closed or missing trade date"):
        export_research_dataset(
            source_connection,
            catalog=ResearchCatalog(tmp_path / "research.duckdb"),
            lake_root=tmp_path / "lake",
            dataset="minute_bar",
            start_date=date(2026, 7, 13),
            end_date=date(2026, 7, 13),
            code_commit=_COMMIT,
            dry_run=True,
        )
    with pytest.raises(ValueError, match="partition is in the future"):
        export_research_dataset(
            source_connection,
            catalog=ResearchCatalog(tmp_path / "research.duckdb"),
            lake_root=tmp_path / "lake",
            dataset="minute_bar",
            start_date=date(2026, 7, 15),
            end_date=date(2026, 7, 15),
            code_commit=_COMMIT,
            dry_run=True,
            as_of_date=date(2026, 7, 14),
        )


def test_non_dry_run_requires_clean_full_commit(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    for commit in ("unknown", f"{_COMMIT}-dirty", "abc123"):
        with pytest.raises(ValueError, match="clean 40-character code commit"):
            export_research_dataset(
                source_connection,
                catalog=ResearchCatalog(tmp_path / "research.duckdb"),
                lake_root=tmp_path / "lake",
                dataset="minute_bar",
                start_date=date(2026, 7, 14),
                end_date=date(2026, 7, 14),
                code_commit=commit,
            )
    assert not (tmp_path / "lake").exists()


def test_next_attempt_closes_interrupted_running_ingest(
    source_connection: duckdb.DuckDBPyConnection,
    tmp_path: Path,
) -> None:
    catalog = ResearchCatalog(tmp_path / "research.duckdb")
    partition_id = "auction_bar:2026-07-14"
    catalog.begin_run(
        dataset="auction_bar",
        partition_id=partition_id,
        code_commit=_COMMIT,
    )

    export_research_dataset(
        source_connection,
        catalog=catalog,
        lake_root=tmp_path / "lake",
        dataset="auction_bar",
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
        code_commit=_COMMIT,
    )

    runs = catalog.list_ingest_runs("auction_bar")
    assert [run.status for run in runs] == ["failed", "exported"]
    assert runs[0].error == "superseded after interrupted export"
