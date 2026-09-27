"""Only explicitly approved business fields can enter the catalog sample artifact."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from rquant.data_catalog.models import CatalogDocument
from rquant.data_catalog.sample_policy import SAMPLE_FIELDS
from rquant.data_catalog.samples import build_samples

CATALOG = Path(__file__).resolve().parents[2] / "src/rquant/data_catalog/catalog-v1.json"


def _table(connection: duckdb.DuckDBPyConnection, dataset_id: str) -> None:
    catalog = CatalogDocument.model_validate_json(CATALOG.read_text(encoding="utf-8"))
    item = next(item for item in catalog.datasets if item.dataset_id == dataset_id)
    columns = ", ".join(f'"{field.key}" {field.data_type}' for field in item.fields)
    connection.execute(f'CREATE TABLE "{item.table_name}" ({columns})')


def _source(path: Path) -> None:
    with duckdb.connect(str(path)) as connection:
        _table(connection, "stock_status_daily")
        _table(connection, "stock_suspend_coverage")
        connection.executemany(
            "INSERT INTO stock_status_daily "
            "(ts_code, trade_date, name, is_st, name_source, st_source, conflict_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "000001.SZ",
                    f"2026-09-{day:02d}",
                    "/private/secrets/source_file" if day == 25 else "平安银行",
                    False,
                    "notifier.admin.shadow.v1",
                    "/private/secrets/price.json",
                    '{"source_file":"/private/secrets/price.json"}',
                )
                for day in range(1, 26)
            ],
        )
        connection.execute(
            "INSERT INTO stock_suspend_coverage "
            "(source, trade_date, coverage_state, row_count, snapshot_hash, queried_at) "
            "VALUES ('svc-internal', DATE '2026-09-25', 'degraded:internal', 3, ?, "
            "TIMESTAMP '2026-09-25 10:00:00')",
            ["a" * 64],
        )


def test_each_catalog_dataset_has_an_explicit_small_business_whitelist() -> None:
    catalog = CatalogDocument.model_validate_json(CATALOG.read_text(encoding="utf-8"))
    assert set(SAMPLE_FIELDS) == {item.dataset_id for item in catalog.datasets}
    forbidden = {
        "source",
        "source_file",
        "snapshot_hash",
        "conflict_reason",
        "name_source",
        "st_source",
    }
    for item in catalog.datasets:
        selected = SAMPLE_FIELDS[item.dataset_id]
        assert 2 <= len(selected) <= 6
        assert len(selected) == len(set(selected))
        assert set(selected) <= {field.key for field in item.fields}
        assert not set(selected) & forbidden


def test_builder_writes_only_safe_columns_and_latest_twenty_rows(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.duckdb"
    output = tmp_path / "samples.json"
    _source(source)

    payload = build_samples(source, output)
    status = payload["datasets"]["stock_status_daily"]
    assert status["state"] == "available"
    assert len(status["rows"]) == 20
    assert list(status["rows"][0]) == list(SAMPLE_FIELDS["stock_status_daily"])
    assert [row["trade_date"] for row in status["rows"]] == [
        f"2026-09-{day:02d}" for day in range(25, 5, -1)
    ]
    assert status["rows"][0]["name"] is None
    assert status["rows"][1]["name"] == "平安银行"
    assert payload["datasets"]["stock_suspend_coverage"]["rows"][0]["row_count"] == 3
    assert payload["datasets"]["daily_bar"] == {"state": "missing", "rows": []}
    serialized = output.read_text(encoding="utf-8")
    for secret in (
        "notifier.admin.shadow.v1",
        "/private/secrets/price.json",
        "source_file",
        "conflict_reason",
        "snapshot_hash",
        "a" * 64,
        "svc-internal",
        "degraded:internal",
    ):
        assert secret not in serialized


def test_bad_schema_and_output_alias_preserve_source_and_previous_artifact(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.duckdb"
    output = tmp_path / "samples.json"
    _source(source)
    build_samples(source, output)
    before = output.read_bytes()
    source_bytes = source.read_bytes()

    for alias in (source, tmp_path / "alias.json", tmp_path / "hardlink.json"):
        if alias.name == "alias.json":
            alias.symlink_to(source)
        if alias.name == "hardlink.json":
            alias.hardlink_to(source)
        with pytest.raises(ValueError, match="output must differ from source"):
            build_samples(source, alias)
        assert source.read_bytes() == source_bytes

    with duckdb.connect(str(source)) as connection:
        connection.execute("ALTER TABLE stock_status_daily ADD COLUMN source_file VARCHAR")
    with pytest.raises(ValueError, match="stock_status_daily"):
        build_samples(source, output)
    assert output.read_bytes() == before


def test_empty_table_nonfinite_value_and_failed_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "snapshot.duckdb"
    output = tmp_path / "samples.json"
    with duckdb.connect(str(source)) as connection:
        _table(connection, "daily_bar")
        _table(connection, "adj_factor")
        connection.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close, pct_chg, vol) "
            "VALUES ('000001.SZ', DATE '2026-09-25', 'NaN'::DOUBLE, "
            "'Infinity'::DOUBLE, 100)"
        )
    payload = build_samples(source, output)
    assert payload["datasets"]["daily_bar"]["rows"][0]["close"] is None
    assert payload["datasets"]["daily_bar"]["rows"][0]["pct_chg"] is None
    assert payload["datasets"]["adj_factor"] == {"state": "empty", "rows": []}
    before = output.read_bytes()

    def fail_replace(_source: Path, _destination: Path) -> None:
        raise OSError("synthetic rename failure")

    monkeypatch.setattr("rquant.data_catalog.samples.os.replace", fail_replace)
    with pytest.raises(OSError, match="synthetic rename failure"):
        build_samples(source, output)
    assert output.read_bytes() == before
    assert not list(tmp_path.glob(".samples-*"))
