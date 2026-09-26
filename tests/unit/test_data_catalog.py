"""The published directory is built from real contracts and a fresh local schema."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from rquant.data_catalog.build import build_catalog, schema_from_connection
from rquant.data_catalog.descriptions import DATASETS, FIELDS, DatasetCopy, FieldCopy
from rquant.data_catalog.models import CatalogDocument
from rquant.data_contracts import DATASET_CONTRACTS
from rquant.storage.migrations import initialize_schema


def test_catalog_keeps_contract_truth_and_real_field_types() -> None:
    contract = DATASET_CONTRACTS[0]
    result = build_catalog(
        (contract,),
        {"daily_bar": (("ts_code", "VARCHAR"), ("trade_date", "DATE"), ("pct_chg", "DOUBLE"))},
        {"daily_bar": DatasetCopy("日线行情", "查看每日价格变化", "行情")},
        {
            "ts_code": FieldCopy("证券代码", "该记录的证券代码"),
            "trade_date": FieldCopy("交易日期", "行情所属交易日"),
            "pct_chg": FieldCopy("涨跌幅", "当日涨跌幅", "%"),
        },
    )

    item = result.datasets[0]
    assert (item.dataset_id, item.table_name, item.name) == ("daily_bar", "daily_bar", "日线行情")
    assert item.primary_key == ["ts_code", "trade_date"]
    assert [(field.key, field.data_type, field.unit) for field in item.fields] == [
        ("ts_code", "VARCHAR", None),
        ("trade_date", "DATE", None),
        ("pct_chg", "DOUBLE", "%"),
    ]
    assert item.schema_available is True
    assert item.sample_available is False
    assert item.update_note == "最多落后 1 个交易日"


def test_missing_handwritten_description_is_a_build_error() -> None:
    contract = DATASET_CONTRACTS[0]
    copy = DatasetCopy("日线行情", "查看每日价格变化", "行情")
    with pytest.raises(ValueError, match="missing dataset description.*daily_bar"):
        build_catalog((contract,), {"daily_bar": ()}, {}, {})
    with pytest.raises(ValueError, match="missing field description.*pct_chg"):
        build_catalog(
            (contract,),
            {"daily_bar": (("ts_code", "VARCHAR"), ("trade_date", "DATE"), ("pct_chg", "DOUBLE"))},
            {"daily_bar": copy},
            {"ts_code": FieldCopy("代码", "证券代码"), "trade_date": FieldCopy("日期", "交易日期")},
        )


def test_missing_schema_is_explicit_and_does_not_invent_fields() -> None:
    result = build_catalog(
        (DATASET_CONTRACTS[0],),
        {},
        {"daily_bar": DatasetCopy("日线行情", "查看每日价格变化", "行情")},
        {},
    )
    item = result.datasets[0]
    assert item.schema_available is False
    assert item.fields == []
    assert item.sample_available is False


def test_new_source_needs_a_plain_name_before_publication() -> None:
    contract = DATASET_CONTRACTS[0].model_copy(update={"sources": ("new_source",)})
    with pytest.raises(ValueError, match="missing source name.*new_source"):
        build_catalog(
            (contract,),
            {},
            {"daily_bar": DatasetCopy("日线行情", "查看每日价格变化", "行情")},
            {},
        )


def test_all_real_contracts_are_described_and_snapshot_matches(tmp_path: Path) -> None:
    with duckdb.connect(":memory:") as connection:
        initialize_schema(connection)
        schemas = schema_from_connection(connection)
    result = build_catalog(DATASET_CONTRACTS, schemas, DATASETS, FIELDS)
    assert len(result.datasets) == len(DATASET_CONTRACTS)
    assert all(item.name and item.purpose and item.fields for item in result.datasets)
    assert all(
        field.name and field.description for item in result.datasets for field in item.fields
    )
    artifact = Path(__file__).resolve().parents[2] / "src/rquant/data_catalog/catalog-v1.json"
    assert CatalogDocument.model_validate_json(artifact.read_text(encoding="utf-8")) == result
    assert json.loads(artifact.read_text(encoding="utf-8"))["version"] == 1
