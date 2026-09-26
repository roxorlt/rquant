"""Build the public directory from declared contracts and a fresh local schema."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import duckdb

from rquant.data_catalog.descriptions import DatasetCopy, FieldCopy
from rquant.data_catalog.models import CatalogDataset, CatalogDocument, CatalogField
from rquant.data_contracts import DATASET_CONTRACTS, DatasetContract, VisibilityRule
from rquant.storage.migrations import initialize_schema

SCHEMA = Mapping[str, tuple[tuple[str, str], ...]]

SOURCE_NAMES = {
    "tushare": "Tushare Pro",
    "tushare_rt": "Tushare 实时行情",
    "tushare_rt_daily": "Tushare 实时日线",
    "minute_0930_fallback": "09:30 分钟行情兜底",
}


def schema_from_connection(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, tuple[tuple[str, str], ...]]:
    """Read physical column types; never inspect a user's or production database."""
    rows = connection.execute(
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'main' ORDER BY table_name, ordinal_position"
    ).fetchall()
    found: dict[str, list[tuple[str, str]]] = {}
    for table, name, data_type in rows:
        found.setdefault(str(table), []).append((str(name), str(data_type)))
    return {table: tuple(columns) for table, columns in found.items()}


def _update_note(contract: DatasetContract) -> str:
    freshness = contract.freshness
    if freshness.max_wall_clock_lag is not None:
        minutes = int(freshness.max_wall_clock_lag.total_seconds() / 60)
        return f"最多延迟 {minutes} 分钟"
    if freshness.event_driven:
        return "有事件时更新"
    if freshness.max_trading_session_lag == 0:
        return "交易日数据需在当日可用"
    if freshness.max_trading_session_lag is not None:
        return f"最多落后 {freshness.max_trading_session_lag} 个交易日"
    return "更新要求待确认"


def _visibility_note(contract: DatasetContract) -> str:
    if contract.visibility is VisibilityRule.MINUTE_AS_OF:
        return "记录时间之后可见"
    if contract.visibility is VisibilityRule.PANEL_CLOSE_NEXT_SESSION:
        return "下一交易日可见"
    if contract.visibility is VisibilityRule.AUCTION_0925:
        times = ", ".join(
            f"{SOURCE_NAMES.get(item.source, item.source)} {item.available_at:%H:%M}"
            for item in contract.source_availability
        )
        return f"竞价后可见（{times}）"
    return "可见时刻待确认"


def build_catalog(
    contracts: Sequence[DatasetContract],
    schemas: SCHEMA,
    descriptions: Mapping[str, DatasetCopy],
    field_descriptions: Mapping[str, FieldCopy],
) -> CatalogDocument:
    """Strictly join human copy to contracts and physical types, with no inferred columns."""
    datasets: list[CatalogDataset] = []
    for contract in contracts:
        copy = descriptions.get(contract.dataset_id)
        if (
            copy is None
            or not copy.name.strip()
            or not copy.purpose.strip()
            or not copy.category.strip()
        ):
            raise ValueError(f"missing dataset description: {contract.dataset_id}")
        unknown_sources = set(contract.sources) - SOURCE_NAMES.keys()
        if unknown_sources:
            raise ValueError(f"missing source name: {', '.join(sorted(unknown_sources))}")
        schema = schemas.get(contract.table_name)
        fields: list[CatalogField] = []
        if schema is not None:
            names = {name for name, _ in schema}
            required = {*contract.physical_primary_key, contract.freshness.watermark_column}
            required.update(
                name
                for name in (
                    contract.event_date_column,
                    contract.event_time_column,
                    contract.ingested_at_column,
                )
                if name is not None
            )
            if missing := required - names:
                missing_text = ", ".join(sorted(missing))
                raise ValueError(
                    f"contract columns missing from schema: {contract.dataset_id}: {missing_text}"
                )
            for name, data_type in schema:
                field = field_descriptions.get(name)
                if field is None or not field.name.strip() or not field.description.strip():
                    raise ValueError(f"missing field description: {contract.dataset_id}.{name}")
                fields.append(
                    CatalogField(
                        key=name,
                        name=field.name,
                        description=field.description,
                        data_type=data_type,
                        unit=field.unit,
                        is_primary_key=name in contract.physical_primary_key,
                    )
                )
        datasets.append(
            CatalogDataset(
                dataset_id=contract.dataset_id,
                table_name=contract.table_name,
                name=copy.name,
                purpose=copy.purpose,
                category=copy.category,
                sources=[SOURCE_NAMES[source] for source in contract.sources],
                update_note=_update_note(contract),
                visibility_note=_visibility_note(contract),
                primary_key=list(contract.physical_primary_key),
                schema_available=schema is not None,
                fields=fields,
            )
        )
    return CatalogDocument(version=1, datasets=datasets)


def write_current_catalog(destination: Path) -> CatalogDocument:
    """Generate without opening any existing DuckDB file or touching production state."""
    from rquant.data_catalog.descriptions import DATASETS, FIELDS

    contract_ids = {contract.dataset_id for contract in DATASET_CONTRACTS}
    if set(DATASETS) != contract_ids:
        missing = sorted(contract_ids - set(DATASETS))
        obsolete = sorted(set(DATASETS) - contract_ids)
        raise ValueError(
            f"dataset descriptions must match contracts: missing={missing}, obsolete={obsolete}"
        )
    with duckdb.connect(":memory:") as connection:
        initialize_schema(connection)
        schemas = schema_from_connection(connection)
    document = build_catalog(DATASET_CONTRACTS, schemas, DATASETS, FIELDS)
    destination.write_text(
        json.dumps(document.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return document
