"""Build bounded public examples from an explicitly supplied read-only DuckDB copy."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from rquant.data_catalog.build import CATALOG_CONTRACTS, schema_from_connection
from rquant.data_catalog.models import (
    CatalogDocument,
    CatalogSample,
    CatalogSamplesDocument,
    SampleState,
)
from rquant.data_catalog.sample_policy import public_sample_value, sample_fields

CATALOG_FILE = Path(__file__).with_name("catalog-v1.json")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\Z")
MAX_ARTIFACT_BYTES = 5_000_000
MAX_ROWS = 20


def _identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError("catalog contains an invalid SQL identifier")
    return f'"{value}"'


def _validate_paths(readonly_db: Path, destination: Path) -> Path:
    source = readonly_db.resolve(strict=True)
    target = destination.resolve(strict=False)
    if source == target or (destination.exists() and destination.samefile(source)):
        raise ValueError("output must differ from source")
    if not source.is_file():
        raise ValueError("read-only source must be a file")
    return source


def _ordered_columns(fields: list[str]) -> list[str]:
    temporal = [
        key
        for key in ("trade_time", "trade_date", "updated_at", "queried_at", "available_at")
        if key in fields
    ]
    return [*temporal, *(key for key in fields if key not in temporal)]


def build_samples(readonly_db: Path, destination: Path) -> dict[str, object]:
    """Publish only reviewed columns after every present table has passed schema checks."""
    source = _validate_paths(readonly_db, destination)
    catalog = CatalogDocument.model_validate_json(CATALOG_FILE.read_text(encoding="utf-8"))
    by_id = {item.dataset_id: item for item in catalog.datasets}
    if set(by_id) != {contract.dataset_id for contract in CATALOG_CONTRACTS}:
        raise ValueError("catalog dataset registry drift")

    datasets: dict[str, CatalogSample] = {}
    with duckdb.connect(str(source), read_only=True) as connection:
        schemas = schema_from_connection(connection)
        for contract in CATALOG_CONTRACTS:
            item = by_id[contract.dataset_id]
            table = item.table_name
            if table != contract.table_name:
                raise ValueError(f"catalog table drift: {item.dataset_id}")
            selected = sample_fields(item.dataset_id, item.fields)
            if not selected:
                datasets[item.dataset_id] = CatalogSample(state=SampleState.UNSUPPORTED, rows=[])
                continue
            schema = schemas.get(table)
            if schema is None:
                datasets[item.dataset_id] = CatalogSample(state=SampleState.MISSING, rows=[])
                continue
            expected = tuple((field.key, field.data_type) for field in item.fields)
            if schema != expected:
                raise ValueError(f"source schema differs from catalog: {item.dataset_id}")
            keys = [field.key for field in selected]
            columns = ", ".join(_identifier(key) for key in keys)
            ordering = ", ".join(
                f"{_identifier(key)} DESC NULLS LAST" for key in _ordered_columns(keys)
            )
            query = (
                f"SELECT {columns} FROM {_identifier(table)} ORDER BY {ordering} LIMIT {MAX_ROWS}"
            )
            records = connection.execute(query).fetchall()
            rows = [
                {
                    field.key: public_sample_value(field, value)
                    for field, value in zip(selected, record, strict=True)
                }
                for record in records
            ]
            state = SampleState.AVAILABLE if rows else SampleState.EMPTY
            datasets[item.dataset_id] = CatalogSample(state=state, rows=rows)

    payload = CatalogSamplesDocument(built_at=datetime.now(UTC), datasets=datasets).model_dump(
        mode="json"
    )
    serialized = (
        json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    if len(serialized) > MAX_ARTIFACT_BYTES:
        raise ValueError("sample artifact exceeds size limit")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent, prefix=".samples-", suffix=".json", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build public data directory samples")
    parser.add_argument("--readonly-db", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    payload = build_samples(args.readonly_db, args.output)
    counts = {
        state: sum(item["state"] == state for item in payload["datasets"].values())
        for state in ("available", "empty", "missing", "unsupported")
    }
    print(json.dumps(counts, ensure_ascii=False))


if __name__ == "__main__":
    main()
