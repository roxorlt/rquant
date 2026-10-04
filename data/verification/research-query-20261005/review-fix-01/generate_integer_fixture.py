"""Generate actual DuckDB → product scalar → Pydantic JSON input for the React test."""

# The isolated interpreter must select this worktree before importing product code.
# ruff: noqa: E402

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

worktree = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(worktree / "src"))

import duckdb

from rquant.research_query.child import scalar
from rquant.research_query.contracts import QueryResult

values = [-(2**53 + 1), -(2**53), -(2**53 - 1), 2**53 - 1, 2**53, 2**53 + 1]
rows = []
with duckdb.connect(":memory:") as connection:
    for value in values:
        actual = connection.execute("SELECT ?::BIGINT", [value]).fetchone()[0]
        assert actual == value and type(actual) is int
        rows.append((scalar(actual),))
wire = QueryResult(
    status="ready", columns=({"name": "n", "data_type": "BIGINT"},), rows=rows, elapsed_ms=1
).model_dump_json()
fixture = worktree / "web/src/pages/query/bigint-wire.fixture.json"
previous = Path(__file__).resolve().parent / "bigint-wire-red.fixture.json"
if not previous.exists():
    previous.write_bytes(fixture.read_bytes())
fixture.write_text(
    json.dumps(
        {
            "origin": "Actual DuckDB BIGINT → child.scalar → QueryResult JSON",
            "duckdb_version": duckdb.__version__,
            "wire_json": wire,
            "expected_text": [str(value) for value in values],
        },
        ensure_ascii=False,
        indent=2,
    )
    + "\n"
)
print(
    json.dumps(
        {
            "path": str(fixture.relative_to(worktree)),
            "sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
        }
    )
)
