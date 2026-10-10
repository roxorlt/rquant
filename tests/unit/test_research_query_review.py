"""RQ-R02/R04/R05: original final-review counterexamples and direct boundaries."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import duckdb
import pytest

from rquant.research_query import QueryRequest, QueryResult, VerifiedQuerySnapshot
from rquant.research_query.child import scalar
from rquant.research_query.snapshot import verify_public_schema
from tests.unit.test_research_query import _published


@pytest.mark.parametrize("value", [-(2**53 + 1), -(2**53), -1, 0, 2**53 - 1, 2**53, 2**53 + 1])
def test_rq_r02_actual_integer_wire_preserves_safe_boundary(value: int) -> None:
    with duckdb.connect(":memory:") as connection:
        cursor = connection.execute(f"SELECT {value} AS n")
        original = cursor.fetchone()[0]
        kind = str(cursor.description[0][1])
    assert original == value and kind in {"BIGINT", "INTEGER"}
    encoded = scalar(original)
    if abs(value) <= 2**53 - 1:
        assert encoded == value and type(encoded) is int
    else:
        assert encoded == {"kind": "integer", "text": str(value)}
    actual = QueryResult(
        status="ready",
        columns=({"name": "n", "data_type": kind},),
        rows=((encoded,),),
        elapsed_ms=1,
    )
    decoded = json.loads(actual.model_dump_json())["rows"][0][0]
    assert decoded == encoded


def test_rq_r02_raw_unsafe_integer_is_not_accepted_as_float() -> None:
    for value in (-(2**53), 2**53, 2**53 + 1):
        with pytest.raises(ValueError):
            QueryResult(status="ready", rows=((value,),), elapsed_ms=1)
    assert scalar(True) is True and scalar(False) is False


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT $$COPY;$$ AS text", "COPY;"),
        ("SELECT $label$COPY; read_csv$label$ AS text", "COPY; read_csv"),
        ("SELECT 1 /* outer /* inner */ COPY ; */", 1),
        (r"SELECT E'\'COPY;' AS text", "'COPY;"),
    ],
)
def test_rq_r04_legal_literals_reach_actual_select_parser(sql: str, expected: object) -> None:
    request = QueryRequest(sql=sql)
    assert request.sql == sql
    with duckdb.connect(":memory:", config={"enable_external_access": False}) as connection:
        statements = connection.extract_statements(request.sql)
        assert len(statements) == 1 and statements[0].type == duckdb.StatementType.SELECT
        assert connection.execute(request.sql).fetchone()[0] == expected


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT $$safe$$; SELECT 2",
        "SELECT $tag$safe$tag$; ATTACH '/private' AS p",
        r"SELECT E'\'safe'; COPY (SELECT 1) TO '/private'",
        "SELECT 1 /* outer /* nested */ end */; SET enable_external_access=true",
        "SELECT 1 /* unfinished",
        "SELECT $$unfinished",
    ],
)
def test_rq_r04_lexical_mask_never_hides_following_statements(sql: str) -> None:
    with pytest.raises(ValueError):
        QueryRequest(sql=sql)


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TYPE private_notes AS ENUM ('synthetic-bob-private-note')",
        "CREATE TYPE custom_id AS VARCHAR",
        "CREATE INDEX private_index ON daily_bar(ts_code)",
    ],
)
def test_rq_r05_extra_persistent_objects_rejected_by_guard_and_loader(
    tmp_path: Path, ddl: str
) -> None:
    snapshot, manifest, source, source_hash = _published(tmp_path)
    snapshot.path.chmod(0o600)
    with duckdb.connect(str(snapshot.path)) as connection:
        connection.execute(ddl)
        if "ENUM" in ddl:
            assert connection.execute(
                "SELECT unnest(enum_range(NULL::private_notes))"
            ).fetchone() == ("synthetic-bob-private-note",)
        with pytest.raises(ValueError, match="额外对象"):
            verify_public_schema(connection)
    payload = manifest.model_dump(mode="json")
    payload["file_sha256"] = hashlib.sha256(snapshot.path.read_bytes()).hexdigest()
    payload["filename"] = f"query-{payload['file_sha256']}.duckdb"
    snapshot.path.rename(snapshot.root / payload["filename"])
    os.chmod(snapshot.root / payload["filename"], 0o400)
    snapshot.manifest_path.chmod(0o600)
    snapshot.manifest_path.write_text(json.dumps(payload))
    snapshot.manifest_path.chmod(0o400)
    with pytest.raises(ValueError, match="额外对象"):
        VerifiedQuerySnapshot(snapshot.root)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
