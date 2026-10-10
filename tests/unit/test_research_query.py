"""RQ-02/03/05/06: public snapshot and bounded SQL contracts."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from datetime import UTC, datetime
from importlib.util import find_spec
from pathlib import Path

import duckdb
import pytest

NOW = datetime(2026, 10, 5, tzinfo=UTC)


def _api():
    assert find_spec("rquant.research_query") is not None, "research query is not implemented"
    return importlib.import_module("rquant.research_query")


def _source(path: Path) -> str:
    from rquant.storage.schema import ADJ_FACTOR_DDL, DAILY_BAR_DDL, TRADE_CALENDAR_DDL

    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path)) as connection:
        for ddl in (DAILY_BAR_DDL, ADJ_FACTOR_DDL, TRADE_CALENDAR_DDL):
            connection.execute(ddl)
        connection.execute(
            "INSERT INTO daily_bar(ts_code,trade_date,close) VALUES ('600001.SH','2026-09-30',12.5)"
        )
        connection.execute("INSERT INTO adj_factor VALUES ('600001.SH','2026-09-30',1.2)")
        connection.execute(
            "INSERT INTO trade_calendar VALUES ('SSE','2026-09-30',true,'2026-09-29','test',now())"
        )
        connection.execute("CREATE TABLE manual_watchlist(owner_id VARCHAR, secret VARCHAR)")
        connection.execute(
            "INSERT INTO manual_watchlist VALUES ('alice','private-a'),('bob','private-b')"
        )
        connection.execute("CREATE TABLE notification_log(owner_id VARCHAR,secret VARCHAR)")
        connection.execute(
            "INSERT INTO notification_log VALUES ('alice','notice-a'),('bob','notice-b')"
        )
    os.chmod(path, 0o400)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _published(tmp_path: Path):
    api = _api()
    source = tmp_path / "original.duckdb"
    digest = _source(source)
    manifest = api.build_query_snapshot(
        source, tmp_path / "public", source_sha256=digest, source_at=NOW
    )
    return api.VerifiedQuerySnapshot(tmp_path / "public"), manifest, source, digest


def test_public_snapshot_contains_only_exact_public_columns_and_source_is_unchanged(
    tmp_path: Path,
) -> None:
    api = _api()
    snapshot, manifest, source, digest = _published(tmp_path)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
    assert manifest.source_sha256 == digest
    info = source.stat()
    assert manifest.source_identity == (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
    assert snapshot.manifest == manifest
    with duckdb.connect(
        str(snapshot.path), read_only=True, config={"enable_external_access": False}
    ) as connection:
        assert connection.execute("SHOW TABLES").fetchall() == [
            ("adj_factor",),
            ("daily_bar",),
            ("trade_calendar",),
        ]
        for table, columns in api.PUBLIC_SCHEMA.items():
            assert connection.execute(
                "SELECT column_name,data_type FROM information_schema.columns "
                "WHERE table_name=? ORDER BY ordinal_position",
                [table],
            ).fetchall() == list(columns)
        for sql in (
            "SELECT * FROM main.manual_watchlist",
            "SELECT * FROM main.notification_log",
            "WITH w AS (SELECT * FROM manual_watchlist) SELECT * FROM w",
            "SELECT * FROM query_table('manual_watchlist')",
        ):
            with pytest.raises(duckdb.Error):
                connection.execute(sql)


def test_full_original_database_and_extra_objects_are_rejected(tmp_path: Path) -> None:
    api = _api()
    source = tmp_path / "original.duckdb"
    _source(source)
    with pytest.raises(ValueError):
        api.VerifiedQuerySnapshot(tmp_path)
    snapshot, manifest, _, _ = _published(tmp_path / "published")
    os.chmod(snapshot.path, 0o600)
    with duckdb.connect(str(snapshot.path)) as connection:
        connection.execute("CREATE VIEW leak AS SELECT 1")
    with pytest.raises(ValueError):
        snapshot.verify_current()
    payload = manifest.model_dump(mode="json")
    payload["file_sha256"] = hashlib.sha256(snapshot.path.read_bytes()).hexdigest()
    payload["filename"] = f"query-{payload['file_sha256']}.duckdb"
    snapshot.path.rename(snapshot.root / payload["filename"])
    os.chmod(snapshot.root / payload["filename"], 0o400)
    os.chmod(snapshot.manifest_path, 0o600)
    snapshot.manifest_path.write_text(json.dumps(payload))
    os.chmod(snapshot.manifest_path, 0o400)
    with pytest.raises(ValueError):
        api.VerifiedQuerySnapshot(snapshot.root)


def test_snapshot_bad_digest_symlink_and_source_change_fail_closed(tmp_path: Path) -> None:
    api = _api()
    source = tmp_path / "original.duckdb"
    digest = _source(source)
    with pytest.raises(ValueError):
        api.build_query_snapshot(source, tmp_path / "bad", source_sha256="0" * 64, source_at=NOW)
    link = tmp_path / "link.duckdb"
    link.symlink_to(source)
    with pytest.raises(ValueError):
        api.build_query_snapshot(link, tmp_path / "linked", source_sha256=digest, source_at=NOW)


def test_snapshot_replaced_with_identical_bytes_is_rejected(tmp_path: Path) -> None:
    snapshot, _, _, _ = _published(tmp_path)
    replacement = snapshot.root / "replacement.duckdb"
    replacement.write_bytes(snapshot.path.read_bytes())
    replacement.chmod(0o400)
    os.replace(replacement, snapshot.path)
    with pytest.raises(ValueError, match="发生变化"):
        snapshot.verify_current()


def test_original_changes_during_copy_never_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _api()
    module = importlib.import_module("rquant.research_query.snapshot")
    source = tmp_path / "original.duckdb"
    digest = _source(source)
    original_verify = module.verify_public_schema

    def mutate_after_copy(connection: object) -> None:
        original_verify(connection)
        source.chmod(0o600)
        source.write_bytes(source.read_bytes() + b"changed-source")
        source.chmod(0o400)

    monkeypatch.setattr(module, "verify_public_schema", mutate_after_copy)
    published = tmp_path / "public"
    with pytest.raises(ValueError, match="发布期间发生变化"):
        api.build_query_snapshot(source, published, source_sha256=digest, source_at=NOW)
    assert list(published.iterdir()) == []


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO daily_bar VALUES (1)",
        "WITH x AS (DELETE FROM daily_bar RETURNING *) SELECT * FROM x",
        "SELECT 1; SELECT 2",
        "ATTACH '/tmp/private' AS p",
        "COPY (SELECT 1) TO '/tmp/out'",
        "SELECT * FROM read_csv('/tmp/private')",
        "SELECT * FROM read_parquet('/tmp/private')",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SET enable_external_access=true",
        "PRAGMA database_list",
    ],
)
def test_forbidden_sql_is_rejected_before_execution(sql: str) -> None:
    with pytest.raises(ValueError):
        _api().QueryRequest(sql=sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'INSERT; COPY; read_csv' AS text;",
        "-- DROP\nWITH x AS (SELECT 1) SELECT * FROM x",
        "SELECT 1 /* ATTACH */",
        'SELECT 1 AS "UPDATE"',
    ],
)
def test_quoted_content_and_comments_do_not_change_actual_select(sql: str) -> None:
    assert _api().QueryRequest(sql=sql).sql == sql


def test_sql_byte_budget_and_client_cannot_raise_limits() -> None:
    api = _api()
    with pytest.raises(ValueError):
        api.QueryRequest(sql="SELECT '" + "量" * 11000 + "'")
    with pytest.raises(ValueError):
        api.QueryRequest(sql="SELECT 1", max_rows=100001)


def test_csv_formula_prefixes_quotes_binary_nonfinite_and_duplicates() -> None:
    api = _api()
    result = api.QueryResult(
        status="ready",
        columns=(
            api.QueryColumn(name="same", data_type="VARCHAR"),
            api.QueryColumn(name="same", data_type="VARCHAR"),
        ),
        rows=(("=1+1", 'a,"b\n'), (" \t@sum(1)", "<script>")),
        elapsed_ms=1,
        source_at=NOW,
        snapshot_sha256="a" * 64,
    )
    csv = api.result_csv(result)
    assert "'=1+1" in csv
    assert "' \t@sum(1)" in csv
    assert '"a,""b\n"' in csv
    assert "<script>" in csv
