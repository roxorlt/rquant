"""RQ-02..06: real child/resource tests; unsupported kernels fail closed."""

from __future__ import annotations

import importlib
import json
import os
import stat
import sys
from pathlib import Path

import pytest

from tests.unit.test_research_query import _published


def _api():
    module = importlib.import_module("rquant.research_query")
    assert hasattr(module, "QueryExecutor"), "restricted child executor is not implemented"
    return module


def test_unsupported_kernel_closes_query_and_leaves_no_scratch(tmp_path: Path) -> None:
    api = _api()
    snapshot, _, _, _ = _published(tmp_path)
    executor = api.QueryExecutor(snapshot, tmp_path / "scratch")
    result = executor.execute(api.QueryRequest(sql="SELECT 1"))
    if sys.platform == "linux":
        assert result.status == "ready"
    else:
        assert result.status == "unavailable"
    assert list((tmp_path / "scratch").iterdir()) == []
    assert executor.active == 0


def test_cannot_raise_resource_deadline_or_result_limits() -> None:
    api = _api()
    for kwargs in (
        {"address_bytes": 2**31 + 1},
        {"file_bytes": 512 * 2**20 + 1},
        {"memory_bytes": 512 * 2**20 + 1},
        {"spill_bytes": 512 * 2**20 + 1},
        {"seconds": 31},
        {"rows": 10001},
        {"result_bytes": 16 * 2**20 + 1},
    ):
        with pytest.raises(ValueError):
            api.QueryLimits(**kwargs)


def test_disk_spill_is_disabled_and_cannot_be_enabled() -> None:
    api = _api()
    assert api.QueryLimits().spill_bytes == 0
    for amount in (1, 1024, 64 * 2**20, 512 * 2**20):
        with pytest.raises(ValueError):
            api.QueryLimits(spill_bytes=amount)


def test_actual_duckdb_configuration_has_no_temporary_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _api()
    child = importlib.import_module("rquant.research_query.child")
    snapshot, _, _, _ = _published(tmp_path)
    request, result = tmp_path / "request.json", tmp_path / "result.json"
    payload = api.QueryLimits().model_dump()
    payload.update(
        query={
            "sql": "SELECT current_setting('temp_directory'), "
            "current_setting('max_temp_directory_size'), current_setting('lock_configuration')",
            "mode": "query",
        },
        src_root=str(Path(child.__file__).resolve().parents[2]),
        snapshot_path=str(snapshot.path),
        snapshot_identity=snapshot.identity,
        source_at=snapshot.manifest.source_at.isoformat(),
        snapshot_sha256=snapshot.manifest.file_sha256,
    )
    request.write_text(json.dumps(payload))
    monkeypatch.setattr(sys, "argv", ["child.py", str(request), str(result)])
    # Configuration regression only; Linux proof separately exercises real limits.
    monkeypatch.setattr(child, "enforce_limits", lambda *args: None)
    assert child.main() == 0
    actual = api.QueryResult.model_validate_json(result.read_bytes())
    assert actual.status == "ready"
    assert actual.rows == (("", "0 bytes", True),)
    assert not list(tmp_path.rglob("duckdb_temp*"))


@pytest.mark.parametrize("kind", ["python", "duckdb"])
def test_memory_failure_has_clear_scope_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    import duckdb

    api = _api()
    child = importlib.import_module("rquant.research_query.child")
    snapshot, _, _, _ = _published(tmp_path)
    request, result = tmp_path / "request.json", tmp_path / "result.json"
    payload = api.QueryLimits().model_dump()
    payload.update(
        query={"sql": "SELECT 1", "mode": "query"},
        src_root=str(Path(child.__file__).resolve().parents[2]),
        snapshot_path=str(snapshot.path),
        snapshot_identity=snapshot.identity,
        source_at=snapshot.manifest.source_at.isoformat(),
        snapshot_sha256=snapshot.manifest.file_sha256,
    )
    request.write_text(json.dumps(payload))
    monkeypatch.setattr(sys, "argv", ["child.py", str(request), str(result)])
    monkeypatch.setattr(child, "enforce_limits", lambda *args: None)

    def out_of_memory(*args: object, **kwargs: object) -> None:
        if kind == "python":
            raise MemoryError("synthetic conversion memory failure")
        raise duckdb.OutOfMemoryException("synthetic memory failure with a private path")

    monkeypatch.setattr(duckdb, "connect", out_of_memory)
    assert child.main() == 0
    actual = api.QueryResult.model_validate_json(result.read_bytes())
    assert actual.status == "failed"
    assert actual.message == "查询超过可用内存，请缩小日期、股票或字段范围。"


def test_hard_limit_setup_failure_never_imports_duckdb_and_result_stays_private(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    child = importlib.import_module("rquant.research_query.child")
    request, result = tmp_path / "request.json", tmp_path / "result.json"
    request.write_text(json.dumps({"address_bytes": 2**31, "file_bytes": 512 * 2**20}))
    monkeypatch.setattr(sys, "argv", ["child.py", str(request), str(result)])

    def failed(*args: object) -> None:
        raise OSError("synthetic setrlimit failure")

    monkeypatch.setattr(child, "enforce_limits", failed)
    import builtins

    original_import = builtins.__import__

    def checked_import(name: str, *args: object, **kwargs: object) -> object:
        assert name != "duckdb", "DuckDB was imported before resource limits"
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", checked_import)
    assert child.main() == 0
    assert json.loads(result.read_bytes())["status"] == "unavailable"
    assert stat.S_IMODE(result.stat().st_mode) == 0o600


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Linux RLIMIT_AS enforcement is required; macOS must fail closed",
)
def test_real_child_types_caps_actual_statement_and_source_privacy(tmp_path: Path) -> None:
    api = _api()
    snapshot, _, source, digest = _published(tmp_path)
    executor = api.QueryExecutor(
        snapshot, tmp_path / "scratch", limits=api.QueryLimits(rows=3, result_bytes=4096)
    )
    result = executor.execute(api.QueryRequest(sql="SELECT * FROM range(5)"))
    assert result.status == "partial" and len(result.rows) == 3
    assert executor.execute(api.QueryRequest(sql="SELECT repeat('x',5000)")).status == "partial"
    types = executor.execute(
        api.QueryRequest(
            sql="SELECT 1 AS n, 2 AS n, DATE '2026-09-30' AS d, "
            "1.25::DECIMAL(5,2) AS dec, '\\x41'::BLOB AS b, "
            "'NaN'::DOUBLE AS nf, true AS yes"
        )
    )
    assert types.status == "ready" and len(types.columns) == 7
    assert types.columns[0].name == types.columns[1].name
    assert types.rows[0][2].kind == "date"
    assert types.rows[0][3].kind == "decimal"
    assert types.rows[0][4].kind == "binary"
    assert types.rows[0][5].kind == "nonfinite"
    for sql in (
        "SELECT * FROM main.manual_watchlist",
        "SELECT * FROM query_table('manual_watchlist')",
        "SELECT * FROM query('SELECT * FROM manual_watchlist')",
        "SELECT * FROM glob('/etc/*')",
    ):
        assert executor.execute(api.QueryRequest(sql=sql)).status == "failed"
    assert (
        executor.execute(api.QueryRequest(sql="SELECT 'COPY; read_csv' AS text")).status == "ready"
    )
    assert executor.execute(api.QueryRequest(sql="SELECT 1", mode="explain")).status == "ready"
    import hashlib

    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
    assert list((tmp_path / "scratch").iterdir()) == []


@pytest.mark.skipif(sys.platform != "linux", reason="actual kernel enforcement requires Linux")
def test_actual_timeout_memory_failure_and_child_reaped(tmp_path: Path) -> None:
    api = _api()
    snapshot, _, _, _ = _published(tmp_path)
    executor = api.QueryExecutor(
        snapshot, tmp_path / "scratch", limits=api.QueryLimits(seconds=0.3)
    )
    result = executor.execute(
        api.QueryRequest(sql="SELECT sum(a.i*b.i) FROM range(10000000) a(i),range(10000000) b(i)")
    )
    assert result.status == "timeout"
    assert executor.active == 0 and list((tmp_path / "scratch").iterdir()) == []
    if executor.last_pid is not None:
        with pytest.raises(ChildProcessError):
            os.waitpid(executor.last_pid, os.WNOHANG)
    executor = api.QueryExecutor(snapshot, tmp_path / "scratch")
    result = executor.execute(api.QueryRequest(sql="SELECT repeat('x',2147483647)"))
    assert result.status == "failed"
    assert list((tmp_path / "scratch").iterdir()) == []
