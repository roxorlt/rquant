"""Standalone stdlib bootstrap: apply hard limits before importing DuckDB."""

from __future__ import annotations

import base64
import errno
import json
import math
import os
import signal
import sys
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path


def enforce_limits(address_bytes: int, file_bytes: int, scratch: Path) -> None:
    if sys.platform != "linux":
        raise RuntimeError("hard address-space enforcement is unavailable")
    import mmap
    import resource

    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_AS, (address_bytes, address_bytes))
    resource.setrlimit(resource.RLIMIT_FSIZE, (file_bytes, file_bytes))
    if resource.getrlimit(resource.RLIMIT_AS) != (
        address_bytes,
        address_bytes,
    ) or resource.getrlimit(resource.RLIMIT_FSIZE) != (file_bytes, file_bytes):
        raise RuntimeError("hard limits could not be installed")
    try:
        allocation = mmap.mmap(-1, address_bytes + mmap.PAGESIZE)
    except (OSError, MemoryError):
        pass
    else:
        allocation.close()
        raise RuntimeError("hard address-space limit was not enforced")
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    probe = scratch / "limit-probe"
    try:
        with probe.open("xb", buffering=0) as handle:
            handle.seek(file_bytes)
            try:
                handle.write(b"x")
                handle.flush()
            except OSError as exc:
                if exc.errno != errno.EFBIG:
                    raise
            else:
                raise RuntimeError("hard file limit was not enforced")
    finally:
        probe.unlink(missing_ok=True)


def scalar(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"kind": "nonfinite", "text": str(value)}
    if isinstance(value, Decimal):
        return {"kind": "decimal", "text": str(value)}
    if isinstance(value, (date, datetime)):
        return {"kind": "date", "text": value.isoformat()}
    if isinstance(value, bytes):
        return {"kind": "binary", "text": base64.b64encode(value).decode("ascii")}
    if isinstance(value, timedelta):
        return {"kind": "interval", "text": str(value)}
    return {
        "kind": "structured",
        "text": json.dumps(value, default=str, ensure_ascii=False, allow_nan=False),
    }


def encode(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
        "utf-8"
    )


def write_private_result(path: Path, payload: object) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(encode(payload))


def main() -> int:
    started = time.monotonic()
    request_path, result_path = map(Path, sys.argv[1:3])
    raw = request_path.read_bytes()
    if len(raw) > 256 * 1024:
        return 2
    request = json.loads(raw)
    result: dict[str, object] = {
        "status": "unavailable",
        "columns": [],
        "rows": [],
        "elapsed_ms": 0,
        "message": "当前环境无法限制查询资源，查询未启动。",
    }
    try:
        enforce_limits(request["address_bytes"], request["file_bytes"], result_path.parent)
    except Exception:
        write_private_result(result_path, result)
        return 0
    # No third-party import, source open, SQL parsing or conversion happens earlier.
    sys.path.insert(0, request["src_root"])
    connection = None
    memory_errors: tuple[type[Exception], ...] = (MemoryError,)
    try:
        import duckdb

        memory_errors += (duckdb.OutOfMemoryException,)
        from rquant.research_query.contracts import QueryRequest
        from rquant.research_query.snapshot import file_identity, verify_public_schema

        sql_request = QueryRequest.model_validate(request["query"])
        source = Path(request["snapshot_path"])
        expected = tuple(request["snapshot_identity"])
        if file_identity(source, readonly=True) != expected:
            raise ValueError("source changed")
        connection = duckdb.connect(
            str(source),
            read_only=True,
            config={
                "enable_external_access": False,
                "threads": 1,
                "memory_limit": f"{request['memory_bytes']}B",
                "max_temp_directory_size": "0B",
                "temp_directory": "",
                "autoload_known_extensions": False,
                "autoinstall_known_extensions": False,
            },
        )
        verify_public_schema(connection)
        connection.execute("SET lock_configuration=true")
        statements = connection.extract_statements(sql_request.sql)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise ValueError("actual statement is not SELECT")
        cursor = connection.execute(
            ("EXPLAIN " if sql_request.mode == "explain" else "") + sql_request.sql
        )
        columns = [{"name": str(item[0]), "data_type": str(item[1])} for item in cursor.description]
        result = {
            "status": "ready",
            "columns": columns,
            "rows": [],
            "elapsed_ms": 0,
            "source_at": request["source_at"],
            "snapshot_sha256": request["snapshot_sha256"],
            "message": "",
        }
        rows: list[list[object]] = []
        # Reserve full envelope overhead; actual serialized bytes are measured below.
        total = len(encode(result)) + 1024
        if total > request["result_bytes"]:
            raise ValueError("column metadata exceeds result budget")
        while True:
            batch = cursor.fetchmany(1)
            if not batch:
                break
            row = [scalar(value) for value in batch[0]]
            size = len(encode(row)) + 1
            if len(rows) == request["rows"] or total + size > request["result_bytes"]:
                result["status"] = "partial"
                result["message"] = "结果超过限制，仅显示可返回的部分。"
                break
            rows.append(row)
            total += size
        result["rows"] = rows
        connection.close()
        connection = None
        if file_identity(source, readonly=True) != expected:
            raise ValueError("source changed during query")
    except Exception as exc:
        result = {
            "status": "failed",
            "columns": [],
            "rows": [],
            "elapsed_ms": 0,
            "source_at": request["source_at"],
            "snapshot_sha256": request["snapshot_sha256"],
            "message": (
                "查询超过可用内存，请缩小日期、股票或字段范围。"
                if isinstance(exc, memory_errors)
                else "查询未能执行，请检查 SQL 或数据规模。"
            ),
        }
    finally:
        if connection is not None:
            connection.close()
    result["elapsed_ms"] = max(0, (time.monotonic() - started) * 1000)
    encoded = encode(result)
    if len(encoded) > request["result_bytes"]:
        return 3
    descriptor = os.open(result_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
