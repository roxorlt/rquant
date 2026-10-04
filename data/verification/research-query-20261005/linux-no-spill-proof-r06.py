"""Synthetic Linux proof for the final query backend; no credentials or production IO."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

source_root = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(source_root))

import duckdb

from rquant.research_query import (
    PUBLIC_SCHEMA,
    QueryExecutor,
    QueryLimits,
    QueryRequest,
    VerifiedQuerySnapshot,
    build_query_snapshot,
)

record: dict[str, object] = {
    "kind": "research_query_no_spill_and_capacity_r06",
    "production_write": False,
    "provider_http": 0,
    "python": sys.version,
    "duckdb": duckdb.__version__,
    "cases": [],
    "spill_samples": 0,
    "peak_spill_logical_bytes": 0,
    "peak_spill_allocated_bytes": 0,
}
processes: list[subprocess.Popen[bytes]] = []
process_lock = threading.Lock()
real_popen = subprocess.Popen


def track_popen(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
    process = real_popen(*args, **kwargs)
    with process_lock:
        processes.append(process)
    return process


def fd_map() -> dict[str, str]:
    result = {}
    for path in Path("/proc/self/fd").iterdir():
        try:
            result[path.name] = os.readlink(path)
        except FileNotFoundError:
            pass
    return result


def reaped(process: subprocess.Popen[bytes]) -> bool:
    if process.poll() is None:
        return False
    try:
        os.waitpid(process.pid, os.WNOHANG)
    except ChildProcessError:
        return True
    return False


temporary_root: Path | None = None
stop = threading.Event()
sampler: threading.Thread | None = None
started = time.monotonic()
try:
    assert sys.platform == "linux", "Linux hard resource enforcement is required"
    record["fd_before_initialization"] = fd_map()
    with tempfile.TemporaryDirectory(prefix="rq-zero-spill-") as directory:
        temporary_root = Path(directory)
        original = temporary_root / "source.duckdb"
        with duckdb.connect(str(original), config={"threads": 1}) as connection:
            for table, columns in PUBLIC_SCHEMA.items():
                connection.execute(
                    "CREATE TABLE " + table + " ("
                    + ",".join(name + " " + kind for name, kind in columns) + ")"
                )
            connection.execute(
                "INSERT INTO daily_bar(ts_code,trade_date,close) "
                "VALUES ('600001.SH','2026-09-30',12.5)"
            )
            connection.execute("CREATE TABLE manual_watchlist(owner_id VARCHAR,secret VARCHAR)")
            connection.execute("INSERT INTO manual_watchlist VALUES ('alice','private-a')")
        original.chmod(0o400)
        source_hash = hashlib.sha256(original.read_bytes()).hexdigest()
        manifest = build_query_snapshot(
            original,
            temporary_root / "public",
            source_sha256=source_hash,
            source_at=datetime(2026, 10, 5, tzinfo=UTC),
        )
        snapshot = VerifiedQuerySnapshot(temporary_root / "public")
        record["manifest_source_identity"] = manifest.source_identity
        actual_stat = original.stat()
        assert manifest.source_identity == (
            actual_stat.st_dev, actual_stat.st_ino, actual_stat.st_size, actual_stat.st_mtime_ns
        )
        scratch = temporary_root / "scratch"
        executor = QueryExecutor(snapshot, scratch)
        settings_request = QueryRequest(
            sql="SELECT current_setting('temp_directory'), "
            "current_setting('max_temp_directory_size'), current_setting('lock_configuration'), "
            "current_setting('memory_limit'), current_setting('threads'), "
            "current_setting('enable_external_access')"
        )
        record["fd_initialized_baseline"] = fd_map()
        subprocess.Popen = track_popen

        def sample() -> None:
            while not stop.wait(0.002):
                logical = allocated = 0
                files = []
                for path in scratch.rglob("*"):
                    if not path.name.startswith("duckdb_temp"):
                        continue
                    try:
                        info = path.stat()
                        if path.is_file():
                            logical += info.st_size
                            allocated += info.st_blocks * 512
                            files.append(str(path.relative_to(scratch)))
                    except FileNotFoundError:
                        continue
                record["spill_samples"] += 1
                record["peak_spill_logical_bytes"] = max(
                    record["peak_spill_logical_bytes"], logical
                )
                record["peak_spill_allocated_bytes"] = max(
                    record["peak_spill_allocated_bytes"], allocated
                )
                if files:
                    record["unexpected_spill_files"] = files

        sampler = threading.Thread(target=sample)
        sampler.start()
        configuration = executor.execute(settings_request)
        record["cases"].append({
            "name": "actual_locked_configuration",
            "result": configuration.model_dump(mode="json"),
        })
        assert configuration.status == "ready"
        assert configuration.rows == (("", "0 bytes", True, "512.0 MiB", 1, False),)

        normal = executor.execute(QueryRequest(sql="SELECT ts_code,close FROM daily_bar"))
        record["cases"].append({"name": "normal_public_query", "result": normal.model_dump(mode="json")})
        assert normal.status == "ready" and normal.rows == (("600001.SH", 12.5),)

        small = QueryExecutor(snapshot, scratch, limits=QueryLimits(memory_bytes=64 * 2**20))
        before_sort = len(processes)
        large = small.execute(QueryRequest(
            sql="SELECT i,repeat(md5(i::VARCHAR),10) AS s FROM range(2000000) t(i) ORDER BY s"
        ))
        record["cases"].append({
            "name": "actual_large_sort_no_disk",
            "limits": small.limits.model_dump(),
            "result": large.model_dump(mode="json"),
            "child_reaped": all(reaped(item) for item in processes[before_sort:]),
            "active": small.active,
            "scratch_empty": list(scratch.iterdir()) == [],
        })
        assert large.status == "failed"
        assert large.message == "查询超过可用内存，请缩小日期、股票或字段范围。"
        assert small.active == 0 and list(scratch.iterdir()) == []
        assert all(reaped(item) for item in processes[before_sort:])

        capacity = QueryExecutor(snapshot, scratch, limits=QueryLimits(seconds=6))
        cancel = threading.Event()
        slow = QueryRequest(
            sql="SELECT sum(a.i*b.i) FROM range(100000000) a(i),range(100000000) b(i)"
        )
        before_capacity = len(processes)
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(capacity.execute, slow, cancel=cancel) for _ in range(10)]
            try:
                deadline = time.monotonic() + 4
                while not (
                    capacity.active == 2
                    and capacity._slots._value == 0
                    and len(processes) - before_capacity == 2
                ):
                    assert time.monotonic() < deadline, "did not reach two running/eight waiting"
                    time.sleep(0.005)
                live_children = sum(item.poll() is None for item in processes[before_capacity:])
                rejected = capacity.execute(QueryRequest(sql="SELECT 1"))
                record["capacity_at_full"] = {
                    "active": capacity.active,
                    "admitted": 10 - capacity._slots._value,
                    "waiting": 10 - capacity.active,
                    "live_children": live_children,
                    "eleventh_status": rejected.status,
                }
                assert live_children == 2 and rejected.status == "busy"
            finally:
                cancel.set()
            cancelled = [future.result(timeout=8).status for future in futures]
        record["capacity_cancelled_statuses"] = cancelled
        assert cancelled == ["timeout"] * 10
        assert capacity.active == 0 and list(scratch.iterdir()) == []
        assert all(reaped(item) for item in processes[before_capacity:])
        recovered = capacity.execute(QueryRequest(sql="SELECT 1"))
        record["cases"].append({"name": "capacity_recovered", "result": recovered.model_dump(mode="json")})
        assert recovered.status == "ready" and recovered.rows == ((1,),)

        stop.set()
        sampler.join()
        record["fd_after_requests"] = fd_map()
        assert record["fd_initialized_baseline"] == record["fd_after_requests"]
        assert record["spill_samples"] > 0
        assert record["peak_spill_logical_bytes"] == 0
        assert record["peak_spill_allocated_bytes"] == 0
        record["source_sha256_after"] = hashlib.sha256(original.read_bytes()).hexdigest()
        record["snapshot_sha256_after"] = hashlib.sha256(snapshot.path.read_bytes()).hexdigest()
        record["source_unchanged"] = record["source_sha256_after"] == source_hash
        record["snapshot_unchanged"] = record["snapshot_sha256_after"] == manifest.file_sha256
        record["scratch_empty"] = list(scratch.iterdir()) == []
        assert record["source_unchanged"] and record["snapshot_unchanged"] and record["scratch_empty"]
        record["status"] = "passed"
finally:
    stop.set()
    if sampler is not None:
        sampler.join()
    subprocess.Popen = real_popen
    for process in processes:
        if process.poll() is None:
            process.kill()
        process.wait()
    record["child_pids"] = [item.pid for item in processes]
    record["children_reaped"] = all(reaped(item) for item in processes)
    record["private_directory_removed"] = temporary_root is not None and not temporary_root.exists()
    record["elapsed_seconds"] = time.monotonic() - started
    print(json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=True), flush=True)
