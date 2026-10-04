"""Bounded child lifecycle, capacity, deadline and scratch ownership."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import suppress
from pathlib import Path

from pydantic import Field

from .contracts import (
    MAX_RESULT_BYTES,
    MAX_ROWS,
    MAX_SECONDS,
    QueryModel,
    QueryRequest,
    QueryResult,
)
from .snapshot import VerifiedQuerySnapshot


class QueryLimits(QueryModel):
    address_bytes: int = Field(default=2**31, ge=128 * 2**20, le=2**31)
    file_bytes: int = Field(default=512 * 2**20, ge=1024, le=512 * 2**20)
    memory_bytes: int = Field(default=512 * 2**20, ge=1024 * 1024, le=512 * 2**20)
    spill_bytes: int = Field(default=0, ge=0, le=0)
    seconds: float = Field(default=MAX_SECONDS, gt=0, le=MAX_SECONDS)
    rows: int = Field(default=MAX_ROWS, ge=1, le=MAX_ROWS)
    result_bytes: int = Field(default=MAX_RESULT_BYTES, ge=2048, le=MAX_RESULT_BYTES)


class QueryExecutor:
    def __init__(
        self,
        snapshot: VerifiedQuerySnapshot,
        scratch_root: Path,
        *,
        limits: QueryLimits | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.limits = limits or QueryLimits()
        self.scratch_root = Path(scratch_root)
        self.scratch_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.scratch_root.lstat()
        if (
            self.scratch_root != self.scratch_root.resolve()
            or stat.S_IMODE(info.st_mode) != 0o700
            or info.st_uid != os.geteuid()
        ):
            raise ValueError("查询临时目录未通过核验")
        self._slots = threading.BoundedSemaphore(10)
        self._running = threading.BoundedSemaphore(2)
        self._lock = threading.Lock()
        self.active = 0
        self.last_pid: int | None = None

    def execute(self, query: QueryRequest, *, cancel: threading.Event | None = None) -> QueryResult:
        started = time.monotonic()
        if not self._slots.acquire(blocking=False):
            return QueryResult(status="busy", elapsed_ms=0, message="查询已满，请稍后重试。")
        acquired = False
        try:
            while not acquired:
                if (
                    cancel is not None and cancel.is_set()
                ) or time.monotonic() - started >= self.limits.seconds:
                    return QueryResult(
                        status="timeout",
                        elapsed_ms=(time.monotonic() - started) * 1000,
                        message="查询等待超时，请稍后重试。",
                    )
                acquired = self._running.acquire(timeout=min(0.05, self.limits.seconds))
            with self._lock:
                self.active += 1
            try:
                self.snapshot.verify_current()
            except Exception:
                return QueryResult(
                    status="unavailable",
                    elapsed_ms=(time.monotonic() - started) * 1000,
                    message="查询数据未通过核验，请更新后重试。",
                )
            with tempfile.TemporaryDirectory(prefix="query-", dir=self.scratch_root) as directory:
                scratch = Path(directory)
                request_path, result_path = scratch / "request.json", scratch / "result.json"
                payload = self.limits.model_dump()
                payload.update(
                    query=query.model_dump(mode="json"),
                    src_root=str(Path(__file__).resolve().parents[2]),
                    snapshot_path=str(self.snapshot.path),
                    snapshot_identity=self.snapshot.identity,
                    source_at=self.snapshot.manifest.source_at.isoformat(),
                    snapshot_sha256=self.snapshot.manifest.file_sha256,
                )
                request_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                os.chmod(request_path, 0o600)
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-I",
                        "-B",
                        str(Path(__file__).with_name("child.py")),
                        str(request_path),
                        str(result_path),
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    close_fds=True,
                    env={
                        "LANG": "C.UTF-8",
                        "LC_ALL": "C.UTF-8",
                        "TZ": "Asia/Shanghai",
                        "OPENBLAS_NUM_THREADS": "1",
                        "OMP_NUM_THREADS": "1",
                        "MKL_NUM_THREADS": "1",
                    },
                )
                self.last_pid = process.pid
                try:
                    deadline = time.monotonic() + self.limits.seconds
                    while process.poll() is None:
                        if time.monotonic() >= deadline or (cancel is not None and cancel.is_set()):
                            process.terminate()
                            try:
                                process.wait(timeout=0.2)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait()
                            return QueryResult(
                                status="timeout",
                                elapsed_ms=(time.monotonic() - started) * 1000,
                                message="查询已停止，请缩小范围后重试。",
                            )
                        with suppress(subprocess.TimeoutExpired):
                            process.wait(timeout=0.05)
                    process.wait()
                    if (
                        process.returncode != 0
                        or not result_path.is_file()
                        or result_path.stat().st_size > self.limits.result_bytes
                    ):
                        return QueryResult(
                            status="failed",
                            elapsed_ms=(time.monotonic() - started) * 1000,
                            message="查询超过资源限制或未能执行。",
                        )
                    result = QueryResult.model_validate_json(result_path.read_bytes())
                    self.snapshot.verify_current()
                    return result.model_copy(
                        update={"elapsed_ms": (time.monotonic() - started) * 1000}
                    )
                except Exception:
                    return QueryResult(
                        status="unavailable",
                        elapsed_ms=(time.monotonic() - started) * 1000,
                        message="查询结果暂时无法核验，请重试。",
                    )
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.wait()
        finally:
            if acquired:
                with self._lock:
                    self.active -= 1
                self._running.release()
            self._slots.release()
