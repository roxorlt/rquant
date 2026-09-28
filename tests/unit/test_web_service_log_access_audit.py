"""The service-log audit is durable before the protected read begins."""

from __future__ import annotations

import json
import multiprocessing
import os
import stat
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.web import service_log_access_audit as audit_module
from rquant.web.service_log_access_audit import ServiceLogAccessRecord


def _event() -> ServiceLogAccessRecord:
    return ServiceLogAccessRecord(
        operator="liutong",
        unit="rquant-daily.service",
        at=datetime(2026, 9, 28, 4, 0, tzinfo=UTC),
    )


def _record_in_process(directory: str, count: int) -> None:
    sink = audit_module.JsonlServiceLogAccessAudit(Path(directory))
    for _ in range(count):
        sink.record(_event())


def test_first_record_is_fsynced_before_return_and_contains_only_closed_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    synced: list[int] = []
    original_fsync = os.fsync

    def observed_fsync(descriptor: int) -> None:
        synced.append(stat.S_IFMT(os.fstat(descriptor).st_mode))
        original_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", observed_fsync)
    sink = audit_module.JsonlServiceLogAccessAudit(directory)
    sink.record(_event())

    path = directory / audit_module.AUDIT_FILE_NAME
    rows = path.read_bytes().splitlines()
    assert len(rows) == 1
    assert len(rows[0]) + 1 <= 512
    assert json.loads(rows[0]) == {
        "operator": "liutong",
        "unit": "rquant-daily.service",
        "result_class": "admitted",
        "at": "2026-09-28T04:00:00Z",
    }
    assert synced == [stat.S_IFREG, stat.S_IFDIR]
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_parallel_threads_and_processes_append_whole_lines(tmp_path: Path) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    sink = audit_module.JsonlServiceLogAccessAudit(directory)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _index: sink.record(_event()), range(20)))
    with ProcessPoolExecutor(
        max_workers=2, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        futures = [pool.submit(_record_in_process, str(directory), 15) for _ in range(2)]
        for future in futures:
            future.result(timeout=10)
    rows = (directory / audit_module.AUDIT_FILE_NAME).read_bytes().splitlines()
    assert len(rows) == 50
    assert all(json.loads(row) == json.loads(rows[0]) for row in rows)


def test_directory_must_be_existing_private_owned_nonsymlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(OSError):
        audit_module.JsonlServiceLogAccessAudit(missing)
    with pytest.raises(ValueError, match="absolute"):
        audit_module.JsonlServiceLogAccessAudit(Path("relative"))
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    alias = tmp_path / "alias"
    alias.symlink_to(directory, target_is_directory=True)
    with pytest.raises(OSError):
        audit_module.JsonlServiceLogAccessAudit(alias)
    directory.chmod(0o750)
    with pytest.raises(ValueError, match="unsafe"):
        audit_module.JsonlServiceLogAccessAudit(directory)
    directory.chmod(0o700)
    monkeypatch.setattr(os, "geteuid", lambda: os.stat(directory).st_uid + 1)
    with pytest.raises(ValueError, match="unsafe"):
        audit_module.JsonlServiceLogAccessAudit(directory)


@pytest.mark.parametrize("bad_file", ("symlink", "mode", "hardlink", "directory", "fifo"))
def test_existing_unsafe_audit_file_fails_closed(tmp_path: Path, bad_file: str) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    path = directory / audit_module.AUDIT_FILE_NAME
    if bad_file == "symlink":
        path.symlink_to(tmp_path / "target")
    elif bad_file == "directory":
        path.mkdir()
    elif bad_file == "fifo":
        os.mkfifo(path, 0o600)
    else:
        path.write_bytes(b"\n")
        if bad_file == "mode":
            path.chmod(0o644)
        else:
            os.link(path, tmp_path / "other")
    sink = audit_module.JsonlServiceLogAccessAudit(directory)
    with pytest.raises((OSError, ValueError)):
        sink.record(_event())


def test_replaced_directory_and_partial_tail_fail_closed(tmp_path: Path) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    sink = audit_module.JsonlServiceLogAccessAudit(directory)
    path = directory / audit_module.AUDIT_FILE_NAME
    path.write_bytes(b"partial")
    path.chmod(0o600)
    with pytest.raises(ValueError, match="incomplete"):
        sink.record(_event())
    path.unlink()
    directory.rename(tmp_path / "old-audit")
    directory.symlink_to(tmp_path / "old-audit", target_is_directory=True)
    with pytest.raises(OSError):
        sink.record(_event())


def test_capacity_is_enforced_without_appending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    sink = audit_module.JsonlServiceLogAccessAudit(directory)
    sink.record(_event())
    path = directory / audit_module.AUDIT_FILE_NAME
    previous = path.read_bytes()
    monkeypatch.setattr(audit_module, "_MAX_AUDIT_BYTES", len(previous))
    with pytest.raises(ValueError, match="capacity"):
        sink.record(_event())
    assert path.read_bytes() == previous


def test_fsync_failure_is_propagated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    sink = audit_module.JsonlServiceLogAccessAudit(directory)

    def fail_sync(_descriptor: int) -> None:
        raise OSError("storage unavailable")

    monkeypatch.setattr(os, "fsync", fail_sync)
    with pytest.raises(OSError, match="storage unavailable"):
        sink.record(_event())


def test_directory_sync_is_retried_after_first_creation_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    sink = audit_module.JsonlServiceLogAccessAudit(directory)
    original_fsync = os.fsync
    synced: list[int] = []

    def fail_first_directory_sync(descriptor: int) -> None:
        kind = stat.S_IFMT(os.fstat(descriptor).st_mode)
        synced.append(kind)
        if kind == stat.S_IFDIR and synced.count(stat.S_IFDIR) == 1:
            raise OSError("directory sync failed")
        original_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_first_directory_sync)
    with pytest.raises(OSError, match="directory sync failed"):
        sink.record(_event())
    sink.record(_event())
    assert synced == [stat.S_IFREG, stat.S_IFDIR, stat.S_IFREG, stat.S_IFDIR]
