"""The standalone audit runner consumes only previously admitted read-only jobs."""

from __future__ import annotations

import json
import os
import select
import shutil
import signal
import subprocess
import sys
from pathlib import Path

import duckdb

from rquant.data_audit_report_jobs import DataAuditReportJobStore
from tests.unit.test_data_audit_report import _database
from tests.unit.test_data_audit_report_jobs import _request


def _run(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.data_audit_report_runner",
            "--state-path",
            str(tmp_path / "audit-jobs.sqlite"),
            "--report-directory",
            str(tmp_path / "reports"),
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env={
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "RQUANT_DISABLE_DOTENV": "1",
        },
    )


def _sources(tmp_path: Path) -> tuple[Path, Path]:
    primary = _database(tmp_path / "primary.duckdb")
    replica = tmp_path / "replica.duckdb"
    shutil.copyfile(primary, replica)
    return primary, replica


def _store(tmp_path: Path) -> DataAuditReportJobStore:
    return DataAuditReportJobStore(
        state_path=tmp_path / "audit-jobs.sqlite",
        report_directory=tmp_path / "reports",
    )


def test_once_processes_previously_queued_audit_report(tmp_path: Path) -> None:
    primary, replica = _sources(tmp_path)
    store = _store(tmp_path)
    queued = store.submit(_request(primary, replica))

    result = _run(tmp_path, "--once")

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "task_id": queued.task_id,
        "status": "succeeded",
        "error_code": None,
    }
    assert store.status(queued.task_id).status == "succeeded"


def test_once_idle_exits_without_report_directory(tmp_path: Path) -> None:
    result = _run(tmp_path, "--once")

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"status": "idle"}
    assert not (tmp_path / "reports").exists()


def test_once_failure_exits_nonzero_and_records_safe_error(tmp_path: Path) -> None:
    primary = tmp_path / "empty.duckdb"
    with duckdb.connect(str(primary)):
        pass
    replica = tmp_path / "replica.duckdb"
    shutil.copyfile(primary, replica)
    store = _store(tmp_path)
    queued = store.submit(_request(primary, replica))

    result = _run(tmp_path, "--once")

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "task_id": queued.task_id,
        "status": "failed",
        "error_code": "invalid_evidence",
    }
    assert store.status(queued.task_id).status == "failed"


def test_poll_stops_on_sigterm_after_reporting_idle_once(tmp_path: Path) -> None:
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "rquant.data_audit_report_runner",
            "--state-path",
            str(tmp_path / "audit-jobs.sqlite"),
            "--report-directory",
            str(tmp_path / "reports"),
            "--poll",
            "--poll-interval",
            "1",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "RQUANT_DISABLE_DOTENV": "1",
        },
    )
    try:
        assert process.stdout is not None
        ready, _, _ = select.select([process.stdout], [], [], 10)
        assert ready, "audit runner did not report its idle poll"
        assert json.loads(process.stdout.readline()) == {"status": "idle"}
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, stderr
        assert json.loads(stdout.strip()) == {"status": "stopped"}
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)


def test_runner_rejects_noncanonical_or_source_alias_state(tmp_path: Path) -> None:
    primary, _replica = _sources(tmp_path)
    before = primary.read_bytes()
    alias = tmp_path / "audit-jobs.sqlite"
    os.link(primary, alias)
    result = _run(tmp_path, "--once")
    assert result.returncode == 2
    assert "state" in result.stderr
    assert primary.read_bytes() == before

    alias.unlink()
    alias.symlink_to(primary)
    assert _run(tmp_path, "--once").returncode == 2
    alias.unlink()
    assert _run(tmp_path / "x" / "..", "--once").returncode == 2


def test_once_rejects_poll_interval(tmp_path: Path) -> None:
    result = _run(tmp_path, "--once", "--poll-interval", "5")
    assert result.returncode == 2
    assert "--poll-interval requires --poll" in result.stderr
    assert not (tmp_path / "audit-jobs.sqlite").exists()
