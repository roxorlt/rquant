"""The standalone runner consumes only previously submitted read-only plan jobs."""

from __future__ import annotations

import json
import select
import signal
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from rquant.backfill_plan_jobs import BackfillPlanJobStore
from tests.unit.test_backfill_plan_artifact import _snapshot
from tests.unit.test_backfill_plan_jobs import _request


def _run(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.backfill_plan_runner",
            "--state-path",
            str(tmp_path / "jobs.sqlite"),
            "--plan-directory",
            str(tmp_path / "plans"),
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


def test_once_processes_previously_queued_plan(tmp_path: Path) -> None:
    snapshot = _snapshot(tmp_path)
    store = BackfillPlanJobStore(
        state_path=tmp_path / "jobs.sqlite",
        plan_directory=tmp_path / "plans",
        clock=lambda: datetime(2026, 2, 6, 2, tzinfo=UTC),
    )
    queued = store.submit(_request(snapshot))

    result = _run(tmp_path, "--once")

    assert result.returncode == 0, result.stderr
    assert store.status(queued.task_id).status == "succeeded"
    assert "succeeded" in result.stdout


def test_once_empty_queue_exits_without_creating_plan(tmp_path: Path) -> None:
    result = _run(tmp_path, "--once")

    assert result.returncode == 0
    assert json.loads(result.stdout) == {"status": "idle"}
    assert not (tmp_path / "plans").exists()


def test_once_failed_job_exits_nonzero_with_durable_failure(tmp_path: Path) -> None:
    snapshot = tmp_path / "empty.duckdb"
    with duckdb.connect(str(snapshot)):
        pass
    store = BackfillPlanJobStore(
        state_path=tmp_path / "jobs.sqlite",
        plan_directory=tmp_path / "plans",
    )
    queued = store.submit(_request(snapshot))

    result = _run(tmp_path, "--once")

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "task_id": queued.task_id,
        "status": "failed",
        "error_code": "invalid_evidence",
    }
    assert store.status(queued.task_id).status == "failed"


def test_poll_consumes_new_job_and_stops_cleanly_on_sigterm(tmp_path: Path) -> None:
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "rquant.backfill_plan_runner",
            "--state-path",
            str(tmp_path / "jobs.sqlite"),
            "--plan-directory",
            str(tmp_path / "plans"),
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
        assert ready, "runner did not report an idle poll"
        assert json.loads(process.stdout.readline()) == {"status": "idle"}

        snapshot = _snapshot(tmp_path)
        store = BackfillPlanJobStore(
            state_path=tmp_path / "jobs.sqlite",
            plan_directory=tmp_path / "plans",
        )
        queued = store.submit(_request(snapshot))
        ready, _, _ = select.select([process.stdout], [], [], 10)
        assert ready, "runner did not poll a newly queued job"
        assert json.loads(process.stdout.readline()) == {
            "task_id": queued.task_id,
            "status": "succeeded",
            "error_code": None,
        }
        assert store.status(queued.task_id).status == "succeeded"

        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, stderr
        assert json.loads(stdout.strip()) == {"status": "stopped"}
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)


def test_once_rejects_explicit_poll_interval(tmp_path: Path) -> None:
    result = _run(tmp_path, "--once", "--poll-interval", "5")

    assert result.returncode == 2
    assert "--poll-interval requires --poll" in result.stderr
    assert not (tmp_path / "jobs.sqlite").exists()


def test_runner_rejects_primary_duckdb_as_job_state(tmp_path: Path) -> None:
    primary = _snapshot(tmp_path)
    before = primary.read_bytes()
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.backfill_plan_runner",
            "--state-path",
            str(primary),
            "--plan-directory",
            str(tmp_path / "plans"),
            "--once",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        env={
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "RQUANT_DISABLE_DOTENV": "1",
        },
    )

    assert result.returncode == 2
    assert "job state" in result.stderr
    assert primary.read_bytes() == before


def test_poll_does_not_repeat_idle_output_each_interval(tmp_path: Path) -> None:
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "rquant.backfill_plan_runner",
            "--state-path",
            str(tmp_path / "jobs.sqlite"),
            "--plan-directory",
            str(tmp_path / "plans"),
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
        assert ready
        assert json.loads(process.stdout.readline()) == {"status": "idle"}
        ready, _, _ = select.select([process.stdout], [], [], 2.2)
        assert not ready, "idle polling should not log on every interval"
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        process.communicate(timeout=10)
