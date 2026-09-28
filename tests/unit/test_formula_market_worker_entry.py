"""A private local worker command completes only jobs bound to its configured sources."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.page_control_service as page_control_service
from rquant.formula_market_job_projection import (
    read_formula_market_job_snapshot,
    read_formula_market_result,
)
from rquant.formula_market_page_backend import (
    FormulaMarketPageBackendConfig,
    load_private_formula_market_config,
)
from rquant.page_control import PageControlStatus
from rquant.screen.formula_market_jobs import FormulaMarketJobStore
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_formula_market_jobs import _request
from tests.unit.test_formula_market_run import _history, _market
from tests.unit.test_formula_market_service_entry import (
    COMMIT,
    _entry_profile,
    _isolate_entrypoint,
    _private_config,
)


def _worker(config_path: Path | None) -> subprocess.CompletedProcess[str]:
    command = [sys.executable, "-m", "rquant.formula_market_worker_entry"]
    if config_path is not None:
        command.extend(("--config", str(config_path)))
    environment = os.environ.copy()
    environment["RQUANT_DISABLE_DOTENV"] = "1"
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2] / "src")
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        env=environment,
        timeout=30,
        check=False,
    )


def _write_config(path: Path, config: FormulaMarketPageBackendConfig) -> None:
    path.write_bytes(canonical_json_bytes(config.model_dump(mode="json")))
    os.chmod(path, 0o600)


def test_real_page_entry_to_one_shot_worker_to_serving_verified_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = _entry_profile(tmp_path)
    config_path = _private_config(tmp_path)
    observed = _isolate_entrypoint(monkeypatch, command_id="formula-worker-entry")
    page_control_service.main(
        argv=[
            "--manifest",
            str(tmp_path / "wrapper-manifest.json"),
            "--control-root",
            str(tmp_path / "control"),
            "--expected-commit",
            COMMIT,
            "--expected-generation",
            "b" * 64,
            "--formula-market-config",
            str(config_path),
        ],
        runtime_root=runtime_root,
    )
    queued = next(
        item for item in observed if getattr(item, "command_id", None) == "formula-worker-entry"
    )
    assert queued.status is PageControlStatus.SUCCEEDED
    assert queued.result is not None and queued.result["outcome"] == "task_queued"
    task_id = queued.result["task_id"]

    first = _worker(config_path)
    assert first.returncode == 0, first.stderr
    assert json.loads(first.stdout) == {"status": "succeeded", "task_id": task_id}
    assert "CLOSE>2" not in first.stdout
    assert str(tmp_path) not in first.stdout

    config = load_private_formula_market_config(config_path)
    snapshot = read_formula_market_job_snapshot(
        config.state_path,
        config.artifact_directory,
        observed_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    assert len(snapshot.jobs) == len(snapshot.artifacts) == 1
    assert snapshot.jobs[0].task_id == task_id
    assert snapshot.jobs[0].status == "succeeded"
    result = read_formula_market_result(config.artifact_directory, snapshot.artifacts[0])
    assert result.summary.match_codes == ("000001.SZ", "830001.BJ")
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in config.artifact_directory.iterdir())

    second = _worker(config_path)
    assert second.returncode == 0, second.stderr
    assert json.loads(second.stdout) == {"status": "idle"}
    assert len(tuple(config.artifact_directory.iterdir())) == 1


@pytest.mark.parametrize("fault", ("missing", "loose", "symlink", "noncanonical", "relative-root"))
def test_bad_config_is_rejected_before_task_state_opens(tmp_path: Path, fault: str) -> None:
    config_path = _private_config(tmp_path)
    config = load_private_formula_market_config(config_path)
    assert not config.state_path.exists()
    selected: Path | None = config_path
    if fault == "missing":
        selected = None
    elif fault == "loose":
        os.chmod(config_path, 0o644)
    elif fault == "symlink":
        selected = tmp_path / "linked-config.json"
        selected.symlink_to(config_path)
    elif fault == "noncanonical":
        config_path.write_bytes(config_path.read_bytes() + b"\n")
    elif fault == "relative-root":
        values = json.loads(config_path.read_bytes())
        values["universe_root"] = "relative/market"
        config_path.write_bytes(canonical_json_bytes(values))

    completed = _worker(selected)
    assert completed.returncode != 0
    assert completed.stdout == ""
    if fault == "missing":
        assert "--config" in completed.stderr
    else:
        assert completed.stderr == "worker_config_invalid\n"
    assert not config.state_path.exists()


def test_worker_rejects_queued_request_with_other_source_roots(tmp_path: Path) -> None:
    old_market, old_history = _market(tmp_path), _history(tmp_path)
    current = tmp_path / "current"
    current.mkdir()
    market, history = _market(current), _history(current)
    config = FormulaMarketPageBackendConfig(
        universe_root=market[0],
        projection_root=history[0],
        state_path=tmp_path / "state" / "formula-jobs.sqlite",
        artifact_directory=tmp_path / "results",
    )
    config_path = tmp_path / "formula-worker-config.json"
    _write_config(config_path, config)
    store = FormulaMarketJobStore(
        state_path=config.state_path,
        artifact_directory=config.artifact_directory,
    )
    queued = store.submit(_request(old_market, old_history))

    completed = _worker(config_path)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "status": "failed",
        "task_id": queued.task_id,
        "error_code": "source_changed",
    }
    assert store.status(queued.task_id).status == "failed"
    assert tuple(config.artifact_directory.iterdir()) == ()
    snapshot = read_formula_market_job_snapshot(
        config.state_path,
        config.artifact_directory,
        observed_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    assert snapshot.jobs[0].error_code == "source_changed"
    assert snapshot.artifacts == ()
