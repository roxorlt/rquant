"""Consistent DuckDB backup snapshot shell-script tests."""

from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "rquant"
    (project / "scripts").mkdir(parents=True)
    (project / "data").mkdir()
    (project / ".venv").symlink_to((ROOT / ".venv").resolve(), target_is_directory=True)
    shutil.copy2(
        ROOT / "scripts" / "backup-snapshot.sh",
        project / "scripts" / "backup-snapshot.sh",
    )
    return project


def _write_db(path: Path, marker: str) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE marker (value VARCHAR)")
    conn.execute("INSERT INTO marker VALUES (?)", [marker])
    conn.close()


def _run(
    project: Path,
    *,
    source: str | None = None,
    backup_project: Path | None = None,
    max_source_lag_seconds: int | None = None,
    replica_wait_seconds: int | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if source is not None:
        env["RQUANT_BACKUP_SOURCE"] = source
    if backup_project is not None:
        env["RQUANT_BACKUP_PROJECT_DIR"] = str(backup_project)
    if max_source_lag_seconds is not None:
        env["RQUANT_BACKUP_MAX_SOURCE_LAG_SECONDS"] = str(max_source_lag_seconds)
    if replica_wait_seconds is not None:
        env["RQUANT_BACKUP_REPLICA_WAIT_SECONDS"] = str(replica_wait_seconds)
    return subprocess.run(
        [str(project / "scripts" / "backup-snapshot.sh")],
        cwd=project,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _restore_marker(project: Path, output: Path) -> str:
    with gzip.open(project / "backup" / "latest.duckdb.gz", "rb") as source:
        output.write_bytes(source.read())
    conn = duckdb.connect(str(output), read_only=True)
    marker = str(conn.execute("SELECT value FROM marker").fetchone()[0])
    conn.close()
    return marker


def test_scheduled_backup_defaults_to_verified_replica(tmp_path: Path) -> None:
    project = _project(tmp_path)
    _write_db(project / "data" / "rquant.duckdb", "main")
    _write_db(project / "data" / "rquant_ro.duckdb", "replica")

    result = _run(project)

    assert result.returncode == 0, result.stderr
    assert _restore_marker(project, tmp_path / "restored.duckdb") == "replica"
    metadata = json.loads((project / "backup" / "latest.json").read_text())
    assert metadata["source"] == "replica"
    assert metadata["verified"] is True
    assert metadata["table_count"] >= 1
    assert metadata["source_lag_seconds"] >= 0


def test_quiescent_main_backup_checkpoints_and_verifies_snapshot(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path)
    _write_db(project / "data" / "rquant.duckdb", "main")

    result = _run(project, source="main")

    assert result.returncode == 0, result.stderr
    assert _restore_marker(project, tmp_path / "restored.duckdb") == "main"
    metadata = json.loads((project / "backup" / "latest.json").read_text())
    assert metadata["source"] == "main"
    assert metadata["verified"] is True


def test_release_worktree_script_can_backup_production_project(
    tmp_path: Path,
) -> None:
    release_worktree = _project(tmp_path / "release")
    production = _project(tmp_path / "production")
    _write_db(production / "data" / "rquant.duckdb", "production-main")

    result = _run(
        release_worktree,
        source="main",
        backup_project=production,
    )

    assert result.returncode == 0, result.stderr
    assert _restore_marker(production, tmp_path / "restored-production.duckdb") == (
        "production-main"
    )
    assert not (release_worktree / "backup" / "latest.duckdb.gz").exists()


def test_locked_main_backup_fails_without_replacing_last_good_snapshot(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path)
    database = project / "data" / "rquant.duckdb"
    _write_db(database, "before")
    assert _run(project, source="main").returncode == 0
    previous = (project / "backup" / "latest.duckdb.gz").read_bytes()

    writer = duckdb.connect(str(database))
    writer.execute("INSERT INTO marker VALUES ('during-lock')")
    try:
        result = _run(project, source="main")
    finally:
        writer.close()

    assert result.returncode != 0
    assert (project / "backup" / "latest.duckdb.gz").read_bytes() == previous


def test_stale_replica_fails_without_replacing_last_good_snapshot(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path)
    main = project / "data" / "rquant.duckdb"
    replica = project / "data" / "rquant_ro.duckdb"
    _write_db(main, "main")
    _write_db(replica, "replica")
    assert _run(project).returncode == 0
    previous = (project / "backup" / "latest.duckdb.gz").read_bytes()

    os.utime(replica, (1, 1))
    os.utime(main, (10_000, 10_000))
    result = _run(
        project,
        max_source_lag_seconds=300,
        replica_wait_seconds=0,
    )

    assert result.returncode != 0
    assert (project / "backup" / "latest.duckdb.gz").read_bytes() == previous
