"""WAL-free read-only replica publication tests."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "rquant"
    (project / "scripts").mkdir(parents=True)
    (project / "data").mkdir()
    (project / ".venv").symlink_to((ROOT / ".venv").resolve(), target_is_directory=True)
    (project / "src").symlink_to(ROOT / "src", target_is_directory=True)
    arbiter = project / "test-libexec/rquant-workload-arbiter"
    arbiter.parent.mkdir()
    arbiter.write_text(
        """#!/usr/bin/env bash
while [[ "${1:-}" != "--" ]]; do shift; done
shift
RQUANT_WORKLOAD_ARBITER_HELD=maintenance exec "$@"
""",
        encoding="utf-8",
    )
    arbiter.chmod(0o755)
    source = (ROOT / "scripts/sync-readonly-replica.sh").read_text(encoding="utf-8")
    script = project / "scripts/sync-readonly-replica.sh"
    script.write_text(
        source.replace("/usr/local/libexec/rquant-workload-arbiter", str(arbiter)),
        encoding="utf-8",
    )
    script.chmod(0o755)
    return project


def _run(project: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    return subprocess.run(
        [str(project / "scripts" / "sync-readonly-replica.sh")],
        cwd=project,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _marker(path: Path) -> list[str]:
    conn = duckdb.connect(str(path), read_only=True)
    values = [
        str(row[0])
        for row in conn.execute("SELECT value FROM marker ORDER BY value").fetchall()
    ]
    conn.close()
    return values


def test_replica_consolidates_source_wal_before_atomic_publish(tmp_path: Path) -> None:
    project = _project(tmp_path)
    main = project / "data" / "rquant.duckdb"
    replica = project / "data" / "rquant_ro.duckdb"
    writer = duckdb.connect(str(main))
    writer.execute("CREATE TABLE marker (value VARCHAR)")
    writer.execute("INSERT INTO marker VALUES ('base'), ('wal')")
    assert Path(f"{main}.wal").exists()
    try:
        result = _run(project)
    finally:
        writer.close()

    assert result.returncode == 0, result.stderr
    assert _marker(replica) == ["base", "wal"]
    assert not Path(f"{replica}.wal").exists()
    generation = json.loads(
        Path(f"{replica}.generation.json").read_text(encoding="utf-8")
    )
    assert generation["schema_version"] == 1
    assert generation["source_database"] == str(main.resolve())
    assert generation["source_before"] == generation["source_after"]
    assert generation["replica"]["size"] == replica.stat().st_size


def test_invalid_source_preserves_previous_verified_replica(tmp_path: Path) -> None:
    project = _project(tmp_path)
    main = project / "data" / "rquant.duckdb"
    replica = project / "data" / "rquant_ro.duckdb"
    conn = duckdb.connect(str(main))
    conn.execute("CREATE TABLE marker (value VARCHAR)")
    conn.execute("INSERT INTO marker VALUES ('good')")
    conn.close()
    assert _run(project).returncode == 0
    generation_path = Path(f"{replica}.generation.json")
    previous_generation = generation_path.read_bytes()

    main.write_bytes(b"not a duckdb database")
    result = _run(project)

    assert result.returncode != 0
    assert _marker(replica) == ["good"]
    assert generation_path.read_bytes() == previous_generation
