"""WAL-free read-only replica publication tests."""

from __future__ import annotations

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
        ROOT / "scripts" / "sync-readonly-replica.sh",
        project / "scripts" / "sync-readonly-replica.sh",
    )
    return project


def _run(project: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(project / "scripts" / "sync-readonly-replica.sh")],
        cwd=project,
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


def test_invalid_source_preserves_previous_verified_replica(tmp_path: Path) -> None:
    project = _project(tmp_path)
    main = project / "data" / "rquant.duckdb"
    replica = project / "data" / "rquant_ro.duckdb"
    conn = duckdb.connect(str(main))
    conn.execute("CREATE TABLE marker (value VARCHAR)")
    conn.execute("INSERT INTO marker VALUES ('good')")
    conn.close()
    assert _run(project).returncode == 0

    main.write_bytes(b"not a duckdb database")
    result = _run(project)

    assert result.returncode != 0
    assert _marker(replica) == ["good"]
