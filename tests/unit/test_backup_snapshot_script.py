"""Consistent DuckDB backup snapshot shell-script tests."""

from __future__ import annotations

import gzip
import json
import os
import signal
import subprocess
import time
from configparser import ConfigParser
from contextlib import suppress
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
BACKUP_UNIT = ROOT / "deploy" / "systemd" / "rquant-backup.service"


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "rquant"
    (project / "scripts").mkdir(parents=True)
    (project / "data").mkdir()
    (project / ".venv").symlink_to((ROOT / ".venv").resolve(), target_is_directory=True)
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
    source = (ROOT / "scripts/backup-snapshot.sh").read_text(encoding="utf-8")
    script = project / "scripts/backup-snapshot.sh"
    script.write_text(
        source.replace("/usr/local/libexec/rquant-workload-arbiter", str(arbiter)),
        encoding="utf-8",
    )
    script.chmod(0o755)
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
    recovery_enabled: bool = False,
    recovery_cli: Path | None = None,
    recovery_config: Path | None = None,
    recovery_credential: Path | None = None,
    recovery_profile_generation: str | None = None,
    recovery_signer_key_id: str | None = None,
    runtime_root: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for name in (
        "RQUANT_RECOVERY_BACKUP_ENABLED",
        "RQUANT_RECOVERY_BACKUP_CONFIG",
        "RQUANT_RECOVERY_CREDENTIAL_FILE",
        "RQUANT_RECOVERY_PROFILE_GENERATION",
        "RQUANT_RECOVERY_SIGNER_KEY_ID",
        "RQUANT_RUNTIME_ROOT",
    ):
        env.pop(name, None)
    if source is not None:
        env["RQUANT_BACKUP_SOURCE"] = source
    if backup_project is not None:
        env["RQUANT_BACKUP_PROJECT_DIR"] = str(backup_project)
    if max_source_lag_seconds is not None:
        env["RQUANT_BACKUP_MAX_SOURCE_LAG_SECONDS"] = str(max_source_lag_seconds)
    if replica_wait_seconds is not None:
        env["RQUANT_BACKUP_REPLICA_WAIT_SECONDS"] = str(replica_wait_seconds)
    if recovery_enabled:
        env["RQUANT_RECOVERY_BACKUP_ENABLED"] = "true"
    if recovery_cli is not None:
        env["RQUANT_RECOVERY_CLI"] = str(recovery_cli)
    if recovery_config is not None:
        env["RQUANT_RECOVERY_BACKUP_CONFIG"] = str(recovery_config)
    if recovery_credential is not None:
        env["RQUANT_RECOVERY_CREDENTIAL_FILE"] = str(recovery_credential)
    if recovery_profile_generation is not None:
        env["RQUANT_RECOVERY_PROFILE_GENERATION"] = recovery_profile_generation
    if recovery_signer_key_id is not None:
        env["RQUANT_RECOVERY_SIGNER_KEY_ID"] = recovery_signer_key_id
    if runtime_root is not None:
        env["RQUANT_RUNTIME_ROOT"] = str(runtime_root)
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


def test_backup_unit_allows_large_snapshot_compression_to_finish() -> None:
    unit = ConfigParser(interpolation=None, strict=True)
    unit.read_string(BACKUP_UNIT.read_text(encoding="utf-8"))

    assert unit.get("Service", "TimeoutStartSec") == "10min"


def test_terminated_backup_cleans_private_generation(
    tmp_path: Path,
) -> None:
    project = _project(tmp_path)
    _write_db(project / "data" / "rquant.duckdb", "main")
    assert _run(project, source="main").returncode == 0
    previous = (project / "backup" / "latest.duckdb.gz").read_bytes()

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    entered = tmp_path / "gzip-entered"
    fake_gzip = fake_bin / "gzip"
    fake_gzip.write_text(
        '#!/usr/bin/env bash\nset -e\ntouch "${RQUANT_TEST_GZIP_ENTERED}"\nsleep 60\n',
        encoding="utf-8",
    )
    fake_gzip.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["RQUANT_BACKUP_SOURCE"] = "main"
    env["RQUANT_TEST_GZIP_ENTERED"] = str(entered)
    process = subprocess.Popen(
        [str(project / "scripts" / "backup-snapshot.sh")],
        cwd=project,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not entered.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert entered.exists()

        os.killpg(process.pid, signal.SIGTERM)
        process.communicate(timeout=10)
    finally:
        if process.poll() is None:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=10)

    assert process.returncode != 0
    assert not tuple((project / "backup").glob(".latest.*"))
    assert (project / "backup" / "latest.duckdb.gz").read_bytes() == previous


@pytest.mark.parametrize("recovery_enabled", [True, False], ids=["explicit", "profile-auto"])
def test_verified_legacy_snapshot_can_publish_recovery_bundle_via_explicit_gate(
    tmp_path: Path,
    recovery_enabled: bool,
) -> None:
    project = _project(tmp_path)
    _write_db(project / "data" / "rquant.duckdb", "main")
    cli = tmp_path / "fake-rquant"
    calls = tmp_path / "recovery-calls.txt"
    runtime_root = project / "runtime"
    if not recovery_enabled:
        runtime_root.mkdir()
        (runtime_root / "current").write_text("generation", encoding="ascii")
    config = tmp_path / "recovery-backup.json"
    credential = tmp_path / "recovery-credential.json"
    config.write_text("{}", encoding="ascii")
    credential.write_text("{}", encoding="ascii")
    cli.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$*" >> "${RQUANT_TEST_RECOVERY_CALLS}"\n'
        'if [[ "$1" == "runtime-recovery-production-config" ]]; then\n'
        "  printf '%s\\n' "
        + repr(
            json.dumps(
                {
                    "status": "ready",
                    "runtime_root": str(runtime_root),
                    "producer_commit": "a" * 40,
                    "profile_id": "2" * 64,
                    "profile_generation": "0" * 63 + "1",
                    "backup_environment": {
                        "RQUANT_RECOVERY_BACKUP_ENABLED": "true",
                        "RQUANT_RECOVERY_BACKUP_CONFIG": str(config),
                        "RQUANT_RECOVERY_CREDENTIAL_FILE": str(credential),
                        "RQUANT_RECOVERY_PROFILE_GENERATION": "0" * 63 + "1",
                        "RQUANT_RECOVERY_SIGNER_KEY_ID": "production-recovery-v1",
                    },
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        + "\n"
        'elif [[ "$2" == "dry-run" ]]; then\n'
        '  printf \'{"plan_id":"%064d","target_profile_generation":"%064d",'
        '"signer_key_id":"production-recovery-v1"}\\n\' 0 1\n'
        "else\n"
        '  printf \'{"status":"succeeded"}\\n\'\n'
        "fi\n",
        encoding="ascii",
    )
    cli.chmod(0o755)
    env_calls = os.environ.get("RQUANT_TEST_RECOVERY_CALLS")
    os.environ["RQUANT_TEST_RECOVERY_CALLS"] = str(calls)
    try:
        result = _run(
            project,
            source="main",
            recovery_enabled=recovery_enabled,
            recovery_cli=cli,
            recovery_config=config,
            recovery_credential=credential,
            recovery_profile_generation="0" * 63 + "1",
            recovery_signer_key_id="production-recovery-v1",
            runtime_root=runtime_root,
        )
    finally:
        if env_calls is None:
            os.environ.pop("RQUANT_TEST_RECOVERY_CALLS", None)
        else:
            os.environ["RQUANT_TEST_RECOVERY_CALLS"] = env_calls

    assert result.returncode == 0, result.stderr
    invocations = calls.read_text(encoding="utf-8").splitlines()
    assert invocations[0] == (f"runtime-recovery-production-config --runtime-root {runtime_root}")
    assert invocations[1].startswith("runtime-recovery-backup dry-run")
    assert invocations[2].startswith("runtime-recovery-backup execute")
    assert "--plan-id " + "0" * 64 in invocations[2]


def test_recovery_backup_gate_rejects_missing_profile_generation(tmp_path: Path) -> None:
    project = _project(tmp_path)
    _write_db(project / "data" / "rquant.duckdb", "main")
    config = tmp_path / "recovery-backup.json"
    credential = tmp_path / "recovery-credential.json"
    config.write_text("{}", encoding="ascii")
    credential.write_text("{}", encoding="ascii")

    result = _run(
        project,
        source="main",
        recovery_enabled=True,
        recovery_config=config,
        recovery_credential=credential,
    )

    assert result.returncode == 2
