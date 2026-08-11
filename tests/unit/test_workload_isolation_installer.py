from __future__ import annotations

import hashlib
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts/install-workload-isolation-infra.sh"


def _run(root: Path, *, fault: str | None = None) -> subprocess.CompletedProcess[str]:
    command = ["/bin/bash", str(INSTALLER), "--test-root", str(root)]
    if fault is not None:
        command.extend(("--fail-step", fault))
    return subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_installer_publishes_fixed_helper_and_locks_without_systemctl(
    tmp_path: Path,
) -> None:
    result = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    helper = tmp_path / "usr/local/libexec/rquant-workload-arbiter"
    helper_hash = tmp_path / "usr/local/libexec/rquant-workload-arbiter.sha256"
    assert helper.read_bytes() == (ROOT / "deploy/libexec/rquant-workload-arbiter").read_bytes()
    assert stat.S_IMODE(helper.stat().st_mode) == 0o755
    assert helper_hash.read_text(encoding="ascii") == (
        f"{hashlib.sha256(helper.read_bytes()).hexdigest()}\n"
    )
    assert stat.S_IMODE(helper_hash.stat().st_mode) == 0o444
    runtime = tmp_path / "run/rquant-workload-isolation"
    assert stat.S_IMODE(runtime.stat().st_mode) == 0o770
    migration = tmp_path / "var/lib/rquant/workload-isolation/migration"
    assert stat.S_IMODE(migration.stat().st_mode) == 0o700
    assert stat.S_IMODE((migration / "migration.lock").stat().st_mode) == 0o600
    for name in (
        "intent.lock",
        "research-transition.lock",
        "research-active.lock",
        "maintenance-active.lock",
    ):
        assert stat.S_IMODE((runtime / name).stat().st_mode) == 0o660
    assert "no unit state changed" in result.stdout
    assert "systemctl" not in (ROOT / "scripts/install-workload-isolation-infra.sh").read_text()


def test_installer_is_idempotent(tmp_path: Path) -> None:
    assert _run(tmp_path).returncode == 0

    second = _run(tmp_path)

    assert second.returncode == 0, second.stderr


@pytest.mark.parametrize(
    "fault",
    ("helper_publish", "helper_hash_publish", "tmpfiles_publish", "runtime_create"),
)
def test_installer_fault_restores_previous_candidate_files(
    tmp_path: Path,
    fault: str,
) -> None:
    helper = tmp_path / "usr/local/libexec/rquant-workload-arbiter"
    helper_hash = tmp_path / "usr/local/libexec/rquant-workload-arbiter.sha256"
    config = tmp_path / "etc/tmpfiles.d/rquant-workload-isolation.conf"
    helper.parent.mkdir(parents=True)
    config.parent.mkdir(parents=True)
    helper.write_bytes(b"old-helper\n")
    helper_hash.write_bytes(b"old-hash\n")
    config.write_bytes(b"old-config\n")

    result = _run(tmp_path, fault=fault)

    assert result.returncode != 0
    assert helper.read_bytes() == b"old-helper\n"
    assert helper_hash.read_bytes() == b"old-hash\n"
    assert config.read_bytes() == b"old-config\n"
    assert "rolled back" in result.stderr
