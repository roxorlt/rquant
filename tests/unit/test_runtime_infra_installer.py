from __future__ import annotations

import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts" / "install-runtime-credential-infra.sh"


@pytest.mark.parametrize(
    "failure_step",
    (
        "libexec_dir",
        "helper_install",
        "helper_publish",
        "sudoers_install",
        "sudoers_validate_staging",
        "sudoers_publish",
        "sudoers_validate_final",
    ),
)
def test_failure_is_nonzero_and_same_head_rerun_recovers(
    tmp_path: Path,
    failure_step: str,
) -> None:
    failed = subprocess.run(
        [
            "/bin/bash",
            str(INSTALLER),
            "--test-root",
            str(tmp_path),
            "--fail-step",
            failure_step,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert failed.returncode != 0
    assert "installed" not in failed.stdout.lower()

    recovered = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert recovered.returncode == 0, recovered.stderr
    helper = tmp_path / "usr/local/libexec/rquant-runtime-credential-sealer"
    sudoers = tmp_path / "etc/sudoers.d/rquant-production-deploy"
    assert (
        helper.read_bytes()
        == (ROOT / "deploy/libexec/rquant-runtime-credential-sealer").read_bytes()
    )
    assert sudoers.read_bytes() == (ROOT / "deploy/sudoers/rquant-production-deploy").read_bytes()
    assert helper.stat().st_mode & 0o777 == 0o755
    assert sudoers.stat().st_mode & 0o777 == 0o440
    assert stat.S_IMODE(sudoers.parent.stat().st_mode) == 0o750


def test_failed_sudoers_restore_preserves_backup_for_next_run(tmp_path: Path) -> None:
    sudoers_dir = tmp_path / "etc/sudoers.d"
    sudoers_dir.mkdir(parents=True, mode=0o750)
    sudoers_dir.chmod(0o750)
    target = sudoers_dir / "rquant-production-deploy"
    target.write_text("old-known-good\n")
    target.chmod(0o440)

    failed = subprocess.run(
        [
            "/bin/bash",
            str(INSTALLER),
            "--test-root",
            str(tmp_path),
            "--fail-step",
            "sudoers_validate_final,sudoers_restore",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert failed.returncode != 0
    backup = Path(f"{target}.backup")
    assert backup.read_text() == "old-known-good\n"
    assert str(backup) in failed.stderr

    recovered = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert recovered.returncode == 0, recovered.stderr
    assert target.read_bytes() == (ROOT / "deploy/sudoers/rquant-production-deploy").read_bytes()
    assert not backup.exists()
    assert stat.S_IMODE(sudoers_dir.stat().st_mode) == 0o750


def test_existing_sudoers_directory_mode_is_preserved(tmp_path: Path) -> None:
    sudoers_dir = tmp_path / "etc/sudoers.d"
    sudoers_dir.mkdir(parents=True, mode=0o700)
    sudoers_dir.chmod(0o700)

    result = subprocess.run(
        ["/bin/bash", str(INSTALLER), "--test-root", str(tmp_path)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert stat.S_IMODE(sudoers_dir.stat().st_mode) == 0o700


def test_legacy_deployer_reconciles_infra_before_same_head_exit() -> None:
    deployer = (ROOT / "scripts/deploy.sh").read_text(encoding="utf-8")

    reconcile = deployer.index("install-runtime-credential-infra.sh")
    same_head_exit = deployer.index('if [[ "${PRE_HEAD}" == "${POST_HEAD}" ]]')
    assert reconcile < same_head_exit
