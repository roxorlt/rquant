from __future__ import annotations

import fcntl
import os
import shutil
import subprocess
import sys
import time
import venv
from pathlib import Path

import pytest

from rquant.release_generation import (
    ReleaseGenerationAuthority,
    marker_path_for_lock,
)

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = ROOT / "scripts" / "bootstrap-production-deploy.py"
AUTHORITY = ROOT / "src" / "rquant" / "release_generation.py"
TRUSTED_GIT = Path("/usr/bin/git")


def _git(checkout: Path, *arguments: str) -> str:
    return subprocess.run(
        [str(TRUSTED_GIT), *arguments],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _checkout(
    tmp_path: Path,
    *,
    publish_marker: bool = True,
) -> tuple[Path, Path, Path, str]:
    checkout = tmp_path / "rquant"
    package = checkout / "src" / "rquant"
    ops = package / "ops"
    scripts = checkout / "scripts"
    ops.mkdir(parents=True)
    scripts.mkdir()
    shutil.copy2(BOOTSTRAP, scripts / BOOTSTRAP.name)
    shutil.copy2(AUTHORITY, package / AUTHORITY.name)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (ops / "__init__.py").write_text("", encoding="utf-8")
    (ops / "production_deploy.py").write_text(
        "from __future__ import annotations\n"
        "import fcntl, os, time\n"
        "from pathlib import Path\n"
        "lock_fd = os.open(os.environ['DEPLOY_LOCK'], os.O_RDONLY)\n"
        "try:\n"
        "    fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)\n"
        "except BlockingIOError:\n"
        "    Path(os.environ['IMPORT_MARKER']).write_text('locked', encoding='utf-8')\n"
        "else:\n"
        "    Path(os.environ['IMPORT_MARKER']).write_text('unlocked', encoding='utf-8')\n"
        "finally:\n"
        "    os.close(lock_fd)\n"
        "def main(argv=None):\n"
        "    Path(os.environ['RUN_MARKER']).write_text('ran', encoding='utf-8')\n"
        "    time.sleep(float(os.environ.get('DEPLOY_HOLD_SECONDS', '0')))\n"
        "    return 0\n",
        encoding="utf-8",
    )
    (checkout / ".gitignore").write_text("/.venv\n", encoding="utf-8")
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.0"\n',
        encoding="utf-8",
    )
    (checkout / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    venv.EnvBuilder(with_pip=False, symlinks=False).create(checkout / ".venv")
    python_library = (
        Path(sys.base_prefix)
        / "lib"
        / f"libpython{sys.version_info.major}.{sys.version_info.minor}.dylib"
    )
    if python_library.exists():
        shutil.copy2(python_library, checkout / ".venv" / "lib" / python_library.name)
    python = checkout / ".venv" / "bin" / "python"
    rquant = checkout / ".venv" / "bin" / "rquant"
    rquant.write_text(
        f"#!{python}\n"
        "import os, sys\n"
        "if sys.argv[1:] != ['preflight']:\n"
        "    raise SystemExit(64)\n"
        "raise SystemExit(int(os.environ.get('PREFLIGHT_EXIT', '0')))\n",
        encoding="utf-8",
    )
    rquant.chmod(0o700)
    uv = checkout / ".venv" / "bin" / "uv"
    uv.write_text(
        f"#!{python}\n"
        "import os, sys\n"
        "if sys.argv[1:] != ['sync', '--frozen']:\n"
        "    raise SystemExit(64)\n"
        "raise SystemExit(int(os.environ.get('UV_SYNC_EXIT', '0')))\n",
        encoding="utf-8",
    )
    uv.chmod(0o700)
    subprocess.run([str(TRUSTED_GIT), "init", "-q", "-b", "main"], cwd=checkout, check=True)
    subprocess.run([str(TRUSTED_GIT), "add", "."], cwd=checkout, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "bootstrap generation",
        ],
        cwd=checkout,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        [str(TRUSTED_GIT), "update-ref", "refs/remotes/origin/main", commit],
        cwd=checkout,
        check=True,
    )
    lock_root = tmp_path / ".rquant-deploy"
    lock_root.mkdir(mode=0o700)
    lock_path = lock_root / "rquant.lock"
    if publish_marker:
        lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            ReleaseGenerationAuthority(
                repo=checkout,
                lock_path=lock_path,
                lock_fd=lock_fd,
                python_path=python,
                git_path=TRUSTED_GIT,
                writable=True,
            ).publish(expected_commit=commit)
        finally:
            os.close(lock_fd)
    return checkout, python, lock_path, commit


def _commit_next_release(checkout: Path) -> str:
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.1"\n',
        encoding="utf-8",
    )
    (checkout / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    _git(checkout, "add", "pyproject.toml", "uv.lock")
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "next generation",
    )
    commit = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "update-ref", "refs/remotes/origin/main", commit)
    return commit


def _command(
    checkout: Path,
    python: Path,
    lock_path: Path,
    *,
    target: str = "v0.99.0",
    mode: str = "deploy",
    recovery_action: str | None = None,
) -> list[str]:
    command = [
        str(python),
        "-I",
        "-S",
        str(checkout / "scripts" / BOOTSTRAP.name),
        "--expected-checkout-root",
        str(checkout),
        "--trusted-git-path",
        str(TRUSTED_GIT),
        "--deployment-lock-path",
        str(lock_path),
        "--python-path",
        str(python),
        "--uv-path",
        str(checkout / ".venv" / "bin" / "uv"),
    ]
    if mode == "initialize":
        command.append("--initialize-generation")
    elif mode == "recover":
        command.append("--recover-generation")
        command.extend(["--recovery-action", str(recovery_action)])
    command.extend(["--", "--target", target])
    return command


def test_deploy_bootstrap_holds_exclusive_generation_before_project_import(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    first_import = tmp_path / "first-import"
    first_run = tmp_path / "first-run"
    second_import = tmp_path / "second-import"
    second_run = tmp_path / "second-run"
    first_env = {
        **os.environ,
        "DEPLOY_LOCK": str(lock_path),
        "IMPORT_MARKER": str(first_import),
        "RUN_MARKER": str(first_run),
        "DEPLOY_HOLD_SECONDS": "1.0",
    }
    first = subprocess.Popen(
        _command(checkout, python, lock_path),
        cwd=checkout,
        env=first_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 5
    while not first_import.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert first_import.read_text(encoding="utf-8") == "locked"
    second = subprocess.run(
        _command(checkout, python, lock_path),
        cwd=checkout,
        env={
            **os.environ,
            "DEPLOY_LOCK": str(lock_path),
            "IMPORT_MARKER": str(second_import),
            "RUN_MARKER": str(second_run),
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    first_stdout, first_stderr = first.communicate(timeout=5)

    assert first.returncode == 0, first_stdout + first_stderr
    assert first_run.read_text(encoding="utf-8") == "ran"
    assert second.returncode == 2
    assert "generation is active" in second.stderr
    assert not second_import.exists()
    assert not second_run.exists()


def test_initialize_generation_publishes_first_marker_without_importing_deployer(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    imported = tmp_path / "imported"
    ran = tmp_path / "ran"

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="initialize",
        ),
        cwd=checkout,
        env={
            **os.environ,
            "DEPLOY_LOCK": str(lock_path),
            "IMPORT_MARKER": str(imported),
            "RUN_MARKER": str(ran),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert marker_path_for_lock(lock_path).is_file()
    assert not imported.exists()
    assert not ran.exists()


@pytest.mark.parametrize("failure_env", [{"UV_SYNC_EXIT": "1"}, {"PREFLIGHT_EXIT": "1"}])
def test_initialize_generation_interruption_can_restart_without_partial_marker(
    tmp_path: Path,
    failure_env: dict[str, str],
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        mode="initialize",
    )

    failed = subprocess.run(
        command,
        cwd=checkout,
        env={**os.environ, **failure_env},
        capture_output=True,
        text=True,
        check=False,
    )
    assert failed.returncode == 2
    assert "Traceback" not in failed.stderr
    assert not marker_path_for_lock(lock_path).exists()

    recovered = subprocess.run(
        command,
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert recovered.returncode == 0, recovered.stderr
    assert marker_path_for_lock(lock_path).is_file()


@pytest.mark.parametrize("recovery_action", ["resume", "rollback"])
def test_recover_generation_republishes_only_exact_verified_target(
    tmp_path: Path,
    recovery_action: str,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path)
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="recover",
            recovery_action=recovery_action,
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert marker_path_for_lock(lock_path).is_file()


@pytest.mark.parametrize("failure_env", [{"UV_SYNC_EXIT": "1"}, {"PREFLIGHT_EXIT": "1"}])
def test_recover_generation_failure_stays_unpublished_and_can_restart(
    tmp_path: Path,
    failure_env: dict[str, str],
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path)
    commit = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    marker = marker_path_for_lock(lock_path)
    marker.unlink()
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        mode="recover",
        recovery_action="resume",
    )

    failed = subprocess.run(
        command,
        cwd=checkout,
        env={**os.environ, **failure_env},
        capture_output=True,
        text=True,
        check=False,
    )
    assert failed.returncode == 2
    assert "Traceback" not in failed.stderr
    assert _git(checkout, "rev-parse", "HEAD") == commit
    assert not marker.exists()

    recovered = subprocess.run(
        command,
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert recovered.returncode == 0, recovered.stderr
    assert marker.is_file()


def test_recover_generation_resumes_fast_forward_target_after_interruption(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=target,
            mode="recover",
            recovery_action="resume",
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == target
    assert marker_path_for_lock(lock_path).is_file()


def test_recover_generation_rolls_back_to_verified_previous_release(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path)
    _commit_next_release(checkout)
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=previous,
            mode="recover",
            recovery_action="rollback",
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == previous
    assert marker_path_for_lock(lock_path).is_file()


def test_generation_mode_rejects_target_that_is_not_current_head(tmp_path: Path) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path, publish_marker=False)

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target="f" * 40,
            mode="initialize",
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "Traceback" not in result.stderr
    assert not marker_path_for_lock(lock_path).exists()


def test_missing_generation_is_controlled_exit_without_traceback(tmp_path: Path) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(checkout, python, lock_path),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "marker is missing" in result.stderr
    assert "Traceback" not in result.stderr


def test_dirty_release_authority_is_rejected_before_project_import(tmp_path: Path) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    imported = tmp_path / "imported"
    ran = tmp_path / "ran"
    authority = checkout / "src" / "rquant" / "release_generation.py"
    authority.write_text(
        authority.read_text(encoding="utf-8") + "\nDIRTY = True\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        _command(checkout, python, lock_path),
        cwd=checkout,
        env={
            **os.environ,
            "DEPLOY_LOCK": str(lock_path),
            "IMPORT_MARKER": str(imported),
            "RUN_MARKER": str(ran),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "checkout is dirty" in result.stderr
    assert "Traceback" not in result.stderr
    assert not imported.exists()
    assert not ran.exists()
