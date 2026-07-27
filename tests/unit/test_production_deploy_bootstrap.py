from __future__ import annotations

import fcntl
import os
import shutil
import subprocess
import sys
import time
import venv
from pathlib import Path

from rquant.release_generation import ReleaseGenerationAuthority

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = ROOT / "scripts" / "bootstrap-production-deploy.py"
AUTHORITY = ROOT / "src" / "rquant" / "release_generation.py"
TRUSTED_GIT = Path("/usr/bin/git")


def _checkout(tmp_path: Path) -> tuple[Path, Path, Path]:
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
    subprocess.run([str(TRUSTED_GIT), "init", "-q"], cwd=checkout, check=True)
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
    lock_root = tmp_path / ".rquant-deploy"
    lock_root.mkdir(mode=0o700)
    lock_path = lock_root / "rquant.lock"
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
    return checkout, python, lock_path


def _command(checkout: Path, python: Path, lock_path: Path) -> list[str]:
    return [
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
        "--",
        "--target",
        "v0.99.0",
    ]


def test_deploy_bootstrap_holds_exclusive_generation_before_project_import(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path = _checkout(tmp_path)
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
