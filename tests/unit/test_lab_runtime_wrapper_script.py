from __future__ import annotations

import ast
import fcntl
import json
import os
import runpy
import shutil
import subprocess
import sys
import time
import venv
from pathlib import Path

import pytest

from rquant.release_generation import ReleaseGenerationAuthority, marker_path_for_lock

ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / "scripts" / "run-lab-daemon.py"
PREFLIGHT = ROOT / "scripts" / "preflight-lab-runtime.py"
BOOTSTRAP = ROOT / "scripts" / "bootstrap-lab-daemon.py"
TRUSTED_GIT = Path("/usr/bin/git")
RELEASE_AUTHORITY = ROOT / "src" / "rquant" / "release_generation.py"


def _runtime_checkout(tmp_path: Path) -> tuple[Path, Path, Path]:
    checkout = tmp_path / "checkout"
    scripts = checkout / "scripts"
    package = checkout / "src" / "rquant"
    scripts.mkdir(parents=True)
    package.mkdir(parents=True)
    shutil.copy2(WRAPPER, scripts / WRAPPER.name)
    shutil.copy2(PREFLIGHT, scripts / PREFLIGHT.name)
    shutil.copy2(BOOTSTRAP, scripts / BOOTSTRAP.name)
    shutil.copy2(RELEASE_AUTHORITY, package / RELEASE_AUTHORITY.name)
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    (checkout / ".gitignore").write_text(
        "/.venv\n__pycache__/\n*.pyc\n*.pyo\n*.so\n*.dylib\n*.pyd\n",
        encoding="utf-8",
    )
    marker = checkout / "daemon.json"
    (package / "__init__.py").write_text("", encoding="utf-8")
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.0"\n',
        encoding="utf-8",
    )
    (checkout / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (package / "cli.py").write_text(
        "from __future__ import annotations\n"
        "import json, os, sys, time\n"
        "from pathlib import Path\n"
        "def main():\n"
        "    Path(os.environ['LAB_WRAPPER_MARKER']).write_text("
        "json.dumps(sys.argv[1:]), encoding='utf-8')\n"
        "    time.sleep(float(os.environ.get('LAB_WRAPPER_HOLD_SECONDS', '0')))\n"
        "    print('fake daemon executed', flush=True)\n",
        encoding="utf-8",
    )
    venv.EnvBuilder(with_pip=False, symlinks=False).create(checkout / ".venv")
    python_library = (
        Path(sys.base_prefix)
        / "lib"
        / f"libpython{sys.version_info.major}.{sys.version_info.minor}.dylib"
    )
    if python_library.exists():
        shutil.copy2(python_library, checkout / ".venv" / "lib" / python_library.name)
    python = checkout / ".venv" / "bin" / "python"
    executable = checkout / ".venv" / "bin" / "rquant"
    executable.write_text(
        f"#!{python}\n"
        "import sys\n"
        f"sys.path.insert(0, {str(checkout / 'src')!r})\n"
        "from rquant import main\n"
        "main()\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    subprocess.run(["git", "add", "."], cwd=checkout, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "test fixture",
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
    lock_path = _deployment_lock_path(checkout)
    lock_path.parent.mkdir(mode=0o700)
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
    return checkout, executable, marker


def _deployment_lock_path(checkout: Path) -> Path:
    return checkout.parent / ".rquant-deploy" / f"{checkout.name}.lock"


def _run_wrapper(
    checkout: Path,
    executable: Path,
    marker: Path,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        environment.pop(variable, None)
    environment["LAB_WRAPPER_MARKER"] = str(marker)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [
            str(checkout / ".venv" / "bin" / "python"),
            "-I",
            "-S",
            str(checkout / "scripts" / WRAPPER.name),
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--deployment-lock-path",
            str(_deployment_lock_path(checkout)),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )


def test_lab_runtime_wrapper_runs_preflight_before_daemon_exec(tmp_path: Path) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.index("Lab runtime preflight") < result.stdout.index(
        "fake daemon executed"
    )
    assert json.loads(marker.read_text(encoding="utf-8"))[:2] == [
        "lab-worker",
        "--expected-checkout-root",
    ]


def test_lab_runtime_wrapper_rejects_missing_release_generation_marker(
    tmp_path: Path,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    marker_path_for_lock(_deployment_lock_path(checkout)).unlink()

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode == 1
    assert "generation marker" in result.stderr.lower()
    assert not marker.exists()


def test_lab_runtime_bootstrap_never_processes_site_or_pth_hooks(tmp_path: Path) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    site_packages = (
        checkout
        / ".venv"
        / "lib"
        / (f"python{sys.version_info.major}.{sys.version_info.minor}")
        / "site-packages"
    )
    hook_marker = tmp_path / "preimport-hook-ran"
    (site_packages / "sitecustomize.py").write_text(
        f"from pathlib import Path\nPath({str(hook_marker)!r}).write_text('site')\n",
        encoding="utf-8",
    )
    (site_packages / "untrusted-hook.pth").write_text(
        f"import pathlib; pathlib.Path({str(hook_marker)!r}).write_text('pth')\n",
        encoding="utf-8",
    )

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode == 1
    assert "generation marker" in result.stderr.lower()
    assert not marker.exists()
    assert not hook_marker.exists()


def test_lab_runtime_wrapper_fails_while_deployment_generation_is_locked(
    tmp_path: Path,
) -> None:
    import fcntl

    checkout, executable, marker = _runtime_checkout(tmp_path)
    lock_path = _deployment_lock_path(checkout)
    lock_path.parent.mkdir(mode=0o700, exist_ok=True)
    lock_path.touch(mode=0o600)
    lock_path.chmod(0o600)
    with lock_path.open("r+b") as deployment_lock:
        fcntl.flock(deployment_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _run_wrapper(checkout, executable, marker)

    assert result.returncode != 0
    assert "deployment generation" in result.stderr.lower()
    assert not marker.exists()


def test_running_daemon_holds_one_complete_generation_against_deployment(
    tmp_path: Path,
) -> None:
    import fcntl

    checkout, executable, marker = _runtime_checkout(tmp_path)
    environment = os.environ.copy()
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        environment.pop(variable, None)
    environment.update(
        {
            "LAB_WRAPPER_MARKER": str(marker),
            "LAB_WRAPPER_HOLD_SECONDS": "1.0",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    process = subprocess.Popen(
        [
            str(checkout / ".venv" / "bin" / "python"),
            "-I",
            "-S",
            str(checkout / "scripts" / WRAPPER.name),
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--deployment-lock-path",
            str(_deployment_lock_path(checkout)),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ],
        cwd=checkout,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 10
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert marker.exists()
    with (
        _deployment_lock_path(checkout).open("r+b") as deployment_lock,
        pytest.raises(BlockingIOError),
    ):
        fcntl.flock(deployment_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, stdout + stderr


@pytest.mark.parametrize("suffix", [".pyc", ".pyo", ".so", ".dylib", ".pyd"])
def test_lab_runtime_wrapper_never_imports_rquant_when_preflight_fails(
    tmp_path: Path,
    suffix: str,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    artifact = checkout / "src" / "rquant" / f"untrusted{suffix}"
    artifact.write_bytes(b"untrusted executable")

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode != 0
    assert "preflight failed" in result.stderr.lower()
    assert not marker.exists()
    assert artifact.read_bytes() == b"untrusted executable"


def test_lab_runtime_wrapper_rejects_package_symlink_before_import(tmp_path: Path) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    external = tmp_path / "external-package"
    external.mkdir()
    (external / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    (checkout / "src" / "rquant" / "external").symlink_to(external)

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode != 0
    assert not marker.exists()
    assert (external / "__init__.py").read_text(encoding="utf-8") == "VALUE = 1\n"


def test_lab_runtime_wrapper_rejects_symlinked_virtualenv_bin_before_import(
    tmp_path: Path,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    physical_bin = checkout / ".venv" / "physical-bin"
    (checkout / ".venv" / "bin").rename(physical_bin)
    (checkout / ".venv" / "bin").symlink_to(physical_bin, target_is_directory=True)

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode != 0
    assert "physical" in result.stderr.lower()
    assert not marker.exists()


def test_lab_runtime_wrapper_rejects_executable_inode_replacement_during_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    namespace = runpy.run_path(str(WRAPPER), run_name="lab_wrapper_test")
    namespace["main"].__globals__["__file__"] = str(checkout / "scripts" / WRAPPER.name)
    original_bytes = executable.read_bytes()
    displaced = checkout / ".venv" / "bin" / "rquant.displaced"
    exec_calls: list[tuple[object, ...]] = []
    original_run = subprocess.run
    replaced = False

    def replace_during_preflight(
        *args: object,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal replaced
        result = original_run(*args, **kwargs)
        command = args[0]
        if not replaced and any(Path(str(value)).name == PREFLIGHT.name for value in command):
            executable.rename(displaced)
            executable.write_bytes(original_bytes)
            executable.chmod(0o700)
            replaced = True
        return result

    monkeypatch.setattr(subprocess, "run", replace_during_preflight)
    monkeypatch.setattr(os, "execv", lambda *args: exec_calls.append(args))
    monkeypatch.setattr(sys, "executable", str(checkout / ".venv" / "bin" / "python"))
    monkeypatch.chdir(checkout)
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        monkeypatch.delenv(variable, raising=False)

    result = namespace["main"](
        [
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--deployment-lock-path",
            str(_deployment_lock_path(checkout)),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ]
    )

    assert result == 1
    assert exec_calls == []
    assert not marker.exists()


def test_lab_runtime_wrapper_rechecks_tracked_cleanliness_after_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    namespace = runpy.run_path(str(WRAPPER), run_name="lab_wrapper_test")
    namespace["main"].__globals__["__file__"] = str(checkout / "scripts" / WRAPPER.name)
    tracked = checkout / "src" / "rquant" / "__init__.py"
    exec_calls: list[tuple[object, ...]] = []
    original_run = subprocess.run
    dirtied = False

    def dirty_after_preflight(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal dirtied
        result = original_run(*args, **kwargs)
        command = args[0]
        if not dirtied and any(Path(str(value)).name == PREFLIGHT.name for value in command):
            tracked.write_text("UNTRUSTED = True\n", encoding="utf-8")
            dirtied = True
        return result

    monkeypatch.setattr(subprocess, "run", dirty_after_preflight)
    monkeypatch.setattr(os, "execv", lambda *args: exec_calls.append(args))
    monkeypatch.setattr(sys, "executable", str(checkout / ".venv" / "bin" / "python"))
    monkeypatch.chdir(checkout)
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        monkeypatch.delenv(variable, raising=False)

    result = namespace["main"](
        [
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--deployment-lock-path",
            str(_deployment_lock_path(checkout)),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ]
    )

    assert result == 1
    assert exec_calls == []
    assert not marker.exists()


def test_lab_runtime_wrapper_rechecks_complete_checkout_after_second_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    namespace = runpy.run_path(str(WRAPPER), run_name="lab_wrapper_test")
    namespace["main"].__globals__["__file__"] = str(checkout / "scripts" / WRAPPER.name)
    tracked = checkout / "src" / "rquant" / "__init__.py"
    exec_calls: list[tuple[object, ...]] = []
    original_run = subprocess.run
    preflight_calls = 0

    def dirty_after_second_preflight(
        *args: object,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal preflight_calls
        result = original_run(*args, **kwargs)
        command = args[0]
        if any(Path(str(value)).name == PREFLIGHT.name for value in command):
            preflight_calls += 1
            if preflight_calls == 2:
                tracked.write_text("UNTRUSTED_AFTER_SECOND = True\n", encoding="utf-8")
        return result

    monkeypatch.setattr(subprocess, "run", dirty_after_second_preflight)
    monkeypatch.setattr(os, "execv", lambda *args: exec_calls.append(args))
    monkeypatch.setattr(sys, "executable", str(checkout / ".venv" / "bin" / "python"))
    monkeypatch.chdir(checkout)
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        monkeypatch.delenv(variable, raising=False)

    result = namespace["main"](
        [
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--deployment-lock-path",
            str(_deployment_lock_path(checkout)),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ]
    )

    assert preflight_calls >= 2
    assert result == 1
    assert exec_calls == []
    assert not marker.exists()


def test_lab_runtime_wrapper_ignores_fake_venv_git(
    tmp_path: Path,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    fake_marker = tmp_path / "fake-git-ran"
    fake_git = checkout / ".venv" / "bin" / "git"
    fake_git.write_text(
        f"#!/bin/sh\ntouch {fake_marker!s}\nexit 0\n",
        encoding="utf-8",
    )
    fake_git.chmod(0o700)
    (checkout / "src" / "rquant" / "__init__.py").write_text(
        "UNTRUSTED = True\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        environment.pop(variable, None)
    environment["PATH"] = f"{fake_git.parent}:{environment.get('PATH', '')}"
    environment["LAB_WRAPPER_MARKER"] = str(marker)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    result = subprocess.run(
        [
            str(checkout / ".venv" / "bin" / "python"),
            "-I",
            "-S",
            str(checkout / "scripts" / WRAPPER.name),
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--deployment-lock-path",
            str(_deployment_lock_path(checkout)),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--worker-id",
            "rquant-mac-primary",
            "--once",
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode != 0
    assert not fake_marker.exists()
    assert not marker.exists()


def test_lab_runtime_wrapper_rejects_mismatched_daemon_root(tmp_path: Path) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    environment = os.environ.copy()
    for variable in ("PYTHONHOME", "PYTHONINSPECT", "PYTHONPATH", "PYTHONSTARTUP"):
        environment.pop(variable, None)
    environment["LAB_WRAPPER_MARKER"] = str(marker)

    result = subprocess.run(
        [
            str(checkout / ".venv" / "bin" / "python"),
            "-I",
            "-S",
            str(checkout / "scripts" / WRAPPER.name),
            "--expected-checkout-root",
            str(checkout),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--deployment-lock-path",
            str(_deployment_lock_path(checkout)),
            "--",
            str(executable),
            "lab-worker",
            "--expected-checkout-root",
            str(other),
            "--trusted-git-path",
            str(TRUSTED_GIT),
            "--once",
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode != 0
    assert "checkout root" in result.stderr.lower()
    assert not marker.exists()


@pytest.mark.parametrize("script", [WRAPPER, BOOTSTRAP])
def test_lab_runtime_startup_scripts_are_stdlib_only(script: Path) -> None:
    source = script.read_text(encoding="utf-8")
    imported_roots: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", maxsplit=1)[0])

    allowed = sys.stdlib_module_names | {"__future__"}
    if script == BOOTSTRAP:
        allowed = allowed | {"rquant"}
        assert source.index("from rquant.cli import main") > source.index(
            "_run_preflight(\n            root=root"
        )
    assert imported_roots <= allowed
