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
from collections.abc import Iterator
from pathlib import Path

import pytest

from rquant.release_generation import ReleaseGenerationAuthority, marker_path_for_lock

ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / "scripts" / "run-lab-daemon.py"
PREFLIGHT = ROOT / "scripts" / "preflight-lab-runtime.py"
BOOTSTRAP = ROOT / "scripts" / "bootstrap-lab-daemon.py"
TRUSTED_GIT = Path("/usr/bin/git")
RELEASE_AUTHORITY = ROOT / "src" / "rquant" / "release_generation.py"
_ORIGINAL_OS_WALK = os.walk


@pytest.fixture(autouse=True)
def _remove_immutable_test_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    monkeypatch.setenv("RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES", "0")
    try:
        yield
    finally:
        generation_roots = [
            Path(current_root) / name
            for current_root, directory_names, _file_names in _ORIGINAL_OS_WALK(tmp_path)
            for name in directory_names
            if name.endswith(".venvs")
        ]
        for root in generation_roots:
            if root.is_symlink() or not root.is_dir():
                continue
            for current_root, _directory_names, file_names in _ORIGINAL_OS_WALK(root):
                current = Path(current_root)
                if hasattr(os, "chflags"):
                    os.chflags(current, 0)
                current.chmod(0o700)
                for name in file_names:
                    path = current / name
                    if not path.is_symlink():
                        if hasattr(os, "chflags"):
                            os.chflags(path, 0)
                        path.chmod(0o600)
            shutil.rmtree(root)


def _tiny_test_venv(checkout: Path, *, symlink_python: bool = False) -> Path:
    venv_root = checkout / ".venv"
    python = venv_root / "bin" / "python"
    python.parent.mkdir(parents=True)
    if symlink_python:
        python.symlink_to(Path(sys.executable).resolve(strict=True))
    else:
        shutil.copy2(sys.executable, python)
        python.chmod(0o700)
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    (venv_root / "pyvenv.cfg").write_text(
        f"home = {Path(sys.base_prefix) / 'bin'}\nversion = {version}\n",
        encoding="utf-8",
    )
    (venv_root / "lib" / f"python{version}" / "site-packages").mkdir(parents=True)
    python_library = Path(sys.base_prefix) / "lib" / f"libpython{version}.dylib"
    if python_library.exists():
        shutil.copy2(python_library, venv_root / "lib" / python_library.name)
    return python


def _prepare_fake_lab_runtime(checkout: Path) -> None:
    runtime = checkout / "data" / "lab-runtime"
    runtime.mkdir(parents=True, mode=0o700)
    runtime.chmod(0o700)
    directory_names = {
        "lab command spool": "commands",
        "lab claim spool": "claims",
        "lab report spool": "reports",
        "lab worker artifact root": "worker-artifacts",
        "lab final artifact root": "final-artifacts",
        "lab artifact commit spool": "artifact-commits",
        "lab daemon lock root": "locks",
        "lab finalizer state root": "finalizer-state",
        "lab readiness root": "readiness",
    }
    directory_bindings: dict[str, dict[str, object]] = {}
    for label, name in directory_names.items():
        path = runtime / name
        path.mkdir(mode=0o700)
        path.chmod(0o700)
        observed = path.stat()
        directory_bindings[label] = {
            "path": str(path),
            "device": observed.st_dev,
            "inode": observed.st_ino,
            "mode": 0o700,
        }
    database = runtime / "lab_jobs.sqlite3"
    database.write_bytes(b"")
    database.chmod(0o600)
    database_observed = database.stat()
    root_observed = runtime.stat()
    sentinel = runtime / ".prepared.json"
    sentinel.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "checkout_root": str(checkout),
                "runtime_root": str(runtime),
                "runtime_device": root_observed.st_dev,
                "runtime_inode": root_observed.st_ino,
                "runtime_authority_id": "a" * 32,
                "prepared_by_commit": "0" * 40,
                "managed_directories": directory_bindings,
                "managed_files": {
                    "lab jobs SQLite": {
                        "path": str(database),
                        "device": database_observed.st_dev,
                        "inode": database_observed.st_ino,
                        "mode": 0o600,
                        "exists": True,
                    }
                },
                "migration_sources": {},
                "prepared_at": "2026-07-28T00:00:00+00:00",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    sentinel.chmod(0o600)
    lock = runtime / ".prepared.lock"
    lock.write_bytes(b"")
    lock.chmod(0o600)


def _runtime_checkout(
    tmp_path: Path,
    *,
    symlink_python: bool = False,
    publish_generation: bool = True,
    prepare_runtime: bool = True,
) -> tuple[Path, Path, Path]:
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
        "    if os.environ.get('LAB_RUNTIME_IDENTITY_MARKER'):\n"
        "        Path(os.environ['LAB_RUNTIME_IDENTITY_MARKER']).write_text("
        "json.dumps({'executable': sys.executable, 'prefix': sys.prefix}), encoding='utf-8')\n"
        "    time.sleep(float(os.environ.get('LAB_WRAPPER_HOLD_SECONDS', '0')))\n"
        "    print('fake daemon executed', flush=True)\n",
        encoding="utf-8",
    )
    python = _tiny_test_venv(checkout, symlink_python=symlink_python)
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
    if publish_generation:
        lock_path = _deployment_lock_path(checkout)
        lock_path.parent.mkdir(mode=0o700)
        lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            authority = ReleaseGenerationAuthority(
                repo=checkout,
                lock_path=lock_path,
                lock_fd=lock_fd,
                python_path=python,
                git_path=TRUSTED_GIT,
                writable=True,
                environment_builder=lambda destination: shutil.copytree(
                    checkout / ".venv",
                    destination,
                    dirs_exist_ok=True,
                    symlinks=True,
                ),
            )
            initialization = authority.begin_initialization(target_sha=commit)
            authority.publish(
                expected_commit=commit,
                operation_id=initialization.operation_id,
                transaction_kind="initialization",
            )
            authority.complete_initialization(operation_id=initialization.operation_id)
            authority.commit_generation(
                operation_id=initialization.operation_id,
                transaction_kind="initialization",
            )
        finally:
            os.close(lock_fd)
    if prepare_runtime:
        _prepare_fake_lab_runtime(checkout)
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
    environment["LAB_RUNTIME_IDENTITY_MARKER"] = str(marker.with_suffix(".runtime.json"))
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
    daemon_argv = json.loads(marker.read_text(encoding="utf-8"))
    assert daemon_argv[:2] == [
        "lab-worker",
        "--expected-checkout-root",
    ]
    assert daemon_argv[daemon_argv.index("--deployment-operation-id") + 1]
    assert len(daemon_argv[daemon_argv.index("--deployment-environment-generation") + 1]) == 64
    runtime = json.loads(marker.with_suffix(".runtime.json").read_text(encoding="utf-8"))
    assert runtime["executable"] == runtime["prefix"] + "/bin/python"
    assert runtime["prefix"].startswith(
        str(_deployment_lock_path(checkout).parent / "checkout.venvs")
    )
    assert runtime["prefix"] != str(checkout / ".venv")


def test_lab_runtime_wrapper_missing_prepared_sentinel_has_zero_config_side_effects(
    tmp_path: Path,
) -> None:
    checkout, executable, marker = _runtime_checkout(
        tmp_path,
        publish_generation=False,
        prepare_runtime=False,
    )
    future_data = tmp_path / "future-data"
    future_parquet = tmp_path / "future-parquet"
    future_logs = tmp_path / "future-logs"
    dotenv = checkout / ".env"
    dotenv.write_text(
        "\n".join(
            (
                f"export data_dir = '{future_data}' # Settings-compatible path",
                f"DUCKDB_PATH={future_data / 'rquant.duckdb'}",
                f"PARQUET_DIR={future_parquet}",
                f"LOG_DIR={future_logs}",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    dotenv.chmod(0o600)
    index = checkout / ".git" / "index"
    index_before = index.read_bytes()
    index_stat_before = index.stat()
    lock_root = _deployment_lock_path(checkout).parent
    tree_before = tuple(sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*")))

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode != 0
    assert "prepared sentinel" in result.stderr.lower()
    assert not marker.exists()
    assert not future_data.exists()
    assert not future_parquet.exists()
    assert not future_logs.exists()
    assert not lock_root.exists()
    assert index.read_bytes() == index_before
    index_stat_after = index.stat()
    assert (index_stat_after.st_dev, index_stat_after.st_ino) == (
        index_stat_before.st_dev,
        index_stat_before.st_ino,
    )
    assert index_stat_after.st_mtime_ns == index_stat_before.st_mtime_ns
    assert (
        tuple(sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*")))
        == tree_before
    )


def test_lab_runtime_wrapper_reads_provisional_handoff_for_marker_operation(
    tmp_path: Path,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    lock_path = _deployment_lock_path(checkout)
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    lock_fd = os.open(lock_path, os.O_RDWR)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    labels = (
        "com.roxor.rquant-lab-scheduler",
        "com.roxor.rquant-lab-worker",
        "com.roxor.rquant-lab-finalizer",
    )
    handoff_a = "a" * 32
    authority = ReleaseGenerationAuthority(
        repo=checkout,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=checkout / ".venv" / "bin" / "python",
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=lambda destination: shutil.copytree(
            checkout / ".venv",
            destination,
            dirs_exist_ok=True,
            symlinks=True,
        ),
    )
    current = authority.verify(expected_commit=commit)
    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation=current.content_hash(),
        previous_generation_id=current.environment_generation_id,
        handoff_operation_id=handoff_a,
        handoff_labels=labels,
    )
    authority.invalidate()
    authority.update_deployment_intent(operation_id=intent.operation_id, stage="timers_restored")
    authority.publish(
        expected_commit=commit,
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    authority.update_deployment_intent(operation_id=intent.operation_id, stage="completed")
    authority.commit_generation(
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    os.close(lock_fd)
    provisional = {
        "schema_version": 1,
        "operation_id": handoff_a,
        "labels": list(labels),
        "loaded_labels": list(labels),
        "stopped_labels": list(labels),
        "restarted_labels": [],
        "stage": "restarting",
    }
    operation_path = lock_path.with_name(f"{lock_path.stem}.lab-handoff.{handoff_a}.json")
    operation_path.write_text(json.dumps(provisional), encoding="utf-8")
    operation_path.chmod(0o600)
    active_path = lock_path.with_name(f"{lock_path.stem}.lab-handoff.json")
    active_path.write_text(
        json.dumps(
            {
                **provisional,
                "operation_id": "b" * 32,
                "stage": "stopping",
            }
        ),
        encoding="utf-8",
    )
    active_path.chmod(0o600)

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode == 0, result.stdout + result.stderr
    assert marker.is_file()


def test_rollback_rebinds_marker_to_superseding_handoff_for_normal_wrapper(
    tmp_path: Path,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    lock_path = _deployment_lock_path(checkout)
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    lock_fd = os.open(lock_path, os.O_RDWR)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    labels = (
        "com.roxor.rquant-lab-scheduler",
        "com.roxor.rquant-lab-worker",
        "com.roxor.rquant-lab-finalizer",
    )
    target_handoff = "a" * 32
    rollback_handoff = "c" * 32
    authority = ReleaseGenerationAuthority(
        repo=checkout,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=checkout / ".venv" / "bin" / "python",
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=lambda destination: shutil.copytree(
            checkout / ".venv",
            destination,
            dirs_exist_ok=True,
            symlinks=True,
        ),
    )
    current = authority.verify(expected_commit=commit)
    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation=current.content_hash(),
        previous_generation_id=current.environment_generation_id,
        handoff_operation_id=target_handoff,
        handoff_labels=labels,
    )
    authority.rebind_deployment_handoff(
        operation_id=intent.operation_id,
        handoff_operation_id=rollback_handoff,
        handoff_labels=labels,
    )
    authority.invalidate()
    authority.update_deployment_intent(operation_id=intent.operation_id, stage="timers_restored")
    published = authority.publish(
        expected_commit=commit,
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    authority.update_deployment_intent(operation_id=intent.operation_id, stage="completed")
    authority.commit_generation(
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    os.close(lock_fd)
    proof = {
        "schema_version": 1,
        "operation_id": rollback_handoff,
        "labels": list(labels),
        "loaded_labels": list(labels),
        "stopped_labels": list(labels),
        "restarted_labels": list(labels),
        "stage": "completed",
        "generation_operation_id": intent.operation_id,
        "environment_generation_id": published.environment_generation_id,
        "code_sha": commit,
    }
    proof_path = lock_path.with_name(
        f"{lock_path.stem}.lab-handoff.{rollback_handoff}.completed.json"
    )
    proof_path.write_text(json.dumps(proof), encoding="utf-8")
    proof_path.chmod(0o600)
    active_path = lock_path.with_name(f"{lock_path.stem}.lab-handoff.json")
    active_path.write_text(
        json.dumps({**proof, "operation_id": target_handoff, "stage": "restarting"}),
        encoding="utf-8",
    )
    active_path.chmod(0o600)

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode == 0, result.stdout + result.stderr
    assert marker.is_file()


def test_lab_runtime_wrapper_executes_verified_uv_style_python_symlink(
    tmp_path: Path,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path, symlink_python=True)

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode == 0, result.stdout + result.stderr
    runtime = json.loads(marker.with_suffix(".runtime.json").read_text(encoding="utf-8"))
    assert Path(runtime["executable"]).is_symlink()


def test_lab_runtime_wrapper_rejects_missing_release_generation_marker(
    tmp_path: Path,
) -> None:
    checkout, executable, marker = _runtime_checkout(tmp_path)
    marker_path_for_lock(_deployment_lock_path(checkout)).unlink()

    result = _run_wrapper(checkout, executable, marker)

    assert result.returncode == 1
    assert "generation marker" in result.stderr.lower()
    assert not marker.exists()


def test_lab_runtime_bootstrap_ignores_hooks_added_to_mutable_source_venv(tmp_path: Path) -> None:
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

    assert result.returncode == 0, result.stderr
    assert marker.exists()
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


def test_lab_runtime_wrapper_readonly_git_preserves_index_and_disables_optional_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, _executable, _marker = _runtime_checkout(
        tmp_path,
        publish_generation=False,
    )
    namespace = runpy.run_path(str(WRAPPER))
    git_path, git_identity = namespace["_require_trusted_git"](TRUSTED_GIT)
    index = checkout / ".git" / "index"
    before = (index.read_bytes(), index.stat())
    original_run = subprocess.run
    environments: list[dict[str, str]] = []

    def capture_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        command = args[0]
        if isinstance(command, list) and command and command[0] == str(TRUSTED_GIT):
            environment = kwargs.get("env")
            assert isinstance(environment, dict)
            environments.append(environment)
        return original_run(*args, **kwargs)

    monkeypatch.setattr(subprocess, "run", capture_run)

    assert namespace["_git_commit"](
        checkout,
        git_path=git_path,
        git_identity=git_identity,
    )
    after = index.stat()
    assert environments
    assert all(environment["GIT_OPTIONAL_LOCKS"] == "0" for environment in environments)
    assert index.read_bytes() == before[0]
    assert (after.st_ino, after.st_mtime_ns) == (before[1].st_ino, before[1].st_mtime_ns)


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
