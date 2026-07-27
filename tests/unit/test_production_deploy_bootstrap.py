from __future__ import annotations

import fcntl
import json
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
    commit_path_for_lock,
    marker_path_for_lock,
)

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = ROOT / "scripts" / "bootstrap-production-deploy.py"
AUTHORITY = ROOT / "src" / "rquant" / "release_generation.py"
PRODUCTION_DEPLOYER = ROOT / "src" / "rquant" / "ops" / "production_deploy.py"
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
    real_deployer: bool = False,
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
    if real_deployer:
        shutil.copy2(PRODUCTION_DEPLOYER, ops / PRODUCTION_DEPLOYER.name)
    else:
        (ops / "production_deploy.py").write_text(
            "from __future__ import annotations\n"
            "import fcntl, os, time\n"
            "from pathlib import Path\n"
            "if os.environ.get('DEPLOY_LOCK'):\n"
            "    lock_fd = os.open(os.environ['DEPLOY_LOCK'], os.O_RDONLY)\n"
            "    try:\n"
            "        fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)\n"
            "    except BlockingIOError:\n"
            "        state = 'locked'\n"
            "    else:\n"
            "        state = 'unlocked'\n"
            "    finally:\n"
            "        os.close(lock_fd)\n"
            "    if os.environ.get('IMPORT_MARKER'):\n"
            "        Path(os.environ['IMPORT_MARKER']).write_text(state, encoding='utf-8')\n"
            "def main(argv=None):\n"
            "    if os.environ.get('RUN_MARKER'):\n"
            "        Path(os.environ['RUN_MARKER']).write_text('ran', encoding='utf-8')\n"
            "    time.sleep(float(os.environ.get('DEPLOY_HOLD_SECONDS', '0')))\n"
            "    return int(os.environ.get('DEPLOY_EXIT', '0'))\n",
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
            authority = ReleaseGenerationAuthority(
                repo=checkout,
                lock_path=lock_path,
                lock_fd=lock_fd,
                python_path=python,
                git_path=TRUSTED_GIT,
                writable=True,
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


def _begin_intent(
    checkout: Path,
    python: Path,
    lock_path: Path,
    *,
    previous: str,
    target: str,
    target_ref: str | None = None,
) -> str:
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
        )
        intent = authority.begin_deployment_intent(
            previous_sha=previous,
            target_sha=target,
            target_ref=target_ref or target,
            changed_files=("src/rquant/preflight.py",),
            restart_services=(),
            active_services=(),
            active_timers=(),
        )
        return intent.operation_id
    finally:
        os.close(lock_fd)


def _command(
    checkout: Path,
    python: Path,
    lock_path: Path,
    *,
    target: str = "v0.99.0",
    mode: str = "deploy",
    recovery_action: str | None = None,
    operation_id: str | None = None,
    inherited_lock_fd: int | None = None,
    finalize_phase: str = "publish",
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
    elif mode == "finalize":
        command.append("--finalize-generation")
        command.extend(
            [
                "--finalize-action",
                str(recovery_action),
                "--finalize-phase",
                finalize_phase,
                "--operation-id",
                str(operation_id),
                "--inherited-lock-fd",
                str(inherited_lock_fd),
            ]
        )
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


def test_initialize_generation_cannot_be_replayed_after_marker_deletion(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        mode="initialize",
    )
    first = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )
    assert first.returncode == 0, first.stderr
    marker_path_for_lock(lock_path).unlink()

    replay = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert replay.returncode == 2
    assert "already completed" in replay.stderr
    assert not marker_path_for_lock(lock_path).exists()


def test_initialize_generation_recovers_completed_transaction_before_commit_record(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
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
        )
        initialization = authority.begin_initialization(target_sha=commit)
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )
        authority.complete_initialization(operation_id=initialization.operation_id)
    finally:
        os.close(lock_fd)

    recovered = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="initialize",
        ),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert recovered.returncode == 0, recovered.stderr
    assert commit_path_for_lock(lock_path).is_file()

    replay = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="initialize",
        ),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )
    assert replay.returncode == 2
    assert "already completed" in replay.stderr


def test_initialize_generation_refuses_replaying_a_committed_initialization(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path)
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        mode="initialize",
    )

    migrated = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )
    replay = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert migrated.returncode == 2
    assert "already completed" in migrated.stderr
    assert replay.returncode == 2
    assert "already completed" in replay.stderr
    assert marker_path_for_lock(lock_path).is_file()


@pytest.mark.parametrize("recovery_action", ["resume", "rollback"])
def test_recover_generation_republishes_only_exact_verified_target(
    tmp_path: Path,
    recovery_action: str,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, real_deployer=True)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=commit,
        target=commit,
    )
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
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    commit = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=commit,
    )
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
    assert failed.returncode == 1
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
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
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
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
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


def test_recovery_target_remains_pinned_when_origin_main_advances(tmp_path: Path) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
    _git(checkout, "reset", "--hard", target)
    (checkout / "uv.lock").write_text("version = 3\n", encoding="utf-8")
    _git(checkout, "add", "uv.lock")
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "later origin generation",
    )
    later = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "update-ref", "refs/remotes/origin/main", later)
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
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == target


def test_target_checkout_authority_publishes_its_marker_schema(tmp_path: Path) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path)
    authority_path = checkout / "src" / "rquant" / "release_generation.py"
    authority_path.write_text(
        authority_path.read_text(encoding="utf-8").replace(
            "MARKER_SCHEMA_VERSION = 1",
            "MARKER_SCHEMA_VERSION = 2",
        ),
        encoding="utf-8",
    )
    _git(checkout, "add", str(authority_path.relative_to(checkout)))
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "marker schema v2",
    )
    target = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "update-ref", "refs/remotes/origin/main", target)
    _git(checkout, "reset", "--hard", previous)
    operation_id = _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
    update_fd = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(update_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        update_authority = ReleaseGenerationAuthority(
            repo=checkout,
            lock_path=lock_path,
            lock_fd=update_fd,
            python_path=python,
            git_path=TRUSTED_GIT,
            writable=True,
        )
        update_authority.update_deployment_intent(
            operation_id=operation_id,
            stage="timers_restored",
        )
    finally:
        os.close(update_fd)
    marker_path_for_lock(lock_path).unlink()
    _git(checkout, "merge", "--ff-only", target)
    lock_fd = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(
            _command(
                checkout,
                python,
                lock_path,
                target=target,
                mode="finalize",
                recovery_action="resume",
                operation_id=operation_id,
                inherited_lock_fd=lock_fd,
            ),
            cwd=checkout,
            pass_fds=(lock_fd,),
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        os.close(lock_fd)

    assert result.returncode == 0, result.stderr
    marker = json.loads(marker_path_for_lock(lock_path).read_text(encoding="utf-8"))
    assert marker["schema_version"] == 2


def test_rollback_uses_previous_checkout_marker_schema(tmp_path: Path) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    authority_path = checkout / "src" / "rquant" / "release_generation.py"
    authority_path.write_text(
        authority_path.read_text(encoding="utf-8").replace(
            "MARKER_SCHEMA_VERSION = 1",
            "MARKER_SCHEMA_VERSION = 2",
        ),
        encoding="utf-8",
    )
    _git(checkout, "add", str(authority_path.relative_to(checkout)))
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "marker schema v2",
    )
    target = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "update-ref", "refs/remotes/origin/main", target)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
    _git(checkout, "merge", "--ff-only", target)
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
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == previous
    marker = json.loads(marker_path_for_lock(lock_path).read_text(encoding="utf-8"))
    assert marker["schema_version"] == 1


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
