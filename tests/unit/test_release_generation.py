from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from rquant.release_generation import (
    DeploymentIntent,
    ReleaseGenerationAuthority,
    ReleaseGenerationError,
    commit_path_for_lock,
    environment_manifest_path_for_lock,
    environment_root_for_lock,
    environment_selector_path_for_lock,
    initialization_path_for_lock,
    intent_path_for_lock,
    marker_path_for_lock,
)

_ORIGINAL_OS_WALK = os.walk

TRUSTED_GIT = Path("/usr/bin/git")


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


def _generation(tmp_path: Path) -> tuple[Path, Path, str, Path]:
    repo = tmp_path / "rquant"
    package = repo / "src" / "rquant"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (repo / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.0"\n',
        encoding="utf-8",
    )
    (repo / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    venv = repo / ".venv"
    python = venv / "bin" / "python"
    python.parent.mkdir(parents=True)
    shutil.copy2(sys.executable, python)
    python.chmod(0o700)
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    library = Path(sys.base_prefix) / "lib" / f"libpython{python_version}.dylib"
    if library.exists():
        (venv / "lib").mkdir()
        shutil.copy2(library, venv / "lib" / library.name)
    (venv / "pyvenv.cfg").write_text(
        f"home = {Path(sys.base_prefix) / 'bin'}\nversion = {python_version}\n",
        encoding="utf-8",
    )
    site_packages = (
        venv / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    )
    site_packages.mkdir(parents=True)
    (repo / ".gitignore").write_text("/.venv\n", encoding="utf-8")
    subprocess.run([str(TRUSTED_GIT), "init", "-q"], cwd=repo, check=True)
    subprocess.run([str(TRUSTED_GIT), "add", "."], cwd=repo, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "generation",
        ],
        cwd=repo,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    lock_root = tmp_path / ".rquant-deploy"
    lock_root.mkdir(mode=0o700)
    lock_path = lock_root / "rquant.lock"
    return repo, lock_path, commit, python


def _authority(
    repo: Path,
    lock_path: Path,
    lock_fd: int,
    python: Path,
    *,
    mutation_hook: object | None = None,
    gc_grace_seconds: float | None = None,
    minimum_free_bytes: int | None = None,
    uv_path: Path | None = None,
) -> ReleaseGenerationAuthority:
    def copy_fixture_environment(destination: Path) -> None:
        shutil.copytree(repo / ".venv", destination, dirs_exist_ok=True, symlinks=True)

    return ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        mutation_hook=mutation_hook,
        gc_grace_seconds=gc_grace_seconds,
        minimum_free_bytes=minimum_free_bytes,
        uv_path=uv_path,
        environment_builder=(None if uv_path is not None else copy_fixture_environment),
    )


def _publish_initialized(
    authority: ReleaseGenerationAuthority,
    *,
    commit: str,
) -> object:
    initialization = authority.begin_initialization(target_sha=commit)
    marker = authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)
    authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    return marker


def test_release_generation_marker_binds_checkout_lock_python_and_venv(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

    marker = _publish_initialized(authority, commit=commit)
    verified = authority.verify(expected_commit=commit)

    assert verified == marker
    assert marker.commit == commit
    assert marker.uv_lock_sha256
    assert marker.package_version == "0.99.0"
    assert marker.python_abi
    assert marker.venv_identity.inode > 0
    assert marker_path_for_lock(lock_path).is_file()
    os.close(lock_fd)


def test_real_minimal_uv_venv_is_accepted_for_initialization_and_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uv_name = shutil.which("uv")
    assert uv_name is not None
    uv_path = Path(uv_name).resolve(strict=True)
    repo, lock_path, commit, _python = _generation(tmp_path)
    package = repo / "src" / "rquant"
    (package / "cli.py").write_text(
        "def main():\n    print('tiny-rquant-ok')\n",
        encoding="utf-8",
    )
    pyproject = repo / "pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8")
        + (
            '\n[project.scripts]\nrquant = "rquant.cli:main"\n'
            '\n[build-system]\nrequires = []\nbuild-backend = "backend"\n'
            'backend-path = ["."]\n'
        ),
        encoding="utf-8",
    )
    (repo / "backend.py").write_text(
        "from pathlib import Path\n"
        "from zipfile import ZIP_DEFLATED, ZipFile\n"
        "def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):\n"
        "    del config_settings, metadata_directory\n"
        "    name = 'rquant-0.99.0-py3-none-any.whl'\n"
        "    target = Path(wheel_directory) / name\n"
        "    dist = 'rquant-0.99.0.dist-info'\n"
        "    with ZipFile(target, 'w', ZIP_DEFLATED) as wheel:\n"
        "        wheel.write('src/rquant/__init__.py', 'rquant/__init__.py')\n"
        "        wheel.write('src/rquant/cli.py', 'rquant/cli.py')\n"
        "        wheel.writestr(dist + '/METADATA', "
        "'Metadata-Version: 2.1\\nName: rquant\\nVersion: 0.99.0\\n')\n"
        "        wheel.writestr(dist + '/WHEEL', "
        "'Wheel-Version: 1.0\\nGenerator: rquant-test\\nRoot-Is-Purelib: true\\n'"
        "'Tag: py3-none-any\\n')\n"
        "        wheel.writestr(dist + '/entry_points.txt', "
        "'[console_scripts]\\nrquant = rquant.cli:main\\n')\n"
        "        wheel.writestr(dist + '/RECORD', '')\n"
        "    return name\n"
        "build_editable = build_wheel\n",
        encoding="utf-8",
    )
    shutil.rmtree(repo / ".venv")
    cache = tmp_path / "uv-cache"
    monkeypatch.setenv("UV_CACHE_DIR", str(cache))
    subprocess.run(
        [str(uv_path), "venv", "--python", sys.executable, str(repo / ".venv")],
        cwd=repo,
        check=True,
        env={**os.environ, "UV_CACHE_DIR": str(cache)},
        capture_output=True,
        text=True,
    )
    (repo / "uv.lock").unlink()
    subprocess.run(
        [str(uv_path), "lock", "--python", sys.executable],
        cwd=repo,
        check=True,
        env={**os.environ, "UV_CACHE_DIR": str(cache)},
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "add",
            "backend.py",
            "pyproject.toml",
            "src/rquant/cli.py",
            "uv.lock",
        ],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "lock real uv environment",
        ],
        cwd=repo,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    python = repo / ".venv" / "bin" / "python"
    assert python.is_symlink()
    assert (repo / ".venv" / "bin" / "python3").readlink() == Path("python")
    assert sum(path.lstat().st_size for path in (repo / ".venv").rglob("*")) < 1_000_000
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python, uv_path=uv_path)

    initialized = _publish_initialized(authority, commit=commit)
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha="e" * 40,
        target_ref="e" * 40,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="timers_restored",
    )
    deployed = authority.publish(
        expected_commit=commit,
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )

    assert initialized.environment_generation_id != deployed.environment_generation_id
    assert deployed.previous_generation_id == initialized.environment_generation_id
    launcher = Path(deployed.venv_path) / "bin" / "rquant"
    executed = subprocess.run(
        [str(launcher)],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    assert executed.stdout.strip() == "tiny-rquant-ok"
    launcher_payload = launcher.read_bytes()
    assert launcher_payload.startswith(b"#!")
    assert b".building" not in launcher_payload
    assert str(repo / ".venv").encode() not in launcher_payload
    manifest = json.loads(
        environment_manifest_path_for_lock(
            lock_path,
            deployed.environment_generation_id,
        ).read_text(encoding="utf-8")
    )
    assert manifest["uv_binding"]["physical_path"] == str(uv_path)
    assert manifest["uv_binding"]["sha256"] == hashlib.sha256(uv_path.read_bytes()).hexdigest()
    os.close(lock_fd)


def test_console_entry_points_are_rebound_from_staging_to_final_generation(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def console_entry_point_environment(destination: Path) -> None:
        shutil.copytree(repo / ".venv", destination, dirs_exist_ok=True, symlinks=True)
        launcher = destination / "bin" / "rquant"
        launcher.write_text(
            f"#!{destination / 'bin' / 'python'}\nfrom rquant.cli import main\nmain()\n",
            encoding="utf-8",
        )
        launcher.chmod(0o700)

    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=console_entry_point_environment,
    )
    try:
        marker = _publish_initialized(authority, commit=commit)
    finally:
        os.close(lock_fd)

    generation = Path(marker.venv_path)
    launcher = generation / "bin" / "rquant"
    assert launcher.read_text(encoding="utf-8").splitlines()[0] == (
        f"#!{generation / 'bin' / 'python'}"
    )
    for path in (generation / "bin").iterdir():
        if path.is_file() and not path.is_symlink():
            payload = path.read_bytes()
            assert b".building" not in payload
            assert str(repo / ".venv").encode() not in payload


def test_environment_builder_rejects_non_whitelisted_symlink(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    external = tmp_path / "external-module.so"
    external.write_bytes(b"do-not-touch")
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unsafe_builder(destination: Path) -> None:
        shutil.copytree(repo / ".venv", destination, dirs_exist_ok=True, symlinks=True)
        injected = destination / "lib" / "python3.12" / "site-packages" / "evil.so"
        injected.parent.mkdir(parents=True, exist_ok=True)
        injected.symlink_to(external)

    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=unsafe_builder,
    )
    initialization = authority.begin_initialization(target_sha=commit)
    try:
        with pytest.raises(ReleaseGenerationError, match="unsafe symlink"):
            authority.publish(
                expected_commit=commit,
                operation_id=initialization.operation_id,
                transaction_kind="initialization",
            )
    finally:
        os.close(lock_fd)

    assert external.read_bytes() == b"do-not-touch"
    assert list(environment_root_for_lock(lock_path).iterdir()) == []
    assert not lock_path.with_name(f"{lock_path.stem}.environment.json").exists()


def test_release_generation_rejects_uv_lock_but_ignores_mutable_source_venv_drift(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)

    (repo / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    with pytest.raises(ReleaseGenerationError, match="uv.lock"):
        authority.verify(expected_commit=commit)

    (repo / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    displaced = tmp_path / "old-venv"
    (repo / ".venv").rename(displaced)
    (repo / ".venv").mkdir()
    assert authority.verify(expected_commit=commit).commit == commit
    os.close(lock_fd)


def test_interrupted_atomic_marker_publication_leaves_marker_absent(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "marker_temp_fsynced":
            raise KeyboardInterrupt

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )

    with pytest.raises(KeyboardInterrupt):
        initialization = authority.begin_initialization(target_sha=commit)
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    assert not marker_path_for_lock(lock_path).exists()
    os.close(lock_fd)


def test_invalidated_generation_cannot_be_verified_until_republished(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)

    authority.invalidate()

    with pytest.raises(ReleaseGenerationError, match="marker"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_release_generation_rejects_tracked_source_drift(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    (repo / "src" / "rquant" / "__init__.py").write_text(
        "UNTRUSTED = True\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseGenerationError, match="tracked checkout"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_release_generation_marker_handles_short_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    real_write: Callable[[int, bytes], int] = os.write
    writes: list[int] = []

    def short_write(descriptor: int, payload: bytes) -> int:
        chunk = payload[: max(1, len(payload) // 3)]
        writes.append(len(chunk))
        return real_write(descriptor, chunk)

    monkeypatch.setattr("rquant.release_generation.os.write", short_write)

    initialization = authority.begin_initialization(target_sha=commit)
    published = authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)
    authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )

    assert len(writes) > 1
    assert authority.verify(expected_commit=commit) == published
    os.close(lock_fd)


def test_release_generation_does_not_publish_unverified_temporary_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

    def reject_temporary_content(*_args: object, **_kwargs: object) -> None:
        raise ReleaseGenerationError("temporary release marker content mismatch")

    monkeypatch.setattr(
        "rquant.release_generation._verify_temporary_payload",
        reject_temporary_content,
    )

    with pytest.raises(ReleaseGenerationError, match="temporary release marker"):
        initialization = authority.begin_initialization(target_sha=commit)
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    assert not marker_path_for_lock(lock_path).exists()
    os.close(lock_fd)


def test_deployment_intent_pins_plan_before_marker_invalidation(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    marker = _publish_initialized(authority, commit=commit)

    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha="b" * 40,
        target_ref="v0.99.1",
        changed_files=("src/rquant/monitor.py",),
        restart_services=("rquant-monitor.service",),
        active_services=("rquant-monitor.service",),
        active_timers=("rquant-monitor.timer",),
    )
    authority.invalidate()

    persisted = authority.read_deployment_intent()
    assert persisted == intent
    assert persisted.marker_generation == marker.content_hash()
    assert persisted.stage == "planned"
    assert intent_path_for_lock(lock_path).is_file()
    assert not marker_path_for_lock(lock_path).exists()
    os.close(lock_fd)


def test_initialization_sentinel_cannot_be_recreated_by_deleting_marker(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

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
    marker_path_for_lock(lock_path).unlink()

    with pytest.raises(ReleaseGenerationError, match="already completed"):
        authority.begin_initialization(target_sha=commit)

    sentinel = initialization_path_for_lock(lock_path)
    assert sentinel.is_file()
    assert (
        DeploymentIntent.from_payload(json.loads(sentinel.read_text(encoding="utf-8"))).stage
        == "completed"
    )
    os.close(lock_fd)


def test_marker_is_rejected_until_intent_and_commit_record_are_complete(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    initialization = authority.begin_initialization(target_sha=commit)

    marker = authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )

    assert marker.operation_id == initialization.operation_id
    assert not commit_path_for_lock(lock_path).exists()
    with pytest.raises(ReleaseGenerationError, match="transaction.*completed"):
        authority.verify(expected_commit=commit)

    authority.complete_initialization(operation_id=initialization.operation_id)
    with pytest.raises(ReleaseGenerationError, match="commit record"):
        authority.verify(expected_commit=commit)

    authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    assert authority.verify(expected_commit=commit) == marker
    os.close(lock_fd)


def test_deployment_marker_requires_completed_launchd_handoff(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    initialized = _publish_initialized(authority, commit=commit)
    labels = (
        "com.roxor.rquant-lab-scheduler",
        "com.roxor.rquant-lab-worker",
        "com.roxor.rquant-lab-finalizer",
    )
    handoff_operation = "d" * 32
    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation=initialized.content_hash(),
        previous_generation_id=initialized.environment_generation_id,
        handoff_operation_id=handoff_operation,
        handoff_labels=labels,
    )
    authority.invalidate()
    authority.update_deployment_intent(
        operation_id=intent.operation_id,
        stage="timers_restored",
    )
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
    handoff_path = lock_path.with_name(f"{lock_path.stem}.lab-handoff.{handoff_operation}.json")
    payload = {
        "schema_version": 1,
        "operation_id": handoff_operation,
        "labels": list(labels),
        "loaded_labels": list(labels),
        "stopped_labels": list(labels),
        "restarted_labels": [],
        "stage": "restarting",
    }
    handoff_path.write_text(json.dumps(payload), encoding="utf-8")
    handoff_path.chmod(0o600)

    with pytest.raises(ReleaseGenerationError, match="handoff is not completed"):
        authority.verify(expected_commit=commit)
    authority.verify(expected_commit=commit, provisional_handoff_label=labels[0])

    payload["restarted_labels"] = list(labels)
    payload["stage"] = "completed"
    payload["generation_operation_id"] = intent.operation_id
    payload["environment_generation_id"] = published.environment_generation_id
    payload["code_sha"] = published.commit
    completed_path = handoff_path.with_name(
        f"{lock_path.stem}.lab-handoff.{handoff_operation}.completed.json"
    )
    completed_path.write_text(json.dumps(payload), encoding="utf-8")
    completed_path.chmod(0o600)
    authority.verify(expected_commit=commit)

    active_handoff_path = lock_path.with_name(f"{lock_path.stem}.lab-handoff.json")
    active_handoff_path.write_text(
        json.dumps(
            {
                **payload,
                "operation_id": "e" * 32,
                "restarted_labels": [],
                "stage": "stopping",
            }
        ),
        encoding="utf-8",
    )
    active_handoff_path.chmod(0o600)
    authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_generation_environment_build_timeout_kills_uv_process_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uv_name = shutil.which("uv")
    assert uv_name is not None
    uv_path = Path(uv_name).resolve(strict=True)
    repo, lock_path, _commit, _python = _generation(tmp_path)
    uv_cache = tmp_path / "uv-cache"
    monkeypatch.setenv("UV_CACHE_DIR", str(uv_cache))
    descendant_marker = tmp_path / "uv-builder-descendant-survived"
    pyproject = repo / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "rquant"\nversion = "0.99.0"\n'
        '\n[build-system]\nrequires = []\nbuild-backend = "backend"\n'
        'backend-path = ["."]\n',
        encoding="utf-8",
    )
    (repo / "backend.py").write_text(
        "import subprocess, sys, time\n"
        "def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):\n"
        "    del wheel_directory, config_settings, metadata_directory\n"
        "    subprocess.Popen([sys.executable, '-c', "
        f'"import pathlib,time;time.sleep(2);pathlib.Path({str(descendant_marker)!r})'
        ".write_text('alive')\"])\n"
        "    time.sleep(30)\n"
        "build_editable = build_wheel\n",
        encoding="utf-8",
    )
    (repo / "uv.lock").unlink()
    subprocess.run(
        [str(uv_path), "lock", "--python", sys.executable],
        cwd=repo,
        env={**os.environ, "UV_CACHE_DIR": str(uv_cache)},
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    subprocess.run(
        [str(TRUSTED_GIT), "add", "backend.py", "pyproject.toml", "uv.lock"],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "add hanging tiny build backend",
        ],
        cwd=repo,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    python = repo / ".venv" / "bin" / "python"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        uv_path=uv_path,
        command_timeout_seconds=1.0,
        overall_deadline_monotonic=time.monotonic() + 1.5,
    )
    initialization = authority.begin_initialization(target_sha=commit)

    with pytest.raises(ReleaseGenerationError, match="timed out"):
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )
    time.sleep(2.2)

    assert not descendant_marker.exists()
    os.close(lock_fd)


def test_environment_generation_is_immutable_and_content_bound(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

    marker = _publish_initialized(authority, commit=commit)
    selected_python = Path(marker.python_path)

    assert selected_python.is_file()
    assert not selected_python.is_symlink()
    assert not selected_python.is_relative_to(repo / ".venv")
    assert environment_selector_path_for_lock(lock_path).is_file()

    selected_python.chmod(0o700)
    with selected_python.open("ab") as handle:
        handle.write(b"environment-drift")
    selected_python.chmod(0o500)
    with pytest.raises(ReleaseGenerationError, match="environment generation"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_environment_selector_is_not_switched_when_generation_copy_is_interrupted(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    first = _publish_initialized(authority, commit=commit)
    selector = environment_selector_path_for_lock(lock_path)
    before = selector.read_bytes()

    authority.invalidate()
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation=first.content_hash(),
        previous_generation_id=first.environment_generation_id,
    )
    for stage in (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
    ):
        deployment = authority.update_deployment_intent(
            operation_id=deployment.operation_id,
            stage=stage,
        )

    def interrupt(stage: str) -> None:
        if stage == "environment_staged":
            raise KeyboardInterrupt

    interrupted = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    with pytest.raises(KeyboardInterrupt):
        interrupted.publish(
            expected_commit=commit,
            operation_id=deployment.operation_id,
            transaction_kind="deployment",
        )

    assert selector.read_bytes() == before
    os.close(lock_fd)


def test_environment_generation_can_resume_after_rename_before_manifest(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "environment_generation_ready":
            raise KeyboardInterrupt

    interrupted = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    initialization = interrupted.begin_initialization(target_sha=commit)
    with pytest.raises(KeyboardInterrupt):
        interrupted.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    generations = [
        path
        for path in environment_root_for_lock(lock_path).iterdir()
        if not path.name.startswith(".")
    ]
    assert len(generations) == 1
    assert not environment_selector_path_for_lock(lock_path).exists()

    recovered = _authority(repo, lock_path, lock_fd, python)
    marker = recovered.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    recovered.complete_initialization(operation_id=initialization.operation_id)
    recovered.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )

    assert recovered.verify(expected_commit=commit) == marker
    os.close(lock_fd)


def test_commit_record_is_the_only_generation_acceptance_boundary(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "before_generation_commit":
            raise KeyboardInterrupt

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    initialization = authority.begin_initialization(target_sha=commit)
    authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)

    with pytest.raises(KeyboardInterrupt):
        authority.commit_generation(
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    with pytest.raises(ReleaseGenerationError, match="commit record"):
        authority.verify(expected_commit=commit)
    recovered = _authority(repo, lock_path, lock_fd, python)
    recovered.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    assert recovered.verify(expected_commit=commit).operation_id == initialization.operation_id
    os.close(lock_fd)


def test_crash_after_commit_record_publication_leaves_an_accepted_generation(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "generation_committed":
            raise KeyboardInterrupt

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    initialization = authority.begin_initialization(target_sha=commit)
    authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)

    with pytest.raises(KeyboardInterrupt):
        authority.commit_generation(
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    verifier = _authority(repo, lock_path, lock_fd, python)
    assert verifier.verify(expected_commit=commit).operation_id == initialization.operation_id
    os.close(lock_fd)


def test_generation_commit_retry_is_idempotent(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    initialization = authority.begin_initialization(target_sha=commit)
    authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)

    first = authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    before = commit_path_for_lock(lock_path).read_bytes()
    second = authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )

    assert second == first
    assert commit_path_for_lock(lock_path).read_bytes() == before
    os.close(lock_fd)


def test_marker_publication_before_transaction_completion_is_never_accepted(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "marker_published":
            raise KeyboardInterrupt

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    initialization = authority.begin_initialization(target_sha=commit)

    with pytest.raises(KeyboardInterrupt):
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    assert marker_path_for_lock(lock_path).is_file()
    assert not commit_path_for_lock(lock_path).exists()
    with pytest.raises(ReleaseGenerationError, match="transaction.*completed"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_selector_switch_without_marker_never_accepts_the_new_environment(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    original = _publish_initialized(authority, commit=commit)
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    deployment = authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="timers_restored",
    )

    def interrupt(stage: str) -> None:
        if stage == "environment_selector_published":
            raise KeyboardInterrupt

    interrupted = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    with pytest.raises(KeyboardInterrupt):
        interrupted.publish(
            expected_commit=commit,
            operation_id=deployment.operation_id,
            transaction_kind="deployment",
        )

    selector = json.loads(environment_selector_path_for_lock(lock_path).read_text(encoding="utf-8"))
    assert selector["generation_id"] != original.environment_generation_id
    with pytest.raises(ReleaseGenerationError, match="marker"):
        authority.verify(expected_commit=commit)

    marker = authority.publish(
        expected_commit=commit,
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="completed",
    )
    authority.commit_generation(
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    assert authority.verify(expected_commit=commit) == marker
    os.close(lock_fd)


def _archive_completed_deployment(
    authority: ReleaseGenerationAuthority,
    lock_path: Path,
    *,
    commit: str,
) -> tuple[str, Path]:
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="timers_restored",
    )
    authority.publish(
        expected_commit=commit,
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="completed",
    )
    authority.commit_generation(
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    active = intent_path_for_lock(lock_path)
    archive = active.with_name(f"{active.stem}.{deployment.operation_id}.completed.json")
    active.replace(archive)
    return deployment.operation_id, archive


def test_completed_intent_archive_is_used_only_when_active_intent_is_absent(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    _operation_id, archive = _archive_completed_deployment(
        authority,
        lock_path,
        commit=commit,
    )

    assert archive.is_file()
    assert authority.verify(expected_commit=commit).commit == commit
    os.close(lock_fd)


@pytest.mark.parametrize("active_kind", ["corrupt", "loose", "symlink"])
def test_unsafe_active_intent_never_falls_back_to_valid_archive(
    tmp_path: Path,
    active_kind: str,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    _operation_id, archive = _archive_completed_deployment(
        authority,
        lock_path,
        commit=commit,
    )
    active = intent_path_for_lock(lock_path)
    if active_kind == "corrupt":
        active.write_text("{not-json\n", encoding="utf-8")
        active.chmod(0o600)
    elif active_kind == "loose":
        active.write_bytes(archive.read_bytes())
        active.chmod(0o644)
    else:
        active.symlink_to(archive)

    with pytest.raises(ReleaseGenerationError, match="deployment record"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


@pytest.mark.parametrize("active_kind", ["corrupt", "loose", "valid"])
def test_initialization_generation_is_blocked_by_any_active_deployment_intent(
    tmp_path: Path,
    active_kind: str,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    active = intent_path_for_lock(lock_path)
    if active_kind == "valid":
        authority.begin_deployment_intent(
            previous_sha=commit,
            target_sha="e" * 40,
            target_ref="e" * 40,
            changed_files=(),
            restart_services=(),
            active_services=(),
            active_timers=(),
        )
    else:
        active.write_text("{not-json\n" if active_kind == "corrupt" else "{}\n")
        active.chmod(0o600 if active_kind == "corrupt" else 0o644)

    with pytest.raises(ReleaseGenerationError, match="deployment|record"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_generation_gc_retains_authority_references_and_removes_only_old_orphans(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    marker = _publish_initialized(authority, commit=commit)
    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha="e" * 40,
        target_ref="e" * 40,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    referenced = {
        hashlib.sha256(f"{intent.operation_id}:{sha}".encode()).hexdigest()
        for sha in (intent.previous_sha, intent.target_sha)
    }
    root = environment_root_for_lock(lock_path)
    previous = marker.environment_generation_id
    orphan = "c" * 64
    failed = f".{('d' * 64)}.0123456789abcdef.building"
    for name in (orphan, failed, *referenced):
        candidate = root / name
        candidate.mkdir(mode=0o700)
        payload = candidate / "payload"
        payload.write_text(name, encoding="utf-8")
        payload.chmod(0o400)
        candidate.chmod(0o500)
    manifests: dict[str, Path] = {}
    for generation_id in (orphan,):
        manifest = lock_path.with_name(f"{lock_path.stem}.venv-{generation_id}.manifest.json")
        manifest.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "generation_id": generation_id,
                    "environment_path": str(root / generation_id),
                    "entries": [{"path": "."}],
                }
            ),
            encoding="utf-8",
        )
        manifest.chmod(0o600)
        manifests[generation_id] = manifest
    now = time.time()
    os.utime(manifests[orphan], (now + 1_000, now + 1_000), follow_symlinks=False)
    os.utime(root / orphan, (now - 200, now - 200), follow_symlinks=False)
    os.utime(root / failed, (now - 300, now - 300), follow_symlinks=False)
    for generation_id in referenced:
        os.utime(root / generation_id, (now - 400, now - 400), follow_symlinks=False)

    metrics = authority.garbage_collect_environments(reason="unit-test")

    assert marker.environment_generation_id in metrics.retained_generation_ids
    assert previous in metrics.retained_generation_ids
    assert referenced <= set(metrics.retained_generation_ids)
    assert not (root / orphan).exists()
    assert not manifests[orphan].exists()
    assert not (root / failed).exists()
    assert metrics.deleted_generations == 2
    audit = lock_path.with_name(f"{lock_path.stem}.generation-gc.jsonl")
    assert audit.stat().st_mode & 0o777 == 0o600
    assert '"reason":"unit-test"' in audit.read_text(encoding="utf-8")
    os.close(lock_fd)


def test_generation_gc_uses_exact_previous_id_not_newer_orphan_mtime(tmp_path: Path) -> None:
    repo, lock_path, first_commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    first = _publish_initialized(authority, commit=first_commit)
    (repo / "README.md").write_text("next generation\n", encoding="utf-8")
    subprocess.run([str(TRUSTED_GIT), "add", "README.md"], cwd=repo, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "next generation",
        ],
        cwd=repo,
        check=True,
    )
    second_commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        [str(TRUSTED_GIT), "reset", "--hard", first_commit],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    intent = authority.begin_deployment_intent(
        previous_sha=first_commit,
        target_sha=second_commit,
        target_ref=second_commit,
        changed_files=("README.md",),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    subprocess.run(
        [str(TRUSTED_GIT), "reset", "--hard", second_commit],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    authority.update_deployment_intent(operation_id=intent.operation_id, stage="timers_restored")
    second = authority.publish(
        expected_commit=second_commit,
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    authority.update_deployment_intent(operation_id=intent.operation_id, stage="completed")
    authority.commit_generation(
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    assert second.previous_generation_id == first.environment_generation_id

    root = environment_root_for_lock(lock_path)
    orphan = "f" * 64
    candidate = root / orphan
    candidate.mkdir(mode=0o700)
    payload = candidate / "payload"
    payload.write_text("orphan", encoding="utf-8")
    payload.chmod(0o400)
    candidate.chmod(0o500)
    orphan_manifest = lock_path.with_name(f"{lock_path.stem}.venv-{orphan}.manifest.json")
    orphan_manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generation_id": orphan,
                "environment_path": str(candidate),
                "entries": [{"path": "."}],
            }
        ),
        encoding="utf-8",
    )
    orphan_manifest.chmod(0o600)
    now = time.time()
    os.utime(orphan_manifest, (now + 10_000, now + 10_000), follow_symlinks=False)
    os.utime(candidate, (now - 100, now - 100), follow_symlinks=False)

    metrics = authority.garbage_collect_environments(reason="clock-skew-test")

    assert first.environment_generation_id in metrics.retained_generation_ids
    assert second.environment_generation_id in metrics.retained_generation_ids
    assert not candidate.exists()
    assert not orphan_manifest.exists()
    os.close(lock_fd)


def test_generation_publish_fails_before_copy_when_disk_budget_is_insufficient(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=1,
    )
    monkeypatch.setattr(
        "rquant.release_generation.shutil.disk_usage",
        lambda _path: shutil._ntuple_diskusage(total=100, used=100, free=0),
    )
    initialization = authority.begin_initialization(target_sha=commit)

    with pytest.raises(ReleaseGenerationError, match="disk budget"):
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    root = environment_root_for_lock(lock_path)
    assert not any(path.name.endswith(".building") for path in root.iterdir())
    os.close(lock_fd)


def test_generation_gc_rejects_symlink_candidate_without_touching_external_data(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    _publish_initialized(authority, commit=commit)
    external = tmp_path / "external"
    external.mkdir()
    payload = external / "keep.txt"
    payload.write_text("keep", encoding="utf-8")
    candidate = environment_root_for_lock(lock_path) / ("f" * 64)
    candidate.symlink_to(external, target_is_directory=True)

    with pytest.raises(ReleaseGenerationError, match="generation is unsafe"):
        authority.garbage_collect_environments(reason="symlink-test")

    assert payload.read_text(encoding="utf-8") == "keep"
    assert candidate.is_symlink()
    os.close(lock_fd)


def test_writable_generation_authority_requires_exclusive_lock(tmp_path: Path) -> None:
    repo, lock_path, _commit, python = _generation(tmp_path)
    first = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    second = os.open(lock_path, os.O_RDWR)
    fcntl.flock(first, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(second, fcntl.LOCK_SH | fcntl.LOCK_NB)

    with pytest.raises(ReleaseGenerationError, match="exclusive"):
        _authority(repo, lock_path, first, python)

    os.close(second)
    os.close(first)


def test_rollback_selects_a_verified_immutable_previous_environment(
    tmp_path: Path,
) -> None:
    repo, lock_path, previous, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    original = _publish_initialized(authority, commit=previous)

    (repo / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.1"\n',
        encoding="utf-8",
    )
    (repo / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    subprocess.run([str(TRUSTED_GIT), "add", "pyproject.toml", "uv.lock"], cwd=repo, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "target generation",
        ],
        cwd=repo,
        check=True,
    )
    target = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        [str(TRUSTED_GIT), "reset", "--hard", previous],
        cwd=repo,
        check=True,
        capture_output=True,
    )

    deployment = authority.begin_deployment_intent(
        previous_sha=previous,
        target_sha=target,
        target_ref=target,
        changed_files=("pyproject.toml", "uv.lock"),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="recovery_started",
    )
    authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="timers_restored",
    )
    marker = authority.publish(
        expected_commit=previous,
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="completed",
    )
    authority.commit_generation(
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )

    selector_payload = json.loads(
        environment_selector_path_for_lock(lock_path).read_text(encoding="utf-8")
    )
    commit_payload = json.loads(commit_path_for_lock(lock_path).read_text(encoding="utf-8"))

    assert marker.commit == previous
    assert marker.environment_generation_id != original.environment_generation_id
    assert marker.previous_generation_id == original.environment_generation_id
    assert selector_payload["previous_generation_id"] == original.environment_generation_id
    assert commit_payload["previous_generation_id"] == original.environment_generation_id
    assert authority.verify(expected_commit=previous) == marker
    os.close(lock_fd)
