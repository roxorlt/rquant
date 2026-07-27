from __future__ import annotations

import fcntl
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from rquant.release_generation import (
    DeploymentIntent,
    ReleaseGenerationAuthority,
    ReleaseGenerationError,
    initialization_path_for_lock,
    intent_path_for_lock,
    marker_path_for_lock,
)

TRUSTED_GIT = Path("/usr/bin/git")


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
) -> ReleaseGenerationAuthority:
    return ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        mutation_hook=mutation_hook,
    )


def test_release_generation_marker_binds_checkout_lock_python_and_venv(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

    marker = authority.publish(expected_commit=commit)
    verified = authority.verify(expected_commit=commit)

    assert verified == marker
    assert marker.commit == commit
    assert marker.uv_lock_sha256
    assert marker.package_version == "0.99.0"
    assert marker.python_abi
    assert marker.venv_identity.inode > 0
    assert marker_path_for_lock(lock_path).is_file()
    os.close(lock_fd)


def test_release_generation_rejects_uv_lock_or_venv_drift(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    authority.publish(expected_commit=commit)

    (repo / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    with pytest.raises(ReleaseGenerationError, match="uv.lock"):
        authority.verify(expected_commit=commit)

    (repo / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    displaced = tmp_path / "old-venv"
    (repo / ".venv").rename(displaced)
    (repo / ".venv").mkdir()
    with pytest.raises(ReleaseGenerationError, match="venv"):
        authority.verify(expected_commit=commit)
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
        authority.publish(expected_commit=commit)

    assert not marker_path_for_lock(lock_path).exists()
    os.close(lock_fd)


def test_invalidated_generation_cannot_be_verified_until_republished(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    authority.publish(expected_commit=commit)

    authority.invalidate()

    with pytest.raises(ReleaseGenerationError, match="marker"):
        authority.verify(expected_commit=commit)
    authority.publish(expected_commit=commit)
    assert authority.verify(expected_commit=commit).commit == commit
    os.close(lock_fd)


def test_release_generation_rejects_tracked_source_drift(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    authority.publish(expected_commit=commit)
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

    published = authority.publish(expected_commit=commit)

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
        authority.publish(expected_commit=commit)

    assert not marker_path_for_lock(lock_path).exists()
    os.close(lock_fd)


def test_deployment_intent_pins_plan_before_marker_invalidation(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    marker = authority.publish(expected_commit=commit)

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
    authority.publish(expected_commit=commit)
    authority.complete_initialization(operation_id=initialization.operation_id)
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
