from __future__ import annotations

import fcntl
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from rquant.release_generation import (
    DeploymentIntent,
    ReleaseGenerationAuthority,
    ReleaseGenerationError,
    commit_path_for_lock,
    environment_root_for_lock,
    environment_selector_path_for_lock,
    initialization_path_for_lock,
    intent_path_for_lock,
    marker_path_for_lock,
)

TRUSTED_GIT = Path("/usr/bin/git")


@pytest.fixture(autouse=True)
def _thaw_immutable_test_generations(tmp_path: Path) -> Iterator[None]:
    yield
    for path in sorted(tmp_path.rglob("*"), key=lambda value: len(value.parts)):
        if path.is_dir() and not path.is_symlink():
            path.chmod(0o700)
        elif path.is_file() and not path.is_symlink():
            path.chmod(0o600)


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

    assert marker.commit == previous
    assert marker.environment_generation_id != original.environment_generation_id
    assert authority.verify(expected_commit=previous) == marker
    os.close(lock_fd)
