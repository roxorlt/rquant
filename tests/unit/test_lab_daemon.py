from __future__ import annotations

import fcntl
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.lab_artifact_protocol import LabFinalizerAuthorityKey
from rquant.lab_daemon import (
    LabAuthorityKeyring,
    LabDaemonConfigurationError,
    LabDaemonLock,
    LabDaemonReadinessPublisher,
    LabFinalizerDaemon,
    LabFinalizerDaemonState,
    LabFinalizerFailureState,
    LabFinalizerStateStore,
    LabRuntimeGuard,
    ensure_private_directory,
    load_lab_job_center_authority_manifest,
    prepare_private_sqlite_path,
    require_clean_code_sha,
    require_private_directory,
    require_unique_runtime_paths,
)
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.strict_json import canonical_model_json_bytes


def _write_private(path: Path, payload: str) -> None:
    path.write_text(payload, encoding="ascii")
    path.chmod(0o600)


def _write_private_json(path: Path, payload: object) -> None:
    _write_private(
        path,
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
    )


def test_job_center_authority_manifest_is_private_exact_and_path_bound(
    tmp_path: Path,
) -> None:
    from .test_job_center_authority import CODE_SHA, _publish_and_install

    path, paths = _publish_and_install(tmp_path)

    loaded = load_lab_job_center_authority_manifest(
        path,
        expected_code_sha=CODE_SHA,
        expected_research_root=paths["runtime_root"],
        expected_lab_jobs_path=paths["lab_jobs_path"],
        expected_command_spool_path=paths["command_spool_path"],
        expected_final_artifact_root=paths["final_artifact_root"],
    )

    assert loaded.code_sha == CODE_SHA
    with pytest.raises(LabDaemonConfigurationError, match="manifest is invalid"):
        load_lab_job_center_authority_manifest(
            path,
            expected_code_sha=CODE_SHA,
            expected_research_root=paths["runtime_root"],
            expected_lab_jobs_path=paths["runtime_root"] / "other.sqlite3",
            expected_command_spool_path=paths["command_spool_path"],
            expected_final_artifact_root=paths["final_artifact_root"],
        )


def test_daemon_readiness_is_generation_bound_and_monotonic(tmp_path: Path) -> None:
    authority_root = tmp_path / "authority"
    authority_root.mkdir(mode=0o700)
    lock_path = authority_root / "rquant.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    monotonic_values = iter((10.0, 11.0))
    publisher = LabDaemonReadinessPublisher(
        deployment_lock_path=lock_path,
        deployment_lock_fd=lock_fd,
        label="com.roxor.rquant-lab-worker",
        operation_id="a" * 32,
        environment_generation_id="b" * 64,
        code_sha="c" * 40,
        heartbeat_interval_seconds=1,
        monotonic_provider=lambda: next(monotonic_values),
    )
    try:
        first = publisher.publish_once()
        second = publisher.publish_once()
        observed = LabDaemonReadinessPublisher.read(
            deployment_lock_path=lock_path,
            label="com.roxor.rquant-lab-worker",
        )
    finally:
        publisher.close()
        os.close(lock_fd)

    assert first.heartbeat_monotonic == 10.0
    assert second.heartbeat_monotonic == 11.0
    assert observed == second
    assert observed.environment_generation_id == "b" * 64
    assert observed.operation_id == "a" * 32


def test_readiness_close_fails_closed_without_forgetting_live_thread(tmp_path: Path) -> None:
    authority = tmp_path / "authority"
    authority.mkdir(mode=0o700)
    lock_path = authority / "rquant.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    entered = threading.Event()
    release = threading.Event()
    publisher = LabDaemonReadinessPublisher(
        deployment_lock_path=lock_path,
        deployment_lock_fd=lock_fd,
        label="com.roxor.rquant-lab-worker",
        operation_id="a" * 32,
        environment_generation_id="b" * 64,
        code_sha="c" * 40,
        heartbeat_interval_seconds=0.1,
    )
    publisher.start()

    def blocked_publish() -> object:
        entered.set()
        release.wait(timeout=5)
        return object()

    publisher.publish_once = blocked_publish  # type: ignore[method-assign]
    try:
        assert entered.wait(timeout=1)
        thread = publisher._thread
        assert thread is not None
        with pytest.raises(RuntimeError, match="readiness.*did not stop"):
            publisher.close()
        assert publisher._thread is thread
        assert thread.is_alive()
    finally:
        release.set()
        if publisher._thread is not None:
            publisher._thread.join(timeout=2)
        publisher.close()
        os.close(lock_fd)


def test_readiness_live_thread_keeps_daemon_authority_lease(tmp_path: Path) -> None:
    authority = tmp_path / "authority"
    authority.mkdir(mode=0o700)
    deployment_lock_path = authority / "rquant.lock"
    deployment_lock_fd = os.open(deployment_lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    lock_root = tmp_path / "runtime" / "locks"
    lock_root.parent.mkdir(mode=0o700)
    first = LabDaemonLock(lock_root, "worker")
    second = LabDaemonLock(lock_root, "worker")
    entered = threading.Event()
    release = threading.Event()
    first.acquire()
    publisher = LabDaemonReadinessPublisher(
        deployment_lock_path=deployment_lock_path,
        deployment_lock_fd=deployment_lock_fd,
        daemon_authority_lease_fd=first.duplicate_authority_lease(),
        label="com.roxor.rquant-lab-worker",
        operation_id="a" * 32,
        environment_generation_id="b" * 64,
        code_sha="c" * 40,
        heartbeat_interval_seconds=0.1,
    )
    publisher.start()

    def blocked_publish() -> object:
        entered.set()
        release.wait(timeout=5)
        return object()

    publisher.publish_once = blocked_publish  # type: ignore[method-assign]
    try:
        assert entered.wait(timeout=1)
        with pytest.raises(RuntimeError, match="readiness.*did not stop"):
            publisher.close()
        first.release()
        with pytest.raises(LabDaemonConfigurationError, match="already running"):
            second.acquire()
    finally:
        release.set()
        if publisher._thread is not None:
            publisher._thread.join(timeout=2)
        publisher.close()
        first.release()
        os.close(deployment_lock_fd)

    second.acquire()
    second.release()


@pytest.mark.parametrize(
    "label",
    (
        "com.roxor.rquant-lab-scheduler",
        "com.roxor.rquant-lab-worker",
        "com.roxor.rquant-lab-finalizer",
    ),
)
def test_readiness_thread_start_failure_preserves_error_and_releases_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    label: str,
) -> None:
    from rquant import lab_daemon

    authority = tmp_path / "authority"
    authority.mkdir(mode=0o700)
    deployment_lock_path = authority / "rquant.lock"
    deployment_lock_fd = os.open(deployment_lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    lock_root = tmp_path / "runtime" / "locks"
    lock_root.parent.mkdir(mode=0o700)
    daemon_lock = LabDaemonLock(lock_root, label.rsplit("-", 1)[-1])
    daemon_lock.acquire()
    lease_fd = daemon_lock.duplicate_authority_lease()
    publisher = LabDaemonReadinessPublisher(
        deployment_lock_path=deployment_lock_path,
        deployment_lock_fd=deployment_lock_fd,
        daemon_authority_lease_fd=lease_fd,
        label=label,
        operation_id="a" * 32,
        environment_generation_id="b" * 64,
        code_sha="c" * 40,
        heartbeat_interval_seconds=0.1,
    )
    primary = OSError("start boom")

    class FailingThread:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            raise primary

        def join(self, *, timeout: float) -> None:
            pytest.fail(f"unstarted thread was joined with timeout={timeout}")

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(lab_daemon, "Thread", FailingThread)
    try:
        with pytest.raises(OSError) as caught:
            publisher.start()
        assert caught.value is primary
        assert publisher._thread is None
        assert publisher._thread_state == "stopped"
        assert publisher._daemon_authority_lease_fd == -1
        with pytest.raises(OSError):
            os.fstat(lease_fd)
        publisher.close()
        publisher.close()
    finally:
        publisher.close()
        daemon_lock.release()
        os.close(deployment_lock_fd)


@pytest.mark.parametrize(
    "label",
    (
        "com.roxor.rquant-lab-scheduler",
        "com.roxor.rquant-lab-worker",
        "com.roxor.rquant-lab-finalizer",
    ),
)
def test_readiness_start_interrupt_after_thread_entry_keeps_authority_until_thread_stops(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    label: str,
) -> None:
    from rquant import lab_daemon

    authority = tmp_path / "authority"
    authority.mkdir(mode=0o700)
    deployment_lock_path = authority / "rquant.lock"
    deployment_lock_fd = os.open(deployment_lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    lock_root = tmp_path / "runtime" / "locks"
    lock_root.parent.mkdir(mode=0o700)
    lock_name = label.rsplit("-", 1)[-1]
    first = LabDaemonLock(lock_root, lock_name)
    second = LabDaemonLock(lock_root, lock_name)
    entered = threading.Event()
    release = threading.Event()
    fake_threads: list[object] = []
    first.acquire()
    publisher = LabDaemonReadinessPublisher(
        deployment_lock_path=deployment_lock_path,
        deployment_lock_fd=deployment_lock_fd,
        daemon_authority_lease_fd=first.duplicate_authority_lease(),
        label=label,
        operation_id="a" * 32,
        environment_generation_id="b" * 64,
        code_sha="c" * 40,
        heartbeat_interval_seconds=0.1,
    )

    def blocked_run() -> None:
        entered.set()
        release.wait(timeout=5)

    publisher._run = blocked_run  # type: ignore[method-assign]

    class StartedThenInterruptedThread:
        def __init__(self, *, target: object, name: str, daemon: bool) -> None:
            assert callable(target)
            self._inner = threading.Thread(target=target, name=name, daemon=daemon)
            fake_threads.append(self)

        def start(self) -> None:
            self._inner.start()
            assert entered.wait(timeout=1)
            raise KeyboardInterrupt("interrupt after thread entry")

        def join(self, *, timeout: float) -> None:
            del timeout

        def is_alive(self) -> bool:
            return self._inner.is_alive()

    monkeypatch.setattr(lab_daemon, "Thread", StartedThenInterruptedThread)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            publisher.start()
        first.release()
        assert publisher._thread is fake_threads[0]
        assert publisher._daemon_authority_lease_fd >= 0
        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert any("did not stop" in str(error) for error in cleanup_group.exceptions)
        with pytest.raises(LabDaemonConfigurationError, match="already running"):
            second.acquire()
    finally:
        release.set()
        for fake in fake_threads:
            fake._inner.join(timeout=2)  # type: ignore[attr-defined]
        publisher.close()
        first.release()
        second.release()
        os.close(deployment_lock_fd)

    second.acquire()
    second.release()
    publisher.close()


def test_daemon_readiness_rejects_invalid_generation_before_namespace_creation(
    tmp_path: Path,
) -> None:
    authority_root = tmp_path / "authority"
    authority_root.mkdir(mode=0o700)
    lock_path = authority_root / "rquant.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        with pytest.raises(LabDaemonConfigurationError, match="operation id"):
            LabDaemonReadinessPublisher(
                deployment_lock_path=lock_path,
                deployment_lock_fd=lock_fd,
                label="com.roxor.rquant-lab-worker",
                operation_id="short",
                environment_generation_id="b" * 64,
                code_sha="c" * 40,
                heartbeat_interval_seconds=1,
            )
    finally:
        os.close(lock_fd)

    assert not lock_path.with_name("rquant.lab-readiness").exists()


def test_daemon_readiness_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    authority_root = tmp_path / "authority"
    authority_root.mkdir(mode=0o700)
    lock_path = authority_root / "rquant.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    publisher = LabDaemonReadinessPublisher(
        deployment_lock_path=lock_path,
        deployment_lock_fd=lock_fd,
        label="com.roxor.rquant-lab-worker",
        operation_id="a" * 32,
        environment_generation_id="b" * 64,
        code_sha="c" * 40,
        heartbeat_interval_seconds=1,
    )
    try:
        publisher.publish_once()
        original = publisher.path.read_text(encoding="utf-8").lstrip()
        publisher.path.write_text('{"label":"forged",' + original[1:], encoding="utf-8")
        publisher.path.chmod(0o600)

        with pytest.raises(LabDaemonConfigurationError, match="heartbeat is invalid"):
            LabDaemonReadinessPublisher.read(
                deployment_lock_path=lock_path,
                label="com.roxor.rquant-lab-worker",
            )
    finally:
        publisher.close()
        os.close(lock_fd)


def test_authority_keyring_loads_active_and_rotated_keys(tmp_path: Path) -> None:
    active = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(active, "61" * 32 + "\n")
    _write_private(
        ring,
        json.dumps(
            {
                "schema_version": 1,
                "keys": {
                    "previous": "62" * 32,
                    "active": "61" * 32,
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
    )

    keys = LabAuthorityKeyring.load(
        active_key_id="active",
        active_key_path=active,
        verification_keyring_path=ring,
    )

    assert keys.signing_key() == LabFinalizerAuthorityKey(key_id="active", secret=b"a" * 32)
    assert keys.verification_key("previous") == LabFinalizerAuthorityKey(
        key_id="previous", secret=b"b" * 32
    )
    assert keys.verification_key("missing") is None


def test_authority_keyring_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    active = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(active, "61" * 32 + "\n")
    _write_private(
        ring,
        '{"schema_version":1,"schema_version":1,"keys":{"active":"' + "61" * 32 + '"}}\n',
    )

    with pytest.raises(LabDaemonConfigurationError, match="duplicate|valid JSON"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=active,
            verification_keyring_path=ring,
        )


@pytest.mark.parametrize(
    "payload",
    [
        '{"schema_version": 1, "keys": {"active": "' + "61" * 32 + '"}}\n',
        '{"schema_version":1,"keys":{"active":"' + "61" * 32 + '"}}\n',
        '{"keys":{"active":"' + "61" * 32 + '"},"schema_version":1}',
    ],
)
def test_authority_keyring_rejects_noncanonical_json(
    tmp_path: Path,
    payload: str,
) -> None:
    active = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(active, "61" * 32 + "\n")
    _write_private(ring, payload)

    with pytest.raises(LabDaemonConfigurationError, match="valid JSON"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=active,
            verification_keyring_path=ring,
        )


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o606])
def test_authority_keyring_rejects_non_private_key_files(
    tmp_path: Path,
    mode: int,
) -> None:
    key = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(key, "61" * 32 + "\n")
    _write_private_json(
        ring,
        {"schema_version": 1, "keys": {"active": "61" * 32}},
    )
    key.chmod(mode)

    with pytest.raises(LabDaemonConfigurationError, match="private"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=key,
            verification_keyring_path=ring,
        )


def test_authority_keyring_rejects_missing_or_symlinked_key(tmp_path: Path) -> None:
    missing = tmp_path / "missing.key"
    ring = tmp_path / "keyring.json"
    _write_private_json(
        ring,
        {"schema_version": 1, "keys": {"active": "61" * 32}},
    )

    with pytest.raises(LabDaemonConfigurationError, match="key file"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=missing,
            verification_keyring_path=ring,
        )


def test_authority_keyring_rejects_public_ring_and_non_ascii_active_key(
    tmp_path: Path,
) -> None:
    key = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(key, "61" * 32 + "\n")
    _write_private_json(
        ring,
        {"schema_version": 1, "keys": {"active": "61" * 32}},
    )
    ring.chmod(0o644)
    with pytest.raises(LabDaemonConfigurationError, match="private"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=key,
            verification_keyring_path=ring,
        )

    ring.chmod(0o600)
    key.write_bytes(b"\xff" * 32)
    key.chmod(0o600)
    with pytest.raises(LabDaemonConfigurationError, match="ASCII"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=key,
            verification_keyring_path=ring,
        )

    real = tmp_path / "real.key"
    link = tmp_path / "link.key"
    _write_private(real, "61" * 32 + "\n")
    link.symlink_to(real)
    with pytest.raises(LabDaemonConfigurationError, match="symlink"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=link,
            verification_keyring_path=ring,
        )


def test_authority_keyring_rejects_wrong_active_key_and_weak_secret(tmp_path: Path) -> None:
    key = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(key, "61" * 31 + "\n")
    _write_private_json(
        ring,
        {"schema_version": 1, "keys": {"active": "62" * 32}},
    )

    with pytest.raises(LabDaemonConfigurationError, match="32 bytes"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=key,
            verification_keyring_path=ring,
        )

    _write_private(key, "61" * 32 + "\n")
    with pytest.raises(LabDaemonConfigurationError, match="does not match"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=key,
            verification_keyring_path=ring,
        )


def test_authority_keyring_rejects_hardlinked_key_without_reading_it(tmp_path: Path) -> None:
    victim = tmp_path / "victim.key"
    key = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(victim, "61" * 32 + "\n")
    key.hardlink_to(victim)
    _write_private_json(
        ring,
        {"schema_version": 1, "keys": {"active": "61" * 32}},
    )

    with pytest.raises(LabDaemonConfigurationError, match="hardlink"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=key,
            verification_keyring_path=ring,
        )


@pytest.mark.parametrize("replacement_target", ["active", "keyring"])
def test_authority_keyring_rejects_active_path_replacement_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_target: str,
) -> None:
    active = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(active, "61" * 32 + "\n")
    _write_private_json(
        ring,
        {"schema_version": 1, "keys": {"active": "61" * 32}},
    )
    target = active if replacement_target == "active" else ring
    target_identity = target.stat()
    displaced = tmp_path / f"original-{target.name}"
    real_read = os.read
    swapped = False

    def replacing_read(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        payload = real_read(descriptor, size)
        current = os.fstat(descriptor)
        if not swapped and (current.st_dev, current.st_ino) == (
            target_identity.st_dev,
            target_identity.st_ino,
        ):
            swapped = True
            target.rename(displaced)
            _write_private(target, displaced.read_text(encoding="ascii"))
        return payload

    monkeypatch.setattr("rquant.lab_daemon.os.read", replacing_read)
    with pytest.raises(LabDaemonConfigurationError, match="changed during read"):
        LabAuthorityKeyring.load(
            active_key_id="active",
            active_key_path=active,
            verification_keyring_path=ring,
        )


@pytest.mark.parametrize(
    "value",
    [None, "", "a" * 39, "a" * 41, "A" * 40, "a" * 40 + "-dirty"],
)
def test_require_clean_code_sha_fails_closed(value: str | None) -> None:
    with pytest.raises(LabDaemonConfigurationError, match="clean 40-character"):
        require_clean_code_sha(lambda: value)


def test_daemon_lock_is_single_instance_and_private(tmp_path: Path) -> None:
    lock_dir = tmp_path / "locks"

    with LabDaemonLock(lock_dir, "scheduler"):
        lock_path = lock_dir / "scheduler.lock"
        assert lock_path.exists()
        assert lock_path.stat().st_mode & 0o777 == 0o600
        assert lock_dir.stat().st_mode & 0o777 == 0o700
        with (
            pytest.raises(LabDaemonConfigurationError, match="already running"),
            LabDaemonLock(lock_dir, "scheduler"),
        ):
            pass

    with LabDaemonLock(lock_dir, "scheduler"):
        assert os.getpid() > 0


@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_daemon_lock_rejects_linked_existing_file_without_truncating_victim(
    tmp_path: Path,
    link_kind: str,
) -> None:
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir(mode=0o700)
    victim = tmp_path / "victim.txt"
    victim.write_text("keep me\n", encoding="utf-8")
    victim.chmod(0o600)
    lock_path = lock_dir / "scheduler.lock"
    if link_kind == "symlink":
        lock_path.symlink_to(victim)
    else:
        lock_path.hardlink_to(victim)

    with pytest.raises(LabDaemonConfigurationError, match=link_kind):
        LabDaemonLock(lock_dir, "scheduler").acquire()

    assert victim.read_text(encoding="utf-8") == "keep me\n"


def test_daemon_lock_rejects_public_or_non_regular_existing_file(tmp_path: Path) -> None:
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir(mode=0o700)
    lock_path = lock_dir / "scheduler.lock"
    lock_path.write_text("old\n", encoding="utf-8")
    lock_path.chmod(0o644)
    with pytest.raises(LabDaemonConfigurationError, match="private"):
        LabDaemonLock(lock_dir, "scheduler").acquire()

    lock_path.unlink()
    lock_path.mkdir(mode=0o700)
    with pytest.raises(LabDaemonConfigurationError, match="regular"):
        LabDaemonLock(lock_dir, "scheduler").acquire()


def test_daemon_lock_rejects_root_replacement_before_touching_lock_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir(mode=0o700)
    original = tmp_path / "original-locks"
    replacement_victim = tmp_path / "replacement-victim.txt"
    replacement_victim.write_text("keep me\n", encoding="utf-8")
    replacement_victim.chmod(0o600)
    real_open = os.open
    swapped = False

    def swapping_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal swapped
        descriptor = real_open(path, flags, *args, **kwargs)
        if not swapped and Path(path) == Path(lock_dir.name) and kwargs.get("dir_fd") is not None:
            swapped = True
            lock_dir.rename(original)
            lock_dir.mkdir(mode=0o700)
            (lock_dir / "scheduler.lock").symlink_to(replacement_victim)
        return descriptor

    monkeypatch.setattr("rquant.lab_daemon.os.open", swapping_open)
    with pytest.raises(LabDaemonConfigurationError, match="root identity changed"):
        LabDaemonLock(lock_dir, "scheduler").acquire()

    assert replacement_victim.read_text(encoding="utf-8") == "keep me\n"


def test_daemon_lock_remains_exclusive_after_root_is_renamed_and_recreated(
    tmp_path: Path,
) -> None:
    lock_dir = tmp_path / "locks"
    displaced = tmp_path / "displaced-locks"
    first = LabDaemonLock(lock_dir, "scheduler")
    first.acquire()
    try:
        lock_dir.rename(displaced)
        lock_dir.mkdir(mode=0o700)

        second = LabDaemonLock(lock_dir, "scheduler")
        with pytest.raises(LabDaemonConfigurationError, match="already running"):
            second.acquire()
        assert first.authority_path == second.authority_path
        assert first.authority_path is not None
        assert first.authority_path.parent == tmp_path
        assert first.authority_path.stat().st_mode & 0o777 == 0o600
        second.release()
    finally:
        first.release()


def test_scheduler_prepares_private_sqlite_under_public_umask(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    prior_umask = os.umask(0o022)
    try:
        prepared = prepare_private_sqlite_path(
            path,
            label="lab jobs SQLite",
            create=True,
        )
    finally:
        os.umask(prior_umask)

    try:
        assert prepared.path == path
        assert path.is_file()
        assert path.stat().st_mode & 0o777 == 0o600
        assert path.stat().st_nlink == 1
    finally:
        prepared.close()


def test_finalizer_private_sqlite_check_never_creates_missing_file(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"

    with pytest.raises(LabDaemonConfigurationError, match="does not exist"):
        prepare_private_sqlite_path(path, label="lab jobs SQLite", create=False)

    assert not path.exists()


@pytest.mark.parametrize("replacement_kind", ["rename", "symlink"])
def test_scheduler_sqlite_authority_rejects_replacement_before_first_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_kind: str,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    replacement = root / "replacement.sqlite3"
    with sqlite3.connect(replacement) as connection:
        connection.execute("CREATE TABLE reviewer_marker(value TEXT)")
        connection.execute("INSERT INTO reviewer_marker VALUES ('untouched')")
    replacement.chmod(0o600)
    original = root / "original.sqlite3"
    real_connect = sqlite3.connect
    swapped = False

    def swapping_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        nonlocal swapped
        if not swapped:
            swapped = True
            path.rename(original)
            if replacement_kind == "rename":
                replacement.rename(path)
            else:
                path.symlink_to(replacement)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr("rquant.lab_jobs.sqlite3.connect", swapping_connect)
    store = LabJobStore(path, identity_authority=authority)
    try:
        with pytest.raises(LabDaemonConfigurationError, match="identity changed"):
            store.initialize()
    finally:
        authority.close()

    marker_path = path if replacement_kind == "rename" else replacement
    with real_connect(marker_path) as connection:
        assert connection.execute("SELECT value FROM reviewer_marker").fetchone() == ("untouched",)
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'lab_%'"
        ).fetchone() == (0,)


def test_scheduler_sqlite_rw_uri_never_creates_missing_symlink_target_after_precheck(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    original = root / "original.sqlite3"
    missing_target = root / "missing.sqlite3"
    original_assert_current = authority.assert_current
    checked = False

    def swap_after_precheck() -> None:
        nonlocal checked
        original_assert_current()
        if not checked:
            checked = True
            path.rename(original)
            path.symlink_to(missing_target)

    monkeypatch.setattr(authority, "assert_current", swap_after_precheck)
    try:
        with pytest.raises(sqlite3.OperationalError):
            LabJobStore(path, identity_authority=authority).initialize()
    finally:
        authority.close()

    assert not missing_target.exists()


def test_private_sqlite_rejects_parent_replacement_immediately_after_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    displaced = tmp_path / "displaced-state"
    path = root / "lab_jobs.sqlite3"
    real_open = os.open
    swapped = False

    def replacing_open(path_arg: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal swapped
        descriptor = real_open(path_arg, flags, *args, **kwargs)
        if not swapped and Path(path_arg) == root and kwargs.get("dir_fd") is None:
            swapped = True
            root.rename(displaced)
            root.mkdir(mode=0o700)
        return descriptor

    monkeypatch.setattr("rquant.lab_daemon.os.open", replacing_open)
    with pytest.raises(LabDaemonConfigurationError, match="parent identity changed"):
        prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)

    assert not (root / path.name).exists()
    assert not (displaced / path.name).exists()


def test_finalizer_sqlite_authority_rejects_rename_swap_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    LabJobStore(path, identity_authority=authority).initialize()
    replacement = root / "replacement.sqlite3"
    with sqlite3.connect(replacement) as connection:
        connection.execute("CREATE TABLE reviewer_marker(value TEXT)")
        connection.execute("INSERT INTO reviewer_marker VALUES ('must-not-read')")
    replacement.chmod(0o600)
    original = root / "original.sqlite3"
    real_connect = sqlite3.connect
    swapped = False

    def swapping_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        nonlocal swapped
        if not swapped:
            swapped = True
            path.rename(original)
            replacement.rename(path)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr("rquant.lab_jobs.sqlite3.connect", swapping_connect)
    reader = LabJobReader(path, identity_authority=authority)
    try:
        with pytest.raises(LabDaemonConfigurationError, match="identity changed"):
            reader.list_finalization_candidates(limit=1)
    finally:
        authority.close()


def test_sqlite_authority_connection_remains_bound_to_original_inode_after_swap(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    connection = authority.open_verified_connection(
        lambda verified_path: sqlite3.connect(verified_path, isolation_level=None)
    )
    original = root / "original.sqlite3"
    replacement = root / "replacement.sqlite3"
    with sqlite3.connect(replacement) as other:
        other.execute("CREATE TABLE replacement_only(value TEXT)")
    replacement.chmod(0o600)
    path.rename(original)
    replacement.rename(path)
    try:
        connection.execute("CREATE TABLE original_only(value TEXT)")
    finally:
        connection.close()
        authority.close()

    with sqlite3.connect(original) as original_connection:
        assert original_connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'original_only'"
        ).fetchone() == (1,)
    with sqlite3.connect(path) as replacement_connection:
        assert replacement_connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'original_only'"
        ).fetchone() == (0,)


def test_sqlite_authority_fences_entire_wal_write_transaction_and_rolls_back(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    store = LabJobStore(path, identity_authority=authority)
    store.initialize()
    original = root / "original.sqlite3"
    replacement = root / "replacement.sqlite3"
    with sqlite3.connect(path) as source, sqlite3.connect(replacement) as target:
        source.backup(target)
    replacement.chmod(0o600)

    try:
        with (
            pytest.raises(LabDaemonConfigurationError, match="identity changed"),
            store._transaction() as connection,
        ):
            connection.execute("CREATE TABLE transaction_marker(value TEXT)")
            path.rename(original)
            replacement.rename(path)
    finally:
        if path.exists():
            path.rename(replacement)
        if original.exists():
            original.rename(path)
        authority.close()

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'transaction_marker'"
        ).fetchone() == (0,)


def test_sqlite_authority_fences_read_snapshot_before_return_after_path_swap(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    LabJobStore(path, identity_authority=authority).initialize()
    reader = LabJobReader(path, identity_authority=authority)
    original = root / "original.sqlite3"
    replacement = root / "replacement.sqlite3"
    with sqlite3.connect(path) as source, sqlite3.connect(replacement) as target:
        source.backup(target)
    replacement.chmod(0o600)

    try:
        with (
            pytest.raises(LabDaemonConfigurationError, match="identity changed"),
            reader._read_snapshot(label="replacement test") as connection,
        ):
            connection.execute("SELECT COUNT(*) FROM lab_job").fetchone()
            path.rename(original)
            replacement.rename(path)
    finally:
        if path.exists():
            path.rename(replacement)
        if original.exists():
            original.rename(path)
        authority.close()


def test_sqlite_authority_holds_shared_parent_maintenance_lock(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    root_descriptor = os.open(root, os.O_RDONLY)
    try:
        with pytest.raises(BlockingIOError):
            fcntl.flock(root_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        authority.close()
        fcntl.flock(root_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(root_descriptor, fcntl.LOCK_UN)
    finally:
        authority.close()
        os.close(root_descriptor)


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("public", "private mode 0600"),
        ("symlink", "symlink"),
        ("hardlink", "hardlink"),
        ("directory", "regular file"),
    ],
)
def test_private_sqlite_rejects_unsafe_existing_identity(
    tmp_path: Path,
    kind: str,
    message: str,
) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    victim = tmp_path / "victim.sqlite3"
    victim.write_bytes(b"private state")
    victim.chmod(0o600)
    if kind == "public":
        path.write_bytes(b"state")
        path.chmod(0o644)
    elif kind == "symlink":
        path.symlink_to(victim)
    elif kind == "hardlink":
        path.hardlink_to(victim)
    else:
        path.mkdir(mode=0o700)

    with pytest.raises(LabDaemonConfigurationError, match=message):
        prepare_private_sqlite_path(path, label="lab jobs SQLite", create=False)

    assert victim.read_bytes() == b"private state"


@pytest.mark.parametrize("mode", [0o755, 0o711])
def test_private_sqlite_requires_private_0700_parent(tmp_path: Path, mode: int) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    root.chmod(mode)
    path = root / "lab_jobs.sqlite3"
    path.write_bytes(b"")
    path.chmod(0o600)

    with (
        pytest.raises(LabDaemonConfigurationError, match="private mode 0700"),
        prepare_private_sqlite_path(path, label="lab jobs SQLite", create=False),
    ):
        pass


def test_sqlite_authority_rejects_parent_permission_drift(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    path = root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(path, label="lab jobs SQLite", create=True)
    root.chmod(0o711)
    try:
        with pytest.raises(LabDaemonConfigurationError, match="parent identity changed"):
            authority.assert_current()
    finally:
        root.chmod(0o700)
        authority.close()


def test_private_directory_gate_rejects_public_or_symlinked_roots(tmp_path: Path) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    require_private_directory(private, label="command spool")

    private.chmod(0o755)
    with pytest.raises(LabDaemonConfigurationError, match="mode 0700"):
        require_private_directory(private, label="command spool")

    private.chmod(0o700)
    linked = tmp_path / "linked"
    linked.symlink_to(private, target_is_directory=True)
    with pytest.raises(LabDaemonConfigurationError, match="real directory"):
        require_private_directory(linked, label="command spool")


@pytest.mark.parametrize("mode", [0o500, 0o600, 0o755])
def test_private_directory_gate_requires_exact_mode_0700(
    tmp_path: Path,
    mode: int,
) -> None:
    root = tmp_path / "managed"
    root.mkdir(mode=mode)
    root.chmod(mode)

    with pytest.raises(LabDaemonConfigurationError, match="mode 0700"):
        require_private_directory(root, label="lab managed root")


def test_private_lab_runtime_layout_migrates_without_chmoding_shared_data(
    tmp_path: Path,
) -> None:
    from rquant import lab_daemon

    data = tmp_path / "data"
    data.mkdir(mode=0o755)
    data.chmod(0o755)
    legacy_database = data / "lab_jobs.sqlite3"
    legacy_database.write_bytes(b"sqlite")
    legacy_database.chmod(0o600)
    legacy_commands = data / "lab_job_commands"
    legacy_commands.mkdir(mode=0o700)
    (legacy_commands / "pending.json").write_text("{}", encoding="utf-8")
    (legacy_commands / "pending.json").chmod(0o600)
    runtime = data / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    commands = runtime / "commands"

    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": commands, "readiness": runtime / "readiness"},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={database: legacy_database, commands: legacy_commands},
        mutation_guard=lambda: "a" * 40,
    )

    assert stat.S_IMODE(data.stat().st_mode) == 0o755
    assert stat.S_IMODE(runtime.stat().st_mode) == 0o700
    assert database.read_bytes() == b"sqlite"
    assert stat.S_IMODE(database.stat().st_mode) == 0o600
    assert (commands / "pending.json").read_text(encoding="utf-8") == "{}"
    assert stat.S_IMODE(commands.stat().st_mode) == 0o700
    assert stat.S_IMODE((runtime / "readiness").stat().st_mode) == 0o700
    assert not legacy_database.exists()
    assert not legacy_commands.exists()
    prepared = lab_daemon.verify_lab_runtime_prepared(
        runtime,
        checkout_root=tmp_path,
        expected_commit="a" * 40,
        managed_directories={"commands": commands, "readiness": runtime / "readiness"},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={database: legacy_database, commands: legacy_commands},
    )
    assert prepared["runtime_root"] == str(runtime)
    assert prepared["migration_sources"][str(database)]["source"] == str(legacy_database)
    assert prepared["migration_sources"][str(database)]["migrated"] is True

    sentinel_before = lab_daemon.lab_runtime_prepared_path(runtime).read_bytes()
    upgraded_release = lab_daemon.verify_lab_runtime_prepared(
        runtime,
        checkout_root=tmp_path,
        expected_commit="b" * 40,
        managed_directories={"commands": commands, "readiness": runtime / "readiness"},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={database: legacy_database, commands: legacy_commands},
    )
    assert upgraded_release["runtime_authority_id"] == prepared["runtime_authority_id"]
    assert lab_daemon.lab_runtime_prepared_path(runtime).read_bytes() == sentinel_before


def test_prepared_runtime_requires_owner_registration_of_first_sqlite_identity(
    tmp_path: Path,
) -> None:
    from rquant import lab_daemon

    runtime = tmp_path / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    commands = runtime / "commands"
    directories = {"commands": commands}
    files = {"lab jobs SQLite": database}
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories=directories,
        managed_files=files,
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )

    database.write_bytes(b"sqlite-authority")
    database.chmod(0o600)
    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="not registered",
    ):
        lab_daemon.verify_lab_runtime_prepared(
            runtime,
            checkout_root=tmp_path,
            expected_commit="b" * 40,
            managed_directories=directories,
            managed_files=files,
            legacy_paths={},
        )
    registered = lab_daemon.register_lab_runtime_managed_file(
        runtime,
        label="lab jobs SQLite",
        path=database,
        mutation_guard=lambda: "b" * 40,
    )
    observed = database.lstat()
    assert registered["managed_files"]["lab jobs SQLite"] == {
        "path": str(database),
        "device": observed.st_dev,
        "inode": observed.st_ino,
        "mode": 0o600,
        "exists": True,
    }
    lab_daemon.verify_lab_runtime_prepared(
        runtime,
        checkout_root=tmp_path,
        expected_commit="b" * 40,
        managed_directories=directories,
        managed_files=files,
        legacy_paths={},
    )

    displaced = runtime / "lab_jobs.displaced.sqlite3"
    database.rename(displaced)
    database.write_bytes(b"replacement")
    database.chmod(0o600)
    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="prepared sentinel binding changed",
    ):
        lab_daemon.verify_lab_runtime_prepared(
            runtime,
            checkout_root=tmp_path,
            expected_commit="b" * 40,
            managed_directories=directories,
            managed_files=files,
            legacy_paths={},
        )


def test_prepared_runtime_sentinel_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    from rquant import lab_daemon

    runtime = tmp_path / "lab-runtime"
    commands = runtime / "commands"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": commands},
        managed_files={},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    sentinel = lab_daemon.lab_runtime_prepared_path(runtime)
    original = sentinel.read_text(encoding="utf-8").lstrip()
    sentinel.write_text(
        '{"schema_version":2,' + original[1:],
        encoding="utf-8",
    )
    sentinel.chmod(0o600)

    with pytest.raises(LabDaemonConfigurationError, match="duplicate|malformed|unavailable"):
        lab_daemon.verify_lab_runtime_prepared(
            runtime,
            checkout_root=tmp_path,
            expected_commit="a" * 40,
            managed_directories={"commands": commands},
            managed_files={},
            legacy_paths={},
        )


def test_prepared_sentinel_read_rejects_runtime_root_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon

    runtime = tmp_path / "lab-runtime"
    commands = runtime / "commands"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": commands},
        managed_files={},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    sentinel = lab_daemon.lab_runtime_prepared_path(runtime)
    sentinel_identity = sentinel.stat()
    displaced = tmp_path / "lab-runtime.displaced"
    original_read = os.read
    swapped = False

    def replace_root(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        opened = os.fstat(descriptor)
        if not swapped and (opened.st_dev, opened.st_ino) == (
            sentinel_identity.st_dev,
            sentinel_identity.st_ino,
        ):
            swapped = True
            runtime.rename(displaced)
            runtime.mkdir(mode=0o700)
            (displaced / sentinel.name).rename(runtime / sentinel.name)
        return original_read(descriptor, size)

    monkeypatch.setattr(lab_daemon.os, "read", replace_root)

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="runtime root (?:ancestor )?identity changed",
    ):
        lab_daemon._read_runtime_prepared_sentinel_record(runtime)


def test_prepared_sentinel_rejects_symlink_in_runtime_ancestor_chain(
    tmp_path: Path,
) -> None:
    from rquant import lab_daemon

    physical_parent = tmp_path / "physical"
    runtime = physical_parent / "lab-runtime"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": runtime / "commands"},
        managed_files={},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    alias = tmp_path / "alias"
    alias.symlink_to(physical_parent, target_is_directory=True)

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="ancestor|physical path|symlink",
    ):
        lab_daemon._read_runtime_prepared_sentinel_record(alias / "lab-runtime")


def test_prepared_sentinel_read_rejects_ancestor_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon

    authority_root = tmp_path / "authority"
    runtime = authority_root / "lab-runtime"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": runtime / "commands"},
        managed_files={},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    sentinel_identity = lab_daemon.lab_runtime_prepared_path(runtime).stat()
    displaced = tmp_path / "authority.displaced"
    original_read = os.read
    swapped = False

    def replace_ancestor(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        opened = os.fstat(descriptor)
        if not swapped and (opened.st_dev, opened.st_ino) == (
            sentinel_identity.st_dev,
            sentinel_identity.st_ino,
        ):
            swapped = True
            authority_root.rename(displaced)
            authority_root.mkdir(mode=0o700)
            replacement = authority_root / "lab-runtime"
            replacement.mkdir(mode=0o700)
            shutil.copy2(
                displaced / "lab-runtime" / ".prepared.json",
                replacement / ".prepared.json",
            )
        return original_read(descriptor, size)

    monkeypatch.setattr(lab_daemon.os, "read", replace_ancestor)

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="ancestor|runtime root identity changed",
    ):
        lab_daemon._read_runtime_prepared_sentinel_record(runtime)


def test_first_sqlite_registration_rejects_runtime_root_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon

    runtime = tmp_path / "lab-runtime"
    commands = runtime / "commands"
    database = runtime / "lab_jobs.sqlite3"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": commands},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    database.write_bytes(b"sqlite-authority")
    database.chmod(0o600)
    sentinel = lab_daemon.lab_runtime_prepared_path(runtime)
    sentinel_before = sentinel.read_bytes()
    sentinel_identity = sentinel.stat()
    displaced = tmp_path / "lab-runtime.displaced"
    original_read = os.read
    swapped = False

    def replace_root(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        opened = os.fstat(descriptor)
        if not swapped and (opened.st_dev, opened.st_ino) == (
            sentinel_identity.st_dev,
            sentinel_identity.st_ino,
        ):
            swapped = True
            runtime.rename(displaced)
            runtime.mkdir(mode=0o700)
            (displaced / sentinel.name).rename(runtime / sentinel.name)
            (displaced / database.name).rename(runtime / database.name)
        return original_read(descriptor, size)

    monkeypatch.setattr(lab_daemon.os, "read", replace_root)

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="runtime root (?:ancestor )?identity changed",
    ):
        lab_daemon.register_lab_runtime_managed_file(
            runtime,
            label="lab jobs SQLite",
            path=database,
            mutation_guard=lambda: "b" * 40,
        )

    assert sentinel.read_bytes() == sentinel_before


def test_first_sqlite_registration_opens_database_from_trusted_runtime_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon

    runtime = tmp_path / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": runtime / "commands"},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    database.write_bytes(b"sqlite-authority")
    database.chmod(0o600)
    original_open = os.open
    database_opened_from_runtime_fd = False

    def trace_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal database_opened_from_runtime_fd
        if path == database.name and dir_fd is not None:
            database_opened_from_runtime_fd = True
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(lab_daemon.os, "open", trace_open)

    registered = lab_daemon.register_lab_runtime_managed_file(
        runtime,
        label="lab jobs SQLite",
        path=database,
        mutation_guard=lambda: "b" * 40,
    )

    assert database_opened_from_runtime_fd
    assert registered["managed_files"]["lab jobs SQLite"]["inode"] == database.stat().st_ino


def test_first_sqlite_registration_rejects_sentinel_runtime_identity_before_database_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon

    runtime = tmp_path / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": runtime / "commands"},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    database.write_bytes(b"sqlite-authority")
    database.chmod(0o600)
    sentinel = lab_daemon.lab_runtime_prepared_path(runtime)
    payload = json.loads(sentinel.read_text(encoding="utf-8"))
    payload["runtime_inode"] += 1
    _write_private_json(sentinel, payload)
    original_open = os.open
    database_opened = False

    def trace_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal database_opened
        if path == database.name:
            database_opened = True
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(lab_daemon.os, "open", trace_open)

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="runtime.*identity|prepared sentinel binding",
    ):
        lab_daemon.register_lab_runtime_managed_file(
            runtime,
            label="lab jobs SQLite",
            path=database,
            mutation_guard=lambda: "b" * 40,
        )

    assert not database_opened


def test_atomic_first_sqlite_prepare_rejects_ancestor_replacement_without_database_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon

    parent = tmp_path / "authority"
    runtime = parent / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": runtime / "commands"},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    displaced = tmp_path / "authority.displaced"
    original_read = lab_daemon._read_runtime_prepared_sentinel_record
    swapped = False

    def replace_after_preflight(*args: object, **kwargs: object) -> object:
        nonlocal swapped
        result = original_read(*args, **kwargs)
        if not swapped:
            swapped = True
            parent.rename(displaced)
            parent.mkdir(mode=0o700)
            replacement = parent / "lab-runtime"
            replacement.mkdir(mode=0o700)
            shutil.copy2(
                displaced / "lab-runtime" / ".prepared.json",
                replacement / ".prepared.json",
            )
        return result

    monkeypatch.setattr(
        lab_daemon,
        "_read_runtime_prepared_sentinel_record",
        replace_after_preflight,
    )

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="ancestor|runtime root.*identity",
    ):
        authority = lab_daemon.prepare_lab_runtime_sqlite_authority(
            runtime,
            label="lab jobs SQLite",
            path=database,
            mutation_guard=lambda: "b" * 40,
        )
        authority.close()

    assert not database.exists()
    assert not (displaced / "lab-runtime" / database.name).exists()


def test_atomic_first_sqlite_prepare_removes_created_database_when_registration_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon

    parent = tmp_path / "authority"
    runtime = parent / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": runtime / "commands"},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    displaced = tmp_path / "authority.displaced"
    original_write = lab_daemon._write_runtime_prepared_sentinel

    def replace_ancestor_before_registration(*args: object, **kwargs: object) -> object:
        parent.rename(displaced)
        parent.mkdir(mode=0o700)
        replacement = parent / "lab-runtime"
        replacement.mkdir(mode=0o700)
        return original_write(*args, **kwargs)

    monkeypatch.setattr(
        lab_daemon,
        "_write_runtime_prepared_sentinel",
        replace_ancestor_before_registration,
    )

    with pytest.raises(lab_daemon.LabDaemonConfigurationError, match="identity|ancestor"):
        lab_daemon.prepare_lab_runtime_sqlite_authority(
            runtime,
            label="lab jobs SQLite",
            path=database,
            mutation_guard=lambda: "b" * 40,
        )

    assert not (displaced / "lab-runtime" / database.name).exists()
    assert not database.exists()


def test_atomic_sqlite_registration_failure_never_unlinks_existing_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import lab_daemon

    runtime = tmp_path / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": runtime / "commands"},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    database.write_bytes(b"existing-database")
    database.chmod(0o600)
    before = (database.read_bytes(), database.stat().st_ino)
    monkeypatch.setattr(
        lab_daemon,
        "_write_runtime_prepared_sentinel",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            lab_daemon.LabDaemonConfigurationError("registration failed")
        ),
    )

    with pytest.raises(lab_daemon.LabDaemonConfigurationError, match="registration failed"):
        lab_daemon.prepare_lab_runtime_sqlite_authority(
            runtime,
            label="lab jobs SQLite",
            path=database,
            mutation_guard=lambda: "b" * 40,
        )

    assert (database.read_bytes(), database.stat().st_ino) == before


def test_private_lab_runtime_layout_refuses_legacy_target_conflict(tmp_path: Path) -> None:
    from rquant import lab_daemon

    data = tmp_path / "data"
    data.mkdir(mode=0o755)
    runtime = data / "lab-runtime"
    runtime.mkdir(mode=0o700)
    target = runtime / "commands"
    target.mkdir(mode=0o700)
    legacy = data / "lab_job_commands"
    legacy.mkdir(mode=0o700)

    with pytest.raises(lab_daemon.LabDaemonConfigurationError, match="both exist"):
        lab_daemon.prepare_lab_runtime_layout(
            runtime,
            checkout_root=tmp_path,
            managed_directories={"commands": target},
            managed_files={},
            legacy_paths={target: legacy},
            mutation_guard=lambda: "a" * 40,
        )


def test_private_lab_runtime_layout_refuses_live_legacy_sqlite_sidecars(
    tmp_path: Path,
) -> None:
    from rquant import lab_daemon

    data = tmp_path / "data"
    data.mkdir(mode=0o755)
    legacy_database = data / "lab_jobs.sqlite3"
    legacy_database.write_bytes(b"sqlite")
    legacy_database.chmod(0o600)
    legacy_wal = data / "lab_jobs.sqlite3-wal"
    legacy_wal.write_bytes(b"live-wal")
    legacy_wal.chmod(0o600)
    runtime = data / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="checkpoint.*SQLite sidecars",
    ):
        lab_daemon.prepare_lab_runtime_layout(
            runtime,
            checkout_root=tmp_path,
            managed_directories={"commands": runtime / "commands"},
            managed_files={"lab jobs SQLite": database},
            legacy_paths={database: legacy_database},
            mutation_guard=lambda: "a" * 40,
        )

    assert legacy_database.read_bytes() == b"sqlite"
    assert legacy_wal.read_bytes() == b"live-wal"
    assert not database.exists()


def test_private_lab_runtime_layout_refuses_hot_legacy_sqlite_journal(
    tmp_path: Path,
) -> None:
    from rquant import lab_daemon

    data = tmp_path / "data"
    data.mkdir(mode=0o755)
    legacy_database = data / "lab_jobs.sqlite3"
    legacy_database.write_bytes(b"sqlite")
    legacy_database.chmod(0o600)
    legacy_journal = data / "lab_jobs.sqlite3-journal"
    legacy_journal.write_bytes(b"hot-journal")
    legacy_journal.chmod(0o600)
    runtime = data / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="checkpoint.*SQLite sidecars",
    ):
        lab_daemon.prepare_lab_runtime_layout(
            runtime,
            checkout_root=tmp_path,
            managed_directories={"commands": runtime / "commands"},
            managed_files={"lab jobs SQLite": database},
            legacy_paths={database: legacy_database},
            mutation_guard=lambda: "a" * 40,
        )

    assert legacy_database.read_bytes() == b"sqlite"
    assert legacy_journal.read_bytes() == b"hot-journal"
    assert not database.exists()


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_private_lab_runtime_layout_refuses_hot_target_sqlite_sidecars(
    tmp_path: Path,
    suffix: str,
) -> None:
    from rquant import lab_daemon

    data = tmp_path / "data"
    data.mkdir(mode=0o755)
    runtime = data / "lab-runtime"
    runtime.mkdir(mode=0o700)
    database = runtime / "lab_jobs.sqlite3"
    database.write_bytes(b"sqlite")
    database.chmod(0o600)
    sidecar = Path(f"{database}{suffix}")
    sidecar.write_bytes(b"hot")
    sidecar.chmod(0o600)

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="checkpoint.*SQLite sidecars",
    ):
        lab_daemon.prepare_lab_runtime_layout(
            runtime,
            checkout_root=tmp_path,
            managed_directories={"commands": runtime / "commands"},
            managed_files={"lab jobs SQLite": database},
            legacy_paths={},
            mutation_guard=lambda: "a" * 40,
        )

    assert sidecar.read_bytes() == b"hot"
    assert not lab_daemon.lab_runtime_prepared_path(runtime).exists()


def test_prepared_runtime_verification_rejects_new_target_sqlite_sidecar(
    tmp_path: Path,
) -> None:
    from rquant import lab_daemon

    runtime = tmp_path / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    commands = runtime / "commands"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": commands},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    database.write_bytes(b"sqlite")
    database.chmod(0o600)
    wal = Path(f"{database}-wal")
    wal.write_bytes(b"hot")
    wal.chmod(0o600)

    with pytest.raises(
        lab_daemon.LabDaemonConfigurationError,
        match="checkpoint.*SQLite sidecars",
    ):
        lab_daemon.verify_lab_runtime_prepared(
            runtime,
            checkout_root=tmp_path,
            expected_commit="a" * 40,
            managed_directories={"commands": commands},
            managed_files={"lab jobs SQLite": database},
            legacy_paths={},
        )


def test_lab_runtime_prepared_sentinel_rejects_tampering_and_legacy_split(
    tmp_path: Path,
) -> None:
    from rquant import lab_daemon

    data = tmp_path / "data"
    data.mkdir(mode=0o755)
    runtime = data / "lab-runtime"
    database = runtime / "lab_jobs.sqlite3"
    commands = runtime / "commands"
    legacy_database = data / "lab_jobs.sqlite3"
    lab_daemon.prepare_lab_runtime_layout(
        runtime,
        checkout_root=tmp_path,
        managed_directories={"commands": commands},
        managed_files={"lab jobs SQLite": database},
        legacy_paths={database: legacy_database},
        mutation_guard=lambda: "b" * 40,
    )
    sentinel = lab_daemon.lab_runtime_prepared_path(runtime)
    payload = json.loads(sentinel.read_text(encoding="utf-8"))
    authority_id = payload["runtime_authority_id"]
    payload["runtime_authority_id"] = "invalid"
    _write_private_json(sentinel, payload)

    with pytest.raises(lab_daemon.LabDaemonConfigurationError, match="prepared sentinel"):
        lab_daemon.verify_lab_runtime_prepared(
            runtime,
            checkout_root=tmp_path,
            expected_commit="b" * 40,
            managed_directories={"commands": commands},
            managed_files={"lab jobs SQLite": database},
            legacy_paths={database: legacy_database},
        )

    payload["runtime_authority_id"] = authority_id
    _write_private_json(sentinel, payload)
    legacy_database.write_bytes(b"split")
    legacy_database.chmod(0o600)
    with pytest.raises(lab_daemon.LabDaemonConfigurationError, match="legacy.*still exists"):
        lab_daemon.verify_lab_runtime_prepared(
            runtime,
            checkout_root=tmp_path,
            expected_commit="b" * 40,
            managed_directories={"commands": commands},
            managed_files={"lab jobs SQLite": database},
            legacy_paths={database: legacy_database},
            allow_missing_files=frozenset({"lab jobs SQLite"}),
        )


def test_private_directory_runtime_ensure_creates_only_private_leaf(tmp_path: Path) -> None:
    path = tmp_path / "runtime" / "commands"
    prior_umask = os.umask(0o022)
    try:
        ensured = ensure_private_directory(path, label="command spool")
    finally:
        os.umask(prior_umask)

    assert ensured == path
    assert path.stat().st_mode & 0o777 == 0o700


def test_private_directory_runtime_ensure_checks_guard_inside_create_boundary(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runtime" / "commands"

    def mutation_guard() -> str:
        raise LabDaemonConfigurationError("runtime drifted before directory creation")

    with pytest.raises(LabDaemonConfigurationError, match="before directory creation"):
        ensure_private_directory(
            path,
            label="command spool",
            mutation_guard=mutation_guard,
        )

    assert not (tmp_path / "runtime").exists()


def test_prepare_private_sqlite_checks_guard_inside_create_boundary(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    path = parent / "lab-jobs.sqlite3"

    def mutation_guard() -> str:
        raise LabDaemonConfigurationError("runtime drifted before SQLite creation")

    with pytest.raises(LabDaemonConfigurationError, match="before SQLite creation"):
        prepare_private_sqlite_path(
            path,
            label="lab jobs SQLite",
            create=True,
            mutation_guard=mutation_guard,
        )

    assert not path.exists()


def test_daemon_lock_checks_guard_inside_namespace_create_boundary(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    root = parent / "locks"

    def mutation_guard() -> str:
        raise LabDaemonConfigurationError("runtime drifted before lock namespace creation")

    with pytest.raises(LabDaemonConfigurationError, match="lock namespace creation"):
        LabDaemonLock(root, "scheduler", mutation_guard=mutation_guard).acquire()

    assert not root.exists()


def test_private_directory_runtime_ensure_does_not_repair_public_directory(
    tmp_path: Path,
) -> None:
    path = tmp_path / "commands"
    path.mkdir(mode=0o755)

    with pytest.raises(LabDaemonConfigurationError, match="mode 0700"):
        ensure_private_directory(path, label="command spool")

    assert path.stat().st_mode & 0o777 == 0o755


def test_runtime_path_identity_gate_rejects_duplicate_inode(tmp_path: Path) -> None:
    first = tmp_path / "first.key"
    second = tmp_path / "second.key"
    _write_private(first, "payload")
    second.hardlink_to(first)

    with pytest.raises(LabDaemonConfigurationError, match="same filesystem identity"):
        require_unique_runtime_paths({"first": first, "second": second})


def test_runtime_path_identity_gate_rejects_case_alias_on_casefolding_filesystem(
    tmp_path: Path,
) -> None:
    first = tmp_path / "LabCommands"
    first.mkdir(mode=0o700)
    second = tmp_path / "labcommands"
    if not second.exists() or second.stat().st_ino != first.stat().st_ino:
        pytest.skip("test filesystem is case-sensitive")

    with pytest.raises(LabDaemonConfigurationError, match="same filesystem identity"):
        require_unique_runtime_paths({"commands": first, "claims": second})


def test_runtime_binding_rejects_package_from_another_checkout(tmp_path: Path) -> None:
    from rquant.lab_daemon import verify_lab_runtime_binding

    expected = tmp_path / "expected"
    imported = tmp_path / "imported"
    for root in (expected, imported):
        (root / "src" / "rquant").mkdir(parents=True)
        (root / ".venv" / "bin").mkdir(parents=True)
        (root / "src" / "rquant" / "__init__.py").touch()
        (root / ".venv" / "bin" / "python").touch()
        (root / ".venv" / "bin" / "rquant").touch()

    with pytest.raises(LabDaemonConfigurationError, match="package root"):
        verify_lab_runtime_binding(
            expected_checkout_root=expected,
            executable=expected / ".venv" / "bin" / "python",
            launcher=expected / ".venv" / "bin" / "rquant",
            virtualenv_prefix=expected / ".venv",
            console_interpreter=expected / ".venv" / "bin" / "python",
            package_file=imported / "src" / "rquant" / "__init__.py",
            working_directory=expected,
            verified_code_sha="1" * 40,
            git_top_level=expected,
            git_head="1" * 40,
        )


def test_runtime_binding_rejects_symlinked_checkout_virtualenv(tmp_path: Path) -> None:
    from rquant.lab_daemon import verify_lab_runtime_binding

    expected = tmp_path / "expected"
    shared_venv = tmp_path / "shared-venv"
    (expected / "src" / "rquant").mkdir(parents=True)
    (shared_venv / "bin").mkdir(parents=True)
    package_file = expected / "src" / "rquant" / "__init__.py"
    executable = expected / ".venv" / "bin" / "python"
    launcher = expected / ".venv" / "bin" / "rquant"
    package_file.touch()
    (shared_venv / "bin" / "python").touch()
    (shared_venv / "bin" / "rquant").touch()
    (expected / ".venv").symlink_to(shared_venv, target_is_directory=True)

    with pytest.raises(LabDaemonConfigurationError, match="physical virtualenv"):
        verify_lab_runtime_binding(
            expected_checkout_root=expected,
            executable=executable,
            launcher=launcher,
            virtualenv_prefix=expected / ".venv",
            console_interpreter=executable,
            package_file=package_file,
            working_directory=expected,
            verified_code_sha="1" * 40,
            git_top_level=expected,
            git_head="1" * 40,
        )


def test_runtime_binding_rejects_symlinked_venv_before_git_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_daemon import require_lab_runtime_binding

    expected = tmp_path / "expected"
    shared_venv = tmp_path / "shared-venv"
    expected.mkdir()
    shared_venv.mkdir()
    (expected / ".venv").symlink_to(shared_venv, target_is_directory=True)

    def reject_probe(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Git probe must not run before the physical venv gate")

    monkeypatch.setattr("rquant.lab_daemon.subprocess.run", reject_probe)
    with pytest.raises(LabDaemonConfigurationError, match="physical virtualenv"):
        require_lab_runtime_binding(expected)


def test_runtime_binding_reuses_one_deadline_across_git_probes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_daemon as daemon_module
    import rquant.research_manifest as manifest_module

    expected = tmp_path / "expected"
    expected.mkdir()
    deadline = 11.0
    now = 10.0
    launched: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        daemon_module,
        "_require_physical_checkout_virtualenv",
        lambda root: (root, root / ".venv"),
    )
    monkeypatch.setattr(daemon_module.time, "monotonic", lambda: now)
    monkeypatch.setattr(
        manifest_module,
        "bind_trusted_git_executable",
        lambda path: SimpleNamespace(path=path),
    )

    def probe(
        _binding: object,
        arguments: list[str],
        *,
        cwd: Path,
        text: bool = True,
        deadline_monotonic: float | None = None,
    ) -> object:
        del cwd, text
        nonlocal now
        assert deadline_monotonic == deadline
        if now >= deadline:
            raise subprocess.TimeoutExpired(arguments, 0)
        launched.append(tuple(arguments))
        now = deadline
        return SimpleNamespace(returncode=0, stdout=str(expected) + "\n")

    monkeypatch.setattr(manifest_module, "_run_trusted_git", probe)

    with pytest.raises(LabDaemonConfigurationError, match="Git probe failed"):
        daemon_module.require_lab_runtime_binding(
            expected,
            startup_deadline_monotonic=deadline,
        )

    assert launched == [("rev-parse", "--show-toplevel")]


@pytest.mark.parametrize("drift", ["head", "tracked", "ignored_native"])
def test_runtime_guard_rechecks_checkout_identity_between_ticks(
    tmp_path: Path,
    drift: str,
) -> None:
    import subprocess

    from rquant.research_manifest import detect_verified_code_commit

    repo = tmp_path / "checkout"
    package = repo / "src" / "rquant"
    package.mkdir(parents=True)
    (repo / ".gitignore").write_text("src/rquant/*.so\n", encoding="utf-8")
    tracked = package / "runtime.py"
    tracked.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "baseline"], cwd=repo, check=True, capture_output=True)
    startup_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    def verify(checkout: Path) -> str:
        observed = detect_verified_code_commit(checkout)
        if observed is None or observed.endswith("-dirty"):
            raise LabDaemonConfigurationError("runtime checkout is dirty")
        return observed

    guard = LabRuntimeGuard(repo, startup_sha, verifier=verify)
    assert guard.verify() == startup_sha
    if drift == "head":
        (repo / "next.txt").write_text("next\n", encoding="utf-8")
        subprocess.run(["git", "add", "next.txt"], cwd=repo, check=True)
        subprocess.run(["git", "commit", "-m", "next"], cwd=repo, check=True, capture_output=True)
    elif drift == "tracked":
        tracked.write_text("VALUE = 2\n", encoding="utf-8")
    else:
        (package / "runtime.so").write_bytes(b"native")

    with pytest.raises(LabDaemonConfigurationError, match="runtime"):
        guard.verify()


def test_runtime_guard_remains_bound_to_shared_deployment_generation(tmp_path: Path) -> None:
    root = tmp_path / "checkout"
    root.mkdir(mode=0o700)
    lock_root = tmp_path / ".rquant-deploy"
    lock_root.mkdir(mode=0o700)
    lock_path = lock_root / "checkout.lock"
    _write_private(lock_path, "")
    descriptor = os.open(lock_path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
    fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    sha = "1" * 40
    guard = LabRuntimeGuard(
        root,
        sha,
        verifier=lambda _root: sha,
        deployment_generation=sha,
        deployment_lock_path=lock_path,
        deployment_generation_fd=descriptor,
    )
    try:
        assert guard.verify() == sha
        displaced = lock_path.with_suffix(".displaced")
        lock_path.rename(displaced)
        _write_private(lock_path, "")
        with pytest.raises(LabDaemonConfigurationError, match="lock identity changed"):
            guard.verify()
    finally:
        os.close(descriptor)


@pytest.mark.parametrize(
    "mismatch",
    ["executable", "launcher", "prefix", "shebang", "cwd", "git", "sha"],
)
def test_runtime_binding_rejects_identity_mismatch(tmp_path: Path, mismatch: str) -> None:
    from rquant.lab_daemon import verify_lab_runtime_binding

    expected = tmp_path / "expected"
    other = tmp_path / "other"
    (expected / "src" / "rquant").mkdir(parents=True)
    (expected / ".venv" / "bin").mkdir(parents=True)
    other.mkdir()
    package_file = expected / "src" / "rquant" / "__init__.py"
    executable = expected / ".venv" / "bin" / "python"
    launcher = expected / ".venv" / "bin" / "rquant"
    for path in (package_file, executable, launcher):
        path.touch()
    values = {
        "expected_checkout_root": expected,
        "executable": executable,
        "launcher": launcher,
        "virtualenv_prefix": expected / ".venv",
        "console_interpreter": executable,
        "package_file": package_file,
        "working_directory": expected,
        "verified_code_sha": "1" * 40,
        "git_top_level": expected,
        "git_head": "1" * 40,
    }
    if mismatch == "executable":
        values["executable"] = other / "python"
    elif mismatch == "launcher":
        values["launcher"] = other / "rquant"
    elif mismatch == "prefix":
        values["virtualenv_prefix"] = other
    elif mismatch == "shebang":
        values["console_interpreter"] = other / "python"
    elif mismatch == "cwd":
        values["working_directory"] = other
    elif mismatch == "git":
        values["git_top_level"] = other
    else:
        values["git_head"] = "2" * 40

    with pytest.raises(LabDaemonConfigurationError, match="runtime binding"):
        verify_lab_runtime_binding(**values)


def _private_state_dir(tmp_path: Path) -> Path:
    path = tmp_path / "finalizer-state"
    path.mkdir(mode=0o700)
    return path


def _finalization_candidate(job_id: UUID, *, version: int = 1) -> SimpleNamespace:
    return SimpleNamespace(
        job_id=job_id,
        job_version=version,
        spec_hash=f"{job_id.int:064x}",
        updated_at=datetime(2026, 7, 27, 1, version, tzinfo=UTC),
    )


def test_finalizer_daemon_runs_a_bounded_tick_and_reports_first_error(
    tmp_path: Path,
) -> None:
    job_ids = (
        UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
    )

    class Reader:
        def list_finalization_candidates(self, *, limit: int, cursor: str | None = None):
            assert limit == 2
            assert cursor is None
            return SimpleNamespace(
                items=tuple(_finalization_candidate(job_id) for job_id in job_ids),
                has_more=False,
                next_cursor=None,
            )

    class Finalizer:
        def finalize(self, job_id: UUID):
            if job_id == job_ids[0]:
                return SimpleNamespace(status="published")
            raise ValueError("broken candidate")

    daemon = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=LabFinalizerStateStore(_private_state_dir(tmp_path)),
        max_jobs_per_tick=2,
        poll_interval_ms=10,
        failure_cooldown_seconds=10,
        failure_cooldown_max_seconds=60,
    )

    result = daemon.run_once()

    assert result.candidates == 2
    assert result.published == 1
    assert result.failed == 1
    assert result.first_error_type == "ValueError"
    assert result.first_error_message == "broken candidate"


def test_finalizer_runtime_drift_between_candidates_stops_without_state_ack(
    tmp_path: Path,
) -> None:
    job_ids = (
        UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
    )
    guard_calls = 0
    finalized: list[UUID] = []

    class Reader:
        def list_finalization_candidates(self, *, limit: int, cursor: str | None = None):
            return SimpleNamespace(
                items=tuple(_finalization_candidate(job_id) for job_id in job_ids),
                has_more=False,
                next_cursor=None,
            )

    class Finalizer:
        def finalize(self, job_id: UUID):
            finalized.append(job_id)
            return SimpleNamespace(status="published")

    def runtime_guard() -> str:
        nonlocal guard_calls
        guard_calls += 1
        if guard_calls >= 3:
            raise LabDaemonConfigurationError("runtime checkout drifted")
        return "1" * 40

    state_store = LabFinalizerStateStore(_private_state_dir(tmp_path))
    daemon = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=state_store,
        max_jobs_per_tick=2,
        poll_interval_ms=10,
        failure_cooldown_seconds=10,
        failure_cooldown_max_seconds=60,
        runtime_guard=runtime_guard,
    )

    with pytest.raises(LabDaemonConfigurationError, match="drifted"):
        daemon.run_once()

    assert finalized == [job_ids[0]]
    assert state_store.load().cursor is None


def test_finalizer_daemon_stop_prevents_busy_loop(tmp_path: Path) -> None:
    calls: list[str] = []

    class Reader:
        def list_finalization_candidates(self, *, limit: int, cursor: str | None = None):
            calls.append(f"read:{limit}")
            return SimpleNamespace(items=(), has_more=False, next_cursor=None)

    class Finalizer:
        def finalize(self, job_id: UUID):  # pragma: no cover - no candidates
            raise AssertionError(job_id)

    daemon = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=LabFinalizerStateStore(_private_state_dir(tmp_path)),
        max_jobs_per_tick=3,
        poll_interval_ms=10,
        failure_cooldown_seconds=10,
        failure_cooldown_max_seconds=60,
    )
    daemon.request_stop()

    daemon.run_forever()

    assert calls == []


def test_finalizer_persists_cursor_so_failed_first_page_does_not_starve_later_jobs(
    tmp_path: Path,
) -> None:
    failed_ids = (
        UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
    )
    healthy_id = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
    cursors: list[str | None] = []

    class Reader:
        def list_finalization_candidates(self, *, limit: int, cursor: str | None = None):
            cursors.append(cursor)
            if cursor is None:
                return SimpleNamespace(
                    items=tuple(_finalization_candidate(job_id) for job_id in failed_ids[:2]),
                    has_more=True,
                    next_cursor="page-2",
                )
            if cursor == "page-2":
                return SimpleNamespace(
                    items=(_finalization_candidate(failed_ids[2]),),
                    has_more=True,
                    next_cursor="page-3",
                )
            assert cursor == "page-3"
            return SimpleNamespace(
                items=(_finalization_candidate(healthy_id),),
                has_more=False,
                next_cursor=None,
            )

    finalized: list[UUID] = []

    class Finalizer:
        def finalize(self, job_id: UUID) -> SimpleNamespace:
            finalized.append(job_id)
            if job_id in failed_ids:
                raise RuntimeError("fixture failure")
            return SimpleNamespace(status="published")

    state_store = LabFinalizerStateStore(_private_state_dir(tmp_path))
    first = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=state_store,
        max_jobs_per_tick=2,
        poll_interval_ms=1,
        failure_cooldown_seconds=30,
        failure_cooldown_max_seconds=300,
    )
    assert first.run_once().failed == 2

    restarted = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=state_store,
        max_jobs_per_tick=2,
        poll_interval_ms=1,
        failure_cooldown_seconds=30,
        failure_cooldown_max_seconds=300,
    )
    assert restarted.run_once().failed == 1
    assert restarted.run_once().published == 1
    assert restarted.run_once().cooled_down == 2

    assert cursors == [None, "page-2", "page-3", None]
    assert healthy_id in finalized


def test_finalizer_restart_preserves_fingerprint_cooldown(tmp_path: Path) -> None:
    job_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    now = datetime(2026, 7, 27, 2, 0, tzinfo=UTC)

    class Reader:
        def list_finalization_candidates(self, *, limit: int, cursor: str | None = None):
            return SimpleNamespace(
                items=(_finalization_candidate(job_id),),
                has_more=False,
                next_cursor=None,
            )

    calls = 0

    class Finalizer:
        def finalize(self, _job_id: UUID) -> SimpleNamespace:
            nonlocal calls
            calls += 1
            raise RuntimeError("fixture failure")

    state_store = LabFinalizerStateStore(_private_state_dir(tmp_path))
    daemon = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=state_store,
        max_jobs_per_tick=1,
        poll_interval_ms=1,
        failure_cooldown_seconds=30,
        failure_cooldown_max_seconds=300,
        now_provider=lambda: now,
    )
    assert daemon.run_once().failed == 1
    restarted = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=state_store,
        max_jobs_per_tick=1,
        poll_interval_ms=1,
        failure_cooldown_seconds=30,
        failure_cooldown_max_seconds=300,
        now_provider=lambda: now + timedelta(seconds=10),
    )
    assert restarted.run_once().cooled_down == 1
    assert calls == 1


def test_finalizer_failure_capacity_evicts_stale_entries_and_reaches_next_page(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 27, 2, 0, tzinfo=UTC)
    failures = {
        str(UUID(int=index + 1)): LabFinalizerFailureState(
            fingerprint=f"{index:064x}",
            attempts=1,
            cooldown_until=now,
        )
        for index in range(4_096)
    }
    state_store = LabFinalizerStateStore(_private_state_dir(tmp_path))
    state_store.save(LabFinalizerDaemonState(cycle=1, failures=failures))
    failing_id = UUID(int=5_000)
    healthy_id = UUID(int=5_001)

    class Reader:
        def list_finalization_candidates(self, *, limit: int, cursor: str | None = None):
            assert limit == 1
            if cursor is None:
                return SimpleNamespace(
                    items=(_finalization_candidate(failing_id),),
                    has_more=True,
                    next_cursor="healthy-page",
                )
            assert cursor == "healthy-page"
            return SimpleNamespace(
                items=(_finalization_candidate(healthy_id),),
                has_more=False,
                next_cursor=None,
            )

    class Finalizer:
        def finalize(self, job_id: UUID) -> SimpleNamespace:
            if job_id == failing_id:
                raise RuntimeError("new failure")
            assert job_id == healthy_id
            return SimpleNamespace(status="published")

    first = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=state_store,
        max_jobs_per_tick=1,
        poll_interval_ms=1,
        failure_cooldown_seconds=30,
        failure_cooldown_max_seconds=300,
        now_provider=lambda: now,
    )
    assert first.run_once().failed == 1
    after_first = state_store.load()
    assert after_first.cursor == "healthy-page"
    assert len(after_first.failures) == 4_096
    assert str(failing_id) in after_first.failures

    restarted = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        state_store=state_store,
        max_jobs_per_tick=1,
        poll_interval_ms=1,
        failure_cooldown_seconds=30,
        failure_cooldown_max_seconds=300,
        now_provider=lambda: now + timedelta(seconds=1),
    )
    assert restarted.run_once().published == 1
    completed_cycle = state_store.load()
    assert completed_cycle.cursor is None
    assert completed_cycle.cycle == 2
    assert set(completed_cycle.failures) == {str(failing_id)}


def test_finalizer_corrupt_state_blocks_before_reader_access(tmp_path: Path) -> None:
    state_dir = _private_state_dir(tmp_path)
    state_path = state_dir / "state.json"
    state_path.write_text("not-json", encoding="utf-8")
    state_path.chmod(0o600)
    reads = 0

    class Reader:
        def list_finalization_candidates(self, *, limit: int, cursor: str | None = None):
            nonlocal reads
            reads += 1
            raise AssertionError("reader must not be touched")

    daemon = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=SimpleNamespace(finalize=lambda _job_id: None),
        state_store=LabFinalizerStateStore(state_dir),
        max_jobs_per_tick=1,
        poll_interval_ms=1,
        failure_cooldown_seconds=30,
        failure_cooldown_max_seconds=300,
    )
    with pytest.raises(LabDaemonConfigurationError, match="state is corrupt"):
        daemon.run_once()
    assert reads == 0


def test_finalizer_state_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=3))
    state_path = state_dir / "state.json"
    original = state_path.read_text(encoding="utf-8").lstrip()
    state_path.write_text('{"schema_version":1,' + original[1:], encoding="utf-8")
    state_path.chmod(0o600)

    with pytest.raises(LabDaemonConfigurationError, match="state is corrupt"):
        store.load()


@pytest.mark.parametrize(
    ("unsafe_kind", "message"),
    [("public", "0600"), ("symlink", "symlink"), ("hardlink", "hardlink")],
)
def test_finalizer_state_rejects_unsafe_file_identity(
    tmp_path: Path,
    unsafe_kind: str,
    message: str,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    state_path = state_dir / "state.json"
    victim = tmp_path / "victim.json"
    victim.write_text('{"schema_version":1}', encoding="utf-8")
    victim.chmod(0o600)
    if unsafe_kind == "public":
        state_path.write_text('{"schema_version":1}', encoding="utf-8")
        state_path.chmod(0o644)
    elif unsafe_kind == "symlink":
        state_path.symlink_to(victim)
    else:
        state_path.hardlink_to(victim)

    with pytest.raises(LabDaemonConfigurationError, match=message):
        LabFinalizerStateStore(state_dir).load()


def test_finalizer_state_rejects_hardlink_created_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=3))
    state_path = state_dir / "state.json"
    linked = state_dir / "linked-state.json"
    real_read = os.read
    linked_once = False

    def linking_read(descriptor: int, size: int) -> bytes:
        nonlocal linked_once
        payload = real_read(descriptor, size)
        if not linked_once:
            linked_once = True
            linked.hardlink_to(state_path)
        return payload

    monkeypatch.setattr("rquant.lab_daemon.os.read", linking_read)
    with pytest.raises(LabDaemonConfigurationError, match="hardlink"):
        store.load()


def test_finalizer_state_open_rejects_root_replaced_before_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    displaced = tmp_path / "displaced-before-open"
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=2))
    real_open = os.open
    swapped = False

    def replacing_open(path_arg: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal swapped
        if not swapped and Path(path_arg) == state_dir and kwargs.get("dir_fd") is None:
            swapped = True
            state_dir.rename(displaced)
            state_dir.mkdir(mode=0o700)
        return real_open(path_arg, flags, *args, **kwargs)

    monkeypatch.setattr("rquant.lab_daemon.os.open", replacing_open)
    with pytest.raises(LabDaemonConfigurationError, match="directory identity changed"):
        store.load()


def test_finalizer_state_missing_result_rechecks_root_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    displaced = tmp_path / "displaced-missing-state"
    store = LabFinalizerStateStore(state_dir)
    real_stat = os.stat
    swapped = False

    def replacing_stat(path_arg: object, *args: object, **kwargs: object) -> os.stat_result:
        nonlocal swapped
        if not swapped and path_arg == "state.json" and kwargs.get("dir_fd") is not None:
            swapped = True
            state_dir.rename(displaced)
            state_dir.mkdir(mode=0o700)
            raise FileNotFoundError(path_arg)
        return real_stat(path_arg, *args, **kwargs)

    monkeypatch.setattr("rquant.lab_daemon.os.stat", replacing_stat)
    with pytest.raises(LabDaemonConfigurationError, match="directory identity changed"):
        store.load()


def test_finalizer_state_read_rechecks_root_identity_before_return(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    displaced = tmp_path / "displaced-during-read"
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=3))
    real_read = os.read
    swapped = False

    def replacing_read(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        payload = real_read(descriptor, size)
        if not swapped:
            swapped = True
            state_dir.rename(displaced)
            state_dir.mkdir(mode=0o700)
        return payload

    monkeypatch.setattr("rquant.lab_daemon.os.read", replacing_read)
    with pytest.raises(LabDaemonConfigurationError, match="directory identity changed"):
        store.load()


def test_finalizer_state_load_rejects_same_size_active_file_replacement_after_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=3))
    state_path = state_dir / "state.json"
    displaced = state_dir / "original.json"
    original_assert_root = store._assert_root_current
    checks = 0

    def replace_after_parse(descriptor: int, expected: os.stat_result) -> None:
        nonlocal checks
        checks += 1
        if checks == 2:
            payload = state_path.read_bytes()
            state_path.rename(displaced)
            state_path.write_bytes(payload)
            state_path.chmod(0o600)
        original_assert_root(descriptor, expected)

    monkeypatch.setattr(store, "_assert_root_current", replace_after_parse)

    with pytest.raises(LabDaemonConfigurationError, match="state.*changed"):
        store.load()


def test_finalizer_state_load_rejects_concurrent_creation_after_missing_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    original_assert = store._assert_root_current
    checks = 0

    def create_after_missing(descriptor: int, expected: os.stat_result) -> None:
        nonlocal checks
        original_assert(descriptor, expected)
        checks += 1
        if checks == 2:
            store.path.write_bytes(canonical_model_json_bytes(LabFinalizerDaemonState(cycle=99)))
            store.path.chmod(0o600)

    monkeypatch.setattr(store, "_assert_root_current", create_after_missing)

    with pytest.raises(LabDaemonConfigurationError, match="appeared"):
        store.load()

    assert LabFinalizerDaemonState.model_validate_json(store.path.read_bytes()).cycle == 99


def test_finalizer_state_save_does_not_overwrite_concurrent_absent_state_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    concurrent = canonical_model_json_bytes(LabFinalizerDaemonState(cycle=99))
    original_assert = store._assert_root_current
    checks = 0

    def create_before_publish(descriptor: int, expected: os.stat_result) -> None:
        nonlocal checks
        original_assert(descriptor, expected)
        checks += 1
        if checks == 2:
            store.path.write_bytes(concurrent)
            store.path.chmod(0o600)

    monkeypatch.setattr(store, "_assert_root_current", create_before_publish)

    with pytest.raises(LabDaemonConfigurationError, match="concurrent|atomically"):
        store.save(LabFinalizerDaemonState(cycle=1))

    assert store.path.read_bytes() == concurrent


def test_finalizer_state_internal_mutation_fence_preserves_existing_state(
    tmp_path: Path,
) -> None:
    store = LabFinalizerStateStore(_private_state_dir(tmp_path))
    original = LabFinalizerDaemonState(cycle=7)
    store.save(original)
    calls = 0

    def mutation_guard() -> str:
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise LabDaemonConfigurationError("runtime drifted inside state lock")
        return "1" * 40

    mutation_guard()
    with pytest.raises(LabDaemonConfigurationError, match="inside state lock"):
        store.save(LabFinalizerDaemonState(cycle=8), mutation_guard=mutation_guard)

    assert store.load() == original


def test_finalizer_state_existing_concurrent_replacement_is_not_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LabFinalizerStateStore(_private_state_dir(tmp_path))
    store.save(LabFinalizerDaemonState(cycle=1))
    concurrent = canonical_model_json_bytes(LabFinalizerDaemonState(cycle=99))

    def replace_before_exchange(root_descriptor: int) -> None:
        replacement_name = ".concurrent-state.tmp"
        descriptor = os.open(
            replacement_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_descriptor,
        )
        try:
            os.write(descriptor, concurrent)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(
            replacement_name,
            store.path.name,
            src_dir_fd=root_descriptor,
            dst_dir_fd=root_descriptor,
        )

    monkeypatch.setattr(store, "_before_state_exchange", replace_before_exchange, raising=False)

    with pytest.raises(LabDaemonConfigurationError, match="concurrent|changed"):
        store.save(LabFinalizerDaemonState(cycle=2))

    assert store.path.read_bytes() == concurrent


def test_finalizer_state_recovery_does_not_overwrite_concurrent_legal_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=1))
    concurrent = canonical_model_json_bytes(LabFinalizerDaemonState(cycle=99))
    real_replace = os.replace
    injected = False

    def replace_then_update(*args: object, **kwargs: object) -> None:
        nonlocal injected
        real_replace(*args, **kwargs)
        if not injected and args[1] == store.path.name:
            injected = True
            replacement = state_dir / ".concurrent-legal-state.tmp"
            replacement.write_bytes(concurrent)
            replacement.chmod(0o600)
            real_replace(replacement, store.path)

    monkeypatch.setattr("rquant.lab_daemon.os.replace", replace_then_update)

    with pytest.raises(LabDaemonConfigurationError, match="identity changed after commit"):
        store.save(LabFinalizerDaemonState(cycle=2))

    assert store.load().cycle == 99


def test_finalizer_state_post_publish_drift_blocks_compensation_mutations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=1))
    real_replace = os.replace
    real_assert_root = store._assert_root_current
    drifted = False

    def replace_then_drift(*args: object, **kwargs: object) -> None:
        nonlocal drifted
        real_replace(*args, **kwargs)
        if args[1] == store.path.name:
            drifted = True

    def fail_post_publish(descriptor: int, identity: os.stat_result) -> None:
        if drifted:
            raise LabDaemonConfigurationError("post-publish validation failed")
        real_assert_root(descriptor, identity)

    def mutation_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("stale runtime compensation blocked")
        return "1" * 40

    monkeypatch.setattr("rquant.lab_daemon.os.replace", replace_then_drift)
    monkeypatch.setattr(store, "_assert_root_current", fail_post_publish)

    with pytest.raises(BaseException, match="stale runtime compensation blocked"):
        store.save(
            LabFinalizerDaemonState(cycle=2),
            mutation_guard=mutation_guard,
        )

    persisted = LabFinalizerDaemonState.model_validate_json(store.path.read_bytes())
    assert persisted.cycle == 2
    assert not tuple(state_dir.glob(".state.restore.*"))
    assert not tuple(state_dir.glob(".state.failed.*"))


def test_finalizer_state_save_preserves_same_size_concurrent_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=7))
    state_path = state_dir / "state.json"
    real_replace = os.replace
    attacked = False
    attacker_payload = b" " * len(state_path.read_bytes())

    def replace_then_attack(*args: object, **kwargs: object) -> None:
        nonlocal attacked
        real_replace(*args, **kwargs)
        if not attacked and args[1] == "state.json":
            attacked = True
            attacker = state_dir / "attacker.json"
            attacker.write_bytes(attacker_payload)
            attacker.chmod(0o600)
            real_replace(attacker, state_path)

    monkeypatch.setattr("rquant.lab_daemon.os.replace", replace_then_attack)

    with pytest.raises(LabDaemonConfigurationError, match="identity changed after commit"):
        store.save(LabFinalizerDaemonState(cycle=8))

    assert state_path.read_bytes() == attacker_payload


def test_finalizer_state_save_rejects_root_replacement_after_atomic_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=1))
    displaced = tmp_path / "displaced-state"
    real_replace = os.replace

    def replacing_root(
        source: str,
        destination: str,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        real_replace(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )
        state_dir.rename(displaced)
        state_dir.mkdir(mode=0o700)

    monkeypatch.setattr("rquant.lab_daemon.os.replace", replacing_root)
    with pytest.raises(LabDaemonConfigurationError, match="directory identity changed"):
        store.save(LabFinalizerDaemonState(cycle=2))

    assert (displaced / "state.json").is_file()
    assert not (state_dir / "state.json").exists()


def test_finalizer_state_save_rejects_root_replacement_before_atomic_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=1))
    original = (state_dir / "state.json").read_bytes()
    displaced = tmp_path / "displaced-before-commit"
    real_fsync = os.fsync
    swapped = False

    def replacing_root(descriptor: int) -> None:
        nonlocal swapped
        real_fsync(descriptor)
        if not swapped:
            swapped = True
            state_dir.rename(displaced)
            state_dir.mkdir(mode=0o700)

    monkeypatch.setattr("rquant.lab_daemon.os.fsync", replacing_root)
    with pytest.raises(LabDaemonConfigurationError, match="directory identity changed"):
        store.save(LabFinalizerDaemonState(cycle=2))

    assert (displaced / "state.json").read_bytes() == original
    assert not (state_dir / "state.json").exists()


def test_finalizer_state_save_failure_preserves_previous_valid_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = _private_state_dir(tmp_path)
    store = LabFinalizerStateStore(state_dir)
    store.save(LabFinalizerDaemonState(cycle=4))
    state_path = state_dir / "state.json"
    original = state_path.read_bytes()

    def reject_replace(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected replace failure")

    monkeypatch.setattr("rquant.lab_daemon.os.replace", reject_replace)
    with pytest.raises(LabDaemonConfigurationError, match="committed atomically"):
        store.save(LabFinalizerDaemonState(cycle=5))

    assert state_path.read_bytes() == original
    assert store.load().cycle == 4


def test_finalizer_failure_cooldown_requires_aware_datetime_and_normalizes_utc() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        LabFinalizerFailureState(
            fingerprint="a" * 64,
            attempts=1,
            cooldown_until=datetime(2026, 7, 27, 1, 0),
        )

    observed = LabFinalizerFailureState(
        fingerprint="a" * 64,
        attempts=1,
        cooldown_until=datetime.fromisoformat("2026-07-27T09:00:00+08:00"),
    )
    assert observed.cooldown_until == datetime(2026, 7, 27, 1, 0, tzinfo=UTC)
    assert observed.cooldown_until.tzinfo is UTC
