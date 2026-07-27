from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from rquant.lab_artifact_protocol import LabFinalizerAuthorityKey
from rquant.lab_daemon import (
    LabAuthorityKeyring,
    LabDaemonConfigurationError,
    LabDaemonLock,
    LabFinalizerDaemon,
    require_clean_code_sha,
    require_private_directory,
)


def _write_private(path: Path, payload: str) -> None:
    path.write_text(payload, encoding="ascii")
    path.chmod(0o600)


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


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o606])
def test_authority_keyring_rejects_non_private_key_files(
    tmp_path: Path,
    mode: int,
) -> None:
    key = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(key, "61" * 32 + "\n")
    _write_private(ring, '{"schema_version":1,"keys":{"active":"' + "61" * 32 + '"}}\n')
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
    _write_private(ring, '{"schema_version":1,"keys":{"active":"' + "61" * 32 + '"}}\n')

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
    _write_private(ring, '{"schema_version":1,"keys":{"active":"' + "61" * 32 + '"}}\n')
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
    _write_private(ring, '{"schema_version":1,"keys":{"active":"' + "62" * 32 + '"}}\n')

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


def test_private_directory_gate_rejects_public_or_symlinked_roots(tmp_path: Path) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    require_private_directory(private, label="command spool")

    private.chmod(0o755)
    with pytest.raises(LabDaemonConfigurationError, match="private permissions"):
        require_private_directory(private, label="command spool")

    private.chmod(0o700)
    linked = tmp_path / "linked"
    linked.symlink_to(private, target_is_directory=True)
    with pytest.raises(LabDaemonConfigurationError, match="real directory"):
        require_private_directory(linked, label="command spool")


def test_finalizer_daemon_runs_a_bounded_tick_and_reports_first_error() -> None:
    job_ids = (
        UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
    )

    class Reader:
        def list_finalization_candidates(self, *, limit: int):
            assert limit == 2
            return SimpleNamespace(
                items=tuple(SimpleNamespace(job_id=job_id) for job_id in job_ids),
            )

    class Finalizer:
        def finalize(self, job_id: UUID):
            if job_id == job_ids[0]:
                return SimpleNamespace(status="published")
            raise ValueError("broken candidate")

    daemon = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        max_jobs_per_tick=2,
        poll_interval_ms=10,
    )

    result = daemon.run_once()

    assert result.candidates == 2
    assert result.published == 1
    assert result.failed == 1
    assert result.first_error_type == "ValueError"
    assert result.first_error_message == "broken candidate"


def test_finalizer_daemon_stop_prevents_busy_loop() -> None:
    calls: list[str] = []

    class Reader:
        def list_finalization_candidates(self, *, limit: int):
            calls.append(f"read:{limit}")
            return SimpleNamespace(items=())

    class Finalizer:
        def finalize(self, job_id: UUID):  # pragma: no cover - no candidates
            raise AssertionError(job_id)

    daemon = LabFinalizerDaemon(
        reader=Reader(),
        finalizer=Finalizer(),
        max_jobs_per_tick=3,
        poll_interval_ms=10,
    )
    daemon.request_stop()

    daemon.run_forever()

    assert calls == []
