from __future__ import annotations

import fcntl
import json
import os
import sqlite3
from datetime import UTC, datetime, timedelta
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
    LabFinalizerStateStore,
    ensure_private_directory,
    prepare_private_sqlite_path,
    require_clean_code_sha,
    require_private_directory,
)
from rquant.lab_jobs import LabJobReader, LabJobStore


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


def test_authority_keyring_rejects_hardlinked_key_without_reading_it(tmp_path: Path) -> None:
    victim = tmp_path / "victim.key"
    key = tmp_path / "active.key"
    ring = tmp_path / "keyring.json"
    _write_private(victim, "61" * 32 + "\n")
    key.hardlink_to(victim)
    _write_private(ring, '{"schema_version":1,"keys":{"active":"' + "61" * 32 + '"}}\n')

    with pytest.raises(LabDaemonConfigurationError, match="hardlink"):
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
        if not swapped and Path(path) == lock_dir and kwargs.get("dir_fd") is None:
            swapped = True
            lock_dir.rename(original)
            lock_dir.mkdir(mode=0o700)
            (lock_dir / "scheduler.lock").symlink_to(replacement_victim)
        return descriptor

    monkeypatch.setattr("rquant.lab_daemon.os.open", swapping_open)
    with pytest.raises(LabDaemonConfigurationError, match="root identity changed"):
        LabDaemonLock(lock_dir, "scheduler").acquire()

    assert replacement_victim.read_text(encoding="utf-8") == "keep me\n"


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


def test_private_directory_runtime_ensure_creates_only_private_leaf(tmp_path: Path) -> None:
    path = tmp_path / "runtime" / "commands"
    prior_umask = os.umask(0o022)
    try:
        ensured = ensure_private_directory(path, label="command spool")
    finally:
        os.umask(prior_umask)

    assert ensured == path
    assert path.stat().st_mode & 0o777 == 0o700


def test_private_directory_runtime_ensure_does_not_repair_public_directory(
    tmp_path: Path,
) -> None:
    path = tmp_path / "commands"
    path.mkdir(mode=0o755)

    with pytest.raises(LabDaemonConfigurationError, match="private permissions"):
        ensure_private_directory(path, label="command spool")

    assert path.stat().st_mode & 0o777 == 0o755


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
