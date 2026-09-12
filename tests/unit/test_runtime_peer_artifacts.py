"""The rule that separates "the owner has not started" from "this artifact is broken".

Both used to end the same way — the process exited while its step was being built, and
`OnFailure=rquant-alert@%n.service` sent a push. The 2026-09-08 Route A window did that
ten times over one night for artifacts that were merely absent (#231, #232, #220).
"""

from __future__ import annotations

import gc
import sqlite3
from pathlib import Path

import pytest

from rquant.runtime_peer_artifacts import (
    DeferredPeerArtifact,
    PeerArtifactUnavailableError,
    dormant_wal_peer_wait,
    is_dormant_wal_database,
)


def _artifact(
    path: Path,
    *,
    opens: list[int] | None = None,
    fail_with: Exception | None = None,
) -> DeferredPeerArtifact[str]:
    calls = opens if opens is not None else []

    def open_artifact() -> str:
        calls.append(1)
        if fail_with is not None:
            raise fail_with
        return f"opened:{path.name}"

    return DeferredPeerArtifact(
        reader="signal_router",
        artifact="runner source",
        path=path,
        open_artifact=open_artifact,
    )


def test_an_absent_artifact_is_waited_for_by_name(tmp_path: Path) -> None:
    missing = tmp_path / "live" / "strategies" / "svc-1" / "runner.sqlite3"
    opens: list[int] = []
    artifact = _artifact(missing, opens=opens)

    assert artifact.probe() is None
    with pytest.raises(PeerArtifactUnavailableError) as raised:
        artifact.get()

    #: the heartbeat's `last_error` has to say which file, or the operator is guessing
    assert str(missing) in str(raised.value)
    assert raised.value.path == missing
    assert raised.value.reader == "signal_router"
    assert raised.value.artifact == "runner source"
    assert opens == []


def test_a_waiting_artifact_stays_a_value_error(tmp_path: Path) -> None:
    """`run_service_loop` catches `Exception` and the callers catch `ValueError`."""

    assert issubclass(PeerArtifactUnavailableError, ValueError)


def test_an_artifact_that_appears_later_is_opened_exactly_once(tmp_path: Path) -> None:
    path = tmp_path / "runner.sqlite3"
    opens: list[int] = []
    artifact = _artifact(path, opens=opens)

    assert artifact.probe() is None
    path.write_bytes(b"")

    assert artifact.get() == "opened:runner.sqlite3"
    assert artifact.get() == "opened:runner.sqlite3"
    assert opens == [1]


def test_an_artifact_already_on_disk_is_opened_while_the_step_is_built(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runner.sqlite3"
    path.write_bytes(b"")
    opens: list[int] = []
    artifact = _artifact(path, opens=opens)

    assert artifact.probe() == "opened:runner.sqlite3"
    assert opens == [1]


def test_an_artifact_that_exists_and_does_not_open_still_fails_closed(
    tmp_path: Path,
) -> None:
    """Absence defers. Anything present is judged by the opener's own checks, now."""

    path = tmp_path / "runner.sqlite3"
    path.write_bytes(b"")
    artifact = _artifact(path, fail_with=ValueError("runner source identity does not match"))

    with pytest.raises(ValueError, match="identity does not match"):
        artifact.probe()
    with pytest.raises(ValueError, match="identity does not match"):
        artifact.get()


def test_a_dangling_symlink_is_present_and_the_opener_judges_it(tmp_path: Path) -> None:
    """A path someone replaced with a link to nowhere is not "not created yet"."""

    path = tmp_path / "runner.sqlite3"
    path.symlink_to(tmp_path / "gone.sqlite3")
    artifact = _artifact(path, fail_with=ValueError("runner source path contains a symlink"))

    with pytest.raises(ValueError, match="symlink"):
        artifact.probe()


def test_a_missing_parent_directory_is_absence_not_breakage(tmp_path: Path) -> None:
    artifact = _artifact(tmp_path / "not-created-yet" / "runner.sqlite3")

    with pytest.raises(PeerArtifactUnavailableError):
        artifact.get()


def test_the_path_must_be_absolute_and_normalized(tmp_path: Path) -> None:
    for candidate in (Path("runner.sqlite3"), tmp_path / ".." / "runner.sqlite3"):
        with pytest.raises(ValueError, match="absolute and normalized"):
            DeferredPeerArtifact(
                reader="signal_router",
                artifact="runner source",
                path=candidate,
                open_artifact=lambda: "never",
            )


def test_the_reader_and_artifact_names_are_required(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        DeferredPeerArtifact(
            reader=" ",
            artifact="runner source",
            path=tmp_path / "runner.sqlite3",
            open_artifact=lambda: "never",
        )


@pytest.mark.parametrize("kind", ("parent-symlink-loop", "file-as-parent"))
def test_a_path_that_cannot_be_stat_ed_is_present_not_missing(
    tmp_path: Path,
    kind: str,
) -> None:
    """`FileNotFoundError` is the only answer that means "the owner has not written it".

    Everything else `lstat` can raise describes something that is there and wrong: a
    symlink loop where the artifact belongs, a parent segment somebody replaced with a
    file. Counting those as "not created yet" would turn a substituted artifact into a
    silent, permanent wait, which is the fail-closed boundary this package must not move.
    """

    if kind == "parent-symlink-loop":
        #: ELOOP: `lstat` does not follow the last component, so the loop has to be in a
        #: parent segment to be the thing that raises
        (tmp_path / "strategies").symlink_to(tmp_path / "live")
        (tmp_path / "live").symlink_to(tmp_path / "strategies")
        path = tmp_path / "strategies" / "runner.sqlite3"
    else:
        (tmp_path / "strategies").write_bytes(b"not a directory")
        path = tmp_path / "strategies" / "runner.sqlite3"

    with pytest.raises(OSError) as observed:
        path.lstat()
    assert not isinstance(observed.value, FileNotFoundError)

    artifact = _artifact(path, fail_with=ValueError("runner source is unusable"))
    assert artifact.exists is True
    with pytest.raises(ValueError, match="unusable"):
        artifact.probe()
    with pytest.raises(ValueError, match="unusable"):
        artifact.get()


# ---------------------------------------------------------------------------------------
# #252 / #263: a WAL database whose owner is not running, in a directory we cannot write
# ---------------------------------------------------------------------------------------


def _dormant_wal_database(tmp_path: Path) -> Path:
    """A real WAL database, cleanly closed, in a directory this process cannot write.

    That is what a cleanly stopped `paper_broker` leaves in `live/paper-brokers/<svc>/`
    for `strategy_live` (#252) and what a cleanly stopped `strategy_live` leaves in
    `live/strategies/<svc>/` for `signal_router` (#263): SQLite checkpoints and removes
    `-wal` and `-shm` when the last connection closes, and both readers mount the
    directory read-only. The caller chmods the directory, so the file is left writable
    here and every shape below can be written before the wait is judged.
    """

    directory = tmp_path / "live" / "svc"
    directory.mkdir(parents=True)
    path = directory / "owner.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("CREATE TABLE owned (a INTEGER)")
        connection.commit()
    finally:
        connection.close()
    gc.collect()
    assert not path.with_name(f"{path.name}-wal").exists()
    assert not path.with_name(f"{path.name}-shm").exists()
    return path


@pytest.mark.parametrize(
    "shape",
    (
        "stopped_owner",
        "absent",
        "not_sqlite",
        "truncated_header",
        "rollback_journal",
        "wal_sidecar_present",
        "shm_sidecar_present",
        "writable_directory",
        "unreadable_header",
    ),
)
def test_only_the_stopped_owner_shape_is_a_dormant_wal_database(
    tmp_path: Path,
    shape: str,
) -> None:
    """The whole contract of the judgement both readers share, shape by shape.

    `unable to open database file` is what SQLite says for a stopped owner *and* for
    several things that are genuinely wrong, and the API gives no way to tell them apart
    -- on macOS the same state answers `attempt to write a readonly database` instead, so
    the message cannot be part of the rule at all. The shape on disk is, and only the
    exact stopped-owner shape may ever become a wait: everything else must keep failing
    closed in the reader that asked.
    """

    path = _dormant_wal_database(tmp_path)
    if shape == "absent":
        path.unlink()
    elif shape == "not_sqlite":
        payload = bytearray(path.read_bytes())
        payload[:16] = b"NotSQLite fmt 3\x00"
        path.write_bytes(bytes(payload))
    elif shape == "truncated_header":
        path.write_bytes(path.read_bytes()[:8])
    elif shape == "rollback_journal":
        connection = sqlite3.connect(path)
        try:
            connection.execute("PRAGMA journal_mode = DELETE")
        finally:
            connection.close()
        gc.collect()
    elif shape == "wal_sidecar_present":
        path.with_name(f"{path.name}-wal").write_bytes(b"")
    elif shape == "shm_sidecar_present":
        path.with_name(f"{path.name}-shm").write_bytes(b"")
    elif shape == "unreadable_header":
        path.chmod(0o000)

    if shape != "writable_directory":
        path.parent.chmod(0o500)
    try:
        assert is_dormant_wal_database(path) is (shape == "stopped_owner"), shape
    finally:
        path.parent.chmod(0o700)
        if shape == "unreadable_header":
            path.chmod(0o600)


def test_the_wait_a_stopped_owner_earns_names_the_reader_the_artifact_and_the_path(
    tmp_path: Path,
) -> None:
    """`waiting_for` in the heartbeat is `str(error.path)` (`runtime_service_control`)."""

    path = _dormant_wal_database(tmp_path)
    path.parent.chmod(0o500)
    try:
        pending = dormant_wal_peer_wait(
            reader="signal_router",
            artifact="runner source",
            path=path,
            owner="strategy",
        )
    finally:
        path.parent.chmod(0o700)

    assert isinstance(pending, PeerArtifactUnavailableError)
    assert pending.path == path
    assert pending.reader == "signal_router"
    assert pending.artifact == "runner source"
    assert "wal" in str(pending).lower()
    assert "stopped strategy" in str(pending)


def test_a_shape_that_is_not_a_stopped_owner_earns_no_wait_at_all(tmp_path: Path) -> None:
    """`None` rather than an exception, so the caller keeps its own refusal and wording."""

    path = _dormant_wal_database(tmp_path)
    assert (
        dormant_wal_peer_wait(
            reader="signal_router",
            artifact="runner source",
            path=path,
            owner="strategy",
        )
        is None
    )


def test_both_readers_of_a_peers_sqlite_database_bind_the_same_judgement() -> None:
    """One rule, not two that can drift: #252's reader and #263's reader call this one.

    The strategy met this state on `broker.sqlite3` in the seventh window and the router
    met it on `runner.sqlite3` in the ninth. A second copy of the judgement is how one of
    them would later be widened -- to swallow any `OperationalError`, say -- without the
    other's tests noticing.
    """

    import rquant.signal_router_runtime as router
    import rquant.strategy_paper_lifecycle as lifecycle

    for module in (router, lifecycle):
        assert module.dormant_wal_peer_wait is dormant_wal_peer_wait
        source = Path(module.__file__).read_text()
        #: no module may carry its own header magic: that is what a copy looks like
        assert "SQLite format 3" not in source, module.__name__
