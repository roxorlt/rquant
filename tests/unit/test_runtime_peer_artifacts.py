"""The rule that separates "the owner has not started" from "this artifact is broken".

Both used to end the same way — the process exited while its step was being built, and
`OnFailure=rquant-alert@%n.service` sent a push. The 2026-09-08 Route A window did that
ten times over one night for artifacts that were merely absent (#231, #232, #220).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rquant.runtime_peer_artifacts import (
    DeferredPeerArtifact,
    PeerArtifactUnavailableError,
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
