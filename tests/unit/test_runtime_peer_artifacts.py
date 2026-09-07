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
