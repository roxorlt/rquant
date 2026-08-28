"""A private temporary root for the Phase C suites, with the guard written down.

Two ancestry walks in this subsystem — `open_child_workspace_root` and
`runtime_serving_authority._open_existing_directory_chain` — refuse any path whose ancestry
contains a group- or world-writable directory, deliberately and with no `S_ISVTX` exemption.
pytest's `tmp_path` is derived from `TMPDIR`, which on Linux defaults to a sticky `1777`
`/tmp`, so running these files with a plain `pytest` produced around a hundred failures whose
message never mentioned the temp directory. CI never hit it because
`scripts/full_suite_shards.py prepare-environment` builds a private root first — the guard
existed only as a convention in a CI script.

This module puts the guard in the code. The modules that need it import `tmp_path` from here,
which shadows pytest's fixture for those modules only, and roots every temporary directory in
a `0700` directory under `$HOME`. The precondition is verified rather than assumed: if any
ancestor is group- or world-writable the session fails immediately, naming the directory and
its mode, instead of surfacing as an unexplained `CHILD_LAUNCH_FAILED` a hundred times.

`$HOME` is used rather than `TMPDIR` precisely because `TMPDIR` is the thing that cannot be
trusted here. No `S_ISVTX` exemption is added to the production walks: the workspace anchor
exists to guarantee that no other user can create entries beside the child's, and a sticky
parent is exactly the case it must refuse.
"""

from __future__ import annotations

import shutil
import stat
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

#: The bits either ancestry walk refuses, anywhere above the directory under test.
UNSAFE_ANCESTOR_BITS = stat.S_IWGRP | stat.S_IWOTH


def unsafe_ancestors(path: Path) -> tuple[tuple[Path, int], ...]:
    """Every ancestor of `path`, inclusive, that a Phase C ancestry walk would refuse."""

    offenders: list[tuple[Path, int]] = []
    for node in [path, *path.parents]:
        mode = node.stat().st_mode
        if mode & UNSAFE_ANCESTOR_BITS:
            offenders.append((node, stat.S_IMODE(mode)))
    return tuple(offenders)


def require_private_ancestry(path: Path) -> None:
    """Fail the run with the actual cause rather than a hundred derived rejections."""

    offenders = unsafe_ancestors(path)
    if offenders:
        rendered = ", ".join(f"{node} (mode {mode:04o})" for node, mode in offenders)
        pytest.fail(
            "the Phase C suites need a temporary root whose whole ancestry is private, "
            "because the verifier workspace and the serving authority both refuse a group- "
            f"or world-writable ancestor. These are writable: {rendered}",
            pytrace=False,
        )


@pytest.fixture(scope="session")
def signal_family_private_root() -> Iterator[Path]:
    """One `0700` root under `$HOME`, checked once per session."""

    root = Path(tempfile.mkdtemp(prefix="rquant-signal-family-", dir=Path.home()))
    root.chmod(0o700)
    try:
        require_private_ancestry(root)
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def tmp_path(
    request: pytest.FixtureRequest,
    signal_family_private_root: Path,
) -> Iterator[Path]:
    """Shadow pytest's `tmp_path` for the Phase C modules that import this name."""

    safe_name = "".join(
        character if character.isalnum() or character in "-_" else "-"
        for character in request.node.name
    )[:48]
    path = Path(
        tempfile.mkdtemp(prefix=f"{safe_name}-", dir=signal_family_private_root)
    )
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


__all__ = [
    "UNSAFE_ANCESTOR_BITS",
    "require_private_ancestry",
    "signal_family_private_root",
    "tmp_path",
    "unsafe_ancestors",
]
