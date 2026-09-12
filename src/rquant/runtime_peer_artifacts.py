"""Read artifacts another runtime role owns without failing closed before they exist.

Every isolated role owns exactly the paths its systemd unit lists in `ReadWritePaths` —
the same list `_WRITABLE_PATH_SETTINGS` in `runtime_deployment_bundle.py` validates — and
reads every other role's artifacts through `_READONLY_PATH_SETTINGS`. Opening a peer's
artifact while the step is being *built* silently turned each of those read edges into a
start order, and the 2026-09-08 Route A window is what that costs: `signal_router` exited
five times because three `strategy_live` instances had not written `runner.sqlite3` yet
(#232), while the strategies could not come up either because only the router creates
`signal_bus.sqlite3` (#220). Every exit fired the `OnFailure` alert relay.

A peer artifact that does not exist yet is not a fault — the role that owns it has not
started. A peer artifact that exists and fails its own integrity checks is a fault and
stays one. `DeferredPeerArtifact` is exactly that distinction:

* `probe()` runs while the service is being built. Anything already on disk is opened
  there and then, so an artifact that exists and is unusable still refuses to start.
* `get()` retries the open inside the service loop. While the owner has not created it,
  it raises `PeerArtifactUnavailableError`, which `run_service_loop` records as
  `last_error` on the heartbeat and then keeps the service alive for the next iteration.

A third state means the same thing and does not look like it: an artifact that is on
disk, is a WAL SQLite database, and has no `-wal`/`-shm` beside it, in a directory this
role cannot write. A read-only open of a WAL database has to *create* the `-shm`
wal-index, so SQLite answers `unable to open database file` -- the same words a real
fault gets. Only the owner's own open keeps those sidecars there, so that shape is
"the owner is not running" as surely as an absent file is. `is_dormant_wal_database`
is that judgement and `dormant_wal_peer_wait` is the wait it earns; both readers of a
peer's SQLite database on this plane use them, so there is one rule and not two:
`strategy_live` -> `broker.sqlite3` (#252) and `signal_router` -> `runner.sqlite3`
(#263).

Nothing here loosens a check: the callables handed to `open_artifact` are the same
constructors, with the same validation, that the builders used to call eagerly.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Generic, TypeVar

ArtifactT = TypeVar("ArtifactT")

_SQLITE_MAGIC = b"SQLite format 3\x00"
_SQLITE_HEADER_BYTES = 20


class PeerArtifactUnavailableError(ValueError):
    """The role that owns this artifact has not created it, or not yet rotated it."""

    def __init__(
        self,
        *,
        reader: str,
        artifact: str,
        path: Path,
        reason: str | None = None,
    ) -> None:
        self.reader = reader
        self.artifact = artifact
        self.path = Path(path)
        self.reason = reason
        detail = "" if reason is None else f" ({reason})"
        super().__init__(
            f"{reader} is waiting for the {artifact} its owner creates: {self.path}{detail}"
        )


class DeferredPeerArtifact(Generic[ArtifactT]):
    """One artifact another role owns, opened as soon as that role has created it."""

    def __init__(
        self,
        *,
        reader: str,
        artifact: str,
        path: Path,
        open_artifact: Callable[[], ArtifactT],
    ) -> None:
        if not reader.strip() or not artifact.strip():
            raise ValueError("peer artifact reader and artifact name cannot be empty")
        candidate = Path(path)
        if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
            raise ValueError("peer artifact path must be absolute and normalized")
        if not callable(open_artifact):
            raise TypeError("open_artifact must be callable")
        self.reader = reader
        self.artifact = artifact
        self.path = candidate
        self._open_artifact = open_artifact
        self._opened: ArtifactT | None = None
        self._pending_reason: str | None = None

    @property
    def exists(self) -> bool:
        """Whether anything at all sits at the path.

        `FileNotFoundError` is the one answer that means "the owner has not written it".
        Every other `OSError` — a non-directory parent, a symlink loop, a directory the
        sandbox refuses to traverse — describes something that is present and wrong, and
        those belong to the opener's own checks, not to this waiting rule.
        """

        try:
            self.path.lstat()
        except FileNotFoundError:
            return False
        except OSError:
            return True
        return True

    def probe(self) -> ArtifactT | None:
        """Open the artifact if it is there, else leave it for a later iteration.

        An opener may itself answer "present, but its owner has not finished with it":
        a strategy runner database that still carries the previous generation's identity
        is on disk and will be archived and recreated the moment the strategy starts
        (#248). That is the same kind of wait as an absent file, so the opener raises
        `PeerArtifactUnavailableError` and it is kept for the next iteration rather than
        taking the reader down. Every other error still comes straight out: an artifact
        that is present and wrong is a fault, and stays one.
        """

        if self._opened is None and self.exists:
            try:
                self._opened = self._open_artifact()
            except PeerArtifactUnavailableError as pending:
                self._pending_reason = pending.reason
                return None
            self._pending_reason = None
        return self._opened

    def get(self) -> ArtifactT:
        """The opened artifact, or `PeerArtifactUnavailableError` while it is absent."""

        opened = self.probe()
        if opened is None:
            raise PeerArtifactUnavailableError(
                reader=self.reader,
                artifact=self.artifact,
                path=self.path,
                reason=self._pending_reason,
            )
        return opened


def is_dormant_wal_database(path: Path) -> bool:
    """Exactly the shape a cleanly stopped owner leaves behind, and nothing wider.

    Four things must all hold, and each one is checked against the filesystem rather than
    against the error text: the header must really be SQLite's, it must really say WAL,
    both sidecars must really be absent, and the directory must really be one this
    process cannot write. A truncated or corrupt header, a rollback-journal database, a
    database whose owner is running (its sidecars are there), one in a directory this
    role *can* write, and a header this process cannot read all answer `False`, so the
    caller refuses exactly as it did before.
    """

    candidate = Path(path)
    try:
        with open(candidate, "rb") as handle:
            header = handle.read(_SQLITE_HEADER_BYTES)
    except OSError:
        return False
    if len(header) < _SQLITE_HEADER_BYTES or not header.startswith(_SQLITE_MAGIC):
        return False
    #: bytes 18 and 19 are the write and read file format versions; 2 is WAL
    if header[18] != 2 or header[19] != 2:
        return False
    parent = candidate.parent
    for sidecar in (f"{candidate.name}-wal", f"{candidate.name}-shm"):
        try:
            (parent / sidecar).lstat()
        except FileNotFoundError:
            continue
        except OSError:
            return False
        return False
    return not os.access(parent, os.W_OK)


def dormant_wal_peer_wait(
    *,
    reader: str,
    artifact: str,
    path: Path,
    owner: str,
) -> PeerArtifactUnavailableError | None:
    """The wait a cleanly stopped `owner` earns, or `None` when the shape is not that.

    Returned rather than raised so the caller keeps its own refusal -- its own exception
    type and its own wording -- for every shape this does not recognise.
    """

    if not is_dormant_wal_database(path):
        return None
    return PeerArtifactUnavailableError(
        reader=reader,
        artifact=artifact,
        path=Path(path),
        reason=(
            "it is a WAL database with no -wal/-shm sidecars in a directory this role "
            f"cannot write, which is what a stopped {owner} leaves"
        ),
    )


__all__ = [
    "DeferredPeerArtifact",
    "PeerArtifactUnavailableError",
    "dormant_wal_peer_wait",
    "is_dormant_wal_database",
]
