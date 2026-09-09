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

Nothing here loosens a check: the callables handed to `open_artifact` are the same
constructors, with the same validation, that the builders used to call eagerly.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Generic, TypeVar

ArtifactT = TypeVar("ArtifactT")


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


__all__ = [
    "DeferredPeerArtifact",
    "PeerArtifactUnavailableError",
]
