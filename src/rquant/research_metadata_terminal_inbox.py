"""Durable typed input for research metadata terminal facts.

The research services that produce Stage-1 audit and snapshot evidence do not
share a runtime worker with artifact retention.  This inbox gives them one
restart-safe, production-shaped way to hand completed business facts to the
metadata authority.  Retention later observes those facts through its
read-only hooks; producers never touch the retention outbox or reference
store.
"""

from __future__ import annotations

import os
import stat
from contextlib import suppress
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DatasetSnapshot,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotFinalization,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strict_json import canonical_json_bytes

_MAX_COMMAND_BYTES = 1024 * 1024


def _command_payload(command: ResearchMetadataTerminalCommand) -> dict[str, object]:
    return command.model_dump(mode="json", exclude_computed_fields=True)


def _command_bytes(command: ResearchMetadataTerminalCommand) -> bytes:
    return canonical_json_bytes(_command_payload(command))


class ResearchMetadataTerminalInboxError(RuntimeError):
    """A metadata terminal command is malformed or its durable inbox is unsafe."""


class ResearchMetadataTerminalCommand(RuntimeContractModel):
    """One immutable completion fact accepted by the metadata authority."""

    command_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    kind: Literal["audit_completed", "snapshot_ready"]
    submitted_at: AwareUtcDatetime
    audit_run: DataAuditRun | None = None
    audit_finalization: DataAuditRunFinalization | None = None
    snapshot: DatasetSnapshot | None = None
    snapshot_finalization: DatasetSnapshotFinalization | None = None
    snapshot_binding: DatasetSnapshotBinding | None = None
    snapshot_binding_finalization: DatasetSnapshotBindingFinalization | None = None

    @model_validator(mode="after")
    def bind_command_identity(self) -> ResearchMetadataTerminalCommand:
        audit_fields = (self.audit_run, self.audit_finalization)
        snapshot_fields = (
            self.snapshot,
            self.snapshot_finalization,
            self.snapshot_binding,
            self.snapshot_binding_finalization,
        )
        if self.kind == "audit_completed":
            if any(value is None for value in audit_fields) or any(
                value is not None for value in snapshot_fields
            ):
                raise ValueError("audit completion command must contain only audit evidence")
            assert self.audit_run is not None
            assert self.audit_finalization is not None
            if self.audit_run.status != "running":
                raise ValueError("audit completion command requires a running audit")
            if self.audit_finalization.completed_at < self.audit_run.observed_at:
                raise ValueError("audit completion cannot precede observation")
        else:
            if any(value is None for value in snapshot_fields) or any(
                value is not None for value in audit_fields
            ):
                raise ValueError("snapshot command must contain only snapshot evidence")
            assert self.snapshot is not None
            assert self.snapshot_finalization is not None
            assert self.snapshot_binding is not None
            assert self.snapshot_binding_finalization is not None
            if self.snapshot.status != "building":
                raise ValueError("snapshot completion command requires a building snapshot")
            if self.snapshot_finalization.completed_at < self.snapshot.created_at:
                raise ValueError("snapshot completion cannot precede creation")
            if self.snapshot_binding.snapshot_id != self.snapshot.snapshot_id:
                raise ValueError("snapshot completion binding does not belong to the snapshot")
            if (
                self.snapshot_binding_finalization.completed_at
                < self.snapshot_finalization.completed_at
            ):
                raise ValueError("snapshot binding cannot complete before the snapshot")
        expected = canonical_sha256(
            self.model_dump(
                mode="json",
                exclude={"command_id"},
                exclude_computed_fields=True,
            )
        )
        if self.command_id is None:
            object.__setattr__(self, "command_id", expected)
        elif self.command_id != expected:
            raise ValueError("metadata terminal command identity is invalid")
        return self


class ResearchMetadataTerminalInbox:
    """Private immutable file inbox with atomic claim/complete transitions."""

    def __init__(self, root: Path) -> None:
        normalized = Path(os.path.abspath(root))
        if not root.is_absolute() or root != normalized:
            raise ValueError("metadata terminal inbox root must be absolute and normalized")
        self.root = normalized
        for name in ("queued", "claimed", "completed"):
            self._ensure_directory(self.root / name)

    def submit(self, command: ResearchMetadataTerminalCommand) -> bool:
        command = command.model_copy(deep=True)
        for state in ("completed", "claimed", "queued"):
            existing = self._path(state, command)
            if existing.exists():
                if self._load(existing) != command:
                    raise ResearchMetadataTerminalInboxError(
                        "metadata terminal command conflicts with immutable history"
                    )
                return False
        destination = self._path("queued", command)
        payload = _command_bytes(command)
        try:
            descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError:
            if self._load(destination) != command:
                raise ResearchMetadataTerminalInboxError(
                    "metadata terminal command conflicts with immutable queue"
                ) from None
            return False
        try:
            os.write(descriptor, payload)
            os.fsync(descriptor)
        except BaseException:
            with suppress(FileNotFoundError):
                os.unlink(destination)
            raise
        finally:
            os.close(descriptor)
        self._sync_directory(destination.parent)
        return True

    def recover_claims(self) -> int:
        recovered = 0
        for path in self._paths("claimed"):
            self._move(path, self.root / "queued" / path.name)
            recovered += 1
        return recovered

    def claim_next(self, *, limit: int) -> tuple[ResearchMetadataTerminalCommand, ...]:
        if not 1 <= limit <= 10_000:
            raise ValueError("metadata terminal command claim limit is out of bounds")
        claimed: list[ResearchMetadataTerminalCommand] = []
        for path in self._paths("queued"):
            if len(claimed) >= limit:
                break
            destination = self.root / "claimed" / path.name
            try:
                self._move(path, destination)
            except FileNotFoundError:
                continue
            try:
                claimed.append(self._load(destination))
            except BaseException:
                self._move(destination, path)
                raise
        return tuple(claimed)

    def complete(self, command: ResearchMetadataTerminalCommand) -> None:
        command = command.model_copy(deep=True)
        claimed = self._path("claimed", command)
        completed = self._path("completed", command)
        if completed.exists():
            if self._load(completed) != command:
                raise ResearchMetadataTerminalInboxError(
                    "completed metadata terminal command conflicts with history"
                )
            if claimed.exists():
                if self._load(claimed) != command:
                    raise ResearchMetadataTerminalInboxError(
                        "claimed metadata terminal command conflicts with history"
                    )
                os.unlink(claimed)
                self._sync_directory(claimed.parent)
            return
        if self._load(claimed) != command:
            raise ResearchMetadataTerminalInboxError("claimed metadata terminal command changed")
        self._move(claimed, completed)

    def pending_count(self) -> int:
        return len(self._paths("queued")) + len(self._paths("claimed"))

    def _path(self, state: str, command: ResearchMetadataTerminalCommand) -> Path:
        assert command.command_id is not None
        return self.root / state / f"{command.command_id}.json"

    def _paths(self, state: str) -> tuple[Path, ...]:
        directory = self.root / state
        self._ensure_directory(directory)
        paths: list[Path] = []
        for path in sorted(directory.glob("*.json")):
            if not path.is_file() or path.is_symlink() or len(path.stem) != 64:
                raise ResearchMetadataTerminalInboxError("metadata terminal inbox path is unsafe")
            paths.append(path)
        return tuple(paths)

    def _load(self, path: Path) -> ResearchMetadataTerminalCommand:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or stat.S_IMODE(observed.st_mode) != 0o600
                or observed.st_nlink != 1
                or observed.st_size > _MAX_COMMAND_BYTES
            ):
                raise ResearchMetadataTerminalInboxError("metadata terminal command file is unsafe")
            payload = os.read(descriptor, _MAX_COMMAND_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(payload) > _MAX_COMMAND_BYTES:
            raise ResearchMetadataTerminalInboxError("metadata terminal command exceeds size")
        try:
            command = ResearchMetadataTerminalCommand.model_validate_json(payload)
        except (TypeError, ValueError) as exc:
            raise ResearchMetadataTerminalInboxError(
                "metadata terminal command is invalid"
            ) from exc
        if payload != _command_bytes(command):
            raise ResearchMetadataTerminalInboxError("metadata terminal command is not canonical")
        assert command.command_id is not None
        if path.name != f"{command.command_id}.json":
            raise ResearchMetadataTerminalInboxError(
                "metadata terminal command filename is invalid"
            )
        return command

    def _move(self, source: Path, destination: Path) -> None:
        self._ensure_directory(source.parent)
        self._ensure_directory(destination.parent)
        os.replace(source, destination)
        self._sync_directory(source.parent)
        if source.parent != destination.parent:
            self._sync_directory(destination.parent)

    @staticmethod
    def _ensure_directory(path: Path) -> None:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        observed = os.lstat(path)
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o700
        ):
            raise ResearchMetadataTerminalInboxError("metadata terminal inbox directory is unsafe")

    @staticmethod
    def _sync_directory(path: Path) -> None:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class ResearchMetadataTerminalCommandProcessor:
    """Apply typed terminal facts to DuckDB exactly once across restarts."""

    def __init__(self, *, inbox: ResearchMetadataTerminalInbox, database_path: Path) -> None:
        selected = Path(database_path)
        normalized = Path(os.path.abspath(selected))
        if not selected.is_absolute() or selected != normalized:
            raise ValueError("metadata terminal database path must be absolute and normalized")
        self.inbox = inbox
        self.database_path = normalized

    def run_once(self, *, limit: int = 128) -> int:
        self.inbox.recover_claims()
        applied = 0
        for command in self.inbox.claim_next(limit=limit):
            with DuckDBStore(self.database_path) as store:
                if command.kind == "audit_completed":
                    assert command.audit_run is not None
                    assert command.audit_finalization is not None
                    store.begin_data_audit_run(command.audit_run)
                    store.finalize_data_audit_run(
                        command.audit_run.audit_run_id,
                        command.audit_finalization,
                    )
                else:
                    assert command.snapshot is not None
                    assert command.snapshot_finalization is not None
                    assert command.snapshot_binding is not None
                    assert command.snapshot_binding_finalization is not None
                    store.begin_dataset_snapshot(command.snapshot)
                    store.finalize_dataset_snapshot(
                        command.snapshot.snapshot_id,
                        command.snapshot_finalization,
                    )
                    store.begin_dataset_snapshot_binding(command.snapshot_binding)
                    store.finalize_dataset_snapshot_binding(
                        command.snapshot.snapshot_id,
                        command.snapshot_binding_finalization,
                    )
            self.inbox.complete(command)
            applied += 1
        return applied


__all__ = [
    "ResearchMetadataTerminalCommand",
    "ResearchMetadataTerminalInbox",
    "ResearchMetadataTerminalInboxError",
    "ResearchMetadataTerminalCommandProcessor",
]
