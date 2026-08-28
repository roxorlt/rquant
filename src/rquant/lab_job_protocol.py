"""Typed, durable command protocol for the Strategy Lab control plane."""

from __future__ import annotations

import fcntl
import fnmatch
import hashlib
import heapq
import os
import re
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.private_fs import rename_noreplace_at
from rquant.research_run_spec import ResearchRunSpec
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

_LabSpoolFileType = Literal[
    "regular",
    "symlink",
    "directory",
    "fifo",
    "socket",
    "block_device",
    "char_device",
    "other",
]

_OwnedIsolationLifecycleState = Literal[
    "SOURCE_OBSERVED",
    "SOURCE_BOUND",
    "CREATED_UNBOUND",
    "CONTAINER_BOUND",
    "CONTAINER_DURABLE",
    "EVIDENCE_PUBLISHING",
    "PREPARED",
    "MOVE_UNCERTAIN",
    "MOVED_DURABLE",
    "COMPLETE",
]


@dataclass(frozen=True)
class _ManagedDirectoryIdentity:
    device: int
    inode: int
    mode: int
    owner: int


class RequestContentConflictError(RuntimeError):
    """A request id was reused with different immutable content."""


class LabProtocolModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


class LabSpoolFileIdentity(LabProtocolModel):
    # (device, inode) cannot name a file across an unlink: ext4 returns a just-freed inode
    # to the very next create in the same directory, so an unlink-and-recreate replacement
    # wears the identity of the entry it replaced. Timestamps do not separate them either --
    # the production kernel stamps inode times from a coarse clock whose granularity is one
    # tick, and an unlink followed immediately by a create lands inside one tick. What does
    # separate them is the bytes, so a singly-linked regular entry carries the digest of what
    # was read out of it, and the isolation path refuses to act on one that has none.
    path: Path
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    file_type: _LabSpoolFileType = "regular"
    link_count: int = Field(default=1, ge=1)
    link_target: str | None = None
    byte_count: int | None = Field(default=None, ge=0)
    content_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_link_target(self) -> LabSpoolFileIdentity:
        if self.file_type == "symlink" and self.link_target is None:
            raise ValueError("symlink identity requires link_target")
        if self.file_type != "symlink" and self.link_target is not None:
            raise ValueError("only symlink identity may have link_target")
        if self.content_sha256 is not None and (
            self.file_type != "regular" or self.link_count != 1
        ):
            raise ValueError("only a singly-linked regular entry may bind a content hash")
        if self.content_sha256 is not None and self.byte_count is None:
            raise ValueError("a content hash requires the byte count it was read from")
        return self

    @property
    def binds_content(self) -> bool:
        """Whether this identity names bytes and not only a reusable inode number."""

        return self.content_sha256 is not None


class InvalidCommandEnvelopeError(ValueError):
    """A spool file is not a valid, self-consistent command envelope."""

    def __init__(
        self,
        message: str,
        *,
        file_identity: LabSpoolFileIdentity | None = None,
    ) -> None:
        super().__init__(message)
        self.file_identity = file_identity


class SubmitJobCommand(LabProtocolModel):
    command_type: Literal["submit"] = "submit"
    job_id: UUID
    spec: ResearchRunSpec
    max_attempts: int = Field(default=1, strict=True, ge=1)


class CancelJobCommand(LabProtocolModel):
    command_type: Literal["cancel"] = "cancel"
    job_id: UUID
    expected_version: int = Field(strict=True, ge=0)
    reason: str = Field(min_length=1)


class PauseJobCommand(LabProtocolModel):
    command_type: Literal["pause"] = "pause"
    job_id: UUID
    expected_version: int = Field(strict=True, ge=0)
    reason: str = Field(min_length=1)


class ResumeJobCommand(LabProtocolModel):
    command_type: Literal["resume"] = "resume"
    job_id: UUID
    expected_version: int = Field(strict=True, ge=0)
    reason: str = Field(min_length=1)


class RetryJobCommand(LabProtocolModel):
    command_type: Literal["retry"] = "retry"
    job_id: UUID
    expected_version: int = Field(strict=True, ge=0)
    reason: str = Field(min_length=1)


LabCommand = Annotated[
    SubmitJobCommand | PauseJobCommand | ResumeJobCommand | CancelJobCommand | RetryJobCommand,
    Field(discriminator="command_type"),
]


def _command_hash(command: LabCommand) -> str:
    if isinstance(command, SubmitJobCommand):
        payload: dict[str, object] = {
            "command_type": command.command_type,
            "job_id": str(command.job_id),
            "max_attempts": command.max_attempts,
            "spec_hash": command.spec.spec_hash,
        }
    else:
        payload = {
            "command_type": command.command_type,
            "expected_version": command.expected_version,
            "job_id": str(command.job_id),
            "reason": command.reason,
        }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class LabCommandEnvelope(LabProtocolModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    command: LabCommand
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabCommandEnvelope:
        expected = _command_hash(self.command)
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match canonical command content")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabCommandReceipt(LabProtocolModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    job_id: UUID
    status: Literal["applied", "rejected"]
    reason: str = Field(min_length=1)
    job_version: int | None = Field(default=None, strict=True, ge=0)


class LabSpoolEntry(LabProtocolModel):
    path: Path
    envelope: LabCommandEnvelope
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


class LabAcknowledgedCommand(LabProtocolModel):
    path: Path
    receipt: LabCommandReceipt


class LabQuarantinedCommand(LabProtocolModel):
    path: Path
    reason: str = Field(min_length=1)


class LabDisappearedQuarantineArtifact(LabProtocolModel):
    schema_version: Literal[1] = 1
    original_name: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class _LabOwnedInvalidEvidenceIdentity(LabProtocolModel):
    name: str = Field(min_length=1)
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    mode: int = Field(ge=0)
    link_count: int = Field(ge=1)
    file_type: _LabSpoolFileType
    byte_count: int = Field(ge=0)
    content_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    link_target: str | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> _LabOwnedInvalidEvidenceIdentity:
        match = LabCommandSpool._OWNED_INVALID_EVIDENCE_NAME.fullmatch(self.name)
        if match is None or (
            int(match["device"], 16),
            int(match["inode"], 16),
            int(match["mode"], 16),
            int(match["link_count"]),
            int(match["byte_count"]),
            match["content_hash"],
        ) != (
            self.device,
            self.inode,
            self.mode,
            self.link_count,
            self.byte_count,
            self.content_hash or "unread",
        ):
            raise ValueError("invalid evidence basename does not match its identity")
        if LabCommandSpool._spool_file_type(self.mode) != self.file_type:
            raise ValueError("invalid evidence file_type does not match mode")
        if (self.file_type == "symlink") != (self.link_target is not None):
            raise ValueError("invalid evidence symlink identity requires link_target")
        if self.content_hash is not None and (self.file_type != "regular" or self.link_count != 1):
            raise ValueError("only a singly-linked regular entry may bind a content hash")
        return self


class _LabOwnedEntryIsolationEvidence(LabProtocolModel):
    schema_version: Literal[1] = 1
    isolation_id: UUID
    source_area: Literal["root", "pending", "quarantine", "recovered"]
    source_name: str = Field(min_length=1)
    destination_name: Literal["entry"] = "entry"
    reason: str = Field(min_length=1)
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    mode: int = Field(ge=0)
    link_count: int = Field(ge=1)
    file_type: _LabSpoolFileType
    byte_count: int = Field(ge=0)
    link_target: str | None = None
    manual_retention: bool = False
    invalid_evidence: _LabOwnedInvalidEvidenceIdentity | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> _LabOwnedEntryIsolationEvidence:
        if Path(self.source_name).name != self.source_name:
            raise ValueError("isolated source_name must be a basename")
        if LabCommandSpool._spool_file_type(self.mode) != self.file_type:
            raise ValueError("isolated file_type does not match mode")
        if (self.file_type == "symlink") != (self.link_target is not None):
            raise ValueError("isolated symlink identity requires link_target")
        return self

    def canonical_json_bytes(self) -> bytes:
        excluded = {"invalid_evidence"} if self.invalid_evidence is None else set()
        return canonical_json_bytes(self.model_dump(mode="json", exclude=excluded))


@dataclass(frozen=True)
class _LabOwnedIsolationRecord:
    container: Path
    container_stat: os.stat_result
    modified_at_ns: int
    byte_count: int


class LabCommandSpool:
    """Atomic filesystem inbox with durable receipts and quarantine."""

    _PENDING_NAME = re.compile(
        r"(?:(?P<sequence>[0-9]{20})-)?"
        r"(?P<request_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12})\.json"
    )
    _ACK_NAME = re.compile(
        r"(?P<request_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12})\.json"
    )
    _OWNED_ISOLATION_NAME = re.compile(
        r"owned-entry-(?P<isolation_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12})\.dead"
    )
    _OWNED_INVALID_EVIDENCE_NAME = re.compile(
        r"invalid-evidence-d(?P<device>[0-9a-f]+)-i(?P<inode>[0-9a-f]+)-"
        r"m(?P<mode>[0-9a-f]+)-n(?P<link_count>[1-9][0-9]*)-"
        r"s(?P<byte_count>[0-9]+)-h(?P<content_hash>[0-9a-f]{64}|unread)\.raw"
    )
    _OWNED_EVIDENCE_PUBLICATION_TEMP_NAME = re.compile(
        r"\.evidence\.json\.(?P<publication_id>[0-9a-f]{32})\.tmp"
    )
    _OWNED_EVIDENCE_PUBLICATION_TEMP_PREFIX = ".evidence.json."
    _OWNED_EVIDENCE_MAX_BYTES = 1024 * 1024

    def __init__(
        self,
        root: Path,
        *,
        max_isolation_records: int = 256,
        max_isolation_bytes: int = 64 * 1024 * 1024,
        mutation_guard: Callable[[], object] | None = None,
    ) -> None:
        if max_isolation_records < 1:
            raise ValueError("max_isolation_records must be positive")
        if max_isolation_bytes < 1:
            raise ValueError("max_isolation_bytes must be positive")
        self.root = Path(os.path.abspath(root))
        self.pending_dir = self.root / "pending"
        self.ack_dir = self.root / "ack"
        self.quarantine_dir = self.root / "quarantine"
        lock_digest = hashlib.sha256(os.fsencode(self.root)).hexdigest()[:16]
        self._lock_path = self.root.parent / f".{self.root.name}.{lock_digest}.spool.lock"
        self._sequence_path = self.root / ".delivery-sequence"
        self._thread_lock = RLock()
        self._managed_directory_identities: dict[Path, _ManagedDirectoryIdentity] = {}
        self._root_identity: _ManagedDirectoryIdentity | None = None
        self._lock_parent_identity: _ManagedDirectoryIdentity | None = None
        self._active_lock_descriptor: int | None = None
        self._active_lock_identity: os.stat_result | None = None
        self._active_lock_parent_descriptor: int | None = None
        self._active_root_descriptor: int | None = None
        self.mutation_guard = mutation_guard
        self.max_isolation_records = max_isolation_records
        self.max_isolation_bytes = max_isolation_bytes
        with self._exclusive_lock(require_root=False):
            self._ensure_private_root()
            for path in (self.pending_dir, self.ack_dir, self.quarantine_dir):
                self._ensure_directory(path)
            self._reconcile_owned_isolations_locked()
            self._prune_owned_isolations_locked()

    @contextmanager
    def _exclusive_lock(self, *, require_root: bool = True) -> Iterator[None]:
        with self._thread_lock:
            parent_descriptor = self._open_lock_parent()
            descriptor = -1
            root_descriptor = -1
            try:
                descriptor = self._open_private_lock(parent_descriptor)
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                active_lock = os.fstat(descriptor)
                self._active_lock_parent_descriptor = parent_descriptor
                self._active_lock_descriptor = descriptor
                self._active_lock_identity = active_lock
                if require_root:
                    root_descriptor = self._open_private_root()
                    self._active_root_descriptor = root_descriptor
                    self._assert_managed_directories_bound()
                yield
            finally:
                self._active_root_descriptor = None
                self._active_lock_identity = None
                self._active_lock_descriptor = None
                self._active_lock_parent_descriptor = None
                if root_descriptor >= 0:
                    os.close(root_descriptor)
                if descriptor >= 0:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                    os.close(descriptor)
                os.close(parent_descriptor)

    def _fsync_directory(self, path: Path) -> None:
        descriptor = self._open_managed_directory(path)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _guard_mutation(self) -> None:
        if self._active_lock_descriptor is not None:
            self._assert_active_lock_authority()
        if self.mutation_guard is not None:
            self.mutation_guard()

    @staticmethod
    def _same_stat(
        left: os.stat_result,
        right: os.stat_result,
        *,
        include_link_count: bool,
    ) -> bool:
        identity_matches = (
            left.st_dev,
            left.st_ino,
            left.st_mode,
            left.st_uid,
        ) == (
            right.st_dev,
            right.st_ino,
            right.st_mode,
            right.st_uid,
        )
        return identity_matches and (not include_link_count or left.st_nlink == right.st_nlink)

    @staticmethod
    def _validate_private_directory_stat(observed: os.stat_result, *, label: str) -> None:
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) != 0o700
        ):
            raise InvalidCommandEnvelopeError(
                f"{label} must be an owned physical 0700 private directory"
            )

    def _ensure_private_root(self) -> None:
        try:
            observed = self.root.lstat()
        except FileNotFoundError:
            self._guard_mutation()
            with suppress(FileExistsError):
                os.mkdir(self.root, 0o700)
            observed = self.root.lstat()
        except OSError as exc:
            raise InvalidCommandEnvelopeError("command spool root is unsafe") from exc
        self._validate_private_directory_stat(observed, label="command spool root")
        identity = self._directory_identity(observed)
        if self._root_identity is not None and identity != self._root_identity:
            raise InvalidCommandEnvelopeError("command spool root identity changed")
        self._root_identity = identity
        descriptor = self._open_private_root()
        os.close(descriptor)

    def _open_private_root(self) -> int:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            before = self.root.lstat()
            self._validate_private_directory_stat(before, label="command spool root")
            descriptor = os.open(self.root, flags)
            opened = os.fstat(descriptor)
            active = self.root.lstat()
            if not self._same_stat(
                before,
                opened,
                include_link_count=False,
            ) or not self._same_stat(opened, active, include_link_count=False):
                raise InvalidCommandEnvelopeError("command spool root identity changed")
            if self._root_identity is not None and (
                self._directory_identity(opened) != self._root_identity
                or self._directory_identity(active) != self._root_identity
            ):
                raise InvalidCommandEnvelopeError("command spool root identity changed")
            return descriptor
        except BaseException:
            if "descriptor" in locals():
                os.close(descriptor)
            raise

    def _ensure_directory(self, path: Path, *, mode: int = 0o700) -> None:
        if mode != 0o700:
            raise InvalidCommandEnvelopeError("managed spool directories must use mode 0700")
        try:
            relative = path.relative_to(self.root)
        except ValueError as exc:
            raise InvalidCommandEnvelopeError("unsafe managed spool directory path") from exc
        if not relative.parts:
            self._ensure_private_root()
            return
        parent_descriptor = self._open_private_root()
        opened_descriptors = [parent_descriptor]
        try:
            for part in relative.parts:
                if part in {"", ".", ".."}:
                    raise InvalidCommandEnvelopeError("unsafe managed spool directory component")
                try:
                    observed = os.stat(part, dir_fd=parent_descriptor, follow_symlinks=False)
                except FileNotFoundError:
                    self._guard_mutation()
                    with suppress(FileExistsError):
                        os.mkdir(part, 0o700, dir_fd=parent_descriptor)
                    observed = os.stat(part, dir_fd=parent_descriptor, follow_symlinks=False)
                self._validate_private_directory_stat(
                    observed,
                    label="managed spool path",
                )
                child_descriptor = os.open(
                    part,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent_descriptor,
                )
                opened = os.fstat(child_descriptor)
                active = os.stat(part, dir_fd=parent_descriptor, follow_symlinks=False)
                if not self._same_stat(
                    observed,
                    opened,
                    include_link_count=False,
                ) or not self._same_stat(opened, active, include_link_count=False):
                    os.close(child_descriptor)
                    raise InvalidCommandEnvelopeError("managed spool path identity changed")
                opened_descriptors.append(child_descriptor)
                parent_descriptor = child_descriptor
        except OSError as exc:
            raise InvalidCommandEnvelopeError("managed spool private directory is unsafe") from exc
        finally:
            for descriptor in reversed(opened_descriptors):
                os.close(descriptor)
        self._bind_managed_directory(path)

    @staticmethod
    def _directory_identity(observed: os.stat_result) -> _ManagedDirectoryIdentity:
        return _ManagedDirectoryIdentity(
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=observed.st_mode,
            owner=observed.st_uid,
        )

    def _open_managed_directory(self, path: Path) -> int:
        normalized = Path(os.path.abspath(path))
        if normalized == self.root:
            return self._open_private_root()
        try:
            relative = normalized.relative_to(self.root)
        except ValueError as exc:
            raise InvalidCommandEnvelopeError("managed spool directory escaped root") from exc
        parent_descriptor = self._open_private_root()
        opened_descriptors = [parent_descriptor]
        try:
            current = self.root
            for part in relative.parts:
                current = current / part
                descriptor = os.open(
                    part,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent_descriptor,
                )
                opened_descriptors.append(descriptor)
                parent_descriptor = descriptor
                bound = self._managed_directory_identities.get(current)
                opened = os.fstat(descriptor)
                active = os.stat(part, dir_fd=opened_descriptors[-2], follow_symlinks=False)
                if bound is not None and (
                    self._directory_identity(opened) != bound
                    or self._directory_identity(active) != bound
                ):
                    raise InvalidCommandEnvelopeError(
                        f"managed spool directory identity changed: {current.name}"
                    )
                if bound is None:
                    self._validate_private_directory_stat(
                        opened,
                        label="dynamic private spool path",
                    )
                    if not self._same_stat(
                        opened,
                        active,
                        include_link_count=False,
                    ):
                        raise InvalidCommandEnvelopeError(
                            f"dynamic spool directory identity changed: {current.name}"
                        )
            result = os.dup(parent_descriptor)
        except (OSError, InvalidCommandEnvelopeError) as exc:
            if isinstance(exc, InvalidCommandEnvelopeError):
                raise
            raise InvalidCommandEnvelopeError(
                f"managed spool directory identity changed: {normalized.name}"
            ) from exc
        finally:
            for descriptor in reversed(opened_descriptors):
                os.close(descriptor)
        return result

    def _bind_managed_directory(self, path: Path) -> None:
        normalized = Path(os.path.abspath(path))
        if normalized in self._managed_directory_identities:
            descriptor = self._open_managed_directory(normalized)
            os.close(descriptor)
            return
        try:
            observed = normalized.lstat()
        except OSError as exc:
            raise InvalidCommandEnvelopeError("managed spool directory is unavailable") from exc
        self._validate_private_directory_stat(observed, label="managed spool path")
        self._managed_directory_identities[normalized] = self._directory_identity(observed)
        try:
            descriptor = self._open_managed_directory(normalized)
        except BaseException:
            self._managed_directory_identities.pop(normalized, None)
            raise
        os.close(descriptor)

    def _assert_managed_directories_bound(self) -> None:
        for path in tuple(self._managed_directory_identities):
            descriptor = self._open_managed_directory(path)
            os.close(descriptor)

    def _managed_paths(self, directory: Path, pattern: str) -> tuple[Path, ...]:
        descriptor = self._open_managed_directory(directory)
        try:
            names = tuple(os.listdir(descriptor))
        finally:
            os.close(descriptor)
        return tuple(directory / name for name in names if fnmatch.fnmatchcase(name, pattern))

    def _managed_entry_exists(self, path: Path, directory: Path) -> bool:
        name = self._direct_child_name(path, directory)
        descriptor = self._open_managed_directory(directory)
        try:
            try:
                os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return False
            return True
        finally:
            os.close(descriptor)

    def _managed_entry_stat(self, path: Path, directory: Path) -> os.stat_result:
        name = self._direct_child_name(path, directory)
        descriptor = self._open_managed_directory(directory)
        try:
            return os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        finally:
            os.close(descriptor)

    def _managed_link_target(self, path: Path, directory: Path) -> str:
        name = self._direct_child_name(path, directory)
        descriptor = self._open_managed_directory(directory)
        try:
            return os.readlink(name, dir_fd=descriptor)
        finally:
            os.close(descriptor)

    def _unlink_managed_entry(
        self,
        path: Path,
        directory: Path,
        *,
        expected: os.stat_result | None = None,
    ) -> None:
        name = self._direct_child_name(path, directory)
        descriptor = self._open_managed_directory(directory)
        try:
            current = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if expected is not None and not self._stat_matches_bound_entry(current, expected):
                raise InvalidCommandEnvelopeError("managed spool entry identity changed")
            self._guard_mutation()
            os.unlink(name, dir_fd=descriptor)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _open_lock_parent(self) -> int:
        parent = self.root.parent
        try:
            before = parent.lstat()
            self._validate_private_directory_stat(before, label="spool lock parent")
            descriptor = os.open(
                parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            opened = os.fstat(descriptor)
            active = parent.lstat()
        except BaseException:
            if "descriptor" in locals():
                os.close(descriptor)
            raise
        identity = self._directory_identity(opened)
        if (
            not self._same_stat(before, opened, include_link_count=False)
            or not self._same_stat(opened, active, include_link_count=False)
            or (self._lock_parent_identity is not None and identity != self._lock_parent_identity)
        ):
            os.close(descriptor)
            raise InvalidCommandEnvelopeError("spool lock parent identity changed")
        self._lock_parent_identity = identity
        return descriptor

    def _open_private_lock(self, parent_descriptor: int) -> int:
        flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        try:
            observed = os.stat(
                self._lock_path.name,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            self._guard_mutation()
            try:
                descriptor = os.open(
                    self._lock_path.name,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=parent_descriptor,
                )
            except FileExistsError:
                descriptor = -1
            if descriptor >= 0:
                observed = os.fstat(descriptor)
            else:
                observed = os.stat(
                    self._lock_path.name,
                    dir_fd=parent_descriptor,
                    follow_symlinks=False,
                )
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != 0o600
        ):
            if "descriptor" in locals() and descriptor >= 0:
                os.close(descriptor)
            raise InvalidCommandEnvelopeError("spool lock must be an owned 0600 regular file")
        if "descriptor" not in locals() or descriptor < 0:
            try:
                descriptor = os.open(
                    self._lock_path.name,
                    flags,
                    dir_fd=parent_descriptor,
                )
            except OSError as exc:
                raise InvalidCommandEnvelopeError("spool lock is unsafe") from exc
        opened = os.fstat(descriptor)
        active = os.stat(
            self._lock_path.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if not self._same_stat(
            observed,
            opened,
            include_link_count=True,
        ) or not self._same_stat(opened, active, include_link_count=True):
            os.close(descriptor)
            raise InvalidCommandEnvelopeError("spool lock identity changed")
        return descriptor

    def _assert_active_lock_authority(self) -> None:
        descriptor = self._active_lock_descriptor
        parent_descriptor = self._active_lock_parent_descriptor
        expected = self._active_lock_identity
        if descriptor is None or parent_descriptor is None or expected is None:
            raise InvalidCommandEnvelopeError("spool lock authority is unavailable")
        opened = os.fstat(descriptor)
        active = os.stat(
            self._lock_path.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        current_parent = self.root.parent.lstat()
        if (
            not self._same_stat(expected, opened, include_link_count=True)
            or not self._same_stat(opened, active, include_link_count=True)
            or self._lock_parent_identity is None
            or self._directory_identity(current_parent) != self._lock_parent_identity
        ):
            raise InvalidCommandEnvelopeError("spool lock identity changed")
        if self._root_identity is not None:
            try:
                current_root = self.root.lstat()
            except OSError as exc:
                raise InvalidCommandEnvelopeError("command spool root identity changed") from exc
            if self._directory_identity(current_root) != self._root_identity:
                raise InvalidCommandEnvelopeError("command spool root identity changed")
            if self._active_root_descriptor is not None and (
                self._directory_identity(os.fstat(self._active_root_descriptor))
                != self._root_identity
            ):
                raise InvalidCommandEnvelopeError("command spool root identity changed")

    def _publish_no_clobber(self, target: Path, payload: bytes) -> bool:
        target_name = self._direct_child_name(target, target.parent)
        temporary_name = f".{target.name}.{uuid4().hex}.tmp"
        directory_descriptor = self._open_managed_directory(target.parent)
        temporary_descriptor = -1
        try:
            temporary_descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_descriptor,
            )
            offset = 0
            while offset < len(payload):
                offset += os.write(temporary_descriptor, payload[offset:])
            os.fsync(temporary_descriptor)
            try:
                self._guard_mutation()
                os.link(
                    temporary_name,
                    target_name,
                    src_dir_fd=directory_descriptor,
                    dst_dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            except FileExistsError:
                return False
            os.fsync(directory_descriptor)
            return True
        finally:
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=directory_descriptor)
            os.close(directory_descriptor)

    def _replace_managed_payload(self, target: Path, payload: bytes) -> None:
        target_name = self._direct_child_name(target, target.parent)
        temporary_name = f".{target_name}.{uuid4().hex}.tmp"
        directory_descriptor = self._open_managed_directory(target.parent)
        temporary_descriptor = -1
        try:
            temporary_descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_descriptor,
            )
            offset = 0
            while offset < len(payload):
                offset += os.write(temporary_descriptor, payload[offset:])
            os.fsync(temporary_descriptor)
            self._guard_mutation()
            os.replace(
                temporary_name,
                target_name,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
            )
            os.fsync(directory_descriptor)
        finally:
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=directory_descriptor)
            os.close(directory_descriptor)

    def _next_sequence_locked(self) -> int:
        root_descriptor = self._open_private_root()
        temporary_name = f".{self._sequence_path.name}.{uuid4().hex}.tmp"
        temporary_descriptor = -1
        try:
            try:
                sequence_descriptor = os.open(
                    self._sequence_path.name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_descriptor,
                )
            except FileNotFoundError:
                current = 0
            else:
                try:
                    observed = os.fstat(sequence_descriptor)
                    if (
                        not stat.S_ISREG(observed.st_mode)
                        or observed.st_uid != os.getuid()
                        or observed.st_nlink != 1
                        or stat.S_IMODE(observed.st_mode) != 0o600
                        or observed.st_size > 64
                    ):
                        raise InvalidCommandEnvelopeError("invalid durable delivery sequence")
                    raw = os.read(sequence_descriptor, 64).decode("ascii").strip()
                    active = os.stat(
                        self._sequence_path.name,
                        dir_fd=root_descriptor,
                        follow_symlinks=False,
                    )
                    if not self._same_stat(observed, active, include_link_count=True):
                        raise InvalidCommandEnvelopeError(
                            "durable delivery sequence identity changed"
                        )
                    if not raw.isdigit():
                        raise InvalidCommandEnvelopeError("invalid durable delivery sequence")
                    current = int(raw)
                finally:
                    os.close(sequence_descriptor)
            sequence = current + 1
            temporary_descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_descriptor,
            )
            payload = f"{sequence}\n".encode("ascii")
            offset = 0
            while offset < len(payload):
                offset += os.write(temporary_descriptor, payload[offset:])
            os.fsync(temporary_descriptor)
            self._after_sequence_stage("temporary_written", self.root / temporary_name)
            self._guard_mutation()
            temporary_identity = os.fstat(temporary_descriptor)
            active_temporary = os.stat(
                temporary_name,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
            if not self._same_stat(
                temporary_identity,
                active_temporary,
                include_link_count=True,
            ):
                raise InvalidCommandEnvelopeError("delivery sequence temporary identity changed")
            os.replace(
                temporary_name,
                self._sequence_path.name,
                src_dir_fd=root_descriptor,
                dst_dir_fd=root_descriptor,
            )
            published = os.stat(
                self._sequence_path.name,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
            if not self._same_stat(
                temporary_identity,
                published,
                include_link_count=True,
            ):
                raise InvalidCommandEnvelopeError("delivery sequence publish identity changed")
            os.fsync(root_descriptor)
            self._after_sequence_stage("sequence_replaced", self._sequence_path)
        finally:
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_descriptor)
            os.close(root_descriptor)
        return sequence

    @staticmethod
    def _after_sequence_stage(_stage: str, _path: Path) -> None:
        """Fault-injection boundary for the durable delivery sequence."""

    @staticmethod
    def _direct_child_name(path: Path, parent: Path) -> str:
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        expected_parent = Path(parent)
        if candidate.parent != expected_parent:
            raise InvalidCommandEnvelopeError(
                f"unsafe spool path outside {expected_parent.name}: {candidate}"
            )
        return candidate.name

    @staticmethod
    def _spool_file_type(mode: int) -> _LabSpoolFileType:
        if stat.S_ISREG(mode):
            return "regular"
        if stat.S_ISLNK(mode):
            return "symlink"
        if stat.S_ISDIR(mode):
            return "directory"
        if stat.S_ISFIFO(mode):
            return "fifo"
        if stat.S_ISSOCK(mode):
            return "socket"
        if stat.S_ISBLK(mode):
            return "block_device"
        if stat.S_ISCHR(mode):
            return "char_device"
        return "other"

    @staticmethod
    def _spool_identity(
        path: Path,
        observed: os.stat_result,
        *,
        link_target: str | None = None,
        payload: bytes | None = None,
    ) -> LabSpoolFileIdentity:
        """Name a spool file by the bytes read out of it, not only by its inode number.

        ``payload`` is the exact byte string read from ``observed``'s open descriptor. It is
        bound as a digest only for a singly-linked regular entry: a second hard link can
        rewrite the content without this directory entry moving at all, so a digest there
        would name something the entry does not control.
        """

        file_type = LabCommandSpool._spool_file_type(observed.st_mode)
        binds_content = payload is not None and file_type == "regular" and observed.st_nlink == 1
        return LabSpoolFileIdentity(
            path=path,
            device=observed.st_dev,
            inode=observed.st_ino,
            file_type=file_type,
            link_count=observed.st_nlink,
            link_target=link_target,
            byte_count=len(payload) if binds_content and payload is not None else None,
            content_sha256=(
                hashlib.sha256(payload).hexdigest()
                if binds_content and payload is not None
                else None
            ),
        )

    def _owned_source_area(self, parent: Path) -> Literal["root", "pending", "quarantine"]:
        normalized = Path(os.path.abspath(parent))
        if normalized == self.root:
            return "root"
        if normalized == self.pending_dir:
            return "pending"
        if normalized == self.quarantine_dir:
            return "quarantine"
        raise InvalidCommandEnvelopeError(f"unsafe isolation source parent: {parent}")

    def _owned_source_path(
        self,
        evidence: _LabOwnedEntryIsolationEvidence,
    ) -> Path | None:
        parents = {
            "root": self.root,
            "pending": self.pending_dir,
            "quarantine": self.quarantine_dir,
        }
        parent = parents.get(evidence.source_area)
        return None if parent is None else parent / evidence.source_name

    @staticmethod
    def _stat_matches_isolation(
        observed: os.stat_result,
        evidence: _LabOwnedEntryIsolationEvidence,
    ) -> bool:
        return (
            observed.st_dev == evidence.device
            and observed.st_ino == evidence.inode
            and observed.st_mode == evidence.mode
            and observed.st_nlink == evidence.link_count
        )

    @staticmethod
    def _stat_matches_bound_entry(
        current: os.stat_result,
        observed: os.stat_result,
    ) -> bool:
        return (
            current.st_dev == observed.st_dev
            and current.st_ino == observed.st_ino
            and current.st_mode == observed.st_mode
            and current.st_nlink == observed.st_nlink
        )

    @classmethod
    def _require_bound_entry_stat(
        cls,
        current: os.stat_result,
        observed: os.stat_result,
        source: Path,
    ) -> None:
        if cls._stat_matches_bound_entry(current, observed):
            return
        if (
            current.st_dev == observed.st_dev
            and current.st_ino == observed.st_ino
            and current.st_nlink != observed.st_nlink
        ):
            raise InvalidCommandEnvelopeError(
                f"owned entry link count changed before isolation: {source.name}"
            )
        raise InvalidCommandEnvelopeError(f"owned entry changed before isolation: {source.name}")

    def _open_bound_regular_entry(
        self,
        source: Path,
        observed: os.stat_result,
    ) -> int:
        if not stat.S_ISREG(observed.st_mode):
            raise InvalidCommandEnvelopeError(f"owned entry is not regular: {source.name}")
        if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_NONBLOCK"):
            raise InvalidCommandEnvelopeError(
                "regular entry isolation requires no-follow nonblocking open support"
            )
        source_name = self._direct_child_name(source, source.parent)
        source_fd = self._open_managed_directory(source.parent)
        descriptor = -1
        try:
            flags = (
                getattr(os, "O_PATH", os.O_RDONLY)
                | os.O_NOFOLLOW
                | os.O_NONBLOCK
                | getattr(os, "O_CLOEXEC", 0)
            )
            descriptor = os.open(source_name, flags, dir_fd=source_fd)
            opened = os.fstat(descriptor)
            current = os.stat(source_name, dir_fd=source_fd, follow_symlinks=False)
            if not stat.S_ISREG(opened.st_mode):
                raise InvalidCommandEnvelopeError(
                    f"owned entry changed before isolation: {source.name}"
                )
            self._require_bound_entry_stat(opened, observed, source)
            self._require_bound_entry_stat(current, observed, source)
            return descriptor
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
            raise
        finally:
            os.close(source_fd)

    @staticmethod
    def _after_owned_entry_isolation_stage(
        _stage: Literal["evidence_written", "entry_moved"],
        _source: Path,
        _container: Path,
    ) -> None:
        """Fault-injection boundary for the common owned-entry isolation primitive."""

    @staticmethod
    def _before_owned_entry_move(_source: Path, _container: Path) -> None:
        """Fault-injection boundary immediately before atomic no-clobber move."""

    def _move_bound_entry_into_container_locked(
        self,
        source: Path,
        container: Path,
        observed: os.stat_result,
        *,
        expected_link_target: str | None,
        destination_name: str = "entry",
        bound_regular_descriptor: int | None = None,
    ) -> Path:
        source_name = self._direct_child_name(source, source.parent)
        source_fd = self._open_managed_directory(source.parent)
        container_fd = self._open_managed_directory(container)
        try:
            if bound_regular_descriptor is not None:
                anchored = os.fstat(bound_regular_descriptor)
                if not stat.S_ISREG(anchored.st_mode):
                    raise InvalidCommandEnvelopeError(
                        f"owned entry changed before isolation: {source.name}"
                    )
                self._require_bound_entry_stat(anchored, observed, source)
            current = os.stat(source_name, dir_fd=source_fd, follow_symlinks=False)
            self._require_bound_entry_stat(current, observed, source)
            if expected_link_target is not None and (
                not stat.S_ISLNK(current.st_mode)
                or os.readlink(source_name, dir_fd=source_fd) != expected_link_target
            ):
                raise InvalidCommandEnvelopeError(
                    f"owned symlink target changed before isolation: {source.name}"
                )
            try:
                os.stat(destination_name, dir_fd=container_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise InvalidCommandEnvelopeError(
                    f"owned isolation destination already exists: {container.name}"
                )
            # The private 0700 container is newly created by the sole application writer.
            # This application capability is an integrity boundary, not protection against
            # a process that can arbitrarily rewrite the spool directory itself.
            if destination_name == "entry":
                self._before_owned_entry_move(source, container)
                if bound_regular_descriptor is not None:
                    anchored = os.fstat(bound_regular_descriptor)
                    self._require_bound_entry_stat(anchored, observed, source)
                current = os.stat(source_name, dir_fd=source_fd, follow_symlinks=False)
                self._require_bound_entry_stat(current, observed, source)
                if expected_link_target is not None and (
                    not stat.S_ISLNK(current.st_mode)
                    or os.readlink(source_name, dir_fd=source_fd) != expected_link_target
                ):
                    raise InvalidCommandEnvelopeError(
                        f"owned symlink target changed before isolation: {source.name}"
                    )
            self._guard_mutation()
            rename_noreplace_at(
                source_fd,
                source_name,
                container_fd,
                destination_name,
            )
            destination = os.stat(
                destination_name,
                dir_fd=container_fd,
                follow_symlinks=False,
            )
            if not self._stat_matches_bound_entry(destination, observed):
                raise InvalidCommandEnvelopeError(
                    f"owned entry identity changed during isolation: {source.name}"
                )
            if expected_link_target is not None and (
                not stat.S_ISLNK(destination.st_mode)
                or os.readlink(destination_name, dir_fd=container_fd) != expected_link_target
            ):
                raise InvalidCommandEnvelopeError(
                    f"owned symlink target changed during isolation: {source.name}"
                )
            os.fsync(source_fd)
            os.fsync(container_fd)
        finally:
            os.close(container_fd)
            os.close(source_fd)
        self._fsync_directory(self.quarantine_dir)
        return container / destination_name

    def _discard_interrupted_isolation_attempt_locked(
        self,
        source: Path,
        container: Path,
        observed: os.stat_result,
    ) -> None:
        try:
            current = self._managed_entry_stat(source, source.parent)
        except FileNotFoundError:
            return
        if not self._stat_matches_bound_entry(
            current,
            observed,
        ) or self._managed_entry_exists(container / "entry", container):
            return
        for record in self._owned_isolation_records_locked():
            if record.container == container:
                self._remove_owned_isolation_record_locked(record)
                return

    def _discard_created_unbound_isolation_container_locked(
        self,
        quarantine_fd: int,
        container: Path,
    ) -> None:
        try:
            if self._active_lock_descriptor is not None:
                self._assert_active_lock_authority()
            os.rmdir(container.name, dir_fd=quarantine_fd)
        except OSError:
            pass
        finally:
            with suppress(OSError):
                os.fsync(quarantine_fd)

    def _discard_bound_empty_isolation_container_locked(
        self,
        quarantine_fd: int,
        container_fd: int,
        container: Path,
    ) -> None:
        try:
            self._guard_mutation()
            anchored = os.fstat(container_fd)
            active = os.stat(
                container.name,
                dir_fd=quarantine_fd,
                follow_symlinks=False,
            )
            if self._stat_matches_bound_entry(anchored, active):
                os.rmdir(container.name, dir_fd=quarantine_fd)
        except OSError:
            pass
        finally:
            with suppress(OSError):
                os.fsync(quarantine_fd)

    def _isolate_owned_entry_locked(
        self,
        source: Path,
        observed: os.stat_result,
        *,
        reason: str,
        expected_link_target: str | None = None,
    ) -> LabQuarantinedCommand:
        source = Path(os.path.abspath(source))
        source_name = self._direct_child_name(source, source.parent)
        source_area = self._owned_source_area(source.parent)
        file_type = self._spool_file_type(observed.st_mode)
        if file_type == "symlink" and expected_link_target is None:
            source_fd = self._open_managed_directory(source.parent)
            try:
                expected_link_target = os.readlink(source_name, dir_fd=source_fd)
            finally:
                os.close(source_fd)
        isolation_id = uuid4()
        container = self.quarantine_dir / f"owned-entry-{isolation_id}.dead"
        lifecycle_state: _OwnedIsolationLifecycleState = "SOURCE_OBSERVED"
        bound_regular_descriptor = (
            self._open_bound_regular_entry(source, observed)
            if stat.S_ISREG(observed.st_mode)
            else None
        )
        lifecycle_state = "SOURCE_BOUND"
        quarantine_fd = -1
        container_fd = -1
        try:
            quarantine_fd = self._open_managed_directory(self.quarantine_dir)
            self._guard_mutation()
            os.mkdir(container.name, mode=0o700, dir_fd=quarantine_fd)
            lifecycle_state = "CREATED_UNBOUND"
            container_fd = self._open_managed_directory(container)
            anchored_container = os.fstat(container_fd)
            active_container = os.stat(
                container.name,
                dir_fd=quarantine_fd,
                follow_symlinks=False,
            )
            if not self._stat_matches_bound_entry(anchored_container, active_container):
                raise InvalidCommandEnvelopeError(
                    f"owned isolation container identity changed: {container.name}"
                )
            lifecycle_state = "CONTAINER_BOUND"
            os.fsync(quarantine_fd)
            lifecycle_state = "CONTAINER_DURABLE"
            evidence = _LabOwnedEntryIsolationEvidence(
                isolation_id=isolation_id,
                source_area=source_area,
                source_name=source_name,
                reason=reason,
                device=observed.st_dev,
                inode=observed.st_ino,
                mode=observed.st_mode,
                link_count=observed.st_nlink,
                file_type=file_type,
                byte_count=max(0, observed.st_size),
                link_target=expected_link_target,
                manual_retention=file_type == "directory",
            )
            evidence_path = container / "evidence.json"
            lifecycle_state = "EVIDENCE_PUBLISHING"
            if not self._publish_no_clobber(
                evidence_path,
                evidence.canonical_json_bytes(),
            ):
                raise RequestContentConflictError(
                    f"owned isolation evidence already exists: {container.name}"
                )
            self._fsync_directory(self.quarantine_dir)
            lifecycle_state = "PREPARED"
            try:
                self._after_owned_entry_isolation_stage("evidence_written", source, container)
                if stat.S_ISREG(observed.st_mode) and observed.st_nlink != 1:
                    self._after_hardlink_quarantine_evidence(
                        self._spool_identity(source, observed),
                        evidence_path,
                    )
                lifecycle_state = "MOVE_UNCERTAIN"
                destination = self._move_bound_entry_into_container_locked(
                    source,
                    container,
                    observed,
                    expected_link_target=expected_link_target,
                    bound_regular_descriptor=bound_regular_descriptor,
                )
                lifecycle_state = "MOVED_DURABLE"
                self._after_owned_entry_isolation_stage("entry_moved", source, container)
                lifecycle_state = "COMPLETE"
                return LabQuarantinedCommand(path=destination, reason=reason)
            except InterruptedError:
                with suppress(OSError, InvalidCommandEnvelopeError, ValueError):
                    self._discard_interrupted_isolation_attempt_locked(
                        source,
                        container,
                        observed,
                    )
                self._fsync_directory(self.quarantine_dir)
                raise
            except BaseException:
                # A prepared bundle is intentionally retained. Startup either resumes the
                # identity-bound move or prunes an incomplete record within configured limits.
                self._fsync_directory(self.quarantine_dir)
                raise
        except BaseException:
            if (
                lifecycle_state
                in {
                    "CREATED_UNBOUND",
                    "CONTAINER_BOUND",
                    "CONTAINER_DURABLE",
                    "EVIDENCE_PUBLISHING",
                }
                and quarantine_fd >= 0
            ):
                with suppress(OSError, InvalidCommandEnvelopeError, ValueError):
                    if container_fd >= 0:
                        self._discard_bound_empty_isolation_container_locked(
                            quarantine_fd,
                            container_fd,
                            container,
                        )
                    else:
                        self._discard_created_unbound_isolation_container_locked(
                            quarantine_fd,
                            container,
                        )
            raise
        finally:
            if container_fd >= 0:
                os.close(container_fd)
            if quarantine_fd >= 0:
                os.close(quarantine_fd)
            if bound_regular_descriptor is not None:
                os.close(bound_regular_descriptor)

    def _load_owned_isolation_evidence_with_stat(
        self,
        container: Path,
    ) -> tuple[_LabOwnedEntryIsolationEvidence, os.stat_result]:
        match = self._OWNED_ISOLATION_NAME.fullmatch(container.name)
        if match is None:
            raise InvalidCommandEnvelopeError(
                f"invalid owned isolation container: {container.name}"
            )
        _path, payload, file_stat = self._read_regular_child(
            container / "evidence.json",
            container,
        )
        evidence = strict_model_validate_canonical_json(
            _LabOwnedEntryIsolationEvidence,
            payload,
        )
        if str(evidence.isolation_id) != match["isolation_id"]:
            raise InvalidCommandEnvelopeError(
                f"owned isolation evidence id mismatch: {container.name}"
            )
        return evidence, file_stat

    def _load_owned_isolation_evidence(
        self,
        container: Path,
    ) -> _LabOwnedEntryIsolationEvidence:
        evidence, _file_stat = self._load_owned_isolation_evidence_with_stat(container)
        return evidence

    @classmethod
    def _owned_evidence_publication_stat_is_valid(
        cls,
        observed: os.stat_result,
        *,
        link_count: int,
    ) -> bool:
        return (
            stat.S_ISREG(observed.st_mode)
            and stat.S_IMODE(observed.st_mode) == 0o600
            and observed.st_uid == os.getuid()
            and observed.st_nlink == link_count
            and 0 <= observed.st_size <= cls._OWNED_EVIDENCE_MAX_BYTES
        )

    @classmethod
    def _same_owned_evidence_publication_stat(
        cls,
        left: os.stat_result,
        right: os.stat_result,
        *,
        link_count: int,
    ) -> bool:
        return (
            cls._owned_evidence_publication_stat_is_valid(
                left,
                link_count=link_count,
            )
            and cls._owned_evidence_publication_stat_is_valid(
                right,
                link_count=link_count,
            )
            and (
                left.st_dev,
                left.st_ino,
                left.st_mode,
                left.st_uid,
                left.st_nlink,
                left.st_size,
            )
            == (
                right.st_dev,
                right.st_ino,
                right.st_mode,
                right.st_uid,
                right.st_nlink,
                right.st_size,
            )
        )

    def _open_owned_evidence_publication_child(
        self,
        container_fd: int,
        name: str,
        *,
        link_count: int,
    ) -> tuple[int, os.stat_result]:
        descriptor = -1
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            observed = os.stat(name, dir_fd=container_fd, follow_symlinks=False)
            if not self._owned_evidence_publication_stat_is_valid(
                observed,
                link_count=link_count,
            ):
                raise InvalidCommandEnvelopeError(
                    f"unsafe owned evidence publication temporary: {name}"
                )
            descriptor = os.open(name, flags, dir_fd=container_fd)
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=container_fd, follow_symlinks=False)
            if not self._same_owned_evidence_publication_stat(
                observed,
                opened,
                link_count=link_count,
            ) or not self._same_owned_evidence_publication_stat(
                opened,
                active,
                link_count=link_count,
            ):
                raise InvalidCommandEnvelopeError(
                    f"owned evidence publication temporary changed: {name}"
                )
            return descriptor, opened
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
            raise

    @classmethod
    def _read_owned_evidence_publication_descriptor(
        cls,
        descriptor: int,
        observed: os.stat_result,
        *,
        link_count: int,
    ) -> bytes:
        os.lseek(descriptor, 0, os.SEEK_SET)
        remaining = observed.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                raise InvalidCommandEnvelopeError("owned evidence publication target was truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise InvalidCommandEnvelopeError(
                "owned evidence publication target exceeded its observed size"
            )
        after = os.fstat(descriptor)
        if not cls._same_owned_evidence_publication_stat(
            observed,
            after,
            link_count=link_count,
        ):
            raise InvalidCommandEnvelopeError(
                "owned evidence publication target changed while reading"
            )
        return b"".join(chunks)

    def _validate_linked_owned_evidence_publication(
        self,
        container: Path,
        isolation_match: re.Match[str],
        evidence_descriptor: int,
        evidence_stat: os.stat_result,
    ) -> None:
        payload = self._read_owned_evidence_publication_descriptor(
            evidence_descriptor,
            evidence_stat,
            link_count=2,
        )
        evidence = strict_model_validate_canonical_json(
            _LabOwnedEntryIsolationEvidence,
            payload,
        )
        if str(evidence.isolation_id) != isolation_match["isolation_id"]:
            raise InvalidCommandEnvelopeError(
                f"owned isolation evidence id mismatch: {container.name}"
            )
        source = self._owned_source_path(evidence)
        if source is None or source.name != evidence.source_name:
            raise InvalidCommandEnvelopeError(
                f"owned isolation source basename mismatch: {container.name}"
            )
        source_stat = self._managed_entry_stat(source, source.parent)
        if not self._stat_matches_isolation(source_stat, evidence):
            raise InvalidCommandEnvelopeError(
                f"owned isolation source identity mismatch: {container.name}"
            )
        if (
            evidence.link_target is not None
            and self._managed_link_target(source, source.parent) != evidence.link_target
        ):
            raise InvalidCommandEnvelopeError(
                f"owned isolation source link target mismatch: {container.name}"
            )

    def _owned_evidence_publication_container_is_current(
        self,
        quarantine_fd: int,
        container_fd: int,
        container_name: str,
        bound_container_stat: os.stat_result,
    ) -> bool:
        try:
            anchored = os.fstat(container_fd)
            active = os.stat(
                container_name,
                dir_fd=quarantine_fd,
                follow_symlinks=False,
            )
            self._validate_private_directory_stat(
                anchored,
                label="owned isolation container",
            )
        except (InvalidCommandEnvelopeError, OSError):
            return False
        return self._same_stat(
            bound_container_stat,
            anchored,
            include_link_count=False,
        ) and self._same_stat(
            anchored,
            active,
            include_link_count=False,
        )

    def _owned_evidence_publication_child_is_current(
        self,
        container_fd: int,
        child_name: str,
        child_descriptor: int,
        bound_child_stat: os.stat_result,
        *,
        link_count: int,
    ) -> bool:
        try:
            anchored = os.fstat(child_descriptor)
            active = os.stat(
                child_name,
                dir_fd=container_fd,
                follow_symlinks=False,
            )
        except OSError:
            return False
        return self._same_owned_evidence_publication_stat(
            bound_child_stat,
            anchored,
            link_count=link_count,
        ) and self._same_owned_evidence_publication_stat(
            anchored,
            active,
            link_count=link_count,
        )

    def _reconcile_owned_evidence_publication_transaction_locked(
        self,
        quarantine_fd: int,
        container_fd: int,
        container: Path,
        bound_container_stat: os.stat_result,
        isolation_match: re.Match[str],
    ) -> Literal["continue", "removed", "preserve"]:
        temporary_descriptor = -1
        evidence_descriptor = -1
        try:
            if not self._owned_evidence_publication_container_is_current(
                quarantine_fd,
                container_fd,
                container.name,
                bound_container_stat,
            ):
                return "preserve"

            # BOUND -> CLASSIFIED: only exact publisher states are actionable.
            names = frozenset(os.listdir(container_fd))
            publication_names = tuple(
                name
                for name in names
                if name.startswith(self._OWNED_EVIDENCE_PUBLICATION_TEMP_PREFIX)
            )
            if not publication_names:
                return "continue"
            if len(publication_names) != 1:
                return "preserve"
            temporary_name = publication_names[0]
            temporary_match = self._OWNED_EVIDENCE_PUBLICATION_TEMP_NAME.fullmatch(temporary_name)
            if temporary_match is None or Path(temporary_name).name != temporary_name:
                return "preserve"
            publication_id = UUID(hex=temporary_match["publication_id"])
            if (
                publication_id.version != 4
                or publication_id.hex != temporary_match["publication_id"]
            ):
                return "preserve"
            temporary_only = names == {temporary_name}
            if not temporary_only and names != {temporary_name, "evidence.json"}:
                return "preserve"
            link_count = 1 if temporary_only else 2
            temporary_descriptor, temporary_stat = self._open_owned_evidence_publication_child(
                container_fd,
                temporary_name,
                link_count=link_count,
            )
            evidence_stat: os.stat_result | None = None
            if not temporary_only:
                evidence_descriptor, evidence_stat = self._open_owned_evidence_publication_child(
                    container_fd,
                    "evidence.json",
                    link_count=2,
                )
                if not self._same_owned_evidence_publication_stat(
                    temporary_stat,
                    evidence_stat,
                    link_count=2,
                ):
                    return "preserve"
                self._validate_linked_owned_evidence_publication(
                    container,
                    isolation_match,
                    evidence_descriptor,
                    evidence_stat,
                )

            # PRECOMMIT_FENCE -> REVALIDATED: this is the transaction's only callback.
            self._guard_mutation()
            if not self._owned_evidence_publication_container_is_current(
                quarantine_fd,
                container_fd,
                container.name,
                bound_container_stat,
            ) or not self._owned_evidence_publication_child_is_current(
                container_fd,
                temporary_name,
                temporary_descriptor,
                temporary_stat,
                link_count=link_count,
            ):
                return "preserve"
            if not temporary_only:
                if evidence_stat is None or not self._owned_evidence_publication_child_is_current(
                    container_fd,
                    "evidence.json",
                    evidence_descriptor,
                    evidence_stat,
                    link_count=2,
                ):
                    return "preserve"
                if not self._same_owned_evidence_publication_stat(
                    os.fstat(temporary_descriptor),
                    os.fstat(evidence_descriptor),
                    link_count=2,
                ):
                    return "preserve"

            # MUTATED -> POSTVERIFIED: no callback occurs after the final checks.
            os.unlink(temporary_name, dir_fd=container_fd)
            os.fsync(container_fd)
            if not self._owned_evidence_publication_container_is_current(
                quarantine_fd,
                container_fd,
                container.name,
                bound_container_stat,
            ):
                return "preserve"
            expected_names = frozenset() if temporary_only else frozenset({"evidence.json"})
            if frozenset(os.listdir(container_fd)) != expected_names:
                return "preserve"
            if not temporary_only:
                return "continue"
            if not self._owned_evidence_publication_container_is_current(
                quarantine_fd,
                container_fd,
                container.name,
                bound_container_stat,
            ):
                return "preserve"
            os.rmdir(container.name, dir_fd=quarantine_fd)
            os.fsync(quarantine_fd)
            return "removed"
        except (InvalidCommandEnvelopeError, OSError, ValueError):
            return "preserve"
        finally:
            if evidence_descriptor >= 0:
                os.close(evidence_descriptor)
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)

    @classmethod
    def _invalid_evidence_name(
        cls,
        observed: os.stat_result,
        *,
        content_hash: str | None,
    ) -> str:
        return (
            f"invalid-evidence-d{observed.st_dev:x}-i{observed.st_ino:x}-"
            f"m{observed.st_mode:x}-n{observed.st_nlink}-s{max(0, observed.st_size)}-"
            f"h{content_hash or 'unread'}.raw"
        )

    def _invalid_evidence_identity_locked(
        self,
        path: Path,
        observed: os.stat_result,
    ) -> _LabOwnedInvalidEvidenceIdentity:
        content_hash: str | None = None
        if stat.S_ISREG(observed.st_mode) and observed.st_nlink == 1:
            _candidate, payload, file_stat = self._read_regular_child(path, path.parent)
            if not self._stat_matches_bound_entry(file_stat, observed):
                raise InvalidCommandEnvelopeError(
                    f"invalid identity evidence changed while reading: {path.name}"
                )
            content_hash = hashlib.sha256(payload).hexdigest()
        link_target = (
            self._managed_link_target(path, path.parent) if stat.S_ISLNK(observed.st_mode) else None
        )
        return _LabOwnedInvalidEvidenceIdentity(
            name=self._invalid_evidence_name(observed, content_hash=content_hash),
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=observed.st_mode,
            link_count=observed.st_nlink,
            file_type=self._spool_file_type(observed.st_mode),
            byte_count=max(0, observed.st_size),
            content_hash=content_hash,
            link_target=link_target,
        )

    def _write_recovered_isolation_evidence_locked(
        self,
        container: Path,
        isolation_id: UUID,
        entry_stat: os.stat_result,
        *,
        invalid_evidence: _LabOwnedInvalidEvidenceIdentity | None,
    ) -> _LabOwnedEntryIsolationEvidence:
        link_target = (
            self._managed_link_target(container / "entry", container)
            if stat.S_ISLNK(entry_stat.st_mode)
            else None
        )
        evidence = _LabOwnedEntryIsolationEvidence(
            isolation_id=isolation_id,
            source_area="recovered",
            source_name=container.name,
            reason="startup recovered moved entry with missing or invalid identity evidence",
            device=entry_stat.st_dev,
            inode=entry_stat.st_ino,
            mode=entry_stat.st_mode,
            link_count=entry_stat.st_nlink,
            file_type=self._spool_file_type(entry_stat.st_mode),
            byte_count=max(0, entry_stat.st_size),
            link_target=link_target,
            manual_retention=stat.S_ISDIR(entry_stat.st_mode),
            invalid_evidence=invalid_evidence,
        )
        evidence_path = container / "evidence.json"
        if not self._publish_no_clobber(
            evidence_path,
            evidence.canonical_json_bytes(),
        ):
            raise InvalidCommandEnvelopeError(
                f"cannot replace invalid isolation evidence: {container.name}"
            )
        return evidence

    def _discard_empty_unpublished_isolation_container_locked(
        self,
        container: Path,
    ) -> None:
        quarantine_fd = self._open_managed_directory(self.quarantine_dir)
        container_fd = -1
        try:
            container_fd = self._open_managed_directory(container)
            if os.listdir(container_fd):
                return
            self._discard_bound_empty_isolation_container_locked(
                quarantine_fd,
                container_fd,
                container,
            )
        finally:
            if container_fd >= 0:
                os.close(container_fd)
            os.close(quarantine_fd)

    def _reconcile_owned_isolation_container_locked(self, container: Path) -> None:
        match = self._OWNED_ISOLATION_NAME.fullmatch(container.name)
        if match is None:
            return
        quarantine_fd = self._open_managed_directory(self.quarantine_dir)
        container_fd = -1
        try:
            try:
                container_stat = os.stat(
                    container.name,
                    dir_fd=quarantine_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return
            if not stat.S_ISDIR(container_stat.st_mode):
                os.close(quarantine_fd)
                quarantine_fd = -1
                self._isolate_owned_entry_locked(
                    container,
                    container_stat,
                    reason="owned isolation namespace occupied by a non-directory entry",
                )
                return
            container_fd = os.open(
                container.name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=quarantine_fd,
            )
            opened_container = os.fstat(container_fd)
            active_container = os.stat(
                container.name,
                dir_fd=quarantine_fd,
                follow_symlinks=False,
            )
            self._validate_private_directory_stat(
                opened_container,
                label="owned isolation container",
            )
            if not self._same_stat(
                container_stat,
                opened_container,
                include_link_count=False,
            ) or not self._same_stat(
                opened_container,
                active_container,
                include_link_count=False,
            ):
                raise InvalidCommandEnvelopeError(
                    f"owned isolation container identity changed: {container.name}"
                )
            publication_result = self._reconcile_owned_evidence_publication_transaction_locked(
                quarantine_fd,
                container_fd,
                container,
                opened_container,
                match,
            )
        finally:
            if container_fd >= 0:
                os.close(container_fd)
            if quarantine_fd >= 0:
                os.close(quarantine_fd)
        if publication_result in {"removed", "preserve"}:
            return
        entry = container / "entry"
        try:
            entry_stat = self._managed_entry_stat(entry, container)
        except FileNotFoundError:
            entry_stat = None
        try:
            evidence = self._load_owned_isolation_evidence(container)
        except (InvalidCommandEnvelopeError, ValueError):
            if entry_stat is None:
                self._discard_empty_unpublished_isolation_container_locked(container)
                return
            evidence_path = container / "evidence.json"
            invalid_evidence: _LabOwnedInvalidEvidenceIdentity | None = None
            if self._managed_entry_exists(evidence_path, container):
                invalid_stat = self._managed_entry_stat(evidence_path, container)
                invalid_evidence = self._invalid_evidence_identity_locked(
                    evidence_path,
                    invalid_stat,
                )
                self._move_bound_entry_into_container_locked(
                    evidence_path,
                    container,
                    invalid_stat,
                    expected_link_target=invalid_evidence.link_target,
                    destination_name=invalid_evidence.name,
                )
            evidence = self._write_recovered_isolation_evidence_locked(
                container,
                UUID(match["isolation_id"]),
                entry_stat,
                invalid_evidence=invalid_evidence,
            )
        if entry_stat is not None:
            if not self._stat_matches_isolation(entry_stat, evidence):
                return
            if (
                evidence.link_target is not None
                and self._managed_link_target(entry, container) != evidence.link_target
            ):
                return
            return
        source = self._owned_source_path(evidence)
        if source is None:
            return
        try:
            source_stat = self._managed_entry_stat(source, source.parent)
        except FileNotFoundError:
            return
        if not self._stat_matches_isolation(source_stat, evidence):
            return
        self._move_bound_entry_into_container_locked(
            source,
            container,
            source_stat,
            expected_link_target=evidence.link_target,
        )

    def _reconcile_owned_isolations_locked(self) -> None:
        for container in sorted(self._managed_paths(self.quarantine_dir, "owned-entry-*.dead")):
            if self._OWNED_ISOLATION_NAME.fullmatch(container.name) is None:
                continue
            with suppress(OSError, InvalidCommandEnvelopeError, ValueError):
                self._reconcile_owned_isolation_container_locked(container)

    def _owned_isolation_records_locked(self) -> list[_LabOwnedIsolationRecord]:
        records: list[_LabOwnedIsolationRecord] = []
        for container in self._managed_paths(self.quarantine_dir, "owned-entry-*.dead"):
            if self._OWNED_ISOLATION_NAME.fullmatch(container.name) is None:
                continue
            try:
                container_stat = self._managed_entry_stat(container, self.quarantine_dir)
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(container_stat.st_mode):
                records.append(
                    _LabOwnedIsolationRecord(
                        container=container,
                        container_stat=container_stat,
                        modified_at_ns=container_stat.st_mtime_ns,
                        byte_count=max(0, container_stat.st_size),
                    )
                )
                continue
            modified_at_ns = container_stat.st_mtime_ns
            byte_count = 0
            container_fd = -1
            try:
                container_fd = self._open_managed_directory(container)
                opened = os.fstat(container_fd)
                if not self._stat_matches_bound_entry(opened, container_stat):
                    continue
                for name in os.listdir(container_fd):
                    try:
                        child_stat = os.stat(
                            name,
                            dir_fd=container_fd,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        continue
                    modified_at_ns = max(modified_at_ns, child_stat.st_mtime_ns)
                    byte_count += max(0, child_stat.st_size)
            except (OSError, InvalidCommandEnvelopeError):
                pass
            finally:
                if container_fd >= 0:
                    os.close(container_fd)
            records.append(
                _LabOwnedIsolationRecord(
                    container=container,
                    container_stat=container_stat,
                    modified_at_ns=modified_at_ns,
                    byte_count=byte_count,
                )
            )
        return records

    def _remove_bound_directory_entry(
        self,
        parent_fd: int,
        name: str,
        observed: os.stat_result,
    ) -> bool:
        try:
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return True
        if not self._stat_matches_bound_entry(current, observed):
            return False
        try:
            self._guard_mutation()
            if stat.S_ISDIR(current.st_mode):
                os.rmdir(name, dir_fd=parent_fd)
            else:
                os.unlink(name, dir_fd=parent_fd)
        except OSError:
            # Non-empty directories are never traversed or recursively deleted. They stay
            # as observable manual dead letters while other queue entries keep progressing.
            return False
        os.fsync(parent_fd)
        return True

    def _remove_owned_isolation_record_locked(
        self,
        record: _LabOwnedIsolationRecord,
    ) -> bool:
        try:
            current_container = self._managed_entry_stat(
                record.container,
                self.quarantine_dir,
            )
        except FileNotFoundError:
            return True
        if (
            current_container.st_dev != record.container_stat.st_dev
            or current_container.st_ino != record.container_stat.st_ino
            or current_container.st_mode != record.container_stat.st_mode
        ):
            return False
        if not stat.S_ISDIR(current_container.st_mode):
            quarantine_fd = self._open_managed_directory(self.quarantine_dir)
            try:
                return self._remove_bound_directory_entry(
                    quarantine_fd,
                    record.container.name,
                    current_container,
                )
            finally:
                os.close(quarantine_fd)
        container_fd = self._open_managed_directory(record.container)
        try:
            if not self._stat_matches_bound_entry(os.fstat(container_fd), current_container):
                return False
            names = os.listdir(container_fd)
            try:
                evidence, evidence_stat = self._load_owned_isolation_evidence_with_stat(
                    record.container
                )
            except (InvalidCommandEnvelopeError, ValueError, OSError):
                return False
            if evidence.manual_retention:
                return False
            allowed_names = {"entry", "evidence.json"}
            if evidence.invalid_evidence is not None:
                allowed_names.add(evidence.invalid_evidence.name)
            if any(name not in allowed_names for name in names):
                return False
            observed_children: dict[str, os.stat_result] = {"evidence.json": evidence_stat}
            for name in names:
                if name == "evidence.json":
                    continue
                try:
                    child_stat = os.stat(name, dir_fd=container_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if name == "entry":
                    if not self._stat_matches_isolation(child_stat, evidence):
                        return False
                    if evidence.link_target is not None and (
                        not stat.S_ISLNK(child_stat.st_mode)
                        or os.readlink(name, dir_fd=container_fd) != evidence.link_target
                    ):
                        return False
                else:
                    invalid = evidence.invalid_evidence
                    if (
                        invalid is None
                        or name != invalid.name
                        or (
                            child_stat.st_dev,
                            child_stat.st_ino,
                            child_stat.st_mode,
                            child_stat.st_nlink,
                            max(0, child_stat.st_size),
                        )
                        != (
                            invalid.device,
                            invalid.inode,
                            invalid.mode,
                            invalid.link_count,
                            invalid.byte_count,
                        )
                    ):
                        return False
                    if invalid.link_target is not None and (
                        not stat.S_ISLNK(child_stat.st_mode)
                        or os.readlink(name, dir_fd=container_fd) != invalid.link_target
                    ):
                        return False
                    if invalid.content_hash is not None:
                        try:
                            _path, payload, opened_stat = self._read_regular_child(
                                record.container / name,
                                record.container,
                            )
                        except InvalidCommandEnvelopeError:
                            return False
                        if not self._stat_matches_bound_entry(opened_stat, child_stat) or (
                            hashlib.sha256(payload).hexdigest() != invalid.content_hash
                        ):
                            return False
                observed_children[name] = child_stat
            removal_order = [
                name
                for name in (
                    "entry",
                    evidence.invalid_evidence.name if evidence.invalid_evidence else None,
                )
                if name is not None and name in observed_children
            ]
            removal_order.append("evidence.json")
            for name in removal_order:
                if not self._remove_bound_directory_entry(
                    container_fd,
                    name,
                    observed_children[name],
                ):
                    return False
        finally:
            os.close(container_fd)
        quarantine_fd = self._open_managed_directory(self.quarantine_dir)
        try:
            current_container = os.stat(
                record.container.name,
                dir_fd=quarantine_fd,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISDIR(current_container.st_mode)
                or current_container.st_dev != record.container_stat.st_dev
                or current_container.st_ino != record.container_stat.st_ino
            ):
                return False
            self._guard_mutation()
            os.rmdir(record.container.name, dir_fd=quarantine_fd)
            os.fsync(quarantine_fd)
            return True
        except OSError:
            return False
        finally:
            os.close(quarantine_fd)

    def _prune_owned_isolations_locked(self) -> None:
        records = self._owned_isolation_records_locked()
        records.sort(key=lambda item: (item.modified_at_ns, item.container.name))
        total_bytes = sum(record.byte_count for record in records)
        while len(records) > self.max_isolation_records or (
            total_bytes > self.max_isolation_bytes and len(records) > 1
        ):
            removed = False
            for index, record in enumerate(records[:-1] or records):
                if not self._remove_owned_isolation_record_locked(record):
                    continue
                total_bytes -= record.byte_count
                records.pop(index)
                removed = True
                break
            if not removed:
                break

    def _prune_quarantine_locked(self) -> None:
        self._prune_owned_isolations_locked()

    def _read_regular_child(
        self,
        path: Path,
        parent: Path,
        *,
        allowed_link_counts: frozenset[int] = frozenset({1}),
    ) -> tuple[Path, bytes, os.stat_result]:
        name = self._direct_child_name(path, parent)
        normalized = Path(os.path.abspath(parent)) / name
        file_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        directory_fd = self._open_managed_directory(parent)
        try:
            try:
                path_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except OSError as exc:
                raise InvalidCommandEnvelopeError(f"unsafe spool file {name}: {exc}") from exc
            if stat.S_ISLNK(path_stat.st_mode):
                link_target = os.readlink(name, dir_fd=directory_fd)
                identity = self._spool_identity(
                    normalized,
                    path_stat,
                    link_target=link_target,
                )
                raise InvalidCommandEnvelopeError(
                    f"spool file {name} is a symlink",
                    file_identity=identity,
                )
            if not stat.S_ISREG(path_stat.st_mode):
                identity = self._spool_identity(normalized, path_stat)
                raise InvalidCommandEnvelopeError(
                    f"spool file {name} is not regular",
                    file_identity=identity,
                )
            if path_stat.st_nlink not in allowed_link_counts:
                raise InvalidCommandEnvelopeError(
                    f"spool file {name} has an external hard link",
                    file_identity=self._spool_identity(normalized, path_stat),
                )
            try:
                descriptor = os.open(name, file_flags, dir_fd=directory_fd)
            except OSError as exc:
                raise InvalidCommandEnvelopeError(f"unsafe spool file {name}: {exc}") from exc
            try:
                file_stat = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(file_stat.st_mode)
                    or file_stat.st_dev != path_stat.st_dev
                    or file_stat.st_ino != path_stat.st_ino
                    or file_stat.st_nlink not in allowed_link_counts
                ):
                    raise InvalidCommandEnvelopeError(
                        f"spool file {name} was replaced while opening",
                        file_identity=self._spool_identity(normalized, path_stat),
                    )
                chunks: list[bytes] = []
                while chunk := os.read(descriptor, 1024 * 1024):
                    chunks.append(chunk)
                return normalized, b"".join(chunks), file_stat
            finally:
                os.close(descriptor)
        finally:
            os.close(directory_fd)

    @classmethod
    def _pending_name_parts(cls, name: str) -> tuple[int | None, UUID]:
        match = cls._PENDING_NAME.fullmatch(name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid pending command basename: {name}")
        sequence = match.group("sequence")
        return (int(sequence) if sequence is not None else None, UUID(match.group("request_id")))

    @classmethod
    def _ack_request_id(cls, name: str) -> UUID:
        match = cls._ACK_NAME.fullmatch(name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid ack basename: {name}")
        return UUID(match.group("request_id"))

    def _pending_for_request_locked(self, request_id: UUID) -> Path | None:
        matches: list[Path] = []
        for candidate in self._managed_paths(self.pending_dir, "*.json"):
            try:
                _sequence, candidate_request_id = self._pending_name_parts(candidate.name)
            except InvalidCommandEnvelopeError:
                continue
            if candidate_request_id == request_id:
                matches.append(candidate)
        if len(matches) > 1:
            raise InvalidCommandEnvelopeError(
                f"multiple pending commands for request_id {request_id}"
            )
        return matches[0] if matches else None

    def find(
        self,
        request_id: UUID,
    ) -> LabSpoolEntry | LabAcknowledgedCommand | None:
        """Return one durable request identity without creating or moving entries."""

        with self._exclusive_lock():
            ack_path = self.ack_dir / f"{request_id}.json"
            pending_path = self._pending_for_request_locked(request_id)
            if self._managed_entry_exists(ack_path, self.ack_dir):
                receipt = self.load_receipt(ack_path)
                if pending_path is not None:
                    pending = self.load(pending_path)
                    if pending.envelope.content_hash != receipt.content_hash:
                        raise RequestContentConflictError(
                            f"request_id {request_id} has conflicting ack and pending"
                        )
                return LabAcknowledgedCommand(path=ack_path, receipt=receipt)
            return self.load(pending_path) if pending_path is not None else None

    def publish(
        self,
        envelope: LabCommandEnvelope,
    ) -> LabSpoolEntry | LabAcknowledgedCommand:
        validated = LabCommandEnvelope.model_validate(envelope)
        command = validated.command
        if (
            isinstance(command, SubmitJobCommand)
            and command.spec.schema_version == 2
            and command.spec.research_status != "exploratory"
        ):
            raise InvalidCommandEnvelopeError(
                "new v2 comparable submissions require explicit exploratory migration"
            )
        payload = canonical_model_json_bytes(validated)
        with self._exclusive_lock():
            ack_path = self.ack_dir / f"{validated.request_id}.json"
            pending_path = self._pending_for_request_locked(validated.request_id)
            if self._managed_entry_exists(ack_path, self.ack_dir):
                receipt = self.load_receipt(ack_path)
                if pending_path is not None:
                    pending = self.load(pending_path)
                    if pending.envelope.content_hash != receipt.content_hash:
                        raise RequestContentConflictError(
                            f"request_id {validated.request_id} has conflicting ack and pending"
                        )
                if receipt.content_hash != validated.content_hash:
                    raise RequestContentConflictError(
                        f"request_id {validated.request_id} already has different content"
                    )
                if receipt.job_id != validated.command.job_id:
                    raise InvalidCommandEnvelopeError(
                        f"ack job_id does not match request_id {validated.request_id}"
                    )
                return LabAcknowledgedCommand(path=ack_path, receipt=receipt)
            if pending_path is not None:
                existing = self.load(pending_path)
                if existing.envelope.content_hash != validated.content_hash:
                    raise RequestContentConflictError(
                        f"request_id {validated.request_id} already has different content"
                    )
                return existing
            sequence = self._next_sequence_locked()
            target = self.pending_dir / f"{sequence:020d}-{validated.request_id}.json"
            if not self._publish_no_clobber(target, payload):
                raise RequestContentConflictError(f"delivery sequence {sequence} already exists")
            return self.load(target)

    def load(self, path: Path) -> LabSpoolEntry:
        candidate, payload, file_stat = self._read_regular_child(Path(path), self.pending_dir)
        identity = self._spool_identity(candidate, file_stat, payload=payload)
        try:
            _sequence, filename_request_id = self._pending_name_parts(candidate.name)
        except InvalidCommandEnvelopeError as exc:
            raise InvalidCommandEnvelopeError(
                str(exc),
                file_identity=identity,
            ) from exc
        try:
            envelope = strict_model_validate_canonical_json(LabCommandEnvelope, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid command envelope {candidate.name}: {exc}",
                file_identity=identity,
            ) from exc
        if envelope.request_id != filename_request_id:
            raise InvalidCommandEnvelopeError(
                f"command request_id does not match basename {candidate.name}",
                file_identity=identity,
            )
        return LabSpoolEntry(
            path=candidate,
            envelope=envelope,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )

    @staticmethod
    def _delivery_key(path: Path) -> tuple[int, int, str]:
        try:
            sequence, _request_id = LabCommandSpool._pending_name_parts(path.name)
        except InvalidCommandEnvelopeError:
            return (0, 0, path.name)
        if sequence is None:
            return (0, 0, path.name)
        return (1, sequence, path.name)

    def _apply_command_precedence(self, paths: tuple[Path, ...]) -> tuple[Path, ...]:
        # Global visibility is intentional: cancel precedence cannot be derived per file.
        entries: dict[int, LabSpoolEntry] = {}
        for index, path in enumerate(paths):
            try:
                entries[index] = self.load(path)
            except InvalidCommandEnvelopeError:
                continue
        edges: list[set[int]] = [set() for _path in paths]
        indegree = [0 for _path in paths]

        def add_edge(before: int, after: int) -> None:
            if before != after and after not in edges[before]:
                edges[before].add(after)
                indegree[after] += 1

        for submit_index, submit_entry in entries.items():
            if not isinstance(submit_entry.envelope.command, SubmitJobCommand):
                continue
            for control_index, control_entry in entries.items():
                if isinstance(control_entry.envelope.command, SubmitJobCommand):
                    continue
                if control_entry.envelope.command.job_id == submit_entry.envelope.command.job_id:
                    add_edge(submit_index, control_index)
        for cancel_index, cancel_entry in entries.items():
            cancel = cancel_entry.envelope.command
            if not isinstance(cancel, CancelJobCommand):
                continue
            for control_index, control_entry in entries.items():
                control = control_entry.envelope.command
                if isinstance(control, PauseJobCommand | ResumeJobCommand) and (
                    control.job_id == cancel.job_id
                    and control.expected_version == cancel.expected_version
                ):
                    add_edge(cancel_index, control_index)

        ready = [index for index, count in enumerate(indegree) if count == 0]
        heapq.heapify(ready)
        ordered: list[Path] = []
        while ready:
            index = heapq.heappop(ready)
            ordered.append(paths[index])
            for dependent in edges[index]:
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    heapq.heappush(ready, dependent)
        if len(ordered) != len(paths):
            raise InvalidCommandEnvelopeError("cyclic command precedence in pending spool")
        return tuple(ordered)

    def pending_paths(self, *, limit: int | None = None) -> tuple[Path, ...]:
        with self._exclusive_lock():
            paths = tuple(
                sorted(
                    self._managed_paths(self.pending_dir, "*.json"),
                    key=self._delivery_key,
                )
            )
            ordered = self._apply_command_precedence(paths)
            return ordered if limit is None else ordered[:limit]

    def pending(self, *, limit: int | None = None) -> tuple[LabSpoolEntry, ...]:
        return tuple(self.load(path) for path in self.pending_paths(limit=limit))

    def ack(
        self,
        entry: LabSpoolEntry,
        receipt: LabCommandReceipt,
    ) -> LabAcknowledgedCommand:
        if (
            receipt.request_id != entry.envelope.request_id
            or receipt.content_hash != entry.envelope.content_hash
            or receipt.job_id != entry.envelope.command.job_id
        ):
            raise ValueError("receipt does not match command envelope")
        with self._exclusive_lock():
            current = self.load(entry.path)
            if (current.device, current.inode) != (entry.device, entry.inode):
                raise InvalidCommandEnvelopeError("pending command was replaced before ack")
            if current.envelope != entry.envelope:
                raise InvalidCommandEnvelopeError("pending command changed before ack")
            target = self.ack_dir / f"{receipt.request_id}.json"
            payload = canonical_model_json_bytes(receipt)
            created = self._publish_no_clobber(target, payload)
            if not created and self.load_receipt(target) != receipt:
                raise RequestContentConflictError(
                    f"request_id {receipt.request_id} already has a different receipt"
                )
            self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
            return LabAcknowledgedCommand(path=target, receipt=receipt)

    def load_receipt(self, path: Path) -> LabCommandReceipt:
        candidate, payload, _file_stat = self._read_regular_child(Path(path), self.ack_dir)
        filename_request_id = self._ack_request_id(candidate.name)
        try:
            receipt = strict_model_validate_canonical_json(LabCommandReceipt, payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid command receipt {candidate.name}: {exc}"
            ) from exc
        if receipt.request_id != filename_request_id:
            raise InvalidCommandEnvelopeError(
                f"receipt request_id does not match basename {candidate.name}"
            )
        return receipt

    def _unlink_pending(
        self,
        path: Path,
        *,
        device: int,
        inode: int,
        expected_link_count: int = 1,
    ) -> None:
        name = self._direct_child_name(path, self.pending_dir)
        directory_fd = self._open_managed_directory(self.pending_dir)
        try:
            try:
                current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except OSError as exc:
                raise InvalidCommandEnvelopeError(
                    f"pending command disappeared before unlink: {name}"
                ) from exc
            if (
                not stat.S_ISREG(current.st_mode)
                or current.st_dev != device
                or current.st_ino != inode
                or current.st_nlink != expected_link_count
            ):
                raise InvalidCommandEnvelopeError("pending command was replaced before unlink")
            self._guard_mutation()
            os.unlink(name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def quarantine(
        self,
        entry_or_path: LabSpoolEntry | LabSpoolFileIdentity | Path,
        *,
        reason: str,
    ) -> LabQuarantinedCommand:
        source = (
            entry_or_path.path
            if isinstance(entry_or_path, LabSpoolEntry | LabSpoolFileIdentity)
            else Path(entry_or_path)
        )
        with self._exclusive_lock():
            self._guard_mutation()
            source = Path(os.path.abspath(source))
            self._direct_child_name(source, self.pending_dir)
            try:
                observed = self._managed_entry_stat(source, self.pending_dir)
            except FileNotFoundError:
                return self._record_disappeared_locked(source, reason=reason)
            expected_link_target: str | None = None
            if isinstance(entry_or_path, LabSpoolFileIdentity):
                expected_type = entry_or_path.file_type
                if (
                    observed.st_dev != entry_or_path.device
                    or observed.st_ino != entry_or_path.inode
                    or self._spool_file_type(observed.st_mode) != expected_type
                ):
                    raise InvalidCommandEnvelopeError(
                        "pending command was replaced before quarantine"
                    )
                if observed.st_nlink != entry_or_path.link_count:
                    raise InvalidCommandEnvelopeError(
                        "pending command link count changed before quarantine"
                    )
                if expected_type == "regular" and observed.st_nlink == 1:
                    # The one shape whose bytes this directory entry owns, and the one shape
                    # a freed inode can be handed back to. Read it again and require the very
                    # bytes the identity was taken from: an inode reuse then reads as the
                    # replacement it is, not as the file that was observed to be bad.
                    if not entry_or_path.binds_content:
                        raise InvalidCommandEnvelopeError(
                            "pending command identity binds no content before quarantine"
                        )
                    _normalized, payload, observed = self._read_regular_child(
                        source,
                        self.pending_dir,
                    )
                    if (
                        len(payload) != entry_or_path.byte_count
                        or hashlib.sha256(payload).hexdigest() != entry_or_path.content_sha256
                    ):
                        raise InvalidCommandEnvelopeError(
                            "pending command was replaced before quarantine"
                        )
                expected_link_target = entry_or_path.link_target
            elif isinstance(entry_or_path, LabSpoolEntry):
                if (
                    not stat.S_ISREG(observed.st_mode)
                    or observed.st_dev != entry_or_path.device
                    or observed.st_ino != entry_or_path.inode
                    or observed.st_nlink != 1
                ):
                    raise InvalidCommandEnvelopeError(
                        "pending command was replaced before quarantine"
                    )
                if self.load(source).envelope != entry_or_path.envelope:
                    raise InvalidCommandEnvelopeError("pending command changed before quarantine")
            else:
                normalized, payload, observed = self._read_regular_child(
                    source,
                    self.pending_dir,
                )
                try:
                    _sequence, filename_request_id = self._pending_name_parts(normalized.name)
                    envelope = strict_model_validate_canonical_json(LabCommandEnvelope, payload)
                except (InvalidCommandEnvelopeError, ValueError):
                    envelope = None
                    filename_request_id = None
                if envelope is not None and envelope.request_id != filename_request_id:
                    raise InvalidCommandEnvelopeError(
                        f"command request_id does not match basename {normalized.name}"
                    )
            isolated = self._isolate_owned_entry_locked(
                source,
                observed,
                reason=reason,
                expected_link_target=expected_link_target,
            )
            self._prune_quarantine_locked()
            return isolated

    def _record_disappeared_locked(
        self,
        path: Path,
        *,
        reason: str,
    ) -> LabQuarantinedCommand:
        name = self._direct_child_name(path, self.pending_dir)
        reason_hash = hashlib.sha256(reason.encode("utf-8")).hexdigest()[:16]
        target = self.quarantine_dir / f"{name}.{reason_hash}.disappeared.bad.json"
        artifact = LabDisappearedQuarantineArtifact(
            original_name=name,
            reason=reason,
        )
        payload = canonical_model_json_bytes(artifact)
        if not self._publish_no_clobber(target, payload):
            _candidate, existing, _file_stat = self._read_regular_child(
                target,
                self.quarantine_dir,
            )
            if existing != payload:
                raise RequestContentConflictError(
                    f"disappeared quarantine evidence conflicts: {target.name}"
                )
        return LabQuarantinedCommand(path=target, reason=reason)

    @staticmethod
    def _after_hardlink_quarantine_evidence(
        _identity: LabSpoolFileIdentity,
        _evidence_path: Path,
    ) -> None:
        """Fault-injection boundary before the final hard-link identity check."""
