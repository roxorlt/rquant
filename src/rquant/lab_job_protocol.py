"""Typed, durable command protocol for the Strategy Lab control plane."""

from __future__ import annotations

import fcntl
import hashlib
import heapq
import json
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_run_spec import ResearchRunSpec

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
    path: Path
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    file_type: _LabSpoolFileType = "regular"
    link_count: int = Field(default=1, ge=1)
    link_target: str | None = None

    @model_validator(mode="after")
    def validate_link_target(self) -> LabSpoolFileIdentity:
        if self.file_type == "symlink" and self.link_target is None:
            raise ValueError("symlink identity requires link_target")
        if self.file_type != "symlink" and self.link_target is not None:
            raise ValueError("only symlink identity may have link_target")
        return self


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
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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

    @model_validator(mode="after")
    def validate_identity(self) -> _LabOwnedEntryIsolationEvidence:
        if Path(self.source_name).name != self.source_name:
            raise ValueError("isolated source_name must be a basename")
        if LabCommandSpool._spool_file_type(self.mode) != self.file_type:
            raise ValueError("isolated file_type does not match mode")
        if (self.file_type == "symlink") != (self.link_target is not None):
            raise ValueError("isolated symlink identity requires link_target")
        return self


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

    def __init__(
        self,
        root: Path,
        *,
        max_isolation_records: int = 256,
        max_isolation_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if max_isolation_records < 1:
            raise ValueError("max_isolation_records must be positive")
        if max_isolation_bytes < 1:
            raise ValueError("max_isolation_bytes must be positive")
        self.root = Path(os.path.abspath(root))
        self.pending_dir = self.root / "pending"
        self.ack_dir = self.root / "ack"
        self.quarantine_dir = self.root / "quarantine"
        self._lock_path = self.root / ".spool.lock"
        self._sequence_path = self.root / ".delivery-sequence"
        self._thread_lock = RLock()
        self.max_isolation_records = max_isolation_records
        self.max_isolation_bytes = max_isolation_bytes
        for path in (self.pending_dir, self.ack_dir, self.quarantine_dir):
            path.mkdir(parents=True, exist_ok=True)
        with self._exclusive_lock():
            self._reconcile_owned_isolations_locked()
            self._prune_owned_isolations_locked()

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        with self._thread_lock:
            descriptor = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @classmethod
    def _publish_no_clobber(cls, target: Path, payload: bytes) -> bool:
        temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                return False
            cls._fsync_directory(target.parent)
            return True
        finally:
            temporary.unlink(missing_ok=True)

    def _next_sequence_locked(self) -> int:
        if self._sequence_path.exists():
            raw = self._sequence_path.read_text(encoding="ascii").strip()
            if not raw.isdigit():
                raise InvalidCommandEnvelopeError("invalid durable delivery sequence")
            current = int(raw)
        else:
            current = 0
        sequence = current + 1
        temporary = self.root / f".{self._sequence_path.name}.{uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as stream:
                stream.write(f"{sequence}\n".encode("ascii"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._sequence_path)
            self._fsync_directory(self.root)
        finally:
            temporary.unlink(missing_ok=True)
        return sequence

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

    @staticmethod
    def _after_owned_entry_isolation_stage(
        _stage: Literal["evidence_written", "entry_moved"],
        _source: Path,
        _container: Path,
    ) -> None:
        """Fault-injection boundary for the common owned-entry isolation primitive."""

    def _move_bound_entry_into_container_locked(
        self,
        source: Path,
        container: Path,
        observed: os.stat_result,
        *,
        expected_link_target: str | None,
    ) -> Path:
        source_name = self._direct_child_name(source, source.parent)
        source_fd = os.open(
            source.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        container_fd = os.open(
            container,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            current = os.stat(source_name, dir_fd=source_fd, follow_symlinks=False)
            if not self._stat_matches_bound_entry(current, observed):
                if (
                    current.st_dev == observed.st_dev
                    and current.st_ino == observed.st_ino
                    and current.st_nlink != observed.st_nlink
                ):
                    raise InvalidCommandEnvelopeError(
                        f"owned entry link count changed before isolation: {source.name}"
                    )
                raise InvalidCommandEnvelopeError(
                    f"owned entry changed before isolation: {source.name}"
                )
            if expected_link_target is not None and (
                not stat.S_ISLNK(current.st_mode)
                or os.readlink(source_name, dir_fd=source_fd) != expected_link_target
            ):
                raise InvalidCommandEnvelopeError(
                    f"owned symlink target changed before isolation: {source.name}"
                )
            try:
                os.stat("entry", dir_fd=container_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise InvalidCommandEnvelopeError(
                    f"owned isolation destination already exists: {container.name}"
                )
            # The private 0700 container is newly created by the sole application writer.
            # This application capability is an integrity boundary, not protection against
            # a process that can arbitrarily rewrite the spool directory itself.
            os.rename(source_name, "entry", src_dir_fd=source_fd, dst_dir_fd=container_fd)
            destination = os.stat("entry", dir_fd=container_fd, follow_symlinks=False)
            if not self._stat_matches_bound_entry(destination, observed):
                raise InvalidCommandEnvelopeError(
                    f"owned entry identity changed during isolation: {source.name}"
                )
            if expected_link_target is not None and (
                not stat.S_ISLNK(destination.st_mode)
                or os.readlink("entry", dir_fd=container_fd) != expected_link_target
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
        return container / "entry"

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
            source_fd = os.open(
                source.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                expected_link_target = os.readlink(source_name, dir_fd=source_fd)
            finally:
                os.close(source_fd)
        isolation_id = uuid4()
        container = self.quarantine_dir / f"owned-entry-{isolation_id}.dead"
        os.mkdir(container, mode=0o700)
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
        try:
            if not self._publish_no_clobber(
                evidence_path,
                evidence.model_dump_json().encode("utf-8"),
            ):
                raise RequestContentConflictError(
                    f"owned isolation evidence already exists: {container.name}"
                )
            self._fsync_directory(self.quarantine_dir)
            self._after_owned_entry_isolation_stage("evidence_written", source, container)
            if stat.S_ISREG(observed.st_mode) and observed.st_nlink != 1:
                self._after_hardlink_quarantine_evidence(
                    LabSpoolFileIdentity(
                        path=source,
                        device=observed.st_dev,
                        inode=observed.st_ino,
                        link_count=observed.st_nlink,
                    ),
                    evidence_path,
                )
            destination = self._move_bound_entry_into_container_locked(
                source,
                container,
                observed,
                expected_link_target=expected_link_target,
            )
            self._after_owned_entry_isolation_stage("entry_moved", source, container)
            return LabQuarantinedCommand(path=destination, reason=reason)
        except BaseException:
            # A prepared bundle is intentionally retained. Startup either resumes the
            # identity-bound move or prunes an incomplete record within configured limits.
            self._fsync_directory(self.quarantine_dir)
            raise

    def _load_owned_isolation_evidence(
        self,
        container: Path,
    ) -> _LabOwnedEntryIsolationEvidence:
        match = self._OWNED_ISOLATION_NAME.fullmatch(container.name)
        if match is None:
            raise InvalidCommandEnvelopeError(
                f"invalid owned isolation container: {container.name}"
            )
        _path, payload, _file_stat = self._read_regular_child(
            container / "evidence.json",
            container,
        )
        evidence = _LabOwnedEntryIsolationEvidence.model_validate_json(payload)
        if evidence.model_dump_json().encode("utf-8") != payload:
            raise InvalidCommandEnvelopeError(
                f"owned isolation evidence is not canonical: {container.name}"
            )
        if str(evidence.isolation_id) != match["isolation_id"]:
            raise InvalidCommandEnvelopeError(
                f"owned isolation evidence id mismatch: {container.name}"
            )
        return evidence

    def _write_recovered_isolation_evidence_locked(
        self,
        container: Path,
        isolation_id: UUID,
        entry_stat: os.stat_result,
    ) -> _LabOwnedEntryIsolationEvidence:
        link_target = os.readlink(container / "entry") if stat.S_ISLNK(entry_stat.st_mode) else None
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
        )
        evidence_path = container / "evidence.json"
        if not self._publish_no_clobber(
            evidence_path,
            evidence.model_dump_json().encode("utf-8"),
        ):
            raise InvalidCommandEnvelopeError(
                f"cannot replace invalid isolation evidence: {container.name}"
            )
        return evidence

    def _reconcile_owned_isolation_container_locked(self, container: Path) -> None:
        match = self._OWNED_ISOLATION_NAME.fullmatch(container.name)
        if match is None:
            return
        try:
            container_stat = container.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISDIR(container_stat.st_mode):
            self._isolate_owned_entry_locked(
                container,
                container_stat,
                reason="owned isolation namespace occupied by a non-directory entry",
            )
            return
        entry = container / "entry"
        try:
            entry_stat = entry.lstat()
        except FileNotFoundError:
            entry_stat = None
        try:
            evidence = self._load_owned_isolation_evidence(container)
        except (InvalidCommandEnvelopeError, ValueError):
            if entry_stat is None:
                return
            evidence_path = container / "evidence.json"
            if os.path.lexists(evidence_path):
                invalid_path = container / f"invalid-evidence-{uuid4().hex}.raw"
                os.rename(evidence_path, invalid_path)
                self._fsync_directory(container)
            evidence = self._write_recovered_isolation_evidence_locked(
                container,
                UUID(match["isolation_id"]),
                entry_stat,
            )
        if entry_stat is not None:
            if not self._stat_matches_isolation(entry_stat, evidence):
                return
            if evidence.link_target is not None and os.readlink(entry) != evidence.link_target:
                return
            return
        source = self._owned_source_path(evidence)
        if source is None:
            return
        try:
            source_stat = source.lstat()
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
        for container in sorted(self.quarantine_dir.glob("owned-entry-*.dead")):
            if self._OWNED_ISOLATION_NAME.fullmatch(container.name) is None:
                continue
            with suppress(OSError, InvalidCommandEnvelopeError, ValueError):
                self._reconcile_owned_isolation_container_locked(container)

    def _owned_isolation_records_locked(self) -> list[_LabOwnedIsolationRecord]:
        records: list[_LabOwnedIsolationRecord] = []
        for container in self.quarantine_dir.glob("owned-entry-*.dead"):
            if self._OWNED_ISOLATION_NAME.fullmatch(container.name) is None:
                continue
            try:
                container_stat = container.lstat()
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
            try:
                names = os.listdir(container)
            except OSError:
                names = []
            for name in names:
                try:
                    child_stat = (container / name).lstat()
                except FileNotFoundError:
                    continue
                modified_at_ns = max(modified_at_ns, child_stat.st_mtime_ns)
                byte_count += max(0, child_stat.st_size)
            records.append(
                _LabOwnedIsolationRecord(
                    container=container,
                    container_stat=container_stat,
                    modified_at_ns=modified_at_ns,
                    byte_count=byte_count,
                )
            )
        return records

    @classmethod
    def _remove_bound_directory_entry(
        cls,
        parent_fd: int,
        name: str,
        observed: os.stat_result,
    ) -> bool:
        try:
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return True
        if not cls._stat_matches_bound_entry(current, observed):
            return False
        try:
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
            current_container = record.container.lstat()
        except FileNotFoundError:
            return True
        if (
            current_container.st_dev != record.container_stat.st_dev
            or current_container.st_ino != record.container_stat.st_ino
            or current_container.st_mode != record.container_stat.st_mode
        ):
            return False
        if not stat.S_ISDIR(current_container.st_mode):
            quarantine_fd = os.open(
                self.quarantine_dir,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                return self._remove_bound_directory_entry(
                    quarantine_fd,
                    record.container.name,
                    current_container,
                )
            finally:
                os.close(quarantine_fd)
        container_fd = os.open(
            record.container,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            names = os.listdir(container_fd)
            if any(
                name not in {"entry", "evidence.json"} and not name.startswith("invalid-evidence-")
                for name in names
            ):
                return False
            for name in names:
                try:
                    child_stat = os.stat(name, dir_fd=container_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not self._remove_bound_directory_entry(container_fd, name, child_stat):
                    return False
        finally:
            os.close(container_fd)
        quarantine_fd = os.open(
            self.quarantine_dir,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
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

    @staticmethod
    def _read_regular_child(
        path: Path,
        parent: Path,
        *,
        allowed_link_counts: frozenset[int] = frozenset({1}),
    ) -> tuple[Path, bytes, os.stat_result]:
        name = LabCommandSpool._direct_child_name(path, parent)
        normalized = Path(os.path.abspath(parent)) / name
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        file_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        directory_fd = os.open(parent, directory_flags)
        try:
            try:
                path_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except OSError as exc:
                raise InvalidCommandEnvelopeError(f"unsafe spool file {name}: {exc}") from exc
            if stat.S_ISLNK(path_stat.st_mode):
                link_target = os.readlink(name, dir_fd=directory_fd)
                identity = LabSpoolFileIdentity(
                    path=normalized,
                    device=path_stat.st_dev,
                    inode=path_stat.st_ino,
                    file_type="symlink",
                    link_count=path_stat.st_nlink,
                    link_target=link_target,
                )
                raise InvalidCommandEnvelopeError(
                    f"spool file {name} is a symlink",
                    file_identity=identity,
                )
            if not stat.S_ISREG(path_stat.st_mode):
                identity = LabSpoolFileIdentity(
                    path=normalized,
                    device=path_stat.st_dev,
                    inode=path_stat.st_ino,
                    file_type=LabCommandSpool._spool_file_type(path_stat.st_mode),
                    link_count=path_stat.st_nlink,
                )
                raise InvalidCommandEnvelopeError(
                    f"spool file {name} is not regular",
                    file_identity=identity,
                )
            if path_stat.st_nlink not in allowed_link_counts:
                raise InvalidCommandEnvelopeError(
                    f"spool file {name} has an external hard link",
                    file_identity=LabSpoolFileIdentity(
                        path=normalized,
                        device=path_stat.st_dev,
                        inode=path_stat.st_ino,
                        link_count=path_stat.st_nlink,
                    ),
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
                        file_identity=LabSpoolFileIdentity(
                            path=normalized,
                            device=path_stat.st_dev,
                            inode=path_stat.st_ino,
                            link_count=path_stat.st_nlink,
                        ),
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
        for candidate in self.pending_dir.glob("*.json"):
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

    def publish(
        self,
        envelope: LabCommandEnvelope,
    ) -> LabSpoolEntry | LabAcknowledgedCommand:
        validated = LabCommandEnvelope.model_validate(envelope)
        payload = validated.model_dump_json().encode("utf-8")
        with self._exclusive_lock():
            ack_path = self.ack_dir / f"{validated.request_id}.json"
            pending_path = self._pending_for_request_locked(validated.request_id)
            if os.path.lexists(ack_path):
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
        identity = LabSpoolFileIdentity(
            path=candidate,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )
        try:
            _sequence, filename_request_id = self._pending_name_parts(candidate.name)
        except InvalidCommandEnvelopeError as exc:
            raise InvalidCommandEnvelopeError(
                str(exc),
                file_identity=identity,
            ) from exc
        try:
            envelope = LabCommandEnvelope.model_validate_json(payload)
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
            paths = tuple(sorted(self.pending_dir.glob("*.json"), key=self._delivery_key))
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
            payload = receipt.model_dump_json().encode("utf-8")
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
            receipt = LabCommandReceipt.model_validate_json(payload)
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
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(self.pending_dir, directory_flags)
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
            source = Path(os.path.abspath(source))
            self._direct_child_name(source, self.pending_dir)
            try:
                observed = source.lstat()
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
                    envelope = LabCommandEnvelope.model_validate_json(payload)
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
            self._prune_owned_isolations_locked()
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
        payload = artifact.model_dump_json().encode("utf-8")
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
