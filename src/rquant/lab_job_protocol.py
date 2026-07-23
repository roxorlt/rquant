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
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_run_spec import ResearchRunSpec


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
    file_type: Literal["regular", "symlink"] = "regular"
    link_target: str | None = None

    @model_validator(mode="after")
    def validate_link_target(self) -> LabSpoolFileIdentity:
        if self.file_type == "symlink" and self.link_target is None:
            raise ValueError("symlink identity requires link_target")
        if self.file_type == "regular" and self.link_target is not None:
            raise ValueError("regular identity must not have link_target")
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


class LabSymlinkQuarantineArtifact(LabProtocolModel):
    schema_version: Literal[1] = 1
    original_name: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    link_target: str
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


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

    def __init__(self, root: Path) -> None:
        self.root = Path(os.path.abspath(root))
        self.pending_dir = self.root / "pending"
        self.ack_dir = self.root / "ack"
        self.quarantine_dir = self.root / "quarantine"
        self._lock_path = self.root / ".spool.lock"
        self._sequence_path = self.root / ".delivery-sequence"
        self._thread_lock = RLock()
        for path in (self.pending_dir, self.ack_dir, self.quarantine_dir):
            path.mkdir(parents=True, exist_ok=True)

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
    def _read_regular_child(path: Path, parent: Path) -> tuple[Path, bytes, os.stat_result]:
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
                    link_target=link_target,
                )
                raise InvalidCommandEnvelopeError(
                    f"spool file {name} is a symlink",
                    file_identity=identity,
                )
            if not stat.S_ISREG(path_stat.st_mode):
                raise InvalidCommandEnvelopeError(f"spool file {name} is not regular")
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
                ):
                    raise InvalidCommandEnvelopeError(
                        f"spool file {name} was replaced while opening",
                        file_identity=LabSpoolFileIdentity(
                            path=normalized,
                            device=path_stat.st_dev,
                            inode=path_stat.st_ino,
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

    def _unlink_pending(self, path: Path, *, device: int, inode: int) -> None:
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
            if (
                isinstance(entry_or_path, LabSpoolFileIdentity)
                and entry_or_path.file_type == "symlink"
            ):
                return self._quarantine_symlink_locked(entry_or_path, reason=reason)
            normalized, payload, source_stat = self._read_regular_child(source, self.pending_dir)
            try:
                _sequence, filename_request_id = self._pending_name_parts(normalized.name)
            except InvalidCommandEnvelopeError:
                filename_request_id = None
            try:
                envelope = LabCommandEnvelope.model_validate_json(payload)
            except Exception:
                envelope = None
            if (
                envelope is not None
                and filename_request_id is not None
                and envelope.request_id != filename_request_id
            ):
                raise InvalidCommandEnvelopeError(
                    f"command request_id does not match basename {normalized.name}"
                )
            if isinstance(entry_or_path, LabSpoolEntry | LabSpoolFileIdentity):
                if (source_stat.st_dev, source_stat.st_ino) != (
                    entry_or_path.device,
                    entry_or_path.inode,
                ):
                    raise InvalidCommandEnvelopeError(
                        "pending command was replaced before quarantine"
                    )
                if isinstance(entry_or_path, LabSpoolEntry) and envelope != entry_or_path.envelope:
                    raise InvalidCommandEnvelopeError("pending command changed before quarantine")
            target = self.quarantine_dir / f"{normalized.name}.bad"
            while os.path.lexists(target):
                target = self.quarantine_dir / f"{normalized.name}.{uuid4().hex}.bad"
            source_fd = os.open(
                self.pending_dir,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            target_fd = os.open(
                self.quarantine_dir,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.link(
                    normalized.name,
                    target.name,
                    src_dir_fd=source_fd,
                    dst_dir_fd=target_fd,
                    follow_symlinks=False,
                )
                os.fsync(target_fd)
            finally:
                os.close(target_fd)
                os.close(source_fd)
            quarantined = LabQuarantinedCommand(path=target, reason=reason)
            metadata = self.quarantine_dir / f"{target.name}.json"
            self._publish_no_clobber(
                metadata,
                quarantined.model_dump_json().encode("utf-8"),
            )
            self._unlink_pending(
                normalized,
                device=source_stat.st_dev,
                inode=source_stat.st_ino,
            )
            return quarantined

    def _quarantine_symlink_locked(
        self,
        identity: LabSpoolFileIdentity,
        *,
        reason: str,
    ) -> LabQuarantinedCommand:
        name = self._direct_child_name(identity.path, self.pending_dir)
        directory_fd = os.open(
            self.pending_dir,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                not stat.S_ISLNK(current.st_mode)
                or current.st_dev != identity.device
                or current.st_ino != identity.inode
            ):
                raise InvalidCommandEnvelopeError("pending symlink was replaced before quarantine")
            link_target = os.readlink(name, dir_fd=directory_fd)
            if link_target != identity.link_target:
                raise InvalidCommandEnvelopeError(
                    "pending symlink target changed before quarantine"
                )
            target = self.quarantine_dir / f"{name}.symlink.bad.json"
            while os.path.lexists(target):
                target = self.quarantine_dir / f"{name}.{uuid4().hex}.symlink.bad.json"
            artifact = LabSymlinkQuarantineArtifact(
                original_name=name,
                reason=reason,
                link_target=link_target,
                device=identity.device,
                inode=identity.inode,
            )
            if not self._publish_no_clobber(
                target,
                artifact.model_dump_json().encode("utf-8"),
            ):
                raise RequestContentConflictError(
                    f"symlink quarantine artifact already exists: {target.name}"
                )
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                not stat.S_ISLNK(current.st_mode)
                or current.st_dev != identity.device
                or current.st_ino != identity.inode
            ):
                raise InvalidCommandEnvelopeError("pending symlink was replaced before unlink")
            os.unlink(name, dir_fd=directory_fd)
            os.fsync(directory_fd)
            return LabQuarantinedCommand(path=target, reason=reason)
        finally:
            os.close(directory_fd)
