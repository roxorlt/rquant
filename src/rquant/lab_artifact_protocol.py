"""Typed durable commit channel for complete Strategy Lab result artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from bisect import bisect_right
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.lab_job_protocol import (
    InvalidCommandEnvelopeError,
    LabCommandSpool,
    LabQuarantinedCommand,
    LabSpoolFileIdentity,
    RequestContentConflictError,
)
from rquant.research_run_spec import DatasetSnapshotIdentity

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_CODE_SHA_PATTERN = r"^[0-9a-f]{40}$"


class LabArtifactCommitProtocolModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=False,
    )


class LabArtifactCommit(LabArtifactCommitProtocolModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    result_contract_version: str = Field(min_length=1)
    code_sha: str = Field(pattern=_CODE_SHA_PATTERN)
    dataset_snapshot: DatasetSnapshotIdentity | None
    manifest_hash: str = Field(pattern=_HASH_PATTERN)
    complete_result_hash: str = Field(pattern=_HASH_PATTERN)
    sealed_path: Path

    @model_validator(mode="after")
    def validate_sealed_path(self) -> LabArtifactCommit:
        normalized = Path(os.path.abspath(self.sealed_path))
        if not self.sealed_path.is_absolute() or self.sealed_path != normalized:
            raise ValueError("sealed_path must be an absolute normalized path")
        return self

    def canonical_json_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")


class LabArtifactCommitEnvelope(LabArtifactCommitProtocolModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    commit: LabArtifactCommit
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabArtifactCommitEnvelope:
        expected = hashlib.sha256(self.commit.canonical_json_bytes()).hexdigest()
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match canonical artifact commit content")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabArtifactCommitReceipt(LabArtifactCommitProtocolModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    content_hash: str = Field(pattern=_HASH_PATTERN)
    job_id: UUID
    status: Literal["accepted", "rejected"]
    reason: str = Field(min_length=1)
    accepted_at: datetime
    job_version: int | None = Field(default=None, strict=True, ge=0)

    @model_validator(mode="after")
    def validate_accepted_at(self) -> LabArtifactCommitReceipt:
        if self.accepted_at.tzinfo is None or self.accepted_at.utcoffset() is None:
            raise ValueError("accepted_at must be timezone-aware")
        return self

    @classmethod
    def from_envelope(
        cls,
        envelope: LabArtifactCommitEnvelope,
        *,
        status: Literal["accepted", "rejected"],
        reason: str,
        accepted_at: datetime,
        job_version: int | None,
    ) -> LabArtifactCommitReceipt:
        return cls(
            request_id=envelope.request_id,
            content_hash=envelope.content_hash,
            job_id=envelope.commit.job_id,
            status=status,
            reason=reason,
            accepted_at=accepted_at,
            job_version=job_version,
        )


class LabArtifactCommitSpoolEntry(LabArtifactCommitProtocolModel):
    path: Path
    envelope: LabArtifactCommitEnvelope
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


class LabAcknowledgedArtifactCommit(LabArtifactCommitProtocolModel):
    path: Path
    receipt: LabArtifactCommitReceipt


class LabQuarantinedArtifactCommit(LabArtifactCommitProtocolModel):
    path: Path
    reason: str = Field(min_length=1)


class LabArtifactConflictEvidence(LabArtifactCommitProtocolModel):
    schema_version: Literal[1] = 1
    state: Literal["complete"] = "complete"
    request_id: UUID
    content_hash: str = Field(pattern=_HASH_PATTERN)
    reason_hash: str = Field(pattern=r"^[0-9a-f]{16}$")
    reason: str = Field(min_length=1)
    envelope: LabArtifactCommitEnvelope

    @model_validator(mode="after")
    def validate_identity(self) -> LabArtifactConflictEvidence:
        if self.request_id != self.envelope.request_id:
            raise ValueError("conflict evidence request_id mismatch")
        if self.content_hash != self.envelope.content_hash:
            raise ValueError("conflict evidence content_hash mismatch")
        expected_reason_hash = hashlib.sha256(self.reason.encode("utf-8")).hexdigest()[:16]
        if self.reason_hash != expected_reason_hash:
            raise ValueError("conflict evidence reason_hash mismatch")
        return self

    @classmethod
    def from_conflict(
        cls,
        envelope: LabArtifactCommitEnvelope,
        *,
        reason: str,
    ) -> LabArtifactConflictEvidence:
        return cls(
            request_id=envelope.request_id,
            content_hash=envelope.content_hash,
            reason_hash=hashlib.sha256(reason.encode("utf-8")).hexdigest()[:16],
            reason=reason,
            envelope=envelope,
        )


class LabArtifactCommitScanCursor(LabArtifactCommitProtocolModel):
    schema_version: Literal[1] = 1
    last_pending_name: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_pending_name(self) -> LabArtifactCommitScanCursor:
        is_basename = Path(self.last_pending_name).name == self.last_pending_name
        if not is_basename or not self.last_pending_name.endswith(".json"):
            raise ValueError("artifact scan cursor must contain a pending JSON basename")
        return self


class LabCorruptConflictTempEvidence(LabArtifactCommitProtocolModel):
    schema_version: Literal[1] = 1
    state: Literal["corrupt_temporary"] = "corrupt_temporary"
    temporary_name: str = Field(min_length=1)
    target_name: str = Field(min_length=1)
    raw_name: str | None = Field(default=None, min_length=1)
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    byte_count: int = Field(ge=0)
    content_hash: str = Field(pattern=_HASH_PATTERN)
    reason: Literal["invalid_typed_conflict_temporary"] = "invalid_typed_conflict_temporary"


@dataclass(frozen=True)
class _ConflictEvidenceRecord:
    modified_at_ns: int
    name: str
    size: int
    files: tuple[tuple[Path, os.stat_result], ...]


class LabArtifactCommitSpool(LabCommandSpool):
    """Atomic commit inbox; conflict evidence defaults to 256 bundles or 64 MiB.

    Cleanup removes only validated complete or owned-incomplete records, oldest
    first. A single newest record survives even when it exceeds the byte budget.
    """

    _CONFLICT_NAME = re.compile(
        r"(?P<request_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12})\."
        r"(?P<content_hash>[0-9a-f]{64})\."
        r"(?P<reason_hash>[0-9a-f]{16})\.conflict\.evidence\.json"
    )
    _CONFLICT_TEMP_NAME = re.compile(
        r"\.(?P<target>"
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\."
        r"[0-9a-f]{64}\.[0-9a-f]{16}\.conflict\.evidence\.json"
        r")\.publishing\.tmp"
    )
    _LEGACY_CONFLICT_NAME = re.compile(
        r"(?P<request_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
        r"[0-9a-f]{4}-[0-9a-f]{12})\."
        r"(?P<content_hash>[0-9a-f]{64})\."
        r"(?P<reason_hash>[0-9a-f]{16})\.conflict\.bad"
    )
    _CORRUPT_TEMP_NAME = re.compile(
        r"(?P<temporary_hash>[0-9a-f]{16})\."
        r"(?P<device>[0-9a-f]+)\.(?P<inode>[0-9a-f]+)\."
        r"(?P<content_hash>[0-9a-f]{16})\.corrupt-conflict-temp\.bad\.json"
    )
    _CORRUPT_TEMP_RAW_NAME = re.compile(
        r"(?P<target>"
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\."
        r"[0-9a-f]{64}\.[0-9a-f]{16}\.conflict\.evidence\.json"
        r")\.(?P<device>[0-9a-f]+)\.(?P<inode>[0-9a-f]+)\."
        r"corrupt-conflict-temp\.raw\.bad"
    )

    def __init__(
        self,
        root: Path,
        *,
        max_conflict_records: int = 256,
        max_conflict_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if max_conflict_records < 1:
            raise ValueError("max_conflict_records must be positive")
        if max_conflict_bytes < 1:
            raise ValueError("max_conflict_bytes must be positive")
        super().__init__(root)
        self._scan_cursor_path = self.root / ".artifact-commit-scan-cursor.json"
        self.max_conflict_records = max_conflict_records
        self.max_conflict_bytes = max_conflict_bytes
        with self._exclusive_lock():
            self._recover_conflict_evidence_locked()
            self._prune_conflicts_locked()

    @staticmethod
    def _after_conflict_evidence_stage(
        _stage: Literal["temporary_written", "target_linked", "temporary_unlinked"],
        _path: Path,
    ) -> None:
        """Fault-injection boundary for atomic conflict evidence publication."""

    def _conflict_evidence_path(self, evidence: LabArtifactConflictEvidence) -> Path:
        return self.quarantine_dir / (
            f"{evidence.request_id}.{evidence.content_hash}.{evidence.reason_hash}."
            "conflict.evidence.json"
        )

    def _conflict_temporary_path(self, evidence: LabArtifactConflictEvidence) -> Path:
        target = self._conflict_evidence_path(evidence)
        return self.quarantine_dir / f".{target.name}.publishing.tmp"

    def _load_scan_cursor_locked(self) -> LabArtifactCommitScanCursor | None:
        if not os.path.lexists(self._scan_cursor_path):
            return None
        try:
            observed = self._scan_cursor_path.lstat()
        except FileNotFoundError:
            return None
        try:
            _candidate, payload, _file_stat = self._read_regular_child(
                self._scan_cursor_path,
                self.root,
            )
            cursor = LabArtifactCommitScanCursor.model_validate_json(payload)
            if cursor.model_dump_json().encode("utf-8") != payload:
                raise ValueError("artifact scan cursor JSON is not canonical")
            return cursor
        except (InvalidCommandEnvelopeError, ValueError) as exc:
            with suppress(OSError):
                self._isolate_scan_cursor_locked(observed, reason=str(exc))
            return None

    def _isolate_scan_cursor_locked(
        self,
        observed: os.stat_result,
        *,
        reason: str,
    ) -> bool:
        file_type = self._spool_file_type(observed.st_mode)
        reason_hash = hashlib.sha256(reason.encode("utf-8")).hexdigest()[:16]
        target = self.quarantine_dir / (
            f"artifact-commit-scan-cursor.{observed.st_dev:x}.{observed.st_ino:x}."
            f"{observed.st_nlink}.{file_type}.{reason_hash}.bad"
        )
        while os.path.lexists(target):
            target = self.quarantine_dir / (
                f"artifact-commit-scan-cursor.{observed.st_dev:x}.{observed.st_ino:x}."
                f"{observed.st_nlink}.{file_type}.{reason_hash}.{uuid4().hex}.bad"
            )
        root_fd = os.open(self.root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        quarantine_fd = os.open(
            self.quarantine_dir,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            try:
                current = os.stat(
                    self._scan_cursor_path.name,
                    dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return True
            if (
                stat.S_IFMT(current.st_mode) != stat.S_IFMT(observed.st_mode)
                or current.st_dev != observed.st_dev
                or current.st_ino != observed.st_ino
                or current.st_nlink != observed.st_nlink
            ):
                return False
            os.rename(
                self._scan_cursor_path.name,
                target.name,
                src_dir_fd=root_fd,
                dst_dir_fd=quarantine_fd,
            )
            os.fsync(root_fd)
            os.fsync(quarantine_fd)
            return True
        finally:
            os.close(quarantine_fd)
            os.close(root_fd)

    @staticmethod
    def _after_scan_cursor_stage(
        _stage: Literal["temporary_written", "cursor_replaced"],
        _path: Path,
    ) -> None:
        """Fault-injection boundary for advisory scan cursor publication."""

    def _write_scan_cursor_locked(self, cursor: LabArtifactCommitScanCursor) -> None:
        temporary = self.root / f".{self._scan_cursor_path.name}.{uuid4().hex}.tmp"
        try:
            try:
                with temporary.open("xb") as stream:
                    stream.write(cursor.model_dump_json().encode("utf-8"))
                    stream.flush()
                    os.fsync(stream.fileno())
                self._fsync_directory(self.root)
                self._after_scan_cursor_stage("temporary_written", temporary)
                os.replace(temporary, self._scan_cursor_path)
                self._fsync_directory(self.root)
                self._after_scan_cursor_stage("cursor_replaced", self._scan_cursor_path)
            except OSError:
                return
        finally:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)

    def fair_pending_paths(self, *, limit: int) -> tuple[Path, ...]:
        if limit < 1:
            raise ValueError("artifact fair scan limit must be positive")
        with self._exclusive_lock():
            paths = tuple(sorted(self.pending_dir.glob("*.json"), key=self._delivery_key))
            if not paths:
                return ()
            cursor = self._load_scan_cursor_locked()
            start = 0
            if cursor is not None:
                keys = tuple(self._delivery_key(path) for path in paths)
                start = bisect_right(keys, self._delivery_key(Path(cursor.last_pending_name)))
                if start == len(paths):
                    start = 0
            rotated = paths[start:] + paths[:start]
            selected = rotated[:limit]
            self._write_scan_cursor_locked(
                LabArtifactCommitScanCursor(last_pending_name=selected[-1].name)
            )
            return selected

    @classmethod
    def _evidence_matches_name(
        cls,
        evidence: LabArtifactConflictEvidence,
        name: str,
    ) -> bool:
        match = cls._CONFLICT_NAME.fullmatch(name)
        return match is not None and (
            str(evidence.request_id),
            evidence.content_hash,
            evidence.reason_hash,
        ) == (
            match["request_id"],
            match["content_hash"],
            match["reason_hash"],
        )

    def _load_conflict_evidence_file(
        self,
        path: Path,
        *,
        allowed_link_counts: frozenset[int] = frozenset({1}),
    ) -> tuple[LabArtifactConflictEvidence, bytes, os.stat_result]:
        _candidate, payload, file_stat = self._read_regular_child(
            path,
            self.quarantine_dir,
            allowed_link_counts=allowed_link_counts,
        )
        evidence = LabArtifactConflictEvidence.model_validate_json(payload)
        return evidence, payload, file_stat

    @staticmethod
    def _corrupt_temp_evidence_name(evidence: LabCorruptConflictTempEvidence) -> str:
        temporary_hash = hashlib.sha256(evidence.temporary_name.encode("utf-8")).hexdigest()[:16]
        return (
            f"{temporary_hash}.{evidence.device:x}.{evidence.inode:x}."
            f"{evidence.content_hash[:16]}.corrupt-conflict-temp.bad.json"
        )

    def _load_corrupt_temp_evidence_file(
        self,
        path: Path,
    ) -> tuple[LabCorruptConflictTempEvidence, os.stat_result]:
        _candidate, payload, file_stat = self._read_regular_child(path, self.quarantine_dir)
        evidence = LabCorruptConflictTempEvidence.model_validate_json(payload)
        temporary_match = self._CONFLICT_TEMP_NAME.fullmatch(evidence.temporary_name)
        if temporary_match is None or temporary_match["target"] != evidence.target_name:
            raise InvalidCommandEnvelopeError(
                f"corrupt conflict temp identity mismatch: {path.name}"
            )
        if evidence.raw_name is not None:
            raw_match = self._CORRUPT_TEMP_RAW_NAME.fullmatch(evidence.raw_name)
            if raw_match is None or (
                raw_match["target"] != evidence.target_name
                or int(raw_match["device"], 16) != evidence.device
                or int(raw_match["inode"], 16) != evidence.inode
            ):
                raise InvalidCommandEnvelopeError(
                    f"corrupt conflict raw identity mismatch: {path.name}"
                )
        if path.name != self._corrupt_temp_evidence_name(evidence):
            raise InvalidCommandEnvelopeError(
                f"corrupt conflict temp evidence name mismatch: {path.name}"
            )
        return evidence, file_stat

    @classmethod
    def _corrupt_temp_raw_name(
        cls,
        *,
        target_name: str,
        file_stat: os.stat_result,
    ) -> str:
        if cls._CONFLICT_NAME.fullmatch(target_name) is None:
            raise InvalidCommandEnvelopeError(
                f"invalid corrupt conflict target name: {target_name}"
            )
        return (
            f"{target_name}.{file_stat.st_dev:x}.{file_stat.st_ino:x}.corrupt-conflict-temp.raw.bad"
        )

    @staticmethod
    def _after_corrupt_conflict_stage(
        _stage: Literal[
            "raw_moved",
            "evidence_written",
            "before_target_unlink",
            "target_unlinked",
        ],
        _raw_path: Path,
        _target_path: Path,
    ) -> None:
        """Fault-injection boundary for corrupt conflict evidence recovery."""

    @staticmethod
    def _before_conflict_temp_unlink(*_args: object) -> None:
        """Fault-injection boundary immediately before final link verification."""

    def _move_conflict_temp_to_raw_locked(
        self,
        temporary: Path,
        *,
        target_name: str,
        file_stat: os.stat_result,
    ) -> Path:
        raw = self.quarantine_dir / self._corrupt_temp_raw_name(
            target_name=target_name,
            file_stat=file_stat,
        )
        directory_fd = os.open(
            self.quarantine_dir,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        linked_raw = False
        try:
            current = os.stat(temporary.name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                not stat.S_ISREG(current.st_mode)
                or stat.S_IFMT(current.st_mode) != stat.S_IFMT(file_stat.st_mode)
                or current.st_dev != file_stat.st_dev
                or current.st_ino != file_stat.st_ino
                or current.st_nlink != file_stat.st_nlink
            ):
                raise InvalidCommandEnvelopeError(
                    f"conflict temporary changed before raw isolation: {temporary.name}"
                )
            try:
                os.link(
                    temporary.name,
                    raw.name,
                    src_dir_fd=directory_fd,
                    dst_dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                linked_raw = True
            except FileExistsError:
                raw_stat = os.stat(raw.name, dir_fd=directory_fd, follow_symlinks=False)
                if (
                    not stat.S_ISREG(raw_stat.st_mode)
                    or raw_stat.st_dev != file_stat.st_dev
                    or raw_stat.st_ino != file_stat.st_ino
                    or raw_stat.st_nlink != current.st_nlink
                ):
                    raise InvalidCommandEnvelopeError(
                        f"corrupt conflict raw target conflicts: {raw.name}"
                    ) from None
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        relation = self._matching_regular_entries(temporary, raw)
        if relation is None:
            raise InvalidCommandEnvelopeError(
                f"conflict temporary/raw relation changed: {temporary.name}"
            )
        temporary_current, _raw_current = relation
        self._unlink_regular_identity(
            temporary,
            temporary_current,
            allowed_link_counts=frozenset({temporary_current.st_nlink}),
        )
        if linked_raw:
            self._fsync_directory(self.quarantine_dir)
        self._after_corrupt_conflict_stage(
            "raw_moved",
            raw,
            self.quarantine_dir / target_name,
        )
        raw_stat = raw.lstat()
        if (
            not stat.S_ISREG(raw_stat.st_mode)
            or raw_stat.st_dev != file_stat.st_dev
            or raw_stat.st_ino != file_stat.st_ino
        ):
            raise InvalidCommandEnvelopeError(
                f"corrupt conflict raw changed after isolation: {raw.name}"
            )
        _target_name, raw_payload, raw_stat = self._load_corrupt_raw_locked(raw)
        self._record_corrupt_raw_locked(
            raw,
            target_name=target_name,
            payload=raw_payload,
            file_stat=raw_stat,
        )
        return raw

    def _publish_conflict_evidence_locked(
        self,
        evidence: LabArtifactConflictEvidence,
    ) -> None:
        self._recover_conflict_evidence_locked()
        target = self._conflict_evidence_path(evidence)
        payload = evidence.model_dump_json().encode("utf-8")
        if os.path.lexists(target):
            existing, existing_payload, _file_stat = self._load_conflict_evidence_file(target)
            if existing != evidence or existing_payload != payload:
                raise InvalidCommandEnvelopeError(
                    "artifact conflict evidence target has different content"
                )
            return

        temporary = self._conflict_temporary_path(evidence)
        if os.path.lexists(temporary):
            self._recover_conflict_evidence_locked()
            if os.path.lexists(target):
                existing, existing_payload, _file_stat = self._load_conflict_evidence_file(target)
                if existing == evidence and existing_payload == payload:
                    return
            raise InvalidCommandEnvelopeError(
                "artifact conflict evidence temporary cannot be recovered"
            )

        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        self._fsync_directory(self.quarantine_dir)
        self._after_conflict_evidence_stage("temporary_written", temporary)

        directory_fd = os.open(
            self.quarantine_dir,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.link(
                temporary.name,
                target.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        self._after_conflict_evidence_stage("target_linked", target)

        temporary_stat = temporary.lstat()
        self._unlink_regular_identity(
            temporary,
            temporary_stat,
            allowed_link_counts=frozenset({2}),
        )
        self._after_conflict_evidence_stage("temporary_unlinked", target)
        completed, completed_payload, _file_stat = self._load_conflict_evidence_file(target)
        if completed != evidence or completed_payload != payload:
            raise InvalidCommandEnvelopeError(
                "artifact conflict evidence changed during publication"
            )

    def _quarantine_conflicting_publish_locked(
        self,
        envelope: LabArtifactCommitEnvelope,
        *,
        reason: str,
    ) -> None:
        evidence = LabArtifactConflictEvidence.from_conflict(envelope, reason=reason)
        self._publish_conflict_evidence_locked(evidence)
        self._prune_conflicts_locked()

    @staticmethod
    def _unlink_regular_identity(
        path: Path,
        observed: os.stat_result,
        *,
        allowed_link_counts: frozenset[int] = frozenset({1}),
    ) -> None:
        directory_fd = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            try:
                current = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if (
                not stat.S_ISREG(current.st_mode)
                or current.st_nlink not in allowed_link_counts
                or current.st_dev != observed.st_dev
                or current.st_ino != observed.st_ino
            ):
                raise InvalidCommandEnvelopeError(
                    f"conflict evidence changed before retention cleanup: {path.name}"
                )
            os.unlink(path.name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    @staticmethod
    def _matching_regular_entries(
        first: Path,
        second: Path,
    ) -> tuple[os.stat_result, os.stat_result] | None:
        try:
            first_stat = first.lstat()
            second_stat = second.lstat()
        except FileNotFoundError:
            return None
        if (
            not stat.S_ISREG(first_stat.st_mode)
            or not stat.S_ISREG(second_stat.st_mode)
            or first_stat.st_dev != second_stat.st_dev
            or first_stat.st_ino != second_stat.st_ino
            or first_stat.st_nlink != second_stat.st_nlink
            or first_stat.st_nlink < 2
        ):
            return None
        return first_stat, second_stat

    def _unlink_matching_conflict_target_locked(
        self,
        owned: Path,
        target: Path,
    ) -> bool:
        if self._matching_regular_entries(owned, target) is None:
            return False
        self._before_conflict_temp_unlink(owned, target)
        self._after_corrupt_conflict_stage("before_target_unlink", owned, target)
        matched = self._matching_regular_entries(owned, target)
        if matched is None:
            return False
        _owned_stat, target_stat = matched
        self._unlink_regular_identity(
            target,
            target_stat,
            allowed_link_counts=frozenset({target_stat.st_nlink}),
        )
        self._after_corrupt_conflict_stage("target_unlinked", owned, target)
        return True

    def _load_corrupt_raw_locked(
        self,
        raw: Path,
    ) -> tuple[str, bytes, os.stat_result]:
        match = self._CORRUPT_TEMP_RAW_NAME.fullmatch(raw.name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid corrupt conflict raw name: {raw.name}")
        try:
            observed = raw.lstat()
        except FileNotFoundError as exc:
            raise InvalidCommandEnvelopeError(
                f"corrupt conflict raw disappeared: {raw.name}"
            ) from exc
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_dev != int(match["device"], 16)
            or observed.st_ino != int(match["inode"], 16)
        ):
            raise InvalidCommandEnvelopeError(f"corrupt conflict raw identity mismatch: {raw.name}")
        _candidate, payload, file_stat = self._read_regular_child(
            raw,
            self.quarantine_dir,
            allowed_link_counts=frozenset({observed.st_nlink}),
        )
        return match["target"], payload, file_stat

    def _record_corrupt_raw_locked(
        self,
        raw: Path,
        *,
        target_name: str,
        payload: bytes,
        file_stat: os.stat_result,
    ) -> None:
        temporary_name = f".{target_name}.publishing.tmp"
        evidence = LabCorruptConflictTempEvidence(
            temporary_name=temporary_name,
            target_name=target_name,
            raw_name=raw.name,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
            byte_count=len(payload),
            content_hash=hashlib.sha256(payload).hexdigest(),
        )
        metadata = self.quarantine_dir / self._corrupt_temp_evidence_name(evidence)
        evidence_payload = evidence.model_dump_json().encode("utf-8")
        if not self._publish_no_clobber(metadata, evidence_payload):
            existing, _existing_stat = self._load_corrupt_temp_evidence_file(metadata)
            if existing != evidence:
                raise InvalidCommandEnvelopeError(
                    f"corrupt conflict temp evidence conflicts: {metadata.name}"
                )
        self._after_corrupt_conflict_stage(
            "evidence_written",
            raw,
            self.quarantine_dir / target_name,
        )

    def _recover_corrupt_raw_locked(self) -> None:
        for raw in sorted(self.quarantine_dir.glob("*.corrupt-conflict-temp.raw.bad")):
            try:
                target_name, payload, file_stat = self._load_corrupt_raw_locked(raw)
            except InvalidCommandEnvelopeError:
                continue
            self._record_corrupt_raw_locked(
                raw,
                target_name=target_name,
                payload=payload,
                file_stat=file_stat,
            )
            temporary = self.quarantine_dir / f".{target_name}.publishing.tmp"
            relation = self._matching_regular_entries(raw, temporary)
            if relation is not None:
                _raw_stat, temporary_stat = relation
                self._unlink_regular_identity(
                    temporary,
                    temporary_stat,
                    allowed_link_counts=frozenset({temporary_stat.st_nlink}),
                )
            target = self.quarantine_dir / target_name
            self._unlink_matching_conflict_target_locked(raw, target)

    def _recover_conflict_evidence_locked(self) -> None:
        self._recover_corrupt_raw_locked()
        for temporary in sorted(self.quarantine_dir.glob(".*.publishing.tmp")):
            match = self._CONFLICT_TEMP_NAME.fullmatch(temporary.name)
            if match is None:
                continue
            try:
                observed = temporary.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(observed.st_mode):
                continue
            target_by_name = self.quarantine_dir / match["target"]
            target_relation = self._matching_regular_entries(temporary, target_by_name)
            if observed.st_nlink != 1 and target_relation is None:
                continue
            try:
                _candidate, payload, temporary_stat = self._read_regular_child(
                    temporary,
                    self.quarantine_dir,
                    allowed_link_counts=frozenset({observed.st_nlink}),
                )
            except InvalidCommandEnvelopeError:
                continue
            try:
                evidence = LabArtifactConflictEvidence.model_validate_json(payload)
            except ValueError:
                raw = self._move_conflict_temp_to_raw_locked(
                    temporary,
                    target_name=match["target"],
                    file_stat=temporary_stat,
                )
                self._unlink_matching_conflict_target_locked(raw, target_by_name)
                continue
            target = self._conflict_evidence_path(evidence)
            if temporary.name != f".{target.name}.publishing.tmp":
                raw = self._move_conflict_temp_to_raw_locked(
                    temporary,
                    target_name=match["target"],
                    file_stat=temporary_stat,
                )
                self._unlink_matching_conflict_target_locked(raw, target_by_name)
                continue
            if not os.path.lexists(target):
                if temporary_stat.st_nlink != 1:
                    raw = self._move_conflict_temp_to_raw_locked(
                        temporary,
                        target_name=match["target"],
                        file_stat=temporary_stat,
                    )
                    self._unlink_matching_conflict_target_locked(raw, target)
                    continue
                directory_fd = os.open(
                    self.quarantine_dir,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    with suppress(FileExistsError):
                        os.link(
                            temporary.name,
                            target.name,
                            src_dir_fd=directory_fd,
                            dst_dir_fd=directory_fd,
                            follow_symlinks=False,
                        )
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            relation = self._matching_regular_entries(temporary, target)
            if relation is None:
                raw = self._move_conflict_temp_to_raw_locked(
                    temporary,
                    target_name=match["target"],
                    file_stat=temporary.lstat(),
                )
                self._unlink_matching_conflict_target_locked(raw, target)
                continue
            self._before_conflict_temp_unlink(temporary, target)
            relation = self._matching_regular_entries(temporary, target)
            if relation is None:
                current = temporary.lstat()
                raw = self._move_conflict_temp_to_raw_locked(
                    temporary,
                    target_name=match["target"],
                    file_stat=current,
                )
                self._unlink_matching_conflict_target_locked(raw, target)
                continue
            temporary_current, _target_current = relation
            self._unlink_regular_identity(
                temporary,
                temporary_current,
                allowed_link_counts=frozenset({temporary_current.st_nlink}),
            )

    def _new_conflict_records_locked(self) -> list[_ConflictEvidenceRecord]:
        records: list[_ConflictEvidenceRecord] = []
        for path in self.quarantine_dir.glob("*.conflict.evidence.json"):
            try:
                evidence, _payload, file_stat = self._load_conflict_evidence_file(path)
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            if not self._evidence_matches_name(evidence, path.name):
                continue
            records.append(
                _ConflictEvidenceRecord(
                    modified_at_ns=file_stat.st_mtime_ns,
                    name=path.name,
                    size=file_stat.st_size,
                    files=((path, file_stat),),
                )
            )
        for path in self.quarantine_dir.glob(".*.publishing.tmp"):
            if self._CONFLICT_TEMP_NAME.fullmatch(path.name) is None:
                continue
            try:
                evidence, _payload, file_stat = self._load_conflict_evidence_file(
                    path,
                    allowed_link_counts=frozenset({1, 2}),
                )
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            if path != self._conflict_temporary_path(evidence):
                continue
            records.append(
                _ConflictEvidenceRecord(
                    modified_at_ns=file_stat.st_mtime_ns,
                    name=path.name,
                    size=file_stat.st_size,
                    files=((path, file_stat),),
                )
            )
        return records

    def _legacy_conflict_records_locked(self) -> list[_ConflictEvidenceRecord]:
        records: list[_ConflictEvidenceRecord] = []
        seen_metadata: set[Path] = set()
        for payload_path in self.quarantine_dir.glob("*.conflict.bad"):
            match = self._LEGACY_CONFLICT_NAME.fullmatch(payload_path.name)
            if match is None:
                continue
            try:
                _payload, payload, payload_stat = self._read_regular_child(
                    payload_path,
                    self.quarantine_dir,
                )
                envelope = LabArtifactCommitEnvelope.model_validate_json(payload)
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            if (str(envelope.request_id), envelope.content_hash) != (
                match["request_id"],
                match["content_hash"],
            ):
                continue
            files: list[tuple[Path, os.stat_result]] = [(payload_path, payload_stat)]
            metadata_path = Path(f"{payload_path}.json")
            if os.path.lexists(metadata_path):
                try:
                    _metadata, metadata, metadata_stat = self._read_regular_child(
                        metadata_path,
                        self.quarantine_dir,
                    )
                    record = LabQuarantinedArtifactCommit.model_validate_json(metadata)
                    if (
                        record.path == payload_path
                        and hashlib.sha256(record.reason.encode("utf-8")).hexdigest()[:16]
                        == match["reason_hash"]
                    ):
                        files.insert(0, (metadata_path, metadata_stat))
                        seen_metadata.add(metadata_path)
                except (InvalidCommandEnvelopeError, ValueError):
                    pass
            records.append(
                _ConflictEvidenceRecord(
                    modified_at_ns=max(item[1].st_mtime_ns for item in files),
                    name=payload_path.name,
                    size=sum(item[1].st_size for item in files),
                    files=tuple(files),
                )
            )
        for metadata_path in self.quarantine_dir.glob("*.conflict.bad.json"):
            if metadata_path in seen_metadata:
                continue
            payload_path = Path(str(metadata_path)[: -len(".json")])
            match = self._LEGACY_CONFLICT_NAME.fullmatch(payload_path.name)
            if match is None:
                continue
            try:
                _metadata, metadata, metadata_stat = self._read_regular_child(
                    metadata_path,
                    self.quarantine_dir,
                )
                record = LabQuarantinedArtifactCommit.model_validate_json(metadata)
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            if (
                record.path != payload_path
                or hashlib.sha256(record.reason.encode("utf-8")).hexdigest()[:16]
                != match["reason_hash"]
            ):
                continue
            records.append(
                _ConflictEvidenceRecord(
                    modified_at_ns=metadata_stat.st_mtime_ns,
                    name=metadata_path.name,
                    size=metadata_stat.st_size,
                    files=((metadata_path, metadata_stat),),
                )
            )
        return records

    def _corrupt_temp_records_locked(self) -> list[_ConflictEvidenceRecord]:
        records: list[_ConflictEvidenceRecord] = []
        owned_raw: set[Path] = set()
        for path in self.quarantine_dir.glob("*.corrupt-conflict-temp.bad.json"):
            if self._CORRUPT_TEMP_NAME.fullmatch(path.name) is None:
                continue
            try:
                evidence, file_stat = self._load_corrupt_temp_evidence_file(path)
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            files: list[tuple[Path, os.stat_result]] = [(path, file_stat)]
            if evidence.raw_name is not None:
                raw = self.quarantine_dir / evidence.raw_name
                try:
                    target_name, payload, raw_stat = self._load_corrupt_raw_locked(raw)
                except InvalidCommandEnvelopeError:
                    pass
                else:
                    raw_mismatch = (
                        target_name != evidence.target_name
                        or raw_stat.st_dev != evidence.device
                        or raw_stat.st_ino != evidence.inode
                        or len(payload) != evidence.byte_count
                        or hashlib.sha256(payload).hexdigest() != evidence.content_hash
                    )
                    if not raw_mismatch:
                        files.append((raw, raw_stat))
                        owned_raw.add(raw)
            records.append(
                _ConflictEvidenceRecord(
                    modified_at_ns=max(item[1].st_mtime_ns for item in files),
                    name=path.name,
                    size=sum(item[1].st_size for item in files),
                    files=tuple(files),
                )
            )
        for raw in self.quarantine_dir.glob("*.corrupt-conflict-temp.raw.bad"):
            if raw in owned_raw:
                continue
            try:
                _target_name, _payload, raw_stat = self._load_corrupt_raw_locked(raw)
            except InvalidCommandEnvelopeError:
                continue
            records.append(
                _ConflictEvidenceRecord(
                    modified_at_ns=raw_stat.st_mtime_ns,
                    name=raw.name,
                    size=raw_stat.st_size,
                    files=((raw, raw_stat),),
                )
            )
        return records

    def _prune_conflicts_locked(self) -> None:
        self._recover_conflict_evidence_locked()
        records = (
            self._new_conflict_records_locked()
            + self._legacy_conflict_records_locked()
            + self._corrupt_temp_records_locked()
        )
        records.sort(key=lambda record: (record.modified_at_ns, record.name))
        total_bytes = sum(record.size for record in records)
        while len(records) > self.max_conflict_records or (
            total_bytes > self.max_conflict_bytes and len(records) > 1
        ):
            record = records.pop(0)
            total_bytes -= record.size
            for path, file_stat in record.files:
                self._unlink_regular_identity(
                    path,
                    file_stat,
                    allowed_link_counts=frozenset({file_stat.st_nlink}),
                )

    def conflict_evidence(self) -> tuple[LabArtifactConflictEvidence, ...]:
        with self._exclusive_lock():
            self._recover_conflict_evidence_locked()
            self._prune_conflicts_locked()
            evidence: list[LabArtifactConflictEvidence] = []
            for path in sorted(self.quarantine_dir.glob("*.conflict.evidence.json")):
                try:
                    item, _payload, _file_stat = self._load_conflict_evidence_file(path)
                except (InvalidCommandEnvelopeError, ValueError):
                    continue
                if self._evidence_matches_name(item, path.name):
                    evidence.append(item)
            return tuple(evidence)

    def publish(
        self,
        envelope: LabArtifactCommitEnvelope,
    ) -> LabArtifactCommitSpoolEntry | LabAcknowledgedArtifactCommit:
        validated = LabArtifactCommitEnvelope.model_validate(envelope)
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
                    self._quarantine_conflicting_publish_locked(
                        validated,
                        reason="request_id already acknowledged with different content",
                    )
                    raise RequestContentConflictError(
                        f"request_id {validated.request_id} already has different content"
                    )
                if receipt.job_id != validated.commit.job_id:
                    raise InvalidCommandEnvelopeError(
                        f"ack job_id does not match request_id {validated.request_id}"
                    )
                return LabAcknowledgedArtifactCommit(path=ack_path, receipt=receipt)
            if pending_path is not None:
                existing = self.load(pending_path)
                if existing.envelope != validated:
                    self._quarantine_conflicting_publish_locked(
                        validated,
                        reason="request_id already pending with different content",
                    )
                    raise RequestContentConflictError(
                        f"request_id {validated.request_id} already has different content"
                    )
                return existing
            sequence = self._next_sequence_locked()
            target = self.pending_dir / f"{sequence:020d}-{validated.request_id}.json"
            if not self._publish_no_clobber(target, payload):
                raise RequestContentConflictError(f"delivery sequence {sequence} already exists")
            return self.load(target)

    def load(self, path: Path) -> LabArtifactCommitSpoolEntry:
        candidate, payload, file_stat = self._read_regular_child(Path(path), self.pending_dir)
        identity = LabSpoolFileIdentity(
            path=candidate,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )
        try:
            _sequence, filename_request_id = self._pending_name_parts(candidate.name)
            envelope = LabArtifactCommitEnvelope.model_validate_json(payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid artifact commit envelope {candidate.name}: {exc}",
                file_identity=identity,
            ) from exc
        if envelope.request_id != filename_request_id:
            raise InvalidCommandEnvelopeError(
                f"artifact commit request_id does not match basename {candidate.name}",
                file_identity=identity,
            )
        return LabArtifactCommitSpoolEntry(
            path=candidate,
            envelope=envelope,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )

    def pending_paths(self, *, limit: int | None = None) -> tuple[Path, ...]:
        with self._exclusive_lock():
            paths = tuple(sorted(self.pending_dir.glob("*.json"), key=self._delivery_key))
            return paths if limit is None else paths[:limit]

    def pending(
        self,
        *,
        limit: int | None = None,
    ) -> tuple[LabArtifactCommitSpoolEntry, ...]:
        return tuple(self.load(path) for path in self.pending_paths(limit=limit))

    def ack(
        self,
        entry: LabArtifactCommitSpoolEntry,
        receipt: LabArtifactCommitReceipt,
    ) -> LabAcknowledgedArtifactCommit:
        if (
            receipt.request_id != entry.envelope.request_id
            or receipt.content_hash != entry.envelope.content_hash
            or receipt.job_id != entry.envelope.commit.job_id
        ):
            raise ValueError("receipt does not match artifact commit envelope")
        with self._exclusive_lock():
            current = self.load(entry.path)
            if (current.device, current.inode) != (entry.device, entry.inode):
                raise InvalidCommandEnvelopeError("pending artifact commit was replaced before ack")
            if current.envelope != entry.envelope:
                raise InvalidCommandEnvelopeError("pending artifact commit changed before ack")
            target = self.ack_dir / f"{receipt.request_id}.json"
            created = self._publish_no_clobber(
                target,
                receipt.model_dump_json().encode("utf-8"),
            )
            if not created and self.load_receipt(target) != receipt:
                raise RequestContentConflictError(
                    f"request_id {receipt.request_id} already has a different receipt"
                )
            self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
            return LabAcknowledgedArtifactCommit(path=target, receipt=receipt)

    def load_receipt(self, path: Path) -> LabArtifactCommitReceipt:
        candidate, payload, _file_stat = self._read_regular_child(Path(path), self.ack_dir)
        filename_request_id = self._ack_request_id(candidate.name)
        try:
            receipt = LabArtifactCommitReceipt.model_validate_json(payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid artifact commit receipt {candidate.name}: {exc}"
            ) from exc
        if receipt.request_id != filename_request_id:
            raise InvalidCommandEnvelopeError(
                f"artifact commit receipt request_id does not match basename {candidate.name}"
            )
        return receipt

    def quarantine(
        self,
        entry_or_path: LabArtifactCommitSpoolEntry | LabSpoolFileIdentity | Path,
        *,
        reason: str,
    ) -> LabQuarantinedArtifactCommit:
        if isinstance(entry_or_path, LabArtifactCommitSpoolEntry):
            source: LabSpoolFileIdentity | Path = LabSpoolFileIdentity(
                path=entry_or_path.path,
                device=entry_or_path.device,
                inode=entry_or_path.inode,
            )
        else:
            source = entry_or_path
        quarantined: LabQuarantinedCommand = super().quarantine(source, reason=reason)
        return LabQuarantinedArtifactCommit(
            path=quarantined.path,
            reason=quarantined.reason,
        )
