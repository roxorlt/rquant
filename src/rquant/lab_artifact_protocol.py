"""Typed durable commit channel for complete Strategy Lab result artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

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

    def _recover_conflict_evidence_locked(self) -> None:
        for temporary in sorted(self.quarantine_dir.glob(".*.publishing.tmp")):
            match = self._CONFLICT_TEMP_NAME.fullmatch(temporary.name)
            if match is None:
                continue
            try:
                evidence, payload, temporary_stat = self._load_conflict_evidence_file(
                    temporary,
                    allowed_link_counts=frozenset({1, 2}),
                )
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            target = self._conflict_evidence_path(evidence)
            if temporary.name != f".{target.name}.publishing.tmp":
                continue
            if not os.path.lexists(target):
                if temporary_stat.st_nlink != 1:
                    continue
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
            try:
                target_evidence, target_payload, _target_stat = self._load_conflict_evidence_file(
                    target,
                    allowed_link_counts=frozenset({1, 2}),
                )
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            if target_evidence != evidence or target_payload != payload:
                continue
            self._unlink_regular_identity(
                temporary,
                temporary_stat,
                allowed_link_counts=frozenset({1, 2}),
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

    def _prune_conflicts_locked(self) -> None:
        self._recover_conflict_evidence_locked()
        records = self._new_conflict_records_locked() + self._legacy_conflict_records_locked()
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
                    allowed_link_counts=frozenset({1, 2}),
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
