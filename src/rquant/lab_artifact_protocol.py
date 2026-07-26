"""Typed durable commit channel for complete Strategy Lab result artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
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


class LabArtifactCommitSpool(LabCommandSpool):
    """Atomic commit inbox; conflict evidence defaults to 256 pairs or 64 MiB.

    Cleanup removes only validated conflict pairs, oldest first. A single newest
    pair is retained even when it alone exceeds the byte budget.
    """

    _CONFLICT_NAME = re.compile(
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

    def _quarantine_conflicting_publish_locked(
        self,
        envelope: LabArtifactCommitEnvelope,
        *,
        reason: str,
    ) -> None:
        reason_hash = hashlib.sha256(reason.encode("utf-8")).hexdigest()[:16]
        target = self.quarantine_dir / (
            f"{envelope.request_id}.{envelope.content_hash}.{reason_hash}.conflict.bad"
        )
        payload = envelope.model_dump_json().encode("utf-8")
        if not self._publish_no_clobber(target, payload):
            _candidate, existing_payload, _file_stat = self._read_regular_child(
                target,
                self.quarantine_dir,
            )
            if existing_payload != payload:
                raise InvalidCommandEnvelopeError(
                    "artifact conflict quarantine target has different content"
                )
        record = LabQuarantinedArtifactCommit(path=target, reason=reason)
        metadata = self.quarantine_dir / f"{target.name}.json"
        metadata_payload = record.model_dump_json().encode("utf-8")
        if not self._publish_no_clobber(
            metadata,
            metadata_payload,
        ):
            _candidate, existing_metadata, _file_stat = self._read_regular_child(
                metadata,
                self.quarantine_dir,
            )
            if existing_metadata != metadata_payload:
                raise InvalidCommandEnvelopeError(
                    "artifact conflict metadata has different content"
                )
        self._prune_conflicts_locked()

    @staticmethod
    def _unlink_regular_identity(path: Path, observed: os.stat_result) -> None:
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
                or current.st_nlink != 1
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

    def _prune_conflicts_locked(self) -> None:
        records: list[tuple[int, str, int, Path, os.stat_result, Path, os.stat_result]] = []
        for payload_path in self.quarantine_dir.glob("*.conflict.bad"):
            name_match = self._CONFLICT_NAME.fullmatch(payload_path.name)
            if name_match is None:
                continue
            metadata_path = self.quarantine_dir / f"{payload_path.name}.json"
            try:
                _payload, payload_bytes, payload_stat = self._read_regular_child(
                    payload_path,
                    self.quarantine_dir,
                )
                _metadata, metadata_bytes, metadata_stat = self._read_regular_child(
                    metadata_path,
                    self.quarantine_dir,
                )
                envelope = LabArtifactCommitEnvelope.model_validate_json(payload_bytes)
                evidence = LabQuarantinedArtifactCommit.model_validate_json(metadata_bytes)
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            if (
                str(envelope.request_id) != name_match["request_id"]
                or envelope.content_hash != name_match["content_hash"]
                or evidence.path != payload_path
                or hashlib.sha256(evidence.reason.encode("utf-8")).hexdigest()[:16]
                != name_match["reason_hash"]
            ):
                continue
            records.append(
                (
                    max(payload_stat.st_mtime_ns, metadata_stat.st_mtime_ns),
                    payload_path.name,
                    payload_stat.st_size + metadata_stat.st_size,
                    payload_path,
                    payload_stat,
                    metadata_path,
                    metadata_stat,
                )
            )
        records.sort(key=lambda record: (record[0], record[1]))
        total_bytes = sum(record[2] for record in records)
        while len(records) > self.max_conflict_records or (
            total_bytes > self.max_conflict_bytes and len(records) > 1
        ):
            record = records.pop(0)
            total_bytes -= record[2]
            self._unlink_regular_identity(record[5], record[6])
            self._unlink_regular_identity(record[3], record[4])

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
