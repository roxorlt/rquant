"""Typed, durable command protocol for the Strategy Lab control plane."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_run_spec import ResearchRunSpec


class RequestContentConflictError(RuntimeError):
    """A request id was reused with different immutable content."""


class InvalidCommandEnvelopeError(ValueError):
    """A spool file is not a valid, self-consistent command envelope."""


class LabProtocolModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


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
    job_version: int | None = Field(default=None, ge=0)


class LabSpoolEntry(LabProtocolModel):
    path: Path
    envelope: LabCommandEnvelope


class LabAcknowledgedCommand(LabProtocolModel):
    path: Path
    receipt: LabCommandReceipt


class LabQuarantinedCommand(LabProtocolModel):
    path: Path
    reason: str = Field(min_length=1)


class LabCommandSpool:
    """Atomic filesystem inbox with durable receipts and quarantine."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.pending_dir = self.root / "pending"
        self.ack_dir = self.root / "ack"
        self.quarantine_dir = self.root / "quarantine"
        for path in (self.pending_dir, self.ack_dir, self.quarantine_dir):
            path.mkdir(parents=True, exist_ok=True)

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

    def publish(self, envelope: LabCommandEnvelope) -> LabSpoolEntry:
        validated = LabCommandEnvelope.model_validate(envelope)
        target = self.pending_dir / f"{validated.request_id}.json"
        payload = validated.model_dump_json().encode("utf-8")
        created = self._publish_no_clobber(target, payload)
        if not created:
            existing = self.load(target).envelope
            if existing.content_hash != validated.content_hash:
                raise RequestContentConflictError(
                    f"request_id {validated.request_id} already has different content"
                )
        return LabSpoolEntry(path=target, envelope=validated)

    def load(self, path: Path) -> LabSpoolEntry:
        candidate = Path(path)
        try:
            envelope = LabCommandEnvelope.model_validate_json(candidate.read_bytes())
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid command envelope {candidate.name}: {exc}"
            ) from exc
        return LabSpoolEntry(path=candidate, envelope=envelope)

    def pending_paths(self, *, limit: int | None = None) -> tuple[Path, ...]:
        paths = tuple(sorted(self.pending_dir.glob("*.json")))
        return paths if limit is None else paths[:limit]

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
        ):
            raise ValueError("receipt does not match command envelope")
        target = self.ack_dir / f"{receipt.request_id}.json"
        payload = receipt.model_dump_json().encode("utf-8")
        created = self._publish_no_clobber(target, payload)
        if not created and self.load_receipt(target) != receipt:
            raise RequestContentConflictError(
                f"request_id {receipt.request_id} already has a different receipt"
            )
        entry.path.unlink(missing_ok=True)
        self._fsync_directory(entry.path.parent)
        return LabAcknowledgedCommand(path=target, receipt=receipt)

    @staticmethod
    def load_receipt(path: Path) -> LabCommandReceipt:
        try:
            return LabCommandReceipt.model_validate_json(Path(path).read_bytes())
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid command receipt {Path(path).name}: {exc}"
            ) from exc

    def quarantine(self, path: Path, *, reason: str) -> LabQuarantinedCommand:
        source = Path(path)
        target = self.quarantine_dir / f"{source.name}.bad"
        while True:
            try:
                os.link(source, target)
                break
            except FileExistsError:
                target = self.quarantine_dir / f"{source.name}.{uuid4().hex}.bad"
        self._fsync_directory(self.quarantine_dir)
        source.unlink()
        self._fsync_directory(source.parent)
        return LabQuarantinedCommand(path=target, reason=reason)
