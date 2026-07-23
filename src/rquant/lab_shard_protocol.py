"""Typed durable shard claim and worker report protocol for Strategy Lab."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.lab_job_protocol import (
    InvalidCommandEnvelopeError,
    LabCommandSpool,
    LabQuarantinedCommand,
    LabSpoolFileIdentity,
    RequestContentConflictError,
)

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_SPOOL_NAME = re.compile(
    r"(?:(?P<sequence>[0-9]{20})-)?"
    r"(?P<message_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\.json"
)
_ACK_NAME = re.compile(
    r"(?P<message_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\.json"
)


class LabShardProtocolModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _reject_float(_: str) -> float:
    raise ValueError("floating-point JSON values are not allowed")


def _reject_constant(_: str) -> object:
    raise ValueError("payload must contain finite JSON values")


def _canonical_json_object(raw: str, *, field: str) -> str:
    try:
        value = json.loads(
            raw,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid {field}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{field} must encode a JSON object")
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_hash(payload: dict[str, object]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return _sha256_text(canonical)


class LabShardDefinition(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    shard_id: UUID = UUID(int=0)
    shard_index: int = Field(strict=True, ge=0)
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    payload_json: str = Field(min_length=2)
    payload_hash: str = ""

    @classmethod
    def from_payload(
        cls,
        *,
        shard_index: int,
        adapter_id: str,
        adapter_version: str,
        plan_hash: str,
        payload_json: str,
    ) -> LabShardDefinition:
        return cls(
            shard_index=shard_index,
            adapter_id=adapter_id,
            adapter_version=adapter_version,
            plan_hash=plan_hash,
            payload_json=payload_json,
        )

    @model_validator(mode="after")
    def validate_identity(self) -> LabShardDefinition:
        canonical_payload = _canonical_json_object(self.payload_json, field="payload_json")
        payload_hash = _sha256_text(canonical_payload)
        if self.payload_hash and self.payload_hash != payload_hash:
            raise ValueError("payload_hash does not match canonical payload_json")
        shard_name = json.dumps(
            {
                "adapter_id": self.adapter_id,
                "adapter_version": self.adapter_version,
                "payload_hash": payload_hash,
                "plan_hash": self.plan_hash,
                "shard_index": self.shard_index,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        shard_id = uuid5(NAMESPACE_URL, f"rquant:lab-shard:{shard_name}")
        if self.shard_id.int and self.shard_id != shard_id:
            raise ValueError("shard_id does not match deterministic shard definition")
        object.__setattr__(self, "payload_json", canonical_payload)
        object.__setattr__(self, "payload_hash", payload_hash)
        object.__setattr__(self, "shard_id", shard_id)
        return self


class LabShardClaim(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    job_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    definition: LabShardDefinition
    worker_id: str = Field(min_length=1)
    claim_token: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    claimed_at: datetime
    lease_expires_at: datetime

    @model_validator(mode="after")
    def validate_lease(self) -> LabShardClaim:
        claimed_at = _utc(self.claimed_at, field="claimed_at")
        expires_at = _utc(self.lease_expires_at, field="lease_expires_at")
        if expires_at <= claimed_at:
            raise ValueError("lease_expires_at must be after claimed_at")
        object.__setattr__(self, "claimed_at", claimed_at)
        object.__setattr__(self, "lease_expires_at", expires_at)
        return self

    @property
    def shard_id(self) -> UUID:
        return self.definition.shard_id

    @property
    def shard_index(self) -> int:
        return self.definition.shard_index

    @property
    def payload_hash(self) -> str:
        return self.definition.payload_hash

    @property
    def plan_hash(self) -> str:
        return self.definition.plan_hash


class LabShardHeartbeat(LabShardProtocolModel):
    report_type: Literal["heartbeat"] = "heartbeat"
    lease_extension_seconds: int = Field(strict=True, ge=1)


class LabShardSucceeded(LabShardProtocolModel):
    report_type: Literal["shard_succeeded"] = "shard_succeeded"
    result_manifest_hash: str = Field(pattern=_HASH_PATTERN)


class LabShardFailed(LabShardProtocolModel):
    report_type: Literal["shard_failed"] = "shard_failed"
    failure_json: str = Field(min_length=2)
    failure_hash: str = ""

    @model_validator(mode="after")
    def validate_failure(self) -> LabShardFailed:
        canonical = _canonical_json_object(self.failure_json, field="failure_json")
        failure_hash = _sha256_text(canonical)
        if self.failure_hash and self.failure_hash != failure_hash:
            raise ValueError("failure_hash does not match canonical failure_json")
        object.__setattr__(self, "failure_json", canonical)
        object.__setattr__(self, "failure_hash", failure_hash)
        return self


class LabWorkerStopped(LabShardProtocolModel):
    report_type: Literal["worker_stopped"] = "worker_stopped"
    reason: str = Field(min_length=1)


LabWorkerReportBody = Annotated[
    LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    Field(discriminator="report_type"),
]


class LabWorkerReport(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    report_id: UUID
    job_id: UUID
    shard_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    payload_hash: str = Field(pattern=_HASH_PATTERN)
    worker_id: str = Field(min_length=1)
    claim_token: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    reported_at: datetime
    body: LabWorkerReportBody
    content_hash: str = ""

    @classmethod
    def from_claim(
        cls,
        claim: LabShardClaim,
        *,
        report_id: UUID,
        reported_at: datetime,
        body: LabWorkerReportBody,
    ) -> LabWorkerReport:
        return cls(
            report_id=report_id,
            job_id=claim.job_id,
            shard_id=claim.shard_id,
            spec_hash=claim.spec_hash,
            payload_hash=claim.payload_hash,
            worker_id=claim.worker_id,
            claim_token=claim.claim_token,
            claim_generation=claim.claim_generation,
            scheduler_fencing_token=claim.scheduler_fencing_token,
            reported_at=reported_at,
            body=body,
        )

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabWorkerReport:
        reported_at = _utc(self.reported_at, field="reported_at")
        body = self.body.model_dump(mode="json")
        expected = _canonical_hash(
            {
                "body": body,
                "claim_generation": self.claim_generation,
                "claim_token": str(self.claim_token),
                "job_id": str(self.job_id),
                "payload_hash": self.payload_hash,
                "report_id": str(self.report_id),
                "reported_at": reported_at.isoformat(timespec="microseconds").replace(
                    "+00:00", "Z"
                ),
                "scheduler_fencing_token": self.scheduler_fencing_token,
                "schema_version": self.schema_version,
                "shard_id": str(self.shard_id),
                "spec_hash": self.spec_hash,
                "worker_id": self.worker_id,
            }
        )
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match canonical worker report")
        object.__setattr__(self, "reported_at", reported_at)
        object.__setattr__(self, "content_hash", expected)
        return self


class LabReportReceipt(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    report_id: UUID
    content_hash: str = Field(pattern=_HASH_PATTERN)
    job_id: UUID
    shard_id: UUID
    status: Literal["accepted", "rejected"]
    reason: str = Field(min_length=1)
    accepted_at: datetime

    @classmethod
    def from_report(
        cls,
        report: LabWorkerReport,
        *,
        status: Literal["accepted", "rejected"],
        reason: str,
        accepted_at: datetime,
    ) -> LabReportReceipt:
        return cls(
            report_id=report.report_id,
            content_hash=report.content_hash,
            job_id=report.job_id,
            shard_id=report.shard_id,
            status=status,
            reason=reason,
            accepted_at=accepted_at,
        )

    @model_validator(mode="after")
    def validate_time(self) -> LabReportReceipt:
        object.__setattr__(self, "accepted_at", _utc(self.accepted_at, field="accepted_at"))
        return self


class LabClaimSpoolEntry(LabShardProtocolModel):
    path: Path
    claim: LabShardClaim
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


class LabReportSpoolEntry(LabShardProtocolModel):
    path: Path
    report: LabWorkerReport
    device: int = Field(ge=0)
    inode: int = Field(ge=1)


class LabAcknowledgedReport(LabShardProtocolModel):
    path: Path
    receipt: LabReportReceipt


class _TypedSpoolBase(LabCommandSpool):
    @staticmethod
    def _message_name_parts(name: str) -> tuple[int | None, UUID]:
        match = _SPOOL_NAME.fullmatch(name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid pending message basename: {name}")
        sequence = match.group("sequence")
        return (int(sequence) if sequence is not None else None, UUID(match.group("message_id")))

    @staticmethod
    def _ack_message_id(name: str) -> UUID:
        match = _ACK_NAME.fullmatch(name)
        if match is None:
            raise InvalidCommandEnvelopeError(f"invalid ack basename: {name}")
        return UUID(match.group("message_id"))

    def _pending_for_message_locked(self, message_id: UUID) -> Path | None:
        matches: list[Path] = []
        for candidate in self.pending_dir.glob("*.json"):
            try:
                _sequence, candidate_id = self._message_name_parts(candidate.name)
            except InvalidCommandEnvelopeError:
                continue
            if candidate_id == message_id:
                matches.append(candidate)
        if len(matches) > 1:
            raise InvalidCommandEnvelopeError(f"multiple pending messages for {message_id}")
        return matches[0] if matches else None

    @staticmethod
    def _delivery_key(path: Path) -> tuple[int, int, str]:
        try:
            sequence, _message_id = _TypedSpoolBase._message_name_parts(path.name)
        except InvalidCommandEnvelopeError:
            return (0, 0, path.name)
        return (1, sequence or 0, path.name)

    def pending_paths(self, *, limit: int | None = None) -> tuple[Path, ...]:
        with self._exclusive_lock():
            paths = tuple(sorted(self.pending_dir.glob("*.json"), key=self._delivery_key))
            return paths if limit is None else paths[:limit]

    def quarantine(
        self,
        entry_or_path: LabClaimSpoolEntry | LabReportSpoolEntry | LabSpoolFileIdentity | Path,
        *,
        reason: str,
    ) -> LabQuarantinedCommand:
        if isinstance(entry_or_path, LabClaimSpoolEntry | LabReportSpoolEntry):
            identity = LabSpoolFileIdentity(
                path=entry_or_path.path,
                device=entry_or_path.device,
                inode=entry_or_path.inode,
            )
            return super().quarantine(identity, reason=reason)
        return super().quarantine(entry_or_path, reason=reason)


class LabClaimSpool(_TypedSpoolBase):
    """Scheduler-to-worker durable claim channel."""

    def publish(self, claim: LabShardClaim) -> LabClaimSpoolEntry:
        validated = LabShardClaim.model_validate(claim)
        payload = validated.model_dump_json().encode("utf-8")
        with self._exclusive_lock():
            pending = self._pending_for_message_locked(validated.claim_token)
            if pending is not None:
                existing = self.load(pending)
                if existing.claim != validated:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} already has different content"
                    )
                return existing
            sequence = self._next_sequence_locked()
            target = self.pending_dir / f"{sequence:020d}-{validated.claim_token}.json"
            if not self._publish_no_clobber(target, payload):
                raise RequestContentConflictError(f"delivery sequence {sequence} already exists")
            return self.load(target)

    def load(self, path: Path) -> LabClaimSpoolEntry:
        candidate, payload, file_stat = self._read_regular_child(Path(path), self.pending_dir)
        identity = LabSpoolFileIdentity(
            path=candidate,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )
        try:
            _sequence, filename_token = self._message_name_parts(candidate.name)
            claim = LabShardClaim.model_validate_json(payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid shard claim {candidate.name}: {exc}",
                file_identity=identity,
            ) from exc
        if claim.claim_token != filename_token:
            raise InvalidCommandEnvelopeError(
                f"claim_token does not match basename {candidate.name}",
                file_identity=identity,
            )
        return LabClaimSpoolEntry(
            path=candidate,
            claim=claim,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )

    def pending(self, *, limit: int | None = None) -> tuple[LabClaimSpoolEntry, ...]:
        return tuple(self.load(path) for path in self.pending_paths(limit=limit))

    def consume(self, entry: LabClaimSpoolEntry) -> LabShardClaim:
        with self._exclusive_lock():
            current = self.load(entry.path)
            if (current.device, current.inode) != (entry.device, entry.inode):
                raise InvalidCommandEnvelopeError("pending claim was replaced before consume")
            if current.claim != entry.claim:
                raise InvalidCommandEnvelopeError("pending claim changed before consume")
            self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
        return entry.claim


class LabReportSpool(_TypedSpoolBase):
    """Worker-to-scheduler durable report channel with exactly-once receipts."""

    def publish(self, report: LabWorkerReport) -> LabReportSpoolEntry | LabAcknowledgedReport:
        validated = LabWorkerReport.model_validate(report)
        payload = validated.model_dump_json().encode("utf-8")
        with self._exclusive_lock():
            ack_path = self.ack_dir / f"{validated.report_id}.json"
            pending = self._pending_for_message_locked(validated.report_id)
            if os.path.lexists(ack_path):
                receipt = self.load_receipt(ack_path)
                if receipt.content_hash != validated.content_hash:
                    raise RequestContentConflictError(
                        f"report_id {validated.report_id} already has different content"
                    )
                return LabAcknowledgedReport(path=ack_path, receipt=receipt)
            if pending is not None:
                existing = self.load(pending)
                if existing.report.content_hash != validated.content_hash:
                    raise RequestContentConflictError(
                        f"report_id {validated.report_id} already has different content"
                    )
                return existing
            sequence = self._next_sequence_locked()
            target = self.pending_dir / f"{sequence:020d}-{validated.report_id}.json"
            if not self._publish_no_clobber(target, payload):
                raise RequestContentConflictError(f"delivery sequence {sequence} already exists")
            return self.load(target)

    def load(self, path: Path) -> LabReportSpoolEntry:
        candidate, payload, file_stat = self._read_regular_child(Path(path), self.pending_dir)
        identity = LabSpoolFileIdentity(
            path=candidate,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )
        try:
            _sequence, filename_id = self._message_name_parts(candidate.name)
            report = LabWorkerReport.model_validate_json(payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid worker report {candidate.name}: {exc}",
                file_identity=identity,
            ) from exc
        if report.report_id != filename_id:
            raise InvalidCommandEnvelopeError(
                f"report_id does not match basename {candidate.name}",
                file_identity=identity,
            )
        return LabReportSpoolEntry(
            path=candidate,
            report=report,
            device=file_stat.st_dev,
            inode=file_stat.st_ino,
        )

    def pending(self, *, limit: int | None = None) -> tuple[LabReportSpoolEntry, ...]:
        return tuple(self.load(path) for path in self.pending_paths(limit=limit))

    def ack(
        self,
        entry: LabReportSpoolEntry,
        receipt: LabReportReceipt,
    ) -> LabAcknowledgedReport:
        if (
            receipt.report_id != entry.report.report_id
            or receipt.content_hash != entry.report.content_hash
            or receipt.job_id != entry.report.job_id
            or receipt.shard_id != entry.report.shard_id
        ):
            raise ValueError("receipt does not match worker report")
        with self._exclusive_lock():
            current = self.load(entry.path)
            if (current.device, current.inode) != (entry.device, entry.inode):
                raise InvalidCommandEnvelopeError("pending report was replaced before ack")
            if current.report != entry.report:
                raise InvalidCommandEnvelopeError("pending report changed before ack")
            target = self.ack_dir / f"{receipt.report_id}.json"
            created = self._publish_no_clobber(
                target,
                receipt.model_dump_json().encode("utf-8"),
            )
            if not created and self.load_receipt(target) != receipt:
                raise RequestContentConflictError(
                    f"report_id {receipt.report_id} already has a different receipt"
                )
            self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
            return LabAcknowledgedReport(path=target, receipt=receipt)

    def load_receipt(self, path: Path) -> LabReportReceipt:
        candidate, payload, _file_stat = self._read_regular_child(Path(path), self.ack_dir)
        filename_id = self._ack_message_id(candidate.name)
        try:
            receipt = LabReportReceipt.model_validate_json(payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid report receipt {candidate.name}: {exc}"
            ) from exc
        if receipt.report_id != filename_id:
            raise InvalidCommandEnvelopeError(
                f"report receipt id does not match basename {candidate.name}"
            )
        return receipt
