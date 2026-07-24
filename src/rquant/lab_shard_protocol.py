"""Typed durable shard claim and worker report protocol for Strategy Lab."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

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
_CURRENT_CLAIM_NAME = re.compile(
    r"(?P<job_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\."
    r"(?P<shard_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})\.json"
)
MAX_SHARD_HEARTBEAT_EXTENSION_SECONDS = 3_600


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


class LabClaimHighWater(LabShardProtocolModel):
    schema_version: Literal[1] = 1
    claim: LabShardClaim
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabClaimHighWater:
        expected = _canonical_hash(self.claim.model_dump(mode="json"))
        if self.content_hash and self.content_hash != expected:
            raise ValueError("content_hash does not match current claim")
        object.__setattr__(self, "content_hash", expected)
        return self


class LabClaimSupersededError(RuntimeError):
    """A claim is older than, or conflicts with, the durable shard high-water."""


class LabShardHeartbeat(LabShardProtocolModel):
    report_type: Literal["heartbeat"] = "heartbeat"
    lease_extension_seconds: int = Field(
        strict=True,
        ge=1,
        le=MAX_SHARD_HEARTBEAT_EXTENSION_SECONDS,
    )


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
    worker_id: str | None = Field(default=None, min_length=1)
    claim_token: UUID | None = None
    claim_generation: int | None = Field(default=None, strict=True, ge=1)
    scheduler_fencing_token: int | None = Field(default=None, strict=True, ge=1)
    report_type: Literal[
        "heartbeat",
        "shard_succeeded",
        "shard_failed",
        "worker_stopped",
    ] | None = None
    result_manifest_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
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
            worker_id=report.worker_id,
            claim_token=report.claim_token,
            claim_generation=report.claim_generation,
            scheduler_fencing_token=report.scheduler_fencing_token,
            report_type=report.body.report_type,
            result_manifest_hash=(
                report.body.result_manifest_hash
                if isinstance(report.body, LabShardSucceeded)
                else None
            ),
            status=status,
            reason=reason,
            accepted_at=accepted_at,
        )

    @model_validator(mode="after")
    def validate_time(self) -> LabReportReceipt:
        identity = (
            self.worker_id,
            self.claim_token,
            self.claim_generation,
            self.scheduler_fencing_token,
            self.report_type,
        )
        if any(value is not None for value in identity) and not all(
            value is not None for value in identity
        ):
            raise ValueError("receipt attempt identity must be complete when present")
        if self.report_type == "shard_succeeded":
            if self.result_manifest_hash is None:
                raise ValueError("success receipt requires result_manifest_hash")
        elif self.result_manifest_hash is not None:
            raise ValueError("only success receipt may contain result_manifest_hash")
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

    def __init__(
        self,
        root: Path,
        *,
        claim_advance_hook: Callable[[LabShardClaim], None] | None = None,
    ) -> None:
        super().__init__(root)
        self.current_dir = self.root / "current"
        self.current_dir.mkdir(parents=True, exist_ok=True)
        self._claim_advance_hook = claim_advance_hook

    def set_claim_advance_hook(
        self,
        hook: Callable[[LabShardClaim], None],
    ) -> None:
        self._claim_advance_hook = hook

    @staticmethod
    def _claim_order(claim: LabShardClaim) -> tuple[int, int, datetime, int]:
        return (
            claim.claim_generation,
            claim.scheduler_fencing_token,
            claim.claimed_at,
            claim.claim_token.int,
        )

    def _current_path(self, job_id: UUID, shard_id: UUID) -> Path:
        return self.current_dir / f"{job_id}.{shard_id}.json"

    def _load_current_locked(self, job_id: UUID, shard_id: UUID) -> LabClaimHighWater:
        path = self._current_path(job_id, shard_id)
        candidate, payload, _file_stat = self._read_regular_child(path, self.current_dir)
        match = _CURRENT_CLAIM_NAME.fullmatch(candidate.name)
        if match is None:
            raise InvalidCommandEnvelopeError(
                f"invalid current claim basename: {candidate.name}"
            )
        try:
            marker = LabClaimHighWater.model_validate_json(payload)
        except Exception as exc:
            raise InvalidCommandEnvelopeError(
                f"invalid current claim marker {candidate.name}: {exc}"
            ) from exc
        if (
            marker.claim.job_id != UUID(match.group("job_id"))
            or marker.claim.shard_id != UUID(match.group("shard_id"))
            or marker.claim.job_id != job_id
            or marker.claim.shard_id != shard_id
        ):
            raise InvalidCommandEnvelopeError(
                f"current claim marker identity does not match basename {candidate.name}"
            )
        return marker

    def current(self, job_id: UUID, shard_id: UUID) -> LabClaimHighWater:
        with self._exclusive_lock():
            return self._load_current_locked(job_id, shard_id)

    def _publish_current_locked(self, marker: LabClaimHighWater) -> None:
        target = self._current_path(marker.claim.job_id, marker.claim.shard_id)
        temporary = self.current_dir / f".{target.name}.{uuid4().hex}.tmp"
        try:
            with temporary.open("xb") as stream:
                stream.write(marker.model_dump_json().encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            self._fsync_directory(self.current_dir)
        finally:
            temporary.unlink(missing_ok=True)

    def is_current(self, claim: LabShardClaim) -> bool:
        validated = LabShardClaim.model_validate(claim)
        with self._exclusive_lock():
            if not os.path.lexists(
                self._current_path(validated.job_id, validated.shard_id)
            ):
                return False
            marker = self._load_current_locked(validated.job_id, validated.shard_id)
            return marker.claim == validated

    def publish(self, claim: LabShardClaim) -> LabClaimSpoolEntry:
        validated = LabShardClaim.model_validate(claim)
        payload = validated.model_dump_json().encode("utf-8")
        with self._exclusive_lock():
            if os.path.lexists(self._current_path(validated.job_id, validated.shard_id)):
                current = self._load_current_locked(validated.job_id, validated.shard_id)
            else:
                current = None
            if (
                current is not None
                and current.claim != validated
                and (
                    validated.claim_generation <= current.claim.claim_generation
                    or validated.scheduler_fencing_token
                    < current.claim.scheduler_fencing_token
                    or self._claim_order(validated) <= self._claim_order(current.claim)
                )
            ):
                raise LabClaimSupersededError(
                    "claim does not advance the durable shard high-water"
                )
            pending = self._pending_for_message_locked(validated.claim_token)
            if pending is not None:
                existing = self.load(pending)
                if existing.claim != validated:
                    raise RequestContentConflictError(
                        f"claim_token {validated.claim_token} already has different content"
                    )
            if current is None or current.claim != validated:
                self._publish_current_locked(LabClaimHighWater(claim=validated))
            if pending is not None:
                entry = existing
            else:
                sequence = self._next_sequence_locked()
                target = self.pending_dir / f"{sequence:020d}-{validated.claim_token}.json"
                if not self._publish_no_clobber(target, payload):
                    raise RequestContentConflictError(
                        f"delivery sequence {sequence} already exists"
                    )
                entry = self.load(target)
        if self._claim_advance_hook is not None:
            self._claim_advance_hook(validated)
        return entry

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
            marker = self._load_current_locked(entry.claim.job_id, entry.claim.shard_id)
            if marker.claim != entry.claim:
                raise LabClaimSupersededError(
                    "pending claim is not the durable shard high-water"
                )
            self._unlink_pending(entry.path, device=entry.device, inode=entry.inode)
        return entry.claim


class LabReportSpool(_TypedSpoolBase):
    """Worker-to-scheduler durable report channel with exactly-once receipts."""

    @contextmanager
    def evidence_lock(self) -> Iterator[None]:
        """Serialize report evidence mutation with artifact isolation."""
        with self._exclusive_lock():
            yield

    def pending_locked(self) -> tuple[LabReportSpoolEntry, ...]:
        paths = tuple(sorted(self.pending_dir.glob("*.json"), key=self._delivery_key))
        return tuple(self.load(path) for path in paths)

    def receipt_paths_locked(self) -> tuple[Path, ...]:
        return tuple(sorted(self.ack_dir.glob("*.json")))

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
        if receipt.claim_token is not None and (
            receipt.worker_id,
            receipt.claim_token,
            receipt.claim_generation,
            receipt.scheduler_fencing_token,
            receipt.report_type,
            receipt.result_manifest_hash,
        ) != (
            entry.report.worker_id,
            entry.report.claim_token,
            entry.report.claim_generation,
            entry.report.scheduler_fencing_token,
            entry.report.body.report_type,
            (
                entry.report.body.result_manifest_hash
                if isinstance(entry.report.body, LabShardSucceeded)
                else None
            ),
        ):
            raise ValueError("receipt attempt identity does not match worker report")
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
