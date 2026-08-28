"""Typed durable commit channel for complete Strategy Lab result artifacts."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import stat
from bisect import bisect_right
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
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
    _LabOwnedIsolationRecord,
)
from rquant.research_run_spec import DatasetSnapshotIdentity
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_CODE_SHA_PATTERN = r"^[0-9a-f]{40}$"
_KEY_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"


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
        return canonical_json_bytes(self.model_dump(mode="json"))


@dataclass(frozen=True)
class LabFinalizerAuthorityKey:
    """Ephemeral HMAC key material supplied by a trusted runtime provider."""

    key_id: str
    secret: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if re.fullmatch(_KEY_ID_PATTERN, self.key_id) is None:
            raise ValueError("authority key_id is invalid")
        if not isinstance(self.secret, bytes) or len(self.secret) < 32:
            raise ValueError("authority secret must contain at least 32 bytes")


LabFinalizerAuthoritySigningKeyProvider = Callable[[], LabFinalizerAuthorityKey]
LabFinalizerAuthorityVerificationKeyProvider = Callable[
    [str],
    LabFinalizerAuthorityKey | None,
]


class LabFinalizerAuthorityAuthenticationError(ValueError):
    """An artifact commit cannot be authenticated by the trusted key ring."""


class LabFinalizerAuthorityShardEvidence(LabArtifactCommitProtocolModel):
    shard_index: int = Field(strict=True, ge=0)
    shard_id: UUID
    payload_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    result_manifest_hash: str = Field(pattern=_HASH_PATTERN)
    accepted_report_content_hash: str = Field(pattern=_HASH_PATTERN)
    claim_token: UUID
    claim_generation: int = Field(strict=True, ge=1)
    scheduler_fencing_token: int = Field(strict=True, ge=1)


class LabFinalizerAuthorityClaims(LabArtifactCommitProtocolModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    commit_content_hash: str = Field(pattern=_HASH_PATTERN)
    job_id: UUID
    ready_event_id: int = Field(strict=True, ge=1)
    ready_job_version: int = Field(strict=True, ge=0)
    scheduler_fencing_token: int = Field(strict=True, ge=1)
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    finalizer_code_sha: str = Field(pattern=_CODE_SHA_PATTERN)
    shards: tuple[LabFinalizerAuthorityShardEvidence, ...]
    artifact_manifest_hash: str = Field(pattern=_HASH_PATTERN)
    complete_result_hash: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_ordered_shards(self) -> LabFinalizerAuthorityClaims:
        if not self.shards:
            raise ValueError("authority proof requires accepted shard evidence")
        if tuple(item.shard_index for item in self.shards) != tuple(range(len(self.shards))):
            raise ValueError("authority shard evidence must be complete and ordered")
        if len({item.shard_id for item in self.shards}) != len(self.shards):
            raise ValueError("authority shard evidence must be unique")
        return self

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


class LabFinalizerAuthorityProof(LabArtifactCommitProtocolModel):
    schema_version: Literal[1] = 1
    key_id: str = Field(pattern=_KEY_ID_PATTERN)
    claims: LabFinalizerAuthorityClaims
    mac_sha256: str = Field(pattern=_HASH_PATTERN)


def sign_finalizer_authority(
    claims: LabFinalizerAuthorityClaims,
    *,
    key_provider: LabFinalizerAuthoritySigningKeyProvider,
) -> LabFinalizerAuthorityProof:
    key = key_provider()
    if not isinstance(key, LabFinalizerAuthorityKey):
        raise TypeError("authority key provider returned an invalid key")
    mac = hmac.new(key.secret, claims.canonical_json_bytes(), hashlib.sha256).hexdigest()
    return LabFinalizerAuthorityProof(key_id=key.key_id, claims=claims, mac_sha256=mac)


def verify_finalizer_authority(
    envelope: LabArtifactCommitEnvelope,
    *,
    key_provider: LabFinalizerAuthorityVerificationKeyProvider,
) -> LabFinalizerAuthorityClaims:
    proof = envelope.authority_proof
    if proof is None:
        raise LabFinalizerAuthorityAuthenticationError(
            "legacy unsigned artifact commit has no authority proof"
        )
    try:
        key = key_provider(proof.key_id)
    except Exception as exc:
        raise LabFinalizerAuthorityAuthenticationError(
            "authority verification key provider failed"
        ) from exc
    if key is None:
        raise LabFinalizerAuthorityAuthenticationError(
            "authority proof references an unknown key_id"
        )
    if not isinstance(key, LabFinalizerAuthorityKey) or key.key_id != proof.key_id:
        raise LabFinalizerAuthorityAuthenticationError(
            "authority verification key provider returned an invalid key"
        )
    expected = hmac.new(
        key.secret,
        proof.claims.canonical_json_bytes(),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(proof.mac_sha256, expected):
        raise LabFinalizerAuthorityAuthenticationError("authority proof MAC is invalid")
    return proof.claims


class LabAuthenticatedArtifactCommitIdentity(LabArtifactCommitProtocolModel):
    """Commit meaning after both the envelope and its authority claims are authenticated."""

    schema_version: Literal[1] = 1
    request_id: UUID
    commit: LabArtifactCommit
    claims: LabFinalizerAuthorityClaims


def authenticate_artifact_commit_identity(
    envelope: LabArtifactCommitEnvelope,
    *,
    key_provider: LabFinalizerAuthorityVerificationKeyProvider,
) -> LabAuthenticatedArtifactCommitIdentity:
    claims = verify_finalizer_authority(envelope, key_provider=key_provider)
    return LabAuthenticatedArtifactCommitIdentity(
        request_id=envelope.request_id,
        commit=envelope.commit,
        claims=claims,
    )


class LabArtifactCommitEnvelope(LabArtifactCommitProtocolModel):
    schema_version: Literal[1, 2] = 1
    request_id: UUID
    commit: LabArtifactCommit
    authority_proof: LabFinalizerAuthorityProof | None = None
    content_hash: str = ""

    @model_validator(mode="after")
    def validate_content_hash(self) -> LabArtifactCommitEnvelope:
        commit_content_hash = hashlib.sha256(self.commit.canonical_json_bytes()).hexdigest()
        if self.authority_proof is None:
            if self.schema_version != 1:
                raise ValueError("artifact commit v2 requires an authority proof")
            expected = commit_content_hash
            if self.content_hash and self.content_hash != expected:
                raise ValueError("content_hash does not match canonical artifact commit content")
            object.__setattr__(self, "content_hash", expected)
            return self
        if self.schema_version != 2:
            raise ValueError("signed artifact commit must use protocol schema v2")
        claims = self.authority_proof.claims
        if (
            claims.request_id != self.request_id
            or claims.commit_content_hash != commit_content_hash
            or claims.job_id != self.commit.job_id
            or claims.spec_hash != self.commit.spec_hash
            or claims.finalizer_code_sha != self.commit.code_sha
            or claims.artifact_manifest_hash != self.commit.manifest_hash
            or claims.complete_result_hash != self.commit.complete_result_hash
        ):
            raise ValueError("authority proof does not match artifact commit identity")
        expected = hashlib.sha256(
            canonical_json_bytes(self.model_dump(mode="json", exclude={"content_hash"}))
        ).hexdigest()
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
    # The bytes this entry was parsed from. (device, inode) is reusable, so quarantining by
    # that pair alone can isolate whatever file took the freed inode next.
    content_sha256: str = Field(pattern=_HASH_PATTERN)
    byte_count: int = Field(ge=0)


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


@dataclass(frozen=True)
class _ConflictEvidenceRecord:
    modified_at_ns: int
    name: str
    size: int
    files: tuple[tuple[Path, os.stat_result], ...]


@dataclass(frozen=True)
class _ArtifactQuarantineRecord:
    modified_at_ns: int
    identity: str
    size: int
    conflict: _ConflictEvidenceRecord | None = None
    isolation: _LabOwnedIsolationRecord | None = None


class LabArtifactCommitSpool(LabCommandSpool):
    """Atomic commit inbox; all owned quarantine evidence shares one bounded budget.

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
    _SCAN_CURSOR_TEMP_NAME = re.compile(r"\.\.artifact-commit-scan-cursor\.json\.[0-9a-f]{32}\.tmp")
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
        mutation_guard: Callable[[], object] | None = None,
    ) -> None:
        if max_conflict_records < 1:
            raise ValueError("max_conflict_records must be positive")
        if max_conflict_bytes < 1:
            raise ValueError("max_conflict_bytes must be positive")
        super().__init__(
            root,
            max_isolation_records=max_conflict_records,
            max_isolation_bytes=max_conflict_bytes,
            mutation_guard=mutation_guard,
        )
        self._scan_cursor_path = self.root / ".artifact-commit-scan-cursor.json"
        self.max_conflict_records = max_conflict_records
        self.max_conflict_bytes = max_conflict_bytes
        with self._exclusive_lock():
            self._recover_scan_cursor_temporaries_locked()
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
        if not self._managed_entry_exists(self._scan_cursor_path, self.root):
            return None
        try:
            observed = self._managed_entry_stat(self._scan_cursor_path, self.root)
        except FileNotFoundError:
            return None
        try:
            _candidate, payload, _file_stat = self._read_regular_child(
                self._scan_cursor_path,
                self.root,
            )
            cursor = strict_model_validate_canonical_json(LabArtifactCommitScanCursor, payload)
            return cursor
        except (InvalidCommandEnvelopeError, ValueError) as exc:
            with suppress(OSError, InvalidCommandEnvelopeError):
                self._isolate_scan_cursor_locked(observed, reason=str(exc))
                self._prune_quarantine_locked()
            return None

    def _recover_scan_cursor_temporaries_locked(self) -> None:
        for temporary in sorted(
            self._managed_paths(self.root, "..artifact-commit-scan-cursor.json.*.tmp")
        ):
            if self._SCAN_CURSOR_TEMP_NAME.fullmatch(temporary.name) is None:
                continue
            try:
                observed = self._managed_entry_stat(temporary, self.root)
            except FileNotFoundError:
                continue
            with suppress(OSError, InvalidCommandEnvelopeError):
                self._isolate_owned_entry_locked(
                    temporary,
                    observed,
                    reason="orphaned artifact scan cursor temporary",
                )

    def _isolate_scan_cursor_locked(
        self,
        observed: os.stat_result,
        *,
        reason: str,
    ) -> bool:
        try:
            self._isolate_owned_entry_locked(
                self._scan_cursor_path,
                observed,
                reason=reason,
            )
            return True
        except (FileNotFoundError, InvalidCommandEnvelopeError):
            return False

    @staticmethod
    def _after_scan_cursor_stage(
        _stage: Literal["temporary_written", "cursor_replaced"],
        _path: Path,
    ) -> None:
        """Fault-injection boundary for advisory scan cursor publication."""

    def _write_scan_cursor_locked(self, cursor: LabArtifactCommitScanCursor) -> None:
        temporary_name = f".{self._scan_cursor_path.name}.{uuid4().hex}.tmp"
        temporary = self.root / temporary_name
        root_descriptor = self._open_private_root()
        temporary_descriptor = -1
        try:
            try:
                temporary_descriptor = os.open(
                    temporary_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=root_descriptor,
                )
                payload = canonical_model_json_bytes(cursor)
                offset = 0
                while offset < len(payload):
                    offset += os.write(temporary_descriptor, payload[offset:])
                os.fsync(temporary_descriptor)
                os.fsync(root_descriptor)
                self._after_scan_cursor_stage("temporary_written", temporary)
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
                    raise InvalidCommandEnvelopeError(
                        "artifact scan cursor temporary identity changed"
                    )
                os.replace(
                    temporary_name,
                    self._scan_cursor_path.name,
                    src_dir_fd=root_descriptor,
                    dst_dir_fd=root_descriptor,
                )
                published = os.stat(
                    self._scan_cursor_path.name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
                if not self._same_stat(
                    temporary_identity,
                    published,
                    include_link_count=True,
                ):
                    raise InvalidCommandEnvelopeError(
                        "artifact scan cursor publish identity changed"
                    )
                os.fsync(root_descriptor)
                self._after_scan_cursor_stage("cursor_replaced", self._scan_cursor_path)
            except OSError:
                return
        finally:
            if temporary_descriptor >= 0:
                os.close(temporary_descriptor)
            with suppress(OSError):
                os.unlink(temporary_name, dir_fd=root_descriptor)
            os.close(root_descriptor)

    def fair_pending_paths(self, *, limit: int) -> tuple[Path, ...]:
        if limit < 1:
            raise ValueError("artifact fair scan limit must be positive")
        with self._exclusive_lock():
            paths = tuple(
                sorted(
                    self._managed_paths(self.pending_dir, "*.json"),
                    key=self._delivery_key,
                )
            )
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
        evidence = strict_model_validate_canonical_json(LabArtifactConflictEvidence, payload)
        return evidence, payload, file_stat

    def _load_conflict_target_locked(
        self,
        path: Path,
    ) -> tuple[LabArtifactConflictEvidence, bytes, os.stat_result]:
        evidence, payload, file_stat = self._load_conflict_evidence_file(path)
        if not self._evidence_matches_name(evidence, path.name):
            raise InvalidCommandEnvelopeError(
                f"artifact conflict evidence basename mismatch: {path.name}"
            )
        return evidence, payload, file_stat

    def _isolate_conflict_entry_locked(
        self,
        path: Path,
        observed: os.stat_result,
        *,
        reason: str,
    ) -> None:
        self._isolate_owned_entry_locked(path, observed, reason=reason)

    def _recover_one_conflict_temporary_locked(
        self,
        temporary: Path,
        target_name: str,
    ) -> None:
        try:
            observed = self._managed_entry_stat(temporary, self.quarantine_dir)
        except FileNotFoundError:
            return
        target = self.quarantine_dir / target_name
        relation = self._matching_regular_entries(temporary, target)
        if not stat.S_ISREG(observed.st_mode) or (observed.st_nlink != 1 and relation is None):
            self._isolate_conflict_entry_locked(
                temporary,
                observed,
                reason="abnormal artifact conflict publication temporary",
            )
            return
        try:
            evidence, payload, temporary_stat = self._load_conflict_evidence_file(
                temporary,
                allowed_link_counts=frozenset({observed.st_nlink}),
            )
        except (InvalidCommandEnvelopeError, ValueError):
            self._isolate_conflict_entry_locked(
                temporary,
                observed,
                reason="invalid typed artifact conflict publication temporary",
            )
            if relation is not None:
                try:
                    target_stat = self._managed_entry_stat(target, self.quarantine_dir)
                except FileNotFoundError:
                    return
                self._isolate_conflict_entry_locked(
                    target,
                    target_stat,
                    reason="corrupt artifact conflict target linked to invalid temporary",
                )
            return
        expected_temporary = self._conflict_temporary_path(evidence)
        if temporary != expected_temporary:
            self._isolate_conflict_entry_locked(
                temporary,
                temporary_stat,
                reason="artifact conflict temporary basename does not match typed evidence",
            )
            return
        if self._managed_entry_exists(target, target.parent) and relation is None:
            try:
                existing, existing_payload, _target_stat = self._load_conflict_target_locked(target)
            except (InvalidCommandEnvelopeError, ValueError):
                target_stat = self._managed_entry_stat(target, self.quarantine_dir)
                self._isolate_conflict_entry_locked(
                    target,
                    target_stat,
                    reason="invalid deterministic artifact conflict evidence target",
                )
            else:
                if existing == evidence and existing_payload == payload:
                    self._isolate_conflict_entry_locked(
                        temporary,
                        temporary_stat,
                        reason="duplicate artifact conflict publication temporary",
                    )
                    return
                target_stat = self._managed_entry_stat(target, self.quarantine_dir)
                self._isolate_conflict_entry_locked(
                    target,
                    target_stat,
                    reason="conflicting deterministic artifact conflict evidence target",
                )
        if not self._managed_entry_exists(target, target.parent):
            directory_fd = self._open_managed_directory(self.quarantine_dir)
            try:
                try:
                    self._guard_mutation()
                    os.link(
                        temporary.name,
                        target.name,
                        src_dir_fd=directory_fd,
                        dst_dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    return
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        relation = self._matching_regular_entries(temporary, target)
        if relation is None:
            current = self._managed_entry_stat(temporary, self.quarantine_dir)
            self._isolate_conflict_entry_locked(
                temporary,
                current,
                reason="artifact conflict temporary lost target identity relation",
            )
            return
        self._before_conflict_temp_unlink(temporary, target)
        relation = self._matching_regular_entries(temporary, target)
        if relation is None:
            current = self._managed_entry_stat(temporary, self.quarantine_dir)
            self._isolate_conflict_entry_locked(
                temporary,
                current,
                reason="artifact conflict temporary changed before completion",
            )
            return
        temporary_current, _target_current = relation
        self._unlink_regular_identity(
            temporary,
            temporary_current,
            allowed_link_counts=frozenset({temporary_current.st_nlink}),
        )

    def _isolate_invalid_conflict_targets_locked(self) -> None:
        for target in sorted(self._managed_paths(self.quarantine_dir, "*.conflict.evidence.json")):
            if self._CONFLICT_NAME.fullmatch(target.name) is None:
                continue
            try:
                self._load_conflict_target_locked(target)
            except (InvalidCommandEnvelopeError, ValueError, OSError):
                try:
                    observed = self._managed_entry_stat(target, self.quarantine_dir)
                except FileNotFoundError:
                    continue
                with suppress(InvalidCommandEnvelopeError, OSError):
                    self._isolate_conflict_entry_locked(
                        target,
                        observed,
                        reason="invalid deterministic artifact conflict evidence",
                    )

    @staticmethod
    def _before_conflict_temp_unlink(*_args: object) -> None:
        """Fault-injection boundary immediately before final link verification."""

    def _publish_conflict_evidence_locked(
        self,
        evidence: LabArtifactConflictEvidence,
    ) -> None:
        self._recover_conflict_evidence_locked()
        target = self._conflict_evidence_path(evidence)
        payload = canonical_model_json_bytes(evidence)
        if self._managed_entry_exists(target, target.parent):
            try:
                existing, existing_payload, _file_stat = self._load_conflict_target_locked(target)
            except (InvalidCommandEnvelopeError, ValueError):
                observed = self._managed_entry_stat(target, self.quarantine_dir)
                self._isolate_conflict_entry_locked(
                    target,
                    observed,
                    reason="invalid deterministic artifact conflict evidence before publish",
                )
            else:
                if existing == evidence and existing_payload == payload:
                    return
                observed = self._managed_entry_stat(target, self.quarantine_dir)
                self._isolate_conflict_entry_locked(
                    target,
                    observed,
                    reason="conflicting deterministic artifact conflict evidence before publish",
                )

        temporary = self._conflict_temporary_path(evidence)
        if self._managed_entry_exists(temporary, temporary.parent):
            self._recover_conflict_evidence_locked()
            if self._managed_entry_exists(target, target.parent):
                existing, existing_payload, _file_stat = self._load_conflict_target_locked(target)
                if existing == evidence and existing_payload == payload:
                    return
            if self._managed_entry_exists(temporary, temporary.parent):
                observed = self._managed_entry_stat(temporary, self.quarantine_dir)
                self._isolate_conflict_entry_locked(
                    temporary,
                    observed,
                    reason="unrecoverable artifact conflict publication temporary",
                )

        directory_fd = self._open_managed_directory(self.quarantine_dir)
        temporary_fd = -1
        try:
            temporary_fd = os.open(
                temporary.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            offset = 0
            while offset < len(payload):
                offset += os.write(temporary_fd, payload[offset:])
            os.fsync(temporary_fd)
            os.fsync(directory_fd)
        except BaseException:
            os.close(directory_fd)
            raise
        finally:
            if temporary_fd >= 0:
                os.close(temporary_fd)
        self._after_conflict_evidence_stage("temporary_written", temporary)

        try:
            try:
                self._guard_mutation()
                os.link(
                    temporary.name,
                    target.name,
                    src_dir_fd=directory_fd,
                    dst_dir_fd=directory_fd,
                    follow_symlinks=False,
                )
            except FileExistsError:
                self._recover_conflict_evidence_locked()
                existing, existing_payload, _file_stat = self._load_conflict_target_locked(target)
                if existing == evidence and existing_payload == payload:
                    return
                raise InvalidCommandEnvelopeError(
                    "artifact conflict target appeared with different content"
                ) from None
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        self._after_conflict_evidence_stage("target_linked", target)

        temporary_stat = self._managed_entry_stat(temporary, self.quarantine_dir)
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

    def _unlink_regular_identity(
        self,
        path: Path,
        observed: os.stat_result,
        *,
        allowed_link_counts: frozenset[int] = frozenset({1}),
    ) -> None:
        directory_fd = self._open_managed_directory(path.parent)
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
            self._guard_mutation()
            os.unlink(path.name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _matching_regular_entries(
        self,
        first: Path,
        second: Path,
    ) -> tuple[os.stat_result, os.stat_result] | None:
        try:
            first_stat = self._managed_entry_stat(first, first.parent)
            second_stat = self._managed_entry_stat(second, second.parent)
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

    def _recover_conflict_evidence_locked(self) -> None:
        for temporary in sorted(self._managed_paths(self.quarantine_dir, ".*.publishing.tmp")):
            match = self._CONFLICT_TEMP_NAME.fullmatch(temporary.name)
            if match is None:
                continue
            with suppress(InvalidCommandEnvelopeError, OSError, ValueError):
                self._recover_one_conflict_temporary_locked(
                    temporary,
                    match["target"],
                )
        self._isolate_invalid_conflict_targets_locked()

    def _new_conflict_records_locked(self) -> list[_ConflictEvidenceRecord]:
        records: list[_ConflictEvidenceRecord] = []
        for path in self._managed_paths(self.quarantine_dir, "*.conflict.evidence.json"):
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
        for path in self._managed_paths(self.quarantine_dir, ".*.publishing.tmp"):
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
        for payload_path in self._managed_paths(self.quarantine_dir, "*.conflict.bad"):
            match = self._LEGACY_CONFLICT_NAME.fullmatch(payload_path.name)
            if match is None:
                continue
            try:
                _payload, payload, payload_stat = self._read_regular_child(
                    payload_path,
                    self.quarantine_dir,
                )
                envelope = strict_model_validate_canonical_json(LabArtifactCommitEnvelope, payload)
            except (InvalidCommandEnvelopeError, ValueError):
                continue
            if (str(envelope.request_id), envelope.content_hash) != (
                match["request_id"],
                match["content_hash"],
            ):
                continue
            files: list[tuple[Path, os.stat_result]] = [(payload_path, payload_stat)]
            metadata_path = Path(f"{payload_path}.json")
            if self._managed_entry_exists(metadata_path, metadata_path.parent):
                try:
                    _metadata, metadata, metadata_stat = self._read_regular_child(
                        metadata_path,
                        self.quarantine_dir,
                    )
                    record = strict_model_validate_canonical_json(
                        LabQuarantinedArtifactCommit, metadata
                    )
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
        for metadata_path in self._managed_paths(
            self.quarantine_dir,
            "*.conflict.bad.json",
        ):
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
                record = strict_model_validate_canonical_json(
                    LabQuarantinedArtifactCommit, metadata
                )
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

    def _artifact_quarantine_records_locked(self) -> list[_ArtifactQuarantineRecord]:
        records = [
            _ArtifactQuarantineRecord(
                modified_at_ns=record.modified_at_ns,
                identity=f"conflict:{record.name}",
                size=record.size,
                conflict=record,
            )
            for record in self._new_conflict_records_locked()
            + self._legacy_conflict_records_locked()
        ]
        records.extend(
            _ArtifactQuarantineRecord(
                modified_at_ns=record.modified_at_ns,
                identity=f"isolation:{record.container.name}",
                size=record.byte_count,
                isolation=record,
            )
            for record in self._owned_isolation_records_locked()
        )
        return records

    def _remove_artifact_quarantine_record_locked(
        self,
        record: _ArtifactQuarantineRecord,
    ) -> bool:
        if record.isolation is not None:
            return self._remove_owned_isolation_record_locked(record.isolation)
        conflict = record.conflict
        if conflict is None:
            return False
        try:
            for path, file_stat in conflict.files:
                self._unlink_regular_identity(
                    path,
                    file_stat,
                    allowed_link_counts=frozenset({file_stat.st_nlink}),
                )
        except (InvalidCommandEnvelopeError, OSError):
            return False
        return True

    def _prune_artifact_quarantine_locked(self) -> None:
        self._recover_conflict_evidence_locked()
        self._reconcile_owned_isolations_locked()
        records = self._artifact_quarantine_records_locked()
        records.sort(key=lambda record: (record.modified_at_ns, record.identity))
        total_bytes = sum(record.size for record in records)
        while len(records) > self.max_conflict_records or (
            total_bytes > self.max_conflict_bytes and len(records) > 1
        ):
            removed = False
            for index, record in enumerate(records):
                if not self._remove_artifact_quarantine_record_locked(record):
                    continue
                total_bytes -= record.size
                records.pop(index)
                removed = True
                break
            if not removed:
                # Non-empty directories and ownership mismatches remain observable manual
                # dead letters. They still consume the shared budget, so newer removable
                # evidence is evicted when necessary rather than silently exceeding it.
                break

    def _prune_conflicts_locked(self) -> None:
        self._prune_artifact_quarantine_locked()

    def _prune_quarantine_locked(self) -> None:
        self._prune_artifact_quarantine_locked()

    def conflict_evidence(self) -> tuple[LabArtifactConflictEvidence, ...]:
        with self._exclusive_lock():
            self._recover_conflict_evidence_locked()
            self._prune_conflicts_locked()
            evidence: list[LabArtifactConflictEvidence] = []
            for path in sorted(
                self._managed_paths(self.quarantine_dir, "*.conflict.evidence.json")
            ):
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
        identity = self._spool_identity(candidate, file_stat, payload=payload)
        try:
            _sequence, filename_request_id = self._pending_name_parts(candidate.name)
            envelope = strict_model_validate_canonical_json(LabArtifactCommitEnvelope, payload)
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
            content_sha256=hashlib.sha256(payload).hexdigest(),
            byte_count=len(payload),
        )

    def pending_paths(self, *, limit: int | None = None) -> tuple[Path, ...]:
        with self._exclusive_lock():
            paths = tuple(
                sorted(
                    self._managed_paths(self.pending_dir, "*.json"),
                    key=self._delivery_key,
                )
            )
            return paths if limit is None else paths[:limit]

    def pending(
        self,
        *,
        limit: int | None = None,
    ) -> tuple[LabArtifactCommitSpoolEntry, ...]:
        return tuple(self.load(path) for path in self.pending_paths(limit=limit))

    def inspect(
        self,
        request_id: UUID,
    ) -> LabArtifactCommitSpoolEntry | LabAcknowledgedArtifactCommit | None:
        """Read one exact durable request state without publishing or acknowledging it."""

        with self._exclusive_lock():
            ack_path = self.ack_dir / f"{request_id}.json"
            pending_path = self._pending_for_request_locked(request_id)
            if self._managed_entry_exists(ack_path, self.ack_dir):
                receipt = self.load_receipt(ack_path)
                if pending_path is not None:
                    pending = self.load(pending_path)
                    if (
                        pending.envelope.content_hash != receipt.content_hash
                        or pending.envelope.commit.job_id != receipt.job_id
                    ):
                        raise InvalidCommandEnvelopeError(
                            f"request_id {request_id} has conflicting ack and pending"
                        )
                return LabAcknowledgedArtifactCommit(path=ack_path, receipt=receipt)
            if pending_path is not None:
                return self.load(pending_path)
            return None

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
                canonical_model_json_bytes(receipt),
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
            receipt = strict_model_validate_canonical_json(LabArtifactCommitReceipt, payload)
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
                byte_count=entry_or_path.byte_count,
                content_sha256=entry_or_path.content_sha256,
            )
        else:
            source = entry_or_path
        quarantined: LabQuarantinedCommand = super().quarantine(source, reason=reason)
        return LabQuarantinedArtifactCommit(
            path=quarantined.path,
            reason=quarantined.reason,
        )
