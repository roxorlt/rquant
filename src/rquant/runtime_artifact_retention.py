"""Crash-recoverable production GC for content-addressed local artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import stat
import time
from collections.abc import Callable
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol, Self
from urllib.parse import unquote, urlparse

from pydantic import Field, model_validator

from rquant.artifact_retention import (
    ArtifactReferenceStore,
    ExpiredGcClaimRecoveryReceipt,
    GcCandidate,
    GcClaim,
    GcPlan,
    ObjectCopyVerification,
    PrivateSqlitePathAuthority,
    RetentionPolicy,
    StorageTier,
    TierMigrationCursor,
    verified_sqlite_connection_scope,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_recovery_artifacts import (
    FixedReplayVerifier,
    RealRecoveryReceipt,
    RealRecoveryTargetManifest,
    RecoveryCurrentPointer,
    RecoveryVerificationBudget,
    load_full_verified_current_recovery_receipt,
)


class GcLeaseBusyError(RuntimeError):
    pass


class GcLeaseGrant(RuntimeContractModel):
    owner_label: str = Field(min_length=1)
    lease_token: str = Field(pattern=r"^[0-9a-f]{64}$")
    fence: int = Field(ge=1)
    expires_at: AwareUtcDatetime


class PhysicalObjectIdentity(RuntimeContractModel):
    storage_uri: str = Field(min_length=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0)
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    ctime_ns: int = Field(ge=0)
    link_count: int = Field(ge=1)


class DeletionQuarantineToken(RuntimeContractModel):
    token_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    claim_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_uri: str = Field(min_length=1)
    quarantine_uri: str = Field(min_length=1)
    physical_identity: PhysicalObjectIdentity
    quarantined_at: AwareUtcDatetime


class PhysicalDeletionReceipt(RuntimeContractModel):
    receipt_id: str | None = None
    token_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_uri: str = Field(min_length=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0)
    deleted_at: AwareUtcDatetime
    recovered_after_unlink: bool = False

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("physical deletion receipt identity is invalid")
        return self


class ArtifactGcTransport(Protocol):
    def verify(self, storage_uri: str) -> ObjectCopyVerification: ...

    def observe(self, candidate: GcCandidate) -> PhysicalObjectIdentity: ...

    def quarantine(
        self,
        candidate: GcCandidate,
        claim: GcClaim,
        expected: PhysicalObjectIdentity,
    ) -> DeletionQuarantineToken: ...

    def delete_quarantined(
        self,
        token: DeletionQuarantineToken,
    ) -> PhysicalDeletionReceipt: ...


class ArtifactQuarantineInspector(Protocol):
    def is_quarantined(
        self,
        candidate: GcCandidate,
        claim: GcClaim,
        expected: PhysicalObjectIdentity,
    ) -> bool: ...


class FullVerifiedDeletionAuthorization(RuntimeContractModel):
    authorization_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    profile: Literal["current"]
    profile_generation: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    verification_level: Literal["full_verified"]
    verified_at: AwareUtcDatetime
    recovery_completed_at: AwareUtcDatetime
    current_published_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_authorization_identity(self) -> Self:
        if self.current_published_at > self.recovery_completed_at:
            raise ValueError("recovery completion cannot precede current publication")
        if self.verified_at != self.recovery_completed_at:
            raise ValueError("recovery verification must bind exact receipt completion")
        if self.expires_at <= self.recovery_completed_at:
            raise ValueError("recovery authorization expiry must follow completion")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"authorization_id"}))
        if self.authorization_id is not None and self.authorization_id != expected:
            raise ValueError("recovery authorization identity is invalid")
        object.__setattr__(self, "authorization_id", expected)
        return self


class ArtifactDeletionGate(Protocol):
    def authorize(
        self,
        candidate: GcCandidate,
        *,
        as_of: datetime,
    ) -> FullVerifiedDeletionAuthorization: ...


class FullVerifiedRecoveryReceiptLoader(Protocol):
    def __call__(
        self,
        *,
        restore_root: Path,
        receipt_id: str,
        target: RealRecoveryTargetManifest,
        fixed_replay_verifier: FixedReplayVerifier,
        verification_budget: RecoveryVerificationBudget | None = None,
    ) -> tuple[RecoveryCurrentPointer, RealRecoveryReceipt]: ...


class ExactFullVerifiedRecoveryDeletionGate:
    """Adapter over recovery's exact current-generation full verification API."""

    def __init__(
        self,
        *,
        restore_root: Path,
        receipt_id: str,
        target: RealRecoveryTargetManifest,
        fixed_replay_verifier: FixedReplayVerifier,
        max_recovery_age: timedelta,
        verification_budget: RecoveryVerificationBudget | None = None,
        loader: FullVerifiedRecoveryReceiptLoader = (load_full_verified_current_recovery_receipt),
    ) -> None:
        self.restore_root = Path(restore_root)
        self.receipt_id = receipt_id
        self.target = target
        self.fixed_replay_verifier = fixed_replay_verifier
        if max_recovery_age <= timedelta(0):
            raise ValueError("max_recovery_age must be positive")
        self.max_recovery_age = max_recovery_age
        self.verification_budget = verification_budget
        self._loader = loader

    def authorize(
        self,
        candidate: GcCandidate,
        *,
        as_of: datetime,
    ) -> FullVerifiedDeletionAuthorization:
        del candidate
        as_of = normalize_aware_utc(as_of)
        current, receipt = self._loader(
            restore_root=self.restore_root,
            receipt_id=self.receipt_id,
            target=self.target,
            fixed_replay_verifier=self.fixed_replay_verifier,
            verification_budget=self.verification_budget,
        )
        expires_at = receipt.completed_at + self.max_recovery_age
        if (
            receipt.status != "succeeded"
            or receipt.receipt_id is None
            or receipt.receipt_id != self.receipt_id
            or receipt.manifest_id != self.target.manifest_id
            or current.generation_id != self.target.manifest_id
            or receipt.published_generation_id != current.generation_id
            or self.target.target_profile_generation != current.target_profile_generation
            or receipt.target_profile_generation != current.target_profile_generation
            or current.published_at > receipt.completed_at
            or current.published_at > as_of
            or receipt.completed_at > as_of
        ):
            raise ValueError("recovery receipt is not the current full-verified generation")
        if as_of >= expires_at:
            raise ValueError("recovery full-verified receipt is expired")
        return FullVerifiedDeletionAuthorization(
            profile="current",
            profile_generation=current.target_profile_generation,
            generation_id=current.generation_id,
            receipt_id=receipt.receipt_id,
            verification_level="full_verified",
            verified_at=receipt.completed_at,
            recovery_completed_at=receipt.completed_at,
            current_published_at=current.published_at,
            expires_at=expires_at,
        )


class GcWorkerConfig(RuntimeContractModel):
    batch_items: int = Field(ge=1, le=10_000)
    batch_bytes: int = Field(ge=1)
    max_runtime: timedelta
    lease_ttl: timedelta
    max_attempts: int = Field(ge=1, le=100)
    retry_delay: timedelta

    @model_validator(mode="after")
    def validate_durations(self) -> Self:
        if self.max_runtime <= timedelta(0):
            raise ValueError("max_runtime must be positive")
        if self.lease_ttl <= self.max_runtime:
            raise ValueError("lease_ttl must exceed max_runtime")
        if self.retry_delay < timedelta(0):
            raise ValueError("retry_delay must be nonnegative")
        return self


class GcRunSummary(RuntimeContractModel):
    completed: int = Field(ge=0)
    failed: int = Field(ge=0)
    dead_lettered: int = Field(ge=0)
    deferred: int = Field(ge=0)
    bytes_deleted: int = Field(ge=0)
    fence: int = Field(ge=1)


class GcRuntimeAuditEvent(RuntimeContractModel):
    sequence: int = Field(ge=1)
    event_type: str = Field(min_length=1)
    work_id: str = Field(min_length=1)
    occurred_at: AwareUtcDatetime
    payload_json: str


class GcRuntimeHealthAggregate(RuntimeContractModel):
    backlog_count: int = Field(ge=0)
    oldest_created_at: AwareUtcDatetime | None
    retry_count: int = Field(ge=0)
    dead_letter_count: int = Field(ge=0)
    lease_fence: int = Field(ge=0)
    lease_active: bool
    scanned_items: int = Field(default=0, ge=0)
    scanned_bytes: int = Field(default=0, ge=0)
    truncated: bool = False


class ArtifactGcHealthCursor(RuntimeContractModel):
    updated_at: AwareUtcDatetime
    work_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class ArtifactGcHealthSummary(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    status: Literal["healthy", "degraded", "critical"]
    backlog_count: int = Field(ge=0)
    oldest_backlog_age_seconds: float | None = Field(default=None, ge=0)
    operation_reconciliation_pending_count: int = Field(ge=0)
    quarantine_orphan_count: int = Field(ge=0)
    retry_count: int = Field(ge=0)
    dead_letter_count: int = Field(ge=0)
    lease_fence: int = Field(ge=0)
    lease_active: bool
    scanned_items: int = Field(default=0, ge=0)
    scanned_bytes: int = Field(default=0, ge=0)
    truncated: bool = False
    next_cursor: ArtifactGcHealthCursor | None = None
    blocked_work_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class _BoundArtifactPath:
    def __init__(self, storage_uri: str, parts: tuple[str, ...], descriptors: list[int]) -> None:
        self.storage_uri = storage_uri
        self.parts = parts
        self.descriptors = descriptors

    @property
    def parent_fd(self) -> int:
        return self.descriptors[-1]

    @property
    def name(self) -> str:
        return self.parts[-1]

    def assert_ancestry_current(self) -> None:
        for index, name in enumerate(self.parts[:-1]):
            parent_fd = self.descriptors[index]
            child_fd = self.descriptors[index + 1]
            named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            opened = os.fstat(child_fd)
            if (
                stat.S_ISLNK(named.st_mode)
                or not stat.S_ISDIR(named.st_mode)
                or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise ValueError("artifact ancestor identity changed")


class LocalAtomicArtifactTransport:
    """Local transport whose entire data path remains below one bound root fd."""

    def __init__(
        self,
        *,
        managed_root: Path,
        clock: Callable[[], datetime] | None = None,
        schema_resolver: Callable[[int], str],
    ) -> None:
        root = Path(managed_root)
        if not root.is_absolute() or str(root) != os.path.normpath(str(root)):
            raise ValueError("artifact root must be an exact absolute path")
        descriptors = self._open_root_descriptor_chain(root)
        observed = os.fstat(descriptors[-1])
        if observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) & 0o077:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            raise ValueError("artifact root owner or mode is unsafe")
        self.root = root
        self._root_components = root.parts[1:]
        self._root_descriptors = descriptors
        self._root_fd = descriptors[-1]
        self._root_identity = (observed.st_dev, observed.st_ino)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._schema_resolver = schema_resolver

    @staticmethod
    def _open_root_descriptor_chain(root: Path) -> list[int]:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptors = [os.open(root.anchor, flags)]
        try:
            for component in root.parts[1:]:
                parent_fd = descriptors[-1]
                named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
                if stat.S_ISLNK(named.st_mode) or not stat.S_ISDIR(named.st_mode):
                    raise ValueError("artifact root ancestor is a symlink or unsafe")
                child_fd = os.open(component, flags, dir_fd=parent_fd)
                opened = os.fstat(child_fd)
                if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
                    os.close(child_fd)
                    raise ValueError("artifact root ancestor changed while binding")
                descriptors.append(child_fd)
            return descriptors
        except BaseException:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            raise

    def _assert_root_chain_current(self) -> None:
        if self._root_fd < 0:
            raise ValueError("artifact root descriptor is closed")
        if len(self._root_descriptors) != len(self._root_components) + 1:
            raise ValueError("artifact root descriptor chain is incomplete")
        for index, component in enumerate(self._root_components):
            parent_fd = self._root_descriptors[index]
            child_fd = self._root_descriptors[index + 1]
            named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            opened = os.fstat(child_fd)
            if (
                stat.S_ISLNK(named.st_mode)
                or not stat.S_ISDIR(named.st_mode)
                or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise ValueError("artifact root naming identity changed")

    def close(self) -> None:
        descriptors = getattr(self, "_root_descriptors", [])
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        self._root_descriptors = []
        self._root_fd = -1

    def __del__(self) -> None:
        self.close()

    def _relative_parts(self, storage_uri: str) -> tuple[str, ...]:
        parsed = urlparse(storage_uri)
        if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("local artifact transport accepts exact file URIs only")
        path = Path(unquote(parsed.path))
        if not path.is_absolute() or str(path) != os.path.normpath(str(path)):
            raise ValueError("artifact path must be exact and absolute")
        try:
            relative = path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("artifact path escapes managed root") from exc
        parts = relative.parts
        if not parts or any(part in {"", ".", ".."} or "/" in part for part in parts):
            raise ValueError("artifact path is not a safe root-relative file")
        return parts

    @staticmethod
    def _validate_private_directory(observed: os.stat_result) -> None:
        if not stat.S_ISDIR(observed.st_mode):
            raise ValueError("artifact ancestor is not a directory")
        if observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) & 0o077:
            raise ValueError("artifact ancestor owner or mode is unsafe")

    @contextmanager
    def _bind(self, storage_uri: str, *, create_parents: bool = False) -> object:
        self._assert_root_chain_current()
        root_observed = os.fstat(self._root_fd)
        if (root_observed.st_dev, root_observed.st_ino) != self._root_identity:
            raise ValueError("artifact root identity changed")
        parts = self._relative_parts(storage_uri)
        descriptors = [os.dup(self._root_fd)]
        try:
            for part in parts[:-1]:
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
                try:
                    child = os.open(part, flags, dir_fd=descriptors[-1])
                except FileNotFoundError:
                    if not create_parents:
                        raise
                    try:
                        os.mkdir(part, 0o700, dir_fd=descriptors[-1])
                        os.fsync(descriptors[-1])
                    except FileExistsError:
                        pass
                    child = os.open(part, flags, dir_fd=descriptors[-1])
                self._validate_private_directory(os.fstat(child))
                named = os.stat(part, dir_fd=descriptors[-1], follow_symlinks=False)
                if (named.st_dev, named.st_ino) != (
                    os.fstat(child).st_dev,
                    os.fstat(child).st_ino,
                ):
                    os.close(child)
                    raise ValueError("artifact ancestor changed while opening")
                descriptors.append(child)
            bound = _BoundArtifactPath(storage_uri, parts, descriptors)
            bound.assert_ancestry_current()
            try:
                yield bound
            finally:
                bound.assert_ancestry_current()
                self._assert_root_chain_current()
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    @staticmethod
    def _entry_exists(parent_fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def _digest_descriptor(descriptor: int) -> tuple[str, int]:
        digest = hashlib.sha256()
        size = 0
        os.lseek(descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
        return digest.hexdigest(), size

    def _open_and_inspect(
        self,
        bound: _BoundArtifactPath,
        *,
        name: str | None = None,
        storage_uri: str | None = None,
    ) -> tuple[int, PhysicalObjectIdentity]:
        entry_name = name or bound.name
        descriptor = os.open(
            entry_name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=bound.parent_fd,
        )
        try:
            observed = os.fstat(descriptor)
            named = os.stat(entry_name, dir_fd=bound.parent_fd, follow_symlinks=False)
            if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(named.st_mode):
                raise ValueError("artifact is not a regular file")
            if observed.st_nlink != 1 or named.st_nlink != 1:
                raise ValueError("artifact has an unsafe hard link")
            if (observed.st_dev, observed.st_ino) != (named.st_dev, named.st_ino):
                raise ValueError("artifact path changed while inspecting")
            content_sha256, size_bytes = self._digest_descriptor(descriptor)
            after = os.fstat(descriptor)
            if (
                observed.st_dev,
                observed.st_ino,
                observed.st_ctime_ns,
                observed.st_size,
            ) != (after.st_dev, after.st_ino, after.st_ctime_ns, after.st_size):
                raise ValueError("artifact changed while hashing")
            identity = PhysicalObjectIdentity(
                storage_uri=storage_uri or bound.storage_uri,
                content_sha256=content_sha256,
                size_bytes=size_bytes,
                device=observed.st_dev,
                inode=observed.st_ino,
                ctime_ns=observed.st_ctime_ns,
                link_count=observed.st_nlink,
            )
            bound.assert_ancestry_current()
            return descriptor, identity
        except BaseException:
            os.close(descriptor)
            raise

    def copy(self, source_uri: str, target_uri: str) -> None:
        with (
            self._bind(source_uri) as source_bound,
            self._bind(target_uri, create_parents=True) as target_bound,
        ):
            source_fd, source_identity = self._open_and_inspect(source_bound)
            temporary = f".{target_bound.name}.rquant-copy-{secrets.token_hex(16)}"
            target_fd = -1
            try:
                if self._entry_exists(target_bound.parent_fd, target_bound.name):
                    target_existing_fd, target_identity = self._open_and_inspect(target_bound)
                    os.close(target_existing_fd)
                    if (
                        source_identity.content_sha256,
                        source_identity.size_bytes,
                    ) != (
                        target_identity.content_sha256,
                        target_identity.size_bytes,
                    ):
                        raise ValueError(
                            "tier migration target exists with conflicting hash or size"
                        )
                    return
                target_fd = os.open(
                    temporary,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=target_bound.parent_fd,
                )
                os.lseek(source_fd, 0, os.SEEK_SET)
                while True:
                    chunk = os.read(source_fd, 1024 * 1024)
                    if not chunk:
                        break
                    view = memoryview(chunk)
                    while view:
                        view = view[os.write(target_fd, view) :]
                os.fchmod(target_fd, 0o600)
                os.fsync(target_fd)
                source_bound.assert_ancestry_current()
                target_bound.assert_ancestry_current()
                os.rename(
                    temporary,
                    target_bound.name,
                    src_dir_fd=target_bound.parent_fd,
                    dst_dir_fd=target_bound.parent_fd,
                )
                for directory_fd in reversed(target_bound.descriptors):
                    os.fsync(directory_fd)
                target_bound.assert_ancestry_current()
            finally:
                if target_fd >= 0:
                    os.close(target_fd)
                os.close(source_fd)
                with suppress(FileNotFoundError):
                    os.unlink(temporary, dir_fd=target_bound.parent_fd)

    def durably_sync(self, storage_uri: str) -> None:
        with self._bind(storage_uri) as bound:
            descriptor, _identity = self._open_and_inspect(bound)
            try:
                os.fsync(descriptor)
                for directory_fd in reversed(bound.descriptors):
                    os.fsync(directory_fd)
                bound.assert_ancestry_current()
            finally:
                os.close(descriptor)

    def verify(self, storage_uri: str) -> ObjectCopyVerification:
        with self._bind(storage_uri) as bound:
            descriptor, identity = self._open_and_inspect(bound)
            resolver_descriptor = os.dup(descriptor)
            try:
                os.lseek(resolver_descriptor, 0, os.SEEK_SET)
                schema_sha256 = self._schema_resolver(resolver_descriptor)
                current = os.fstat(descriptor)
                if (
                    current.st_dev,
                    current.st_ino,
                    current.st_ctime_ns,
                    current.st_nlink,
                ) != (
                    identity.device,
                    identity.inode,
                    identity.ctime_ns,
                    identity.link_count,
                ):
                    raise ValueError("artifact identity changed during schema verification")
                bound.assert_ancestry_current()
            finally:
                os.close(resolver_descriptor)
                os.close(descriptor)
        return ObjectCopyVerification(
            storage_uri=storage_uri,
            content_sha256=identity.content_sha256,
            size_bytes=identity.size_bytes,
            schema_sha256=schema_sha256,
            verified_at=normalize_aware_utc(self._clock()),
        )

    def observe(self, candidate: GcCandidate) -> PhysicalObjectIdentity:
        with self._bind(candidate.object_copy.storage_uri) as bound:
            descriptor, observed = self._open_and_inspect(bound)
            os.close(descriptor)
        self._assert_candidate_identity(candidate, observed)
        return observed

    def quarantine(
        self,
        candidate: GcCandidate,
        claim: GcClaim,
        expected: PhysicalObjectIdentity,
    ) -> DeletionQuarantineToken:
        assert claim.claim_id is not None
        source_uri = candidate.object_copy.storage_uri
        source_path = Path(unquote(urlparse(source_uri).path))
        quarantine_uri = source_path.with_name(f".rquant-gc-{claim.claim_id}").as_uri()
        with self._bind(source_uri) as bound:
            quarantine_name = f".rquant-gc-{claim.claim_id}"
            source_exists = self._entry_exists(bound.parent_fd, bound.name)
            quarantine_exists = self._entry_exists(bound.parent_fd, quarantine_name)
            if source_exists and quarantine_exists:
                raise ValueError("source and quarantine both exist")
            if source_exists:
                descriptor, before = self._open_and_inspect(bound)
                os.close(descriptor)
                self._assert_candidate_identity(candidate, before)
                if before != expected:
                    raise ValueError("artifact path identity changed after deletion claim")
                bound.assert_ancestry_current()
                os.rename(
                    bound.name,
                    quarantine_name,
                    src_dir_fd=bound.parent_fd,
                    dst_dir_fd=bound.parent_fd,
                )
                os.fsync(bound.parent_fd)
            elif not quarantine_exists:
                raise ValueError("artifact source and quarantine are both missing")
            descriptor, observed = self._open_and_inspect(
                bound,
                name=quarantine_name,
                storage_uri=quarantine_uri,
            )
            os.close(descriptor)
            self._assert_candidate_identity(candidate, observed)
            if (observed.device, observed.inode) != (expected.device, expected.inode):
                raise ValueError("quarantined artifact node identity changed")
        token_id = canonical_sha256(
            {
                "claim_id": claim.claim_id,
                "source_uri": source_uri,
                "quarantine_uri": quarantine_uri,
                "physical_identity": observed,
            }
        )
        return DeletionQuarantineToken(
            token_id=token_id,
            claim_id=claim.claim_id,
            source_uri=source_uri,
            quarantine_uri=quarantine_uri,
            physical_identity=observed,
            quarantined_at=normalize_aware_utc(self._clock()),
        )

    def is_quarantined(
        self,
        candidate: GcCandidate,
        claim: GcClaim,
        expected: PhysicalObjectIdentity,
    ) -> bool:
        assert claim.claim_id is not None
        with self._bind(candidate.object_copy.storage_uri) as bound:
            quarantine_name = f".rquant-gc-{claim.claim_id}"
            source_exists = self._entry_exists(bound.parent_fd, bound.name)
            quarantine_exists = self._entry_exists(bound.parent_fd, quarantine_name)
            if source_exists and quarantine_exists:
                raise ValueError("source and quarantine both exist")
            if source_exists or not quarantine_exists:
                return False
            descriptor, observed = self._open_and_inspect(
                bound,
                name=quarantine_name,
            )
            os.close(descriptor)
            self._assert_candidate_identity(candidate, observed)
            if (observed.device, observed.inode) != (expected.device, expected.inode):
                raise ValueError("quarantined artifact node identity changed")
            return True

    @staticmethod
    def _assert_candidate_identity(
        candidate: GcCandidate,
        observed: PhysicalObjectIdentity,
    ) -> None:
        if (
            observed.content_sha256 != candidate.object_identity.content_sha256
            or observed.size_bytes != candidate.object_identity.size_bytes
        ):
            raise ValueError("physical artifact identity does not match GC candidate")

    def delete_quarantined(
        self,
        token: DeletionQuarantineToken,
    ) -> PhysicalDeletionReceipt:
        source_parts = self._relative_parts(token.source_uri)
        quarantine_parts = self._relative_parts(token.quarantine_uri)
        if source_parts[:-1] != quarantine_parts[:-1]:
            raise ValueError("source and quarantine parents differ")
        recovered = False
        with self._bind(token.quarantine_uri) as bound:
            if self._entry_exists(bound.parent_fd, source_parts[-1]):
                raise ValueError("source path was recreated after deletion claim")
            if self._entry_exists(bound.parent_fd, bound.name):
                descriptor, observed = self._open_and_inspect(bound)
                os.close(descriptor)
                if observed != token.physical_identity:
                    raise ValueError("quarantined artifact identity changed")
                named = os.stat(bound.name, dir_fd=bound.parent_fd, follow_symlinks=False)
                if (named.st_dev, named.st_ino, named.st_ctime_ns, named.st_nlink) != (
                    observed.device,
                    observed.inode,
                    observed.ctime_ns,
                    observed.link_count,
                ):
                    raise ValueError("quarantine path changed before deletion")
                bound.assert_ancestry_current()
                os.unlink(bound.name, dir_fd=bound.parent_fd)
                os.fsync(bound.parent_fd)
                bound.assert_ancestry_current()
            else:
                recovered = True
        return PhysicalDeletionReceipt(
            token_id=token.token_id,
            source_uri=token.source_uri,
            content_sha256=token.physical_identity.content_sha256,
            size_bytes=token.physical_identity.size_bytes,
            deleted_at=normalize_aware_utc(self._clock()),
            recovered_after_unlink=recovered,
        )


_RUNTIME_APPLICATION_ID = 0x52514743
_RUNTIME_SCHEMA_VERSION = 6
_RUNTIME_WORK_COLUMNS = (
    "work_id",
    "candidate_id",
    "content_sha256",
    "size_bytes",
    "plan_json",
    "candidate_json",
    "claim_json",
    "physical_identity_json",
    "token_json",
    "deletion_receipt_json",
    "status",
    "attempts",
    "next_attempt_at",
    "last_error",
    "updated_at",
    "authorization_id",
    "authorization_json",
    "created_at",
)
_RUNTIME_SCHEMA = f"""
PRAGMA application_id = {_RUNTIME_APPLICATION_ID};
PRAGMA user_version = {_RUNTIME_SCHEMA_VERSION};
CREATE TABLE IF NOT EXISTS gc_runtime_lease (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    owner_id TEXT,
    fence INTEGER NOT NULL,
    expires_at TEXT,
    lease_token TEXT
);
INSERT OR IGNORE INTO gc_runtime_lease(singleton, owner_id, lease_token, fence, expires_at)
VALUES (1, NULL, NULL, 0, NULL);

CREATE TABLE IF NOT EXISTS gc_runtime_work (
    work_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    plan_json TEXT NOT NULL,
    candidate_json TEXT NOT NULL,
    claim_json TEXT,
    physical_identity_json TEXT,
    token_json TEXT,
    deletion_receipt_json TEXT,
    status TEXT NOT NULL CHECK(status IN (
        'queued', 'claimed', 'quarantined', 'deleted', 'retry', 'completed', 'dead'
    )),
    attempts INTEGER NOT NULL,
    next_attempt_at TEXT NOT NULL,
    last_error TEXT,
    updated_at TEXT NOT NULL,
    authorization_id TEXT,
    authorization_json TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS gc_runtime_work_due_idx
ON gc_runtime_work(status, next_attempt_at, work_id);

CREATE TABLE IF NOT EXISTS gc_runtime_audit (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    work_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS gc_runtime_audit_no_update
BEFORE UPDATE ON gc_runtime_audit
BEGIN SELECT RAISE(ABORT, 'gc_runtime_audit is append-only'); END;
CREATE TRIGGER IF NOT EXISTS gc_runtime_audit_no_delete
BEFORE DELETE ON gc_runtime_audit
BEGIN SELECT RAISE(ABORT, 'gc_runtime_audit is append-only'); END;

CREATE TABLE IF NOT EXISTS gc_tier_migration_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    content_sha256 TEXT,
    tier_rank INTEGER,
    location_id TEXT,
    updated_at TEXT,
    CHECK (
        (content_sha256 IS NULL AND tier_rank IS NULL AND location_id IS NULL)
        OR (
            content_sha256 IS NOT NULL
            AND tier_rank IN (0, 1)
            AND location_id IS NOT NULL
        )
    )
);
INSERT OR IGNORE INTO gc_tier_migration_state(
    singleton, content_sha256, tier_rank, location_id, updated_at
) VALUES (1, NULL, NULL, NULL, NULL);
"""

_RUNTIME_HEALTH_SCHEMA = """
CREATE TABLE IF NOT EXISTS gc_runtime_health_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    backlog_count INTEGER NOT NULL CHECK(backlog_count >= 0),
    retry_count INTEGER NOT NULL CHECK(retry_count >= 0),
    dead_letter_count INTEGER NOT NULL CHECK(dead_letter_count >= 0),
    aggregate_cursor_work_id TEXT,
    aggregate_complete INTEGER NOT NULL CHECK(aggregate_complete IN (0, 1)),
    projection_updated_at TEXT,
    projection_work_id TEXT,
    CHECK (
        (projection_updated_at IS NULL AND projection_work_id IS NULL)
        OR (projection_updated_at IS NOT NULL AND projection_work_id IS NOT NULL)
    )
);
INSERT OR IGNORE INTO gc_runtime_health_state(
    singleton, backlog_count, retry_count, dead_letter_count,
    aggregate_cursor_work_id, aggregate_complete,
    projection_updated_at, projection_work_id
) VALUES (1, 0, 0, 0, NULL, 0, NULL, NULL);

CREATE INDEX IF NOT EXISTS gc_runtime_work_health_status_idx
ON gc_runtime_work(status, created_at, work_id);
CREATE INDEX IF NOT EXISTS gc_runtime_work_health_cursor_idx
ON gc_runtime_work(updated_at, work_id);

CREATE TRIGGER IF NOT EXISTS gc_runtime_health_work_insert
AFTER INSERT ON gc_runtime_work
WHEN (
    SELECT aggregate_complete = 1
        OR NEW.work_id <= COALESCE(aggregate_cursor_work_id, '')
    FROM gc_runtime_health_state WHERE singleton = 1
)
BEGIN
    UPDATE gc_runtime_health_state
    SET backlog_count = backlog_count
            + CASE WHEN NEW.status NOT IN ('completed', 'dead') THEN 1 ELSE 0 END,
        retry_count = retry_count + CASE WHEN NEW.status = 'retry' THEN 1 ELSE 0 END,
        dead_letter_count = dead_letter_count
            + CASE WHEN NEW.status = 'dead' THEN 1 ELSE 0 END
    WHERE singleton = 1;
END;

CREATE TRIGGER IF NOT EXISTS gc_runtime_health_work_status_update
AFTER UPDATE OF status ON gc_runtime_work
WHEN OLD.status != NEW.status AND (
    SELECT aggregate_complete = 1
        OR NEW.work_id <= COALESCE(aggregate_cursor_work_id, '')
    FROM gc_runtime_health_state WHERE singleton = 1
)
BEGIN
    UPDATE gc_runtime_health_state
    SET backlog_count = backlog_count
            - CASE WHEN OLD.status NOT IN ('completed', 'dead') THEN 1 ELSE 0 END
            + CASE WHEN NEW.status NOT IN ('completed', 'dead') THEN 1 ELSE 0 END,
        retry_count = retry_count - CASE WHEN OLD.status = 'retry' THEN 1 ELSE 0 END
            + CASE WHEN NEW.status = 'retry' THEN 1 ELSE 0 END,
        dead_letter_count = dead_letter_count
            - CASE WHEN OLD.status = 'dead' THEN 1 ELSE 0 END
            + CASE WHEN NEW.status = 'dead' THEN 1 ELSE 0 END
    WHERE singleton = 1;
END;
"""


class ArtifactGcRuntimeStore:
    def __init__(self, path: Path, *, managed_trust_root: Path) -> None:
        self.path = Path(path)
        self._authority = PrivateSqlitePathAuthority(
            self.path,
            label="artifact GC runtime store",
            create_if_missing=True,
            managed_trust_root=managed_trust_root,
        )
        connection = self._connect(verify_schema=False)
        with verified_sqlite_connection_scope(connection, self._authority):
            existing_application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
            existing_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            health_state_existed = (
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'gc_runtime_health_state'
                    """
                ).fetchone()
                is not None
            )
            if existing_application_id not in {
                0,
                _RUNTIME_APPLICATION_ID,
            } or existing_version not in {
                0,
                1,
                2,
                3,
                4,
                5,
                _RUNTIME_SCHEMA_VERSION,
            }:
                raise ValueError("artifact GC runtime schema identity is invalid")
            connection.executescript(_RUNTIME_SCHEMA)
            lease_columns = {
                str(row[1])
                for row in connection.execute('PRAGMA table_info("gc_runtime_lease")').fetchall()
            }
            if "lease_token" not in lease_columns:
                connection.execute("ALTER TABLE gc_runtime_lease ADD COLUMN lease_token TEXT")
                connection.execute(
                    """
                    UPDATE gc_runtime_lease
                    SET owner_id = NULL, lease_token = NULL, expires_at = NULL
                    WHERE singleton = 1
                    """
                )
            self._migrate_runtime_work(connection)
            connection.executescript(_RUNTIME_HEALTH_SCHEMA)
            if not health_state_existed:
                has_work = connection.execute("SELECT 1 FROM gc_runtime_work LIMIT 1").fetchone()
                if has_work is None:
                    connection.execute(
                        """
                        UPDATE gc_runtime_health_state
                        SET aggregate_complete = 1
                        WHERE singleton = 1
                        """
                    )
            connection.execute(f"PRAGMA user_version = {_RUNTIME_SCHEMA_VERSION}")
            self._verify_schema(connection)
            self._authority.rebind_ctime_after_trusted_sqlite_setup()

    @staticmethod
    def _migrate_runtime_work(connection: sqlite3.Connection) -> None:
        observed = tuple(
            str(row[1])
            for row in connection.execute('PRAGMA table_info("gc_runtime_work")').fetchall()
        )
        if observed == _RUNTIME_WORK_COLUMNS:
            return
        legacy_base = (
            "work_id",
            "candidate_id",
            "content_sha256",
            "size_bytes",
            "plan_json",
            "candidate_json",
            "claim_json",
            "physical_identity_json",
            "token_json",
            "deletion_receipt_json",
            "status",
            "attempts",
            "next_attempt_at",
            "last_error",
            "updated_at",
        )
        supported = {
            legacy_base,
            legacy_base + ("authorization_id", "authorization_json"),
            legacy_base + ("authorization_id", "authorization_json", "created_at"),
            legacy_base[:6] + ("authorization_id", "authorization_json") + legacy_base[6:],
        }
        if observed not in supported:
            raise ValueError("artifact GC runtime schema columns drifted: gc_runtime_work")
        observed_set = set(observed)
        authorization_id = "authorization_id" if "authorization_id" in observed_set else "NULL"
        authorization_json = (
            "authorization_json" if "authorization_json" in observed_set else "NULL"
        )
        created_at = "created_at" if "created_at" in observed_set else "updated_at"
        connection.execute("DROP INDEX IF EXISTS gc_runtime_work_due_idx")
        connection.execute("ALTER TABLE gc_runtime_work RENAME TO gc_runtime_work_legacy")
        connection.execute(
            """
            CREATE TABLE gc_runtime_work (
                work_id TEXT PRIMARY KEY,
                candidate_id TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                plan_json TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                claim_json TEXT,
                physical_identity_json TEXT,
                token_json TEXT,
                deletion_receipt_json TEXT,
                status TEXT NOT NULL CHECK(status IN (
                    'queued', 'claimed', 'quarantined', 'deleted',
                    'retry', 'completed', 'dead'
                )),
                attempts INTEGER NOT NULL,
                next_attempt_at TEXT NOT NULL,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                authorization_id TEXT,
                authorization_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            f"""
            INSERT INTO gc_runtime_work({", ".join(_RUNTIME_WORK_COLUMNS)})
            SELECT
                work_id, candidate_id, content_sha256, size_bytes,
                plan_json, candidate_json, claim_json, physical_identity_json,
                token_json, deletion_receipt_json, status, attempts,
                next_attempt_at, last_error, updated_at,
                {authorization_id}, {authorization_json}, {created_at}
            FROM gc_runtime_work_legacy
            """
        )
        connection.execute("DROP TABLE gc_runtime_work_legacy")
        connection.execute(
            """
            CREATE INDEX gc_runtime_work_due_idx
            ON gc_runtime_work(status, next_attempt_at, work_id)
            """
        )

    def _connect(self, *, verify_schema: bool = True) -> sqlite3.Connection:
        connection = self._authority.open_verified_connection(
            lambda path: sqlite3.connect(path, timeout=30, isolation_level=None)
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA journal_mode = WAL")
        self._authority.rebind_and_assert_current_after_trusted_sqlite_change()
        if verify_schema:
            self._verify_schema(connection)
        return connection

    @staticmethod
    def _verify_schema(connection: sqlite3.Connection) -> None:
        if int(connection.execute("PRAGMA application_id").fetchone()[0]) != (
            _RUNTIME_APPLICATION_ID
        ) or int(connection.execute("PRAGMA user_version").fetchone()[0]) != (
            _RUNTIME_SCHEMA_VERSION
        ):
            raise ValueError("artifact GC runtime schema identity is invalid")
        expected_columns = {
            "gc_runtime_lease": (
                "singleton",
                "owner_id",
                "fence",
                "expires_at",
                "lease_token",
            ),
            "gc_runtime_work": _RUNTIME_WORK_COLUMNS,
            "gc_runtime_audit": (
                "sequence",
                "event_type",
                "work_id",
                "occurred_at",
                "payload_json",
            ),
            "gc_runtime_health_state": (
                "singleton",
                "backlog_count",
                "retry_count",
                "dead_letter_count",
                "aggregate_cursor_work_id",
                "aggregate_complete",
                "projection_updated_at",
                "projection_work_id",
            ),
            "gc_tier_migration_state": (
                "singleton",
                "content_sha256",
                "tier_rank",
                "location_id",
                "updated_at",
            ),
        }
        tables = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }
        if tables != set(expected_columns):
            raise ValueError("artifact GC runtime schema tables drifted")
        for table, expected in expected_columns.items():
            observed = tuple(
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
            )
            if observed != expected:
                raise ValueError(f"artifact GC runtime schema columns drifted: {table}")
        indexes = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'index' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }
        triggers = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
        }
        views = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'view' LIMIT 1"
        ).fetchone()
        if (
            indexes
            != {
                "gc_runtime_work_due_idx",
                "gc_runtime_work_health_cursor_idx",
                "gc_runtime_work_health_status_idx",
            }
            or triggers
            != {
                "gc_runtime_audit_no_update",
                "gc_runtime_audit_no_delete",
                "gc_runtime_health_work_insert",
                "gc_runtime_health_work_status_update",
            }
            or views is not None
        ):
            raise ValueError("artifact GC runtime schema indexes or triggers drifted")

    def _write(self, operation: Callable[[sqlite3.Connection], object]) -> object:
        connection = self._connect()
        with verified_sqlite_connection_scope(connection, self._authority):
            try:
                connection.execute("BEGIN IMMEDIATE")
                result = operation(connection)
                connection.execute("COMMIT")
                self._authority.rebind_ctime_after_trusted_sqlite_setup()
                return result
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                    self._authority.rebind_ctime_after_trusted_sqlite_setup()
                raise

    def _read(self, operation: Callable[[sqlite3.Connection], object]) -> object:
        connection = self._connect()
        with verified_sqlite_connection_scope(connection, self._authority):
            return operation(connection)

    def tier_migration_cursor(self) -> TierMigrationCursor | None:
        row = self._read(
            lambda connection: connection.execute(
                """
                SELECT content_sha256, tier_rank, location_id
                FROM gc_tier_migration_state WHERE singleton = 1
                """
            ).fetchone()
        )
        assert row is not None  # type: ignore[unreachable]
        if row["content_sha256"] is None:  # type: ignore[index]
            return None
        return TierMigrationCursor(
            content_sha256=str(row["content_sha256"]),  # type: ignore[index]
            tier_rank=int(row["tier_rank"]),  # type: ignore[index]
            location_id=str(row["location_id"]),  # type: ignore[index]
        )

    def persist_tier_migration_cursor(
        self,
        cursor: TierMigrationCursor | None,
        *,
        updated_at: datetime,
    ) -> None:
        observed = normalize_aware_utc(updated_at)
        self._write(
            lambda connection: connection.execute(
                """
                UPDATE gc_tier_migration_state
                SET content_sha256 = ?, tier_rank = ?, location_id = ?, updated_at = ?
                WHERE singleton = 1
                """,
                (
                    cursor.content_sha256 if cursor is not None else None,
                    cursor.tier_rank if cursor is not None else None,
                    cursor.location_id if cursor is not None else None,
                    observed.isoformat(),
                ),
            )
        )

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        event_type: str,
        work_id: str,
        now: datetime,
        payload: object,
    ) -> None:
        connection.execute(
            """
            INSERT INTO gc_runtime_audit(event_type, work_id, occurred_at, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                event_type,
                work_id,
                now.isoformat(),
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
            ),
        )

    def acquire_lease(
        self,
        owner_id: str,
        *,
        lease_token: str,
        now: datetime,
        ttl: timedelta,
    ) -> GcLeaseGrant:
        now = normalize_aware_utc(now)
        if len(lease_token) != 64 or any(
            character not in "0123456789abcdef" for character in lease_token
        ):
            raise ValueError("artifact GC lease token must be a sha256 value")

        def acquire(connection: sqlite3.Connection) -> GcLeaseGrant:
            row = connection.execute(
                "SELECT * FROM gc_runtime_lease WHERE singleton = 1"
            ).fetchone()
            assert row is not None
            expiry = datetime.fromisoformat(row["expires_at"]) if row["expires_at"] else None
            if row["owner_id"] is not None and expiry is not None and expiry > now:
                if row["owner_id"] == owner_id and row["lease_token"] == lease_token:
                    return GcLeaseGrant(
                        owner_label=owner_id,
                        lease_token=lease_token,
                        fence=int(row["fence"]),
                        expires_at=expiry,
                    )
                raise GcLeaseBusyError("artifact GC lease is held by another run token")
            fence = int(row["fence"])
            fence += 1
            expires_at = now + ttl
            connection.execute(
                """
                UPDATE gc_runtime_lease
                SET owner_id = ?, lease_token = ?, fence = ?, expires_at = ?
                WHERE singleton = 1
                """,
                (owner_id, lease_token, fence, expires_at.isoformat()),
            )
            self._audit(
                connection,
                "lease_acquired",
                lease_token,
                now,
                {"owner_label": owner_id, "fence": fence},
            )
            return GcLeaseGrant(
                owner_label=owner_id,
                lease_token=lease_token,
                fence=fence,
                expires_at=expires_at,
            )

        return self._write(acquire)  # type: ignore[return-value]

    def renew_lease(
        self,
        owner_id: str,
        lease_token: str,
        fence: int,
        *,
        now: datetime,
        ttl: timedelta,
    ) -> GcLeaseGrant:
        now = normalize_aware_utc(now)

        def renew(connection: sqlite3.Connection) -> GcLeaseGrant:
            self._assert_lease(connection, owner_id, lease_token, fence, now)
            expires_at = now + ttl
            connection.execute(
                "UPDATE gc_runtime_lease SET expires_at = ? WHERE singleton = 1",
                (expires_at.isoformat(),),
            )
            self._audit(
                connection,
                "lease_renewed",
                lease_token,
                now,
                {"owner_label": owner_id, "fence": fence},
            )
            return GcLeaseGrant(
                owner_label=owner_id,
                lease_token=lease_token,
                fence=fence,
                expires_at=expires_at,
            )

        return self._write(renew)  # type: ignore[return-value]

    def release_lease(
        self,
        owner_id: str,
        lease_token: str,
        fence: int,
        *,
        now: datetime,
    ) -> None:
        now = normalize_aware_utc(now)

        def release(connection: sqlite3.Connection) -> None:
            changed = connection.execute(
                """
                UPDATE gc_runtime_lease
                SET owner_id = NULL, lease_token = NULL, expires_at = NULL
                WHERE singleton = 1 AND owner_id = ? AND lease_token = ? AND fence = ?
                """,
                (owner_id, lease_token, fence),
            ).rowcount
            if changed != 1:
                raise ValueError("artifact GC lease fence is stale")
            self._audit(
                connection,
                "lease_released",
                lease_token,
                now,
                {"owner_label": owner_id, "fence": fence},
            )

        self._write(release)

    def assert_lease_current(
        self,
        owner_id: str,
        lease_token: str,
        fence: int,
        *,
        now: datetime,
    ) -> None:
        now = normalize_aware_utc(now)
        self._read(
            lambda connection: self._assert_lease(
                connection,
                owner_id,
                lease_token,
                fence,
                now,
            )
        )

    @staticmethod
    def _assert_lease(
        connection: sqlite3.Connection,
        owner_id: str,
        lease_token: str,
        fence: int,
        now: datetime,
    ) -> None:
        row = connection.execute("SELECT * FROM gc_runtime_lease WHERE singleton = 1").fetchone()
        if (
            row is None
            or row["owner_id"] != owner_id
            or row["lease_token"] != lease_token
            or int(row["fence"]) != fence
            or row["expires_at"] is None
            or datetime.fromisoformat(row["expires_at"]) <= now
        ):
            raise ValueError("artifact GC lease expired or fence is stale")

    def enqueue(
        self,
        *,
        plan: GcPlan,
        candidate: GcCandidate,
        owner_id: str,
        lease_token: str,
        fence: int,
        now: datetime,
    ) -> str:
        now = normalize_aware_utc(now)
        assert candidate.candidate_id is not None
        work_id = candidate.candidate_id

        def enqueue_row(connection: sqlite3.Connection) -> str:
            self._assert_lease(connection, owner_id, lease_token, fence, now)
            connection.execute(
                """
                INSERT OR IGNORE INTO gc_runtime_work(
                    work_id, candidate_id, content_sha256, size_bytes,
                    plan_json, candidate_json, status, attempts,
                    next_attempt_at, updated_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?)
                """,
                (
                    work_id,
                    candidate.candidate_id,
                    candidate.object_identity.content_sha256,
                    candidate.object_identity.size_bytes,
                    plan.model_dump_json(),
                    candidate.model_dump_json(),
                    now.isoformat(),
                    now.isoformat(),
                    now.isoformat(),
                ),
            )
            self._audit(connection, "work_enqueued", work_id, now, {"fence": fence})
            return work_id

        return str(self._write(enqueue_row))

    def next_due(self, *, now: datetime) -> sqlite3.Row | None:
        now = normalize_aware_utc(now)
        return self._read(
            lambda connection: connection.execute(
                """
                SELECT * FROM gc_runtime_work
                WHERE status IN ('queued', 'retry', 'claimed', 'quarantined', 'deleted')
                  AND next_attempt_at <= ?
                ORDER BY updated_at, work_id LIMIT 1
                """,
                (now.isoformat(),),
            ).fetchone()
        )  # type: ignore[return-value]

    def operation_id_for_candidate(self, candidate_id: str) -> str | None:
        row = self._read(
            lambda connection: connection.execute(
                """
                SELECT work_id FROM gc_runtime_work
                WHERE candidate_id = ?
                ORDER BY updated_at DESC, work_id DESC
                LIMIT 1
                """,
                (candidate_id,),
            ).fetchone()
        )
        return None if row is None else str(row["work_id"])  # type: ignore[index]

    def transition(
        self,
        work_id: str,
        *,
        owner_id: str,
        lease_token: str,
        fence: int,
        now: datetime,
        status: str,
        event_type: str,
        values: dict[str, object] | None = None,
    ) -> None:
        now = normalize_aware_utc(now)
        values = values or {}
        allowed = {
            "claim_json",
            "authorization_id",
            "authorization_json",
            "physical_identity_json",
            "token_json",
            "deletion_receipt_json",
            "last_error",
            "attempts",
            "next_attempt_at",
        }
        if not set(values) <= allowed:
            raise ValueError("unsupported GC work transition field")

        def change(connection: sqlite3.Connection) -> None:
            self._assert_lease(connection, owner_id, lease_token, fence, now)
            assignments = ["status = ?", "updated_at = ?"]
            parameters: list[object] = [status, now.isoformat()]
            for key, value in values.items():
                assignments.append(f"{key} = ?")
                parameters.append(value)
            parameters.append(work_id)
            changed = connection.execute(
                f"UPDATE gc_runtime_work SET {', '.join(assignments)} WHERE work_id = ?",
                tuple(parameters),
            ).rowcount
            if changed != 1:
                raise KeyError(f"unknown GC work: {work_id}")
            audit_values = {
                key.removesuffix("_json") if key.endswith("_json") else key: (
                    json.loads(value) if key.endswith("_json") and isinstance(value, str) else value
                )
                for key, value in values.items()
            }
            self._audit(
                connection,
                event_type,
                work_id,
                now,
                {"status": status, **audit_values},
            )

        self._write(change)

    def completed_count(self) -> int:
        return int(
            self._read(
                lambda connection: connection.execute(
                    "SELECT COUNT(*) FROM gc_runtime_work WHERE status = 'completed'"
                ).fetchone()[0]
            )
        )

    def dead_letter_count(self) -> int:
        return int(
            self._read(
                lambda connection: connection.execute(
                    "SELECT COUNT(*) FROM gc_runtime_work WHERE status = 'dead'"
                ).fetchone()[0]
            )
        )

    def health_aggregate(
        self,
        *,
        now: datetime,
        max_items: int,
        max_bytes: int,
        deadline_monotonic: float,
        monotonic: Callable[[], float],
    ) -> GcRuntimeHealthAggregate:
        now = normalize_aware_utc(now)
        if max_items < 0 or max_bytes < 0:
            raise ValueError("artifact GC health aggregate budgets must be nonnegative")

        def read_health(connection: sqlite3.Connection) -> GcRuntimeHealthAggregate:
            state = connection.execute(
                "SELECT * FROM gc_runtime_health_state WHERE singleton = 1"
            ).fetchone()
            assert state is not None
            scanned_items = 0
            scanned_bytes = 0
            aggregate_complete = bool(state["aggregate_complete"])
            aggregate_cursor = state["aggregate_cursor_work_id"]
            if (
                not aggregate_complete
                and max_items > 0
                and max_bytes > 0
                and monotonic() < deadline_monotonic
            ):
                rows = connection.execute(
                    """
                    SELECT work_id, status, created_at,
                           length(work_id) + length(status) + length(created_at) AS budget_bytes
                    FROM gc_runtime_work
                    WHERE work_id > ?
                    ORDER BY work_id
                    LIMIT ?
                    """,
                    (aggregate_cursor or "", max_items + 1),
                ).fetchall()
                backlog_delta = retry_delta = dead_delta = 0
                for row in rows[:max_items]:
                    if monotonic() >= deadline_monotonic:
                        break
                    row_bytes = int(row["budget_bytes"])
                    if scanned_bytes + row_bytes > max_bytes:
                        break
                    status = str(row["status"])
                    backlog_delta += int(status not in {"completed", "dead"})
                    retry_delta += int(status == "retry")
                    dead_delta += int(status == "dead")
                    scanned_items += 1
                    scanned_bytes += row_bytes
                    aggregate_cursor = str(row["work_id"])
                aggregate_complete = bool(rows) and scanned_items == len(rows)
                if not rows:
                    aggregate_complete = True
                connection.execute(
                    """
                    UPDATE gc_runtime_health_state
                    SET backlog_count = backlog_count + ?,
                        retry_count = retry_count + ?,
                        dead_letter_count = dead_letter_count + ?,
                        aggregate_cursor_work_id = ?,
                        aggregate_complete = ?
                    WHERE singleton = 1
                    """,
                    (
                        backlog_delta,
                        retry_delta,
                        dead_delta,
                        aggregate_cursor,
                        int(aggregate_complete),
                    ),
                )
                state = connection.execute(
                    "SELECT * FROM gc_runtime_health_state WHERE singleton = 1"
                ).fetchone()
                assert state is not None

            oldest_created_at: datetime | None = None
            if int(state["backlog_count"]) and monotonic() < deadline_monotonic:
                oldest_values: list[datetime] = []
                for status in ("queued", "claimed", "quarantined", "deleted", "retry"):
                    if monotonic() >= deadline_monotonic:
                        break
                    row = connection.execute(
                        """
                        SELECT created_at FROM gc_runtime_work
                        INDEXED BY gc_runtime_work_health_status_idx
                        WHERE status = ?
                        ORDER BY created_at, work_id
                        LIMIT 1
                        """,
                        (status,),
                    ).fetchone()
                    if row is not None:
                        oldest_values.append(datetime.fromisoformat(str(row["created_at"])))
                if oldest_values:
                    oldest_created_at = min(oldest_values)
            lease = connection.execute(
                "SELECT * FROM gc_runtime_lease WHERE singleton = 1"
            ).fetchone()
            assert lease is not None
            lease_expiry = (
                datetime.fromisoformat(lease["expires_at"])
                if lease["expires_at"] is not None
                else None
            )
            return GcRuntimeHealthAggregate(
                backlog_count=int(state["backlog_count"]),
                oldest_created_at=oldest_created_at,
                retry_count=int(state["retry_count"]),
                dead_letter_count=int(state["dead_letter_count"]),
                lease_fence=int(lease["fence"]),
                lease_active=(
                    lease["owner_id"] is not None
                    and lease["lease_token"] is not None
                    and lease_expiry is not None
                    and lease_expiry > now
                ),
                scanned_items=scanned_items,
                scanned_bytes=scanned_bytes,
                truncated=(
                    not bool(state["aggregate_complete"]) or monotonic() >= deadline_monotonic
                ),
            )

        return self._write(read_health)  # type: ignore[return-value]

    def health_projection_cursor(self) -> ArtifactGcHealthCursor | None:
        row = self._read(
            lambda connection: connection.execute(
                """
                SELECT projection_updated_at, projection_work_id
                FROM gc_runtime_health_state WHERE singleton = 1
                """
            ).fetchone()
        )
        assert row is not None  # type: ignore[unreachable]
        if row["projection_updated_at"] is None:  # type: ignore[index]
            return None
        return ArtifactGcHealthCursor(
            updated_at=datetime.fromisoformat(str(row["projection_updated_at"])),  # type: ignore[index]
            work_id=str(row["projection_work_id"]),  # type: ignore[index]
        )

    def persist_health_projection_cursor(
        self,
        cursor: ArtifactGcHealthCursor | None,
    ) -> None:
        self._write(
            lambda connection: connection.execute(
                """
                UPDATE gc_runtime_health_state
                SET projection_updated_at = ?, projection_work_id = ?
                WHERE singleton = 1
                """,
                (
                    cursor.updated_at.isoformat() if cursor is not None else None,
                    cursor.work_id if cursor is not None else None,
                ),
            )
        )

    def health_work_page(
        self,
        *,
        after_updated_at: datetime | None,
        after_work_id: str | None,
        recent_since: datetime,
        limit: int = 256,
    ) -> tuple[sqlite3.Row, ...]:
        if limit < 1 or limit > 1024:
            raise ValueError("artifact GC health page limit is out of bounds")
        recent_since = normalize_aware_utc(recent_since)
        cursor_time = normalize_aware_utc(after_updated_at or datetime.min.replace(tzinfo=UTC))
        rows = self._read(
            lambda connection: connection.execute(
                """
                SELECT * FROM gc_runtime_work
                WHERE (
                    status NOT IN ('completed', 'dead')
                    OR (
                        updated_at >= ?
                        AND (status = 'dead' OR last_error IS NOT NULL)
                    )
                )
                AND (
                    updated_at > ?
                    OR (updated_at = ? AND work_id > ?)
                )
                ORDER BY updated_at, work_id
                LIMIT ?
                """,
                (
                    recent_since.isoformat(),
                    cursor_time.isoformat(),
                    cursor_time.isoformat(),
                    after_work_id or "",
                    limit,
                ),
            ).fetchall()
        )
        return tuple(rows)  # type: ignore[arg-type]

    def audit_events(self) -> tuple[GcRuntimeAuditEvent, ...]:
        rows = self._read(
            lambda connection: connection.execute(
                "SELECT * FROM gc_runtime_audit ORDER BY sequence"
            ).fetchall()
        )
        return tuple(
            GcRuntimeAuditEvent(
                sequence=row["sequence"],
                event_type=row["event_type"],
                work_id=row["work_id"],
                occurred_at=datetime.fromisoformat(row["occurred_at"]),
                payload_json=row["payload_json"],
            )
            for row in rows  # type: ignore[union-attr]
        )


class ArtifactGcHealthProjector:
    """Project retention health for the existing health/notifier boundary."""

    def __init__(
        self,
        *,
        catalog: ArtifactReferenceStore,
        state: ArtifactGcRuntimeStore,
        quarantine_inspector: ArtifactQuarantineInspector,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.catalog = catalog
        self.state = state
        self.quarantine_inspector = quarantine_inspector
        self._monotonic = monotonic or time.monotonic

    @staticmethod
    def _operation_needs_reconciliation(
        row: sqlite3.Row,
        operation: object | None,
    ) -> bool:
        claim_json = row["claim_json"]
        if operation is None:
            return claim_json is not None or row["status"] in {
                "claimed",
                "quarantined",
                "deleted",
                "completed",
            }
        if claim_json is None:
            return True
        claim = GcClaim.model_validate_json(claim_json)
        operation_claim = getattr(operation, "claim", None)
        operation_status = getattr(operation, "status", None)
        if operation_claim != claim:
            return True
        if row["status"] == "completed":
            return operation_status != "completed"
        return operation_status == "completed"

    def snapshot(
        self,
        *,
        now: datetime,
        max_items: int = 256,
        max_bytes: int = 1 << 30,
        deadline_monotonic: float | None = None,
        cursor: ArtifactGcHealthCursor | None = None,
        recent_anomaly_window: timedelta = timedelta(days=7),
    ) -> ArtifactGcHealthSummary:
        deadline = self._monotonic() + 1.0 if deadline_monotonic is None else deadline_monotonic
        if self._monotonic() >= deadline:
            return ArtifactGcHealthSummary(
                observed_at=now,
                status="degraded",
                backlog_count=0,
                oldest_backlog_age_seconds=None,
                operation_reconciliation_pending_count=0,
                quarantine_orphan_count=0,
                retry_count=0,
                dead_letter_count=0,
                lease_fence=0,
                lease_active=False,
                truncated=True,
                next_cursor=cursor,
            )
        now = normalize_aware_utc(now)
        if not 1 <= max_items <= 1_023:
            raise ValueError("artifact GC health max_items must be between 1 and 1023")
        if max_bytes < 1:
            raise ValueError("artifact GC health max_bytes must be positive")
        if recent_anomaly_window <= timedelta(0):
            raise ValueError("artifact GC health anomaly window must be positive")
        aggregate = self.state.health_aggregate(
            now=now,
            max_items=max_items,
            max_bytes=max_bytes,
            deadline_monotonic=deadline,
            monotonic=self._monotonic,
        )
        reconciliation = 0
        quarantine_orphans = 0
        scanned_items = aggregate.scanned_items
        scanned_bytes = aggregate.scanned_bytes
        next_cursor: ArtifactGcHealthCursor | None = None
        blocked_cursor: ArtifactGcHealthCursor | None = None
        blocked_work_id: str | None = None
        truncated = aggregate.truncated
        rows: tuple[sqlite3.Row, ...] = ()
        operations: dict[str, object] = {}
        remaining_items = max_items - scanned_items
        remaining_bytes = max_bytes - scanned_bytes
        if self._monotonic() >= deadline:
            truncated = True
        elif remaining_items > 0 and remaining_bytes > 0:
            active_cursor = cursor
            if active_cursor is None:
                active_cursor = self.state.health_projection_cursor()
            if self._monotonic() >= deadline:
                truncated = True
            else:
                rows = self.state.health_work_page(
                    after_updated_at=(
                        active_cursor.updated_at if active_cursor is not None else None
                    ),
                    after_work_id=(active_cursor.work_id if active_cursor is not None else None),
                    recent_since=now - recent_anomaly_window,
                    limit=remaining_items + 1,
                )
            if not rows and active_cursor is not None and self._monotonic() < deadline:
                self.state.persist_health_projection_cursor(None)
                active_cursor = None
                rows = self.state.health_work_page(
                    after_updated_at=None,
                    after_work_id=None,
                    recent_since=now - recent_anomaly_window,
                    limit=remaining_items + 1,
                )
            if self._monotonic() >= deadline:
                truncated = bool(rows)
        if rows:
            truncated = truncated or len(rows) > remaining_items
            bounded_rows: list[sqlite3.Row] = []
            projected_bytes = scanned_bytes
            for row in rows[:remaining_items]:
                if self._monotonic() >= deadline:
                    truncated = True
                    break
                row_size = int(row["size_bytes"])
                if projected_bytes + row_size > max_bytes:
                    blocked_work_id = str(row["work_id"])
                    blocked_cursor = ArtifactGcHealthCursor(
                        updated_at=datetime.fromisoformat(str(row["updated_at"])),
                        work_id=str(row["work_id"]),
                    )
                    truncated = True
                    break
                bounded_rows.append(row)
                projected_bytes += row_size
            if bounded_rows and self._monotonic() < deadline:
                operation_ids = tuple(str(row["work_id"]) for row in bounded_rows)
                operations = self.catalog.get_gc_operations(operation_ids)
            elif bounded_rows:
                truncated = True
                bounded_rows = []
            for row in bounded_rows:
                if self._monotonic() >= deadline:
                    truncated = True
                    break
                row_size = int(row["size_bytes"])
                operation = operations.get(str(row["work_id"]))
                try:
                    reconciliation += int(self._operation_needs_reconciliation(row, operation))
                except (TypeError, ValueError):
                    reconciliation += 1
                if (
                    row["claim_json"] is not None
                    and row["physical_identity_json"] is not None
                    and row["token_json"] is None
                    and row["status"] != "completed"
                ):
                    candidate = GcCandidate.model_validate_json(row["candidate_json"])
                    claim = GcClaim.model_validate_json(row["claim_json"])
                    expected = PhysicalObjectIdentity.model_validate_json(
                        row["physical_identity_json"]
                    )
                    try:
                        quarantine_orphans += int(
                            self.quarantine_inspector.is_quarantined(
                                candidate,
                                claim,
                                expected,
                            )
                        )
                    except (OSError, ValueError):
                        quarantine_orphans += 1
                if self._monotonic() >= deadline:
                    truncated = True
                    break
                scanned_items += 1
                scanned_bytes += row_size
                next_cursor = ArtifactGcHealthCursor(
                    updated_at=datetime.fromisoformat(str(row["updated_at"])),
                    work_id=str(row["work_id"]),
                )
            if blocked_cursor is not None and self._monotonic() < deadline:
                next_cursor = blocked_cursor
            if next_cursor is not None and self._monotonic() < deadline:
                self.state.persist_health_projection_cursor(next_cursor)
        oldest_age = None
        if aggregate.oldest_created_at is not None:
            oldest_age = max(
                0.0,
                (now - aggregate.oldest_created_at).total_seconds(),
            )
        if reconciliation or quarantine_orphans or aggregate.dead_letter_count:
            status: Literal["healthy", "degraded", "critical"] = "critical"
        elif aggregate.retry_count or truncated:
            status = "degraded"
        else:
            status = "healthy"
        return ArtifactGcHealthSummary(
            observed_at=now,
            status=status,
            backlog_count=aggregate.backlog_count,
            oldest_backlog_age_seconds=oldest_age,
            operation_reconciliation_pending_count=reconciliation,
            quarantine_orphan_count=quarantine_orphans,
            retry_count=aggregate.retry_count,
            dead_letter_count=aggregate.dead_letter_count,
            lease_fence=aggregate.lease_fence,
            lease_active=aggregate.lease_active,
            scanned_items=scanned_items,
            scanned_bytes=scanned_bytes,
            truncated=truncated,
            next_cursor=next_cursor,
            blocked_work_id=blocked_work_id,
        )


class ArtifactGcWorker:
    def __init__(
        self,
        *,
        catalog: ArtifactReferenceStore,
        state: ArtifactGcRuntimeStore,
        transport: ArtifactGcTransport,
        deletion_gate: ArtifactDeletionGate,
        policy: RetentionPolicy,
        config: GcWorkerConfig,
        worker_id: str,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.catalog = catalog
        self.state = state
        self.transport = transport
        self.policy = policy
        self.config = config
        self.worker_id = worker_id
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic or time.monotonic
        self.deletion_gate = deletion_gate

    def _checkpoint(
        self,
        *,
        lease_token: str,
        fence: int,
        deadline_monotonic: float,
        stage: str,
    ) -> datetime:
        if self._monotonic() >= deadline_monotonic:
            raise TimeoutError(f"artifact GC deadline reached at {stage}")
        now = normalize_aware_utc(self._clock())
        self.state.assert_lease_current(
            self.worker_id,
            lease_token,
            fence,
            now=now,
        )
        return now

    def _authorize_deletion(
        self,
        candidate: GcCandidate,
        *,
        now: datetime,
    ) -> FullVerifiedDeletionAuthorization:
        authorization = FullVerifiedDeletionAuthorization.model_validate(
            self.deletion_gate.authorize(candidate, as_of=now)
        )
        if authorization.verified_at > now:
            raise ValueError("recovery deletion authorization is from the future")
        if now >= authorization.expires_at:
            raise ValueError("recovery deletion authorization is expired")
        return authorization

    def _reauthorize_same_generation(
        self,
        candidate: GcCandidate,
        *,
        expected: FullVerifiedDeletionAuthorization,
        now: datetime,
    ) -> None:
        current = self._authorize_deletion(candidate, now=now)
        if not self._same_recovery_authority(current, expected):
            raise ValueError("recovery authorization generation changed during GC")

    @staticmethod
    def _same_recovery_authority(
        current: FullVerifiedDeletionAuthorization,
        expected: FullVerifiedDeletionAuthorization,
    ) -> bool:
        return current.authorization_id == expected.authorization_id

    def _bind_authorization(
        self,
        row: sqlite3.Row,
        candidate: GcCandidate,
        *,
        lease_token: str,
        fence: int,
        now: datetime,
    ) -> FullVerifiedDeletionAuthorization:
        authorization_id = row["authorization_id"]
        authorization_json = row["authorization_json"]
        if (authorization_id is None) != (authorization_json is None):
            raise ValueError("persisted recovery authorization is incomplete")
        current = self._authorize_deletion(candidate, now=now)
        assert current.authorization_id is not None
        if authorization_json is not None:
            persisted = FullVerifiedDeletionAuthorization.model_validate_json(authorization_json)
            if persisted.authorization_id != authorization_id or not self._same_recovery_authority(
                current, persisted
            ):
                raise ValueError("persisted recovery authorization is no longer current")
            return persisted
        self.state.transition(
            row["work_id"],
            owner_id=self.worker_id,
            lease_token=lease_token,
            fence=fence,
            now=now,
            status=str(row["status"]),
            event_type="recovery_authorization_bound",
            values={
                "authorization_id": current.authorization_id,
                "authorization_json": current.model_dump_json(),
            },
        )
        return current

    def run_once(self) -> GcRunSummary:
        started = self._monotonic()
        deadline = started + self.config.max_runtime.total_seconds()
        now = normalize_aware_utc(self._clock())
        lease_token = secrets.token_hex(32)
        lease = self.state.acquire_lease(
            self.worker_id,
            lease_token=lease_token,
            now=now,
            ttl=self.config.lease_ttl,
        )
        fence = lease.fence
        completed = failed = dead = deferred = bytes_deleted = 0
        try:
            while completed + failed + deferred < self.config.batch_items:
                if self._monotonic() >= deadline:
                    break
                current_now = self._checkpoint(
                    lease_token=lease_token,
                    fence=fence,
                    deadline_monotonic=deadline,
                    stage="run loop",
                )
                row = self.state.next_due(now=current_now)
                if row is None:
                    if deferred:
                        break
                    elapsed = self._monotonic() - started
                    remaining_items = self.config.batch_items - completed - failed - deferred
                    remaining_bytes = self.config.batch_bytes - bytes_deleted
                    remaining_runtime = self.config.max_runtime - timedelta(seconds=elapsed)
                    if remaining_bytes <= 0 or remaining_runtime <= timedelta(0):
                        break
                    current_now = self._checkpoint(
                        lease_token=lease_token,
                        fence=fence,
                        deadline_monotonic=deadline,
                        stage="GC planning",
                    )
                    plan = self.catalog.plan_gc(
                        now=current_now,
                        policy=self.policy,
                        max_items=remaining_items,
                        max_bytes=remaining_bytes,
                        max_runtime=remaining_runtime,
                        monotonic=self._monotonic,
                        deadline_monotonic=deadline,
                    )
                    current_now = self._checkpoint(
                        lease_token=lease_token,
                        fence=fence,
                        deadline_monotonic=deadline,
                        stage="GC planning completion",
                    )
                    candidate = next(
                        (
                            item
                            for item in plan.candidates
                            if item.object_copy.storage_tier is not StorageTier.COLD
                        ),
                        None,
                    )
                    if candidate is None:
                        deferred += min(len(plan.deferred_candidates), remaining_items)
                        break
                    work_id = self.state.enqueue(
                        plan=plan,
                        candidate=candidate,
                        owner_id=self.worker_id,
                        lease_token=lease_token,
                        fence=fence,
                        now=current_now,
                    )
                    row = self.state.next_due(now=current_now)
                    if row is None or row["work_id"] != work_id:
                        raise RuntimeError("enqueued GC work is not recoverable")
                remaining_bytes = self.config.batch_bytes - bytes_deleted
                if int(row["size_bytes"]) > remaining_bytes:
                    defer_until = current_now + max(
                        self.config.retry_delay,
                        timedelta(seconds=1),
                    )
                    self.state.transition(
                        row["work_id"],
                        owner_id=self.worker_id,
                        lease_token=lease_token,
                        fence=fence,
                        now=current_now,
                        status="retry",
                        event_type="work_budget_deferred",
                        values={
                            "last_error": ("deferred: object exceeds current GC run byte budget"),
                            "next_attempt_at": defer_until.isoformat(),
                        },
                    )
                    deferred += 1
                    continue
                try:
                    deleted_bytes = self._process(
                        row,
                        lease_token=lease_token,
                        fence=fence,
                        now=current_now,
                        deadline_monotonic=deadline,
                    )
                except Exception as exc:
                    attempts = int(row["attempts"]) + 1
                    is_dead = attempts >= self.config.max_attempts
                    failure_now = normalize_aware_utc(self._clock())
                    self.state.transition(
                        row["work_id"],
                        owner_id=self.worker_id,
                        lease_token=lease_token,
                        fence=fence,
                        now=failure_now,
                        status="dead" if is_dead else "retry",
                        event_type="work_dead_lettered" if is_dead else "work_retry_scheduled",
                        values={
                            "attempts": attempts,
                            "last_error": f"{type(exc).__name__}: {exc}"[:2048],
                            "next_attempt_at": (failure_now + self.config.retry_delay).isoformat(),
                        },
                    )
                    failed += 1
                    dead += int(is_dead)
                    break
                else:
                    completed += 1
                    bytes_deleted += deleted_bytes
                    if bytes_deleted >= self.config.batch_bytes:
                        break
        finally:
            self.state.release_lease(
                self.worker_id,
                lease_token,
                fence,
                now=normalize_aware_utc(self._clock()),
            )
        return GcRunSummary(
            completed=completed,
            failed=failed,
            dead_lettered=dead,
            deferred=deferred,
            bytes_deleted=bytes_deleted,
            fence=fence,
        )

    def _process(
        self,
        row: sqlite3.Row,
        *,
        lease_token: str,
        fence: int,
        now: datetime,
        deadline_monotonic: float,
    ) -> int:
        def checkpoint(stage: str) -> datetime:
            return self._checkpoint(
                lease_token=lease_token,
                fence=fence,
                deadline_monotonic=deadline_monotonic,
                stage=stage,
            )

        work_id = str(row["work_id"])
        candidate = GcCandidate.model_validate_json(row["candidate_json"])
        claim = GcClaim.model_validate_json(row["claim_json"]) if row["claim_json"] else None
        token = (
            DeletionQuarantineToken.model_validate_json(row["token_json"])
            if row["token_json"]
            else None
        )
        expected = (
            PhysicalObjectIdentity.model_validate_json(row["physical_identity_json"])
            if row["physical_identity_json"]
            else None
        )
        current_now = checkpoint("recovery authorization")
        authorization = self._bind_authorization(
            row,
            candidate,
            lease_token=lease_token,
            fence=fence,
            now=current_now,
        )
        current_now = checkpoint("recovery authorization completion")
        operation = self.catalog.get_gc_operation(work_id)
        current_now = checkpoint("catalog operation lookup")
        if operation is not None:
            if (
                operation.candidate_id != candidate.candidate_id
                or operation.content_sha256 != candidate.object_identity.content_sha256
                or operation.location_id != candidate.object_copy.location_id
            ):
                raise ValueError("catalog GC operation conflicts with runtime work content")
            if claim is not None and claim != operation.claim:
                raise ValueError("catalog GC operation conflicts with runtime checkpoint")
            if claim is None:
                claim = operation.claim
                self.state.transition(
                    work_id,
                    owner_id=self.worker_id,
                    lease_token=lease_token,
                    fence=fence,
                    now=current_now,
                    status="claimed" if operation.status == "claimed" else "deleted",
                    event_type="catalog_operation_recovered",
                    values={"claim_json": claim.model_dump_json()},
                )
            if operation.status == "released":
                raise ValueError("catalog GC operation was released")
            if operation.status == "completed":
                if not row["token_json"] or not row["deletion_receipt_json"]:
                    raise ValueError("completed catalog operation lacks runtime deletion evidence")
                receipt = PhysicalDeletionReceipt.model_validate_json(row["deletion_receipt_json"])
                self.state.transition(
                    work_id,
                    owner_id=self.worker_id,
                    lease_token=lease_token,
                    fence=fence,
                    now=current_now,
                    status="completed",
                    event_type="catalog_completion_recovered",
                )
                return receipt.size_bytes
        elif claim is not None:
            raise ValueError("runtime checkpoint has no matching catalog GC operation")
        if claim is None:
            current_now = checkpoint("eligibility planning")
            fresh_plan = self.catalog.plan_gc(
                now=current_now,
                policy=self.policy,
                max_runtime=self.config.max_runtime,
                monotonic=self._monotonic,
                deadline_monotonic=deadline_monotonic,
            )
            current_now = checkpoint("eligibility planning completion")
            fresh_candidate = next(
                (
                    item
                    for item in fresh_plan.candidates
                    if item.candidate_id == candidate.candidate_id
                ),
                None,
            )
            if fresh_candidate is None:
                raise ValueError("GC candidate is no longer eligible")
            candidate = fresh_candidate
            if not any(
                copy.storage_tier is StorageTier.COLD
                and copy.location_id != candidate.object_copy.location_id
                for copy in self.catalog.list_active_copies(
                    candidate.object_identity.content_sha256
                )
            ):
                raise ValueError("durable cold copy is not ready for physical deletion")
            if expected is None:
                current_now = checkpoint("physical identity observation")
                expected = self.transport.observe(candidate)
                current_now = checkpoint("physical identity observation completion")
                self.state.transition(
                    work_id,
                    owner_id=self.worker_id,
                    lease_token=lease_token,
                    fence=fence,
                    now=current_now,
                    status="queued",
                    event_type="physical_identity_bound",
                    values={"physical_identity_json": expected.model_dump_json()},
                )
            current_now = checkpoint("catalog deletion claim")
            claim = self.catalog.claim_deletion(
                plan=fresh_plan,
                candidate=candidate,
                owner_id=lease_token,
                now=current_now,
                operation_id=work_id,
            )
            current_now = checkpoint("catalog deletion claim completion")
            self.state.transition(
                work_id,
                owner_id=self.worker_id,
                lease_token=lease_token,
                fence=fence,
                now=current_now,
                status="claimed",
                event_type="catalog_claimed",
                values={"claim_json": claim.model_dump_json()},
            )

        current_now = checkpoint("cold copy lookup")
        cold = next(
            (
                copy
                for copy in self.catalog.list_active_copies(
                    candidate.object_identity.content_sha256
                )
                if copy.storage_tier is StorageTier.COLD
                and copy.location_id != candidate.object_copy.location_id
            ),
            None,
        )
        if cold is None:
            raise ValueError("durable cold copy is required before physical deletion")
        current_now = checkpoint("cold copy verification")
        cold_verification = self.transport.verify(cold.storage_uri)
        current_now = checkpoint("cold copy verification completion")
        if (
            cold_verification.content_sha256 != candidate.object_identity.content_sha256
            or cold_verification.size_bytes != candidate.object_identity.size_bytes
            or cold_verification.verified_at > current_now
            or current_now - cold_verification.verified_at > self.policy.verification_max_age
        ):
            raise ValueError("cold copy durability verification failed")

        if token is None:
            if expected is None:
                raise ValueError("claimed GC work lacks pre-claim physical identity")
            current_now = checkpoint("quarantine authorization")
            self._reauthorize_same_generation(
                candidate,
                expected=authorization,
                now=current_now,
            )
            current_now = checkpoint("quarantine")
            token = self.transport.quarantine(candidate, claim, expected)
            current_now = checkpoint("quarantine completion")
        if not row["token_json"]:
            self.state.transition(
                work_id,
                owner_id=self.worker_id,
                lease_token=lease_token,
                fence=fence,
                now=current_now,
                status="quarantined",
                event_type="deletion_quarantined",
                values={"token_json": token.model_dump_json()},
            )
        if row["deletion_receipt_json"]:
            receipt = PhysicalDeletionReceipt.model_validate_json(row["deletion_receipt_json"])
        else:
            current_now = checkpoint("unlink authorization")
            self._reauthorize_same_generation(
                candidate,
                expected=authorization,
                now=current_now,
            )
            current_now = checkpoint("physical unlink")
            receipt = self.transport.delete_quarantined(token)
            current_now = checkpoint("physical unlink completion")
        if not row["deletion_receipt_json"]:
            self.state.transition(
                work_id,
                owner_id=self.worker_id,
                lease_token=lease_token,
                fence=fence,
                now=current_now,
                status="deleted",
                event_type="physical_deletion_recorded",
                values={"deletion_receipt_json": receipt.model_dump_json()},
            )
        expired_recovery = None
        if current_now > claim.expires_at:
            assert (
                claim.claim_id is not None
                and candidate.candidate_id is not None
                and receipt.receipt_id is not None
            )
            expired_recovery = ExpiredGcClaimRecoveryReceipt(
                claim_id=claim.claim_id,
                candidate_id=candidate.candidate_id,
                token_id=token.token_id,
                deletion_receipt_id=receipt.receipt_id,
                owner_id=claim.owner_id,
                runtime_fence=fence,
                recovered_at=current_now,
            )
        current_now = checkpoint("catalog deletion finalization")
        self.catalog.mark_deleted(
            claim=claim,
            observed_identity=candidate,
            now=current_now,
            expired_recovery=expired_recovery,
        )
        current_now = checkpoint("catalog deletion finalization completion")
        self.state.transition(
            work_id,
            owner_id=self.worker_id,
            lease_token=lease_token,
            fence=fence,
            now=current_now,
            status="completed",
            event_type="deletion_completed",
        )
        return receipt.size_bytes
