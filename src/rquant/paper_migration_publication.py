"""Descriptor-bound publication for offline paper-ledger migrations."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias

from pydantic import ConfigDict, Field, computed_field, field_validator, model_validator

from rquant._paper_sqlite_image import (
    _capture_stable_sqlite_image,
    _open_memory_sqlite_image,
    _revalidate_stable_sqlite_image,
    _StableSQLiteImageBinding,
)
from rquant.paper_contracts import Sha256
from rquant.paper_ledger_v4 import V4LedgerReconciliationReport
from rquant.private_fs import rename_noreplace_at, rename_noreplace_capability
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

PublicationProfile: TypeAlias = Literal["LOCAL_AUDIT", "SEPARATED_IDENTITY"]
PublicationState: TypeAlias = Literal[
    "GENERATION_RENAMED_UNCONFIRMED",
    "GENERATION_DURABLE_VERIFIED",
]
PaperMigrationFaultPoint: TypeAlias = Literal[
    "source_preflight",
    "source_reconciliation",
    "schema_additions",
    "legacy_cost_evidence",
    "archive",
    "v5_schema",
    "archive_protection",
    "attestation",
    "verification",
    "after_sqlite_connections_closed",
    "after_publication_object_first_hash",
    "after_object_noreplace_rename",
    "after_object_rebind_before_metadata",
    "after_manifest_fsync",
    "before_generation_noreplace_rename",
    "after_generation_rename_before_parent_fsync",
    "after_parent_fsync_before_final_verify",
    "during_final_generation_verify",
    "before_result_assembly",
    "before_local_failure_disposition",
    "after_materialization_first_object_hash",
    "during_materialization_copy",
    "after_private_memory_verification_before_final_rebind",
]

MIGRATION_FAULT_POINTS = (
    "source_preflight",
    "source_reconciliation",
    "schema_additions",
    "legacy_cost_evidence",
    "archive",
    "v5_schema",
    "archive_protection",
    "attestation",
    "verification",
    "after_sqlite_connections_closed",
    "after_publication_object_first_hash",
    "after_object_noreplace_rename",
    "after_object_rebind_before_metadata",
    "after_manifest_fsync",
    "before_generation_noreplace_rename",
    "after_generation_rename_before_parent_fsync",
    "after_parent_fsync_before_final_verify",
    "during_final_generation_verify",
    "before_result_assembly",
    "before_local_failure_disposition",
)
RECOVERY_FAULT_POINTS = (
    "after_parent_fsync_before_final_verify",
    "during_final_generation_verify",
    "before_result_assembly",
    "before_local_failure_disposition",
)
MATERIALIZATION_FAULT_POINTS = (
    "after_materialization_first_object_hash",
    "during_materialization_copy",
    "after_private_memory_verification_before_final_rebind",
    "before_local_failure_disposition",
)

_MANIFEST_NAME = "publication-manifest.json"
_COPY_CHUNK_SIZE = 1024 * 1024


def _absolute_lexical_path(value: Path) -> Path:
    path = Path(value)
    if not path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise ValueError("publication paths must be absolute and lexically normalized")
    return path


class PublicationStableDirectoryIdentity(RuntimeContractModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    uid: int = Field(ge=0)
    gid: int = Field(ge=0)
    mode: int = Field(ge=0, le=0o7777)
    file_type: Literal["directory"] = "directory"


class PublicationFileIdentity(RuntimeContractModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    uid: int = Field(ge=0)
    gid: int = Field(ge=0)
    mode: int = Field(ge=0, le=0o7777)
    nlink: Literal[1] = 1
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)
    file_type: Literal["regular"] = "regular"


class _PublicationFullDirectoryIdentityV2(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")
    contract: Literal["rquant-paper-publication-directory-identity/v2"] = (
        "rquant-paper-publication-directory-identity/v2"
    )
    device: int = Field(ge=0)
    inode: int = Field(ge=1)
    uid: int = Field(ge=0)
    gid: int = Field(ge=0)
    mode: int = Field(ge=0, le=0o7777)
    nlink: int = Field(ge=1)
    size: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    ctime_ns: int = Field(ge=0)
    file_type: Literal["directory"] = "directory"


class PublicationRootPolicy(RuntimeContractModel):
    contract: Literal["rquant-paper-publication-root-policy/v1"] = (
        "rquant-paper-publication-root-policy/v1"
    )
    profile: PublicationProfile
    publication_root: Path
    trusted_base: Path | None = None
    owner_uid: int = Field(ge=0)
    group_gid: int = Field(ge=0)
    reader_gid: int | None = Field(default=None, ge=0)
    trusted_base_owner_uid: int | None = Field(default=None, ge=0)
    trusted_base_group_gid: int | None = Field(default=None, ge=0)
    trusted_base_mode: int | None = Field(default=None, ge=0, le=0o7777)
    allow_create_generations: bool
    root_mode: int = Field(ge=0, le=0o7777)
    generations_mode: int = Field(ge=0, le=0o7777)
    building_mode: int = Field(ge=0, le=0o7777)
    committed_generation_mode: int = Field(ge=0, le=0o7777)
    object_mode: int = Field(ge=0, le=0o7777)
    manifest_mode: int = Field(ge=0, le=0o7777)
    acl_requirement: Literal[
        "UNOBSERVED_LOCAL_AUDIT",
        "REQUIRE_NO_EXTENDED_ACL",
        "REQUIRE_APPROVED_ACL_DIGEST",
    ]
    approved_acl_digest: Sha256 | None = None

    @field_validator("publication_root", "trusted_base")
    @classmethod
    def validate_absolute_path(cls, value: Path | None) -> Path | None:
        return None if value is None else _absolute_lexical_path(value)

    @model_validator(mode="after")
    def validate_profile(self) -> PublicationRootPolicy:
        if self.profile == "LOCAL_AUDIT":
            expected = (
                self.trusted_base is None
                and self.reader_gid is None
                and self.trusted_base_owner_uid is None
                and self.trusted_base_group_gid is None
                and self.trusted_base_mode is None
                and self.allow_create_generations
                and self.root_mode == 0o700
                and self.generations_mode == 0o700
                and self.building_mode == 0o700
                and self.committed_generation_mode == 0o700
                and self.object_mode == 0o400
                and self.manifest_mode == 0o400
                and self.acl_requirement == "UNOBSERVED_LOCAL_AUDIT"
                and self.approved_acl_digest is None
            )
            if not expected:
                raise ValueError("LOCAL_AUDIT publication policy is not exact")
        else:
            expected = (
                self.trusted_base is not None
                and self.reader_gid is not None
                and self.group_gid == self.reader_gid
                and self.trusted_base_owner_uid is not None
                and self.trusted_base_group_gid is not None
                and self.trusted_base_mode is not None
                and not self.allow_create_generations
                and self.root_mode == 0o750
                and self.generations_mode == 0o750
                and self.building_mode == 0o700
                and self.committed_generation_mode == 0o750
                and self.object_mode == 0o440
                and self.manifest_mode == 0o440
                and self.acl_requirement != "UNOBSERVED_LOCAL_AUDIT"
            )
            if not expected:
                raise ValueError("SEPARATED_IDENTITY publication policy is not exact")
            if (self.acl_requirement == "REQUIRE_APPROVED_ACL_DIGEST") != (
                self.approved_acl_digest is not None
            ):
                raise ValueError("SEPARATED_IDENTITY ACL digest policy is inconsistent")
            assert self.trusted_base is not None
            if self.publication_root.parent != self.trusted_base:
                raise ValueError("publication root must be a direct trusted-base child")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def policy_id(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"policy_id"}))


class PublicationCapabilityObservation(RuntimeContractModel):
    platform: Literal["darwin", "linux"]
    o_directory: Literal[True] = True
    o_nofollow: Literal[True] = True
    dir_fd_open: Literal[True] = True
    dir_fd_stat: Literal[True] = True
    dir_fd_rename: Literal[True] = True
    dir_fd_unlink: Literal[True] = True
    no_replace_primitive: Literal[
        "renameatx_np/RENAME_EXCL",
        "renameat2/RENAME_NOREPLACE",
    ]
    file_fsync: Literal[True] = True
    directory_fsync: Literal[True] = True
    acl_state: Literal[
        "UNOBSERVED_LOCAL_AUDIT",
        "NO_EXTENDED_ACL",
        "APPROVED_ACL_DIGEST",
    ]
    acl_digest: Sha256 | None = None


class PublicationRootObservation(RuntimeContractModel):
    policy_id: Sha256
    effective_uid: int = Field(ge=0)
    effective_gid: int = Field(ge=0)
    root: PublicationStableDirectoryIdentity
    generations: PublicationStableDirectoryIdentity
    capabilities: PublicationCapabilityObservation


class PaperMigrationPublicationManifest(RuntimeContractModel):
    contract: Literal["rquant-paper-migration-publication-manifest/v1"] = (
        "rquant-paper-migration-publication-manifest/v1"
    )
    policy_profile: PublicationProfile
    root_observation: PublicationRootObservation
    publication_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_name: str = Field(pattern=r"^generation-[0-9a-f]{64}$")
    generation_identity: PublicationStableDirectoryIdentity
    object_name: str = Field(pattern=r"^ledger-[0-9a-f]{64}\.sqlite3$")
    object_identity: PublicationFileIdentity
    candidate_sha256: Sha256
    source_sha256: Sha256
    v4_reconciliation_report_digest: Sha256
    migration_attestation_digest: Sha256
    migration_code_identity: str = Field(min_length=1)
    migration_algorithm_id: Literal["paper-ledger-v4-to-v5-archive-v2"]
    target_schema_identity: str = Field(min_length=1)
    target_schema_version: Literal[5] = 5
    target_internal_migration_version: Literal[4] = 4
    inventory: tuple[str, str]

    @model_validator(mode="after")
    def validate_graph(self) -> PaperMigrationPublicationManifest:
        if self.generation_name != f"generation-{self.publication_nonce}":
            raise ValueError("generation name does not match publication nonce")
        if self.object_name != f"ledger-{self.candidate_sha256}.sqlite3":
            raise ValueError("object name does not match candidate digest")
        if self.inventory != (self.object_name, _MANIFEST_NAME):
            raise ValueError("publication inventory is not exact")
        return self


class PaperMigrationPublicationReceipt(RuntimeContractModel):
    contract: Literal["rquant-paper-migration-publication-receipt/v1"] = (
        "rquant-paper-migration-publication-receipt/v1"
    )
    publication_state: Literal["GENERATION_DURABLE_VERIFIED"] = "GENERATION_DURABLE_VERIFIED"
    manifest: PaperMigrationPublicationManifest
    manifest_sha256: Sha256
    manifest_identity: PublicationFileIdentity

    @computed_field  # type: ignore[prop-decorator]
    @property
    def receipt_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"receipt_sha256"}))


class PaperOfflineMigrationResult(RuntimeContractModel):
    contract: Literal["rquant-paper-offline-migration-result/v2"] = (
        "rquant-paper-offline-migration-result/v2"
    )
    publication_state: Literal["GENERATION_DURABLE_VERIFIED"] = "GENERATION_DURABLE_VERIFIED"
    publication: PaperMigrationPublicationReceipt
    v4_report: V4LedgerReconciliationReport
    anchor_state: Literal["CURRENT_HEAD_UNANCHORED"] = "CURRENT_HEAD_UNANCHORED"
    promotion_allowed: Literal[False] = False

    @model_validator(mode="after")
    def validate_report_binding(self) -> PaperOfflineMigrationResult:
        manifest = self.publication.manifest
        if (
            not self.v4_report.is_verified
            or self.v4_report.digest != manifest.v4_reconciliation_report_digest
            or self.v4_report.source_sha256 != manifest.source_sha256
        ):
            raise ValueError("migration result does not bind the v4 report")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def reconciliation_verified(self) -> bool:
        return self.v4_report.is_verified


class PaperMigrationOrphanState(RuntimeContractModel):
    contract: Literal["rquant-paper-migration-orphan/v1"] = "rquant-paper-migration-orphan/v1"
    outcome: Literal["PRE_RENAME_ORPHANED"] = "PRE_RENAME_ORPHANED"
    policy_id: Sha256
    publication_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    building_name: str = Field(pattern=r"^\.building-[0-9a-f]{64}$")
    failed_phase: str = Field(min_length=1)


class PaperMigrationPostCommitState(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")
    contract: Literal["rquant-paper-migration-post-commit/v2"] = (
        "rquant-paper-migration-post-commit/v2"
    )
    outcome: Literal["POST_COMMIT_INDETERMINATE"] = "POST_COMMIT_INDETERMINATE"
    publication_state: Literal["GENERATION_RENAMED_UNCONFIRMED"] = "GENERATION_RENAMED_UNCONFIRMED"
    reason: Literal[
        "SOURCE_PARENT_FSYNC_FAILED",
        "GENERATIONS_FSYNC_FAILED",
        "FINAL_ROOT_POLICY_FAILED",
        "FINAL_INVENTORY_FAILED",
        "FINAL_MANIFEST_FAILED",
        "FINAL_OBJECT_FAILED",
        "RESULT_ASSEMBLY_FAILED",
        "FAULT_INJECTED",
    ]
    policy_id: Sha256
    publication_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    building_name: str = Field(pattern=r"^\.building-[0-9a-f]{64}$")
    generation_name: str = Field(pattern=r"^generation-[0-9a-f]{64}$")
    object_name: str = Field(pattern=r"^ledger-[0-9a-f]{64}\.sqlite3$")
    candidate_sha256: Sha256
    expected_manifest_sha256: Sha256
    building_identity_before_generation_rename: _PublicationFullDirectoryIdentityV2
    building_identity: _PublicationFullDirectoryIdentityV2

    @model_validator(mode="after")
    def validate_names_and_transition(self) -> PaperMigrationPostCommitState:
        if self.building_name != f".building-{self.publication_nonce}":
            raise ValueError("building name does not match publication nonce")
        if self.generation_name != f"generation-{self.publication_nonce}":
            raise ValueError("generation name does not match publication nonce")
        if self.object_name != f"ledger-{self.candidate_sha256}.sqlite3":
            raise ValueError("object name does not match candidate digest")
        before = self.building_identity_before_generation_rename
        after = self.building_identity
        if (before.device, before.inode, before.uid, before.gid, before.mode, before.file_type) != (
            after.device,
            after.inode,
            after.uid,
            after.gid,
            after.mode,
            after.file_type,
        ):
            raise ValueError("building identity stable fields changed across rename")
        return self


class _PaperMigrationAuditReceiptFactsV2(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")
    contract: Literal["rquant-paper-migration-audit-receipt-facts/v2"] = (
        "rquant-paper-migration-audit-receipt-facts/v2"
    )
    receipt_sha256: Sha256
    manifest_sha256: Sha256
    manifest_identity: PublicationFileIdentity
    policy_id: Sha256
    policy_profile: PublicationProfile
    publication_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_name: str = Field(pattern=r"^generation-[0-9a-f]{64}$")
    object_name: str = Field(pattern=r"^ledger-[0-9a-f]{64}\.sqlite3$")
    object_identity: PublicationFileIdentity
    candidate_sha256: Sha256
    source_sha256: Sha256
    v4_reconciliation_report_digest: Sha256
    migration_attestation_digest: Sha256
    migration_code_identity: str = Field(min_length=1)
    migration_algorithm_id: Literal["paper-ledger-v4-to-v5-archive-v2"]
    target_schema_identity: str = Field(min_length=1)
    target_schema_version: Literal[5] = 5
    target_internal_migration_version: Literal[4] = 4

    @model_validator(mode="after")
    def validate_names(self) -> _PaperMigrationAuditReceiptFactsV2:
        if self.generation_name != f"generation-{self.publication_nonce}":
            raise ValueError("generation name does not match receipt nonce")
        if self.object_name != f"ledger-{self.candidate_sha256}.sqlite3":
            raise ValueError("object name does not match receipt digest")
        return self


class _PaperMigrationAuditVerificationEvidenceV2(_PaperMigrationAuditReceiptFactsV2):
    contract: Literal["rquant-paper-migration-audit-verification/v2"] = (
        "rquant-paper-migration-audit-verification/v2"
    )
    sqlite_integrity: Literal["ok"] = "ok"


class PaperMigrationAuditMaterialization(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")
    contract: Literal["rquant-paper-migration-audit-materialization/v2"] = (
        "rquant-paper-migration-audit-materialization/v2"
    )
    staging_root: Path
    staging_root_identity: _PublicationFullDirectoryIdentityV2
    private_name: str = Field(pattern=r"^paper-migration-audit-[0-9a-f]{64}-[0-9a-f]{64}\.sqlite3$")
    private_nonce: str = Field(pattern=r"^[0-9a-f]{64}$")
    private_path: Path
    receipt: _PaperMigrationAuditReceiptFactsV2
    receipt_sha256: Sha256
    source_sha256: Sha256
    materialized_sha256: Sha256
    materialized_size: int = Field(ge=0)
    private_identity: PublicationFileIdentity
    verification: _PaperMigrationAuditVerificationEvidenceV2

    @field_validator("staging_root", "private_path")
    @classmethod
    def validate_absolute_lexical_path(cls, value: Path) -> Path:
        return _absolute_lexical_path(value)

    @model_validator(mode="after")
    def validate_binding(self) -> PaperMigrationAuditMaterialization:
        if self.staging_root == Path("/"):
            raise ValueError("staging root cannot be filesystem root")
        if "/" in self.private_name or "\\" in self.private_name or ".." in self.private_name:
            raise ValueError("private name contains a forbidden component")
        expected_name = (
            f"paper-migration-audit-{self.receipt.publication_nonce}-{self.private_nonce}.sqlite3"
        )
        if self.private_name != expected_name:
            raise ValueError("private name does not bind receipt and private nonces")
        if (
            self.private_path.parent != self.staging_root
            or self.private_path.name != self.private_name
        ):
            raise ValueError("private path does not bind staging root and private name")
        if self.materialized_size != self.private_identity.size:
            raise ValueError("materialized size does not match private identity")
        if self.private_identity.mode != 0o600:
            raise ValueError("private identity mode is not 0600")
        if self.staging_root_identity.mode != 0o700:
            raise ValueError("staging root identity mode is not 0700")
        if (
            self.receipt_sha256 != self.receipt.receipt_sha256
            or self.source_sha256 != self.receipt.source_sha256
            or self.materialized_sha256 != self.receipt.candidate_sha256
        ):
            raise ValueError("top-level audit hashes do not match receipt facts")
        if self.verification.model_dump(
            mode="python", exclude={"contract", "sqlite_integrity"}
        ) != (self.receipt.model_dump(mode="python", exclude={"contract"})):
            raise ValueError("verification evidence does not exactly match receipt facts")
        return self


class PaperMigrationMaterializationOrphanState(RuntimeContractModel):
    contract: Literal["rquant-paper-migration-materialization-orphan/v1"] = (
        "rquant-paper-migration-materialization-orphan/v1"
    )
    outcome: Literal["AUDIT_MATERIALIZATION_ORPHANED"] = "AUDIT_MATERIALIZATION_ORPHANED"
    receipt_sha256: Sha256
    private_name: str = Field(pattern=r"^paper-migration-audit-[0-9a-f]{64}-[0-9a-f]{64}\.sqlite3$")
    failed_phase: str = Field(min_length=1)


class PaperMigrationPreCommitError(RuntimeError):
    def __init__(self, message: str, *, orphan: PaperMigrationOrphanState | None = None) -> None:
        super().__init__(message)
        self.orphan = orphan


class PaperMigrationPostCommitIndeterminateError(RuntimeError):
    def __init__(self, message: str, *, state: PaperMigrationPostCommitState) -> None:
        super().__init__(message)
        self.state = state


class PaperMigrationMaterializationError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        orphan: PaperMigrationMaterializationOrphanState | None = None,
    ) -> None:
        super().__init__(message)
        self.orphan = orphan


AclObserver: TypeAlias = Callable[[Path], tuple[str, str | None]]


@dataclass
class _PublicationRootHandle:
    policy: PublicationRootPolicy
    observation: PublicationRootObservation
    root_fd: int
    generations_fd: int

    def close(self) -> None:
        os.close(self.generations_fd)
        os.close(self.root_fd)


@dataclass
class _PaperMigrationPublicationContext:
    root: _PublicationRootHandle
    publication_nonce: str
    building_name: str
    generation_name: str
    building_fd: int
    ready_fd: int
    building_path: Path
    ready_path: Path
    building_identity_before_generation_rename: _PublicationFullDirectoryIdentityV2 | None = None
    building_identity: _PublicationFullDirectoryIdentityV2 | None = None

    def close(self) -> None:
        os.close(self.ready_fd)
        os.close(self.building_fd)
        self.root.close()


def local_audit_publication_root_policy(publication_root: Path) -> PublicationRootPolicy:
    return PublicationRootPolicy(
        profile="LOCAL_AUDIT",
        publication_root=_absolute_lexical_path(publication_root),
        owner_uid=os.geteuid(),
        group_gid=os.getegid(),
        allow_create_generations=True,
        root_mode=0o700,
        generations_mode=0o700,
        building_mode=0o700,
        committed_generation_mode=0o700,
        object_mode=0o400,
        manifest_mode=0o400,
        acl_requirement="UNOBSERVED_LOCAL_AUDIT",
    )


def _directory_identity(metadata: os.stat_result) -> PublicationStableDirectoryIdentity:
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("publication entry must be a directory")
    return PublicationStableDirectoryIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        uid=metadata.st_uid,
        gid=metadata.st_gid,
        mode=stat.S_IMODE(metadata.st_mode),
    )


def _full_directory_identity(metadata: os.stat_result) -> _PublicationFullDirectoryIdentityV2:
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("publication entry must be a directory")
    return _PublicationFullDirectoryIdentityV2(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        uid=metadata.st_uid,
        gid=metadata.st_gid,
        mode=stat.S_IMODE(metadata.st_mode),
        nlink=metadata.st_nlink,
        size=metadata.st_size,
        mtime_ns=metadata.st_mtime_ns,
        ctime_ns=metadata.st_ctime_ns,
    )


def _file_identity(metadata: os.stat_result) -> PublicationFileIdentity:
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ValueError("publication entry must be a singly-linked regular file")
    return PublicationFileIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        uid=metadata.st_uid,
        gid=metadata.st_gid,
        mode=stat.S_IMODE(metadata.st_mode),
        nlink=1,
        size=metadata.st_size,
        mtime_ns=metadata.st_mtime_ns,
        ctime_ns=metadata.st_ctime_ns,
    )


def _same_file_object(
    first: PublicationFileIdentity,
    second: PublicationFileIdentity,
) -> bool:
    return (
        first.device,
        first.inode,
        first.uid,
        first.gid,
        first.mode,
        first.nlink,
        first.size,
        first.mtime_ns,
        first.ctime_ns,
    ) == (
        second.device,
        second.inode,
        second.uid,
        second.gid,
        second.mode,
        second.nlink,
        second.size,
        second.mtime_ns,
        second.ctime_ns,
    )


def _same_file_object_except_rename_ctime(
    first: PublicationFileIdentity,
    second: PublicationFileIdentity,
) -> bool:
    """Compare a native no-replace rename handoff before the metadata transition."""

    return (
        first.device,
        first.inode,
        first.uid,
        first.gid,
        first.mode,
        first.nlink,
        first.size,
        first.mtime_ns,
    ) == (
        second.device,
        second.inode,
        second.uid,
        second.gid,
        second.mode,
        second.nlink,
        second.size,
        second.mtime_ns,
    )


def _required_open_flags(*, directory: bool, writable: bool = False) -> int:
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise OSError("required no-follow directory flags are unavailable")
    flags = os.O_RDWR if writable else os.O_RDONLY
    flags |= os.O_NOFOLLOW
    if directory:
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return flags


def _open_absolute_directory(path: Path) -> int:
    path = _absolute_lexical_path(path)
    descriptor = os.open("/", _required_open_flags(directory=True))
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(
                component,
                _required_open_flags(directory=True),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _platform_capability(
    profile: PublicationProfile,
    *,
    acl_state: str,
    acl_digest: str | None,
) -> PublicationCapabilityObservation:
    platform, primitive = _preflight_platform_primitives()
    return PublicationCapabilityObservation(
        platform=platform,
        no_replace_primitive=primitive,
        acl_state=acl_state,
        acl_digest=acl_digest,
    )


def _preflight_platform_primitives() -> tuple[str, str]:
    primitive = rename_noreplace_capability()
    if sys.platform == "darwin":
        platform = "darwin"
    elif sys.platform.startswith("linux"):
        platform = "linux"
    else:
        raise OSError("paper migration publication is unsupported on this platform")
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise OSError("required publication flags are unavailable")
    required_dir_fd_operations = (os.open, os.stat, os.rename, os.unlink)
    if any(operation not in os.supports_dir_fd for operation in required_dir_fd_operations):
        raise OSError("required publication dir_fd operations are unavailable")
    return platform, primitive


def _validate_directory(
    identity: PublicationStableDirectoryIdentity,
    *,
    uid: int,
    gid: int,
    mode: int,
    label: str,
) -> None:
    if (identity.uid, identity.gid, identity.mode) != (uid, gid, mode):
        raise ValueError(f"{label} identity or mode does not match publication policy")


def _validate_full_directory(
    identity: _PublicationFullDirectoryIdentityV2,
    *,
    uid: int,
    gid: int,
    mode: int,
    label: str,
) -> None:
    if (identity.uid, identity.gid, identity.mode) != (uid, gid, mode):
        raise ValueError(f"{label} identity or mode does not match publication policy")


def _validate_file(
    identity: PublicationFileIdentity,
    *,
    uid: int,
    gid: int,
    mode: int,
    label: str,
) -> None:
    if (identity.uid, identity.gid, identity.mode) != (uid, gid, mode):
        raise ValueError(f"{label} identity or mode does not match publication policy")


def observe_publication_root(
    policy: PublicationRootPolicy,
    *,
    create_generations: bool,
    acl_observer: AclObserver | None = None,
) -> _PublicationRootHandle:
    _preflight_platform_primitives()
    if os.geteuid() != policy.owner_uid:
        raise ValueError("effective UID does not match publication policy")
    if policy.profile == "LOCAL_AUDIT" and os.getegid() != policy.group_gid:
        raise ValueError("effective GID does not match LOCAL_AUDIT policy")

    trusted_base_fd: int | None = None
    root_fd: int | None = None
    generations_fd: int | None = None
    try:
        if policy.profile == "SEPARATED_IDENTITY":
            if acl_observer is None:
                raise ValueError("SEPARATED_IDENTITY requires an ACL observer")
            assert policy.trusted_base is not None
            trusted_base_fd = _open_absolute_directory(policy.trusted_base)
            trusted_identity = _directory_identity(os.fstat(trusted_base_fd))
            _validate_directory(
                trusted_identity,
                uid=int(policy.trusted_base_owner_uid),
                gid=int(policy.trusted_base_group_gid),
                mode=int(policy.trusted_base_mode),
                label="trusted base",
            )
        if trusted_base_fd is None:
            root_fd = _open_absolute_directory(policy.publication_root)
        else:
            root_fd = os.open(
                policy.publication_root.name,
                _required_open_flags(directory=True),
                dir_fd=trusted_base_fd,
            )
        root_identity = _directory_identity(os.fstat(root_fd))
        _validate_directory(
            root_identity,
            uid=policy.owner_uid,
            gid=policy.group_gid,
            mode=policy.root_mode,
            label="publication root",
        )
        try:
            generations_fd = os.open(
                "generations",
                _required_open_flags(directory=True),
                dir_fd=root_fd,
            )
        except FileNotFoundError:
            if (
                not create_generations
                or not policy.allow_create_generations
                or policy.profile != "LOCAL_AUDIT"
            ):
                raise ValueError(
                    "publication generations directory must be pre-provisioned"
                ) from None
            os.mkdir("generations", policy.generations_mode, dir_fd=root_fd)
            generations_fd = os.open(
                "generations",
                _required_open_flags(directory=True),
                dir_fd=root_fd,
            )
            os.fchmod(generations_fd, policy.generations_mode)
            os.fsync(root_fd)
        generations_identity = _directory_identity(os.fstat(generations_fd))
        _validate_directory(
            generations_identity,
            uid=policy.owner_uid,
            gid=policy.group_gid,
            mode=policy.generations_mode,
            label="publication generations",
        )
        if policy.profile == "LOCAL_AUDIT":
            acl_state, acl_digest = "UNOBSERVED_LOCAL_AUDIT", None
        else:
            assert acl_observer is not None
            acl_state, acl_digest = acl_observer(policy.publication_root)
            if policy.acl_requirement == "REQUIRE_NO_EXTENDED_ACL":
                if acl_state != "NO_EXTENDED_ACL" or acl_digest is not None:
                    raise ValueError("publication root has an unacceptable extended ACL")
            elif acl_state != "APPROVED_ACL_DIGEST" or acl_digest != policy.approved_acl_digest:
                raise ValueError("publication root ACL digest is not approved")
            groups = set(os.getgroups()) | {os.getegid()}
            if policy.reader_gid not in groups:
                raise ValueError("publisher is not a member of the reader group")
        capabilities = _platform_capability(
            policy.profile,
            acl_state=acl_state,
            acl_digest=acl_digest,
        )
        observation = PublicationRootObservation(
            policy_id=policy.policy_id,
            effective_uid=os.geteuid(),
            effective_gid=os.getegid(),
            root=root_identity,
            generations=generations_identity,
            capabilities=capabilities,
        )
        return _PublicationRootHandle(policy, observation, root_fd, generations_fd)
    except BaseException:
        if generations_fd is not None:
            os.close(generations_fd)
        if root_fd is not None:
            os.close(root_fd)
        raise
    finally:
        if trusted_base_fd is not None:
            os.close(trusted_base_fd)


def _begin_paper_migration_publication(
    publication_root: Path,
    *,
    root_policy: PublicationRootPolicy,
    publication_nonce: str | None = None,
    acl_observer: AclObserver | None = None,
) -> _PaperMigrationPublicationContext:
    if _absolute_lexical_path(publication_root) != root_policy.publication_root:
        raise ValueError("publication root and root policy disagree lexically")
    root = observe_publication_root(
        root_policy,
        create_generations=True,
        acl_observer=acl_observer,
    )
    nonce = publication_nonce or secrets.token_hex(32)
    if len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce):
        root.close()
        raise ValueError("publication nonce must be 64 lowercase hexadecimal characters")
    building_name = f".building-{nonce}"
    generation_name = f"generation-{nonce}"
    building_fd: int | None = None
    ready_fd: int | None = None
    building_visible = False
    try:
        os.mkdir(building_name, root_policy.building_mode, dir_fd=root.generations_fd)
        building_visible = True
        building_fd = os.open(
            building_name,
            _required_open_flags(directory=True),
            dir_fd=root.generations_fd,
        )
        if root_policy.profile == "SEPARATED_IDENTITY":
            assert root_policy.reader_gid is not None
            os.fchown(building_fd, root_policy.owner_uid, root_policy.reader_gid)
        os.fchmod(building_fd, root_policy.building_mode)
        _validate_directory(
            _directory_identity(os.fstat(building_fd)),
            uid=root_policy.owner_uid,
            gid=root_policy.group_gid,
            mode=root_policy.building_mode,
            label="publication building directory",
        )
        os.mkdir("ready", root_policy.building_mode, dir_fd=building_fd)
        ready_fd = os.open(
            "ready",
            _required_open_flags(directory=True),
            dir_fd=building_fd,
        )
        if root_policy.profile == "SEPARATED_IDENTITY":
            assert root_policy.reader_gid is not None
            os.fchown(ready_fd, root_policy.owner_uid, root_policy.reader_gid)
        os.fchmod(ready_fd, root_policy.building_mode)
        return _PaperMigrationPublicationContext(
            root=root,
            publication_nonce=nonce,
            building_name=building_name,
            generation_name=generation_name,
            building_fd=building_fd,
            ready_fd=ready_fd,
            building_path=root_policy.publication_root / "generations" / building_name,
            ready_path=(root_policy.publication_root / "generations" / building_name / "ready"),
        )
    except BaseException as exc:
        if ready_fd is not None:
            os.close(ready_fd)
        if building_fd is not None:
            os.close(building_fd)
        root.close()
        if building_visible:
            raise PaperMigrationPreCommitError(
                "paper migration workspace creation failed",
                orphan=PaperMigrationOrphanState(
                    policy_id=root_policy.policy_id,
                    publication_nonce=nonce,
                    building_name=building_name,
                    failed_phase="workspace_creation",
                ),
            ) from exc
        raise


def canonical_manifest_bytes(manifest: PaperMigrationPublicationManifest) -> bytes:
    return (
        json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def parse_canonical_manifest(raw: bytes) -> PaperMigrationPublicationManifest:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("publication manifest contains duplicate keys")
            result[key] = value
        return result

    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicates)
        manifest = PaperMigrationPublicationManifest.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        if "duplicate keys" in str(exc):
            raise ValueError("publication manifest contains duplicate keys") from exc
        raise ValueError("publication manifest is invalid") from exc
    if canonical_manifest_bytes(manifest) != raw:
        raise ValueError("publication manifest bytes are not canonical")
    return manifest


def _inventory(directory_fd: int) -> tuple[str, ...]:
    return tuple(sorted(os.listdir(directory_fd)))


def _read_all(descriptor: int) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    while chunk := os.read(descriptor, _COPY_CHUNK_SIZE):
        chunks.append(chunk)
    return b"".join(chunks)


def _hash_fd(descriptor: int) -> tuple[str, int]:
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    size = 0
    while chunk := os.read(descriptor, _COPY_CHUNK_SIZE):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _open_regular_at(directory_fd: int, name: str, *, writable: bool = False) -> int:
    descriptor = os.open(
        name,
        _required_open_flags(directory=False, writable=writable),
        dir_fd=directory_fd,
    )
    try:
        _file_identity(os.fstat(descriptor))
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _checkpoint(failure_after_phase: str | None, phase: str) -> None:
    if failure_after_phase == phase:
        raise RuntimeError(f"simulated paper migration failure at {phase}")


def _post_commit_state(
    context: _PaperMigrationPublicationContext,
    *,
    object_name: str,
    candidate_sha256: str,
    manifest_sha256: str,
    reason: str,
) -> PaperMigrationPostCommitState:
    if (
        context.building_identity_before_generation_rename is None
        or context.building_identity is None
    ):
        raise ValueError("post-commit state has no retained building bindings")
    return PaperMigrationPostCommitState(
        reason=reason,
        policy_id=context.root.policy.policy_id,
        publication_nonce=context.publication_nonce,
        building_name=context.building_name,
        generation_name=context.generation_name,
        object_name=object_name,
        candidate_sha256=candidate_sha256,
        expected_manifest_sha256=manifest_sha256,
        building_identity_before_generation_rename=(
            context.building_identity_before_generation_rename
        ),
        building_identity=context.building_identity,
    )


def _validate_receipt_from_generation(
    root: _PublicationRootHandle,
    *,
    generation_name: str,
    expected_receipt: PaperMigrationPublicationReceipt | None = None,
    expected_manifest_sha256: str | None = None,
    expected_object_name: str | None = None,
    expected_candidate_sha256: str | None = None,
    failure_after_phase: str | None = None,
) -> tuple[PaperMigrationPublicationReceipt, int]:
    generation_fd = os.open(
        generation_name,
        _required_open_flags(directory=True),
        dir_fd=root.generations_fd,
    )
    manifest_fd: int | None = None
    object_fd: int | None = None
    try:
        generation_identity = _directory_identity(os.fstat(generation_fd))
        _validate_directory(
            generation_identity,
            uid=root.policy.owner_uid,
            gid=root.policy.group_gid,
            mode=root.policy.committed_generation_mode,
            label="committed generation",
        )
        inventory = _inventory(generation_fd)
        if len(inventory) != 2 or _MANIFEST_NAME not in inventory:
            raise ValueError("committed generation inventory is not exact")
        manifest_fd = _open_regular_at(generation_fd, _MANIFEST_NAME)
        manifest_pre = _file_identity(os.fstat(manifest_fd))
        _validate_file(
            manifest_pre,
            uid=root.policy.owner_uid,
            gid=root.policy.group_gid,
            mode=root.policy.manifest_mode,
            label="publication manifest",
        )
        raw_manifest = _read_all(manifest_fd)
        manifest_post = _file_identity(os.fstat(manifest_fd))
        if manifest_pre != manifest_post:
            raise ValueError("publication manifest changed while read")
        manifest = parse_canonical_manifest(raw_manifest)
        manifest_sha256 = hashlib.sha256(raw_manifest).hexdigest()
        if expected_manifest_sha256 is not None and manifest_sha256 != expected_manifest_sha256:
            raise ValueError("publication manifest digest differs")
        if manifest.generation_name != generation_name:
            raise ValueError("publication manifest generation differs")
        if manifest.generation_identity != generation_identity:
            raise ValueError("publication generation identity differs")
        if manifest.root_observation != root.observation:
            raise ValueError("publication root observation differs")
        if manifest.policy_profile != root.policy.profile:
            raise ValueError("publication profile differs")
        if inventory != manifest.inventory:
            raise ValueError("publication manifest inventory differs")
        if expected_object_name is not None and manifest.object_name != expected_object_name:
            raise ValueError("publication object name differs")
        if (
            expected_candidate_sha256 is not None
            and manifest.candidate_sha256 != expected_candidate_sha256
        ):
            raise ValueError("publication object digest differs")
        object_fd = _open_regular_at(generation_fd, manifest.object_name)
        object_pre = _file_identity(os.fstat(object_fd))
        _validate_file(
            object_pre,
            uid=root.policy.owner_uid,
            gid=root.policy.group_gid,
            mode=root.policy.object_mode,
            label="publication object",
        )
        object_sha256, object_size = _hash_fd(object_fd)
        object_post = _file_identity(os.fstat(object_fd))
        if (
            object_pre != object_post
            or object_pre != manifest.object_identity
            or object_sha256 != manifest.candidate_sha256
            or object_size != manifest.object_identity.size
        ):
            raise ValueError("publication object identity or digest differs")
        receipt = PaperMigrationPublicationReceipt(
            manifest=manifest,
            manifest_sha256=manifest_sha256,
            manifest_identity=manifest_pre,
        )
        if expected_receipt is not None and receipt != expected_receipt:
            raise ValueError("publication receipt facts differ")
        _checkpoint(failure_after_phase, "during_final_generation_verify")
        retained_object_fd = object_fd
        object_fd = None
        return receipt, retained_object_fd
    finally:
        if object_fd is not None:
            os.close(object_fd)
        if manifest_fd is not None:
            os.close(manifest_fd)
        os.close(generation_fd)


def _publish_paper_migration_generation(
    context: _PaperMigrationPublicationContext,
    *,
    source_sha256: str,
    transformed_fd: int,
    semantic_binding: _StableSQLiteImageBinding,
    v4_reconciliation_report_digest: str,
    migration_attestation_digest: str,
    migration_code_identity: str,
    migration_algorithm_id: str,
    target_schema_identity: str,
    target_schema_version: int,
    target_internal_migration_version: int,
    failure_after_phase: str | None,
) -> PaperMigrationPublicationReceipt:
    policy = context.root.policy
    if _inventory(context.ready_fd) != ("transformed.sqlite3",):
        raise ValueError("ready publication inventory is not exact")
    manifest_fd: int | None = None
    object_fd: int | None = None
    renamed = False
    object_name = ""
    candidate_sha256 = semantic_binding.sha256
    candidate_digest = semantic_binding.sha256
    manifest_sha256 = "0" * 64
    try:
        transformed_pre = _file_identity(os.fstat(transformed_fd))
        _validate_file(
            transformed_pre,
            uid=policy.owner_uid,
            gid=context.root.observation.effective_gid,
            mode=0o600,
            label="transformed publication object",
        )
        _revalidate_stable_sqlite_image(transformed_fd, semantic_binding)
        os.fsync(transformed_fd)
        _revalidate_stable_sqlite_image(transformed_fd, semantic_binding)
        transformed_post = _file_identity(os.fstat(transformed_fd))
        if transformed_pre != transformed_post or transformed_pre != semantic_binding.identity:
            raise ValueError("transformed publication object changed while verified")
        _checkpoint(failure_after_phase, "after_publication_object_first_hash")
        object_name = f"ledger-{candidate_sha256}.sqlite3"
        rename_noreplace_at(
            context.ready_fd,
            "transformed.sqlite3",
            context.ready_fd,
            object_name,
        )
        _checkpoint(failure_after_phase, "after_object_noreplace_rename")
        object_fd = _open_regular_at(context.ready_fd, object_name, writable=True)
        rebound_pre = _file_identity(os.fstat(object_fd))
        rebound_digest, rebound_size = _hash_fd(object_fd)
        rebound_post = _file_identity(os.fstat(object_fd))
        if (
            rebound_pre != rebound_post
            or not _same_file_object_except_rename_ctime(rebound_pre, semantic_binding.identity)
            or rebound_digest != candidate_sha256
            or rebound_size != semantic_binding.identity.size
        ):
            raise ValueError("publication object identity changed after no-replace rename")
        _checkpoint(failure_after_phase, "after_object_rebind_before_metadata")
        if policy.profile == "SEPARATED_IDENTITY":
            assert policy.reader_gid is not None
            os.fchown(object_fd, policy.owner_uid, policy.reader_gid)
        os.fchmod(object_fd, policy.object_mode)
        final_pre = _file_identity(os.fstat(object_fd))
        final_digest, final_size = _hash_fd(object_fd)
        final_post = _file_identity(os.fstat(object_fd))
        if (
            final_pre != final_post
            or final_digest != candidate_sha256
            or final_size != semantic_binding.identity.size
            or (final_pre.device, final_pre.inode)
            != (semantic_binding.identity.device, semantic_binding.identity.inode)
        ):
            raise ValueError("publication object changed during final metadata transition")
        object_identity = final_post
        _validate_file(
            object_identity,
            uid=policy.owner_uid,
            gid=policy.group_gid,
            mode=policy.object_mode,
            label="publication object",
        )
        final_rebind_fd = _open_regular_at(context.ready_fd, object_name)
        try:
            final_rebind_pre = _file_identity(os.fstat(final_rebind_fd))
            final_rebind_digest, final_rebind_size = _hash_fd(final_rebind_fd)
            final_rebind_post = _file_identity(os.fstat(final_rebind_fd))
            if (
                final_rebind_pre != final_rebind_post
                or final_rebind_pre != object_identity
                or final_rebind_digest != candidate_sha256
                or final_rebind_size != semantic_binding.identity.size
            ):
                raise ValueError("publication final object binding differs")
        finally:
            os.close(final_rebind_fd)
        if policy.profile == "SEPARATED_IDENTITY":
            assert policy.reader_gid is not None
            os.fchown(context.ready_fd, policy.owner_uid, policy.reader_gid)
        os.fchmod(context.ready_fd, policy.committed_generation_mode)
        generation_identity = _directory_identity(os.fstat(context.ready_fd))
        _validate_directory(
            generation_identity,
            uid=policy.owner_uid,
            gid=policy.group_gid,
            mode=policy.committed_generation_mode,
            label="committed generation",
        )
        manifest = PaperMigrationPublicationManifest(
            policy_profile=policy.profile,
            root_observation=context.root.observation,
            publication_nonce=context.publication_nonce,
            generation_name=context.generation_name,
            generation_identity=generation_identity,
            object_name=object_name,
            object_identity=object_identity,
            candidate_sha256=candidate_sha256,
            source_sha256=source_sha256,
            v4_reconciliation_report_digest=v4_reconciliation_report_digest,
            migration_attestation_digest=migration_attestation_digest,
            migration_code_identity=migration_code_identity,
            migration_algorithm_id=migration_algorithm_id,
            target_schema_identity=target_schema_identity,
            target_schema_version=target_schema_version,
            target_internal_migration_version=target_internal_migration_version,
            inventory=(object_name, _MANIFEST_NAME),
        )
        raw_manifest = canonical_manifest_bytes(manifest)
        manifest_sha256 = hashlib.sha256(raw_manifest).hexdigest()
        manifest_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            manifest_flags |= os.O_CLOEXEC
        manifest_fd = os.open(
            _MANIFEST_NAME,
            manifest_flags,
            policy.manifest_mode,
            dir_fd=context.ready_fd,
        )
        offset = 0
        while offset < len(raw_manifest):
            written = os.write(manifest_fd, raw_manifest[offset:])
            if written <= 0:
                raise OSError("short write while writing publication manifest")
            offset += written
        if policy.profile == "SEPARATED_IDENTITY":
            assert policy.reader_gid is not None
            os.fchown(manifest_fd, policy.owner_uid, policy.reader_gid)
        os.fchmod(manifest_fd, policy.manifest_mode)
        os.fsync(manifest_fd)
        _checkpoint(failure_after_phase, "after_manifest_fsync")
        manifest_identity = _file_identity(os.fstat(manifest_fd))
        _validate_file(
            manifest_identity,
            uid=policy.owner_uid,
            gid=policy.group_gid,
            mode=policy.manifest_mode,
            label="publication manifest",
        )
        if _directory_identity(os.fstat(context.ready_fd)) != generation_identity:
            raise ValueError("ready generation identity changed before publication")
        if _inventory(context.ready_fd) != (object_name, _MANIFEST_NAME):
            raise ValueError("ready generation inventory is not exact")
        os.fsync(object_fd)
        os.fsync(manifest_fd)
        os.fsync(context.ready_fd)
        if _file_identity(os.fstat(object_fd)) != object_identity:
            raise ValueError("publication object identity changed before publication")
        if _file_identity(os.fstat(manifest_fd)) != manifest_identity:
            raise ValueError("publication manifest identity changed before publication")
        if _directory_identity(os.fstat(context.ready_fd)) != generation_identity:
            raise ValueError("ready generation identity changed before publication")
        _checkpoint(failure_after_phase, "before_local_failure_disposition")
        _checkpoint(failure_after_phase, "before_generation_noreplace_rename")

        context.building_identity_before_generation_rename = _full_directory_identity(
            os.fstat(context.building_fd)
        )
        _validate_full_directory(
            context.building_identity_before_generation_rename,
            uid=policy.owner_uid,
            gid=policy.group_gid,
            mode=policy.building_mode,
            label="publication building directory",
        )

        def mark_generation_renamed() -> None:
            nonlocal renamed
            renamed = True
            context.building_identity = _full_directory_identity(os.fstat(context.building_fd))
            _validate_full_directory(
                context.building_identity,
                uid=policy.owner_uid,
                gid=policy.group_gid,
                mode=policy.building_mode,
                label="publication building directory",
            )

        rename_noreplace_at(
            context.building_fd,
            "ready",
            context.root.generations_fd,
            context.generation_name,
            on_success=mark_generation_renamed,
        )
        _checkpoint(failure_after_phase, "after_generation_rename_before_parent_fsync")
        try:
            os.fsync(context.building_fd)
        except BaseException as exc:
            state = _post_commit_state(
                context,
                object_name=object_name,
                candidate_sha256=candidate_sha256,
                manifest_sha256=manifest_sha256,
                reason="SOURCE_PARENT_FSYNC_FAILED",
            )
            raise PaperMigrationPostCommitIndeterminateError(
                "renamed generation source-parent fsync failed", state=state
            ) from exc
        try:
            os.fsync(context.root.generations_fd)
        except BaseException as exc:
            state = _post_commit_state(
                context,
                object_name=object_name,
                candidate_sha256=candidate_sha256,
                manifest_sha256=manifest_sha256,
                reason="GENERATIONS_FSYNC_FAILED",
            )
            raise PaperMigrationPostCommitIndeterminateError(
                "renamed generation destination-parent fsync failed", state=state
            ) from exc
        _checkpoint(failure_after_phase, "after_parent_fsync_before_final_verify")
        final_root = observe_publication_root(policy, create_generations=False)
        try:
            if final_root.observation != context.root.observation:
                raise ValueError("final publication root observation differs")
        finally:
            final_root.close()
        receipt, object_fd = _validate_receipt_from_generation(
            context.root,
            generation_name=context.generation_name,
            expected_manifest_sha256=manifest_sha256,
            expected_object_name=object_name,
            expected_candidate_sha256=candidate_sha256,
            failure_after_phase=failure_after_phase,
        )
        os.close(object_fd)
        object_fd = None
        _checkpoint(failure_after_phase, "before_result_assembly")
        if receipt.manifest_identity != manifest_identity:
            raise ValueError("final publication manifest identity differs")
        return receipt
    except PaperMigrationPostCommitIndeterminateError:
        raise
    except BaseException as exc:
        if renamed:
            if isinstance(exc, RuntimeError):
                reason = "FAULT_INJECTED"
            elif "root" in str(exc):
                reason = "FINAL_ROOT_POLICY_FAILED"
            elif "inventory" in str(exc) or "generation identity" in str(exc):
                reason = "FINAL_INVENTORY_FAILED"
            elif "manifest" in str(exc):
                reason = "FINAL_MANIFEST_FAILED"
            else:
                reason = "FINAL_OBJECT_FAILED"
            state = _post_commit_state(
                context,
                object_name=object_name,
                candidate_sha256=candidate_digest,
                manifest_sha256=manifest_sha256,
                reason=reason,
            )
            raise PaperMigrationPostCommitIndeterminateError(
                "paper migration generation is visible but not durably verified",
                state=state,
            ) from exc
        raise
    finally:
        if manifest_fd is not None:
            os.close(manifest_fd)
        if object_fd is not None:
            os.close(object_fd)


def recover_paper_migration_publication(
    state: PaperMigrationPostCommitState,
    *,
    root_policy: PublicationRootPolicy,
    failure_after_phase: PaperMigrationFaultPoint | None = None,
) -> PaperMigrationPublicationReceipt:
    state = PaperMigrationPostCommitState.model_validate(state)
    if failure_after_phase not in {None, *RECOVERY_FAULT_POINTS}:
        raise ValueError("unsupported publication recovery failure phase")
    if state.policy_id != root_policy.policy_id:
        raise ValueError("post-commit state and root policy disagree")
    root: _PublicationRootHandle | None = None
    building_fd: int | None = None
    try:
        root = observe_publication_root(root_policy, create_generations=False)
        building_fd = os.open(
            state.building_name,
            _required_open_flags(directory=True),
            dir_fd=root.generations_fd,
        )
        building_identity = _full_directory_identity(os.fstat(building_fd))
        _validate_full_directory(
            building_identity,
            uid=root_policy.owner_uid,
            gid=root_policy.group_gid,
            mode=root_policy.building_mode,
            label="publication building directory",
        )
        if building_identity != state.building_identity:
            raise ValueError("publication building directory differs from post-rename binding")
        os.fsync(building_fd)
        os.fsync(root.generations_fd)
        _checkpoint(failure_after_phase, "after_parent_fsync_before_final_verify")
        receipt, object_fd = _validate_receipt_from_generation(
            root,
            generation_name=state.generation_name,
            expected_manifest_sha256=state.expected_manifest_sha256,
            expected_object_name=state.object_name,
            expected_candidate_sha256=state.candidate_sha256,
            failure_after_phase=failure_after_phase,
        )
        os.close(object_fd)
        _checkpoint(failure_after_phase, "before_local_failure_disposition")
        _checkpoint(failure_after_phase, "before_result_assembly")
        return receipt
    except PaperMigrationPostCommitIndeterminateError:
        raise
    except BaseException as exc:
        raise PaperMigrationPostCommitIndeterminateError(
            "paper migration publication recovery remains indeterminate",
            state=state,
        ) from exc
    finally:
        if building_fd is not None:
            os.close(building_fd)
        if root is not None:
            root.close()


def _validate_staging_root(
    staging_root: Path,
    *,
    forbidden: tuple[PublicationStableDirectoryIdentity, ...],
) -> int:
    descriptor = _open_absolute_directory(staging_root)
    try:
        identity = _directory_identity(os.fstat(descriptor))
        _validate_directory(
            identity,
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o700,
            label="audit staging root",
        )
        if any(
            (identity.device, identity.inode) == (item.device, item.inode) for item in forbidden
        ):
            raise ValueError("audit staging root must be distinct from publication directories")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def materialize_paper_migration_for_audit(
    receipt: PaperMigrationPublicationReceipt,
    *,
    root_policy: PublicationRootPolicy,
    staging_root: Path,
    failure_after_phase: PaperMigrationFaultPoint | None = None,
) -> PaperMigrationAuditMaterialization:
    if failure_after_phase not in {None, *MATERIALIZATION_FAULT_POINTS}:
        raise ValueError("unsupported materialization failure phase")
    if receipt.manifest.root_observation.policy_id != root_policy.policy_id:
        raise ValueError("publication receipt and root policy disagree")
    root: _PublicationRootHandle | None = None
    object_fd: int | None = None
    staging_fd: int | None = None
    destination_fd: int | None = None
    private_name: str | None = None
    private_visible = False
    try:
        root = observe_publication_root(root_policy, create_generations=False)
        verified_receipt, object_fd = _validate_receipt_from_generation(
            root,
            generation_name=receipt.manifest.generation_name,
            expected_receipt=receipt,
        )
        source_pre = _file_identity(os.fstat(object_fd))
        first_sha256, first_size = _hash_fd(object_fd)
        source_after_first_hash = _file_identity(os.fstat(object_fd))
        if (
            source_pre != source_after_first_hash
            or source_pre != receipt.manifest.object_identity
            or first_sha256 != receipt.manifest.candidate_sha256
            or first_size != source_pre.size
        ):
            raise ValueError("publication object changed during first materialization hash")
        _checkpoint(failure_after_phase, "after_materialization_first_object_hash")
        generation_identity = receipt.manifest.generation_identity
        staging_path = _absolute_lexical_path(staging_root)
        staging_fd = _validate_staging_root(
            staging_path,
            forbidden=(
                root.observation.root,
                root.observation.generations,
                generation_identity,
            ),
        )
        staging_path = _absolute_lexical_path(staging_root)
        private_name = (
            f"paper-migration-audit-{receipt.manifest.publication_nonce}-"
            f"{secrets.token_hex(32)}.sqlite3"
        )
        destination_flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            destination_flags |= os.O_CLOEXEC
        destination_fd = os.open(private_name, destination_flags, 0o600, dir_fd=staging_fd)
        private_visible = True
        os.fchmod(destination_fd, 0o600)
        destination_pre = _file_identity(os.fstat(destination_fd))
        _validate_file(
            destination_pre,
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o600,
            label="audit materialization destination",
        )
        os.lseek(object_fd, 0, os.SEEK_SET)
        copy_digest = hashlib.sha256()
        copied_size = 0
        injected = False
        while chunk := os.read(object_fd, _COPY_CHUNK_SIZE):
            if not injected:
                _checkpoint(failure_after_phase, "during_materialization_copy")
                injected = True
            copy_digest.update(chunk)
            copied_size += len(chunk)
            offset = 0
            while offset < len(chunk):
                written = os.write(destination_fd, chunk[offset:])
                if written <= 0:
                    raise OSError("short write during paper migration materialization")
                offset += written
        copied_sha256 = copy_digest.hexdigest()
        source_post = _file_identity(os.fstat(object_fd))
        if source_post != source_pre or copied_size != first_size or copied_sha256 != first_sha256:
            raise ValueError("publication object changed during materialization copy")
        destination_before_fsync = _file_identity(os.fstat(destination_fd))
        if (
            destination_pre.device,
            destination_pre.inode,
        ) != (
            destination_before_fsync.device,
            destination_before_fsync.inode,
        ):
            raise ValueError("audit materialization destination identity changed during copy")
        os.fsync(destination_fd)
        destination_post = _file_identity(os.fstat(destination_fd))
        final_sha256, final_size = _hash_fd(destination_fd)
        destination_final = _file_identity(os.fstat(destination_fd))
        if (
            destination_before_fsync != destination_post
            or destination_post != destination_final
            or final_sha256 != copied_sha256
            or final_size != copied_size
        ):
            raise ValueError("audit materialization identity or digest differs")
        _validate_file(
            destination_final,
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o600,
            label="audit materialization destination",
        )
        private_image = _capture_stable_sqlite_image(destination_fd)
        _revalidate_stable_sqlite_image(destination_fd, private_image.binding)
        connection = _open_memory_sqlite_image(private_image)
        try:
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("audit materialization SQLite integrity check failed")
            from rquant.paper_broker import PaperBrokerStore
            from rquant.paper_ledger_migration import validate_migration_attestation

            PaperBrokerStore._verify_v5_migration_in_connection(connection)
            attestation = validate_migration_attestation(connection)
        finally:
            connection.close()
        manifest = verified_receipt.manifest
        if (
            attestation.digest != manifest.migration_attestation_digest
            or attestation.source_sha256 != manifest.source_sha256
            or attestation.v4_reconciliation_report_digest
            != manifest.v4_reconciliation_report_digest
            or attestation.migration_code_identity != manifest.migration_code_identity
            or attestation.migration_algorithm_id != manifest.migration_algorithm_id
            or attestation.target_schema_identity != manifest.target_schema_identity
        ):
            raise ValueError("audit materialization migration attestation differs")
        _revalidate_stable_sqlite_image(destination_fd, private_image.binding)
        _validate_file(
            _file_identity(os.fstat(destination_fd)),
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o600,
            label="audit materialization destination",
        )
        os.fsync(destination_fd)
        os.fsync(staging_fd)
        staging_identity = _full_directory_identity(os.fstat(staging_fd))
        _validate_full_directory(
            staging_identity,
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o700,
            label="audit staging root",
        )
        _checkpoint(failure_after_phase, "after_private_memory_verification_before_final_rebind")
        rebound_fd = _open_regular_at(staging_fd, private_name)
        try:
            rebound_pre = _file_identity(os.fstat(rebound_fd))
            rebound_digest, rebound_size = _hash_fd(rebound_fd)
            rebound_post = _file_identity(os.fstat(rebound_fd))
            if (
                rebound_pre != rebound_post
                or rebound_pre != private_image.binding.identity
                or rebound_digest != private_image.binding.sha256
                or rebound_size != private_image.binding.identity.size
            ):
                raise ValueError("audit private final binding differs")
            if _full_directory_identity(os.fstat(staging_fd)) != staging_identity:
                raise ValueError("audit staging root changed before result")
        finally:
            os.close(rebound_fd)
        facts = _PaperMigrationAuditReceiptFactsV2(
            receipt_sha256=verified_receipt.receipt_sha256,
            manifest_sha256=verified_receipt.manifest_sha256,
            manifest_identity=verified_receipt.manifest_identity,
            policy_id=root.policy.policy_id,
            policy_profile=manifest.policy_profile,
            publication_nonce=manifest.publication_nonce,
            generation_name=manifest.generation_name,
            object_name=manifest.object_name,
            object_identity=manifest.object_identity,
            candidate_sha256=manifest.candidate_sha256,
            source_sha256=manifest.source_sha256,
            v4_reconciliation_report_digest=manifest.v4_reconciliation_report_digest,
            migration_attestation_digest=manifest.migration_attestation_digest,
            migration_code_identity=manifest.migration_code_identity,
            migration_algorithm_id=manifest.migration_algorithm_id,
            target_schema_identity=manifest.target_schema_identity,
        )
        verification = _PaperMigrationAuditVerificationEvidenceV2(
            **facts.model_dump(mode="python", exclude={"contract"})
        )
        _checkpoint(failure_after_phase, "before_local_failure_disposition")
        return PaperMigrationAuditMaterialization(
            staging_root=staging_path,
            staging_root_identity=staging_identity,
            private_name=private_name,
            private_nonce=private_name.rsplit("-", maxsplit=1)[1].removesuffix(".sqlite3"),
            private_path=staging_path / private_name,
            receipt=facts,
            receipt_sha256=verified_receipt.receipt_sha256,
            source_sha256=manifest.source_sha256,
            materialized_sha256=private_image.binding.sha256,
            materialized_size=private_image.binding.identity.size,
            private_identity=private_image.binding.identity,
            verification=verification,
        )
    except BaseException as exc:
        orphan = None
        if private_visible and private_name is not None:
            orphan = PaperMigrationMaterializationOrphanState(
                receipt_sha256=receipt.receipt_sha256,
                private_name=private_name,
                failed_phase=failure_after_phase or type(exc).__name__,
            )
        raise PaperMigrationMaterializationError(
            "paper migration audit materialization failed",
            orphan=orphan,
        ) from exc
    finally:
        if destination_fd is not None:
            os.close(destination_fd)
        if staging_fd is not None:
            os.close(staging_fd)
        if object_fd is not None:
            os.close(object_fd)
        if root is not None:
            root.close()


__all__ = [
    "MATERIALIZATION_FAULT_POINTS",
    "MIGRATION_FAULT_POINTS",
    "PaperMigrationAuditMaterialization",
    "PaperMigrationFaultPoint",
    "PaperMigrationMaterializationError",
    "PaperMigrationMaterializationOrphanState",
    "PaperMigrationOrphanState",
    "PaperMigrationPostCommitIndeterminateError",
    "PaperMigrationPostCommitState",
    "PaperMigrationPreCommitError",
    "PaperMigrationPublicationManifest",
    "PaperMigrationPublicationReceipt",
    "PaperOfflineMigrationResult",
    "PublicationCapabilityObservation",
    "PublicationFileIdentity",
    "PublicationProfile",
    "PublicationRootObservation",
    "PublicationRootPolicy",
    "PublicationStableDirectoryIdentity",
    "PublicationState",
    "local_audit_publication_root_policy",
    "materialize_paper_migration_for_audit",
    "recover_paper_migration_publication",
]
