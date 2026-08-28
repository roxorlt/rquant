"""Production builder for isolated artifact tier migration and garbage collection."""

from __future__ import annotations

import hashlib
import os
import stat
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from pydantic import Field, StrictInt, field_validator, model_validator

from rquant.artifact_catalog_registration_outbox import ArtifactCatalogRegistrationOutbox
from rquant.artifact_retention import (
    ArtifactReferenceStore,
    ArtifactRetentionWriterCredential,
    ArtifactTierMigrationCoordinator,
    ArtifactWriterCapability,
    ArtifactWriterCredential,
    ObjectCopy,
    RetentionPolicy,
    StorageTier,
    TierMigrationCursor,
)
from rquant.artifact_terminal_owners import TerminalReleaseOutboxPublisher
from rquant.runtime_artifact_retention import (
    ArtifactDeletionGate,
    ArtifactGcHealthProjector,
    ArtifactGcHealthSummary,
    ArtifactGcRuntimeStore,
    ArtifactGcWorker,
    ExactFullVerifiedRecoveryDeletionGate,
    GcWorkerConfig,
    LocalAtomicArtifactTransport,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_recovery_backup import load_recovery_backup_generation
from rquant.runtime_recovery_coordinator import RuntimeRecoveryFixedReplayVerifier
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    ArtifactTerminalOwnerStep,
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.strict_json import strict_canonical_json_loads

if TYPE_CHECKING:
    from rquant.runtime_artifact_terminal_lifecycle import ProductionArtifactTerminalLifecycle

_WRITER_CREDENTIAL_CAPABILITY = "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL"


def _writer_credential_from_capabilities(
    values: Mapping[str, str],
) -> ArtifactRetentionWriterCredential:
    payload = values.get(_WRITER_CREDENTIAL_CAPABILITY, "").strip()
    if not payload:
        raise ValueError("retention writer credential capability is required")
    try:
        decoded = strict_canonical_json_loads(payload.encode("utf-8"))
        if not isinstance(decoded, dict):
            raise TypeError("retention writer credential payload must be an object")
        if {"credential", "capability"}.issubset(decoded):
            writer_credential = ArtifactWriterCredential.model_validate(decoded["credential"])
            writer_capability = ArtifactWriterCapability.model_validate(decoded["capability"])
            if writer_credential.key_id != writer_capability.key_id:
                raise ValueError("retention writer credential and capability key ids differ")
            if writer_credential.service_id != writer_capability.service_id:
                raise ValueError("retention writer credential and capability owner differs")
            if (
                writer_credential.secret_sha256
                != hashlib.sha256(bytes.fromhex(writer_capability.secret_hex)).hexdigest()
            ):
                raise ValueError(
                    "retention writer capability secret must match credential identity"
                )
            return ArtifactRetentionWriterCredential(
                key_id=writer_capability.key_id,
                sequence=1,
                secret_hex=writer_capability.secret_hex,
                previous_secret_hex=None,
                not_before=writer_capability.issued_at,
                expires_at=writer_capability.expires_at,
                revoked_at=writer_credential.revoked_at,
            )
        return ArtifactRetentionWriterCredential.model_validate(decoded)
    except (TypeError, ValueError) as exc:
        raise ValueError("retention writer credential capability is invalid") from exc


class DescriptorSchemaBinding(RuntimeContractModel):
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: StrictInt = Field(ge=0)
    schema_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class TrustedDescriptorSchemaAuthority(RuntimeContractModel):
    schema_version: StrictInt = Field(default=1, ge=1, le=1)
    bindings: tuple[DescriptorSchemaBinding, ...] = Field(min_length=1)
    authority_id: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_identity(self) -> TrustedDescriptorSchemaAuthority:
        keys = [(item.content_sha256, item.size_bytes) for item in self.bindings]
        if len(keys) != len(set(keys)):
            raise ValueError("descriptor schema authority contains duplicate bindings")
        expected = canonical_sha256(
            {
                "schema_version": self.schema_version,
                "bindings": [item.model_dump(mode="python") for item in self.bindings],
            }
        )
        if self.authority_id != expected:
            raise ValueError("descriptor schema authority identity mismatch")
        return self


class TrustedDescriptorSchemaResolver:
    """Hash-bound resolver loaded from one descriptor-safe private authority file."""

    _MAX_AUTHORITY_BYTES = 4 * 1024 * 1024

    def __init__(
        self,
        *,
        authority: TrustedDescriptorSchemaAuthority,
    ) -> None:
        self.authority_id = authority.authority_id
        self._bindings = {
            (item.content_sha256, item.size_bytes): item.schema_sha256
            for item in authority.bindings
        }

    @classmethod
    def from_settings(
        cls,
        settings: ArtifactRetentionSettings,
    ) -> TrustedDescriptorSchemaResolver:
        return cls.from_authority(
            root=settings.schema_authority_root,
            path=settings.schema_authority_path,
            expected_sha256=settings.schema_authority_sha256,
        )

    @classmethod
    def from_authority(
        cls,
        *,
        root: Path,
        path: Path,
        expected_sha256: str,
    ) -> TrustedDescriptorSchemaResolver:
        if (
            not root.is_absolute()
            or root != Path(root.absolute())
            or not path.is_absolute()
            or path != Path(path.absolute())
        ):
            raise ValueError("schema authority paths must be exact absolute paths")
        if len(expected_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in expected_sha256
        ):
            raise ValueError("schema authority requires a lowercase SHA-256 identity")
        payload = cls._read_bound_authority(
            root=root,
            path=path,
        )
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise ValueError("descriptor schema authority content hash mismatch")
        decoded = strict_canonical_json_loads(payload)
        authority = TrustedDescriptorSchemaAuthority.model_validate(decoded)
        return cls(authority=authority)

    @staticmethod
    def _open_root_chain(root: Path) -> list[int]:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptors = [os.open(root.anchor, flags)]
        try:
            for component in root.parts[1:]:
                parent_fd = descriptors[-1]
                named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
                if stat.S_ISLNK(named.st_mode) or not stat.S_ISDIR(named.st_mode):
                    raise ValueError("schema authority ancestor is not a trusted directory")
                child_fd = os.open(component, flags, dir_fd=parent_fd)
                opened = os.fstat(child_fd)
                if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
                    os.close(child_fd)
                    raise ValueError("schema authority ancestor changed while binding")
                descriptors.append(child_fd)
            return descriptors
        except BaseException:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            raise

    @classmethod
    def _read_bound_authority(cls, *, root: Path, path: Path) -> bytes:
        try:
            relative = path.relative_to(root)
        except ValueError as exc:
            raise ValueError("schema authority path escapes its trust root") from exc
        if len(relative.parts) != 1 or relative.name in {"", ".", ".."}:
            raise ValueError("schema authority must be a direct trust-root child")
        descriptors = cls._open_root_chain(root)
        descriptor = -1
        try:
            root_stat = os.fstat(descriptors[-1])
            if root_stat.st_uid != os.geteuid() or stat.S_IMODE(root_stat.st_mode) & 0o077:
                raise ValueError("schema authority root owner or mode is unsafe")
            descriptor = os.open(
                relative.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptors[-1],
            )
            named = os.stat(relative.name, dir_fd=descriptors[-1], follow_symlinks=False)
            opened = os.fstat(descriptor)
            if (
                stat.S_ISLNK(named.st_mode)
                or not stat.S_ISREG(opened.st_mode)
                or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) & 0o077
                or opened.st_nlink != 1
                or opened.st_size > cls._MAX_AUTHORITY_BYTES
            ):
                raise ValueError("schema authority file identity or mode is unsafe")
            chunks: list[bytes] = []
            remaining = cls._MAX_AUTHORITY_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(remaining, 1024 * 1024))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
            current = os.stat(relative.name, dir_fd=descriptors[-1], follow_symlinks=False)
            if (
                len(payload) > cls._MAX_AUTHORITY_BYTES
                or (after.st_dev, after.st_ino, after.st_size)
                != (opened.st_dev, opened.st_ino, opened.st_size)
                or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise ValueError("schema authority changed while reading")
            return payload
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            for ancestor in reversed(descriptors):
                os.close(ancestor)

    def __call__(self, descriptor: int) -> str:
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_size < 0:
            raise ValueError("schema resolution requires a regular artifact descriptor")
        digest = hashlib.sha256()
        os.lseek(descriptor, 0, os.SEEK_SET)
        remaining = observed.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise ValueError("artifact changed while resolving schema")
            digest.update(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino, after.st_size) != (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
        ):
            raise ValueError("artifact changed while resolving schema")
        try:
            return self._bindings[(digest.hexdigest(), observed.st_size)]
        except KeyError as exc:
            raise ValueError("artifact is absent from trusted descriptor schema authority") from exc


class DeletionGateFactory(Protocol):
    def __call__(self, settings: ArtifactRetentionSettings) -> ArtifactDeletionGate: ...


class TierMigrationRuntimeConfig(RuntimeContractModel):
    warm_root: Path
    cold_root: Path
    warm_failure_domain: str = Field(min_length=1, max_length=128)
    cold_failure_domain: str = Field(min_length=1, max_length=128)
    batch_items: StrictInt = Field(ge=1, le=10_000)
    batch_bytes: StrictInt = Field(ge=1, le=16 * 1024**4)
    max_runtime: timedelta
    query_page_items: StrictInt = Field(ge=1, le=1024)

    @field_validator("warm_root", "cold_root")
    @classmethod
    def require_exact_absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute() or value != Path(value.absolute()):
            raise ValueError("migration roots must be exact absolute paths")
        return value

    @model_validator(mode="after")
    def validate_migration_budget(self) -> TierMigrationRuntimeConfig:
        if self.max_runtime <= timedelta(0) or self.max_runtime > timedelta(hours=1):
            raise ValueError("migration runtime budget must be in (0, 1h]")
        if self.query_page_items < self.batch_items:
            raise ValueError("migration query page must cover its item budget")
        if self.warm_failure_domain == self.cold_failure_domain:
            raise ValueError("warm and cold migration failure domains must differ")
        return self


class ArtifactRetentionSettings(RuntimeContractModel):
    managed_root: Path
    state_root: Path
    reference_store_path: Path
    catalog_authority_root: Path | None = None
    recovery_publication_root: Path
    recovery_restore_root: Path
    recovery_target_manifest_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    recovery_profile_generation: str = Field(pattern=r"^[0-9a-f]{64}$")
    full_recovery_receipt_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    max_recovery_age: timedelta
    schema_authority_root: Path
    schema_authority_path: Path
    schema_authority_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    migration: TierMigrationRuntimeConfig
    max_bundle_items: StrictInt = Field(ge=1, le=100_000)
    max_bundle_bytes: StrictInt = Field(ge=1, le=16 * 1024**4)
    retention_policy: RetentionPolicy
    worker: GcWorkerConfig
    worker_id: str = Field(default="artifact-retention", min_length=1, max_length=128)
    terminal_publish_batch: StrictInt = Field(default=128, ge=1, le=10_000)
    catalog_registration_batch: StrictInt = Field(default=64, ge=1, le=10_000)
    health_max_items: StrictInt = Field(default=256, ge=1, le=1_023)
    health_max_bytes: StrictInt = Field(default=1024**3, ge=1)
    health_max_seconds: float = Field(default=1.0, gt=0, le=30)

    @field_validator(
        "managed_root",
        "state_root",
        "reference_store_path",
        "catalog_authority_root",
        "recovery_publication_root",
        "recovery_restore_root",
        "schema_authority_root",
        "schema_authority_path",
    )
    @classmethod
    def require_exact_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute() or value != Path(value.absolute()):
            raise ValueError("artifact retention paths must be exact absolute paths")
        return value

    @model_validator(mode="after")
    def validate_isolated_state(self) -> ArtifactRetentionSettings:
        if self.max_recovery_age <= timedelta(0):
            raise ValueError("max_recovery_age must be positive")
        try:
            self.reference_store_path.relative_to(self.state_root)
        except ValueError as exc:
            raise ValueError("reference_store_path must remain below state_root") from exc
        if self.catalog_authority_root is None:
            object.__setattr__(
                self,
                "catalog_authority_root",
                self.state_root / "catalog-authority",
            )
        assert self.catalog_authority_root is not None
        if self.catalog_authority_root != self.state_root / "catalog-authority":
            raise ValueError("catalog_authority_root must remain below retention state root")
        gc_path = self.state_root / "gc-runtime.sqlite3"
        if self.reference_store_path == gc_path:
            raise ValueError("artifact catalog and GC require independent SQLite state")
        if self.managed_root == self.state_root:
            raise ValueError("managed artifact root and state_root must be independent")
        try:
            authority_relative = self.schema_authority_path.relative_to(self.schema_authority_root)
        except ValueError as exc:
            raise ValueError(
                "schema_authority_path must remain below schema_authority_root"
            ) from exc
        if len(authority_relative.parts) != 1:
            raise ValueError("schema_authority_path must be a direct trust-root child")
        for tier_root in (self.migration.warm_root, self.migration.cold_root):
            try:
                tier_root.relative_to(self.managed_root)
            except ValueError as exc:
                raise ValueError("migration tier roots must remain below managed_root") from exc
        if self.migration.warm_root == self.migration.cold_root:
            raise ValueError("warm and cold migration roots must be independent")
        if self.migration.batch_items > self.max_bundle_items:
            raise ValueError("migration item budget exceeds retention bundle limit")
        if self.migration.batch_bytes > self.max_bundle_bytes:
            raise ValueError("migration byte budget exceeds retention bundle limit")
        if self.worker.batch_items > self.max_bundle_items:
            raise ValueError("GC item budget exceeds retention bundle limit")
        if self.worker.batch_bytes > self.max_bundle_bytes:
            raise ValueError("GC byte budget exceeds retention bundle limit")
        return self


class ArtifactRetentionRuntime:
    def __init__(
        self,
        *,
        settings: ArtifactRetentionSettings,
        reference_store: ArtifactReferenceStore,
        gc_state: ArtifactGcRuntimeStore,
        transport: LocalAtomicArtifactTransport,
        migration: ArtifactTierMigrationCoordinator,
        worker: ArtifactGcWorker,
        health: ArtifactGcHealthProjector,
        terminal_publisher: TerminalReleaseOutboxPublisher,
        catalog_registration_outbox: ArtifactCatalogRegistrationOutbox | None = None,
        terminal_lifecycle: ProductionArtifactTerminalLifecycle | None = None,
        catalog_authority_producer_commit: str | None = None,
        monotonic: Callable[[], float],
        clock: Callable[[], datetime],
    ) -> None:
        self.settings = settings
        self.reference_store = reference_store
        self.gc_state = gc_state
        self.transport = transport
        self.migration = migration
        self.worker = worker
        self.health = health
        self.terminal_publisher = terminal_publisher
        self.catalog_registration_outbox = catalog_registration_outbox
        self.terminal_lifecycle = terminal_lifecycle
        self.catalog_authority_producer_commit = catalog_authority_producer_commit
        self._monotonic = monotonic
        self._clock = clock

    def run_step(self) -> RuntimeStepResult:
        catalog_registrations = self._apply_catalog_registrations()
        published = self.terminal_publisher.run_batch(limit=self.settings.terminal_publish_batch)
        migration_summary = self._run_migration_step()
        summary = self.worker.run_once()
        health = self.health.snapshot(
            now=self._clock(),
            max_items=self.settings.health_max_items,
            max_bytes=self.settings.health_max_bytes,
            deadline_monotonic=(self._monotonic() + self.settings.health_max_seconds),
        )
        projected = ArtifactGcHealthAuthorityAdapter().project(
            health,
            processed_count=(
                summary.completed + migration_summary.migrated + catalog_registrations
            ),
            terminal_releases=published,
        )
        if self.catalog_authority_producer_commit is not None:
            from rquant.artifact_retention_catalog_authority import (
                publish_retention_catalog_authority,
            )

            publish_retention_catalog_authority(
                state_root=self.settings.state_root,
                reference_store_path=self.settings.reference_store_path,
                producer_commit=self.catalog_authority_producer_commit,
            )
        return projected.model_copy(
            update={
                "source_generations": {
                    **dict(projected.source_generations),
                    "artifact_gc": canonical_sha256(summary),
                    "artifact_tier_migration": canonical_sha256(migration_summary),
                    "artifact_catalog_registration_outbox": canonical_sha256(
                        {
                            "applied": catalog_registrations,
                            "pending": (
                                0
                                if self.catalog_registration_outbox is None
                                else self.catalog_registration_outbox.pending_count()
                            ),
                        }
                    ),
                }
            }
        )

    def close(self) -> None:
        self.reference_store.close()
        self.transport.close()

    def _apply_catalog_registrations(self) -> int:
        outbox = self.catalog_registration_outbox
        lifecycle = self.terminal_lifecycle
        if outbox is None and lifecycle is None:
            return 0
        if outbox is None or lifecycle is None:
            raise RuntimeError(
                "artifact catalog registration processing requires outbox and terminal lifecycle"
            )
        if lifecycle.hooks is None:
            raise RuntimeError("artifact catalog registration lifecycle hooks are missing")
        outbox.recover_claims()
        applied = 0
        for request in outbox.claim_next(limit=self.settings.catalog_registration_batch):
            self.reference_store.register_bundle_atomic(request.registration)
            observed_at = self._clock()
            for reference in request.registration.references:
                if reference.owner_type in {"audit", "experiment", "snapshot"}:
                    lifecycle.hooks(
                        reference.owner_type,
                        reference.owner_id,
                        observed_at,
                    )
            self.reference_store.apply_catalog_job_terminal_release(
                request.job_terminal_receipt,
                applied_at=observed_at,
            )
            outbox.complete(request)
            applied += 1
        return applied

    @staticmethod
    def _migration_cursor(source: ObjectCopy) -> TierMigrationCursor:
        return TierMigrationCursor(
            content_sha256=source.content_sha256,
            tier_rank=0 if source.storage_tier is StorageTier.HOT else 1,
            location_id=source.location_id,
        )

    def _run_migration_step(self) -> TierMigrationRunSummary:
        started = self._monotonic()
        deadline = started + self.settings.migration.max_runtime.total_seconds()
        observed_at = self._clock()
        cursor = self.gc_state.tier_migration_cursor()
        page = self.reference_store.tier_migration_page(
            now=observed_at,
            policy=self.settings.retention_policy,
            after=cursor,
            scan_limit=self.settings.migration.query_page_items,
        )
        if not page.sources and page.next_cursor is None:
            self.gc_state.persist_tier_migration_cursor(None, updated_at=observed_at)
            return TierMigrationRunSummary(
                migrated=0,
                deferred=0,
                scanned=0,
                copied_bytes=0,
                truncated=False,
                next_cursor=None,
            )
        migrated = deferred = scanned = copied_bytes = 0
        all_sources_consumed = True
        persisted_cursor = cursor
        for source in page.sources:
            if scanned >= self.settings.migration.batch_items or self._monotonic() >= deadline:
                all_sources_consumed = False
                break
            scanned += 1
            source_cursor = self._migration_cursor(source.source_copy)
            remaining_bytes = self.settings.migration.batch_bytes - copied_bytes
            single_oversized_item = (
                copied_bytes == 0
                and source.object_identity.size_bytes > remaining_bytes
                and source.object_identity.size_bytes <= self.settings.max_bundle_bytes
            )
            if source.object_identity.size_bytes > remaining_bytes and not single_oversized_item:
                deferred += 1
                persisted_cursor = source_cursor
                self.gc_state.persist_tier_migration_cursor(
                    persisted_cursor,
                    updated_at=observed_at,
                )
                continue
            verification = self.transport.verify(source.source_copy.storage_uri)
            if self._monotonic() >= deadline:
                all_sources_consumed = False
                break
            target_tier = (
                StorageTier.WARM
                if source.source_copy.storage_tier is StorageTier.HOT
                else StorageTier.COLD
            )
            target_root = (
                self.settings.migration.warm_root
                if target_tier is StorageTier.WARM
                else self.settings.migration.cold_root
            )
            target_domain = (
                self.settings.migration.warm_failure_domain
                if target_tier is StorageTier.WARM
                else self.settings.migration.cold_failure_domain
            )
            target_path = (
                target_root
                / source.object_identity.content_sha256[:2]
                / f"{source.object_identity.content_sha256}.artifact"
            )
            self.migration.migrate(
                source=source.source_copy,
                target=ObjectCopy(
                    content_sha256=source.object_identity.content_sha256,
                    location_id=(
                        f"retention-{target_tier.value}-{source.object_identity.content_sha256}"
                    ),
                    storage_uri=target_path.as_uri(),
                    storage_tier=target_tier,
                    verified_at=observed_at,
                    failure_domain=target_domain,
                    tier_entered_at=observed_at,
                ),
                observed_at=observed_at,
                expected_schema_sha256=verification.schema_sha256,
            )
            migrated += 1
            copied_bytes += source.object_identity.size_bytes
            persisted_cursor = source_cursor
            self.gc_state.persist_tier_migration_cursor(
                persisted_cursor,
                updated_at=observed_at,
            )
        if all_sources_consumed and page.next_cursor is not None:
            persisted_cursor = page.next_cursor
            self.gc_state.persist_tier_migration_cursor(
                persisted_cursor,
                updated_at=observed_at,
            )
        return TierMigrationRunSummary(
            migrated=migrated,
            deferred=deferred,
            scanned=scanned,
            copied_bytes=copied_bytes,
            truncated=(
                not page.exhausted or not all_sources_consumed or self._monotonic() >= deadline
            ),
            next_cursor=persisted_cursor,
        )


class TierMigrationRunSummary(RuntimeContractModel):
    migrated: StrictInt = Field(ge=0)
    deferred: StrictInt = Field(ge=0)
    scanned: StrictInt = Field(ge=0)
    copied_bytes: StrictInt = Field(ge=0)
    truncated: bool
    next_cursor: TierMigrationCursor | None


class ArtifactGcHealthAuthorityAdapter:
    """Map bounded GC health onto the generic runtime heartbeat contract."""

    def project(
        self,
        health: ArtifactGcHealthSummary,
        *,
        processed_count: int,
        terminal_releases: int,
    ) -> RuntimeStepResult:
        reasons: list[str] = []
        if health.status != "healthy":
            reasons.append(f"artifact_gc:{health.status}")
        if health.dead_letter_count:
            reasons.append(f"artifact_gc:dead_letters={health.dead_letter_count}")
        if health.truncated:
            reasons.append("artifact_gc:projection_truncated")
        if health.quarantine_orphan_count:
            reasons.append(f"artifact_gc:quarantine_orphans={health.quarantine_orphan_count}")
        if health.operation_reconciliation_pending_count:
            reasons.append(
                "artifact_gc:reconciliation_pending="
                f"{health.operation_reconciliation_pending_count}"
            )
        return RuntimeStepResult(
            input_sequence=health.lease_fence,
            output_sequence=health.lease_fence,
            processed_count=processed_count + terminal_releases,
            backlog_count=health.backlog_count,
            source_generations={
                "artifact_gc_health": canonical_sha256(health),
                "terminal_release_outbox": canonical_sha256(
                    {
                        "published": terminal_releases,
                        "observed_at": health.observed_at,
                    }
                ),
            },
            degraded_reasons=tuple(reasons),
        )


def _exact_full_deletion_gate(
    settings: ArtifactRetentionSettings,
) -> ArtifactDeletionGate:
    _pointer, _receipt, target, _tool, expectations = load_recovery_backup_generation(
        settings.recovery_publication_root
    )
    if (
        target.manifest_id != settings.recovery_target_manifest_id
        or target.target_profile_generation != settings.recovery_profile_generation
    ):
        raise ValueError("artifact retention recovery target identity conflicts")
    verifier = RuntimeRecoveryFixedReplayVerifier(expectations=expectations.expectations)
    return ExactFullVerifiedRecoveryDeletionGate(
        restore_root=settings.recovery_restore_root,
        receipt_id=settings.full_recovery_receipt_id,
        target=target,
        fixed_replay_verifier=verifier,
        max_recovery_age=settings.max_recovery_age,
    )


def build_artifact_retention_runtime(
    *,
    settings: ArtifactRetentionSettings,
    clock: Callable[[], datetime],
    schema_resolver: Callable[[int], str] | None = None,
    deletion_gate_factory: DeletionGateFactory | None = None,
    monotonic: Callable[[], float] | None = None,
    retention_writer_credential: ArtifactRetentionWriterCredential | None = None,
    catalog_registration_outbox: ArtifactCatalogRegistrationOutbox | None = None,
    terminal_lifecycle: ProductionArtifactTerminalLifecycle | None = None,
    catalog_authority_producer_commit: str | None = None,
) -> ArtifactRetentionRuntime:
    timer = monotonic or time.monotonic
    reference_store = ArtifactReferenceStore(
        settings.reference_store_path,
        managed_trust_root=settings.state_root,
        clock=clock,
        writer_owner=(
            "artifact-retention"
            if retention_writer_credential is not None
            else "artifact-reference-store"
        ),
        retention_writer_credential=retention_writer_credential,
    )
    gc_state = ArtifactGcRuntimeStore(
        settings.state_root / "gc-runtime.sqlite3",
        managed_trust_root=settings.state_root,
    )
    resolved_schema = schema_resolver or TrustedDescriptorSchemaResolver.from_settings(settings)
    transport = LocalAtomicArtifactTransport(
        managed_root=settings.managed_root,
        clock=clock,
        schema_resolver=resolved_schema,
    )
    gate = (deletion_gate_factory or _exact_full_deletion_gate)(settings)
    worker = ArtifactGcWorker(
        catalog=reference_store,
        state=gc_state,
        transport=transport,
        deletion_gate=gate,
        policy=settings.retention_policy,
        config=settings.worker,
        worker_id=settings.worker_id,
        clock=clock,
        monotonic=timer,
    )
    health = ArtifactGcHealthProjector(
        catalog=reference_store,
        state=gc_state,
        quarantine_inspector=transport,
        monotonic=timer,
    )
    return ArtifactRetentionRuntime(
        settings=settings,
        reference_store=reference_store,
        gc_state=gc_state,
        transport=transport,
        migration=ArtifactTierMigrationCoordinator(
            store=reference_store,
            transport=transport,
        ),
        worker=worker,
        health=health,
        terminal_publisher=TerminalReleaseOutboxPublisher(reference_store),
        catalog_registration_outbox=catalog_registration_outbox,
        terminal_lifecycle=terminal_lifecycle,
        catalog_authority_producer_commit=catalog_authority_producer_commit,
        monotonic=timer,
        clock=clock,
    )


def artifact_retention_builder(
    *,
    clock: Callable[[], datetime],
    schema_resolver: Callable[[int], str] | None = None,
    deletion_gate_factory: DeletionGateFactory | None = None,
    capability_environment: Mapping[str, str] | None = None,
    open_artifact_terminal_lifecycle: (
        Callable[[], ProductionArtifactTerminalLifecycle] | None
    ) = None,
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.ARTIFACT_RETENTION:
            raise ValueError("runtime service kind must be artifact retention")
        if manifest.plane is not RuntimeServicePlane.RESEARCH:
            raise ValueError("artifact retention must run on the research plane")
        credential = _writer_credential_from_capabilities(capability_environment or {})
        if open_artifact_terminal_lifecycle is None:
            raise RuntimeError("artifact retention requires the production terminal lifecycle")
        settings = ArtifactRetentionSettings.model_validate(dict(manifest.settings))
        lifecycle = open_artifact_terminal_lifecycle()
        if (
            lifecycle.reference_store is None
            or lifecycle.catalog_registration_outbox is None
            or lifecycle.hooks is None
        ):
            lifecycle.close()
            raise RuntimeError("artifact retention lifecycle capabilities are incomplete")
        if lifecycle.reference_store.path != settings.reference_store_path:
            lifecycle.close()
            raise ValueError("artifact retention lifecycle reference authority path conflicts")
        runtime = build_artifact_retention_runtime(
            settings=settings,
            clock=clock,
            schema_resolver=schema_resolver,
            deletion_gate_factory=deletion_gate_factory,
            retention_writer_credential=credential,
            catalog_registration_outbox=lifecycle.catalog_registration_outbox,
            terminal_lifecycle=lifecycle,
            catalog_authority_producer_commit=manifest.producer_commit,
        )
        return ArtifactTerminalOwnerStep(
            step=runtime.run_step,
            artifact_terminal_lifecycle=lifecycle,
            resource_closer=runtime.close,
        )

    return build


__all__ = [
    "ArtifactGcHealthAuthorityAdapter",
    "ArtifactRetentionRuntime",
    "ArtifactRetentionSettings",
    "DescriptorSchemaBinding",
    "DeletionGateFactory",
    "TrustedDescriptorSchemaAuthority",
    "TrustedDescriptorSchemaResolver",
    "TierMigrationRunSummary",
    "TierMigrationRuntimeConfig",
    "artifact_retention_builder",
    "build_artifact_retention_runtime",
]
