"""Register sealed Strategy Lab shard results in the retention ledger."""

from __future__ import annotations

import ctypes
import hashlib
import os
import re
import stat
import struct
import sys
import time
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from rquant.artifact_retention import (
    ArtifactBundleRegistration,
    ArtifactReferenceStore,
    ObjectCopy,
    ObjectIdentity,
    ObjectReference,
    StorageTier,
)
from rquant.lab_worker import LabShardResultManifest
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.strict_json import strict_model_validate_canonical_json

_HASH_PATTERN = r"^[0-9a-f]{64}$"
_ATTEMPT_PATTERN = re.compile(
    r"(?P<fence>[0-9]{20})-(?P<generation>[0-9]{20})-"
    r"(?P<token>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})"
)
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_FileIdentity = tuple[int, int, int, int, int, int, int]
_HASH_CHUNK_BYTES = 1024 * 1024
_MAX_ARTIFACT_FILE_BYTES = 1024**4
_MAX_BUNDLE_BYTES = 4 * 1024**4
_MAX_STEP_BYTES = 16 * 1024**4
_MAX_VERIFICATION_SECONDS = 3_600.0


class LabArtifactCatalogIntegrityError(RuntimeError):
    """A filesystem or ownership identity is not safe enough to catalog."""


class LabArtifactFrontierMissingError(LabArtifactCatalogIntegrityError):
    """A previously discovered directory frontier no longer exists."""


class _ArtifactVerificationStepBudgetError(RuntimeError):
    """The current bounded step cannot safely admit more artifact bytes."""


@dataclass
class _ArtifactVerificationBudget:
    max_bytes: int
    deadline: float
    monotonic: Callable[[], float]
    consumed_bytes: int = 0

    def check_time(self) -> None:
        self.check_deadline(self.deadline, label="artifact verification step")

    def check_deadline(self, deadline: float, *, label: str) -> None:
        self.check_deadlines(((deadline, label),))

    def check_deadlines(self, deadlines: tuple[tuple[float, str], ...]) -> None:
        now = self.monotonic()
        if now > self.deadline:
            raise _ArtifactVerificationStepBudgetError(
                "artifact verification step time budget exceeded"
            )
        for deadline, label in deadlines:
            if now > deadline:
                raise _ArtifactVerificationStepBudgetError(f"{label} time budget exceeded")

    def ensure_capacity(self, size_bytes: int) -> None:
        self.check_time()
        if self.consumed_bytes + size_bytes > self.max_bytes:
            raise _ArtifactVerificationStepBudgetError(
                "artifact verification step byte budget exceeded"
            )

    def consume(self, size_bytes: int) -> None:
        self.ensure_capacity(size_bytes)
        self.consumed_bytes += size_bytes


class LabArtifactDurableOwners(RuntimeContractModel):
    job_id: UUID
    spec_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    snapshot_id: str = Field(pattern=_HASH_PATTERN)
    experiment_id: str = Field(pattern=_HASH_PATTERN)
    audit_run_id: str = Field(pattern=_HASH_PATTERN)


class LabArtifactCatalogRunResult(RuntimeContractModel):
    status: Literal["completed", "partial"]
    completed_at: AwareUtcDatetime
    scanned_bundles: int = Field(ge=0)
    registered_objects: int = Field(ge=0)
    registered_copies: int = Field(ge=0)
    registered_references: int = Field(ge=0)
    unchanged_bundles: int = Field(ge=0)
    total_bytes: int = Field(ge=0)
    content_hashes: tuple[str, ...]
    has_more: bool
    next_cursor: str | None = None

    @model_validator(mode="after")
    def validate_cursor(self) -> Self:
        if self.has_more != (self.next_cursor is not None):
            raise ValueError("partial catalog result requires exactly one resume cursor")
        if self.status == "partial" and not self.has_more:
            raise ValueError("partial catalog result must have more bundles")
        if self.status == "completed" and self.has_more:
            raise ValueError("completed catalog result cannot have more bundles")
        if len(self.content_hashes) != self.scanned_bundles:
            raise ValueError("content hashes must cover every scanned bundle")
        return self


DirectoryKind = Literal["jobs", "shards", "attempts"]


class LabArtifactDirectoryFrontier(RuntimeContractModel):
    frontier_sequence: int = Field(ge=1)
    revision: int = Field(ge=0)
    scan_generation: int = Field(ge=1)
    relative_directory: str
    directory_kind: DirectoryKind
    directory_device: int | None = Field(default=None, ge=0)
    directory_inode: int | None = Field(default=None, ge=0)
    directory_offset: int = Field(ge=0)
    buffered_entry_names: tuple[str, ...] = ()


class LabArtifactChildDirectory(RuntimeContractModel):
    relative_directory: str
    directory_kind: DirectoryKind


class LabArtifactDirectoryScanPage(RuntimeContractModel):
    frontier_sequence: int = Field(ge=1)
    frontier_revision: int = Field(ge=0)
    directory_device: int = Field(ge=0)
    directory_inode: int = Field(ge=0)
    directory_offset: int = Field(ge=0)
    buffered_entry_names: tuple[str, ...]
    exhausted: bool
    scanned_entries: int = Field(ge=0)
    child_directories: tuple[LabArtifactChildDirectory, ...]
    bundle_paths: tuple[str, ...]


@dataclass(frozen=True)
class _VerifiedBundle:
    path: Path
    relative_path: str
    manifest: LabShardResultManifest
    content_sha256: str
    size_bytes: int
    owners: LabArtifactDurableOwners
    descriptor: int
    identity_guard: Callable[[], None]


OwnerResolver = Callable[[LabShardResultManifest], LabArtifactDurableOwners]
TerminalOwnerReleaser = Callable[
    [LabShardResultManifest, LabArtifactDurableOwners, datetime],
    None,
]


class _ArtifactRootAuthority:
    """Bind one private artifact root and every path component above it."""

    def __init__(self, path: Path) -> None:
        raw = Path(path)
        if not raw.is_absolute() or str(raw) != os.path.normpath(str(raw)) or ".." in raw.parts:
            raise ValueError("artifact_root must be an exact absolute path")
        self.path = raw
        self._generation = self._inspect()

    @property
    def generation(self) -> tuple[tuple[int, int, int], ...]:
        return self._generation

    def assert_current(self) -> None:
        try:
            current = self._inspect()
        except ValueError as exc:
            raise LabArtifactCatalogIntegrityError("artifact root or ancestor is unsafe") from exc
        if current != self._generation:
            raise LabArtifactCatalogIntegrityError("artifact root or ancestor changed identity")

    def rebind_outer_ancestor_ctime(self) -> None:
        """Accept sibling-service ctime changes above, but never at, artifact_root."""

        try:
            current = self._inspect()
        except ValueError as exc:
            raise LabArtifactCatalogIntegrityError("artifact root or ancestor is unsafe") from exc
        root_index = len(self.path.parts) - 1
        if (
            tuple((item[0], item[1]) for item in current)
            != tuple((item[0], item[1]) for item in self._generation)
            or current[root_index] != self._generation[root_index]
        ):
            raise LabArtifactCatalogIntegrityError("artifact root or ancestor changed identity")
        self._generation = (*current[:root_index], *self._generation[root_index:])
        self.assert_current()

    def _inspect(self) -> tuple[tuple[int, int, int], ...]:
        current = Path(self.path.anchor)
        identities: list[tuple[int, int, int]] = []
        try:
            for component in (self.path.anchor, *self.path.parts[1:]):
                if component != self.path.anchor:
                    current /= component
                observed = os.lstat(current)
                if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                    raise ValueError("artifact_root contains a symlink or non-directory")
                identities.append((observed.st_dev, observed.st_ino, observed.st_ctime_ns))
        except OSError as exc:
            raise ValueError("artifact_root must be an existing safe directory") from exc
        root = os.lstat(self.path)
        if root.st_uid != os.geteuid():
            raise ValueError("artifact_root owner is unsafe")
        if stat.S_IMODE(root.st_mode) != 0o700:
            raise ValueError("artifact_root mode must be private 0700")
        return tuple(identities)


class LabArtifactCatalogRegistrar:
    """Bounded single-writer scanner for immutable Lab shard result bundles."""

    def __init__(
        self,
        *,
        artifact_root: Path,
        reference_store: ArtifactReferenceStore,
        owner_resolver: OwnerResolver,
        terminal_owner_releaser: TerminalOwnerReleaser | None = None,
        location_id: str,
        failure_domain: str,
        clock: Callable[[], datetime] | None = None,
        max_manifest_bytes: int = 4 * 1024 * 1024,
        max_artifact_file_bytes: int = 2 * 1024**3,
        max_bundle_bytes: int = 8 * 1024**3,
        max_step_bytes: int = 16 * 1024**3,
        max_artifact_file_verification_seconds: float | None = None,
        max_bundle_verification_seconds: float | None = None,
        max_verification_seconds: float = 30.0,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        root_authority = _ArtifactRootAuthority(artifact_root)
        if not location_id.strip() or not failure_domain.strip():
            raise ValueError("location_id and failure_domain must not be empty")
        if max_manifest_bytes < 1:
            raise ValueError("max_manifest_bytes must be positive")
        self._validate_byte_budget(
            max_artifact_file_bytes,
            label="max_artifact_file_bytes",
            upper_bound=_MAX_ARTIFACT_FILE_BYTES,
        )
        self._validate_byte_budget(
            max_bundle_bytes,
            label="max_bundle_bytes",
            upper_bound=_MAX_BUNDLE_BYTES,
        )
        self._validate_byte_budget(
            max_step_bytes,
            label="max_step_bytes",
            upper_bound=_MAX_STEP_BYTES,
        )
        file_verification_seconds = (
            max_verification_seconds
            if max_artifact_file_verification_seconds is None
            else max_artifact_file_verification_seconds
        )
        bundle_verification_seconds = (
            max_verification_seconds
            if max_bundle_verification_seconds is None
            else max_bundle_verification_seconds
        )
        for label, value in (
            ("max_artifact_file_verification_seconds", file_verification_seconds),
            ("max_bundle_verification_seconds", bundle_verification_seconds),
            ("max_verification_seconds", max_verification_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or value <= 0
                or value > _MAX_VERIFICATION_SECONDS
            ):
                raise ValueError(f"{label} must be positive and no greater than 3600")
        if max_artifact_file_bytes > max_bundle_bytes:
            raise ValueError("artifact file byte budget cannot exceed bundle byte budget")
        if max_bundle_bytes > max_step_bytes:
            raise ValueError("bundle byte budget cannot exceed step byte budget")
        if file_verification_seconds > bundle_verification_seconds:
            raise ValueError("artifact file time budget cannot exceed bundle time budget")
        if bundle_verification_seconds > max_verification_seconds:
            raise ValueError("bundle time budget cannot exceed step time budget")
        self._root_authority = root_authority
        self.artifact_root = root_authority.path
        self.reference_store = reference_store
        self.owner_resolver = owner_resolver
        if terminal_owner_releaser is None:
            releaser_factory = getattr(
                owner_resolver,
                "build_terminal_owner_releaser",
                None,
            )
            if callable(releaser_factory):
                terminal_owner_releaser = releaser_factory(reference_store)
        self.terminal_owner_releaser = terminal_owner_releaser
        self.location_id = location_id.strip()
        self.failure_domain = failure_domain.strip()
        self.clock = clock or (lambda: datetime.now(UTC))
        self.max_manifest_bytes = max_manifest_bytes
        self.max_artifact_file_bytes = max_artifact_file_bytes
        self.max_bundle_bytes = max_bundle_bytes
        self.max_step_bytes = max_step_bytes
        self.max_artifact_file_verification_seconds = file_verification_seconds
        self.max_bundle_verification_seconds = bundle_verification_seconds
        self.max_verification_seconds = max_verification_seconds
        self.monotonic = monotonic or time.monotonic

    def run_once(
        self,
        *,
        bundle_paths: tuple[str, ...],
    ) -> LabArtifactCatalogRunResult:
        self._root_authority.rebind_outer_ancestor_ctime()
        normalized_paths = tuple(self._validated_bundle_path(path) for path in bundle_paths)
        if len(normalized_paths) != len(set(normalized_paths)):
            raise ValueError("bundle_paths must be unique")
        selected = tuple(
            (relative_path, self.artifact_root.joinpath(*PurePosixPath(relative_path).parts))
            for relative_path in normalized_paths
        )
        budget = _ArtifactVerificationBudget(
            max_bytes=self.max_step_bytes,
            deadline=self.monotonic() + self.max_verification_seconds,
            monotonic=self.monotonic,
        )

        with ExitStack() as leases:
            batch_factory = getattr(self.owner_resolver, "batch", None)
            if selected and callable(batch_factory):
                leases.enter_context(batch_factory())
            verified_list: list[_VerifiedBundle] = []
            content_paths: dict[str, str] = {}
            unchanged = 0
            registered_objects = 0
            registered_copies = 0
            registered_references = 0
            for relative_path, path in selected:
                self._root_authority.rebind_outer_ancestor_ctime()
                try:
                    item = self._verify_bundle(relative_path, path, budget=budget)
                except _ArtifactVerificationStepBudgetError as exc:
                    if not verified_list:
                        raise LabArtifactCatalogIntegrityError(str(exc)) from exc
                    break
                try:
                    existing = content_paths.setdefault(
                        item.content_sha256,
                        item.relative_path,
                    )
                    if existing != item.relative_path:
                        raise LabArtifactCatalogIntegrityError(
                            "the same bundle content appears at multiple storage paths"
                        )
                    object_count, copy_count, reference_count = self._register_verified(item)
                    registered_objects += object_count
                    registered_copies += copy_count
                    registered_references += reference_count
                    if not (object_count or copy_count or reference_count):
                        unchanged += 1
                    verified_list.append(item)
                finally:
                    os.close(item.descriptor)
            verified = tuple(verified_list)
        completed_at = normalize_aware_utc(self.clock())
        has_more = len(verified) < len(selected)
        return LabArtifactCatalogRunResult(
            status="partial" if has_more else "completed",
            completed_at=completed_at,
            scanned_bundles=len(verified),
            registered_objects=registered_objects,
            registered_copies=registered_copies,
            registered_references=registered_references,
            unchanged_bundles=unchanged,
            total_bytes=sum(item.size_bytes for item in verified),
            content_hashes=tuple(item.content_sha256 for item in verified),
            has_more=has_more,
            next_cursor=normalized_paths[len(verified)] if has_more else None,
        )

    @staticmethod
    def _validate_byte_budget(value: int, *, label: str, upper_bound: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= upper_bound:
            raise ValueError(f"{label} must be a positive bounded integer")

    @staticmethod
    def _validated_bundle_path(value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or not value or any(part in {"", ".", ".."} for part in path.parts):
            raise ValueError("bundle path must be a safe relative path")
        return path.as_posix()

    def scan_directory_page(
        self,
        frontier: LabArtifactDirectoryFrontier,
        *,
        max_entries: int,
    ) -> LabArtifactDirectoryScanPage:
        """Consume a bounded direct-directory page from a durable frontier."""

        if isinstance(max_entries, bool) or max_entries < 1:
            raise ValueError("max_entries must be a positive integer")
        self._root_authority.rebind_outer_ancestor_ctime()
        relative_directory = self._validated_frontier_directory(frontier)
        directory = self.artifact_root.joinpath(*relative_directory.parts)
        ancestors_before = self._ancestor_identities(directory)
        before = self._require_safe_directory(directory, label="artifact discovery directory")
        expected_identity = (frontier.directory_device, frontier.directory_inode)
        if expected_identity != (None, None) and expected_identity != (
            before.st_dev,
            before.st_ino,
        ):
            raise LabArtifactCatalogIntegrityError(
                "artifact discovery directory changed across bounded scans"
            )

        descriptor = -1
        try:
            descriptor = os.open(directory, _DIRECTORY_FLAGS)
            opened = os.fstat(descriptor)
            self._assert_same_node(before, opened, label="artifact discovery directory")
            buffered = list(frontier.buffered_entry_names)
            offset = frontier.directory_offset
            reached_end = False
            empty_reads = 0
            chunk_bytes = 64 if frontier.directory_kind in {"jobs", "shards"} else 104
            while len(buffered) < max_entries and not reached_end:
                names, offset, reached_end = _read_directory_entry_chunk(
                    descriptor,
                    offset,
                    chunk_bytes=chunk_bytes,
                )
                buffered.extend(names)
                if names:
                    empty_reads = 0
                else:
                    empty_reads += 1
                    if not reached_end and empty_reads > 2:
                        raise LabArtifactCatalogIntegrityError(
                            "artifact discovery directory cursor made no progress"
                        )

            selected = tuple(buffered[:max_entries])
            deferred = tuple(buffered[max_entries:])
            children: list[LabArtifactChildDirectory] = []
            bundles: list[str] = []
            for name in selected:
                child, bundle = self._classify_discovery_entry(
                    descriptor,
                    relative_directory,
                    frontier.directory_kind,
                    name,
                )
                if child is not None:
                    children.append(child)
                if bundle is not None:
                    bundles.append(bundle)

            after_open = os.fstat(descriptor)
            self._assert_same_node(opened, after_open, label="artifact discovery directory")
            after_path = os.lstat(directory)
            self._assert_same_node(opened, after_path, label="artifact discovery path")
            if self._ancestor_identities(directory) != ancestors_before:
                raise LabArtifactCatalogIntegrityError(
                    "artifact discovery ancestor changed during bounded scan"
                )
        except LabArtifactCatalogIntegrityError:
            raise
        except OSError as exc:
            raise LabArtifactCatalogIntegrityError(
                "artifact discovery directory changed during bounded scan"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

        return LabArtifactDirectoryScanPage(
            frontier_sequence=frontier.frontier_sequence,
            frontier_revision=frontier.revision,
            directory_device=before.st_dev,
            directory_inode=before.st_ino,
            directory_offset=offset,
            buffered_entry_names=deferred,
            exhausted=reached_end and not deferred,
            scanned_entries=len(selected),
            child_directories=tuple(children),
            bundle_paths=tuple(bundles),
        )

    def _validated_frontier_directory(
        self,
        frontier: LabArtifactDirectoryFrontier,
    ) -> PurePosixPath:
        path = PurePosixPath(frontier.relative_directory)
        expected_parts: dict[DirectoryKind, int] = {
            "jobs": 1,
            "shards": 3,
            "attempts": 5,
        }
        if (
            path.is_absolute()
            or len(path.parts) != expected_parts[frontier.directory_kind]
            or path.parts[0] != "jobs"
            or (frontier.directory_kind != "jobs" and path.parts[2] != "shards")
            or (frontier.directory_kind == "attempts" and path.parts[4] != "attempts")
        ):
            raise LabArtifactCatalogIntegrityError("artifact discovery frontier is malformed")
        if frontier.directory_kind != "jobs":
            self._canonical_uuid(path.parts[1], label="job")
        if frontier.directory_kind == "attempts":
            self._canonical_uuid(path.parts[3], label="shard")
        return path

    def _classify_discovery_entry(
        self,
        parent_descriptor: int,
        parent: PurePosixPath,
        directory_kind: DirectoryKind,
        name: str,
    ) -> tuple[LabArtifactChildDirectory | None, str | None]:
        if not name or PurePosixPath(name).name != name or name in {".", ".."}:
            raise LabArtifactCatalogIntegrityError("artifact discovery entry name is unsafe")
        try:
            observed = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        except OSError as exc:
            raise LabArtifactCatalogIntegrityError("artifact discovery entry changed") from exc
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) & 0o022
        ):
            raise LabArtifactCatalogIntegrityError("artifact discovery entry is unsafe")

        if directory_kind == "jobs":
            job_id = self._canonical_uuid(name, label="job")
            return (
                LabArtifactChildDirectory(
                    relative_directory=(parent / str(job_id) / "shards").as_posix(),
                    directory_kind="shards",
                ),
                None,
            )
        if directory_kind == "shards":
            shard_id = self._canonical_uuid(name, label="shard")
            return (
                LabArtifactChildDirectory(
                    relative_directory=(parent / str(shard_id) / "attempts").as_posix(),
                    directory_kind="attempts",
                ),
                None,
            )
        if name.startswith("."):
            return None, None
        match = _ATTEMPT_PATTERN.fullmatch(name)
        if match is None:
            raise LabArtifactCatalogIntegrityError(
                f"sealed attempt has an invalid identity: {name}"
            )
        if int(match.group("fence")) < 1 or int(match.group("generation")) < 1:
            raise LabArtifactCatalogIntegrityError(
                "sealed attempt fence and generation must be positive"
            )
        UUID(match.group("token"))
        return None, (parent / name).as_posix()

    def _verify_bundle(
        self,
        relative_path: str,
        path: Path,
        *,
        budget: _ArtifactVerificationBudget,
    ) -> _VerifiedBundle:
        bundle_deadline = min(
            budget.deadline,
            self.monotonic() + self.max_bundle_verification_seconds,
        )
        parts = PurePosixPath(relative_path).parts
        if len(parts) != 6 or parts[0] != "jobs" or parts[2] != "shards" or parts[4] != "attempts":
            raise LabArtifactCatalogIntegrityError("sealed bundle path identity is malformed")
        job_id = self._canonical_uuid(parts[1], label="job")
        shard_id = self._canonical_uuid(parts[3], label="shard")
        attempt = _ATTEMPT_PATTERN.fullmatch(parts[5])
        if attempt is None:
            raise LabArtifactCatalogIntegrityError("sealed bundle attempt identity is malformed")

        ancestors_before = self._ancestor_identities(path)
        before = self._require_safe_directory(path, label="sealed bundle")
        descriptor = -1
        try:
            descriptor = os.open(path, _DIRECTORY_FLAGS)
            opened = os.fstat(descriptor)
            self._assert_same_node(before, opened, label="sealed bundle")
            names = tuple(sorted(os.listdir(descriptor)))
            if "manifest.json" not in names:
                raise LabArtifactCatalogIntegrityError("sealed bundle has no manifest.json")
            raw, manifest_identity = self._read_regular_file(
                descriptor,
                "manifest.json",
                max_bytes=min(
                    self.max_manifest_bytes,
                    self.max_artifact_file_bytes,
                ),
                max_bundle_bytes=self.max_bundle_bytes,
                budget=budget,
                deadline=self.monotonic() + self.max_artifact_file_verification_seconds,
                bundle_deadline=bundle_deadline,
                time_scope="artifact file",
            )
            try:
                manifest = strict_model_validate_canonical_json(LabShardResultManifest, raw)
            except Exception as exc:
                raise LabArtifactCatalogIntegrityError(
                    f"sealed manifest is invalid: {type(exc).__name__}"
                ) from exc
            expected_attempt = (
                f"{manifest.scheduler_fencing_token:020d}-"
                f"{manifest.claim_generation:020d}-{manifest.claim_token}"
            )
            if (
                manifest.job_id != job_id
                or manifest.shard_id != shard_id
                or expected_attempt != path.name
            ):
                raise LabArtifactCatalogIntegrityError(
                    "sealed manifest path identity does not match its typed identity"
                )
            expected_names = tuple(
                sorted(("manifest.json", *(item.file_name for item in manifest.artifacts)))
            )
            if names != expected_names:
                raise LabArtifactCatalogIntegrityError(
                    "sealed bundle inventory conflicts with its manifest"
                )
            total_size = len(raw)
            declared_payload_bytes = sum(item.file_size for item in manifest.artifacts)
            if total_size + declared_payload_bytes > self.max_bundle_bytes:
                raise LabArtifactCatalogIntegrityError("sealed bundle exceeds its byte budget")
            if any(item.file_size > self.max_artifact_file_bytes for item in manifest.artifacts):
                raise LabArtifactCatalogIntegrityError(
                    "sealed artifact file exceeds its single-file byte budget"
                )
            budget.ensure_capacity(declared_payload_bytes)
            file_identities = {"manifest.json": manifest_identity}
            for artifact in manifest.artifacts:
                payload_size, payload_hash, payload_identity = self._hash_regular_file(
                    descriptor,
                    artifact.file_name,
                    max_bytes=self.max_artifact_file_bytes,
                    max_bundle_bytes=self.max_bundle_bytes - total_size,
                    budget=budget,
                    deadline=self.monotonic() + self.max_artifact_file_verification_seconds,
                    bundle_deadline=bundle_deadline,
                    time_scope="artifact file",
                )
                if (payload_size, payload_hash) != (
                    artifact.file_size,
                    artifact.file_sha256,
                ):
                    raise LabArtifactCatalogIntegrityError(
                        f"sealed artifact bytes or hash conflict: {artifact.file_name}"
                    )
                total_size += payload_size
                file_identities[artifact.file_name] = payload_identity
            after_open = os.fstat(descriptor)
            self._assert_same_node(opened, after_open, label="sealed bundle")
            after_path = os.lstat(path)
            self._assert_same_node(opened, after_path, label="sealed bundle path")
            if self._ancestor_identities(path) != ancestors_before:
                raise LabArtifactCatalogIntegrityError(
                    "sealed bundle ancestor changed identity while scanning"
                )

            def identity_guard() -> None:
                self._assert_bundle_lease_current(
                    path=path,
                    descriptor=descriptor,
                    directory_identity=opened,
                    ancestor_identities=ancestors_before,
                    inventory=names,
                    file_identities=file_identities,
                )

            try:
                owners = self.owner_resolver(manifest)
            except Exception as exc:
                raise LabArtifactCatalogIntegrityError("owner binding resolver failed") from exc
            budget.check_deadline(bundle_deadline, label="artifact bundle")
            if not isinstance(owners, LabArtifactDurableOwners) or (
                owners.job_id,
                owners.spec_hash,
                owners.plan_hash,
            ) != (manifest.job_id, manifest.spec_hash, manifest.plan_hash):
                raise LabArtifactCatalogIntegrityError(
                    "owner binding conflicts with the typed shard manifest"
                )
            identity_guard()
        except LabArtifactCatalogIntegrityError:
            if descriptor >= 0:
                os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor >= 0:
                os.close(descriptor)
            raise LabArtifactCatalogIntegrityError("sealed bundle changed while scanning") from exc
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
            raise
        return _VerifiedBundle(
            path=path,
            relative_path=relative_path,
            manifest=manifest,
            content_sha256=manifest.manifest_hash,
            size_bytes=total_size,
            owners=owners,
            descriptor=descriptor,
            identity_guard=identity_guard,
        )

    def _register_verified(self, bundle: _VerifiedBundle) -> tuple[int, int, int]:
        now = normalize_aware_utc(self.clock())
        counts = self.reference_store.register_bundle_atomic(
            ArtifactBundleRegistration(
                object_identity=ObjectIdentity(
                    content_sha256=bundle.content_sha256,
                    size_bytes=bundle.size_bytes,
                    object_kind="strategy_lab_shard_result_bundle",
                    created_at=now,
                ),
                object_copy=ObjectCopy(
                    content_sha256=bundle.content_sha256,
                    location_id=self.location_id,
                    storage_uri=bundle.path.as_uri(),
                    storage_tier=StorageTier.HOT,
                    verified_at=now,
                    failure_domain=self.failure_domain,
                    tier_entered_at=now,
                ),
                references=tuple(
                    ObjectReference(
                        owner_type=owner_type,
                        owner_id=owner_id,
                        content_sha256=bundle.content_sha256,
                        created_at=now,
                    )
                    for owner_type, owner_id in (
                        ("audit", bundle.owners.audit_run_id),
                        ("experiment", bundle.owners.experiment_id),
                        ("job", str(bundle.owners.job_id)),
                        ("snapshot", bundle.owners.snapshot_id),
                    )
                ),
            ),
            identity_guard=bundle.identity_guard,
        )
        bundle.identity_guard()
        if self.terminal_owner_releaser is not None:
            self.terminal_owner_releaser(bundle.manifest, bundle.owners, now)
        return (
            counts.registered_objects,
            counts.registered_copies,
            counts.registered_references,
        )

    @staticmethod
    def _canonical_uuid(value: str, *, label: str) -> UUID:
        try:
            parsed = UUID(value)
        except ValueError as exc:
            raise LabArtifactCatalogIntegrityError(f"{label} directory is not a UUID") from exc
        if str(parsed) != value:
            raise LabArtifactCatalogIntegrityError(f"{label} UUID is not canonical")
        return parsed

    def _ancestor_identities(self, path: Path) -> tuple[tuple[int, int, int], ...]:
        self._root_authority.assert_current()
        try:
            relative = path.relative_to(self.artifact_root)
        except ValueError as exc:
            raise LabArtifactCatalogIntegrityError(
                "sealed bundle escapes the configured artifact root"
            ) from exc
        current = self.artifact_root
        identities = list(self._root_authority.generation)
        for part in relative.parts:
            current /= part
            observed = self._require_safe_directory(current, label="artifact ancestor")
            identities.append((observed.st_dev, observed.st_ino, observed.st_ctime_ns))
        self._root_authority.assert_current()
        return tuple(identities)

    @staticmethod
    def _require_safe_directory(path: Path, *, label: str) -> os.stat_result:
        try:
            observed = os.lstat(path)
        except FileNotFoundError as exc:
            raise LabArtifactFrontierMissingError(f"{label} is missing") from exc
        except OSError as exc:
            raise LabArtifactCatalogIntegrityError(f"{label} is missing") from exc
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise LabArtifactCatalogIntegrityError(f"{label} is a symlink or unsafe")
        if observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) & 0o022:
            raise LabArtifactCatalogIntegrityError(f"{label} permissions or owner are unsafe")
        return observed

    @staticmethod
    def _assert_same_node(
        expected: os.stat_result,
        actual: os.stat_result,
        *,
        label: str,
    ) -> None:
        if (
            expected.st_dev,
            expected.st_ino,
            expected.st_ctime_ns,
            stat.S_IFMT(expected.st_mode),
        ) != (
            actual.st_dev,
            actual.st_ino,
            actual.st_ctime_ns,
            stat.S_IFMT(actual.st_mode),
        ):
            raise LabArtifactCatalogIntegrityError(f"{label} changed identity while scanning")

    @staticmethod
    def _open_safe_file(parent_descriptor: int, name: str) -> tuple[int, os.stat_result]:
        if PurePosixPath(name).name != name:
            raise LabArtifactCatalogIntegrityError("artifact file name escapes its bundle")
        descriptor = os.open(name, _FILE_FLAGS, dir_fd=parent_descriptor)
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_uid != os.geteuid()
        ):
            os.close(descriptor)
            raise LabArtifactCatalogIntegrityError("artifact file is unsafe or has a hard link")
        return descriptor, observed

    def _read_regular_file(
        self,
        parent_descriptor: int,
        name: str,
        *,
        max_bytes: int,
        max_bundle_bytes: int,
        budget: _ArtifactVerificationBudget,
        deadline: float,
        bundle_deadline: float,
        time_scope: str,
    ) -> tuple[bytes, _FileIdentity]:
        descriptor, before = self._open_safe_file(parent_descriptor, name)
        try:
            if before.st_size > max_bytes:
                raise LabArtifactCatalogIntegrityError("sealed manifest exceeds its size budget")
            if before.st_size > max_bundle_bytes:
                raise LabArtifactCatalogIntegrityError("sealed bundle exceeds its byte budget")
            budget.ensure_capacity(before.st_size)
            payload = self._read_descriptor(
                descriptor,
                max_bytes=max_bytes,
                budget=budget,
                deadline=deadline,
                bundle_deadline=bundle_deadline,
                time_scope=time_scope,
            )
            after = os.fstat(descriptor)
            self._assert_unchanged_file(before, after, label=name)
            if len(payload) != before.st_size:
                raise LabArtifactCatalogIntegrityError(
                    f"artifact file changed while reading: {name}"
                )
            return payload, self._file_identity(after)
        finally:
            os.close(descriptor)

    def _hash_regular_file(
        self,
        parent_descriptor: int,
        name: str,
        *,
        max_bytes: int,
        max_bundle_bytes: int,
        budget: _ArtifactVerificationBudget,
        deadline: float,
        bundle_deadline: float,
        time_scope: str,
    ) -> tuple[int, str, _FileIdentity]:
        descriptor, before = self._open_safe_file(parent_descriptor, name)
        try:
            if before.st_size > max_bytes:
                raise LabArtifactCatalogIntegrityError(
                    "sealed artifact file exceeds its single-file byte budget"
                )
            if before.st_size > max_bundle_bytes:
                raise LabArtifactCatalogIntegrityError("sealed bundle exceeds its byte budget")
            budget.ensure_capacity(before.st_size)
            digest = hashlib.sha256()
            size = 0
            while True:
                budget.check_deadlines(
                    (
                        (bundle_deadline, "artifact bundle"),
                        (deadline, time_scope),
                    )
                )
                read_size = min(
                    _HASH_CHUNK_BYTES,
                    max_bytes + 1 - size,
                    max_bundle_bytes + 1 - size,
                    budget.max_bytes + 1 - budget.consumed_bytes,
                )
                chunk = os.read(descriptor, read_size)
                budget.check_deadlines(
                    (
                        (bundle_deadline, "artifact bundle"),
                        (deadline, time_scope),
                    )
                )
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    raise LabArtifactCatalogIntegrityError(
                        "sealed artifact file exceeds its single-file byte budget"
                    )
                if size > max_bundle_bytes:
                    raise LabArtifactCatalogIntegrityError("sealed bundle exceeds its byte budget")
                budget.consume(len(chunk))
                digest.update(chunk)
            after = os.fstat(descriptor)
            self._assert_unchanged_file(before, after, label=name)
            if size != before.st_size:
                raise LabArtifactCatalogIntegrityError(
                    f"artifact file changed while hashing: {name}"
                )
            return size, digest.hexdigest(), self._file_identity(after)
        finally:
            os.close(descriptor)

    @staticmethod
    def _read_descriptor(
        descriptor: int,
        *,
        max_bytes: int,
        budget: _ArtifactVerificationBudget,
        deadline: float,
        bundle_deadline: float,
        time_scope: str,
    ) -> bytes:
        chunks: list[bytes] = []
        size = 0
        while True:
            budget.check_deadlines(
                (
                    (bundle_deadline, "artifact bundle"),
                    (deadline, time_scope),
                )
            )
            chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - size))
            budget.check_deadlines(
                (
                    (bundle_deadline, "artifact bundle"),
                    (deadline, time_scope),
                )
            )
            if not chunk:
                return b"".join(chunks)
            size += len(chunk)
            if size > max_bytes:
                raise LabArtifactCatalogIntegrityError("sealed manifest exceeds its size budget")
            budget.consume(len(chunk))
            chunks.append(chunk)

    @staticmethod
    def _assert_unchanged_file(
        before: os.stat_result,
        after: os.stat_result,
        *,
        label: str,
    ) -> None:
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_nlink,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_nlink,
            after.st_ctime_ns,
        ):
            raise LabArtifactCatalogIntegrityError(f"artifact file changed while reading: {label}")

    @staticmethod
    def _file_identity(value: os.stat_result) -> _FileIdentity:
        return (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_nlink,
            value.st_ctime_ns,
            value.st_mode,
            value.st_uid,
        )

    def _assert_bundle_lease_current(
        self,
        *,
        path: Path,
        descriptor: int,
        directory_identity: os.stat_result,
        ancestor_identities: tuple[tuple[int, int, int], ...],
        inventory: tuple[str, ...],
        file_identities: dict[str, _FileIdentity],
    ) -> None:
        try:
            self._root_authority.rebind_outer_ancestor_ctime()
            root_generation = self._root_authority.generation
            expected_ancestors = (
                *root_generation[:-1],
                *ancestor_identities[len(root_generation) - 1 :],
            )
            opened = os.fstat(descriptor)
            self._assert_same_node(
                directory_identity,
                opened,
                label="sealed bundle descriptor",
            )
            current_path = os.lstat(path)
            self._assert_same_node(opened, current_path, label="sealed bundle path")
            if self._ancestor_identities(path) != expected_ancestors:
                raise LabArtifactCatalogIntegrityError(
                    "sealed bundle ancestor changed while leased"
                )
            if tuple(sorted(os.listdir(descriptor))) != inventory:
                raise LabArtifactCatalogIntegrityError(
                    "sealed bundle inventory changed while leased"
                )
            for name, expected in file_identities.items():
                current_descriptor, current = self._open_safe_file(descriptor, name)
                try:
                    if self._file_identity(current) != expected:
                        raise LabArtifactCatalogIntegrityError(
                            f"sealed artifact replacement detected: {name}"
                        )
                finally:
                    os.close(current_descriptor)
        except LabArtifactCatalogIntegrityError:
            raise
        except OSError as exc:
            raise LabArtifactCatalogIntegrityError(
                "sealed bundle changed while registration was in progress"
            ) from exc


def _read_directory_entry_chunk(
    descriptor: int,
    offset: int,
    *,
    chunk_bytes: int = 4 * 1024,
) -> tuple[tuple[str, ...], int, bool]:
    """Read one direct-directory block and return a restart-stable OS offset."""

    if offset < 0 or chunk_bytes < 32:
        raise ValueError("directory cursor and chunk size must be valid")
    try:
        os.lseek(descriptor, offset, os.SEEK_SET)
    except OSError as exc:
        raise LabArtifactCatalogIntegrityError("artifact directory cursor is invalid") from exc

    libc = ctypes.CDLL(None, use_errno=True)
    symbol = "__getdirentries64" if sys.platform == "darwin" else "getdirentries64"
    try:
        getdirentries = getattr(libc, symbol)
    except AttributeError as exc:
        raise LabArtifactCatalogIntegrityError(
            "durable directory cursors are unsupported on this platform"
        ) from exc
    getdirentries.argtypes = [
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_longlong),
    ]
    getdirentries.restype = ctypes.c_ssize_t
    buffer = ctypes.create_string_buffer(chunk_bytes)
    base_offset = ctypes.c_longlong(offset)
    bytes_read = getdirentries(
        descriptor,
        buffer,
        chunk_bytes,
        ctypes.byref(base_offset),
    )
    if bytes_read < 0:
        error_number = ctypes.get_errno()
        raise LabArtifactCatalogIntegrityError(
            f"cannot read artifact directory entries: errno={error_number}"
        )
    try:
        next_offset = os.lseek(descriptor, 0, os.SEEK_CUR)
    except OSError as exc:
        raise LabArtifactCatalogIntegrityError(
            "cannot checkpoint artifact directory cursor"
        ) from exc
    if bytes_read == 0:
        return (), next_offset, True
    if next_offset == offset:
        raise LabArtifactCatalogIntegrityError("artifact directory cursor did not advance")

    names = _parse_directory_entry_block(
        memoryview(buffer.raw)[:bytes_read],
        platform="darwin" if sys.platform == "darwin" else "linux",
    )
    return names, next_offset, False


def _parse_directory_entry_block(
    payload: bytes | memoryview,
    *,
    platform: Literal["darwin", "linux"],
) -> tuple[str, ...]:
    view = memoryview(payload)
    bytes_read = len(view)
    names: list[str] = []
    position = 0
    name_offset = 21 if platform == "darwin" else 19
    while position < bytes_read:
        if platform == "darwin":
            if bytes_read - position < name_offset:
                raise LabArtifactCatalogIntegrityError("artifact directory record is truncated")
            _inode, _seek, record_length, name_length, _entry_type = struct.unpack_from(
                "=QQHHB", view, position
            )
        else:
            if bytes_read - position < name_offset:
                raise LabArtifactCatalogIntegrityError("artifact directory record is truncated")
            _inode, _seek, record_length, _entry_type = struct.unpack_from("=QQHB", view, position)
            terminator = bytes(view[position + name_offset : position + record_length]).find(b"\0")
            if terminator < 0:
                raise LabArtifactCatalogIntegrityError(
                    "artifact directory record has no name terminator"
                )
            name_length = terminator
        if (
            record_length <= name_offset
            or position + record_length > bytes_read
            or name_length < 1
            or name_offset + name_length >= record_length
        ):
            raise LabArtifactCatalogIntegrityError("artifact directory record is invalid")
        raw_name = bytes(view[position + name_offset : position + name_offset + name_length])
        try:
            name = os.fsdecode(raw_name)
        except UnicodeError as exc:
            raise LabArtifactCatalogIntegrityError(
                "artifact directory entry name is not decodable"
            ) from exc
        if name not in {".", ".."}:
            names.append(name)
        position += record_length
    if position != bytes_read:
        raise LabArtifactCatalogIntegrityError("artifact directory block is malformed")
    return tuple(names)
