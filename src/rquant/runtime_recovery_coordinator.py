"""Immutable recovery objects and isolated restore rehearsals."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Decimal
from enum import StrEnum
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self

import duckdb
from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.executable_dependencies import (
    DependencyFingerprintLimits,
    ExecutableBinding,
    ExecutableDependencyError,
    ExecutableDependencyGuard,
    capture_executable_dependency_guard,
)
from rquant.recovery_manifest import (
    RecoveryArtifactEntry,
    RecoveryArtifactRole,
    RecoveryFaultPoint,
    RecoveryInventoryPlan,
    RecoveryManifest,
    RecoveryWatermarkSummary,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_production_profile import ProductionStrategyBinding
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

if TYPE_CHECKING:
    from rquant.runtime_recovery_artifacts import FixedReplayReceipt

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_CHUNK_SIZE = 1024 * 1024
_MAX_DOCUMENT_SIZE = 16 * 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY_MUTATION_LOCK = threading.RLock()


class RuntimeRecoveryCoordinatorError(RuntimeError):
    """A recovery source, object, manifest, or rehearsal failed closed."""


class RecoveryPlane(StrEnum):
    DATA = "data"
    LIVE = "live"
    CONTROL = "control"
    SERVING = "serving"
    RESEARCH = "research"


def _canonical_absolute_path(value: str | Path, *, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ValueError(f"{label} must be absolute and lexically canonical")
    return path


def _absolute_path(value: str) -> str:
    return str(_canonical_absolute_path(value, label="path"))


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _identity(observed: os.stat_result) -> tuple[int, int, int, int, int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_gid,
        observed.st_nlink,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _file_binding(observed: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        stat.S_IFMT(observed.st_mode),
        observed.st_uid,
        observed.st_gid,
        stat.S_IMODE(observed.st_mode),
        observed.st_nlink,
        observed.st_ctime_ns,
    )


def _file_object_binding(observed: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return _file_binding(observed)[:-1]


def _directory_binding(
    observed: os.stat_result,
) -> tuple[int, int, int, int, int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        stat.S_IFMT(observed.st_mode),
        observed.st_uid,
        observed.st_gid,
        stat.S_IMODE(observed.st_mode),
        observed.st_nlink,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _binding_with_nlink(
    baseline: tuple[int, int, int, int, int, int, int, int, int],
    nlink: int,
) -> tuple[int, int, int, int, int, int, int, int, int]:
    return (*baseline[:6], nlink, *baseline[7:])


def _record_owned_directory_entry(
    *,
    baselines: list[tuple[int, int, int, int, int, int, int, int, int]],
    expected_nlinks: list[int],
    index: int,
    before: os.stat_result,
    after: os.stat_result,
    allowed_nlink_deltas: set[int],
    label: str,
) -> None:
    before_binding = _directory_binding(before)
    after_binding = _directory_binding(after)
    expected_before = _binding_with_nlink(baselines[index], expected_nlinks[index])
    if before_binding != expected_before:
        raise RuntimeRecoveryCoordinatorError(f"{label} directory binding changed")
    nlink_delta = after_binding[6] - before_binding[6]
    if after_binding[:6] != before_binding[:6] or nlink_delta not in allowed_nlink_deltas:
        raise RuntimeRecoveryCoordinatorError(f"{label} directory binding changed")
    baselines[index] = after_binding
    expected_nlinks[index] = after_binding[6]


class _DirectoryLease:
    def __init__(
        self,
        *,
        path: Path | None,
        descriptors: list[int],
        names: list[str],
        baselines: list[tuple[int, int, int, int, int, int, int, int, int]],
        expected_nlinks: list[int],
    ) -> None:
        self.path = path
        self._descriptors = descriptors
        self._names = names
        self._baselines = baselines
        self._expected_nlinks = expected_nlinks

    @property
    def descriptor(self) -> int:
        return self._descriptors[-1]

    def verify(self, *, label: str) -> None:
        display = self.path if self.path is not None else "relative directory"
        try:
            for index, descriptor in enumerate(self._descriptors):
                current = _directory_binding(os.fstat(descriptor))
                expected = _binding_with_nlink(self._baselines[index], self._expected_nlinks[index])
                if current != expected:
                    raise RuntimeRecoveryCoordinatorError(
                        f"{label} directory binding changed: {display}"
                    )
            for index, (parent, name) in enumerate(
                zip(self._descriptors[:-1], self._names, strict=True),
                start=1,
            ):
                current = os.stat(name, dir_fd=parent, follow_symlinks=False)
                expected = _binding_with_nlink(self._baselines[index], self._expected_nlinks[index])
                observed = _directory_binding(current)
                if observed != expected:
                    raise RuntimeRecoveryCoordinatorError(
                        f"{label} directory binding changed: {display}"
                    )
        except OSError as exc:
            raise RuntimeRecoveryCoordinatorError(
                f"{label} directory binding changed: {display}"
            ) from exc

    def record_owned_entry_creation(
        self,
        *,
        before: os.stat_result,
        after: os.stat_result,
        label: str,
    ) -> None:
        _record_owned_directory_entry(
            baselines=self._baselines,
            expected_nlinks=self._expected_nlinks,
            index=len(self._descriptors) - 1,
            before=before,
            after=after,
            allowed_nlink_deltas={0, 1},
            label=label,
        )
        self.verify(label=label)

    def record_owned_entry_deletion(
        self,
        *,
        before: os.stat_result,
        after: os.stat_result,
        label: str,
    ) -> None:
        _record_owned_directory_entry(
            baselines=self._baselines,
            expected_nlinks=self._expected_nlinks,
            index=len(self._descriptors) - 1,
            before=before,
            after=after,
            allowed_nlink_deltas={-1, 0},
            label=label,
        )
        self.verify(label=label)

    def record_owned_entry_rebind(
        self,
        *,
        before: os.stat_result,
        after: os.stat_result,
        label: str,
    ) -> None:
        _record_owned_directory_entry(
            baselines=self._baselines,
            expected_nlinks=self._expected_nlinks,
            index=len(self._descriptors) - 1,
            before=before,
            after=after,
            # APFS decrements a directory's reported nlink when replace(2)
            # atomically consumes a temporary name over an existing entry.
            allowed_nlink_deltas={-1, 0},
            label=label,
        )
        self.verify(label=label)

    def close(self) -> None:
        while self._descriptors:
            os.close(self._descriptors.pop())


def _verify_directory_binding(lease: _DirectoryLease, *, label: str) -> None:
    lease.verify(label=label)


def _open_physical_directory(
    path: Path,
    *,
    create: bool,
    label: str,
    trust_root: Path | None = None,
) -> _DirectoryLease:
    canonical = _canonical_absolute_path(path, label=label)
    trusted = _canonical_absolute_path(
        trust_root if trust_root is not None else (canonical.parent if create else canonical),
        label=f"{label} trust_root",
    )
    if not canonical.is_relative_to(trusted):
        raise RuntimeRecoveryCoordinatorError(f"{label} escapes its explicit trust root")
    descriptors = [os.open(canonical.anchor, _DIRECTORY_FLAGS)]
    names: list[str] = []
    baselines = [_directory_binding(os.fstat(descriptors[0]))]
    expected_nlinks = [baselines[0][6]]
    current = Path(canonical.anchor)
    try:
        for part in canonical.parts[1:]:
            parent = descriptors[-1]
            parent_before = os.fstat(parent)
            parent_is_managed = current == trusted or current.is_relative_to(trusted)
            missing = False
            try:
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=parent)
            except FileNotFoundError:
                if not create:
                    raise RuntimeRecoveryCoordinatorError(f"{label} missing: {canonical}") from None
                missing = True
                with suppress(FileExistsError):
                    os.mkdir(part, mode=0o700, dir_fd=parent)
                try:
                    child = os.open(part, _DIRECTORY_FLAGS, dir_fd=parent)
                except OSError as exc:
                    raise RuntimeRecoveryCoordinatorError(
                        f"cannot open physical {label}: {canonical}"
                    ) from exc
            except OSError as exc:
                raise RuntimeRecoveryCoordinatorError(
                    f"cannot open physical {label}: {canonical}"
                ) from exc
            if missing and parent_is_managed:
                _record_owned_directory_entry(
                    baselines=baselines,
                    expected_nlinks=expected_nlinks,
                    index=len(descriptors) - 1,
                    before=parent_before,
                    after=os.fstat(parent),
                    allowed_nlink_deltas={0, 1},
                    label=label,
                )
            expected_parent = _binding_with_nlink(baselines[-1], expected_nlinks[-1])
            if parent_is_managed and _directory_binding(os.fstat(parent)) != expected_parent:
                os.close(child)
                raise RuntimeRecoveryCoordinatorError(
                    f"{label} parent directory binding changed: {current}"
                )
            child_baseline = _directory_binding(os.fstat(child))
            descriptors.append(child)
            names.append(part)
            baselines.append(child_baseline)
            expected_nlinks.append(child_baseline[6])
            current /= part
        trusted_index = len(trusted.parts) - 1
        if trusted_index >= len(descriptors):
            raise RuntimeRecoveryCoordinatorError(f"{label} trust root was not opened")
        for descriptor in descriptors[:trusted_index]:
            os.close(descriptor)
        descriptors = descriptors[trusted_index:]
        names = names[trusted_index:]
        baselines = baselines[trusted_index:]
        expected_nlinks = expected_nlinks[trusted_index:]
        lease = _DirectoryLease(
            path=canonical,
            descriptors=descriptors,
            names=names,
            baselines=baselines,
            expected_nlinks=expected_nlinks,
        )
        _verify_directory_binding(lease, label=label)
        return lease
    except Exception:
        while descriptors:
            os.close(descriptors.pop())
        raise


def _verify_regular_binding(
    *,
    name: str,
    descriptor: int,
    parent_lease: _DirectoryLease,
    label: str,
    expected: tuple[int, int, int, int, int, int, int, int] | None = None,
) -> os.stat_result:
    _verify_directory_binding(parent_lease, label=f"{label} parent")
    try:
        directory_entry = os.stat(
            name,
            dir_fd=parent_lease.descriptor,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise RuntimeRecoveryCoordinatorError(f"{label} changed after open") from exc
    opened = os.fstat(descriptor)
    if not stat.S_ISREG(directory_entry.st_mode) or not stat.S_ISREG(opened.st_mode):
        raise RuntimeRecoveryCoordinatorError(f"{label} is not a regular file")
    if directory_entry.st_nlink != 1 or opened.st_nlink != 1:
        raise RuntimeRecoveryCoordinatorError(f"{label} is a hardlink")
    observed = _file_binding(opened)
    if _file_binding(directory_entry) != observed or (
        expected is not None and observed != expected
    ):
        raise RuntimeRecoveryCoordinatorError(f"{label} changed after open")
    _verify_directory_binding(parent_lease, label=f"{label} parent")
    return opened


def _open_regular_readonly(
    path: Path,
    *,
    label: str,
    trust_root: Path | None = None,
) -> tuple[int, _DirectoryLease]:
    canonical = _canonical_absolute_path(path, label=label)
    parent_lease = _open_physical_directory(
        canonical.parent,
        create=False,
        label=f"{label} parent",
        trust_root=trust_root if trust_root is not None else canonical.parent,
    )
    descriptor = -1
    try:
        try:
            directory_entry = os.stat(
                canonical.name,
                dir_fd=parent_lease.descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeRecoveryCoordinatorError(f"{label} missing: {canonical}") from exc
        if stat.S_ISLNK(directory_entry.st_mode):
            raise RuntimeRecoveryCoordinatorError(f"{label} is a symlink: {canonical}")
        try:
            descriptor = os.open(
                canonical.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_lease.descriptor,
            )
        except FileNotFoundError as exc:
            raise RuntimeRecoveryCoordinatorError(f"{label} missing: {canonical}") from exc
        except OSError as exc:
            raise RuntimeRecoveryCoordinatorError(f"cannot open {label}: {canonical}") from exc
        _verify_regular_binding(
            name=canonical.name,
            descriptor=descriptor,
            parent_lease=parent_lease,
            label=label,
            expected=_file_binding(directory_entry),
        )
        return descriptor, parent_lease
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        parent_lease.close()
        raise


class _FileDigest(RuntimeContractModel):
    size_bytes: int = Field(ge=0)
    sha256: Sha256
    device: int
    inode: int


def _stream_digest_descriptor(descriptor: int, *, label: str) -> _FileDigest:
    before = os.fstat(descriptor)
    digest = hashlib.sha256()
    size = 0
    while chunk := os.read(descriptor, _CHUNK_SIZE):
        digest.update(chunk)
        size += len(chunk)
    after = os.fstat(descriptor)
    if _identity(before) != _identity(after) or size != after.st_size or after.st_nlink != 1:
        raise RuntimeRecoveryCoordinatorError(f"{label} changed while reading")
    return _FileDigest(
        size_bytes=size,
        sha256=digest.hexdigest(),
        device=before.st_dev,
        inode=before.st_ino,
    )


def _inspect_regular_file(
    path: Path,
    *,
    label: str,
    trust_root: Path | None = None,
) -> _FileDigest:
    descriptor, parent_lease = _open_regular_readonly(
        path,
        label=label,
        trust_root=trust_root,
    )
    try:
        before = os.fstat(descriptor)
        inspected = _stream_digest_descriptor(descriptor, label=label)
        after = _verify_regular_binding(
            name=path.name,
            descriptor=descriptor,
            parent_lease=parent_lease,
            label=label,
            expected=_file_binding(before),
        )
        if _identity(before) != _identity(after):
            raise RuntimeRecoveryCoordinatorError(f"{label} changed while reading")
        return inspected
    finally:
        os.close(descriptor)
        parent_lease.close()


def _read_document(
    path: Path,
    *,
    label: str,
    trust_root: Path | None = None,
) -> tuple[bytes, _FileDigest]:
    descriptor, parent_lease = _open_regular_readonly(
        path,
        label=label,
        trust_root=trust_root,
    )
    try:
        before = os.fstat(descriptor)
        if before.st_size > _MAX_DOCUMENT_SIZE:
            raise RuntimeRecoveryCoordinatorError(f"{label} exceeds document size limit")
        digest = hashlib.sha256()
        content = bytearray()
        while chunk := os.read(descriptor, min(_CHUNK_SIZE, _MAX_DOCUMENT_SIZE + 1 - len(content))):
            content.extend(chunk)
            digest.update(chunk)
            if len(content) > _MAX_DOCUMENT_SIZE:
                raise RuntimeRecoveryCoordinatorError(f"{label} exceeds document size limit")
        after = _verify_regular_binding(
            name=path.name,
            descriptor=descriptor,
            parent_lease=parent_lease,
            label=label,
            expected=_file_binding(before),
        )
        if _identity(before) != _identity(after) or len(content) != after.st_size:
            raise RuntimeRecoveryCoordinatorError(f"{label} changed while reading")
        return bytes(content), _FileDigest(
            size_bytes=len(content),
            sha256=digest.hexdigest(),
            device=before.st_dev,
            inode=before.st_ino,
        )
    finally:
        os.close(descriptor)
        parent_lease.close()


class RecoveryAuthorityManifest(RuntimeContractModel):
    authority_id: Sha256 | None = None
    plane: RecoveryPlane
    logical_role: str = Field(min_length=1)
    artifact_role: RecoveryArtifactRole
    artifact_path: str
    artifact_size_bytes: int = Field(ge=0)
    artifact_sha256: Sha256
    producer_commit: CommitSha
    generation_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    available_at: AwareUtcDatetime
    watermark: RecoveryWatermarkSummary

    @field_validator("artifact_path")
    @classmethod
    def validate_artifact_path(cls, value: str) -> str:
        return _absolute_path(value)

    def identity_payload(self) -> dict[str, object]:
        return self.model_dump(mode="python", exclude={"authority_id"})

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected = canonical_sha256(self.identity_payload())
        if self.authority_id is not None and self.authority_id != expected:
            raise ValueError("authority_id does not match authority manifest content")
        object.__setattr__(self, "authority_id", expected)
        return self


class RecoveryAuthorityExpectation(RuntimeContractModel):
    plane: RecoveryPlane
    logical_role: str = Field(min_length=1)
    artifact_role: RecoveryArtifactRole
    authority_manifest_path: str
    allowed_root: str
    expected_producer_commit: CommitSha
    expected_generation_id: str = Field(min_length=1)

    @field_validator("authority_manifest_path", "allowed_root")
    @classmethod
    def validate_absolute_paths(cls, value: str) -> str:
        return _absolute_path(value)


class RecoveryArtifactReference(RuntimeContractModel):
    logical_role: str = Field(min_length=1)
    artifact_role: RecoveryArtifactRole
    artifact_path: str
    artifact_size_bytes: int = Field(ge=0)
    artifact_sha256: Sha256
    generation_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    watermark: RecoveryWatermarkSummary | None = None

    @field_validator("artifact_path")
    @classmethod
    def validate_artifact_path(cls, value: str) -> str:
        return _absolute_path(value)


class RecoveryArtifactVerificationContext(RuntimeContractModel):
    logical_role: str = Field(min_length=1)
    artifact_role: RecoveryArtifactRole
    artifact_path: str
    artifact_size_bytes: int = Field(ge=0)
    artifact_sha256: Sha256
    generation_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    trust_root: str
    as_of: AwareUtcDatetime
    related_artifacts: tuple[RecoveryArtifactReference, ...]

    @field_validator("artifact_path", "trust_root")
    @classmethod
    def validate_artifact_path(cls, value: str) -> str:
        return _absolute_path(value)

    @model_validator(mode="after")
    def validate_trust_boundary(self) -> Self:
        if not Path(self.artifact_path).is_relative_to(Path(self.trust_root)):
            raise ValueError("verification artifact escapes its explicit trust root")
        return self

    @classmethod
    def for_source(
        cls,
        *,
        logical_role: str,
        artifact_role: RecoveryArtifactRole,
        artifact_path: Path,
        generation_id: str,
        schema_version: str,
        as_of: AwareUtcDatetime,
        related_artifacts: tuple[RecoveryArtifactReference, ...],
        trust_root: Path | None = None,
    ) -> Self:
        trusted = trust_root if trust_root is not None else artifact_path.parent
        inspected = _inspect_regular_file(
            artifact_path,
            label=f"verification source {logical_role}",
            trust_root=trusted,
        )
        return cls(
            logical_role=logical_role,
            artifact_role=artifact_role,
            artifact_path=str(artifact_path),
            artifact_size_bytes=inspected.size_bytes,
            artifact_sha256=inspected.sha256,
            generation_id=generation_id,
            schema_version=schema_version,
            trust_root=str(trusted),
            as_of=as_of,
            related_artifacts=related_artifacts,
        )

    def reference_by_role(self, logical_role: str) -> RecoveryArtifactReference:
        matches = tuple(
            item for item in self.related_artifacts if item.logical_role == logical_role
        )
        if len(matches) != 1:
            raise RuntimeRecoveryCoordinatorError(
                f"expected one related artifact for {logical_role}, found {len(matches)}"
            )
        return matches[0]


class RecoveryRoleVerifier(Protocol):
    def __call__(
        self,
        context: RecoveryArtifactVerificationContext,
    ) -> RecoveryWatermarkSummary: ...


class _WatermarkJsonDocument(RuntimeContractModel):
    max_date: date
    row_count: int = Field(ge=0)


class _ArtifactReferenceDocument(RuntimeContractModel):
    logical_role: str = Field(min_length=1)
    sha256: Sha256


class _FixedReplayResult(RuntimeContractModel):
    dataset_snapshot_id: Sha256
    strategy_registration_fingerprint: Sha256
    strategy_definition_sha256: Sha256
    strategy_executable_sha256: Sha256
    engine_version: str = Field(min_length=1)
    start_date: date
    end_date: date
    trade_count: int = Field(ge=0)
    winning_trade_count: int = Field(ge=0)
    total_return_bps: int
    max_drawdown_bps: int = Field(ge=0)
    result_sha256: Sha256 | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("fixed replay start_date cannot be after end_date")
        if self.winning_trade_count > self.trade_count:
            raise ValueError("fixed replay winning trades exceed total trades")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"result_sha256"}))
        if self.result_sha256 is not None and self.result_sha256 != expected:
            raise ValueError("fixed replay result identity does not match its metrics")
        object.__setattr__(self, "result_sha256", expected)
        return self


class _FixedReplayEvidence(RuntimeContractModel):
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"]
    expected_result_sha256: Sha256
    result: _FixedReplayResult
    status: Literal["passed"]

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if self.result.result_sha256 != self.expected_result_sha256:
            raise ValueError("fixed replay result does not match the expected result")
        return self


class RuntimeRecoveryFixedReplayExpectation(_FixedReplayEvidence):
    """Signed expected output for one trusted production strategy replay."""


class _ArtifactMetadataDocument(_WatermarkJsonDocument):
    references: tuple[_ArtifactReferenceDocument, ...] = Field(min_length=1)
    fixed_replays: tuple[_FixedReplayEvidence, ...] = Field(
        min_length=3,
        max_length=3,
    )

    @field_validator("fixed_replays")
    @classmethod
    def require_all_strategy_replays(
        cls,
        value: tuple[_FixedReplayEvidence, ...],
    ) -> tuple[_FixedReplayEvidence, ...]:
        ordered = tuple(sorted(value, key=lambda item: item.strategy_id))
        if {item.strategy_id for item in ordered} != {
            "n_shape",
            "growth_board_surge",
            "auction_gap",
        }:
            raise ValueError("fixed_replays must cover exactly the three production strategies")
        return ordered


def _build_strategy_replay_executable_fingerprint() -> Callable[[], str]:
    from rquant import (
        auction_gap_strategy,
        formal_smoke_replay,
        growth_board_surge_strategy,
        minute_replay,
        strategy_compare,
    )
    from rquant.dashboard import strategy_lab_data

    roots = (
        (formal_smoke_replay, "build_formal_smoke_spec"),
        (formal_smoke_replay, "_execute_formal_smoke_spec"),
        (formal_smoke_replay, "_execute_n_shape"),
        (formal_smoke_replay, "_execute_growth_board_surge"),
        (formal_smoke_replay, "_execute_auction_gap"),
        (growth_board_surge_strategy, "run_growth_board_surge_replay"),
        (auction_gap_strategy, "run_auction_gap_replay"),
        (auction_gap_strategy, "run_auction_gap_minute_replay"),
        (strategy_lab_data, "growth_board_metric_rows"),
        (strategy_lab_data, "auction_gap_metric_rows"),
        (strategy_compare, "run_entry_mode_comparison"),
        (strategy_compare, "run_minute_strong_carry_replay"),
        (minute_replay, "run_minute_strong_carry_replay"),
        (minute_replay, "build_minute_replay_entry_snapshots"),
        (minute_replay, "replay_entry_snapshots_to_trades"),
    )
    try:
        guard = capture_executable_dependency_guard(
            tuple(
                ExecutableBinding(
                    owner_module=owner,
                    binding_path=(name,),
                    implementation=getattr(owner, name),
                )
                for owner, name in roots
            ),
            contract="runtime-recovery-dependency-closure/v1",
            limits=DependencyFingerprintLimits(
                max_nodes=32_768,
                max_depth=64,
                max_bytes=8 * 1024 * 1024,
            ),
        )
    except ExecutableDependencyError as exc:
        raise RuntimeRecoveryCoordinatorError(
            "trusted fixed replay strategy executable dependency graph is unsafe"
        ) from exc

    def verify_and_fingerprint() -> str:
        try:
            guard.assert_unchanged()
        except ExecutableDependencyError as exc:
            raise RuntimeRecoveryCoordinatorError(
                "trusted fixed replay strategy executable global fingerprint changed"
            ) from exc
        return guard.fingerprint

    return verify_and_fingerprint


_strategy_replay_graph_sha256 = _build_strategy_replay_executable_fingerprint()


def _strategy_fixed_replay_executable_sha256(strategy_id: str) -> str:
    if strategy_id not in {"n_shape", "growth_board_surge", "auction_gap"}:
        raise RuntimeRecoveryCoordinatorError(f"unsupported fixed replay strategy: {strategy_id}")
    return canonical_sha256(
        {
            "contract": "runtime-recovery-formal-smoke-executable/v2",
            "strategy_id": strategy_id,
            "dependency_graph": _strategy_replay_graph_sha256(),
        }
    )


def _n_shape_strategy_executable_sha256() -> str:
    return _strategy_fixed_replay_executable_sha256("n_shape")


_WorkingSetRelation = tuple[str, str, list[object], int, int, int | None]

_AUCTION_GAP_REPLAY_RELATION_BUDGET_LABELS = MappingProxyType(
    {
        "auction_bar": "auction_bar",
        "stock_status_daily": "auction_status",
        "daily_bar": "auction_daily",
        "daily_state": "auction_state",
        "stock_basic": "auction_listing",
        "trade_calendar": "trade_calendar",
        "minute_bar": "auction_minute",
        "limit_list_daily": "auction_limit_list",
        "strategy_eligibility": "auction_eligibility",
    }
)
_AUCTION_GAP_OPTIONAL_REPLAY_RELATIONS = frozenset({"strategy_eligibility"})


def _auction_gap_fixed_replay_relations(
    *,
    start_date: date,
    end_date: date,
) -> tuple[_WorkingSetRelation, ...]:
    candidate_codes = """
        SELECT DISTINCT ts_code
        FROM auction_bar
        WHERE trade_date BETWEEN ? AND ?
    """
    labels = _AUCTION_GAP_REPLAY_RELATION_BUDGET_LABELS
    return (
        (
            labels["auction_bar"],
            "SELECT * FROM auction_bar WHERE trade_date BETWEEN ? AND ?",
            [start_date, end_date],
            65_536,
            16 * 1024 * 1024,
            4_096,
        ),
        (
            labels["stock_status_daily"],
            f"""
            SELECT status.*
            FROM stock_status_daily AS status
            WHERE status.trade_date BETWEEN ? AND ?
              AND status.ts_code IN ({candidate_codes})
            """,
            [start_date, end_date, start_date, end_date],
            65_536,
            16 * 1024 * 1024,
            4_096,
        ),
        (
            labels["daily_bar"],
            f"""
            SELECT daily.*
            FROM daily_bar AS daily
            WHERE daily.ts_code IN ({candidate_codes})
              AND daily.trade_date BETWEEN ? - INTERVAL 120 DAY
                                       AND ? + INTERVAL 5 DAY
            """,
            [start_date, end_date, start_date, end_date],
            65_536,
            32 * 1024 * 1024,
            4_096,
        ),
        (
            labels["daily_state"],
            f"""
            SELECT state.*
            FROM daily_state AS state
            WHERE state.ts_code IN ({candidate_codes})
              AND state.trade_date BETWEEN ? - INTERVAL 120 DAY
                                       AND ? + INTERVAL 5 DAY
            """,
            [start_date, end_date, start_date, end_date],
            65_536,
            32 * 1024 * 1024,
            4_096,
        ),
        (
            labels["stock_basic"],
            f"""
            WITH requested AS ({candidate_codes}),
            first_daily AS (
                SELECT daily.ts_code, MIN(daily.trade_date) AS first_trade_date
                FROM daily_bar AS daily
                WHERE daily.ts_code IN (SELECT ts_code FROM requested)
                  AND daily.trade_date BETWEEN ? - INTERVAL 120 DAY
                                           AND ? + INTERVAL 5 DAY
                GROUP BY daily.ts_code
            )
            SELECT requested.ts_code,
                   COALESCE(basic.list_date, first_daily.first_trade_date) AS list_date
            FROM requested
            LEFT JOIN stock_basic AS basic USING (ts_code)
            LEFT JOIN first_daily USING (ts_code)
            """,
            [start_date, end_date, start_date, end_date],
            4_096,
            1024 * 1024,
            4_096,
        ),
        (
            labels["minute_bar"],
            f"""
            SELECT minute.*
            FROM minute_bar AS minute
            WHERE minute.ts_code IN ({candidate_codes})
              AND CAST(minute.trade_time AS DATE)
                  BETWEEN ? AND ? + INTERVAL 5 DAY
            """,
            [start_date, end_date, start_date, end_date],
            262_144,
            32 * 1024 * 1024,
            4_096,
        ),
        (
            labels["limit_list_daily"],
            f"""
            SELECT limits.*
            FROM limit_list_daily AS limits
            WHERE limits.ts_code IN ({candidate_codes})
              AND limits.trade_date BETWEEN ? AND ? + INTERVAL 5 DAY
              AND limits.limit_status = 'U'
            """,
            [start_date, end_date, start_date, end_date],
            65_536,
            16 * 1024 * 1024,
            4_096,
        ),
        (
            labels["strategy_eligibility"],
            """
            SELECT *
            FROM strategy_eligibility
            WHERE strategy_id = 'auction_gap'
              AND eligibility_date BETWEEN ? AND ?
            """,
            [start_date, end_date],
            65_536,
            16 * 1024 * 1024,
            4_096,
        ),
    )


def _trade_calendar_fixed_replay_relation() -> _WorkingSetRelation:
    return (
        "trade_calendar",
        """
        SELECT cal_date
        FROM trade_calendar
        WHERE exchange = 'SSE' AND is_open = TRUE
        """,
        [],
        8_192,
        1024 * 1024,
        None,
    )


def _auction_gap_replay_relation_contract(
    *,
    start_date: date,
    end_date: date,
) -> dict[str, _WorkingSetRelation]:
    relations = (
        _trade_calendar_fixed_replay_relation(),
        *_auction_gap_fixed_replay_relations(
            start_date=start_date,
            end_date=end_date,
        ),
    )
    by_label = {relation[0]: relation for relation in relations}
    labels = _AUCTION_GAP_REPLAY_RELATION_BUDGET_LABELS
    if set(by_label) != set(labels.values()):
        raise RuntimeRecoveryCoordinatorError(
            "auction replay relation contract does not match its working-set budgets"
        )
    return {relation: by_label[label] for relation, label in labels.items()}


def _assert_fixed_replay_working_set_is_bounded(
    store: object,
    *,
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"],
    start_date: date,
    end_date: date,
) -> None:
    if end_date < start_date or (end_date - start_date).days > 128:
        raise RuntimeRecoveryCoordinatorError(
            "trusted fixed replay date working set exceeds its memory budget"
        )
    connection = store._conn
    common_relations = (_trade_calendar_fixed_replay_relation(),)
    strategy_relations: dict[
        str,
        tuple[_WorkingSetRelation, ...],
    ] = {
        "n_shape": (
            (
                "candidate",
                """
                SELECT DISTINCT ts_code
                FROM screen_result
                WHERE trade_date BETWEEN ? AND ?
                  AND preset_name IN ('n-shape-pool1', 'n-shape-pool2')
                """,
                [start_date, end_date],
                64,
                64 * 1024,
                64,
            ),
            (
                "daily_bar",
                """
                SELECT daily.*
                FROM daily_bar AS daily
                WHERE daily.ts_code IN (
                    SELECT DISTINCT ts_code
                    FROM screen_result
                    WHERE trade_date BETWEEN ? AND ?
                      AND preset_name IN ('n-shape-pool1', 'n-shape-pool2')
                )
                  AND daily.trade_date BETWEEN ? - INTERVAL 120 DAY
                                           AND ? + INTERVAL 5 DAY
                """,
                [start_date, end_date, start_date, end_date],
                8_192,
                8 * 1024 * 1024,
                64,
            ),
            (
                "minute_bar",
                """
                SELECT minute.*
                FROM minute_bar AS minute
                WHERE minute.ts_code IN (
                    SELECT DISTINCT ts_code
                    FROM screen_result
                    WHERE trade_date BETWEEN ? AND ?
                      AND preset_name IN ('n-shape-pool1', 'n-shape-pool2')
                )
                  AND CAST(minute.trade_time AS DATE)
                      BETWEEN ? - INTERVAL 120 DAY AND ? + INTERVAL 5 DAY
                """,
                [start_date, end_date, start_date, end_date],
                65_536,
                16 * 1024 * 1024,
                64,
            ),
        ),
        "growth_board_surge": (
            (
                "growth_status",
                """
                SELECT *
                FROM stock_status_daily
                WHERE trade_date BETWEEN ? AND ?
                  AND (ts_code LIKE '30%' OR ts_code LIKE '68%')
                """,
                [start_date, end_date],
                262_144,
                32 * 1024 * 1024,
                2_048,
            ),
            (
                "growth_daily",
                """
                SELECT *
                FROM daily_bar
                WHERE trade_date BETWEEN ? - INTERVAL 120 DAY
                                     AND ? + INTERVAL 5 DAY
                  AND (ts_code LIKE '30%' OR ts_code LIKE '68%')
                """,
                [start_date, end_date],
                262_144,
                32 * 1024 * 1024,
                2_048,
            ),
            (
                "growth_minute",
                """
                SELECT *
                FROM minute_bar
                WHERE CAST(trade_time AS DATE) BETWEEN ? AND ? + INTERVAL 5 DAY
                  AND (ts_code LIKE '30%' OR ts_code LIKE '68%')
                """,
                [start_date, end_date],
                262_144,
                32 * 1024 * 1024,
                2_048,
            ),
        ),
        "auction_gap": _auction_gap_fixed_replay_relations(
            start_date=start_date,
            end_date=end_date,
        ),
    }
    for label, relation_sql, parameters, row_limit, byte_limit, code_limit in (
        *common_relations,
        *strategy_relations[strategy_id],
    ):
        try:
            row = connection.execute(
                f"""
                SELECT COUNT(*),
                       COALESCE(
                           SUM(octet_length(encode(CAST(bounded AS VARCHAR)))),
                           0
                       )
                FROM (
                    {relation_sql}
                    LIMIT {row_limit + 1}
                ) AS bounded
                """,
                parameters,
            ).fetchone()
        except duckdb.CatalogException:
            optional_label = _AUCTION_GAP_REPLAY_RELATION_BUDGET_LABELS.get("strategy_eligibility")
            if strategy_id == "auction_gap" and label == optional_label:
                continue
            raise
        row_count = int(row[0])
        byte_count = int(row[1])
        if row_count > row_limit or byte_count > byte_limit:
            raise RuntimeRecoveryCoordinatorError(
                f"trusted fixed replay {label} working set exceeds its memory budget"
            )
        if code_limit is None:
            continue
        code_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT ts_code
                    FROM ({relation_sql}) AS code_rows
                    WHERE ts_code IS NOT NULL
                    LIMIT {code_limit + 1}
                ) AS bounded_codes
                """,
                parameters,
            ).fetchone()[0]
        )
        if code_count > code_limit:
            raise RuntimeRecoveryCoordinatorError(
                f"trusted fixed replay {label} code working set exceeds its memory budget"
            )


def _quoted_replay_relation(relation: str) -> str:
    return f'"{relation.replace(chr(34), chr(34) * 2)}"'


def _quoted_replay_path(path: Path) -> str:
    return f"'{str(path).replace(chr(39), chr(39) * 2)}'"


@dataclass(slots=True)
class _BoundedReplayStore:
    path: Path
    _conn: duckdb.DuckDBPyConnection
    _temporary_directory: TemporaryDirectory[str]

    def close(self) -> None:
        try:
            self._conn.close()
        finally:
            self._temporary_directory.cleanup()


def _open_auction_gap_bounded_replay_store(
    store: object,
    *,
    start_date: date,
    end_date: date,
) -> object:
    temporary_directory = TemporaryDirectory(prefix="rquant-auction-replay-")
    bounded_path = Path(":memory:")
    source_connection = store._conn
    bounded_connection = duckdb.connect(":memory:")
    try:
        bounded_connection.execute("SET memory_limit='8MB'")
        bounded_connection.execute("SET threads=1")
        bounded_connection.execute("SET preserve_insertion_order=false")
        contract = _auction_gap_replay_relation_contract(
            start_date=start_date,
            end_date=end_date,
        )
        for index, (relation, spec) in enumerate(contract.items()):
            _, relation_sql, parameters, _, _, _ = spec
            relation_path = Path(temporary_directory.name) / f"bounded-relation-{index}.parquet"
            try:
                source_connection.execute(
                    f"COPY ({relation_sql}) TO {_quoted_replay_path(relation_path)} "
                    "(FORMAT PARQUET)",
                    parameters,
                )
            except duckdb.CatalogException:
                if relation in _AUCTION_GAP_OPTIONAL_REPLAY_RELATIONS:
                    continue
                raise
            bounded_connection.execute(
                f"CREATE TABLE {_quoted_replay_relation(relation)} AS "
                f"SELECT * FROM read_parquet({_quoted_replay_path(relation_path)})"
            )
        bounded_connection.execute("SET enable_external_access=false")
    except Exception:
        bounded_connection.close()
        temporary_directory.cleanup()
        raise
    return _BoundedReplayStore(
        path=bounded_path,
        _conn=bounded_connection,
        _temporary_directory=temporary_directory,
    )


def _compute_strategy_fixed_replay_v1(
    *,
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"],
    dataset_sha256: str,
    expected: _FixedReplayResult,
    store: object,
) -> _FixedReplayResult:
    from rquant import formal_smoke_replay

    spec = formal_smoke_replay.build_formal_smoke_spec(
        strategy_id,
        start_date=expected.start_date,
        end_date=expected.end_date,
    )
    if spec.spec_hash != expected.strategy_definition_sha256:
        raise RuntimeRecoveryCoordinatorError(
            "trusted fixed replay strategy definition fingerprint changed"
        )
    executable_sha256 = _strategy_fixed_replay_executable_sha256(strategy_id)
    if executable_sha256 != expected.strategy_executable_sha256:
        raise RuntimeRecoveryCoordinatorError(
            "trusted fixed replay strategy executable fingerprint changed"
        )
    _assert_fixed_replay_working_set_is_bounded(
        store,
        strategy_id=strategy_id,
        start_date=expected.start_date,
        end_date=expected.end_date,
    )
    replay_store = store
    bounded_store = None
    if strategy_id == "auction_gap":
        bounded_store = _open_auction_gap_bounded_replay_store(
            store,
            start_date=expected.start_date,
            end_date=expected.end_date,
        )
        replay_store = bounded_store
    try:
        computation = formal_smoke_replay._execute_formal_smoke_spec(replay_store, spec)
    finally:
        if bounded_store is not None:
            bounded_store.close()
    if _strategy_fixed_replay_executable_sha256(strategy_id) != executable_sha256:
        raise RuntimeRecoveryCoordinatorError(
            "trusted fixed replay strategy executable changed during replay"
        )
    trades = computation.tables.get("trades")
    replay_returns = () if trades is None or "ret_pct" not in trades.columns else trades["ret_pct"]
    trade_count = 0
    winning_trade_count = 0
    total_return_bps = 0
    cumulative = 0
    peak = 0
    max_drawdown = 0
    for raw_value in replay_returns:
        value = int(
            (Decimal(str(raw_value)) * Decimal(100)).quantize(
                Decimal("1"),
                rounding=ROUND_HALF_EVEN,
            )
        )
        trade_count += 1
        winning_trade_count += int(value > 0)
        total_return_bps += value
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return _FixedReplayResult(
        dataset_snapshot_id=dataset_sha256,
        strategy_registration_fingerprint=expected.strategy_registration_fingerprint,
        strategy_definition_sha256=spec.spec_hash,
        strategy_executable_sha256=executable_sha256,
        engine_version="rquant.formal-smoke.stage1-smoke-v1",
        start_date=expected.start_date,
        end_date=expected.end_date,
        trade_count=trade_count,
        winning_trade_count=winning_trade_count,
        total_return_bps=total_return_bps,
        max_drawdown_bps=max_drawdown,
    )


def _run_fixed_replay_batch_v1(
    *,
    dataset_path: Path,
    dataset_sha256: str,
    trust_root: Path,
    expected: tuple[_FixedReplayEvidence, ...],
) -> tuple[_FixedReplayResult, ...]:
    descriptor, parent_lease = _open_regular_readonly(
        dataset_path,
        label="trusted fixed replay dataset",
        trust_root=trust_root,
    )
    try:
        opened = os.fstat(descriptor)
        digest = _stream_digest_descriptor(descriptor, label="trusted fixed replay dataset")
        if digest.sha256 != dataset_sha256:
            raise RuntimeRecoveryCoordinatorError(
                "trusted fixed replay dataset hash does not match its authority"
            )
        from rquant.storage.duckdb import DuckDBStore

        store = DuckDBStore(dataset_path, read_only=True)
        try:
            store._conn.execute("SET memory_limit='8MB'")
            store._conn.execute("SET threads=1")
            store._conn.execute("SET preserve_insertion_order=false")
            results = tuple(
                _compute_strategy_fixed_replay_v1(
                    strategy_id=evidence.strategy_id,
                    dataset_sha256=dataset_sha256,
                    expected=evidence.result,
                    store=store,
                )
                for evidence in expected
            )
        finally:
            store.close()
        after = _verify_regular_binding(
            name=dataset_path.name,
            descriptor=descriptor,
            parent_lease=parent_lease,
            label="trusted fixed replay dataset",
            expected=_file_binding(opened),
        )
        if _identity(opened) != _identity(after):
            raise RuntimeRecoveryCoordinatorError(
                "trusted fixed replay dataset changed during replay"
            )
        os.lseek(descriptor, 0, os.SEEK_SET)
        if (
            _stream_digest_descriptor(
                descriptor,
                label="trusted fixed replay dataset",
            ).sha256
            != dataset_sha256
        ):
            raise RuntimeRecoveryCoordinatorError(
                "trusted fixed replay dataset changed during replay"
            )
    finally:
        os.close(descriptor)
        parent_lease.close()
    return results


class RuntimeRecoveryFixedReplayVerifier:
    """Run and attest the real bounded replay closure for a recovery target."""

    def __init__(
        self,
        *,
        expectations: tuple[RuntimeRecoveryFixedReplayExpectation, ...],
    ) -> None:
        ordered = tuple(sorted(expectations, key=lambda item: item.strategy_id))
        if {item.strategy_id for item in ordered} != {
            "n_shape",
            "growth_board_surge",
            "auction_gap",
        } or len(ordered) != 3:
            raise ValueError("recovery fixed replay requires exactly all production strategies")
        dataset_ids = {item.result.dataset_snapshot_id for item in ordered}
        if len(dataset_ids) != 1:
            raise ValueError("recovery fixed replay expectations must bind one dataset snapshot")
        self.expectations = ordered
        self.dataset_sha256 = next(iter(dataset_ids))
        self._dependency_graph = _strategy_replay_graph_sha256()
        self.fingerprint = canonical_sha256(
            {
                "contract": "runtime-recovery-fixed-replay-verifier/v1",
                "dependency_graph": self._dependency_graph,
                "dataset_sha256": self.dataset_sha256,
                "expectations": [item.model_dump(mode="json") for item in ordered],
            }
        )

    def verify(
        self,
        *,
        target_root: Path,
        dataset_path: Path,
    ) -> tuple[FixedReplayReceipt, ...]:
        trusted_root = _canonical_absolute_path(target_root, label="fixed replay target_root")
        trusted_dataset = _canonical_absolute_path(dataset_path, label="fixed replay dataset_path")
        if not trusted_dataset.is_relative_to(trusted_root):
            raise RuntimeRecoveryCoordinatorError(
                "trusted fixed replay dataset escapes the recovery target"
            )
        if _strategy_replay_graph_sha256() != self._dependency_graph:
            raise RuntimeRecoveryCoordinatorError(
                "trusted fixed replay verifier dependency graph changed"
            )
        results = _run_fixed_replay_batch_v1(
            dataset_path=trusted_dataset,
            dataset_sha256=self.dataset_sha256,
            trust_root=trusted_root,
            expected=tuple(
                _FixedReplayEvidence.model_validate(item.model_dump(mode="python"))
                for item in self.expectations
            ),
        )
        for expectation, observed in zip(self.expectations, results, strict=True):
            if expectation.result != observed:
                raise RuntimeRecoveryCoordinatorError(
                    "trusted fixed replay result does not match recomputed metrics"
                )
        from rquant.runtime_recovery_artifacts import FixedReplayReceipt

        return tuple(
            FixedReplayReceipt(
                strategy_id=expectation.strategy_id,
                replay_fingerprint=str(observed.result_sha256),
            )
            for expectation, observed in zip(self.expectations, results, strict=True)
        )


def build_runtime_recovery_fixed_replay_expectations(
    *,
    target_root: Path,
    dataset_path: Path,
    strategy_bindings: Iterable[ProductionStrategyBinding],
    start_date: date,
    end_date: date,
) -> tuple[RuntimeRecoveryFixedReplayExpectation, ...]:
    """Compute signed replay expectations from one frozen production snapshot."""

    if start_date > end_date:
        raise ValueError("recovery fixed replay start_date cannot follow end_date")
    bindings = tuple(sorted(strategy_bindings, key=lambda item: item.strategy_id))
    if len(bindings) != 3 or {item.strategy_id for item in bindings} != {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }:
        raise ValueError("recovery fixed replay requires exactly all production bindings")
    trusted_root = _canonical_absolute_path(target_root, label="fixed replay target_root")
    trusted_dataset = _canonical_absolute_path(dataset_path, label="fixed replay dataset_path")
    if not trusted_dataset.is_relative_to(trusted_root):
        raise RuntimeRecoveryCoordinatorError(
            "trusted fixed replay dataset escapes the recovery target"
        )
    descriptor, parent_lease = _open_regular_readonly(
        trusted_dataset,
        label="trusted fixed replay dataset",
        trust_root=trusted_root,
    )
    try:
        dataset_sha256 = _stream_digest_descriptor(
            descriptor,
            label="trusted fixed replay dataset",
        ).sha256
    finally:
        os.close(descriptor)
        parent_lease.close()

    from rquant import formal_smoke_replay

    seed_evidence: list[_FixedReplayEvidence] = []
    for binding in bindings:
        strategy_id = binding.strategy_id
        spec = formal_smoke_replay.build_formal_smoke_spec(
            strategy_id,
            start_date=start_date,
            end_date=end_date,
        )
        seed = _FixedReplayResult(
            dataset_snapshot_id=dataset_sha256,
            strategy_registration_fingerprint=binding.registration_fingerprint,
            strategy_definition_sha256=spec.spec_hash,
            strategy_executable_sha256=_strategy_fixed_replay_executable_sha256(strategy_id),
            engine_version="rquant.formal-smoke.stage1-smoke-v1",
            start_date=start_date,
            end_date=end_date,
            trade_count=0,
            winning_trade_count=0,
            total_return_bps=0,
            max_drawdown_bps=0,
        )
        seed_evidence.append(
            _FixedReplayEvidence(
                strategy_id=strategy_id,
                expected_result_sha256=str(seed.result_sha256),
                result=seed,
                status="passed",
            )
        )
    observed = _run_fixed_replay_batch_v1(
        dataset_path=trusted_dataset,
        dataset_sha256=dataset_sha256,
        trust_root=trusted_root,
        expected=tuple(seed_evidence),
    )
    return tuple(
        RuntimeRecoveryFixedReplayExpectation(
            strategy_id=evidence.strategy_id,
            expected_result_sha256=str(result.result_sha256),
            result=result,
            status="passed",
        )
        for evidence, result in zip(seed_evidence, observed, strict=True)
    )


def _run_strategy_fixed_replay_v1(
    *,
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"],
    dataset_path: Path,
    dataset_sha256: str,
    trust_root: Path,
    expected: _FixedReplayResult,
) -> _FixedReplayResult:
    evidence = _FixedReplayEvidence(
        strategy_id=strategy_id,
        expected_result_sha256=str(expected.result_sha256),
        result=expected,
        status="passed",
    )
    return _run_fixed_replay_batch_v1(
        dataset_path=dataset_path,
        dataset_sha256=dataset_sha256,
        trust_root=trust_root,
        expected=(evidence,),
    )[0]


def _run_n_shape_fixed_replay_v1(
    *,
    dataset_path: Path,
    dataset_sha256: str,
    trust_root: Path,
    expected: _FixedReplayResult,
) -> _FixedReplayResult:
    return _run_strategy_fixed_replay_v1(
        strategy_id="n_shape",
        dataset_path=dataset_path,
        dataset_sha256=dataset_sha256,
        trust_root=trust_root,
        expected=expected,
    )


class _VerifiedStrategyReplay(RuntimeContractModel):
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"]
    registration_fingerprint: Sha256
    definition_fingerprint: Sha256
    executable_fingerprint: Sha256
    result_fingerprint: Sha256


class _TrustedVerificationResult(RuntimeContractModel):
    watermark: RecoveryWatermarkSummary
    verifier_id: str = Field(min_length=1)
    verifier_version: int = Field(ge=1)
    verifier_fingerprint: Sha256
    fixed_replay_verified: bool = False
    strategy_replays: tuple[_VerifiedStrategyReplay, ...] = ()


_TrustedVerifierOutput = (
    tuple[RecoveryWatermarkSummary, bool]
    | tuple[
        RecoveryWatermarkSummary,
        bool,
        tuple[_VerifiedStrategyReplay, ...],
    ]
)


@dataclass(frozen=True, slots=True)
class _TrustedVerifierDefinition:
    artifact_role: RecoveryArtifactRole
    schema_version: str
    verifier_id: str
    verifier_version: int
    binding_name: str
    verify: Callable[
        [RecoveryArtifactVerificationContext],
        _TrustedVerifierOutput,
    ]
    dependencies: tuple[tuple[str, Callable[..., object]], ...]
    executable_guard: ExecutableDependencyGuard
    fingerprint: str

    @classmethod
    def build(
        cls,
        *,
        artifact_role: RecoveryArtifactRole,
        schema_version: str,
        verifier_id: str,
        verifier_version: int,
        verify: Callable[
            [RecoveryArtifactVerificationContext],
            _TrustedVerifierOutput,
        ],
        dependencies: tuple[tuple[str, Callable[..., object]], ...] = (),
    ) -> _TrustedVerifierDefinition:
        binding_name = verify.__name__
        owner_module = sys.modules[__name__]
        try:
            executable_guard = capture_executable_dependency_guard(
                (
                    ExecutableBinding(
                        owner_module=owner_module,
                        binding_path=(binding_name,),
                        implementation=verify,
                    ),
                    *(
                        ExecutableBinding(
                            owner_module=owner_module,
                            binding_path=(name,),
                            implementation=dependency,
                        )
                        for name, dependency in dependencies
                    ),
                ),
                contract="runtime-recovery-trusted-verifier/v2",
                limits=DependencyFingerprintLimits(
                    max_nodes=32_768,
                    max_depth=64,
                    max_bytes=8 * 1024 * 1024,
                ),
            )
        except ExecutableDependencyError as exc:
            raise RuntimeRecoveryCoordinatorError(
                f"trusted verifier executable dependency graph is unsafe for {verifier_id}"
            ) from exc
        fingerprint = canonical_sha256(
            {
                "contract": "runtime-recovery-trusted-verifier-definition/v2",
                "artifact_role": artifact_role,
                "schema_version": schema_version,
                "verifier_id": verifier_id,
                "verifier_version": verifier_version,
                "binding_name": binding_name,
                "executable_dependency_sha256": executable_guard.fingerprint,
            }
        )
        return cls(
            artifact_role=artifact_role,
            schema_version=schema_version,
            verifier_id=verifier_id,
            verifier_version=verifier_version,
            binding_name=binding_name,
            verify=verify,
            dependencies=dependencies,
            executable_guard=executable_guard,
            fingerprint=fingerprint,
        )

    def assert_implementation_is_trusted(self) -> None:
        if globals().get(self.binding_name) is not self.verify:
            raise RuntimeRecoveryCoordinatorError(
                f"trusted verifier binding changed for {self.verifier_id}"
            )
        for name, dependency in self.dependencies:
            if globals().get(name) is not dependency:
                raise RuntimeRecoveryCoordinatorError(
                    f"trusted verifier dependency binding changed for {self.verifier_id}: {name}"
                )
        try:
            self.executable_guard.assert_unchanged()
        except ExecutableDependencyError as exc:
            if "fingerprint changed" in str(exc):
                raise RuntimeRecoveryCoordinatorError(
                    f"trusted verifier implementation fingerprint changed for {self.verifier_id}"
                ) from exc
            raise RuntimeRecoveryCoordinatorError(
                f"trusted verifier executable dependency graph is unsafe for {self.verifier_id}"
            ) from exc
        current_fingerprint = canonical_sha256(
            {
                "contract": "runtime-recovery-trusted-verifier-definition/v2",
                "artifact_role": self.artifact_role,
                "schema_version": self.schema_version,
                "verifier_id": self.verifier_id,
                "verifier_version": self.verifier_version,
                "binding_name": self.binding_name,
                "executable_dependency_sha256": self.executable_guard.fingerprint,
            }
        )
        if current_fingerprint != self.fingerprint:
            raise RuntimeRecoveryCoordinatorError(
                f"trusted verifier definition fingerprint changed for {self.verifier_id}"
            )


class RecoveryAuthorityReceipt(RecoveryArtifactReference):
    plane: RecoveryPlane
    authority_id: Sha256
    authority_document_sha256: Sha256
    producer_commit: CommitSha
    available_at: AwareUtcDatetime
    watermark: RecoveryWatermarkSummary
    verifier_id: str = Field(min_length=1)
    verifier_version: int = Field(ge=1)
    verifier_fingerprint: Sha256
    fixed_replay_verified: bool = False
    strategy_replays: tuple[_VerifiedStrategyReplay, ...] = ()

    @model_validator(mode="after")
    def validate_replay_evidence(self) -> Self:
        is_metadata = self.artifact_role is RecoveryArtifactRole.ARTIFACT_METADATA
        if is_metadata != self.fixed_replay_verified:
            raise ValueError("only artifact metadata may carry fixed replay verification")
        if is_metadata:
            ordered = tuple(sorted(self.strategy_replays, key=lambda item: item.strategy_id))
            if len(ordered) != 3 or {item.strategy_id for item in ordered} != {
                "n_shape",
                "growth_board_surge",
                "auction_gap",
            }:
                raise ValueError("artifact metadata must verify all three strategy replays")
            object.__setattr__(self, "strategy_replays", ordered)
        elif self.strategy_replays:
            raise ValueError("non-metadata authority cannot carry strategy replays")
        return self


class RecoveryStrategyLineage(RuntimeContractModel):
    strategy_id: Literal["n_shape", "growth_board_surge", "auction_gap"]
    strategy_version: Literal[1] = 1
    registration_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
    strategy_spec_fingerprint: Sha256
    executable_fingerprint: Sha256
    fixed_replay_definition_fingerprint: Sha256
    fixed_replay_executable_fingerprint: Sha256
    fixed_replay_fingerprint: Sha256


def _validate_strategy_bindings(
    *,
    producer_commit: str,
    bindings: tuple[ProductionStrategyBinding, ...],
) -> tuple[ProductionStrategyBinding, ...]:
    ordered = tuple(sorted(bindings, key=lambda item: item.strategy_id))
    strategy_ids = tuple(item.strategy_id for item in ordered)
    required = {"n_shape", "growth_board_surge", "auction_gap"}
    if len(strategy_ids) != 3 or set(strategy_ids) != required:
        raise RuntimeRecoveryCoordinatorError(
            "recovery requires exactly n_shape, growth_board_surge, and auction_gap"
        )
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit)
    for binding in ordered:
        definition = registry.load_definition(binding.strategy_id, binding.strategy_version)
        if definition.spec.spec_fingerprint != binding.strategy_spec_fingerprint:
            raise RuntimeRecoveryCoordinatorError(
                f"strategy spec fingerprint mismatch for {binding.strategy_id}"
            )
        if definition.executable_fingerprint != binding.executable_fingerprint:
            raise RuntimeRecoveryCoordinatorError(
                f"strategy executable fingerprint mismatch for {binding.strategy_id}"
            )
        if definition.candidate_schema_fingerprint != binding.candidate_schema_fingerprint:
            raise RuntimeRecoveryCoordinatorError(
                f"strategy candidate schema fingerprint mismatch for {binding.strategy_id}"
            )
    return ordered


def _build_strategy_lineage(
    *,
    bindings: tuple[ProductionStrategyBinding, ...],
    fixed_replays: tuple[_VerifiedStrategyReplay, ...],
) -> tuple[RecoveryStrategyLineage, ...]:
    replay_by_strategy = {item.strategy_id: item for item in fixed_replays}
    if len(replay_by_strategy) != 3 or set(replay_by_strategy) != {
        "n_shape",
        "growth_board_surge",
        "auction_gap",
    }:
        raise RuntimeRecoveryCoordinatorError(
            "recovery requires a verified fixed replay for every production strategy"
        )
    lineage: list[RecoveryStrategyLineage] = []
    for binding in bindings:
        replay = replay_by_strategy[binding.strategy_id]
        if replay.registration_fingerprint != binding.registration_fingerprint:
            raise RuntimeRecoveryCoordinatorError(
                f"fixed replay registration fingerprint mismatch for {binding.strategy_id}"
            )
        lineage.append(
            RecoveryStrategyLineage(
                strategy_id=binding.strategy_id,
                strategy_version=binding.strategy_version,
                registration_fingerprint=binding.registration_fingerprint,
                candidate_schema_fingerprint=binding.candidate_schema_fingerprint,
                strategy_spec_fingerprint=binding.strategy_spec_fingerprint,
                executable_fingerprint=binding.executable_fingerprint,
                fixed_replay_definition_fingerprint=replay.definition_fingerprint,
                fixed_replay_executable_fingerprint=replay.executable_fingerprint,
                fixed_replay_fingerprint=replay.result_fingerprint,
            )
        )
    return tuple(lineage)


class RuntimeRecoveryManifest(RuntimeContractModel):
    manifest_id: Sha256 | None = None
    inventory_plan: RecoveryInventoryPlan
    recovery_manifest: RecoveryManifest
    deployment_topology_id: Sha256
    deployment_profile_id: Sha256
    deployment_profile_generation: Sha256
    strategy_lineage: tuple[RecoveryStrategyLineage, ...] = Field(
        min_length=3,
        max_length=3,
    )
    as_of: AwareUtcDatetime
    authorities: tuple[RecoveryAuthorityReceipt, ...] = Field(min_length=1)

    def identity_payload(self) -> dict[str, object]:
        return self.model_dump(mode="python", exclude={"manifest_id"})

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        strategy_lineage = tuple(sorted(self.strategy_lineage, key=lambda item: item.strategy_id))
        strategy_ids = tuple(item.strategy_id for item in strategy_lineage)
        if set(strategy_ids) != {"n_shape", "growth_board_surge", "auction_gap"}:
            raise ValueError("runtime recovery strategy lineage is incomplete")
        object.__setattr__(self, "strategy_lineage", strategy_lineage)
        ordered = tuple(sorted(self.authorities, key=lambda item: item.logical_role))
        roles = tuple(item.logical_role for item in ordered)
        if len(roles) != len(set(roles)):
            raise ValueError("authority logical roles must be unique")
        required_roles = tuple(item.logical_role for item in self.inventory_plan.requirements)
        if set(roles) != set(required_roles):
            raise ValueError("authorities must exactly cover the inventory plan")
        if self.recovery_manifest.inventory_plan != self.inventory_plan:
            raise ValueError("nested recovery manifest inventory plan differs")
        if self.recovery_manifest.captured_at != self.as_of:
            raise ValueError("nested recovery manifest capture boundary differs")
        entry_by_role = {entry.logical_role: entry for entry in self.recovery_manifest.entries}
        if set(entry_by_role) != set(roles):
            raise ValueError("nested recovery manifest roles differ")
        requirement_by_role = {item.logical_role: item for item in self.inventory_plan.requirements}
        for receipt in ordered:
            entry = entry_by_role[receipt.logical_role]
            requirement = requirement_by_role[receipt.logical_role]
            expected = (
                receipt.artifact_role,
                receipt.artifact_path,
                receipt.artifact_size_bytes,
                receipt.artifact_sha256,
                receipt.generation_id,
                receipt.schema_version,
                receipt.watermark,
                requirement.restore_path,
            )
            observed = (
                entry.artifact_role,
                entry.absolute_path,
                entry.size_bytes,
                entry.sha256,
                entry.generation_id,
                entry.schema_version,
                entry.watermark,
                entry.restore_path,
            )
            if observed != expected:
                raise ValueError(f"nested entry differs from receipt for {receipt.logical_role}")
            if receipt.available_at > self.as_of:
                raise ValueError("authority is not PIT-visible at recovery as_of")
        metadata_receipts = tuple(
            item for item in ordered if item.artifact_role is RecoveryArtifactRole.ARTIFACT_METADATA
        )
        if len(metadata_receipts) != 1:
            raise ValueError("runtime recovery requires one fixed replay authority")
        replay_by_strategy = {
            item.strategy_id: item for item in metadata_receipts[0].strategy_replays
        }
        for lineage in strategy_lineage:
            replay = replay_by_strategy[lineage.strategy_id]
            if (
                lineage.registration_fingerprint != replay.registration_fingerprint
                or lineage.fixed_replay_definition_fingerprint != replay.definition_fingerprint
                or lineage.fixed_replay_executable_fingerprint != replay.executable_fingerprint
                or lineage.fixed_replay_fingerprint != replay.result_fingerprint
            ):
                raise ValueError(
                    f"runtime recovery replay lineage differs for {lineage.strategy_id}"
                )
        object.__setattr__(self, "authorities", ordered)
        expected_id = canonical_sha256(self.identity_payload())
        if self.manifest_id is not None and self.manifest_id != expected_id:
            raise ValueError("manifest_id does not match runtime recovery content")
        object.__setattr__(self, "manifest_id", expected_id)
        return self


class RecoveryRestorePreflightResult(RuntimeContractModel):
    passed: bool
    manifest_id: Sha256
    deployment_topology_id: Sha256
    deployment_profile_id: Sha256
    deployment_profile_generation: Sha256
    strategy_lineage: tuple[RecoveryStrategyLineage, ...] = Field(
        min_length=3,
        max_length=3,
    )
    as_of: AwareUtcDatetime
    verified_roles: tuple[str, ...] = Field(min_length=1)
    checks: tuple[str, ...] = Field(min_length=1)
    fixed_replay_verified: bool


class RecoveryRoleWatermark(RuntimeContractModel):
    logical_role: str = Field(min_length=1)
    artifact_role: RecoveryArtifactRole
    watermark: RecoveryWatermarkSummary
    verifier_fingerprint: Sha256
    fixed_replay_verified: bool = False


class RecoveryRoleRpoLoss(RuntimeContractModel):
    logical_role: str = Field(min_length=1)
    target_watermark: RecoveryWatermarkSummary
    recovered_watermark: RecoveryWatermarkSummary
    met: bool = False
    high_watermark_mismatch: bool | None = None
    max_date_loss_days: int | None = Field(default=None, ge=0)
    row_count_loss: int | None = Field(default=None, ge=0)
    missing_components: tuple[str, ...] = ()

    @model_validator(mode="after")
    def derive_status(self) -> Self:
        allowed = {"high_watermark", "max_date", "row_count"}
        if not set(self.missing_components).issubset(allowed):
            raise ValueError("unknown RPO watermark component")
        if len(self.missing_components) != len(set(self.missing_components)):
            raise ValueError("RPO missing components must be unique")
        object.__setattr__(self, "missing_components", tuple(sorted(self.missing_components)))
        expected = (
            not self.missing_components
            and self.high_watermark_mismatch is not True
            and self.max_date_loss_days in {None, 0}
            and self.row_count_loss in {None, 0}
        )
        if self.met not in {False, expected}:
            raise ValueError("RPO role status does not match watermark loss")
        object.__setattr__(self, "met", expected)
        return self


class RecoveryRehearsalReport(RuntimeContractModel):
    passed: bool
    manifest_id: Sha256
    deployment_topology_id: Sha256
    deployment_profile_id: Sha256
    deployment_profile_generation: Sha256
    strategy_lineage: tuple[RecoveryStrategyLineage, ...] = Field(
        min_length=3,
        max_length=3,
    )
    started_at: AwareUtcDatetime
    finished_at: AwareUtcDatetime
    duration_seconds: float = Field(ge=0)
    rto_target_seconds: float = Field(gt=0)
    rto_met: bool
    rpo_met: bool = False
    rpo_loss: tuple[RecoveryRoleRpoLoss, ...] = ()
    rpo_missing_target_roles: tuple[str, ...] = ()
    rpo_as_of: AwareUtcDatetime
    recovered_watermarks: tuple[RecoveryRoleWatermark, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def require_rpo_and_rto(self) -> Self:
        lineage = tuple(sorted(self.strategy_lineage, key=lambda item: item.strategy_id))
        if {item.strategy_id for item in lineage} != {
            "n_shape",
            "growth_board_surge",
            "auction_gap",
        }:
            raise ValueError("rehearsal report strategy lineage is incomplete")
        object.__setattr__(self, "strategy_lineage", lineage)
        recovered_roles = tuple(item.logical_role for item in self.recovered_watermarks)
        loss_roles = tuple(item.logical_role for item in self.rpo_loss)
        if len(recovered_roles) != len(set(recovered_roles)):
            raise ValueError("recovered RPO roles must be unique")
        if len(loss_roles) != len(set(loss_roles)):
            raise ValueError("RPO loss roles must be unique")
        if len(self.rpo_missing_target_roles) != len(set(self.rpo_missing_target_roles)):
            raise ValueError("missing RPO target roles must be unique")
        object.__setattr__(
            self,
            "rpo_missing_target_roles",
            tuple(sorted(self.rpo_missing_target_roles)),
        )
        if set(loss_roles) & set(self.rpo_missing_target_roles):
            raise ValueError("RPO roles cannot be both assessed and missing")
        if set(loss_roles) | set(self.rpo_missing_target_roles) != set(recovered_roles):
            raise ValueError("RPO roles must exactly cover recovered authority roles")
        expected_rpo = (
            bool(self.rpo_loss)
            and not self.rpo_missing_target_roles
            and all(item.met for item in self.rpo_loss)
        )
        expected = self.rpo_met and self.rto_met
        if self.passed != expected:
            raise ValueError("passed must require both RPO and RTO")
        if self.rpo_met != expected_rpo:
            raise ValueError("rpo_met does not match per-role RPO loss")
        return self


class RecoveryCurrentPointer(RuntimeContractModel):
    pointer_id: Sha256 | None = None
    schema_version: Literal[1] = 1
    manifest_id: Sha256
    deployment_profile_id: Sha256
    deployment_profile_generation: Sha256
    strategy_lineage: tuple[RecoveryStrategyLineage, ...] = Field(
        min_length=3,
        max_length=3,
    )
    generation_id: Sha256
    generation_path: str
    report_sha256: Sha256

    @field_validator("generation_path")
    @classmethod
    def validate_generation_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if path.parts != ("generations", path.name) or path.name in {"", ".", ".."}:
            raise ValueError("generation_path must name one managed generation")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        lineage = tuple(sorted(self.strategy_lineage, key=lambda item: item.strategy_id))
        if {item.strategy_id for item in lineage} != {
            "n_shape",
            "growth_board_surge",
            "auction_gap",
        }:
            raise ValueError("current pointer strategy lineage is incomplete")
        object.__setattr__(self, "strategy_lineage", lineage)
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"pointer_id"}))
        if self.pointer_id is not None and self.pointer_id != expected:
            raise ValueError("pointer_id does not match current pointer content")
        object.__setattr__(self, "pointer_id", expected)
        return self


class RecoveryRehearsalAudit(RuntimeContractModel):
    audit_id: Sha256 | None = None
    operation_id: str = Field(min_length=32, max_length=32)
    status: Literal["passed", "failed"]
    manifest_id: Sha256
    deployment_profile_id: Sha256
    deployment_profile_generation: Sha256
    strategy_lineage: tuple[RecoveryStrategyLineage, ...] = Field(
        min_length=3,
        max_length=3,
    )
    previous_current_sha256: Sha256 | None = None
    published_current_sha256: Sha256 | None = None
    generation_id: Sha256 | None = None
    report_sha256: Sha256 | None = None
    failure_point: str | None = None
    failure_type: str | None = None
    failure_message: str | None = None
    started_at: AwareUtcDatetime
    finished_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_status_and_identity(self) -> Self:
        lineage = tuple(sorted(self.strategy_lineage, key=lambda item: item.strategy_id))
        if {item.strategy_id for item in lineage} != {
            "n_shape",
            "growth_board_surge",
            "auction_gap",
        }:
            raise ValueError("rehearsal audit strategy lineage is incomplete")
        object.__setattr__(self, "strategy_lineage", lineage)
        if self.status == "passed":
            if None in {
                self.published_current_sha256,
                self.generation_id,
                self.report_sha256,
            }:
                raise ValueError("passed audit requires published generation evidence")
            if any(
                item is not None
                for item in (self.failure_point, self.failure_type, self.failure_message)
            ):
                raise ValueError("passed audit cannot contain failure evidence")
        elif self.failure_type is None or self.failure_message is None:
            raise ValueError("failed audit requires failure evidence")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"audit_id"}))
        if self.audit_id is not None and self.audit_id != expected:
            raise ValueError("audit_id does not match rehearsal audit content")
        object.__setattr__(self, "audit_id", expected)
        return self


class _RecoveryPublishIntent(RuntimeContractModel):
    intent_id: Sha256 | None = None
    schema_version: Literal[2] = 2
    operation_id: str = Field(min_length=32, max_length=32)
    owner_id: str = Field(min_length=32, max_length=32)
    candidate_name: str = Field(min_length=32, max_length=32)
    manifest_id: Sha256
    deployment_profile_id: Sha256
    deployment_profile_generation: Sha256
    strategy_lineage: tuple[RecoveryStrategyLineage, ...] = Field(
        min_length=3,
        max_length=3,
    )
    generation_id: Sha256
    stage: Literal[
        "candidate_prepared",
        "copied",
        "verified",
        "generation_staging",
        "generation_staged",
        "publish_prepared",
        "current_switched",
    ]
    generation_created: bool
    previous_current: RecoveryCurrentPointer | None = None
    proposed_current: RecoveryCurrentPointer | None = None
    success_audit: RecoveryRehearsalAudit | None = None
    recovery_audit: RecoveryRehearsalAudit

    @model_validator(mode="after")
    def validate_transaction(self) -> Self:
        if self.owner_id != self.operation_id or self.candidate_name != self.owner_id:
            raise ValueError("publish intent owner/candidate identity mismatch")
        publish_stages = {"publish_prepared", "current_switched"}
        if self.stage in publish_stages:
            if self.proposed_current is None or self.success_audit is None:
                raise ValueError("publish stage requires proposed current and success audit")
        elif self.proposed_current is not None or self.success_audit is not None:
            raise ValueError("pre-publish stage cannot contain publication evidence")
        if self.stage == "generation_staging" and not self.generation_created:
            raise ValueError("owned generation stage requires generation_created")
        if self.recovery_audit.status != "failed":
            raise ValueError("publish intent recovery audit must fail")
        if self.recovery_audit.operation_id != self.operation_id:
            raise ValueError("publish intent recovery operation mismatch")
        if self.recovery_audit.manifest_id != self.manifest_id:
            raise ValueError("publish intent recovery manifest mismatch")
        if (
            self.recovery_audit.deployment_profile_id != self.deployment_profile_id
            or self.recovery_audit.deployment_profile_generation
            != self.deployment_profile_generation
            or self.recovery_audit.strategy_lineage != self.strategy_lineage
        ):
            raise ValueError("publish intent recovery deployment evidence mismatch")
        if self.recovery_audit.generation_id != self.generation_id:
            raise ValueError("publish intent recovery generation mismatch")
        if self.proposed_current is not None and self.success_audit is not None:
            proposed_content = _serialized(self.proposed_current)
            proposed_sha256 = hashlib.sha256(proposed_content).hexdigest()
            if self.proposed_current.manifest_id != self.manifest_id:
                raise ValueError("publish intent proposed manifest mismatch")
            if self.proposed_current.generation_id != self.generation_id:
                raise ValueError("publish intent proposed generation mismatch")
            if self.success_audit.status != "passed":
                raise ValueError("publish intent success audit must pass")
            if self.success_audit.operation_id != self.operation_id:
                raise ValueError("publish intent success operation mismatch")
            if self.success_audit.published_current_sha256 != proposed_sha256:
                raise ValueError("publish intent success current hash mismatch")
            if self.success_audit.manifest_id != self.manifest_id:
                raise ValueError("publish intent success manifest mismatch")
            for evidence in (self.proposed_current, self.success_audit):
                if (
                    evidence.deployment_profile_id != self.deployment_profile_id
                    or evidence.deployment_profile_generation != self.deployment_profile_generation
                    or evidence.strategy_lineage != self.strategy_lineage
                ):
                    raise ValueError("publish intent publication evidence mismatch")
            if self.success_audit.generation_id != self.generation_id:
                raise ValueError("publish intent success generation mismatch")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"intent_id"}))
        if self.intent_id is not None and self.intent_id != expected:
            raise ValueError("publish intent identity does not match content")
        object.__setattr__(self, "intent_id", expected)
        return self


def build_recovery_authority_manifest(
    *,
    plane: RecoveryPlane,
    logical_role: str,
    artifact_role: RecoveryArtifactRole,
    artifact_path: Path,
    producer_commit: str,
    generation_id: str,
    schema_version: str,
    available_at: AwareUtcDatetime,
    watermark: RecoveryWatermarkSummary,
) -> RecoveryAuthorityManifest:
    inspected = _inspect_regular_file(Path(artifact_path), label="authority artifact")
    return RecoveryAuthorityManifest(
        plane=plane,
        logical_role=logical_role,
        artifact_role=artifact_role,
        artifact_path=str(Path(artifact_path)),
        artifact_size_bytes=inspected.size_bytes,
        artifact_sha256=inspected.sha256,
        producer_commit=producer_commit,
        generation_id=generation_id,
        schema_version=schema_version,
        available_at=available_at,
        watermark=watermark,
    )


def _serialized(model: RuntimeContractModel) -> bytes:
    return (model.model_dump_json() + "\n").encode("utf-8")


def _fsync_directory(descriptor: int) -> None:
    os.fsync(descriptor)


def _read_regular_at(
    directory_descriptor: int, name: str, *, label: str
) -> tuple[bytes, _FileDigest]:
    for attempt in range(51):
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_descriptor,
            )
        except OSError as exc:
            raise RuntimeRecoveryCoordinatorError(f"cannot open {label}: {name}") from exc
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise RuntimeRecoveryCoordinatorError(f"{label} is not an isolated regular file")
            if before.st_nlink != 1:
                if before.st_nlink != 2 or attempt == 50:
                    raise RuntimeRecoveryCoordinatorError(f"{label} is a hardlink")
            else:
                if before.st_size > _MAX_DOCUMENT_SIZE:
                    raise RuntimeRecoveryCoordinatorError(f"{label} exceeds document size limit")
                content = bytearray()
                digest = hashlib.sha256()
                while chunk := os.read(
                    descriptor,
                    min(_CHUNK_SIZE, _MAX_DOCUMENT_SIZE + 1 - len(content)),
                ):
                    content.extend(chunk)
                    digest.update(chunk)
                    if len(content) > _MAX_DOCUMENT_SIZE:
                        raise RuntimeRecoveryCoordinatorError(
                            f"{label} exceeds document size limit"
                        )
                after = os.fstat(descriptor)
                if _identity(before) != _identity(after) or len(content) != after.st_size:
                    raise RuntimeRecoveryCoordinatorError(f"{label} changed while reading")
                return bytes(content), _FileDigest(
                    size_bytes=len(content),
                    sha256=digest.hexdigest(),
                    device=before.st_dev,
                    inode=before.st_ino,
                )
        finally:
            os.close(descriptor)
        time.sleep(0.002)
    raise AssertionError("unreachable")


def _append_immutable(directory: Path, object_name: str, content: bytes) -> Path:
    with _DIRECTORY_MUTATION_LOCK:
        return _append_immutable_locked(directory, object_name, content)


def _append_immutable_locked(directory: Path, object_name: str, content: bytes) -> Path:
    directory_lease = _open_physical_directory(
        directory,
        create=True,
        label="immutable store",
    )
    directory_descriptor = directory_lease.descriptor
    temporary = f".{object_name}.{uuid.uuid4().hex}.tmp"
    try:
        directory_before = os.fstat(directory_descriptor)
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_descriptor,
        )
        directory_lease.record_owned_entry_creation(
            before=directory_before,
            after=os.fstat(directory_descriptor),
            label="immutable store",
        )
        try:
            view = memoryview(content)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            directory_before = os.fstat(directory_descriptor)
            os.link(
                temporary,
                object_name,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            directory_lease.record_owned_entry_creation(
                before=directory_before,
                after=os.fstat(directory_descriptor),
                label="immutable store",
            )
        except FileExistsError:
            existing, _ = _read_regular_at(
                directory_descriptor,
                object_name,
                label="immutable object",
            )
            if existing != content:
                raise RuntimeRecoveryCoordinatorError(
                    f"immutable object differs: {directory / object_name}"
                ) from None
        finally:
            directory_before = os.fstat(directory_descriptor)
            os.unlink(temporary, dir_fd=directory_descriptor)
            directory_lease.record_owned_entry_deletion(
                before=directory_before,
                after=os.fstat(directory_descriptor),
                label="immutable store",
            )
        _fsync_directory(directory_descriptor)
        _verify_directory_binding(directory_lease, label="immutable store")
        return directory / object_name
    finally:
        directory_lease.close()


def append_recovery_authority_manifest(
    directory: Path,
    manifest: RecoveryAuthorityManifest,
) -> Path:
    if manifest.authority_id is None:
        raise RuntimeRecoveryCoordinatorError("authority manifest has no content identity")
    return _append_immutable(
        Path(directory),
        f"{manifest.authority_id}.json",
        _serialized(manifest),
    )


def _load_authority(path: Path) -> tuple[RecoveryAuthorityManifest, _FileDigest]:
    content, inspected = _read_document(path, label="authority manifest")
    try:
        manifest = RecoveryAuthorityManifest.model_validate_json(content)
    except ValueError as exc:
        raise RuntimeRecoveryCoordinatorError(f"invalid authority manifest {path}: {exc}") from exc
    if path.name != f"{manifest.authority_id}.json":
        raise RuntimeRecoveryCoordinatorError("authority manifest id/path mismatch")
    if content != _serialized(manifest):
        raise RuntimeRecoveryCoordinatorError("authority manifest is not canonical")
    return manifest, inspected


def _path_within_root(path: Path, root: Path) -> bool:
    canonical_path = _canonical_absolute_path(path, label="artifact path")
    canonical_root = _canonical_absolute_path(root, label="allowed root")
    if not canonical_path.is_relative_to(canonical_root):
        return False
    root_lease = _open_physical_directory(canonical_root, create=False, label="allowed root")
    try:
        parent_lease = _open_physical_directory(
            canonical_path.parent,
            create=False,
            label="artifact parent",
        )
        try:
            _verify_directory_binding(root_lease, label="allowed root")
            _verify_directory_binding(parent_lease, label="artifact parent")
        finally:
            parent_lease.close()
    finally:
        root_lease.close()
    return True


class _ObjectCopyIntent(RuntimeContractModel):
    intent_id: Sha256 | None = None
    schema_version: Literal[1] = 1
    owner_id: str = Field(min_length=32, max_length=32)
    owner_pid: int = Field(ge=1)
    owner_uid: int = Field(ge=0, default_factory=os.getuid)
    owner_gid: int = Field(ge=0, default_factory=os.getgid)
    temporary_name: str
    logical_role: str = Field(min_length=1)
    expected_size: int = Field(ge=0)
    expected_sha256: Sha256
    created_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.temporary_name != f".object.{self.owner_id}.tmp":
            raise ValueError("object copy intent temporary name/owner mismatch")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"intent_id"}))
        if self.intent_id is not None and self.intent_id != expected:
            raise ValueError("object copy intent identity does not match content")
        object.__setattr__(self, "intent_id", expected)
        return self


def _object_copy_intent_name(owner_id: str) -> str:
    return f".object.{owner_id}.intent.json"


def _write_object_copy_intent_at(
    store_lease: _DirectoryLease,
    intent: _ObjectCopyIntent,
) -> None:
    name = _object_copy_intent_name(intent.owner_id)
    before = os.fstat(store_lease.descriptor)
    descriptor = os.open(
        name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
        dir_fd=store_lease.descriptor,
    )
    store_lease.record_owned_entry_creation(
        before=before,
        after=os.fstat(store_lease.descriptor),
        label="backup object store",
    )
    try:
        content = memoryview(_serialized(intent))
        while content:
            written = os.write(descriptor, content)
            content = content[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(store_lease.descriptor)


def _write_object_copy_intent(object_store: Path, intent: _ObjectCopyIntent) -> None:
    store_lease = _open_physical_directory(
        object_store,
        create=True,
        label="backup object store",
    )
    try:
        _recover_interrupted_object_copies(store_lease)
        _write_object_copy_intent_at(store_lease, intent)
    finally:
        store_lease.close()


def _owner_process_is_alive(owner_pid: int) -> bool:
    try:
        os.kill(owner_pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _recover_interrupted_object_copies(store_lease: _DirectoryLease) -> None:
    intent_names = sorted(
        name
        for name in os.listdir(store_lease.descriptor)
        if name.startswith(".object.") and name.endswith(".intent.json")
    )
    for intent_name in intent_names:
        content, _ = _read_regular_at(
            store_lease.descriptor,
            intent_name,
            label="object copy intent",
        )
        try:
            intent = _ObjectCopyIntent.model_validate_json(content)
        except ValueError as exc:
            raise RuntimeRecoveryCoordinatorError(f"invalid object copy intent: {exc}") from exc
        if content != _serialized(intent) or intent_name != _object_copy_intent_name(
            intent.owner_id
        ):
            raise RuntimeRecoveryCoordinatorError(
                "object copy intent is not canonical or owner-bound"
            )
        if _owner_process_is_alive(intent.owner_pid):
            continue
        try:
            observed = os.stat(
                intent.temporary_name,
                dir_fd=store_lease.descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != intent.owner_uid
                or observed.st_gid != intent.owner_gid
                or stat.S_IMODE(observed.st_mode) != 0o600
                or observed.st_nlink != 1
            ):
                raise RuntimeRecoveryCoordinatorError(
                    "interrupted object temporary file ownership changed"
                )
            _remove_managed_file(
                store_lease,
                name=intent.temporary_name,
                label="backup object store",
            )
        _remove_managed_file(
            store_lease,
            name=intent_name,
            label="backup object store",
        )
    _verify_directory_binding(store_lease, label="backup object store")


def _copy_source_to_object(
    source: Path,
    *,
    object_store: Path,
    expected_size: int,
    expected_sha256: str,
    logical_role: str,
    source_trust_root: Path,
) -> _FileDigest:
    with _DIRECTORY_MUTATION_LOCK:
        return _copy_source_to_object_locked(
            source,
            object_store=object_store,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
            logical_role=logical_role,
            source_trust_root=source_trust_root,
        )


def _copy_source_to_object_locked(
    source: Path,
    *,
    object_store: Path,
    expected_size: int,
    expected_sha256: str,
    logical_role: str,
    source_trust_root: Path,
) -> _FileDigest:
    store_lease = _open_physical_directory(
        object_store,
        create=True,
        label="backup object store",
    )
    try:
        _recover_interrupted_object_copies(store_lease)
        source_descriptor, source_parent_lease = _open_regular_readonly(
            source,
            label=f"authority artifact {logical_role}",
            trust_root=source_trust_root,
        )
    except Exception:
        store_lease.close()
        raise
    store_descriptor = store_lease.descriptor
    owner_id = uuid.uuid4().hex
    temporary = f".object.{owner_id}.tmp"
    intent = _ObjectCopyIntent(
        owner_id=owner_id,
        owner_pid=os.getpid(),
        temporary_name=temporary,
        logical_role=logical_role,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
        created_at=datetime.now(UTC),
    )
    target_descriptor = -1
    temporary_created = False
    intent_written = False
    try:
        before = os.fstat(source_descriptor)
        _write_object_copy_intent_at(store_lease, intent)
        intent_written = True
        store_before = os.fstat(store_descriptor)
        target_descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=store_descriptor,
        )
        temporary_created = True
        store_lease.record_owned_entry_creation(
            before=store_before,
            after=os.fstat(store_descriptor),
            label="backup object store",
        )
        digest = hashlib.sha256()
        size = 0
        while chunk := os.read(source_descriptor, _CHUNK_SIZE):
            digest.update(chunk)
            size += len(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(target_descriptor, view)
                view = view[written:]
        os.fsync(target_descriptor)
        os.close(target_descriptor)
        target_descriptor = -1
        after = _verify_regular_binding(
            name=source.name,
            descriptor=source_descriptor,
            parent_lease=source_parent_lease,
            label=f"authority artifact {logical_role}",
            expected=_file_binding(before),
        )
        if _identity(before) != _identity(after) or size != after.st_size or after.st_nlink != 1:
            raise RuntimeRecoveryCoordinatorError(
                f"authority artifact {logical_role} changed while copying"
            )
        observed_sha256 = digest.hexdigest()
        if size != expected_size or observed_sha256 != expected_sha256:
            raise RuntimeRecoveryCoordinatorError(
                f"authority artifact hash/size mismatch for {logical_role}"
            )
        object_name = f"{observed_sha256}.blob"
        try:
            store_before = os.fstat(store_descriptor)
            os.link(
                temporary,
                object_name,
                src_dir_fd=store_descriptor,
                dst_dir_fd=store_descriptor,
                follow_symlinks=False,
            )
            store_lease.record_owned_entry_creation(
                before=store_before,
                after=os.fstat(store_descriptor),
                label="backup object store",
            )
        except FileExistsError:
            existing = _inspect_regular_at(
                store_descriptor,
                object_name,
                label=f"backup object {logical_role}",
            )
            if existing.size_bytes != size or existing.sha256 != observed_sha256:
                raise RuntimeRecoveryCoordinatorError(
                    f"backup object hash/size mismatch for {logical_role}"
                ) from None
        finally:
            _remove_managed_file(
                store_lease,
                name=temporary,
                label="backup object store",
            )
            temporary_created = False
        _remove_managed_file(
            store_lease,
            name=_object_copy_intent_name(owner_id),
            label="backup object store",
        )
        intent_written = False
        _fsync_directory(store_descriptor)
        _verify_directory_binding(store_lease, label="backup object store")
        return _FileDigest(
            size_bytes=size,
            sha256=observed_sha256,
            device=before.st_dev,
            inode=before.st_ino,
        )
    finally:
        if target_descriptor >= 0:
            os.close(target_descriptor)
        if temporary_created:
            _remove_managed_file(
                store_lease,
                name=temporary,
                label="backup object store",
            )
        if intent_written:
            _remove_managed_file(
                store_lease,
                name=_object_copy_intent_name(owner_id),
                label="backup object store",
            )
        store_lease.close()
        os.close(source_descriptor)
        source_parent_lease.close()


def _inspect_regular_at(directory_descriptor: int, name: str, *, label: str) -> _FileDigest:
    for attempt in range(51):
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_descriptor,
            )
        except OSError as exc:
            raise RuntimeRecoveryCoordinatorError(f"cannot open {label}: {name}") from exc
        try:
            observed = os.fstat(descriptor)
            if not stat.S_ISREG(observed.st_mode):
                raise RuntimeRecoveryCoordinatorError(f"{label} is not an isolated regular file")
            if observed.st_nlink == 1:
                return _stream_digest_descriptor(descriptor, label=label)
            if observed.st_nlink != 2 or attempt == 50:
                raise RuntimeRecoveryCoordinatorError(f"{label} is a hardlink")
        finally:
            os.close(descriptor)
        time.sleep(0.002)
    raise AssertionError("unreachable")


def _read_canonical_json_model(
    context: RecoveryArtifactVerificationContext,
    model: type[RuntimeContractModel],
) -> RuntimeContractModel:
    content, _ = _read_document(
        Path(context.artifact_path),
        label=f"typed JSON artifact {context.logical_role}",
        trust_root=Path(context.trust_root),
    )
    try:
        parsed = model.model_validate_json(content)
    except ValueError as exc:
        raise RuntimeRecoveryCoordinatorError(
            f"invalid typed JSON artifact for {context.logical_role}: {exc}"
        ) from exc
    if content != _serialized(parsed):
        raise RuntimeRecoveryCoordinatorError(
            f"typed JSON artifact is not canonical for {context.logical_role}"
        )
    return parsed


def _verify_production_duckdb(
    context: RecoveryArtifactVerificationContext,
) -> tuple[RecoveryWatermarkSummary, bool]:
    try:
        connection = duckdb.connect(context.artifact_path, read_only=True)
        try:
            connection.execute("SET memory_limit='4MB'")
            connection.execute("SET threads=1")
            row_count, max_date = connection.execute(
                "SELECT COUNT(*), MAX(trade_date) FROM daily_bar"
            ).fetchone()
        finally:
            connection.close()
    except Exception as exc:
        raise RuntimeRecoveryCoordinatorError(
            f"production DuckDB schema verification failed: {exc}"
        ) from exc
    return RecoveryWatermarkSummary(row_count=row_count, max_date=max_date), False


def _verify_sqlite_state(
    context: RecoveryArtifactVerificationContext,
) -> tuple[RecoveryWatermarkSummary, bool]:
    try:
        connection = sqlite3.connect(
            f"file:{context.artifact_path}?mode=ro&immutable=1",
            uri=True,
        )
        try:
            cursor = connection.execute("SELECT trade_date FROM events")
            row_count = 0
            max_date: date | None = None
            while batch := cursor.fetchmany(8192):
                row_count += len(batch)
                for (raw_trade_date,) in batch:
                    trade_date = (
                        date.fromisoformat(raw_trade_date) if raw_trade_date is not None else None
                    )
                    if trade_date is not None and (max_date is None or trade_date > max_date):
                        max_date = trade_date
        finally:
            connection.close()
    except Exception as exc:
        raise RuntimeRecoveryCoordinatorError(
            f"SQLite state schema verification failed: {exc}"
        ) from exc
    return (
        RecoveryWatermarkSummary(
            row_count=row_count,
            max_date=max_date,
        ),
        False,
    )


def _verify_watermark_json(
    context: RecoveryArtifactVerificationContext,
) -> tuple[RecoveryWatermarkSummary, bool]:
    document = _WatermarkJsonDocument.model_validate(
        _read_canonical_json_model(context, _WatermarkJsonDocument)
    )
    return (
        RecoveryWatermarkSummary(max_date=document.max_date, row_count=document.row_count),
        False,
    )


def _verify_lake_manifest(
    context: RecoveryArtifactVerificationContext,
) -> tuple[RecoveryWatermarkSummary, bool]:
    try:
        import pyarrow.parquet as parquet

        parquet_file = parquet.ParquetFile(context.artifact_path)
        if "trade_date" not in parquet_file.schema_arrow.names:
            raise RuntimeRecoveryCoordinatorError("lake manifest Parquet is missing trade_date")
        row_count = 0
        max_date: date | None = None
        for batch in parquet_file.iter_batches(
            batch_size=512,
            columns=["trade_date"],
            use_threads=False,
        ):
            if batch.num_rows > 512:
                raise RuntimeRecoveryCoordinatorError(
                    "lake manifest Parquet reader exceeded its batch budget"
                )
            row_count += batch.num_rows
            values = batch.column(0)
            for index in range(batch.num_rows):
                scalar = values[index]
                if not scalar.is_valid:
                    continue
                raw_value = scalar.as_py()
                trade_date = raw_value.date() if isinstance(raw_value, datetime) else raw_value
                if not isinstance(trade_date, date):
                    raise RuntimeRecoveryCoordinatorError(
                        "lake manifest trade_date has an invalid type"
                    )
                if max_date is None or trade_date > max_date:
                    max_date = trade_date
    except Exception as exc:
        if isinstance(exc, RuntimeRecoveryCoordinatorError):
            raise
        raise RuntimeRecoveryCoordinatorError(
            f"lake manifest schema verification failed: {exc}"
        ) from exc
    return RecoveryWatermarkSummary(row_count=row_count, max_date=max_date), False


def _verify_artifact_metadata_document(
    context: RecoveryArtifactVerificationContext,
) -> tuple[
    RecoveryWatermarkSummary,
    bool,
    tuple[_VerifiedStrategyReplay, ...],
]:
    document = _ArtifactMetadataDocument.model_validate(
        _read_canonical_json_model(context, _ArtifactMetadataDocument)
    )
    reference_roles = tuple(item.logical_role for item in document.references)
    if len(reference_roles) != len(set(reference_roles)):
        raise RuntimeRecoveryCoordinatorError("artifact metadata references must be unique")
    for reference in document.references:
        related = context.reference_by_role(reference.logical_role)
        if related.artifact_sha256 != reference.sha256:
            raise RuntimeRecoveryCoordinatorError(
                f"artifact metadata reference hash mismatch for {reference.logical_role}"
            )
    dataset = context.reference_by_role("production.duckdb")
    verified: list[_VerifiedStrategyReplay] = []
    for evidence in document.fixed_replays:
        if evidence.result.dataset_snapshot_id != dataset.artifact_sha256:
            raise RuntimeRecoveryCoordinatorError(
                "fixed replay result is not bound to the referenced dataset snapshot"
            )
    replay_results = _run_fixed_replay_batch_v1(
        dataset_path=Path(dataset.artifact_path),
        dataset_sha256=dataset.artifact_sha256,
        trust_root=Path(context.trust_root),
        expected=document.fixed_replays,
    )
    for evidence, expected_result in zip(
        document.fixed_replays,
        replay_results,
        strict=True,
    ):
        if evidence.result != expected_result:
            raise RuntimeRecoveryCoordinatorError(
                "trusted fixed replay result does not match recomputed metrics"
            )
        verified.append(
            _VerifiedStrategyReplay(
                strategy_id=evidence.strategy_id,
                registration_fingerprint=(expected_result.strategy_registration_fingerprint),
                definition_fingerprint=expected_result.strategy_definition_sha256,
                executable_fingerprint=expected_result.strategy_executable_sha256,
                result_fingerprint=str(expected_result.result_sha256),
            )
        )
    return (
        RecoveryWatermarkSummary(max_date=document.max_date, row_count=document.row_count),
        True,
        tuple(verified),
    )


def _build_trusted_verifier_resolver() -> Callable[
    [RecoveryArtifactRole, str], _TrustedVerifierDefinition | None
]:
    definitions = (
        _TrustedVerifierDefinition.build(
            artifact_role=RecoveryArtifactRole.PRODUCTION_DUCKDB,
            schema_version="v1",
            verifier_id="rquant.production-duckdb.daily-bar",
            verifier_version=1,
            verify=_verify_production_duckdb,
        ),
        _TrustedVerifierDefinition.build(
            artifact_role=RecoveryArtifactRole.SQLITE_STATE,
            schema_version="v1",
            verifier_id="rquant.sqlite-state.events",
            verifier_version=1,
            verify=_verify_sqlite_state,
        ),
        _TrustedVerifierDefinition.build(
            artifact_role=RecoveryArtifactRole.RESEARCH_CATALOG,
            schema_version="v1",
            verifier_id="rquant.research-catalog.document",
            verifier_version=1,
            verify=_verify_watermark_json,
        ),
        _TrustedVerifierDefinition.build(
            artifact_role=RecoveryArtifactRole.LAKE_MANIFEST,
            schema_version="v1",
            verifier_id="rquant.lake-manifest.parquet",
            verifier_version=1,
            verify=_verify_lake_manifest,
        ),
        _TrustedVerifierDefinition.build(
            artifact_role=RecoveryArtifactRole.ARTIFACT_METADATA,
            schema_version="v1",
            verifier_id="rquant.artifact-metadata.fixed-replay",
            verifier_version=1,
            verify=_verify_artifact_metadata_document,
            dependencies=(
                ("_run_fixed_replay_batch_v1", _run_fixed_replay_batch_v1),
                ("_compute_strategy_fixed_replay_v1", _compute_strategy_fixed_replay_v1),
                ("_run_strategy_fixed_replay_v1", _run_strategy_fixed_replay_v1),
                ("_run_n_shape_fixed_replay_v1", _run_n_shape_fixed_replay_v1),
                (
                    "_strategy_fixed_replay_executable_sha256",
                    _strategy_fixed_replay_executable_sha256,
                ),
                ("_strategy_replay_graph_sha256", _strategy_replay_graph_sha256),
                (
                    "_n_shape_strategy_executable_sha256",
                    _n_shape_strategy_executable_sha256,
                ),
                (
                    "_assert_fixed_replay_working_set_is_bounded",
                    _assert_fixed_replay_working_set_is_bounded,
                ),
            ),
        ),
        _TrustedVerifierDefinition.build(
            artifact_role=RecoveryArtifactRole.SERVING_CURRENT,
            schema_version="v1",
            verifier_id="rquant.serving-current.document",
            verifier_version=1,
            verify=_verify_watermark_json,
        ),
        _TrustedVerifierDefinition.build(
            artifact_role=RecoveryArtifactRole.SERVING_MANIFEST,
            schema_version="v1",
            verifier_id="rquant.serving-manifest.document",
            verifier_version=1,
            verify=_verify_watermark_json,
        ),
    )
    registry = MappingProxyType(
        {(item.artifact_role, item.schema_version): item for item in definitions}
    )

    def resolve(
        artifact_role: RecoveryArtifactRole,
        schema_version: str,
    ) -> _TrustedVerifierDefinition | None:
        definition = registry.get((artifact_role, schema_version))
        if definition is not None:
            definition.assert_implementation_is_trusted()
        return definition

    return resolve


_trusted_verifier_resolver_default = _build_trusted_verifier_resolver()


def _call_verifier(
    context: RecoveryArtifactVerificationContext,
    *,
    expected: RecoveryWatermarkSummary,
    _resolve: Callable[
        [RecoveryArtifactRole, str], _TrustedVerifierDefinition | None
    ] = _trusted_verifier_resolver_default,
) -> _TrustedVerificationResult:
    definition = _resolve(context.artifact_role, context.schema_version)
    if definition is None:
        raise RuntimeRecoveryCoordinatorError(
            f"no trusted verifier for {context.artifact_role.value} schema {context.schema_version}"
        )
    descriptor, parent_lease = _open_regular_readonly(
        Path(context.artifact_path),
        label=f"trusted verifier input {context.logical_role}",
        trust_root=Path(context.trust_root),
    )
    try:
        opened_before = os.fstat(descriptor)
        try:
            verifier_output = definition.verify(context)
            if len(verifier_output) == 2:
                observed, fixed_replay_verified = verifier_output
                strategy_replays: tuple[_VerifiedStrategyReplay, ...] = ()
            else:
                observed, fixed_replay_verified, strategy_replays = verifier_output
            observed = RecoveryWatermarkSummary.model_validate(observed)
            strategy_replays = tuple(
                _VerifiedStrategyReplay.model_validate(item) for item in strategy_replays
            )
        except Exception as exc:
            raise RuntimeRecoveryCoordinatorError(
                f"verifier failed for {context.logical_role}: {exc}"
            ) from exc
        opened_after = _verify_regular_binding(
            name=Path(context.artifact_path).name,
            descriptor=descriptor,
            parent_lease=parent_lease,
            label=f"trusted verifier input {context.logical_role}",
            expected=_file_binding(opened_before),
        )
        if _identity(opened_before) != _identity(opened_after):
            raise RuntimeRecoveryCoordinatorError(
                f"trusted verifier input changed for {context.logical_role}"
            )
        os.lseek(descriptor, 0, os.SEEK_SET)
        after = _stream_digest_descriptor(
            descriptor,
            label=f"trusted verifier input {context.logical_role}",
        )
    finally:
        os.close(descriptor)
        parent_lease.close()
    if observed != expected:
        raise RuntimeRecoveryCoordinatorError(
            f"watermark mismatch for {context.logical_role}: "
            f"expected {expected}, observed {observed}"
        )
    if after.size_bytes != context.artifact_size_bytes or after.sha256 != context.artifact_sha256:
        raise RuntimeRecoveryCoordinatorError(
            f"verified artifact hash/size mismatch for {context.logical_role}"
        )
    return _TrustedVerificationResult(
        watermark=observed,
        verifier_id=definition.verifier_id,
        verifier_version=definition.verifier_version,
        verifier_fingerprint=definition.fingerprint,
        fixed_replay_verified=fixed_replay_verified,
        strategy_replays=strategy_replays,
    )


del _trusted_verifier_resolver_default


def _assess_rpo(
    recovered_watermarks: tuple[RecoveryRoleWatermark, ...],
    target_watermarks: Mapping[str, RecoveryWatermarkSummary] | None,
) -> tuple[bool, tuple[RecoveryRoleRpoLoss, ...], tuple[str, ...]]:
    recovered_by_role = {item.logical_role: item.watermark for item in recovered_watermarks}
    targets = target_watermarks or {}
    unknown = sorted(set(targets) - set(recovered_by_role))
    if unknown:
        raise RuntimeRecoveryCoordinatorError(
            f"RPO targets contain unknown logical roles: {', '.join(unknown)}"
        )
    missing_target_roles = tuple(sorted(set(recovered_by_role) - set(targets)))
    losses: list[RecoveryRoleRpoLoss] = []
    for logical_role in sorted(targets):
        try:
            target = RecoveryWatermarkSummary.model_validate(targets[logical_role])
        except ValueError as exc:
            raise RuntimeRecoveryCoordinatorError(
                f"invalid RPO target watermark for {logical_role}: {exc}"
            ) from exc
        recovered = recovered_by_role[logical_role]
        missing: list[str] = []
        high_watermark_mismatch: bool | None = None
        if target.high_watermark is not None:
            if recovered.high_watermark is None:
                missing.append("high_watermark")
            else:
                high_watermark_mismatch = recovered.high_watermark != target.high_watermark
        max_date_loss_days: int | None = None
        if target.max_date is not None:
            if recovered.max_date is None:
                missing.append("max_date")
            else:
                max_date_loss_days = max(0, (target.max_date - recovered.max_date).days)
        row_count_loss: int | None = None
        if target.row_count is not None:
            if recovered.row_count is None:
                missing.append("row_count")
            else:
                row_count_loss = max(0, target.row_count - recovered.row_count)
        losses.append(
            RecoveryRoleRpoLoss(
                logical_role=logical_role,
                target_watermark=target,
                recovered_watermark=recovered,
                high_watermark_mismatch=high_watermark_mismatch,
                max_date_loss_days=max_date_loss_days,
                row_count_loss=row_count_loss,
                missing_components=tuple(missing),
            )
        )
    assessed = tuple(losses)
    return (
        not missing_target_roles and all(item.met for item in assessed),
        assessed,
        missing_target_roles,
    )


def _ensure_managed_restore_layout(target_lease: _DirectoryLease) -> None:
    for name in (
        ".candidates",
        ".failed",
        "audits",
        "generations",
        "reports",
        "transactions",
    ):
        lease = _open_relative_directories(target_lease, (name,))
        lease.close()
    _verify_directory_binding(target_lease, label="restore target")


def _read_current_pointer(target: Path) -> tuple[RecoveryCurrentPointer | None, bytes | None]:
    path = target / "current.json"
    try:
        content, _ = _read_document(path, label="restore current pointer")
    except RuntimeRecoveryCoordinatorError as exc:
        if not path.exists():
            return None, None
        raise exc
    try:
        pointer = RecoveryCurrentPointer.model_validate_json(content)
    except ValueError as exc:
        raise RuntimeRecoveryCoordinatorError(f"invalid restore current pointer: {exc}") from exc
    if content != _serialized(pointer):
        raise RuntimeRecoveryCoordinatorError("restore current pointer is not canonical")
    return pointer, content


def _atomic_replace_managed_file(
    directory_lease: _DirectoryLease,
    *,
    name: str,
    content: bytes,
    label: str,
) -> None:
    temporary = f".{name}.{uuid.uuid4().hex}.tmp"
    descriptor = -1
    created = False
    try:
        before = os.fstat(directory_lease.descriptor)
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_lease.descriptor,
        )
        created = True
        directory_lease.record_owned_entry_creation(
            before=before,
            after=os.fstat(directory_lease.descriptor),
            label=label,
        )
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        before = os.fstat(directory_lease.descriptor)
        try:
            os.replace(
                temporary,
                name,
                src_dir_fd=directory_lease.descriptor,
                dst_dir_fd=directory_lease.descriptor,
            )
        except Exception:
            try:
                os.stat(temporary, dir_fd=directory_lease.descriptor, follow_symlinks=False)
            except FileNotFoundError:
                observed, _ = _read_regular_at(
                    directory_lease.descriptor,
                    name,
                    label=label,
                )
                if observed == content:
                    created = False
                    directory_lease.record_owned_entry_rebind(
                        before=before,
                        after=os.fstat(directory_lease.descriptor),
                        label=label,
                    )
                    _fsync_directory(directory_lease.descriptor)
            raise
        created = False
        directory_lease.record_owned_entry_rebind(
            before=before,
            after=os.fstat(directory_lease.descriptor),
            label=label,
        )
        _fsync_directory(directory_lease.descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if created:
            before = os.fstat(directory_lease.descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=directory_lease.descriptor)
                directory_lease.record_owned_entry_deletion(
                    before=before,
                    after=os.fstat(directory_lease.descriptor),
                    label=label,
                )


def _atomic_replace_current(target_lease: _DirectoryLease, content: bytes) -> None:
    _atomic_replace_managed_file(
        target_lease,
        name="current.json",
        content=content,
        label="restore target",
    )


def _remove_current_pointer(target_lease: _DirectoryLease) -> None:
    before = os.fstat(target_lease.descriptor)
    try:
        os.unlink("current.json", dir_fd=target_lease.descriptor)
    except FileNotFoundError:
        return
    target_lease.record_owned_entry_deletion(
        before=before,
        after=os.fstat(target_lease.descriptor),
        label="restore target",
    )
    _fsync_directory(target_lease.descriptor)


def _remove_managed_file(
    directory_lease: _DirectoryLease,
    *,
    name: str,
    label: str,
) -> None:
    before = os.fstat(directory_lease.descriptor)
    try:
        os.unlink(name, dir_fd=directory_lease.descriptor)
    except FileNotFoundError:
        return
    directory_lease.record_owned_entry_deletion(
        before=before,
        after=os.fstat(directory_lease.descriptor),
        label=label,
    )
    _fsync_directory(directory_lease.descriptor)


def _rename_managed_directory(
    *,
    target: Path,
    source_parent: str,
    source_name: str,
    destination_parent: str,
    destination_name: str,
) -> None:
    source_lease = _open_physical_directory(
        target / source_parent,
        create=False,
        label=f"restore {source_parent}",
    )
    destination_lease = _open_physical_directory(
        target / destination_parent,
        create=False,
        label=f"restore {destination_parent}",
    )
    try:
        source_before = os.fstat(source_lease.descriptor)
        destination_before = os.fstat(destination_lease.descriptor)
        os.rename(
            source_name,
            destination_name,
            src_dir_fd=source_lease.descriptor,
            dst_dir_fd=destination_lease.descriptor,
        )
        source_lease.record_owned_entry_deletion(
            before=source_before,
            after=os.fstat(source_lease.descriptor),
            label=f"restore {source_parent}",
        )
        destination_lease.record_owned_entry_creation(
            before=destination_before,
            after=os.fstat(destination_lease.descriptor),
            label=f"restore {destination_parent}",
        )
        _fsync_directory(source_lease.descriptor)
        _fsync_directory(destination_lease.descriptor)
    finally:
        destination_lease.close()
        source_lease.close()


def _append_rehearsal_audit(target: Path, audit: RecoveryRehearsalAudit) -> Path:
    if audit.audit_id is None:
        raise RuntimeRecoveryCoordinatorError("rehearsal audit has no identity")
    return _append_immutable(
        target / "audits",
        f"{audit.audit_id}.json",
        _serialized(audit),
    )


def _require_original_success_audit(
    target: Path,
    *,
    pointer: RecoveryCurrentPointer,
    current_content: bytes,
) -> RecoveryRehearsalAudit:
    audit_lease = _open_physical_directory(
        target / "audits",
        create=False,
        label="restore rehearsal audits",
        trust_root=target,
    )
    matches: list[RecoveryRehearsalAudit] = []
    try:
        for name in sorted(os.listdir(audit_lease.descriptor)):
            if not name.endswith(".json"):
                raise RuntimeRecoveryCoordinatorError(
                    "restore rehearsal audit store contains an unmanaged entry"
                )
            content, _ = _read_regular_at(
                audit_lease.descriptor,
                name,
                label="restore rehearsal audit",
            )
            try:
                audit = RecoveryRehearsalAudit.model_validate_json(content)
            except ValueError as exc:
                raise RuntimeRecoveryCoordinatorError(
                    f"invalid restore rehearsal audit: {exc}"
                ) from exc
            if content != _serialized(audit) or name != f"{audit.audit_id}.json":
                raise RuntimeRecoveryCoordinatorError(
                    "restore rehearsal audit identity is not canonical"
                )
            if (
                audit.status == "passed"
                and audit.manifest_id == pointer.manifest_id
                and audit.generation_id == pointer.generation_id
                and audit.report_sha256 == pointer.report_sha256
                and audit.deployment_profile_id == pointer.deployment_profile_id
                and audit.deployment_profile_generation == pointer.deployment_profile_generation
                and audit.strategy_lineage == pointer.strategy_lineage
                and audit.published_current_sha256 == hashlib.sha256(current_content).hexdigest()
            ):
                matches.append(audit)
        _verify_directory_binding(audit_lease, label="restore rehearsal audits")
    finally:
        audit_lease.close()
    if len(matches) != 1:
        raise RuntimeRecoveryCoordinatorError(
            "restore current pointer must have exactly one canonical success audit"
        )
    return matches[0]


def _append_rehearsal_report(
    target: Path,
    report: RecoveryRehearsalReport,
) -> tuple[str, Path]:
    report_sha256 = canonical_sha256(report.model_dump(mode="python"))
    path = _append_immutable(
        target / "reports",
        f"{report_sha256}.json",
        _serialized(report),
    )
    return report_sha256, path


def _load_rehearsal_report(target: Path, report_sha256: str) -> RecoveryRehearsalReport:
    content, _ = _read_document(
        target / "reports" / f"{report_sha256}.json",
        label="restore rehearsal report",
        trust_root=target,
    )
    try:
        report = RecoveryRehearsalReport.model_validate_json(content)
    except ValueError as exc:
        raise RuntimeRecoveryCoordinatorError(f"invalid restore rehearsal report: {exc}") from exc
    if content != _serialized(report):
        raise RuntimeRecoveryCoordinatorError("restore rehearsal report is not canonical")
    if canonical_sha256(report.model_dump(mode="python")) != report_sha256:
        raise RuntimeRecoveryCoordinatorError("restore rehearsal report hash mismatch")
    return report


def _read_publish_intent(target: Path) -> _RecoveryPublishIntent | None:
    path = target / "transactions" / "active.json"
    try:
        content, _ = _read_document(
            path,
            label="restore publish intent",
            trust_root=target,
        )
    except RuntimeRecoveryCoordinatorError as exc:
        if not path.exists():
            return None
        raise exc
    try:
        intent = _RecoveryPublishIntent.model_validate_json(content)
    except ValueError as exc:
        raise RuntimeRecoveryCoordinatorError(f"invalid restore publish intent: {exc}") from exc
    if content != _serialized(intent):
        raise RuntimeRecoveryCoordinatorError("restore publish intent is not canonical")
    return intent


def _write_publish_intent(target: Path, intent: _RecoveryPublishIntent) -> None:
    transaction_lease = _open_physical_directory(
        target / "transactions",
        create=False,
        label="restore publish transactions",
        trust_root=target,
    )
    try:
        try:
            os.stat(
                "active.json",
                dir_fd=transaction_lease.descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            raise RuntimeRecoveryCoordinatorError("another restore publish intent is active")
        _atomic_replace_managed_file(
            transaction_lease,
            name="active.json",
            content=_serialized(intent),
            label="restore publish transactions",
        )
    finally:
        transaction_lease.close()


_PUBLISH_STAGE_ORDER = MappingProxyType(
    {
        "candidate_prepared": 0,
        "copied": 1,
        "verified": 2,
        "generation_staging": 3,
        "generation_staged": 4,
        "publish_prepared": 5,
        "current_switched": 6,
    }
)


def _advance_publish_intent(
    target: Path,
    intent: _RecoveryPublishIntent,
    *,
    stage: str,
    **updates: object,
) -> _RecoveryPublishIntent:
    current = _read_publish_intent(target)
    if current is None or current.intent_id != intent.intent_id:
        raise RuntimeRecoveryCoordinatorError("restore publish intent ownership changed")
    if stage not in _PUBLISH_STAGE_ORDER:
        raise RuntimeRecoveryCoordinatorError(f"unknown restore publish stage: {stage}")
    if _PUBLISH_STAGE_ORDER[stage] < _PUBLISH_STAGE_ORDER[intent.stage]:
        raise RuntimeRecoveryCoordinatorError("restore publish intent stage cannot move backward")
    recovery_audit = updates.pop("recovery_audit", None)
    if recovery_audit is None:
        recovery_payload = intent.recovery_audit.model_dump(
            mode="python",
            exclude={"audit_id"},
        )
        recovery_payload.update(
            {
                "failure_point": f"interrupted_{stage}",
                "failure_type": "InterruptedRecoveryStage",
                "failure_message": f"restore transaction interrupted during {stage}",
                "finished_at": datetime.now(UTC),
            }
        )
        recovery_audit = RecoveryRehearsalAudit.model_validate(recovery_payload)
    payload = intent.model_dump(mode="python", exclude={"intent_id"})
    payload.update(updates)
    payload.update({"stage": stage, "recovery_audit": recovery_audit})
    advanced = _RecoveryPublishIntent.model_validate(payload)
    transaction_lease = _open_physical_directory(
        target / "transactions",
        create=False,
        label="restore publish transactions",
        trust_root=target,
    )
    try:
        _atomic_replace_managed_file(
            transaction_lease,
            name="active.json",
            content=_serialized(advanced),
            label="restore publish transactions",
        )
    finally:
        transaction_lease.close()
    return advanced


def _remove_publish_intent(target: Path) -> None:
    transaction_lease = _open_physical_directory(
        target / "transactions",
        create=False,
        label="restore publish transactions",
        trust_root=target,
    )
    try:
        _remove_managed_file(
            transaction_lease,
            name="active.json",
            label="restore publish transactions",
        )
    finally:
        transaction_lease.close()


def _audit_is_durable(target: Path, audit: RecoveryRehearsalAudit) -> bool:
    if audit.audit_id is None:
        raise RuntimeRecoveryCoordinatorError("transaction audit has no identity")
    path = target / "audits" / f"{audit.audit_id}.json"
    try:
        content, _ = _read_document(
            path,
            label="restore transaction audit",
            trust_root=target,
        )
    except RuntimeRecoveryCoordinatorError as exc:
        if not path.exists():
            return False
        raise exc
    if content != _serialized(audit):
        raise RuntimeRecoveryCoordinatorError("restore transaction audit differs from intent")
    return True


def _recover_interrupted_publish(
    *,
    target: Path,
    target_lease: _DirectoryLease,
) -> bool:
    intent = _read_publish_intent(target)
    if intent is None:
        return False
    _, current_content = _read_current_pointer(target)
    previous_content = (
        _serialized(intent.previous_current) if intent.previous_current is not None else None
    )
    proposed_content = (
        _serialized(intent.proposed_current) if intent.proposed_current is not None else None
    )
    success_is_durable = intent.success_audit is not None and _audit_is_durable(
        target, intent.success_audit
    )
    if success_is_durable:
        if current_content != proposed_content:
            raise RuntimeRecoveryCoordinatorError(
                "completed restore publish intent has a divergent current pointer"
            )
        _remove_publish_intent(target)
        return True
    allowed_current = {previous_content}
    if proposed_content is not None:
        allowed_current.add(proposed_content)
    if current_content not in allowed_current:
        raise RuntimeRecoveryCoordinatorError(
            "interrupted restore publish intent has a divergent current pointer"
        )
    if proposed_content is not None and current_content == proposed_content:
        if previous_content is None:
            _remove_current_pointer(target_lease)
        else:
            _atomic_replace_current(target_lease, previous_content)
    candidate = target / ".candidates" / intent.candidate_name
    generation = target / "generations" / intent.generation_id
    quarantined = target / ".failed" / intent.operation_id
    owned_generation = intent.generation_created and generation.exists()
    if candidate.exists() and owned_generation:
        raise RuntimeRecoveryCoordinatorError(
            "interrupted restore candidate exists in candidate and generation roots"
        )
    owned_artifact: tuple[str, str] | None = None
    if candidate.exists():
        owned_artifact = (".candidates", intent.candidate_name)
    elif owned_generation:
        owned_artifact = ("generations", intent.generation_id)
    if owned_artifact is not None:
        if quarantined.exists():
            raise RuntimeRecoveryCoordinatorError(
                "interrupted restore artifact exists in active and quarantine roots"
            )
        _rename_managed_directory(
            target=target,
            source_parent=owned_artifact[0],
            source_name=owned_artifact[1],
            destination_parent=".failed",
            destination_name=intent.operation_id,
        )
    _append_rehearsal_audit(target, intent.recovery_audit)
    _remove_publish_intent(target)
    return True


class RuntimeRecoveryCoordinator:
    def __init__(
        self,
        *,
        inventory_plan: RecoveryInventoryPlan,
        manifest_store: Path,
        backup_object_store: Path,
        deployment_topology_id: str,
        deployment_profile_id: str,
        deployment_profile_generation: str,
        strategy_producer_commit: str,
        strategy_bindings: tuple[ProductionStrategyBinding, ...],
    ) -> None:
        self.inventory_plan = inventory_plan
        self.manifest_store = _canonical_absolute_path(
            manifest_store,
            label="manifest_store",
        )
        self.backup_object_store = _canonical_absolute_path(
            backup_object_store,
            label="backup_object_store",
        )
        if _paths_overlap(self.manifest_store, self.backup_object_store):
            raise ValueError("manifest and backup object stores must be physically isolated")
        if len(deployment_topology_id) != 64 or any(
            character not in "0123456789abcdef" for character in deployment_topology_id
        ):
            raise ValueError("deployment_topology_id must be a lowercase SHA-256")
        self.deployment_topology_id = deployment_topology_id
        for label, value in (
            ("deployment_profile_id", deployment_profile_id),
            ("deployment_profile_generation", deployment_profile_generation),
        ):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError(f"{label} must be a lowercase SHA-256")
        if len(strategy_producer_commit) != 40 or any(
            character not in "0123456789abcdef" for character in strategy_producer_commit
        ):
            raise ValueError("strategy_producer_commit must be a lowercase commit SHA")
        self.deployment_profile_id = deployment_profile_id
        self.deployment_profile_generation = deployment_profile_generation
        self.strategy_producer_commit = strategy_producer_commit
        self.strategy_bindings = _validate_strategy_bindings(
            producer_commit=self.strategy_producer_commit,
            bindings=tuple(strategy_bindings),
        )

    def _deployment_evidence(
        self,
        strategy_lineage: tuple[RecoveryStrategyLineage, ...],
    ) -> dict[str, object]:
        return {
            "deployment_profile_id": self.deployment_profile_id,
            "deployment_profile_generation": self.deployment_profile_generation,
            "strategy_lineage": strategy_lineage,
        }

    def _validate_expectations(
        self,
        expectations: Iterable[RecoveryAuthorityExpectation],
    ) -> tuple[RecoveryAuthorityExpectation, ...]:
        ordered = tuple(sorted(expectations, key=lambda item: item.logical_role))
        roles = tuple(item.logical_role for item in ordered)
        if len(roles) != len(set(roles)):
            raise RuntimeRecoveryCoordinatorError("expectation logical roles must be unique")
        requirement_by_role = {
            requirement.logical_role: requirement
            for requirement in self.inventory_plan.requirements
        }
        if set(roles) != set(requirement_by_role):
            raise RuntimeRecoveryCoordinatorError(
                "expectations must exactly cover the inventory plan"
            )
        protected_paths = (self.manifest_store, self.backup_object_store)
        for expectation in ordered:
            requirement = requirement_by_role[expectation.logical_role]
            if expectation.artifact_role is not requirement.artifact_role:
                raise RuntimeRecoveryCoordinatorError(
                    f"artifact role mismatch for {expectation.logical_role}"
                )
            allowed_root = Path(expectation.allowed_root)
            authority_path = Path(expectation.authority_manifest_path)
            for store in protected_paths:
                if _paths_overlap(store, allowed_root) or _paths_overlap(store, authority_path):
                    raise RuntimeRecoveryCoordinatorError(
                        f"store must be isolated from allowed roots and sources: {store}"
                    )
        return ordered

    @staticmethod
    def _reject_untrusted_verifiers(
        role_verifiers: Mapping[RecoveryArtifactRole, RecoveryRoleVerifier] | None,
    ) -> None:
        if role_verifiers is not None:
            raise RuntimeRecoveryCoordinatorError("untrusted verifier overrides are not accepted")

    def _seal_authorities(
        self,
        *,
        expectations: Iterable[RecoveryAuthorityExpectation],
        as_of: AwareUtcDatetime,
    ) -> tuple[RecoveryAuthorityReceipt, ...]:
        ordered = self._validate_expectations(expectations)
        sealed: list[tuple[RecoveryAuthorityManifest, _FileDigest, Path]] = []
        for expectation in ordered:
            authority, authority_file = _load_authority(Path(expectation.authority_manifest_path))
            if authority.plane is not expectation.plane:
                raise RuntimeRecoveryCoordinatorError(
                    f"plane mismatch for {expectation.logical_role}"
                )
            if (
                authority.logical_role != expectation.logical_role
                or authority.artifact_role is not expectation.artifact_role
            ):
                raise RuntimeRecoveryCoordinatorError(
                    f"authority role mismatch for {expectation.logical_role}"
                )
            if authority.producer_commit != expectation.expected_producer_commit:
                raise RuntimeRecoveryCoordinatorError(
                    f"producer commit mismatch for {expectation.logical_role}"
                )
            if authority.generation_id != expectation.expected_generation_id:
                raise RuntimeRecoveryCoordinatorError(
                    f"generation mismatch for {expectation.logical_role}"
                )
            if authority.available_at > as_of:
                raise RuntimeRecoveryCoordinatorError(
                    f"authority {expectation.logical_role} is not PIT-visible "
                    f"at {as_of.isoformat()}"
                )
            source = Path(authority.artifact_path)
            if not _path_within_root(source, Path(expectation.allowed_root)):
                raise RuntimeRecoveryCoordinatorError(
                    f"artifact path escapes allowed root for {expectation.logical_role}"
                )
            for store in (self.manifest_store, self.backup_object_store):
                if _paths_overlap(store, source):
                    raise RuntimeRecoveryCoordinatorError(
                        f"store must be isolated from allowed roots and sources: {store}"
                    )
            copied = _copy_source_to_object(
                source,
                object_store=self.backup_object_store,
                expected_size=authority.artifact_size_bytes,
                expected_sha256=authority.artifact_sha256,
                logical_role=authority.logical_role,
                source_trust_root=Path(expectation.allowed_root),
            )
            object_path = self.backup_object_store / f"{copied.sha256}.blob"
            sealed.append((authority, authority_file, object_path))

        references = tuple(
            RecoveryArtifactReference(
                logical_role=authority.logical_role,
                artifact_role=authority.artifact_role,
                artifact_path=str(object_path),
                artifact_size_bytes=authority.artifact_size_bytes,
                artifact_sha256=authority.artifact_sha256,
                generation_id=authority.generation_id,
                schema_version=authority.schema_version,
                watermark=authority.watermark,
            )
            for authority, _, object_path in sealed
        )
        receipts: list[RecoveryAuthorityReceipt] = []
        for authority, authority_file, object_path in sealed:
            context = RecoveryArtifactVerificationContext(
                logical_role=authority.logical_role,
                artifact_role=authority.artifact_role,
                artifact_path=str(object_path),
                artifact_size_bytes=authority.artifact_size_bytes,
                artifact_sha256=authority.artifact_sha256,
                generation_id=authority.generation_id,
                schema_version=authority.schema_version,
                trust_root=str(self.backup_object_store),
                as_of=as_of,
                related_artifacts=references,
            )
            verification = _call_verifier(
                context,
                expected=authority.watermark,
            )
            receipts.append(
                RecoveryAuthorityReceipt(
                    plane=authority.plane,
                    logical_role=authority.logical_role,
                    artifact_role=authority.artifact_role,
                    artifact_path=str(object_path),
                    artifact_size_bytes=authority.artifact_size_bytes,
                    artifact_sha256=authority.artifact_sha256,
                    authority_id=str(authority.authority_id),
                    authority_document_sha256=authority_file.sha256,
                    producer_commit=authority.producer_commit,
                    generation_id=authority.generation_id,
                    schema_version=authority.schema_version,
                    available_at=authority.available_at,
                    watermark=verification.watermark,
                    verifier_id=verification.verifier_id,
                    verifier_version=verification.verifier_version,
                    verifier_fingerprint=verification.verifier_fingerprint,
                    fixed_replay_verified=verification.fixed_replay_verified,
                    strategy_replays=verification.strategy_replays,
                )
            )
        return tuple(receipts)

    def _build_manifest(
        self,
        *,
        receipts: tuple[RecoveryAuthorityReceipt, ...],
        as_of: AwareUtcDatetime,
    ) -> RuntimeRecoveryManifest:
        requirement_by_role = {item.logical_role: item for item in self.inventory_plan.requirements}
        entries = tuple(
            RecoveryArtifactEntry(
                logical_role=receipt.logical_role,
                artifact_role=receipt.artifact_role,
                absolute_path=receipt.artifact_path,
                restore_path=requirement_by_role[receipt.logical_role].restore_path,
                size_bytes=receipt.artifact_size_bytes,
                sha256=receipt.artifact_sha256,
                generation_id=receipt.generation_id,
                schema_version=receipt.schema_version,
                watermark=receipt.watermark,
            )
            for receipt in receipts
        )
        nested = RecoveryManifest(
            inventory_plan=self.inventory_plan,
            captured_at=as_of,
            entries=entries,
        )
        fixed_replays = tuple(
            replay
            for receipt in receipts
            if receipt.artifact_role is RecoveryArtifactRole.ARTIFACT_METADATA
            for replay in receipt.strategy_replays
        )
        strategy_lineage = _build_strategy_lineage(
            bindings=self.strategy_bindings,
            fixed_replays=fixed_replays,
        )
        return RuntimeRecoveryManifest(
            inventory_plan=self.inventory_plan,
            recovery_manifest=nested,
            deployment_topology_id=self.deployment_topology_id,
            **self._deployment_evidence(strategy_lineage),
            as_of=as_of,
            authorities=receipts,
        )

    def run_once(
        self,
        *,
        expectations: Iterable[RecoveryAuthorityExpectation],
        as_of: AwareUtcDatetime,
        role_verifiers: Mapping[RecoveryArtifactRole, RecoveryRoleVerifier] | None = None,
    ) -> RuntimeRecoveryManifest:
        self._reject_untrusted_verifiers(role_verifiers)
        with _DIRECTORY_MUTATION_LOCK:
            receipts = self._seal_authorities(
                expectations=expectations,
                as_of=as_of,
            )
            manifest = self._build_manifest(receipts=receipts, as_of=as_of)
            if manifest.manifest_id is None:
                raise RuntimeRecoveryCoordinatorError("runtime recovery manifest has no identity")
            _append_immutable(
                self.manifest_store,
                f"{manifest.manifest_id}.json",
                _serialized(manifest),
            )
            return manifest

    def _load_runtime_manifest(
        self,
        *,
        manifest_id: str,
        expected_as_of: AwareUtcDatetime,
    ) -> RuntimeRecoveryManifest:
        if len(manifest_id) != 64 or any(
            character not in "0123456789abcdef" for character in manifest_id
        ):
            raise RuntimeRecoveryCoordinatorError("invalid runtime recovery manifest id")
        stored, _ = _read_document(
            self.manifest_store / f"{manifest_id}.json",
            label="runtime recovery manifest",
        )
        try:
            manifest = RuntimeRecoveryManifest.model_validate_json(stored)
        except ValueError as exc:
            raise RuntimeRecoveryCoordinatorError(
                f"invalid runtime recovery manifest: {exc}"
            ) from exc
        if stored != _serialized(manifest):
            raise RuntimeRecoveryCoordinatorError("runtime recovery manifest is not canonical")
        if manifest.manifest_id != manifest_id:
            raise RuntimeRecoveryCoordinatorError("runtime recovery manifest id/path mismatch")
        if manifest.inventory_plan != self.inventory_plan:
            raise RuntimeRecoveryCoordinatorError("recovery inventory plan mismatch")
        if manifest.deployment_topology_id != self.deployment_topology_id:
            raise RuntimeRecoveryCoordinatorError("deployment topology mismatch")
        if (
            manifest.deployment_profile_id != self.deployment_profile_id
            or manifest.deployment_profile_generation != self.deployment_profile_generation
        ):
            raise RuntimeRecoveryCoordinatorError("deployment profile mismatch")
        binding_by_strategy = {item.strategy_id: item for item in self.strategy_bindings}
        for lineage in manifest.strategy_lineage:
            binding = binding_by_strategy[lineage.strategy_id]
            if (
                lineage.registration_fingerprint != binding.registration_fingerprint
                or lineage.strategy_spec_fingerprint != binding.strategy_spec_fingerprint
                or lineage.candidate_schema_fingerprint != binding.candidate_schema_fingerprint
                or lineage.executable_fingerprint != binding.executable_fingerprint
                or lineage.fixed_replay_executable_fingerprint
                != _strategy_fixed_replay_executable_sha256(lineage.strategy_id)
            ):
                raise RuntimeRecoveryCoordinatorError(
                    f"strategy lineage mismatch for {lineage.strategy_id}"
                )
        if manifest.as_of != expected_as_of:
            raise RuntimeRecoveryCoordinatorError("recovery as_of mismatch")
        return manifest

    def _verify_manifest_objects(
        self,
        *,
        manifest: RuntimeRecoveryManifest,
    ) -> tuple[RecoveryRoleWatermark, ...]:
        references = tuple(
            RecoveryArtifactReference(
                logical_role=item.logical_role,
                artifact_role=item.artifact_role,
                artifact_path=item.artifact_path,
                artifact_size_bytes=item.artifact_size_bytes,
                artifact_sha256=item.artifact_sha256,
                generation_id=item.generation_id,
                schema_version=item.schema_version,
                watermark=item.watermark,
            )
            for item in manifest.authorities
        )
        watermarks: list[RecoveryRoleWatermark] = []
        for receipt in manifest.authorities:
            expected_path = self.backup_object_store / f"{receipt.artifact_sha256}.blob"
            if Path(receipt.artifact_path) != expected_path:
                raise RuntimeRecoveryCoordinatorError(
                    f"backup object path mismatch for {receipt.logical_role}"
                )
            inspected = _inspect_regular_file(
                expected_path,
                label=f"backup object {receipt.logical_role}",
            )
            if (
                inspected.size_bytes != receipt.artifact_size_bytes
                or inspected.sha256 != receipt.artifact_sha256
            ):
                raise RuntimeRecoveryCoordinatorError(
                    f"backup object hash/size mismatch for {receipt.logical_role}"
                )
            context = RecoveryArtifactVerificationContext(
                logical_role=receipt.logical_role,
                artifact_role=receipt.artifact_role,
                artifact_path=receipt.artifact_path,
                artifact_size_bytes=receipt.artifact_size_bytes,
                artifact_sha256=receipt.artifact_sha256,
                generation_id=receipt.generation_id,
                schema_version=receipt.schema_version,
                trust_root=str(self.backup_object_store),
                as_of=manifest.as_of,
                related_artifacts=references,
            )
            observed = _call_verifier(
                context,
                expected=receipt.watermark,
            )
            if (
                observed.verifier_id != receipt.verifier_id
                or observed.verifier_version != receipt.verifier_version
                or observed.verifier_fingerprint != receipt.verifier_fingerprint
                or observed.fixed_replay_verified != receipt.fixed_replay_verified
                or observed.strategy_replays != receipt.strategy_replays
            ):
                raise RuntimeRecoveryCoordinatorError(
                    f"trusted verifier lineage mismatch for {receipt.logical_role}"
                )
            watermarks.append(
                RecoveryRoleWatermark(
                    logical_role=receipt.logical_role,
                    artifact_role=receipt.artifact_role,
                    watermark=observed.watermark,
                    verifier_fingerprint=observed.verifier_fingerprint,
                    fixed_replay_verified=observed.fixed_replay_verified,
                )
            )
        fixed_replays = tuple(
            replay
            for receipt in manifest.authorities
            if receipt.artifact_role is RecoveryArtifactRole.ARTIFACT_METADATA
            for replay in receipt.strategy_replays
        )
        if (
            _build_strategy_lineage(
                bindings=self.strategy_bindings,
                fixed_replays=fixed_replays,
            )
            != manifest.strategy_lineage
        ):
            raise RuntimeRecoveryCoordinatorError(
                "runtime recovery strategy lineage differs from verified replays"
            )
        return tuple(watermarks)

    def restore_preflight(
        self,
        *,
        manifest_id: str,
        expected_as_of: AwareUtcDatetime,
        role_verifiers: Mapping[RecoveryArtifactRole, RecoveryRoleVerifier] | None = None,
    ) -> RecoveryRestorePreflightResult:
        self._reject_untrusted_verifiers(role_verifiers)
        manifest = self._load_runtime_manifest(
            manifest_id=manifest_id,
            expected_as_of=expected_as_of,
        )
        verified = self._verify_manifest_objects(manifest=manifest)
        fixed_replay_verified = any(
            item.artifact_role is RecoveryArtifactRole.ARTIFACT_METADATA
            and item.fixed_replay_verified
            for item in verified
        )
        if not fixed_replay_verified:
            raise RuntimeRecoveryCoordinatorError("typed fixed replay evidence was not verified")
        return RecoveryRestorePreflightResult(
            passed=True,
            manifest_id=manifest_id,
            deployment_topology_id=manifest.deployment_topology_id,
            **self._deployment_evidence(manifest.strategy_lineage),
            as_of=manifest.as_of,
            verified_roles=tuple(item.logical_role for item in manifest.authorities),
            checks=(
                "immutable_manifest_identity",
                "backup_object_hash_and_size",
                "producer_commit_generation_schema",
                "typed_role_watermarks",
                "typed_fixed_replay",
                "pit_visibility",
                "physical_path_isolation",
            ),
            fixed_replay_verified=True,
        )

    def rehearse_restore(
        self,
        *,
        manifest_id: str,
        target_root: Path,
        expected_as_of: AwareUtcDatetime,
        rto_target_seconds: float,
        rpo_target_watermarks: Mapping[str, RecoveryWatermarkSummary] | None = None,
        role_verifiers: Mapping[RecoveryArtifactRole, RecoveryRoleVerifier] | None = None,
        fault_injector: Callable[[RecoveryFaultPoint], None] | None = None,
    ) -> RecoveryRehearsalReport:
        self._reject_untrusted_verifiers(role_verifiers)
        if rto_target_seconds <= 0:
            raise ValueError("rto_target_seconds must be positive")
        target = _canonical_absolute_path(target_root, label="restore target_root")
        for store in (self.manifest_store, self.backup_object_store):
            if _paths_overlap(target, store):
                raise RuntimeRecoveryCoordinatorError(
                    "restore target must be isolated from recovery stores"
                )
        target_lease = _open_physical_directory(
            target,
            create=False,
            label="restore target",
        )
        operation_id = uuid.uuid4().hex
        candidate_name = operation_id
        candidate_path = target / ".candidates" / candidate_name
        candidate_created = False
        candidate_lease: _DirectoryLease | None = None
        generation_id: str | None = None
        generation_staged = False
        pointer_switched = False
        transaction_prepared = False
        intent: _RecoveryPublishIntent | None = None
        manifest: RuntimeRecoveryManifest | None = None
        previous_current_pointer: RecoveryCurrentPointer | None = None
        previous_current_content: bytes | None = None
        failure_point = "initialization"
        started_at = datetime.now(UTC)
        started_monotonic = time.monotonic()
        try:
            existing = set(os.listdir(target_lease.descriptor))
            allowed = {
                ".candidates",
                ".failed",
                "audits",
                "current.json",
                "generations",
                "reports",
                "transactions",
            }
            if existing - allowed:
                raise RuntimeRecoveryCoordinatorError(
                    "restore target_root must be empty or contain only managed recovery entries"
                )
            _verify_directory_binding(target_lease, label="restore target")
            _ensure_managed_restore_layout(target_lease)
            _recover_interrupted_publish(target=target, target_lease=target_lease)
            previous_current_pointer, previous_current_content = _read_current_pointer(target)
            previous_current_sha256 = (
                hashlib.sha256(previous_current_content).hexdigest()
                if previous_current_content is not None
                else None
            )
            manifest = self._load_runtime_manifest(
                manifest_id=manifest_id,
                expected_as_of=expected_as_of,
            )
            self._verify_manifest_objects(manifest=manifest)
            generation_id = str(manifest.manifest_id)
            generation_path = target / "generations" / generation_id
            _verify_directory_binding(target_lease, label="restore target")
            recovery_audit = RecoveryRehearsalAudit(
                operation_id=operation_id,
                status="failed",
                manifest_id=manifest_id,
                **self._deployment_evidence(manifest.strategy_lineage),
                previous_current_sha256=previous_current_sha256,
                generation_id=generation_id,
                failure_point="interrupted_candidate_prepared",
                failure_type="InterruptedRecoveryStage",
                failure_message="restore transaction interrupted before candidate copy completed",
                started_at=started_at,
                finished_at=datetime.now(UTC),
            )
            intent = _RecoveryPublishIntent(
                operation_id=operation_id,
                owner_id=operation_id,
                candidate_name=candidate_name,
                manifest_id=manifest_id,
                **self._deployment_evidence(manifest.strategy_lineage),
                generation_id=generation_id,
                stage="candidate_prepared",
                generation_created=False,
                previous_current=previous_current_pointer,
                recovery_audit=recovery_audit,
            )
            _write_publish_intent(target, intent)
            transaction_prepared = True

            if generation_path.exists():
                candidate_lease = _open_physical_directory(
                    generation_path,
                    create=False,
                    label="existing restore generation",
                )
                candidate_path = generation_path
            else:
                candidate_lease = _open_relative_directories(
                    target_lease,
                    (".candidates", candidate_name),
                )
                candidate_created = True
                self._restore_entries(manifest=manifest, target_lease=candidate_lease)
            intent = _advance_publish_intent(
                target,
                intent,
                stage="copied",
            )
            failure_point = RecoveryFaultPoint.AFTER_COPY.value
            if fault_injector is not None:
                fault_injector(RecoveryFaultPoint.AFTER_COPY)
            restored_watermarks = self._verify_restored_entries(
                manifest=manifest,
                target_root=candidate_path,
                target_lease=candidate_lease,
            )
            _verify_manifest_outputs(manifest=manifest, target_lease=candidate_lease)
            intent = _advance_publish_intent(
                target,
                intent,
                stage="verified",
            )
            failure_point = RecoveryFaultPoint.AFTER_HASH_VERIFY.value
            if fault_injector is not None:
                fault_injector(RecoveryFaultPoint.AFTER_HASH_VERIFY)
            candidate_lease.close()
            candidate_lease = None

            rpo_met, rpo_loss, rpo_missing_target_roles = _assess_rpo(
                restored_watermarks,
                rpo_target_watermarks,
            )
            same_current_identity = (
                previous_current_pointer is not None
                and previous_current_pointer.manifest_id == manifest_id
                and previous_current_pointer.generation_id == generation_id
                and previous_current_content is not None
            )
            if same_current_identity:
                original_report = _load_rehearsal_report(
                    target,
                    previous_current_pointer.report_sha256,
                )
                same_acceptance_contract = (
                    original_report.passed
                    and original_report.manifest_id == manifest_id
                    and original_report.deployment_topology_id == manifest.deployment_topology_id
                    and original_report.deployment_profile_id == manifest.deployment_profile_id
                    and original_report.deployment_profile_generation
                    == manifest.deployment_profile_generation
                    and original_report.strategy_lineage == manifest.strategy_lineage
                    and original_report.rto_target_seconds == rto_target_seconds
                    and original_report.rpo_met == rpo_met
                    and original_report.rpo_loss == rpo_loss
                    and original_report.rpo_missing_target_roles == rpo_missing_target_roles
                    and original_report.rpo_as_of == manifest.as_of
                    and original_report.recovered_watermarks == restored_watermarks
                )
                if same_acceptance_contract:
                    _require_original_success_audit(
                        target,
                        pointer=previous_current_pointer,
                        current_content=previous_current_content,
                    )
                    _remove_publish_intent(target)
                    transaction_prepared = False
                    return original_report
            duration = time.monotonic() - started_monotonic
            finished_at = datetime.now(UTC)
            rto_met = duration <= rto_target_seconds
            report = RecoveryRehearsalReport(
                passed=rpo_met and rto_met,
                manifest_id=manifest_id,
                deployment_topology_id=manifest.deployment_topology_id,
                **self._deployment_evidence(manifest.strategy_lineage),
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration,
                rto_target_seconds=rto_target_seconds,
                rto_met=rto_met,
                rpo_met=rpo_met,
                rpo_loss=rpo_loss,
                rpo_missing_target_roles=rpo_missing_target_roles,
                rpo_as_of=manifest.as_of,
                recovered_watermarks=restored_watermarks,
            )
            report_sha256, _ = _append_rehearsal_report(target, report)
            if not report.passed:
                failure_point = "rpo_rto_gate"
                if candidate_created:
                    _rename_managed_directory(
                        target=target,
                        source_parent=".candidates",
                        source_name=candidate_name,
                        destination_parent=".failed",
                        destination_name=operation_id,
                    )
                    candidate_created = False
                failure_audit = RecoveryRehearsalAudit(
                    operation_id=operation_id,
                    status="failed",
                    manifest_id=manifest_id,
                    **self._deployment_evidence(manifest.strategy_lineage),
                    previous_current_sha256=previous_current_sha256,
                    generation_id=generation_id,
                    report_sha256=report_sha256,
                    failure_point=failure_point,
                    failure_type="RecoveryAcceptanceGate",
                    failure_message="RPO or RTO acceptance gate did not pass",
                    started_at=started_at,
                    finished_at=finished_at,
                )
                _append_rehearsal_audit(target, failure_audit)
                _remove_publish_intent(target)
                transaction_prepared = False
                return report

            if candidate_created:
                intent = _advance_publish_intent(
                    target,
                    intent,
                    stage="generation_staging",
                    generation_created=True,
                )
                _rename_managed_directory(
                    target=target,
                    source_parent=".candidates",
                    source_name=candidate_name,
                    destination_parent="generations",
                    destination_name=generation_id,
                )
                candidate_created = False
                generation_staged = True
            intent = _advance_publish_intent(
                target,
                intent,
                stage="generation_staged",
                generation_created=generation_staged,
            )
            failure_point = RecoveryFaultPoint.AFTER_GENERATION_STAGE.value
            if fault_injector is not None:
                fault_injector(RecoveryFaultPoint.AFTER_GENERATION_STAGE)
            _verify_directory_binding(target_lease, label="restore target")
            pointer = RecoveryCurrentPointer(
                manifest_id=manifest_id,
                **self._deployment_evidence(manifest.strategy_lineage),
                generation_id=generation_id,
                generation_path=f"generations/{generation_id}",
                report_sha256=report_sha256,
            )
            pointer_content = _serialized(pointer)
            audit = RecoveryRehearsalAudit(
                operation_id=operation_id,
                status="passed",
                manifest_id=manifest_id,
                **self._deployment_evidence(manifest.strategy_lineage),
                previous_current_sha256=previous_current_sha256,
                published_current_sha256=hashlib.sha256(pointer_content).hexdigest(),
                generation_id=generation_id,
                report_sha256=report_sha256,
                started_at=started_at,
                finished_at=finished_at,
            )
            recovery_audit = RecoveryRehearsalAudit(
                operation_id=operation_id,
                status="failed",
                manifest_id=manifest_id,
                **self._deployment_evidence(manifest.strategy_lineage),
                previous_current_sha256=previous_current_sha256,
                generation_id=generation_id,
                report_sha256=report_sha256,
                failure_point="interrupted_current_publish_recovered",
                failure_type="InterruptedRecoveryPublish",
                failure_message=(
                    "current publication was interrupted before its success audit became durable"
                ),
                started_at=started_at,
                finished_at=finished_at,
            )
            intent = _advance_publish_intent(
                target,
                intent,
                stage="publish_prepared",
                generation_created=generation_staged,
                proposed_current=pointer,
                success_audit=audit,
                recovery_audit=recovery_audit,
            )
            failure_point = RecoveryFaultPoint.BEFORE_ATOMIC_PUBLISH.value
            if fault_injector is not None:
                fault_injector(RecoveryFaultPoint.BEFORE_ATOMIC_PUBLISH)
            _atomic_replace_current(target_lease, pointer_content)
            pointer_switched = True
            published_pointer, published_content = _read_current_pointer(target)
            if published_pointer != pointer or published_content != pointer_content:
                raise RuntimeRecoveryCoordinatorError(
                    "published current pointer differs from verified candidate"
                )
            intent = _advance_publish_intent(
                target,
                intent,
                stage="current_switched",
                generation_created=generation_staged,
                proposed_current=pointer,
                success_audit=audit,
                recovery_audit=recovery_audit,
            )
            failure_point = RecoveryFaultPoint.AFTER_CURRENT_SWITCH.value
            if fault_injector is not None:
                fault_injector(RecoveryFaultPoint.AFTER_CURRENT_SWITCH)
            _append_rehearsal_audit(target, audit)
            _remove_publish_intent(target)
            transaction_prepared = False
            return report
        except BaseException as exc:
            if candidate_lease is not None:
                candidate_lease.close()
                candidate_lease = None
            active_intent = _read_publish_intent(target)
            if transaction_prepared or active_intent is not None:
                _recover_interrupted_publish(target=target, target_lease=target_lease)
                transaction_prepared = False
                pointer_switched = False
                generation_staged = False
            if pointer_switched:
                if previous_current_content is None:
                    _remove_current_pointer(target_lease)
                else:
                    _atomic_replace_current(target_lease, previous_current_content)
            if candidate_created and candidate_path.exists():
                try:
                    _rename_managed_directory(
                        target=target,
                        source_parent=".candidates",
                        source_name=candidate_name,
                        destination_parent=".failed",
                        destination_name=operation_id,
                    )
                    candidate_created = False
                except Exception as quarantine_exc:
                    raise RuntimeRecoveryCoordinatorError(
                        f"restore rehearsal failed at {failure_point}; "
                        f"candidate quarantine failed: {quarantine_exc}"
                    ) from exc
            if (
                generation_staged
                and generation_id is not None
                and (target / "generations" / generation_id).exists()
            ):
                try:
                    _rename_managed_directory(
                        target=target,
                        source_parent="generations",
                        source_name=generation_id,
                        destination_parent=".failed",
                        destination_name=operation_id,
                    )
                    generation_staged = False
                except Exception as quarantine_exc:
                    raise RuntimeRecoveryCoordinatorError(
                        f"restore rehearsal failed at {failure_point}; "
                        f"staged generation quarantine failed: {quarantine_exc}"
                    ) from exc
            if manifest is None:
                if not isinstance(exc, Exception):
                    raise
                raise RuntimeRecoveryCoordinatorError(
                    f"restore rehearsal failed at {failure_point}: {exc}"
                ) from exc
            finished_at = datetime.now(UTC)
            failure_audit = RecoveryRehearsalAudit(
                operation_id=operation_id,
                status="failed",
                manifest_id=manifest_id,
                **self._deployment_evidence(manifest.strategy_lineage),
                previous_current_sha256=(
                    hashlib.sha256(previous_current_content).hexdigest()
                    if previous_current_content is not None
                    else None
                ),
                generation_id=generation_id,
                failure_point=failure_point,
                failure_type=type(exc).__name__,
                failure_message=str(exc),
                started_at=started_at,
                finished_at=finished_at,
            )
            try:
                _append_rehearsal_audit(target, failure_audit)
            except Exception as audit_exc:
                raise RuntimeRecoveryCoordinatorError(
                    f"restore rehearsal failed at {failure_point}; "
                    f"failure audit could not be persisted: {audit_exc}"
                ) from exc
            if not isinstance(exc, Exception):
                raise
            raise RuntimeRecoveryCoordinatorError(
                f"restore rehearsal failed at {failure_point}: {exc}"
            ) from exc
        finally:
            target_lease.close()

    def _restore_entries(
        self,
        *,
        manifest: RuntimeRecoveryManifest,
        target_lease: _DirectoryLease,
    ) -> None:
        for entry in manifest.recovery_manifest.entries:
            _verify_directory_binding(target_lease, label="restore target")
            relative = PurePosixPath(entry.restore_path)
            parent_lease = _open_relative_directories(
                target_lease,
                relative.parts[:-1],
            )
            source_path = Path(entry.absolute_path)
            source_descriptor = -1
            source_parent_lease: _DirectoryLease | None = None
            destination_descriptor = -1
            try:
                source_descriptor, source_parent_lease = _open_regular_readonly(
                    source_path,
                    label=f"backup object {entry.logical_role}",
                )
                source_before = os.fstat(source_descriptor)
                parent_before = os.fstat(parent_lease.descriptor)
                retained_root_before = (
                    os.fstat(target_lease.descriptor) if len(relative.parts) == 1 else None
                )
                destination_descriptor = os.open(
                    relative.name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=parent_lease.descriptor,
                )
                if retained_root_before is not None:
                    target_lease.record_owned_entry_creation(
                        before=retained_root_before,
                        after=os.fstat(target_lease.descriptor),
                        label="restore target",
                    )
                parent_lease.record_owned_entry_creation(
                    before=parent_before,
                    after=os.fstat(parent_lease.descriptor),
                    label="restore artifact parent",
                )
                destination_before = os.fstat(destination_descriptor)
                _verify_regular_binding(
                    name=relative.name,
                    descriptor=destination_descriptor,
                    parent_lease=parent_lease,
                    label=f"restored artifact {entry.logical_role}",
                    expected=_file_binding(destination_before),
                )
                digest = hashlib.sha256()
                size = 0
                while chunk := os.read(source_descriptor, _CHUNK_SIZE):
                    digest.update(chunk)
                    size += len(chunk)
                    view = memoryview(chunk)
                    while view:
                        written = os.write(destination_descriptor, view)
                        view = view[written:]
                os.fsync(destination_descriptor)
                source_after = _verify_regular_binding(
                    name=source_path.name,
                    descriptor=source_descriptor,
                    parent_lease=source_parent_lease,
                    label=f"backup object {entry.logical_role}",
                    expected=_file_binding(source_before),
                )
                if _identity(source_before) != _identity(source_after):
                    raise RuntimeRecoveryCoordinatorError(
                        f"backup object {entry.logical_role} changed while copying"
                    )
                destination_after = _verify_regular_binding(
                    name=relative.name,
                    descriptor=destination_descriptor,
                    parent_lease=parent_lease,
                    label=f"restored artifact {entry.logical_role}",
                )
                if (
                    _file_object_binding(destination_before)
                    != _file_object_binding(destination_after)
                    or destination_after.st_size != size
                ):
                    raise RuntimeRecoveryCoordinatorError(
                        f"restored artifact {entry.logical_role} changed while copying"
                    )
                if size != entry.size_bytes or digest.hexdigest() != entry.sha256:
                    raise RuntimeRecoveryCoordinatorError(
                        f"restore source hash/size mismatch for {entry.logical_role}"
                    )
                _fsync_directory(parent_lease.descriptor)
                _verify_directory_binding(parent_lease, label="restore artifact parent")
            finally:
                if destination_descriptor >= 0:
                    os.close(destination_descriptor)
                if source_descriptor >= 0:
                    os.close(source_descriptor)
                if source_parent_lease is not None:
                    source_parent_lease.close()
                parent_lease.close()
        _verify_directory_binding(target_lease, label="restore target")

    def _verify_restored_entries(
        self,
        *,
        manifest: RuntimeRecoveryManifest,
        target_root: Path,
        target_lease: _DirectoryLease,
    ) -> tuple[RecoveryRoleWatermark, ...]:
        entry_by_role = {entry.logical_role: entry for entry in manifest.recovery_manifest.entries}
        references = tuple(
            RecoveryArtifactReference(
                logical_role=receipt.logical_role,
                artifact_role=receipt.artifact_role,
                artifact_path=str(target_root / entry_by_role[receipt.logical_role].restore_path),
                artifact_size_bytes=receipt.artifact_size_bytes,
                artifact_sha256=receipt.artifact_sha256,
                generation_id=receipt.generation_id,
                schema_version=receipt.schema_version,
                watermark=receipt.watermark,
            )
            for receipt in manifest.authorities
        )
        watermarks: list[RecoveryRoleWatermark] = []
        for receipt in manifest.authorities:
            _verify_directory_binding(target_lease, label="restore target")
            reference = next(
                item for item in references if item.logical_role == receipt.logical_role
            )
            context = RecoveryArtifactVerificationContext(
                logical_role=reference.logical_role,
                artifact_role=reference.artifact_role,
                artifact_path=reference.artifact_path,
                artifact_size_bytes=reference.artifact_size_bytes,
                artifact_sha256=reference.artifact_sha256,
                generation_id=reference.generation_id,
                schema_version=reference.schema_version,
                trust_root=str(target_root),
                as_of=manifest.as_of,
                related_artifacts=references,
            )
            observed = _call_verifier(
                context,
                expected=receipt.watermark,
            )
            if observed.verifier_fingerprint != receipt.verifier_fingerprint:
                raise RuntimeRecoveryCoordinatorError(
                    f"trusted verifier lineage mismatch for {receipt.logical_role}"
                )
            watermarks.append(
                RecoveryRoleWatermark(
                    logical_role=receipt.logical_role,
                    artifact_role=receipt.artifact_role,
                    watermark=observed.watermark,
                    verifier_fingerprint=observed.verifier_fingerprint,
                    fixed_replay_verified=observed.fixed_replay_verified,
                )
            )
            _verify_directory_binding(target_lease, label="restore target")
        return tuple(watermarks)


def _open_relative_directories(
    root_lease: _DirectoryLease,
    parts: tuple[str, ...],
) -> _DirectoryLease:
    descriptors = [os.dup(root_lease.descriptor)]
    names: list[str] = []
    baselines = [_directory_binding(os.fstat(descriptors[0]))]
    expected_nlinks = [baselines[0][6]]
    try:
        for part in parts:
            if part in {"", ".", ".."} or "/" in part or "\\" in part:
                raise RuntimeRecoveryCoordinatorError("unsafe restore path")
            parent = descriptors[-1]
            parent_before = os.fstat(parent)
            retained_root_before = (
                os.fstat(root_lease.descriptor) if len(descriptors) == 1 else None
            )
            missing = False
            try:
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=parent)
            except FileNotFoundError:
                missing = True
                try:
                    with suppress(FileExistsError):
                        os.mkdir(part, mode=0o700, dir_fd=parent)
                    child = os.open(part, _DIRECTORY_FLAGS, dir_fd=parent)
                except OSError as exc:
                    raise RuntimeRecoveryCoordinatorError(
                        "cannot open physical restore artifact parent"
                    ) from exc
            except OSError as exc:
                raise RuntimeRecoveryCoordinatorError(
                    "cannot open physical restore artifact parent"
                ) from exc
            if missing:
                if retained_root_before is not None:
                    root_lease.record_owned_entry_creation(
                        before=retained_root_before,
                        after=os.fstat(root_lease.descriptor),
                        label="restore target",
                    )
                _record_owned_directory_entry(
                    baselines=baselines,
                    expected_nlinks=expected_nlinks,
                    index=len(descriptors) - 1,
                    before=parent_before,
                    after=os.fstat(parent),
                    allowed_nlink_deltas={0, 1},
                    label="restore artifact parent",
                )
            expected_parent = _binding_with_nlink(baselines[-1], expected_nlinks[-1])
            if _directory_binding(os.fstat(parent)) != expected_parent:
                os.close(child)
                raise RuntimeRecoveryCoordinatorError(
                    "restore artifact parent directory binding changed"
                )
            child_baseline = _directory_binding(os.fstat(child))
            descriptors.append(child)
            names.append(part)
            baselines.append(child_baseline)
            expected_nlinks.append(child_baseline[6])
        lease = _DirectoryLease(
            path=None,
            descriptors=descriptors,
            names=names,
            baselines=baselines,
            expected_nlinks=expected_nlinks,
        )
        _verify_directory_binding(lease, label="restore artifact parent")
        return lease
    except Exception:
        while descriptors:
            os.close(descriptors.pop())
        raise


def _verify_manifest_outputs(
    *,
    manifest: RuntimeRecoveryManifest,
    target_lease: _DirectoryLease,
) -> None:
    expected_entries = {
        tuple(PurePosixPath(entry.restore_path).parts): entry
        for entry in manifest.recovery_manifest.entries
    }
    expected_files = set(expected_entries)
    expected_directories = {
        parts[:depth] for parts in expected_files for depth in range(1, len(parts))
    }
    observed_files: set[tuple[str, ...]] = set()
    observed_directories: set[tuple[str, ...]] = set()

    def walk(directory_descriptor: int, prefix: tuple[str, ...]) -> None:
        try:
            names = tuple(os.listdir(directory_descriptor))
        except OSError as exc:
            raise RuntimeRecoveryCoordinatorError(
                "restore target entries differ from manifest outputs"
            ) from exc
        for name in names:
            relative = (*prefix, name)
            try:
                observed = os.stat(
                    name,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise RuntimeRecoveryCoordinatorError(
                    "restore target entries differ from manifest outputs"
                ) from exc
            if stat.S_ISDIR(observed.st_mode):
                if relative not in expected_directories:
                    raise RuntimeRecoveryCoordinatorError(
                        "restore target entries differ from manifest outputs"
                    )
                observed_directories.add(relative)
                try:
                    child = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_descriptor)
                except OSError as exc:
                    raise RuntimeRecoveryCoordinatorError(
                        "restore target entries differ from manifest outputs"
                    ) from exc
                try:
                    if _directory_binding(observed) != _directory_binding(os.fstat(child)):
                        raise RuntimeRecoveryCoordinatorError(
                            "restore target entries differ from manifest outputs"
                        )
                    walk(child, relative)
                    try:
                        current = os.stat(
                            name,
                            dir_fd=directory_descriptor,
                            follow_symlinks=False,
                        )
                    except OSError as exc:
                        raise RuntimeRecoveryCoordinatorError(
                            "restore target entries differ from manifest outputs"
                        ) from exc
                    if _directory_binding(current) != _directory_binding(os.fstat(child)):
                        raise RuntimeRecoveryCoordinatorError(
                            "restore target entries differ from manifest outputs"
                        )
                finally:
                    os.close(child)
            elif stat.S_ISREG(observed.st_mode):
                if relative not in expected_files or observed.st_nlink != 1:
                    raise RuntimeRecoveryCoordinatorError(
                        "restore target entries differ from manifest outputs"
                    )
                descriptor = -1
                try:
                    descriptor = os.open(
                        name,
                        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=directory_descriptor,
                    )
                    opened = os.fstat(descriptor)
                    if _file_binding(observed) != _file_binding(opened):
                        raise RuntimeRecoveryCoordinatorError(
                            "restore target entries differ from manifest outputs"
                        )
                    inspected = _stream_digest_descriptor(
                        descriptor,
                        label="restored manifest output",
                    )
                    current = os.stat(
                        name,
                        dir_fd=directory_descriptor,
                        follow_symlinks=False,
                    )
                    after = os.fstat(descriptor)
                    if _identity(current) != _identity(after) or _identity(opened) != _identity(
                        after
                    ):
                        raise RuntimeRecoveryCoordinatorError(
                            "restore target entries differ from manifest outputs"
                        )
                except OSError as exc:
                    raise RuntimeRecoveryCoordinatorError(
                        "restore target entries differ from manifest outputs"
                    ) from exc
                finally:
                    if descriptor >= 0:
                        os.close(descriptor)
                expected = expected_entries[relative]
                if (
                    inspected.size_bytes != expected.size_bytes
                    or inspected.sha256 != expected.sha256
                ):
                    raise RuntimeRecoveryCoordinatorError(
                        "restore target entries differ from manifest outputs"
                    )
                observed_files.add(relative)
            else:
                raise RuntimeRecoveryCoordinatorError(
                    "restore target entries differ from manifest outputs"
                )
        try:
            final_names = tuple(os.listdir(directory_descriptor))
        except OSError as exc:
            raise RuntimeRecoveryCoordinatorError(
                "restore target entries differ from manifest outputs"
            ) from exc
        if set(final_names) != set(names):
            raise RuntimeRecoveryCoordinatorError(
                "restore target entries differ from manifest outputs"
            )

    _verify_directory_binding(target_lease, label="restore target")
    walk(target_lease.descriptor, ())
    _verify_directory_binding(target_lease, label="restore target")
    if observed_files != expected_files or observed_directories != expected_directories:
        raise RuntimeRecoveryCoordinatorError("restore target entries differ from manifest outputs")


__all__ = [
    "RecoveryArtifactReference",
    "RecoveryArtifactVerificationContext",
    "RecoveryAuthorityExpectation",
    "RecoveryAuthorityManifest",
    "RecoveryAuthorityReceipt",
    "RecoveryPlane",
    "RecoveryRehearsalReport",
    "RecoveryRestorePreflightResult",
    "RecoveryRoleRpoLoss",
    "RecoveryRoleVerifier",
    "RecoveryRoleWatermark",
    "RuntimeRecoveryCoordinator",
    "RuntimeRecoveryCoordinatorError",
    "RuntimeRecoveryManifest",
    "append_recovery_authority_manifest",
    "build_recovery_authority_manifest",
]
