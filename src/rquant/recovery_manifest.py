"""Content-addressed recovery inventory and isolated restore rehearsals."""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import uuid
from collections.abc import Callable, Iterable
from datetime import date
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Annotated, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class RecoveryManifestError(RuntimeError):
    """Recovery inventory or immutable storage validation failed."""


class RecoveryArtifactRole(StrEnum):
    PRODUCTION_DUCKDB = "production_duckdb"
    SQLITE_STATE = "sqlite_state"
    RESEARCH_CATALOG = "research_catalog"
    LAKE_MANIFEST = "lake_manifest"
    ARTIFACT_METADATA = "artifact_metadata"
    SERVING_CURRENT = "serving_current"
    SERVING_MANIFEST = "serving_manifest"


REQUIRED_ARTIFACT_ROLES = frozenset(RecoveryArtifactRole)


class RecoveryFaultPoint(StrEnum):
    AFTER_COPY = "after_copy"
    AFTER_HASH_VERIFY = "after_hash_verify"
    BEFORE_ATOMIC_PUBLISH = "before_atomic_publish"
    AFTER_GENERATION_STAGE = "after_generation_stage"
    AFTER_CURRENT_SWITCH = "after_current_switch"


class RecoveryRehearsalStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"


def _validate_safe_relative_path(value: str) -> str:
    if "\\" in value:
        raise ValueError("restore_path must be a safe relative POSIX path")
    path = PurePosixPath(value)
    if not value or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("restore_path must be a safe relative POSIX path")
    if str(path) != value:
        raise ValueError("restore_path must be a safe relative POSIX path")
    return value


def _validate_absolute_path(value: str) -> str:
    if not Path(value).is_absolute():
        raise ValueError("absolute_path must be absolute")
    return value


class RecoveryWatermarkSummary(RuntimeContractModel):
    high_watermark: str | None = None
    max_date: date | None = None
    row_count: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def require_summary_value(self) -> Self:
        if self.high_watermark is None and self.max_date is None and self.row_count is None:
            raise ValueError("watermark summary requires at least one value")
        return self


class RecoveryInventoryRequirement(RuntimeContractModel):
    logical_role: str = Field(min_length=1)
    artifact_role: RecoveryArtifactRole
    restore_path: str

    @field_validator("restore_path")
    @classmethod
    def validate_restore_path(cls, value: str) -> str:
        return _validate_safe_relative_path(value)


class RecoveryInventoryPlan(RuntimeContractModel):
    plan_id: Sha256 | None = None
    plan_version: int = Field(ge=1)
    requirements: tuple[RecoveryInventoryRequirement, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_plan(self) -> Self:
        logical_roles = [item.logical_role for item in self.requirements]
        if len(logical_roles) != len(set(logical_roles)):
            raise ValueError("inventory logical roles must be unique")
        restore_paths = [item.restore_path for item in self.requirements]
        if len(restore_paths) != len(set(restore_paths)):
            raise ValueError("inventory restore paths must be unique")
        present_roles = {item.artifact_role for item in self.requirements}
        missing_roles = REQUIRED_ARTIFACT_ROLES - present_roles
        if missing_roles:
            missing = ", ".join(sorted(role.value for role in missing_roles))
            raise ValueError(f"inventory plan is missing required artifact roles: {missing}")
        canonical = tuple(sorted(self.requirements, key=lambda item: item.logical_role))
        object.__setattr__(self, "requirements", canonical)
        expected = canonical_sha256({"plan_version": self.plan_version, "requirements": canonical})
        if self.plan_id is not None and self.plan_id != expected:
            raise ValueError("plan_id does not match inventory plan content")
        object.__setattr__(self, "plan_id", expected)
        return self


class RecoveryArtifactSource(RuntimeContractModel):
    logical_role: str = Field(min_length=1)
    artifact_role: RecoveryArtifactRole
    absolute_path: str
    generation_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    watermark: RecoveryWatermarkSummary

    @field_validator("absolute_path")
    @classmethod
    def validate_absolute_path(cls, value: str) -> str:
        return _validate_absolute_path(value)


class RecoveryArtifactEntry(RuntimeContractModel):
    logical_role: str = Field(min_length=1)
    artifact_role: RecoveryArtifactRole
    absolute_path: str
    restore_path: str
    size_bytes: int = Field(ge=0)
    sha256: Sha256
    generation_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    watermark: RecoveryWatermarkSummary

    @field_validator("absolute_path")
    @classmethod
    def validate_absolute_path(cls, value: str) -> str:
        return _validate_absolute_path(value)

    @field_validator("restore_path")
    @classmethod
    def validate_restore_path(cls, value: str) -> str:
        return _validate_safe_relative_path(value)


class RecoveryManifest(RuntimeContractModel):
    manifest_id: Sha256 | None = None
    inventory_plan: RecoveryInventoryPlan
    captured_at: AwareUtcDatetime
    entries: tuple[RecoveryArtifactEntry, ...] = Field(min_length=1)

    def identity_payload(self) -> dict[str, object]:
        return {
            "inventory_plan": self.inventory_plan,
            "captured_at": self.captured_at,
            "entries": self.entries,
        }

    def calculate_manifest_id(self) -> str:
        return canonical_sha256(self.identity_payload())

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        logical_roles = [entry.logical_role for entry in self.entries]
        if len(logical_roles) != len(set(logical_roles)):
            raise ValueError("manifest logical roles must be unique")
        restore_paths = [entry.restore_path for entry in self.entries]
        if len(restore_paths) != len(set(restore_paths)):
            raise ValueError("manifest restore paths must be unique")
        canonical_entries = tuple(sorted(self.entries, key=lambda entry: entry.logical_role))
        requirement_by_role = {
            requirement.logical_role: requirement
            for requirement in self.inventory_plan.requirements
        }
        if set(logical_roles) != set(requirement_by_role):
            raise ValueError("manifest entries must exactly cover inventory plan")
        for entry in canonical_entries:
            requirement = requirement_by_role[entry.logical_role]
            if entry.artifact_role is not requirement.artifact_role:
                raise ValueError(f"manifest role mismatch for {entry.logical_role}")
            if entry.restore_path != requirement.restore_path:
                raise ValueError(f"manifest restore path mismatch for {entry.logical_role}")
        object.__setattr__(self, "entries", canonical_entries)
        expected = self.calculate_manifest_id()
        if self.manifest_id is not None and self.manifest_id != expected:
            raise ValueError("manifest_id does not match manifest content")
        object.__setattr__(self, "manifest_id", expected)
        return self


class RecoveryRpoAssessment(RuntimeContractModel):
    realtime_batch_lag: int = Field(ge=0)
    realtime_within_one_batch: bool = False
    research_rebuildable: bool
    research_rebuild_basis: tuple[str, ...]

    @model_validator(mode="after")
    def derive_rpo_status(self) -> Self:
        expected = self.realtime_batch_lag <= 1
        if self.realtime_within_one_batch not in {False, expected}:
            raise ValueError("realtime_within_one_batch does not match realtime_batch_lag")
        object.__setattr__(self, "realtime_within_one_batch", expected)
        if self.research_rebuildable and not self.research_rebuild_basis:
            raise ValueError("rebuildable research requires an explicit rebuild basis")
        if not self.research_rebuildable and self.research_rebuild_basis:
            raise ValueError("non-rebuildable research cannot declare a rebuild basis")
        if len(self.research_rebuild_basis) != len(set(self.research_rebuild_basis)):
            raise ValueError("research rebuild basis must be unique")
        object.__setattr__(
            self,
            "research_rebuild_basis",
            tuple(sorted(self.research_rebuild_basis)),
        )
        return self


class RecoveryVerificationResult(RuntimeContractModel):
    passed: bool
    checks: tuple[str, ...] = Field(min_length=1)
    rpo: RecoveryRpoAssessment

    @field_validator("checks")
    @classmethod
    def validate_checks(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item for item in value):
            raise ValueError("verification checks cannot be empty")
        if len(value) != len(set(value)):
            raise ValueError("verification checks must be unique")
        return value


class RecoveryCurrentPointer(RuntimeContractModel):
    generation_id: str = Field(min_length=1)
    manifest_id: Sha256
    published_at: AwareUtcDatetime
    previous_generation_id: str | None = None

    @model_validator(mode="after")
    def validate_pointer(self) -> Self:
        if self.previous_generation_id == self.generation_id:
            raise ValueError("previous generation must differ from current generation")
        return self


class RecoveryRehearsalReport(RuntimeContractModel):
    report_id: Sha256 | None = None
    manifest_id: Sha256
    status: RecoveryRehearsalStatus
    target_root: str
    started_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    previous_generation_id: str | None = None
    published_generation_id: str | None = None
    restored_logical_roles: tuple[str, ...]
    verification: RecoveryVerificationResult | None = None
    error: str | None = None

    @field_validator("target_root")
    @classmethod
    def validate_target_root(cls, value: str) -> str:
        return _validate_absolute_path(value)

    def identity_payload(self) -> dict[str, object]:
        return self.model_dump(mode="python", exclude={"report_id"})

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("completed_at cannot precede started_at")
        if len(self.restored_logical_roles) != len(set(self.restored_logical_roles)):
            raise ValueError("restored logical roles must be unique")
        object.__setattr__(
            self,
            "restored_logical_roles",
            tuple(sorted(self.restored_logical_roles)),
        )
        if self.status is RecoveryRehearsalStatus.PASSED:
            if self.error is not None:
                raise ValueError("passed rehearsal cannot have an error")
            if self.verification is None or not self.verification.passed:
                raise ValueError("passed rehearsal requires passing verification")
            if self.published_generation_id != self.manifest_id:
                raise ValueError("passed rehearsal must publish the manifest generation")
        else:
            if not self.error:
                raise ValueError("failed rehearsal requires an error")
            if self.published_generation_id is not None:
                raise ValueError("failed rehearsal cannot publish a generation")
        expected = canonical_sha256(self.identity_payload())
        if self.report_id is not None and self.report_id != expected:
            raise ValueError("report_id does not match report content")
        object.__setattr__(self, "report_id", expected)
        return self


class RecoveryRehearsalError(RecoveryManifestError):
    def __init__(self, message: str, report: RecoveryRehearsalReport) -> None:
        super().__init__(message)
        self.report = report


RecoveryVerifier = Callable[[Path, RecoveryManifest], RecoveryVerificationResult]
RecoveryFaultInjector = Callable[[RecoveryFaultPoint, Path], None]


def _reject_symlink_components(path: Path, *, subject: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            raise RecoveryManifestError(f"{subject} path contains symlink component: {current}")


def _source_descriptor(path: Path) -> int:
    _reject_symlink_components(path, subject="source")
    if path.is_symlink():
        raise RecoveryManifestError(f"source is a symlink: {path}")
    try:
        return os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError as exc:
        raise RecoveryManifestError(f"source missing: {path}") from exc
    except OSError as exc:
        if path.is_symlink():
            raise RecoveryManifestError(f"source is a symlink: {path}") from exc
        raise RecoveryManifestError(f"cannot open recovery source {path}: {exc}") from exc


def _hash_descriptor(descriptor: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while chunk := os.read(descriptor, 1024 * 1024):
        digest.update(chunk)
        size += len(chunk)
    return size, digest.hexdigest()


def _inspect_source(path: Path) -> tuple[int, str]:
    descriptor = _source_descriptor(path)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RecoveryManifestError(f"recovery source is not a regular file: {path}")
        size, digest = _hash_descriptor(descriptor)
        after = os.fstat(descriptor)
        identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        if identity_before != identity_after or size != after.st_size:
            raise RecoveryManifestError(f"recovery source changed while hashing: {path}")
        return size, digest
    finally:
        os.close(descriptor)


def build_recovery_manifest(
    *,
    plan: RecoveryInventoryPlan,
    sources: Iterable[RecoveryArtifactSource],
    captured_at: AwareUtcDatetime,
) -> RecoveryManifest:
    source_items = tuple(sources)
    logical_roles = [source.logical_role for source in source_items]
    if len(logical_roles) != len(set(logical_roles)):
        raise RecoveryManifestError("source logical roles must be unique")
    source_by_role = {source.logical_role: source for source in source_items}
    required_by_role = {requirement.logical_role: requirement for requirement in plan.requirements}
    missing = sorted(set(required_by_role) - set(source_by_role))
    if missing:
        raise RecoveryManifestError(f"missing inventory roles: {', '.join(missing)}")
    extra = sorted(set(source_by_role) - set(required_by_role))
    if extra:
        raise RecoveryManifestError(f"undeclared inventory roles: {', '.join(extra)}")

    entries: list[RecoveryArtifactEntry] = []
    for logical_role in sorted(required_by_role):
        requirement = required_by_role[logical_role]
        source = source_by_role[logical_role]
        if source.artifact_role is not requirement.artifact_role:
            raise RecoveryManifestError(f"role mismatch for {logical_role}")
        path = Path(source.absolute_path)
        size_bytes, sha256 = _inspect_source(path)
        entries.append(
            RecoveryArtifactEntry(
                logical_role=logical_role,
                artifact_role=source.artifact_role,
                absolute_path=str(path),
                restore_path=requirement.restore_path,
                size_bytes=size_bytes,
                sha256=sha256,
                generation_id=source.generation_id,
                schema_version=source.schema_version,
                watermark=source.watermark,
            )
        )
    return RecoveryManifest(
        inventory_plan=plan,
        captured_at=captured_at,
        entries=tuple(entries),
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _serialized_bytes(model: RuntimeContractModel) -> bytes:
    return (model.model_dump_json() + "\n").encode("utf-8")


def _append_bytes(directory: Path, object_name: str, content: bytes) -> Path:
    _reject_symlink_components(directory, subject="append-only store")
    directory.mkdir(parents=True, exist_ok=True)
    if directory.is_symlink():
        raise RecoveryManifestError(f"append-only directory is a symlink: {directory}")
    target = directory / object_name
    if target.exists():
        if target.is_symlink() or target.read_bytes() != content:
            raise RecoveryManifestError(f"existing manifest object differs: {target}")
        return target
    temporary = directory / f".{object_name}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, target)
    except FileExistsError as exc:
        if target.is_symlink() or target.read_bytes() != content:
            raise RecoveryManifestError(f"existing manifest object differs: {target}") from exc
    finally:
        temporary.unlink(missing_ok=True)
    _fsync_directory(directory)
    return target


def append_recovery_manifest(directory: Path, manifest: RecoveryManifest) -> Path:
    if manifest.manifest_id is None:
        raise RecoveryManifestError("manifest has no content identity")
    return _append_bytes(
        Path(directory),
        f"{manifest.manifest_id}.json",
        _serialized_bytes(manifest),
    )


def load_recovery_manifest(path: Path) -> RecoveryManifest:
    try:
        return RecoveryManifest.model_validate_json(Path(path).read_bytes())
    except (OSError, ValueError) as exc:
        raise RecoveryManifestError(f"invalid recovery manifest {path}: {exc}") from exc


def read_recovery_current(target_root: Path) -> RecoveryCurrentPointer | None:
    path = Path(target_root) / "CURRENT.json"
    if not path.exists():
        return None
    if path.is_symlink():
        raise RecoveryManifestError("recovery CURRENT pointer cannot be a symlink")
    try:
        return RecoveryCurrentPointer.model_validate_json(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise RecoveryManifestError(f"invalid recovery CURRENT pointer: {exc}") from exc


def _copy_entry(entry: RecoveryArtifactEntry, candidate: Path) -> None:
    source = Path(entry.absolute_path)
    destination = candidate / entry.restore_path
    try:
        destination.relative_to(candidate)
    except ValueError as exc:
        raise RecoveryManifestError(
            f"restore path escapes candidate: {entry.restore_path}"
        ) from exc
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
    source_descriptor = _source_descriptor(source)
    target_descriptor = -1
    try:
        source_stat = os.fstat(source_descriptor)
        if not stat.S_ISREG(source_stat.st_mode):
            raise RecoveryManifestError(f"recovery source is not a regular file: {source}")
        target_descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        digest = hashlib.sha256()
        size = 0
        while chunk := os.read(source_descriptor, 1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(target_descriptor, view)
                view = view[written:]
        os.fsync(target_descriptor)
        final_source_stat = os.fstat(source_descriptor)
        source_identity = (
            source_stat.st_dev,
            source_stat.st_ino,
            source_stat.st_size,
            source_stat.st_mtime_ns,
        )
        final_source_identity = (
            final_source_stat.st_dev,
            final_source_stat.st_ino,
            final_source_stat.st_size,
            final_source_stat.st_mtime_ns,
        )
        if source_identity != final_source_identity:
            raise RecoveryManifestError(f"source changed during restore copy: {source}")
        if size != entry.size_bytes:
            raise RecoveryManifestError(f"source hash/size mismatch for {entry.logical_role}")
        if digest.hexdigest() != entry.sha256:
            raise RecoveryManifestError(f"source hash mismatch for {entry.logical_role}")
    finally:
        if target_descriptor >= 0:
            os.close(target_descriptor)
        os.close(source_descriptor)
    os.replace(temporary, destination)
    _fsync_directory(destination.parent)


def _verify_candidate(manifest: RecoveryManifest, candidate: Path) -> None:
    for entry in manifest.entries:
        restored = candidate / entry.restore_path
        if restored.is_symlink():
            raise RecoveryManifestError(f"restored artifact is a symlink: {entry.logical_role}")
        size, digest = _inspect_source(restored)
        if size != entry.size_bytes:
            raise RecoveryManifestError(f"restored size mismatch for {entry.logical_role}")
        if digest != entry.sha256:
            raise RecoveryManifestError(f"restored hash mismatch for {entry.logical_role}")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _append_report(target_root: Path, report: RecoveryRehearsalReport) -> Path:
    if report.report_id is None:
        raise RecoveryManifestError("rehearsal report has no content identity")
    return _append_bytes(
        target_root / "reports",
        f"{report.report_id}.json",
        _serialized_bytes(report),
    )


def _rpo_error(verification: RecoveryVerificationResult) -> str | None:
    if not verification.passed:
        return "recovery verifier rejected candidate"
    if not verification.rpo.realtime_within_one_batch:
        return "real-time RPO exceeds one batch"
    if not verification.rpo.research_rebuildable:
        return "research is not rebuildable from manifest"
    return None


def _restore_previous_pointer(
    target_root: Path,
    previous_pointer_bytes: bytes | None,
) -> None:
    current_path = target_root / "CURRENT.json"
    if previous_pointer_bytes is None:
        current_path.unlink(missing_ok=True)
        _fsync_directory(target_root)
    else:
        _atomic_write(current_path, previous_pointer_bytes)


def rehearse_restore(
    *,
    manifest: RecoveryManifest,
    target_root: Path,
    started_at: AwareUtcDatetime,
    completed_at: AwareUtcDatetime,
    verifier: RecoveryVerifier,
    fault_injector: RecoveryFaultInjector | None = None,
) -> RecoveryRehearsalReport:
    root = Path(target_root)
    if not root.is_absolute():
        raise RecoveryManifestError("restore target_root must be absolute")
    _reject_symlink_components(root, subject="target")
    if root.exists() and root.is_symlink():
        raise RecoveryManifestError("restore target_root cannot be a symlink")
    root.mkdir(parents=True, exist_ok=True)
    resolved_root = root.resolve()
    for entry in manifest.entries:
        source = Path(entry.absolute_path).resolve(strict=False)
        if source == resolved_root or source.is_relative_to(resolved_root):
            raise RecoveryManifestError("restore target must be isolated from recovery sources")

    previous = read_recovery_current(root)
    current_path = root / "CURRENT.json"
    previous_pointer_bytes = current_path.read_bytes() if current_path.exists() else None
    candidate = root / f".candidate-{manifest.manifest_id}-{uuid.uuid4().hex}"
    generations = root / "generations"
    final_generation = generations / str(manifest.manifest_id)
    verification: RecoveryVerificationResult | None = None
    generation_staged = False
    published = False
    restored_roles: tuple[str, ...] = ()

    try:
        if final_generation.exists():
            raise RecoveryManifestError(
                f"recovery generation already exists: {manifest.manifest_id}"
            )
        candidate.mkdir()
        for entry in manifest.entries:
            _copy_entry(entry, candidate)
        restored_roles = tuple(entry.logical_role for entry in manifest.entries)
        if fault_injector is not None:
            fault_injector(RecoveryFaultPoint.AFTER_COPY, candidate)

        _verify_candidate(manifest, candidate)
        _atomic_write(candidate / "RECOVERY_MANIFEST.json", _serialized_bytes(manifest))
        if fault_injector is not None:
            fault_injector(RecoveryFaultPoint.AFTER_HASH_VERIFY, candidate)

        verification = verifier(candidate, manifest)
        rpo_error = _rpo_error(verification)
        if rpo_error is not None:
            raise RecoveryManifestError(rpo_error)
        if fault_injector is not None:
            fault_injector(RecoveryFaultPoint.BEFORE_ATOMIC_PUBLISH, candidate)

        generations.mkdir(parents=True, exist_ok=True)
        candidate.rename(final_generation)
        generation_staged = True
        _fsync_directory(generations)
        if fault_injector is not None:
            fault_injector(RecoveryFaultPoint.AFTER_GENERATION_STAGE, final_generation)
        pointer = RecoveryCurrentPointer(
            generation_id=str(manifest.manifest_id),
            manifest_id=str(manifest.manifest_id),
            published_at=completed_at,
            previous_generation_id=previous.generation_id if previous is not None else None,
        )
        _atomic_write(current_path, _serialized_bytes(pointer))
        published = True
        if fault_injector is not None:
            fault_injector(RecoveryFaultPoint.AFTER_CURRENT_SWITCH, final_generation)
        report = RecoveryRehearsalReport(
            manifest_id=str(manifest.manifest_id),
            status=RecoveryRehearsalStatus.PASSED,
            target_root=str(root),
            started_at=started_at,
            completed_at=completed_at,
            previous_generation_id=previous.generation_id if previous is not None else None,
            published_generation_id=str(manifest.manifest_id),
            restored_logical_roles=restored_roles,
            verification=verification,
        )
        _append_report(root, report)
        return report
    except Exception as exc:
        if published:
            _restore_previous_pointer(root, previous_pointer_bytes)
        if generation_staged:
            shutil.rmtree(final_generation, ignore_errors=True)
            _fsync_directory(generations)
        shutil.rmtree(candidate, ignore_errors=True)
        _fsync_directory(root)
        report = RecoveryRehearsalReport(
            manifest_id=str(manifest.manifest_id),
            status=RecoveryRehearsalStatus.FAILED,
            target_root=str(root),
            started_at=started_at,
            completed_at=completed_at,
            previous_generation_id=previous.generation_id if previous is not None else None,
            restored_logical_roles=restored_roles,
            verification=verification,
            error=str(exc),
        )
        _append_report(root, report)
        raise RecoveryRehearsalError(str(exc), report) from exc
